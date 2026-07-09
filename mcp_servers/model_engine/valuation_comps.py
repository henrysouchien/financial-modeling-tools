from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import json
import logging
import re
import statistics
from typing import Any

from memory.ticker_utils import normalize_ticker
from api.research.source_html import provider_symbol, provider_symbols_csv

try:
  import fmp
except ModuleNotFoundError:  # pragma: no cover - depends on optional package env
  fmp = None


FMP_PRICE_FIELDS = ("price", "currentPrice", "previousClose")
FMP_EPS_AVG_FIELDS = ("epsAvg", "epsavg", "estimatedEpsAvg")
FMP_PE_FIELDS = ("peRatio", "peRatioTTM", "priceEarningsRatio", "priceToEarningsRatio")
FMP_PEG_FIELDS = (
  "pegRatio",
  "pegRatioTTM",
  "priceToEarningsGrowthRatio",
  "priceToEarningsGrowthRatioTTM",
)
FMP_EV_EBITDA_FIELDS = (
  "enterpriseValueOverEBITDA",
  "enterpriseValueOverEbitda",
  "evToEbitda",
  "evEbitda",
  "enterpriseValueMultiple",
  "enterpriseValueMultipleTTM",
  "evToEBITDA",
)


def comps_build_recovery() -> dict[str, list[str]]:
  return {
    "next_actions": [
      "Pass the v1.2 industry_peer_comparison payload with a non-empty sections list.",
      (
        "If industry_peer_comparison returned only peers, enable "
        "INDUSTRY_ANALYSIS_V1_2_ENABLED=true in the portfolio-reads-mcp runtime and rerun it."
      ),
    ]
  }


def comps_build_error_payload(
  exc: Exception,
  *,
  comps_build_id: str,
  validation_error_type: type[BaseException] | tuple[type[BaseException], ...] | None = None,
  safe_pydantic_errors_fn: Callable[[Any], Any] | None = None,
) -> dict[str, Any]:
  payload: dict[str, Any] = {
    "status": "error",
    "comps_build_id": comps_build_id,
    "error": str(exc),
    "recovery": comps_build_recovery(),
  }
  if (
    validation_error_type is not None
    and safe_pydantic_errors_fn is not None
    and isinstance(exc, validation_error_type)
  ):
    payload["validation_errors"] = safe_pydantic_errors_fn(exc)
  return payload


def fmp_frame_records(frame: Any) -> list[dict[str, Any]]:
  if frame is None:
    return []
  if isinstance(frame, dict):
    return [frame]
  if isinstance(frame, list):
    return [record for record in frame if isinstance(record, dict)]
  if hasattr(frame, "to_dict"):
    try:
      records = frame.to_dict("records")
      return [record for record in records if isinstance(record, dict)]
    except Exception:
      return []
  return []


def fetch_fmp_records(
  endpoint_name: str,
  *,
  fetcher: Any,
  **params: Any,
) -> list[dict[str, Any]]:
  try:
    return fmp_frame_records(fetcher(endpoint_name, **params))
  except Exception as exc:
    logging.info(
      "valuation_comps fallback FMP fetch failed endpoint=%s params=%s: %s",
      endpoint_name,
      params,
      exc,
    )
    return []


def default_fmp_fetch() -> Any:
  if fmp is None:
    raise RuntimeError(
      "FMP fallback data requires the optional fmp-mcp package. "
      "Install financial-model-engine[fmp] or pass an explicit fetcher."
    )
  return fmp.fetch


def financials_records(financials: dict | None, endpoint_name: str) -> list[dict[str, Any]]:
  if not isinstance(financials, dict):
    return []
  return fmp_frame_records(financials.get(endpoint_name))


def coerce_float(value: Any) -> float | None:
  if value is None:
    return None
  try:
    parsed = float(value)
  except (TypeError, ValueError):
    return None
  if not (parsed == parsed and parsed not in (float("inf"), float("-inf"))):
    return None
  return parsed


def first_numeric(record: dict[str, Any], fields: tuple[str, ...]) -> float | None:
  for field in fields:
    value = coerce_float(record.get(field))
    if value is not None:
      return value
  return None


def record_symbol(record: dict[str, Any]) -> str | None:
  for field in ("symbol", "ticker"):
    value = record.get(field)
    if isinstance(value, str) and value.strip():
      return value.strip().upper()
  return None


def quote_prices_by_symbol(records: list[dict[str, Any]]) -> dict[str, float]:
  prices: dict[str, float] = {}
  for record in records:
    symbol = record_symbol(record)
    price = first_numeric(record, FMP_PRICE_FIELDS)
    if symbol and price is not None and price > 0:
      prices[symbol] = price
  return prices


def _collapsed_quote_prices_by_symbol(records: list[dict[str, Any]]) -> dict[str, float]:
  return {
    normalize_ticker(symbol): price
    for symbol, price in quote_prices_by_symbol(records).items()
  }


def extract_peer_symbols(records: list[dict[str, Any]], ticker: str, *, max_peers: int) -> list[str]:
  ticker_upper = ticker.upper()
  peers: list[str] = []

  def _add(candidate: Any) -> bool:
    """Append a normalized peer; return True when max_peers is reached."""
    if not isinstance(candidate, str):
      return False
    peer = candidate.strip().upper()
    if not peer or peer == ticker_upper or peer in peers:
      return False
    peers.append(peer)
    return len(peers) >= max_peers

  for record in records:
    # Legacy schema: a single record carrying an array of peer symbols.
    matched_array = False
    for field in ("peersList", "peerList", "peers", "symbols"):
      raw = record.get(field)
      if raw is None:
        continue
      values = raw
      if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.startswith("["):
          try:
            values = json.loads(stripped)
          except json.JSONDecodeError:
            values = stripped.split(",")
        else:
          values = stripped.split(",")
      if not isinstance(values, list):
        continue
      matched_array = True
      for value in values:
        if _add(value):
          return peers
    # Current FMP stock_peers schema: one row per peer with a flat `symbol` field.
    if not matched_array and _add(record.get("symbol")):
      return peers
  return peers


def parse_record_date(record: dict[str, Any]) -> datetime | None:
  raw = record.get("date") or record.get("fiscalDateEnding") or record.get("calendarYear")
  if raw is None:
    return None
  text = str(raw).strip()
  if not text:
    return None
  if re.fullmatch(r"\d{4}", text):
    text = f"{text}-12-31"
  try:
    return datetime.fromisoformat(text[:10])
  except ValueError:
    return None


def fy1_eps_avg(records: list[dict[str, Any]]) -> float | None:
  candidates: list[tuple[int, datetime, int, float]] = []
  today = datetime.now(timezone.utc).replace(tzinfo=None)
  for index, record in enumerate(records):
    eps = first_numeric(record, FMP_EPS_AVG_FIELDS)
    if eps is None or eps <= 0:
      continue
    record_date = parse_record_date(record) or today
    future_rank = 0 if record_date >= today else 1
    candidates.append((future_rank, record_date, index, eps))
  if not candidates:
    return None
  candidates.sort(key=lambda item: (item[0], item[1], item[2]))
  return candidates[0][3]


def trailing_pe_range(records: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
  values = [
    value
    for record in records
    for value in [first_numeric(record, FMP_PE_FIELDS)]
    if value is not None and value > 0
  ]
  if not values:
    return None, None, None
  return min(values), statistics.median(values), max(values)


def latest_numeric(records: list[dict[str, Any]], fields: tuple[str, ...]) -> float | None:
  dated: list[tuple[datetime, int, float]] = []
  undated: list[tuple[int, float]] = []
  for index, record in enumerate(records):
    value = first_numeric(record, fields)
    if value is None:
      continue
    record_date = parse_record_date(record)
    if record_date is None:
      undated.append((index, value))
    else:
      dated.append((record_date, index, value))
  if dated:
    dated.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return dated[0][2]
  return undated[0][1] if undated else None


def valuation_comp_entry_from_fmp(
  symbol: str,
  *,
  price: float | None,
  estimate_records: list[dict[str, Any]],
  key_metric_records: list[dict[str, Any]],
) -> dict[str, Any] | None:
  # Public keyword kept for compatibility; records now carry ratios trailing multiples.
  eps_avg = fy1_eps_avg(estimate_records)
  forward_pe = price / eps_avg if price is not None and eps_avg else None
  trailing_low, trailing_median, trailing_high = trailing_pe_range(key_metric_records)
  entry: dict[str, Any] = {
    "ticker": symbol.upper(),
    "forward_pe": forward_pe,
    "peg": latest_numeric(key_metric_records, FMP_PEG_FIELDS),
    "ev_ebitda": latest_numeric(key_metric_records, FMP_EV_EBITDA_FIELDS),
    "trailing_low": trailing_low,
    "trailing_median": trailing_median,
    "trailing_high": trailing_high,
    "source": "build_fallback",
  }
  if any(entry.get(key) is not None for key in (
    "forward_pe",
    "peg",
    "ev_ebitda",
    "trailing_low",
    "trailing_median",
    "trailing_high",
  )):
    return entry
  return None


def build_valuation_comps_fallback(
  ticker: str,
  financials: dict | None = None,
  *,
  max_peers: int = 6,
  fetcher: Any | None = None,
) -> dict[str, Any] | None:
  """Best-effort valuation_comps payload using local FMP calls only.

  This intentionally mirrors the forward-P/E arithmetic locally instead of
  importing risk_module. The richer peer_comparison bridge is a PR-C caller
  responsibility; this fallback only gives a fresh build visible comp evidence.
  """

  ticker_upper = ticker.upper()
  fetcher = fetcher or default_fmp_fetch()
  peer_records = fetch_fmp_records("stock_peers", fetcher=fetcher, symbol=provider_symbol(ticker_upper))
  peers = extract_peer_symbols(peer_records, ticker_upper, max_peers=max_peers)
  symbols = [ticker_upper, *peers]
  quote_records = fetch_fmp_records("quote", fetcher=fetcher, symbol=provider_symbols_csv(",".join(symbols)))
  prices = _collapsed_quote_prices_by_symbol(quote_records)
  if ticker_upper not in prices:
    prices.update(_collapsed_quote_prices_by_symbol(financials_records(financials, "quote")))

  entries: dict[str, dict[str, Any]] = {}
  for symbol in symbols:
    estimates = fetch_fmp_records(
      "analyst_estimates",
      fetcher=fetcher,
      symbol=provider_symbol(symbol),
      period="annual",
      limit=4,
    )
    ratios = fetch_fmp_records(
      "ratios",
      fetcher=fetcher,
      symbol=provider_symbol(symbol),
      period="annual",
      limit=10,
    )
    entry = valuation_comp_entry_from_fmp(
      symbol,
      price=prices.get(normalize_ticker(symbol)),
      estimate_records=estimates,
      key_metric_records=ratios,
    )
    if entry is not None:
      entries[symbol] = entry

  target = entries.get(ticker_upper)
  if target is None:
    return None
  return {
    "source": "build_fallback",
    "basis": "forward_ntm_fy1",
    "target": target,
    "peers": [entries[symbol] for symbol in peers if symbol in entries],
  }


__all__ = [
  "FMP_EPS_AVG_FIELDS",
  "FMP_EV_EBITDA_FIELDS",
  "FMP_PE_FIELDS",
  "FMP_PEG_FIELDS",
  "FMP_PRICE_FIELDS",
  "comps_build_error_payload",
  "comps_build_recovery",
  "build_valuation_comps_fallback",
  "coerce_float",
  "extract_peer_symbols",
  "fetch_fmp_records",
  "financials_records",
  "first_numeric",
  "fmp_frame_records",
  "fy1_eps_avg",
  "latest_numeric",
  "parse_record_date",
  "quote_prices_by_symbol",
  "record_symbol",
  "trailing_pe_range",
  "valuation_comp_entry_from_fmp",
]
