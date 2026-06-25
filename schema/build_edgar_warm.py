"""EDGAR cache warming helpers for schema build orchestration."""

from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass
import logging
import sys
from typing import Any, Dict, List

from .segments import EdgarFinancialsFetcher


@dataclass(frozen=True)
class EdgarWarmResult:
    status: str
    payload: dict | None
    message: str | None = None


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        from . import build as parent
    return getattr(parent, name, fallback)


def warm_edgar_cache(
    ticker: str,
    historical_years: List[int],
    financials_fetcher: EdgarFinancialsFetcher,
) -> Dict[int, EdgarWarmResult]:
    """Warm the EDGAR FY financials cache for each historical year.

    Calls `/api/financials` for each year so subsequent `/api/metric/series`
    lookups hit a populated cache instead of silently returning empty.

    Returns {year: EdgarWarmResult}. Payload is retained for "success" and
    "partial" results; partial filings usually still have the balance-sheet
    face needed for presentation-tree diagnostics. The raw upstream status and
    message are preserved, plus the synthesized sentinel "exception" when the
    fetcher itself raises.
    Never raises -- warming is best-effort; populate_from_edgar() will
    surface genuinely unavailable data as `missing_concepts`.
    """
    results: Dict[int, EdgarWarmResult] = {}
    if not historical_years:
        return results

    warm_result = _parent_attr("EdgarWarmResult", EdgarWarmResult)
    warm_message = _parent_attr("_edgar_warm_message", _edgar_warm_message)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(financials_fetcher, ticker, int(year), 4, True): int(year)
            for year in historical_years
        }
        for future in concurrent.futures.as_completed(futures):
            year = futures[future]
            try:
                payload = future.result() or {}
                status = str(payload.get("status", "unknown"))
                retained_payload = payload if status in {"success", "partial"} else None
                message = warm_message(payload)
                results[year] = warm_result(
                    status=status,
                    payload=retained_payload,
                    message=message,
                )
                if status != "success":
                    logging.warning(
                        "EDGAR cache warm returned non-success for %s FY%d: %s%s",
                        ticker,
                        year,
                        status,
                        f" ({message})" if message else "",
                    )
            except Exception as exc:
                logging.warning("EDGAR cache warm failed for %s FY%d: %s", ticker, year, exc)
                results[year] = warm_result(
                    status="exception",
                    payload=None,
                    message=str(exc),
                )
    return results


def _edgar_warm_message(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("message", "error", "detail"):
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
        elif isinstance(value, (int, float, bool)):
            return str(value)
    return None
