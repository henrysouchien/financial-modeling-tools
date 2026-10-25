"""Live FMP transport boundary and legacy bulk-build fetch compatibility."""

from __future__ import annotations

import asyncio
import functools
import importlib
import json
import logging
import os
from pathlib import Path
from typing import Any

from schema.ticker_contract import normalize_ticker


STATEMENT_ENDPOINTS = ("income_statement", "balance_sheet", "cash_flow")
MARKET_INPUT_ENDPOINTS = ("quote", "profile")
STATEMENT_LIMIT = 5
_SEC_NATIVE_SYMBOL_CACHED_ONLY_MEMO: dict[tuple[str, int], dict[str, str]] = {}


def _source_html_cache_root() -> Path:
    configured = os.getenv("SOURCE_HTML_CACHE_DIR")
    if configured is not None:
        return Path(configured)
    user_data = os.getenv("USER_DATA_DIR", "").strip()
    if user_data:
        data_root = Path(user_data).expanduser()
    else:
        state_home = os.getenv("XDG_STATE_HOME", "").strip()
        state_root = (
            Path(state_home).expanduser()
            if state_home
            else Path.home() / ".local" / "state"
        )
        data_root = state_root / "hank" / "data"
    return data_root / "source_html"


def _ticker_records(tickers: Any) -> Any:
    return tickers.values() if isinstance(tickers, dict) else tickers


def _build_sec_native_symbol_map(tickers: Any) -> dict[str, str]:
    aliases: dict[str, str] = {}
    collisions: set[str] = set()
    for record in _ticker_records(tickers):
        if not isinstance(record, dict):
            continue
        native_ticker = str(record.get("ticker") or "").strip()
        if not native_ticker:
            continue
        collapsed = normalize_ticker(native_ticker)
        if not collapsed:
            continue
        existing = aliases.get(collapsed)
        if existing is None:
            if collapsed not in collisions:
                aliases[collapsed] = native_ticker
            continue
        if existing != native_ticker:
            aliases.pop(collapsed, None)
            collisions.add(collapsed)
    return aliases


def reset_sec_native_symbol_cached_only_memo() -> None:
    _SEC_NATIVE_SYMBOL_CACHED_ONLY_MEMO.clear()


def sec_native_symbol_cached_only(collapsed: object) -> str | None:
    try:
        cache_path = _source_html_cache_root() / "sec_cache" / "company_tickers.json"
        stat_result = cache_path.stat()
        memo_key = (str(cache_path), stat_result.st_mtime_ns)
        aliases = _SEC_NATIVE_SYMBOL_CACHED_ONLY_MEMO.get(memo_key)
        if aliases is None:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            aliases = _build_sec_native_symbol_map(payload)
            for existing_key in list(_SEC_NATIVE_SYMBOL_CACHED_ONLY_MEMO):
                if existing_key[0] == memo_key[0] and existing_key != memo_key:
                    _SEC_NATIVE_SYMBOL_CACHED_ONLY_MEMO.pop(existing_key, None)
            _SEC_NATIVE_SYMBOL_CACHED_ONLY_MEMO[memo_key] = aliases
        return aliases.get(normalize_ticker(str(collapsed or "")))
    except Exception:
        return None


def provider_symbol(ticker: str) -> str:
    return sec_native_symbol_cached_only(ticker) or ticker


def provider_symbols_csv(csv: str | None) -> str | None:
    if not isinstance(csv, str) or not csv:
        return csv
    return ",".join(provider_symbol(token.strip()) for token in csv.split(","))


@functools.cache
def _configured_fmp_package() -> Any:
    # This transport is the add-in's FMP provider bootstrap: every in-process
    # fetch (gateway, autonomous child, registry-building scripts) reaches the
    # package through it. The add-in selects the package's standalone dispatch
    # policy (no Risk budget guard) once per process, on first use.
    importlib.import_module("fmp.client").configure_client()
    return importlib.import_module("fmp")


class _LazyFmpPackage:
    def fetch(self, *args: Any, **kwargs: Any) -> Any:
        return _configured_fmp_package().fetch(*args, **kwargs)

    def fetch_with_lineage(self, *args: Any, **kwargs: Any) -> Any:
        return _configured_fmp_package().fetch_with_lineage(*args, **kwargs)


fmp: Any = _LazyFmpPackage()


def records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, dict):
        data = frame.get("data")
        if isinstance(data, list):
            return [record for record in data if isinstance(record, dict)]
        historical = frame.get("historical")
        if isinstance(historical, list):
            return [record for record in historical if isinstance(record, dict)]
        profile = frame.get("profile")
        if isinstance(profile, dict):
            return [profile]
        return [frame] if frame else []
    if isinstance(frame, list):
        return [record for record in frame if isinstance(record, dict)]
    to_dict = getattr(frame, "to_dict", None)
    if not callable(to_dict):
        return []
    rows = to_dict("records")
    if isinstance(rows, list):
        return [record for record in rows if isinstance(record, dict)]
    return []


def live_fetch_with_lineage(
    endpoint: str,
    *,
    ticker: str,
    **params: object,
) -> Any:
    """Fetch one provider-issued result after applying adapter request mapping."""

    provider_ticker = ",".join(
        provider_symbol(symbol) for symbol in ticker.split(",")
    )
    provider_params = (
        dict(params)
        if endpoint == "company_screener"
        else {"symbol": provider_ticker, **params}
    )
    return fmp.fetch_with_lineage(endpoint, **provider_params)


def live_fetch(endpoint: str, *, ticker: str, **params: object) -> list[dict[str, Any]]:
    """Project provider-issued records into the legacy bulk-fetch shape."""

    result = live_fetch_with_lineage(endpoint, ticker=ticker, **params)
    return [dict(record) for record in result.records]


async def fetch_build_financials(
    ticker: str,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch the legacy raw build-input bundle inside the adapter boundary."""

    statement_frames = await asyncio.gather(
        *(
            asyncio.to_thread(
                live_fetch,
                endpoint,
                ticker=ticker,
                limit=STATEMENT_LIMIT,
            )
            for endpoint in STATEMENT_ENDPOINTS
        )
    )
    data = {
        endpoint: frame
        for endpoint, frame in zip(STATEMENT_ENDPOINTS, statement_frames)
    }
    market_frames = await asyncio.gather(
        *(
            asyncio.to_thread(live_fetch, endpoint, ticker=ticker)
            for endpoint in MARKET_INPUT_ENDPOINTS
        ),
        return_exceptions=True,
    )
    for endpoint, result in zip(MARKET_INPUT_ENDPOINTS, market_frames):
        if isinstance(result, BaseException):
            logging.warning("FMP %s fetch failed for %s: %s", endpoint, ticker, result)
            data[endpoint] = []
            continue
        data[endpoint] = result
    return data


__all__ = [
    "_build_sec_native_symbol_map",
    "_source_html_cache_root",
    "_ticker_records",
    "fetch_build_financials",
    "fmp",
    "live_fetch",
    "live_fetch_with_lineage",
    "provider_symbol",
    "provider_symbols_csv",
    "records",
    "reset_sec_native_symbol_cached_only_memo",
    "sec_native_symbol_cached_only",
]
