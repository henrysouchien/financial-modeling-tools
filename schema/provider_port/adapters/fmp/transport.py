"""Live FMP transport boundary and legacy bulk-build fetch compatibility."""

from __future__ import annotations

import asyncio
import functools
import importlib
import logging
from typing import Any

from research.source_html import provider_symbol


STATEMENT_ENDPOINTS = ("income_statement", "balance_sheet", "cash_flow")
MARKET_INPUT_ENDPOINTS = ("quote", "profile")
STATEMENT_LIMIT = 5


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
    "fetch_build_financials",
    "fmp",
    "live_fetch",
    "live_fetch_with_lineage",
    "provider_symbol",
    "records",
]
