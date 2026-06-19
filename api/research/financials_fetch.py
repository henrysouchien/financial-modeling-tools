"""Standalone financial data fetch helpers for model-engine tools."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
from typing import Any
import urllib.error
import urllib.parse
import urllib.request


_STATEMENT_ENDPOINTS = ("income_statement", "balance_sheet", "cash_flow")
_MARKET_INPUT_ENDPOINTS = ("quote", "profile")


class _LazyFmpPackage:
    def fetch(self, *args: Any, **kwargs: Any) -> Any:
        return importlib.import_module("fmp").fetch(*args, **kwargs)


fmp: Any = _LazyFmpPackage()


def _records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, dict):
        return [frame] if frame else []
    if isinstance(frame, list):
        return [record for record in frame if isinstance(record, dict)]
    to_dict = getattr(frame, "to_dict", None)
    if not callable(to_dict):
        return []
    records = to_dict("records")
    if isinstance(records, list):
        return [record for record in records if isinstance(record, dict)]
    return []


async def fetch_fmp_financials(ticker: str) -> dict[str, list[dict[str, Any]]]:
    statement_frames = await asyncio.gather(
        *(
            asyncio.to_thread(fmp.fetch, endpoint, symbol=ticker)
            for endpoint in _STATEMENT_ENDPOINTS
        )
    )
    data = {
        endpoint: _records(frame)
        for endpoint, frame in zip(_STATEMENT_ENDPOINTS, statement_frames)
    }
    market_frames = await asyncio.gather(
        *(
            asyncio.to_thread(fmp.fetch, endpoint, symbol=ticker)
            for endpoint in _MARKET_INPUT_ENDPOINTS
        ),
        return_exceptions=True,
    )
    for endpoint, result in zip(_MARKET_INPUT_ENDPOINTS, market_frames):
        if isinstance(result, Exception):
            logging.warning("FMP %s fetch failed for %s: %s", endpoint, ticker, result)
            data[endpoint] = []
            continue
        data[endpoint] = _records(result)
    return data


def make_edgar_financials_fetcher():
    api_key = os.getenv("EDGAR_API_KEY", "")
    base_url = os.getenv("EDGAR_API_URL", "https://www.edgarparser.com").rstrip("/") + "/api/financials"

    def _fetcher(ticker: str, year: int, quarter: int = 4, full_year_mode: bool = True) -> dict[str, Any]:
        params = {
            "ticker": ticker,
            "year": str(int(year)),
            "quarter": str(int(quarter)),
            "full_year_mode": str(bool(full_year_mode)).lower(),
            "key": api_key,
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"User-Agent": "financial-modeling-tools"})
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("EDGAR API response was not a JSON object")
                return payload
        except urllib.error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            try:
                decoded = json.loads(raw_body) if raw_body else {}
            except json.JSONDecodeError:
                decoded = {}
            payload = decoded if isinstance(decoded, dict) else {}
            payload.setdefault("status", "error")
            payload.setdefault("message", raw_body.strip() or str(exc))
            payload["http_status"] = int(exc.code)
            payload["request_ticker"] = ticker
            payload["request_year"] = int(year)
            return payload
        except Exception as exc:
            raise RuntimeError(f"EDGAR financials fetch failed for {ticker} {year}: {exc}") from exc

    return _fetcher

