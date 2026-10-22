"""Standalone financial data fetch helpers for model-engine tools."""

from __future__ import annotations

import json
import os
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

try:  # full-repo context: real collapsed->native SEC translation (cache-only, fail-open)
    from research.source_html import provider_symbol  # type: ignore
except Exception:  # standalone context without the main api: fail-open identity
    def provider_symbol(ticker: str) -> str:
        return ticker


async def fetch_model_build_financials(
    ticker: str,
) -> dict[str, list[dict[str, Any]]]:
    from schema.provider_port.adapters.fmp.transport import fetch_build_financials

    return await fetch_build_financials(ticker)


def _with_timeout_s(payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    result = dict(payload)
    result["timeout_s"] = timeout_s
    return result


def make_edgar_financials_fetcher():
    api_key = os.getenv("EDGAR_API_KEY", "")
    base_url = os.getenv("EDGAR_API_URL", "https://www.edgarparser.com").rstrip("/") + "/api/financials"

    def _fetcher(ticker: str, year: int, quarter: int = 4, full_year_mode: bool = True) -> dict[str, Any]:
        params = {
            "ticker": provider_symbol(ticker),
            "year": str(int(year)),
            "quarter": str(int(quarter)),
            "full_year_mode": str(bool(full_year_mode)).lower(),
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "financial-modeling-tools",
                "Authorization": f"Bearer {api_key}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("EDGAR API response was not a JSON object")
                return _with_timeout_s(payload, 120)
        except urllib.error.HTTPError as exc:
            return _with_timeout_s(
                {
                    "status": "error",
                    "message": "EDGAR financials request failed",
                    "http_status": int(exc.code),
                    "request_ticker": ticker,
                    "request_year": int(year),
                },
                120,
            )
        except Exception:
            raise RuntimeError("EDGAR financials request failed") from None

    return _fetcher
