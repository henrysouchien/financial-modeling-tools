"""FMP-native record aliases and normalization helpers.

No consumer imports this module.  It is the adapter-owned boundary for legacy
and current FMP response spellings.
"""

from __future__ import annotations

from datetime import datetime
import json
import re
from typing import Any


PRICE_FIELDS = ("price", "currentPrice", "previousClose")
PE_FIELDS = ("peRatio", "peRatioTTM", "priceEarningsRatio", "priceToEarningsRatio")
PE_TTM_FIELDS = PE_FIELDS + ("priceToEarningsRatioTTM", "priceEarningsRatioTTM")
EARNINGS_YIELD_FIELDS = ("earningsYield",)
EARNINGS_YIELD_TTM_FIELDS = ("earningsYieldTTM", "earningsYield")


def coerce_number(value: Any) -> float | None:
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
        value = coerce_number(record.get(field))
        if value is not None:
            return value
    return None


def record_datetime(record: dict[str, Any]) -> datetime | None:
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


def extract_peer_symbols(
    records: tuple[dict, ...] | list[dict],
    ticker: str,
    *,
    max_peers: int,
) -> list[str]:
    ticker_upper = ticker.upper()
    peers: list[str] = []

    def add(candidate: Any) -> bool:
        if not isinstance(candidate, str):
            return False
        peer = candidate.strip().upper()
        if not peer or peer == ticker_upper or peer in peers:
            return False
        peers.append(peer)
        return len(peers) >= max_peers

    for record in records:
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
                if add(value):
                    return peers
        if not matched_array and add(record.get("symbol")):
            return peers
    return peers


__all__ = [
    "EARNINGS_YIELD_FIELDS",
    "EARNINGS_YIELD_TTM_FIELDS",
    "PE_FIELDS",
    "PE_TTM_FIELDS",
    "PRICE_FIELDS",
    "coerce_number",
    "extract_peer_symbols",
    "first_numeric",
    "record_datetime",
]
