"""Ticker symbol normalization and validation utilities."""

from __future__ import annotations

import re


TICKER_RE = re.compile(r"^[A-Z]{1,6}$")
_EXCHANGE_SUFFIXES = (
    ".TO", ".HK", ".AX", ".PA", ".DE", ".SW", ".AS", ".SS", ".SZ",
    ".OL", ".MI", ".CO", ".ST", ".HE", ".BR", ".SA", ".SI", ".KS",
    ".TW", ".BO", ".NS",
    ".L", ".T",
)
_SHARE_CLASS_SUFFIXES = (".A", ".B")


def normalize_ticker(raw: str) -> str:
    value = raw.strip().upper()
    if value.endswith("."):
        value = value[:-1]
    for suffix in _EXCHANGE_SUFFIXES:
        if value.endswith(suffix):
            value = value[:-len(suffix)]
            break
    for suffix in _SHARE_CLASS_SUFFIXES:
        if value.endswith(suffix):
            value = value[:-len(suffix)] + suffix[-1]
            break
    return value

