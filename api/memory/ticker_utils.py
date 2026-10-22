"""Ticker symbol normalization and validation utilities."""

from __future__ import annotations

import re


TICKER_RE = re.compile(r"^[A-Z]{1,6}$")
EXTENDED_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.]{0,14}$")
ENTITY_TICKER_RE = re.compile(r"^[A-Z0-9]{1,6}$")
_EXCHANGE_SUFFIXES = (
    ".TO", ".HK", ".AX", ".PA", ".DE", ".SW", ".AS", ".SS", ".SZ",
    ".OL", ".MI", ".CO", ".ST", ".HE", ".BR", ".SA", ".SI", ".KS",
    ".TW", ".BO", ".NS",
    ".L", ".T",
)
# Dot-form (BRK.A) and hyphen-form (BRK-A) share classes collapse to BRKA.
# Preferred lines (EFC-PC, PPL-PA) do not end in exact -A/-B and stay rejected.
_SHARE_CLASS_SUFFIXES = (".A", ".B", "-A", "-B")


def normalize_ticker(raw: str) -> str:
    value = raw.strip().upper()
    # Preserve invalid double-dot input so callers can reject it before suffix
    # handling or share-class collapse could turn it into a valid-looking value.
    if ".." in value:
        return value
    if value.endswith("."):
        value = value[:-1]
    for suffix in _EXCHANGE_SUFFIXES:
        if value.endswith(suffix):
            value = value[:-len(suffix)]
            break
    for suffix in _SHARE_CLASS_SUFFIXES:
        # len guard: a bare suffix must not collapse to a valid single letter.
        if value.endswith(suffix) and len(value) > len(suffix):
            value = value[:-len(suffix)] + suffix[-1]
            break
    return value


def is_contract_ticker(s: str) -> bool:
    """Return whether *s* satisfies the normalized CONTRACT ticker rule."""
    return bool(EXTENDED_TICKER_RE.fullmatch(s)) and ".." not in s and not s.endswith(".")


def is_entity_ticker(s: str) -> bool:
    """Return whether *s* is a bounded ticker entity with at least one letter."""
    return bool(ENTITY_TICKER_RE.fullmatch(s)) and any("A" <= char <= "Z" for char in s)
