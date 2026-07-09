"""Standalone provider-symbol helpers for model-engine imports.

The full application resolves collapsed tickers through its cached SEC company
map.  That cache is not part of the standalone package, so the compatibility
surface deliberately fails open to the supplied symbol.
"""

from __future__ import annotations


def provider_symbol(ticker: str) -> str:
    return ticker


def provider_symbols_csv(csv: str) -> str:
    if not isinstance(csv, str) or not csv:
        return csv
    return ",".join(provider_symbol(token.strip()) for token in csv.split(","))


__all__ = ["provider_symbol", "provider_symbols_csv"]
