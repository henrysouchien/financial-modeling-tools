from __future__ import annotations

from pathlib import PurePosixPath
import re


_TICKER_RE = re.compile(r"^[A-Z]{1,6}$")
_EXCHANGE_SUFFIXES = (
    ".TO",
    ".HK",
    ".AX",
    ".PA",
    ".DE",
    ".SW",
    ".AS",
    ".SS",
    ".SZ",
    ".OL",
    ".MI",
    ".CO",
    ".ST",
    ".HE",
    ".BR",
    ".SA",
    ".SI",
    ".KS",
    ".TW",
    ".BO",
    ".NS",
    ".L",
    ".T",
)
_SHARE_CLASS_SUFFIXES = (".A", ".B")
_LEVEL_VALUES = frozenset({"low", "medium", "high"})
_SKILL_NAME_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_VALUATION_METHODS = frozenset({"dcf", "multiples", "sum_of_parts", "hybrid", "relative"})
_VALUATION_NUMERIC_FIELDS = frozenset(
    {
        "low",
        "mid",
        "high",
        "current_multiple",
        "wacc",
        "risk_free_rate",
        "equity_risk_premium",
        "cost_of_equity",
        "raw_beta",
        "adjusted_beta",
        "beta_floor",
        "terminal_growth_rate",
        "terminal_multiple",
    }
)
_VALUATION_RATE_FIELDS = frozenset(
    {
        "wacc",
        "risk_free_rate",
        "equity_risk_premium",
        "cost_of_equity",
        "terminal_growth_rate",
    }
)
_VALUATION_METHOD_ALIASES = {
    "discounted_cash_flow": "dcf",
    "ev_ebitda": "multiples",
    "ev_to_ebitda": "multiples",
    "forward_pe": "multiples",
    "forward_pe_triangulation": "multiples",
    "pe": "multiples",
    "price_earnings": "multiples",
    "relative_valuation": "relative",
    "sotp": "sum_of_parts",
    "sum_of_the_parts": "sum_of_parts",
}


def _normalize_optional_identifier(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_ticker(value: object) -> str:
    raw = str(value or "").strip().upper()
    if raw.endswith("."):
        raw = raw[:-1]
    for suffix in _EXCHANGE_SUFFIXES:
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)]
            break
    for suffix in _SHARE_CLASS_SUFFIXES:
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)] + suffix[-1]
            break
    if not _TICKER_RE.match(raw):
        raise ValueError("ticker must match ^[A-Z]{1,6}$")
    return raw


def _normalize_level(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized not in _LEVEL_VALUES:
        raise ValueError("value must be one of low, medium, high")
    return normalized


def _normalize_valuation_method(value: object | None) -> str | None:
    normalized = _normalize_optional_identifier(value)
    if normalized is None:
        return None
    token = re.sub(r"[^a-z0-9]+", "_", normalized.lower()).strip("_")
    if token in _VALUATION_METHODS:
        return token
    return _VALUATION_METHOD_ALIASES.get(token, normalized)


def _normalize_workspace_relative_path(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "\\" in text:
        raise ValueError("workspace-relative paths must use forward slashes")
    path = PurePosixPath(text)
    if path.is_absolute():
        raise ValueError("workspace-relative paths cannot be absolute")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("workspace-relative paths cannot contain empty, '.', or '..' segments")
    return path.as_posix()


__all__ = [
    "_EXCHANGE_SUFFIXES",
    "_LEVEL_VALUES",
    "_SKILL_NAME_RE",
    "_SHARE_CLASS_SUFFIXES",
    "_TICKER_RE",
    "_VALUATION_METHODS",
    "_VALUATION_METHOD_ALIASES",
    "_VALUATION_NUMERIC_FIELDS",
    "_VALUATION_RATE_FIELDS",
    "_normalize_level",
    "_normalize_optional_identifier",
    "_normalize_ticker",
    "_normalize_valuation_method",
    "_normalize_workspace_relative_path",
]
