from __future__ import annotations

import re

from .strategy_categories.registry import STRATEGY_REGISTRY


_DIRECTION_VALUES = frozenset({"long", "short", "hedge", "pair"})
_TIMEFRAME_VALUES = frozenset({"near_term", "medium", "long_term"})
_TIME_HORIZON_VALUES = frozenset({"near-term", "medium-term", "long-term"})

_DIRECTION_ALIASES = {value: value for value in _DIRECTION_VALUES}
_TIMEFRAME_ALIASES = {value: value for value in _TIMEFRAME_VALUES}
_TIME_HORIZON_ALIASES = {
    "near": "near-term",
    "near_term": "near-term",
    "short": "near-term",
    "short_term": "near-term",
    "0_1_years": "near-term",
    "1_year": "near-term",
    "medium": "medium-term",
    "medium_term": "medium-term",
    "mid": "medium-term",
    "mid_term": "medium-term",
    "3_5_years": "medium-term",
    "3_to_5_years": "medium-term",
    "long": "long-term",
    "long_term": "long-term",
    "5_years": "long-term",
    "5_plus_years": "long-term",
    **{value.replace("-", "_"): value for value in _TIME_HORIZON_VALUES},
}


def _normalize_enum_token(value: object, *, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    token = str(value).strip()
    if not token:
        raise ValueError(f"{field_name} is required")
    return re.sub(r"[\s\-_]+", "_", token).lower()


def _canonicalize_enum(
    value: object,
    *,
    field_name: str,
    aliases: dict[str, str],
    error_message: str,
) -> str:
    normalized = _normalize_enum_token(value, field_name=field_name)
    if normalized in aliases:
        return aliases[normalized]
    raise ValueError(error_message)


def canonicalize_direction(value: object) -> str:
    return _canonicalize_enum(
        value,
        field_name="direction",
        aliases=_DIRECTION_ALIASES,
        error_message="direction must be one of long, short, hedge, pair",
    )


def canonicalize_strategy(value: object) -> str:
    if value is None:
        raise ValueError("strategy is required")
    if not isinstance(value, str):
        raise ValueError(f"strategy must be a string, got {type(value).__name__}")
    if not value.strip():
        raise ValueError("strategy is required")
    canonical = STRATEGY_REGISTRY.canonicalize(value)
    if canonical is None:
        valid_ids = ", ".join(sorted(STRATEGY_REGISTRY.all_ids()))
        raise ValueError(f"strategy must be one of: {valid_ids}; got {value!r}")
    return canonical


def canonicalize_timeframe(value: object) -> str:
    return _canonicalize_enum(
        value,
        field_name="timeframe",
        aliases=_TIMEFRAME_ALIASES,
        error_message="timeframe must be one of near_term, medium, long_term",
    )


def canonicalize_time_horizon(value: object) -> str:
    return _canonicalize_enum(
        value,
        field_name="time_horizon",
        aliases=_TIME_HORIZON_ALIASES,
        error_message="time_horizon must be one of near-term, medium-term, long-term",
    )


def canonicalize_optional_direction(value: object | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    return canonicalize_direction(token)


def canonicalize_optional_strategy(value: object | None) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise ValueError(f"strategy must be a string, got {type(value).__name__}")
    canonical = STRATEGY_REGISTRY.canonicalize(value)
    if canonical is None:
        valid_ids = ", ".join(sorted(STRATEGY_REGISTRY.all_ids()))
        raise ValueError(f"strategy must be one of: {valid_ids}; got {value!r}")
    return canonical


def strategy_display_name(canonical: str | None) -> str:
    """Render a canonical strategy id as its registry display_name.

    Built-in 'compounder' -> 'Compounder'. User-registered ids return the
    display_name from their YAML. Empty/None input -> "". Unregistered fallback
    returns the canonical string unchanged.
    """
    if not canonical:
        return ""
    entry = STRATEGY_REGISTRY.get(canonical)
    return entry.display_name if entry is not None else canonical


def canonicalize_optional_timeframe(value: object | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    return canonicalize_timeframe(token)


def canonicalize_optional_time_horizon(value: object | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    return canonicalize_time_horizon(token)


__all__ = [
    "canonicalize_direction",
    "canonicalize_optional_direction",
    "canonicalize_optional_strategy",
    "canonicalize_optional_time_horizon",
    "canonicalize_optional_timeframe",
    "canonicalize_strategy",
    "canonicalize_time_horizon",
    "canonicalize_timeframe",
    "strategy_display_name",
]
