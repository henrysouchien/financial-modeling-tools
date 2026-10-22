"""Period-selection helpers for schema model tools."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

from .models import PERIOD_MODE_QUARTERLY5, FinancialModel


def _model_tool_error(
    code: str,
    message: str,
    *,
    details: Dict[str, Any] | None = None,
    recovery: Dict[str, Any] | None = None,
) -> Exception:
    from .tools import ModelToolError

    return ModelToolError(code, message, details=details, recovery=recovery)


def _historical_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.historical_periods) or list(ts.historical_years)


def _projection_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.projection_periods) or list(ts.projection_years)


def _all_periods(model: FinancialModel) -> List[int]:
    return _historical_periods(model) + _projection_periods(model)


def _period_guidance(model: FinancialModel) -> Dict[str, Any]:
    historical = _historical_periods(model)
    projection = _projection_periods(model)
    all_periods = historical + projection
    mode = model.time_structure.period_mode
    years = sorted({_period_year(period, mode) for period in all_periods})
    if years:
        range_example = f"{years[0]}:{years[-1]}"
    else:
        range_example = "2024:2028"
    return {
        "period_mode": mode,
        "valid_specs": ["all", "historical", "projection", "YYYY:YYYY"],
        "examples": ["all", "historical", "projection", range_example],
        "available_periods": {
            "historical": historical,
            "projection": projection,
            "all": all_periods,
        },
        "available_years": years,
        "override_period_keys": (
            "Use fiscal year integers/strings such as 2026 or '2026'. "
            "For quarterly models, year ranges select all fiscal quarter period keys in that year."
        ),
    }


def _period_year(period: int, mode: str) -> int:
    """Extract the year component from a period key."""
    if mode == PERIOD_MODE_QUARTERLY5:
        return period // 10
    return period


def _period_token_to_matches(token: str, model: FinancialModel) -> List[int]:
    raw = token.strip()
    normalized = re.sub(r"(?i)^FY", "", raw)
    if not re.fullmatch(r"\d{4,5}", normalized):
        return []

    value = int(normalized)
    all_periods = _all_periods(model)
    if value in all_periods:
        return [value]

    if len(normalized) == 4:
        mode = model.time_structure.period_mode
        return [period for period in all_periods if _period_year(period, mode) == value]

    return []


def _resolve_period_token_list(periods: object, model: FinancialModel) -> Tuple[List[int], str] | None:
    if isinstance(periods, str):
        if "," not in periods:
            return None
        tokens = [token.strip() for token in periods.split(",") if token.strip()]
    elif isinstance(periods, (list, tuple, set)):
        tokens = [str(token).strip() for token in periods if str(token).strip()]
    else:
        return None

    if not tokens:
        return None

    selected: List[int] = []
    seen: Set[int] = set()
    invalid_tokens: List[str] = []
    for token in tokens:
        matches = _period_token_to_matches(token, model)
        if not matches:
            invalid_tokens.append(token)
            continue
        for period in matches:
            if period in seen:
                continue
            selected.append(period)
            seen.add(period)

    if invalid_tokens or not selected:
        raise _model_tool_error(
            "invalid_periods",
            "periods list contains values that do not match available model periods",
            details={
                "received": periods,
                "invalid_tokens": invalid_tokens,
                "period_guidance": _period_guidance(model),
            },
            recovery={
                "next_actions": [
                    "Use period keys from period_guidance.available_periods.",
                    "For yearly models, comma lists such as '2024,2025,2026' are accepted.",
                    "For ranges, prefer a compact range like periods='2024:2026'.",
                ]
            },
        )

    return selected, ",".join(tokens)


def _resolve_period_list(periods: str | List[str | int], model: FinancialModel) -> Tuple[List[int], str]:
    """Return (period_list, label) for the given periods spec."""
    token_list_result = _resolve_period_token_list(periods, model)
    if token_list_result is not None:
        return token_list_result

    if not isinstance(periods, str) or not periods.strip():
        raise _model_tool_error(
            "invalid_periods",
            "periods must be a non-empty string: all, projection, historical, or a year range like '2023:2027'",
            details={"received": periods, "period_guidance": _period_guidance(model)},
            recovery={
                "next_actions": [
                    "Call model_summarize(file_path=...) to inspect time_range.",
                    "Retry model_values with periods='projection', periods='historical', periods='all', or periods='YYYY:YYYY'.",
                ]
            },
        )
    periods = periods.strip()
    if periods == "projection":
        return _projection_periods(model), "projection"
    if periods == "historical":
        return _historical_periods(model), "historical"
    if periods == "all":
        return _all_periods(model), "all"

    single_period = _period_token_to_matches(periods, model)
    if single_period:
        return single_period, periods

    match = re.fullmatch(r"(?i)(?:FY)?(\d{4}):(?:FY)?(\d{4})", periods)
    if match:
        start_year = int(match.group(1))
        end_year = int(match.group(2))
        if start_year > end_year:
            raise _model_tool_error(
                "invalid_period_range",
                f"Invalid year range: start ({start_year}) > end ({end_year})",
                details={"received": periods, "period_guidance": _period_guidance(model)},
                recovery={"next_actions": ["Reverse the range order, e.g. '2023:2027'."]},
            )
        all_periods = _all_periods(model)
        mode = model.time_structure.period_mode
        filtered = [period for period in all_periods if start_year <= _period_year(period, mode) <= end_year]
        return filtered, periods

    raise _model_tool_error(
        "invalid_periods",
        "periods must be one of: all, projection, historical, or a year range like '2023:2027'",
        details={"received": periods, "period_guidance": _period_guidance(model)},
        recovery={
            "next_actions": [
                "Call model_summarize(file_path=...) to inspect time_range.",
                "Retry with periods='projection', periods='historical', periods='all', or a concrete range such as '2026:2028'.",
            ]
        },
    )


def _annual_projection_periods(model: FinancialModel) -> List[int]:
    """Return one period per projection year."""
    projection_periods = _projection_periods(model)
    mode = model.time_structure.period_mode
    if mode == PERIOD_MODE_QUARTERLY5:
        return [period for period in projection_periods if period % 10 == 5]
    return projection_periods


def _annual_historical_periods(model: FinancialModel, n: int = 3) -> List[int]:
    """Return the last n annual historical periods."""
    historical_periods = _historical_periods(model)
    mode = model.time_structure.period_mode
    if mode == PERIOD_MODE_QUARTERLY5:
        annual_periods = [period for period in historical_periods if period % 10 == 5]
    else:
        annual_periods = historical_periods
    return annual_periods[-max(n, 0) :]


__all__ = [
    "_all_periods",
    "_annual_historical_periods",
    "_annual_projection_periods",
    "_historical_periods",
    "_period_guidance",
    "_period_token_to_matches",
    "_period_year",
    "_projection_periods",
    "_resolve_period_list",
    "_resolve_period_token_list",
]
