from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, TypeGuard

from .dependency_graph import DependencyGraph, formula_owned_recompute_ids
from .handle import ComputedValues
from .models import FinancialModel


def _computed_values(model: FinancialModel) -> ComputedValues:
    graph = DependencyGraph()
    graph.build(model)
    return graph.compute({}, recompute=formula_owned_recompute_ids(model))


def _historical_periods(model: FinancialModel) -> list[int]:
    ts = model.time_structure
    return [int(period) for period in (ts.historical_periods or ts.historical_years)]


def _projection_periods(model: FinancialModel) -> list[int]:
    ts = model.time_structure
    return [int(period) for period in (ts.projection_periods or ts.projection_years)]


def _is_present(value: Any) -> TypeGuard[int | float]:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    return False


def _latest_numeric_value(values: Mapping[int, float | None]) -> float | None:
    for period in sorted(values, reverse=True):
        value = values.get(period)
        if _is_present(value):
            return float(value)
    return None


def _nearly_equal(left: float | None, right: float | None, *, tolerance: float = 1e-9) -> bool:
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) <= tolerance


def _missing_periods(values: Mapping[int, Any], periods: list[int]) -> list[int]:
    return [period for period in periods if not _is_present(values.get(period))]


def _has_any_value(values: Mapping[int, Any], periods: list[int]) -> bool:
    return any(_is_present(values.get(period)) for period in periods)


def _normalize_computed_values(
    values: ComputedValues,
) -> ComputedValues:
    normalized: dict[str, dict[int, float | None]] = {}
    for item_id, item_values in (values or {}).items():
        normalized[str(item_id)] = {
            int(period): value
            for period, value in (item_values or {}).items()
        }
    return normalized


__all__ = [
    "_computed_values",
    "_has_any_value",
    "_historical_periods",
    "_is_present",
    "_latest_numeric_value",
    "_missing_periods",
    "_nearly_equal",
    "_normalize_computed_values",
    "_projection_periods",
]
