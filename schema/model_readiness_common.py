from __future__ import annotations

import math
import sys
from typing import Any

from .dependency_graph import DependencyGraph
from .models import FinancialModel, ItemType


def _compat(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.model_readiness")
    if parent is None:
        parent = sys.modules.get("model_readiness")
    return getattr(parent, name, default) if parent is not None else default


def _computed_values(model: FinancialModel) -> dict[str, dict[int, float]]:
    graph = DependencyGraph()
    graph.build(model)
    derived_ids = {
        item.id
        for item in model._index.values()
        if item.item_type == ItemType.derived
    }
    return graph.compute({}, recompute=derived_ids)


def _historical_periods(model: FinancialModel) -> list[int]:
    ts = model.time_structure
    return [int(period) for period in (ts.historical_periods or ts.historical_years)]


def _projection_periods(model: FinancialModel) -> list[int]:
    ts = model.time_structure
    return [int(period) for period in (ts.projection_periods or ts.projection_years)]


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    return False


def _latest_numeric_value(values: dict[int, Any]) -> float | None:
    is_present = _compat("_is_present", _is_present)
    for period in sorted(values, reverse=True):
        value = values.get(period)
        if is_present(value):
            return float(value)
    return None


def _nearly_equal(left: float | None, right: float | None, *, tolerance: float = 1e-9) -> bool:
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) <= tolerance


def _missing_periods(values: dict[int, Any], periods: list[int]) -> list[int]:
    is_present = _compat("_is_present", _is_present)
    return [period for period in periods if not is_present(values.get(period))]


def _has_any_value(values: dict[int, Any], periods: list[int]) -> bool:
    is_present = _compat("_is_present", _is_present)
    return any(is_present(values.get(period)) for period in periods)


def _normalize_computed_values(values: dict[str, dict[int, float]]) -> dict[str, dict[int, float]]:
    normalized: dict[str, dict[int, float]] = {}
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
