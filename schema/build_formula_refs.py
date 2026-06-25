"""Formula reference and period helpers for schema build orchestration."""

from __future__ import annotations

from typing import List, Optional

from .models import FinancialModel, FormulaSpec, LineItem, LineItemRef, shift_period


def _safe_int(value, default: Optional[int] = 0) -> Optional[int]:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _formula_period_is_valid(
    model: FinancialModel,
    item: LineItem,
    period: int,
    *,
    historical_set: set[int],
    projection_set: set[int],
    available_periods: set[int],
) -> bool:
    spec = _spec_for_period_validation(item, period, historical_set, projection_set)
    if spec is None:
        return True

    mode = model.time_structure.period_mode
    for offset in _collect_ref_offsets(spec.params):
        shifted = shift_period(int(period), int(offset), mode)
        if shifted is None or int(shifted) not in available_periods:
            return False
    return True


def _spec_for_period_validation(
    item: LineItem,
    period: int,
    historical_set: set[int],
    projection_set: set[int],
) -> Optional[FormulaSpec]:
    if item.overrides and int(period) in item.overrides:
        return item.overrides[int(period)]
    if int(period) in historical_set:
        return item.historical
    if int(period) in projection_set:
        return item.projected
    return item.projected or item.historical


def _collect_ref_offsets(obj) -> List[int]:
    offsets: List[int] = []
    if obj is None:
        return offsets
    if isinstance(obj, LineItemRef):
        return [int(obj.t)]
    if isinstance(obj, list):
        for value in obj:
            offsets.extend(_collect_ref_offsets(value))
        return offsets
    if isinstance(obj, tuple):
        for value in obj:
            offsets.extend(_collect_ref_offsets(value))
        return offsets
    if isinstance(obj, dict):
        if "id" in obj:
            try:
                return [int(obj.get("t", 0))]
            except (TypeError, ValueError):
                return []
        for value in obj.values():
            offsets.extend(_collect_ref_offsets(value))
    return offsets


def _available_periods(model: FinancialModel) -> Optional[set[int]]:
    """Return the model time-axis periods, or None when no axis is defined."""

    ts = model.time_structure
    historical = (
        getattr(ts, "historical_periods", None)
        or getattr(ts, "historical_years", None)
        or []
    )
    projection = (
        getattr(ts, "projection_periods", None)
        or getattr(ts, "projection_years", None)
        or []
    )
    if not historical and not projection:
        return None
    return {int(p) for p in historical} | {int(p) for p in projection}


def _all_refs_same_period(params) -> bool:
    offsets = _collect_ref_offsets(params)
    return bool(offsets) and all(offset == 0 for offset in offsets)


def _extract_single_ref(params) -> Optional[tuple[str, int]]:
    """Pull the single source ref from a ref-type formula's params."""

    if not isinstance(params, dict):
        return None
    source = params.get("source")
    if isinstance(source, LineItemRef):
        return (source.id, int(source.t))
    if isinstance(source, dict) and isinstance(source.get("id"), str):
        try:
            t_val = int(source.get("t", 0))
        except (TypeError, ValueError):
            t_val = 0
        return (source["id"], t_val)
    return None


def _extract_ref_ids(obj) -> set[str]:
    """Extract all line-item IDs referenced in formula params."""

    ids: set[str] = set()
    if obj is None:
        return ids
    if isinstance(obj, LineItemRef):
        return {obj.id}
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], str):
            return {obj["id"]}
        for value in obj.values():
            ids |= _extract_ref_ids(value)
        return ids
    if isinstance(obj, (list, tuple)):
        for value in obj:
            ids |= _extract_ref_ids(value)
    return ids


__all__ = [
    "_all_refs_same_period",
    "_available_periods",
    "_collect_ref_offsets",
    "_extract_ref_ids",
    "_extract_single_ref",
    "_formula_period_is_valid",
    "_safe_int",
    "_spec_for_period_validation",
]
