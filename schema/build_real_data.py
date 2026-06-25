"""Real-data predicate helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import Optional

from .build_formula_eval import (
    _MAX_REF_CHAIN_DEPTH as _FORMULA_MAX_REF_CHAIN_DEPTH,
    _constant_override_value as _formula_constant_override_value,
    _shift_period,
)
from .build_formula_refs import (
    _available_periods as _formula_available_periods,
    _extract_single_ref as _formula_extract_single_ref,
)
from .models import FinancialModel, FormulaType, LineItem


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _item_has_real_data(
    item: LineItem,
    period: int,
    *,
    model: Optional[FinancialModel] = None,
    _depth: int = 0,
    _seen: Optional[set[tuple[str, int]]] = None,
) -> bool:
    """Return True when an item has real data for a specific period."""

    if model is not None:
        available_periods = _parent_attr("_available_periods", _formula_available_periods)(model)
        if available_periods is not None and int(period) not in available_periods:
            return False

    if item.values is not None and int(period) in item.values.values:
        cell = item.values.values[int(period)]
        if cell.value is not None:
            return True
    if item.overrides is not None and int(period) in item.overrides:
        spec = item.overrides[int(period)]
        if spec.note != "synthetic":
            return True

    if (
        model is not None
        and item.historical is not None
        and item.historical.type is FormulaType.ref
    ):
        if _depth >= _parent_attr("_MAX_REF_CHAIN_DEPTH", _FORMULA_MAX_REF_CHAIN_DEPTH):
            return False
        key = (str(item.id), int(period))
        if key in (_seen or set()):
            return False
        ref_target = _parent_attr("_extract_single_ref", _formula_extract_single_ref)(
            item.historical.params
        )
        if ref_target is None:
            return False
        target_id, target_t = ref_target
        shifted = _shift_period(int(period), int(target_t), model.time_structure.period_mode)
        if shifted is None:
            return False
        try:
            target_item = model.get_item(target_id)
        except KeyError:
            return False
        seen = (_seen or set()) | {key}
        return _item_has_real_data(
            target_item,
            int(shifted),
            model=model,
            _depth=_depth + 1,
            _seen=seen,
        )

    if (
        model is not None
        and item.historical is not None
        and item.historical.type is FormulaType.constant
    ):
        constant_override_value = _parent_attr(
            "_constant_override_value",
            _formula_constant_override_value,
        )
        if constant_override_value(item.historical) is not None:
            return True

    return False


def _item_has_direct_real_data(item: LineItem, period: int) -> bool:
    if item.values is not None and int(period) in item.values.values:
        cell = item.values.values[int(period)]
        if cell.value is not None:
            return True
    if item.overrides is not None and int(period) in item.overrides:
        spec = item.overrides[int(period)]
        return spec.note != "synthetic"
    return False


__all__ = [
    "_item_has_direct_real_data",
    "_item_has_real_data",
]
