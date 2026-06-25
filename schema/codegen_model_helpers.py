"""Model traversal helpers for :mod:`schema.codegen`."""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, Iterable, List, Tuple

from .models import FinancialModel, ItemType, LineItem, PERIOD_MODE_YEARLY, ValueProvenance


_INPUT_PROVENANCE = {
    ValueProvenance.input,
    ValueProvenance.imported_other,
    ValueProvenance.imported_edgar,
    ValueProvenance.imported_fmp,
}


def _parent_attr(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.codegen")
    if parent is None:
        parent = sys.modules.get("codegen")
    return getattr(parent, name, default) if parent is not None else default


def _time_order(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    if ts.historical_periods or ts.projection_periods:
        return list(ts.historical_periods) + list(ts.projection_periods)
    return list(ts.historical_years) + list(ts.projection_years)


def _historical_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.historical_periods) or list(ts.historical_years)


def _projection_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.projection_periods) or list(ts.projection_years)


def _iter_items(model: FinancialModel) -> Iterable[Tuple[str, str, LineItem]]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, section.label, item


def _build_cached_dicts(
    model: FinancialModel,
) -> Tuple[Dict[str, Dict[int, float]], Dict[str, Dict[int, float]], Dict[str, Dict[int, float]]]:
    input_cached: Dict[str, Dict[int, float]] = {}
    all_cached: Dict[str, Dict[int, float]] = {}
    cached_computed: Dict[str, Dict[int, float]] = {}
    period_mode = model.time_structure.period_mode or PERIOD_MODE_YEARLY
    iter_items = _parent_attr("_iter_items", _iter_items)
    input_provenance = _parent_attr("_INPUT_PROVENANCE", _INPUT_PROVENANCE)

    for _sheet, _section, item in iter_items(model):
        if not item.values:
            continue
        for period, value_cell in item.values.values.items():
            if value_cell.value is None:
                continue
            all_cached.setdefault(item.id, {})[int(period)] = float(value_cell.value)
            if value_cell.provenance in input_provenance:
                input_cached.setdefault(item.id, {})[int(period)] = float(value_cell.value)
            if value_cell.provenance == ValueProvenance.computed:
                cached_computed.setdefault(item.id, {})[int(period)] = float(value_cell.value)
            elif period_mode != PERIOD_MODE_YEARLY and value_cell.provenance in {
                ValueProvenance.imported_other,
                ValueProvenance.imported_edgar,
                ValueProvenance.imported_fmp,
            }:
                cached_computed.setdefault(item.id, {})[int(period)] = float(value_cell.value)

    return input_cached, all_cached, cached_computed


def _build_function_names(model: FinancialModel) -> Dict[str, str]:
    used: Dict[str, int] = {}
    names: Dict[str, str] = {}
    iter_items = _parent_attr("_iter_items", _iter_items)

    for _sheet, _section, item in iter_items(model):
        if item.item_type != ItemType.derived:
            continue
        base = re.sub(r"[^A-Za-z0-9_]", "_", item.id).strip("_")
        if not base:
            base = "item"
        if base[0].isdigit():
            base = f"item_{base}"
        base = base.lower()
        count = used.get(base, 0) + 1
        used[base] = count
        if count == 1:
            names[item.id] = f"_compute_{base}"
        else:
            names[item.id] = f"_compute_{base}_{count}"
    return names


def _item_locations(model: FinancialModel) -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    iter_items = _parent_attr("_iter_items", _iter_items)
    for sheet_name, section_label, item in iter_items(model):
        out[item.id] = (sheet_name, section_label)
    return out


def _value_dict_from_item(item: LineItem) -> Dict[int, float]:
    values: Dict[int, float] = {}
    if not item.values:
        return values
    for period, value_cell in item.values.values.items():
        if value_cell.value is None:
            continue
        values[int(period)] = float(value_cell.value)
    return values


__all__ = [
    "_INPUT_PROVENANCE",
    "_build_cached_dicts",
    "_build_function_names",
    "_historical_periods",
    "_item_locations",
    "_iter_items",
    "_projection_periods",
    "_time_order",
    "_value_dict_from_item",
]
