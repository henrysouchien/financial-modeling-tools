"""Layout and formula-reference helpers for schema model modifications."""

from __future__ import annotations

import sys
from typing import Any, Dict, List, Optional

from schema.models import FinancialModel, FormulaSpec, LineItem, LineItemRef, SheetType
from schema.refs import line_item_ref_from_obj


def _parent_attr(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.modify")
    if parent is None:
        return default
    return getattr(parent, name, default)


def _assert_layout_integrity(model: FinancialModel) -> None:
    """Validate row/coordinate integrity after mutation."""

    sheet_type_cls = _parent_attr("SheetType", SheetType)
    layout_error_cls = _parent_attr("LayoutError", RuntimeError)
    for sheet_name, sheet in model.sheets.items():
        items = [item for section in sheet.sections for item in section.line_items]
        is_fixed_cell_sheet = sheet.sheet_type in {
            sheet_type_cls.valuation,
            sheet_type_cls.scenarios,
        }

        if is_fixed_cell_sheet:
            occupied: Dict[tuple[int, str], str] = {}
            for item in items:
                if item.column is None:
                    continue
                key = (int(item.row), item.column.upper())
                if key in occupied:
                    raise layout_error_cls(
                        f"coordinate collision in {sheet_name}: "
                        f"({item.row}, {item.column}) used by {occupied[key]} and {item.id}"
                    )
                occupied[key] = item.id
            continue

        seen_rows: Dict[int, str] = {}
        for item in items:
            row = int(item.row)
            if row in seen_rows:
                raise layout_error_cls(
                    f"row collision in {sheet_name}: row {row} used by "
                    f"{seen_rows[row]} and {item.id}"
                )
            seen_rows[row] = item.id


def _collect_sheet_items(model: FinancialModel, sheet_name: str) -> List[LineItem]:
    sheet = model.sheets[sheet_name]
    return [item for section in sheet.sections for item in section.line_items]


def _matches_custom_concept_target(model: FinancialModel, item_id: str) -> bool:
    """Return True if item_id matches a custom_concept target for this model's ticker.

    False on any ambiguity, including a missing ticker, missing override file, or
    parse/load error.
    """

    from schema.overrides import load_ticker_overrides

    ticker = getattr(model.company, "ticker", None) if model.company else None
    if not ticker:
        return False

    try:
        overrides = load_ticker_overrides(ticker)
    except Exception:
        return False
    if overrides is None:
        return False

    try:
        for _concept_id, entry in (overrides.custom_concepts or {}).items():
            if entry.get("target_item_id") == item_id:
                return True
    except Exception:
        return False
    return False


def _extract_formula_refs(obj: Any) -> List[LineItemRef]:
    line_item_ref_cls = _parent_attr("LineItemRef", LineItemRef)
    formula_spec_cls = _parent_attr("FormulaSpec", FormulaSpec)
    line_item_ref_from_obj_fn = _parent_attr("line_item_ref_from_obj", line_item_ref_from_obj)
    if obj is None:
        return []
    if isinstance(obj, line_item_ref_cls):
        return [obj]
    if isinstance(obj, formula_spec_cls):
        return _extract_formula_refs(obj.params)

    coerced = line_item_ref_from_obj_fn(obj)
    if coerced is not None:
        return [coerced]

    refs: List[LineItemRef] = []
    if isinstance(obj, dict):
        for value in obj.values():
            refs.extend(_extract_formula_refs(value))
    elif isinstance(obj, (list, tuple, set)):
        for value in obj:
            refs.extend(_extract_formula_refs(value))
    return refs


def _iter_model_items(model: FinancialModel) -> List[LineItem]:
    return [
        item
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    ]


def _iter_item_formula_specs(item: LineItem) -> List[FormulaSpec]:
    specs: List[FormulaSpec] = []
    if item.historical is not None:
        specs.append(item.historical)
    if item.projected is not None:
        specs.append(item.projected)
    if item.overrides:
        specs.extend(item.overrides.values())
    return specs


def _replace_refs_in_item(item: LineItem, old_id: str, new_id: str) -> None:
    iter_item_formula_specs = _parent_attr("_iter_item_formula_specs", _iter_item_formula_specs)
    replace_refs = _parent_attr("_replace_refs", _replace_refs)
    for spec in iter_item_formula_specs(item):
        spec.params = replace_refs(spec.params, old_id, new_id)


def _replace_refs(obj: Any, old_id: str, new_id: str) -> Any:
    line_item_ref_cls = _parent_attr("LineItemRef", LineItemRef)
    line_item_ref_from_obj_fn = _parent_attr("line_item_ref_from_obj", line_item_ref_from_obj)
    if isinstance(obj, line_item_ref_cls):
        if obj.id == old_id:
            return line_item_ref_cls(
                id=new_id,
                t=obj.t,
                resolved=obj.resolved,
                period_anchor=obj.period_anchor,
            )
        return obj

    coerced = line_item_ref_from_obj_fn(obj)
    if coerced is not None and isinstance(obj, dict):
        if coerced.id != old_id:
            return obj
        replacement = coerced.model_dump()
        replacement["id"] = new_id
        return replacement

    if isinstance(obj, dict):
        return {key: _replace_refs(value, old_id, new_id) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_replace_refs(value, old_id, new_id) for value in obj]
    if isinstance(obj, tuple):
        return tuple(_replace_refs(value, old_id, new_id) for value in obj)
    if isinstance(obj, set):
        return {_replace_refs(value, old_id, new_id) for value in obj}
    return obj


def _find_item_location(
    model: FinancialModel,
    item_id: str,
) -> Optional[tuple[str, Any, Any, int]]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item in enumerate(section.line_items):
                if item.id == item_id:
                    return sheet_name, sheet, section, index
    return None


__all__ = [
    "_assert_layout_integrity",
    "_collect_sheet_items",
    "_extract_formula_refs",
    "_find_item_location",
    "_iter_item_formula_specs",
    "_iter_model_items",
    "_matches_custom_concept_target",
    "_replace_refs",
    "_replace_refs_in_item",
]
