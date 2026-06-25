"""Model mutation helpers for segment expansion."""

from __future__ import annotations

from typing import Dict, Iterable

from .models import (
    FinancialModel,
    FormulaSpec,
    LineItem,
    Section,
    SheetType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .segment_formula_helpers import (
    _carry_forward_ref,
    _line_ref_dict,
    _ref_target_id,
    _rewrite_refs,
)


def _shift_rows(items: Iterable[LineItem], at_or_after: int, delta: int) -> None:
    if not delta:
        return
    for item in items:
        if int(item.row) >= int(at_or_after):
            item.row = int(item.row) + int(delta)


def _assert_rows_unique(model: FinancialModel) -> None:
    for sheet_name, sheet in model.sheets.items():
        if sheet.sheet_type in {SheetType.valuation, SheetType.scenarios}:
            continue
        rows = [int(item.row) for section in sheet.sections for item in section.line_items]
        duplicates = {row for row in rows if rows.count(row) > 1}
        if duplicates:
            raise ValueError(f"Duplicate rows detected in sheet '{sheet_name}': {sorted(duplicates)}")


def _assert_no_duplicate_ids(model: FinancialModel) -> None:
    model.build_index()


def _repair_deleted_refs_in_item(item: LineItem, deleted_ids: set[str]) -> bool:
    changed = False

    for attr in ("historical", "projected"):
        spec = getattr(item, attr)
        if spec is None:
            continue
        new_params, updated = _rewrite_refs(
            spec.params,
            lambda ref: _carry_forward_ref(item.id) if ref.get("id") in deleted_ids else None,
        )
        if updated:
            spec.params = new_params
            changed = True

    if item.overrides:
        for period, spec in item.overrides.items():
            new_params, updated = _rewrite_refs(
                spec.params,
                lambda ref: _carry_forward_ref(item.id) if ref.get("id") in deleted_ids else None,
            )
            if updated:
                item.overrides[period] = spec.model_copy(update={"params": new_params})
                changed = True

    return changed


def _rewire_scenario_table_refs(model: FinancialModel, old_id: str, new_id: str) -> None:
    scenario_section = _get_section(model, "Assumptions", "scenario_tables")
    for item in scenario_section.line_items:
        for attr in ("historical", "projected"):
            spec = getattr(item, attr)
            if spec is None:
                continue
            new_params, updated = _rewrite_refs(
                spec.params,
                lambda ref: _line_ref_dict(new_id, int(ref.get("t", 0))) if ref.get("id") == old_id else None,
            )
            if updated:
                spec.params = new_params

        if item.overrides:
            kept_overrides: Dict[int, FormulaSpec] = {}
            for period, spec in item.overrides.items():
                new_params, updated = _rewrite_refs(
                    spec.params,
                    lambda ref: _line_ref_dict(new_id, int(ref.get("t", 0))) if ref.get("id") == old_id else None,
                )
                new_spec = spec.model_copy(update={"params": new_params}) if updated else spec
                if _ref_target_id(new_spec) == new_id:
                    continue
                kept_overrides[int(period)] = new_spec
            item.overrides = kept_overrides or None


def _get_section(model: FinancialModel, sheet_name: str, section_id: str) -> Section:
    sheet = model.sheets[sheet_name]
    for section in sheet.sections:
        if section.id == section_id:
            return section
    raise KeyError((sheet_name, section_id))


def _iter_sheet_items(model: FinancialModel, sheet_name: str) -> Iterable[LineItem]:
    for section in model.sheets[sheet_name].sections:
        for item in section.line_items:
            yield item


def _iter_items_with_section(model: FinancialModel):
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, section.id, item


def _set_imported_value(
    item: LineItem,
    year: int,
    value: float,
    provenance: ValueProvenance = ValueProvenance.imported_edgar,
) -> None:
    if item.values is None:
        item.values = ValueSeries()
    item.values.values[int(year)] = ValueCell(
        period=int(year),
        value=float(value),
        provenance=provenance,
    )


__all__ = [
    "_assert_no_duplicate_ids",
    "_assert_rows_unique",
    "_get_section",
    "_iter_items_with_section",
    "_iter_sheet_items",
    "_repair_deleted_refs_in_item",
    "_rewire_scenario_table_refs",
    "_set_imported_value",
    "_shift_rows",
]
