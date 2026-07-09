"""Pure in-memory operation dispatch for schema financial-model modifications."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from schema.dependency_graph import DependencyGraph
from schema.models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    SheetType,
)
from schema.modify_layout import (
    _collect_sheet_items,
    _extract_formula_refs,
    _find_item_location,
    _iter_model_items,
    _matches_custom_concept_target,
    _replace_refs_in_item,
)
from schema.renderer import _fixed_cell_anchor_period
from schema.segments import _shift_rows


class OperationType(str, Enum):
    set_value = "set_value"
    set_formula = "set_formula"
    rename_item = "rename_item"
    add_item = "add_item"
    remove_item = "remove_item"
    reorder_items = "reorder_items"


class Operation(BaseModel):
    """One persistent modification to apply to the in-memory model."""

    type: OperationType
    item_id: Optional[str] = None

    # set_value (period mode)
    values: Optional[Dict[int, float]] = None

    # set_value (fixed-cell mode)
    value: Optional[float] = None

    # set_formula
    formula: Optional[Dict[str, Any]] = None
    apply_to: Optional[Literal["projected", "historical", "both"]] = None

    # rename_item
    new_label: Optional[str] = None
    new_id: Optional[str] = None

    # add_item / reorder_items
    section_id: Optional[str] = None
    sheet_name: Optional[str] = None
    label: Optional[str] = None
    after_item_id: Optional[str] = None
    item_type: Optional[Literal["input", "derived"]] = None
    unit: Optional[str] = None
    column: Optional[str] = None
    label_column: Optional[str] = None
    item_ids: Optional[List[str]] = None

    # BM safety opt-outs. PR1 accepts the flags but only applies basic D7 refusals.
    force_set_value_on_derived: bool = False
    force_remove: bool = False
    detach_from_business_model: bool = False

    @model_validator(mode="after")
    def _validate_op_consistency(self) -> "Operation":
        if self.type == OperationType.set_value:
            if self.value is not None and self.values is not None:
                raise ValueError("set_value: provide either `value` or `values`, not both")
            if self.value is None and self.values is None:
                raise ValueError("set_value: requires `value` (fixed-cell) or `values` (period)")
        return self


class OperationResult(BaseModel):
    op_type: OperationType
    item_id: Optional[str]
    status: Literal["ok", "error", "skipped", "warning"]
    reason: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class ModifyError(Exception):
    pass


class LayoutError(Exception):
    pass


def _counts(results: List[OperationResult]) -> tuple[int, int]:
    return (
        sum(1 for result in results if result.status in {"ok", "warning"}),
        sum(1 for result in results if result.status == "error"),
    )


def _dispatch_op(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    if op.type == OperationType.set_value:
        return _apply_set_value(model, op, file_path=file_path)
    if op.type == OperationType.set_formula:
        return _apply_set_formula(model, op, file_path=file_path)
    if op.type == OperationType.rename_item:
        return _apply_rename_item(model, op, file_path=file_path)
    if op.type == OperationType.add_item:
        return _apply_add_item(model, op, file_path=file_path)
    if op.type == OperationType.remove_item:
        return _apply_remove_item(model, op, file_path=file_path)
    if op.type == OperationType.reorder_items:
        return _apply_reorder_items(model, op, file_path=file_path)
    return OperationResult(
        op_type=op.type,
        item_id=op.item_id,
        status="error",
        reason="unsupported_operation",
    )


def _apply_set_value(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    del file_path
    if op.item_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_id_required",
        )

    try:
        item = model.get_item(op.item_id)
    except KeyError:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_not_found",
        )

    is_fixed_cell = item.column is not None
    if is_fixed_cell:
        if op.values is not None:
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="period_mismatch_fixed_cell",
                details={
                    "hint": "Fixed-cell row (column-anchored single value). Pass `value: <float>` only - drop `period`, `periods`, `period_values`, and `values`. Mirror the `valuation-inputs` skill RFR/SOFR/credit_spread write pattern.",
                },
            )
        anchor = _fixed_cell_anchor_period(model, item)
        if anchor is None:
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="no_anchor_period",
            )
        periods_to_write = {int(anchor): float(op.value)}
    else:
        if op.values is None:
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="period_required_period_keyed",
                details={
                    "hint": "Projection-row item (no fixed column). Pass `values: {period: float}` for explicit per-period writes - or `period: <int>` + `value: <float>` (the MCP wrapper auto-promotes for projection rows).",
                },
            )
        periods_to_write = {int(period): float(value) for period, value in op.values.items()}

    details: Dict[str, Any] = {"warnings": []}
    if item.historical is not None and not op.force_set_value_on_derived:
        details["warnings"].append(
            "set_value on a row with historical formula. Override WINS at render "
            "time, but may be removed by formula-first at next model_build if "
            "reconciled. Use force_set_value_on_derived=True to suppress."
        )

    if op.item_id.startswith("bm.") or _matches_custom_concept_target(model, op.item_id):
        details["warnings"].append(
            "Workbook-local edit. Next model_build for this ticker will re-fetch "
            "via override JSON / BM compile and clobber this. Use model_override "
            "tool to persist durably."
        )

    if item.overrides is None:
        item.overrides = {}
    for period, value in periods_to_write.items():
        item.overrides[period] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": value},
        )

    return OperationResult(
        op_type=op.type,
        item_id=op.item_id,
        status="warning" if details["warnings"] else "ok",
        details=details,
    )


def _apply_set_formula(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    del file_path
    if op.item_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_id_required",
        )

    try:
        item = model.get_item(op.item_id)
    except KeyError:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_not_found",
        )

    if op.item_id.startswith("bm."):
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="bm_row_protected",
        )

    if op.formula is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="formula_required",
        )

    try:
        spec = FormulaSpec.model_validate(op.formula)
    except Exception as exc:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="invalid_formula",
            details={"error": str(exc)},
        )

    missing_refs = sorted(
        {
            ref.id
            for ref in _extract_formula_refs(spec)
            if ref.id not in model._index
        }
    )
    if missing_refs:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="invalid_ref",
            details={"missing_refs": missing_refs},
        )

    apply_to = op.apply_to or "projected"
    if apply_to in {"projected", "both"}:
        item.projected = spec
    if apply_to in {"historical", "both"}:
        item.historical = spec

    return OperationResult(op_type=op.type, item_id=op.item_id, status="ok")


def _apply_rename_item(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    del file_path
    if op.item_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_id_required",
        )

    try:
        item = model.get_item(op.item_id)
    except KeyError:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_not_found",
        )

    if op.new_label is None and op.new_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="rename_target_required",
        )

    if op.new_id is not None:
        if op.item_id.startswith("bm."):
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="bm_id_rename_requires_recompile",
            )
        if op.new_id != op.item_id and op.new_id in model._index:
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="item_id_exists",
            )

    old_id = op.item_id
    new_id = op.new_id
    if op.new_label is not None:
        item.label = op.new_label

    if new_id is not None and new_id != old_id:
        item.id = new_id
        for candidate in _iter_model_items(model):
            _replace_refs_in_item(candidate, old_id, new_id)
        model.build_index()

    return OperationResult(op_type=op.type, item_id=new_id or op.item_id, status="ok")


def _apply_add_item(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    if op.item_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_id_required",
        )
    if op.sheet_name is None or op.section_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="location_required",
        )
    if op.item_id in model._index:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_id_exists",
        )

    try:
        sheet = model.sheets[op.sheet_name]
    except KeyError:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="sheet_not_found",
        )

    section = next((candidate for candidate in sheet.sections if candidate.id == op.section_id), None)
    if section is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="section_not_found",
        )

    after_item = None
    if op.after_item_id is not None:
        after_item = next((candidate for candidate in section.line_items if candidate.id == op.after_item_id), None)
        if after_item is None:
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="after_item_not_found",
            )

    if after_item is not None:
        insert_index = section.line_items.index(after_item) + 1
        new_row = int(after_item.row) + 1
    else:
        insert_index = len(section.line_items)
        new_row = max((int(item.row) for item in section.line_items), default=0) + 1

    is_fixed_cell_sheet = sheet.sheet_type in {SheetType.valuation, SheetType.scenarios}
    if is_fixed_cell_sheet:
        if op.column is None:
            return OperationResult(
                op_type=op.type,
                item_id=op.item_id,
                status="error",
                reason="column_required_fixed_cell_sheet",
            )
    else:
        _shift_rows(_collect_sheet_items(model, op.sheet_name), at_or_after=new_row, delta=1)

    item = LineItem(
        id=op.item_id,
        label=op.label or op.item_id,
        row=new_row,
        column=op.column.upper() if op.column is not None else None,
        label_column=op.label_column.upper() if op.label_column is not None else None,
        item_type=ItemType(op.item_type or ItemType.input.value),
        unit=op.unit or "dollars",
    )
    section.line_items.insert(insert_index, item)
    model.build_index()

    child_results: List[OperationResult] = []
    if op.values is not None or op.value is not None:
        child_results.append(
            _apply_set_value(
                model,
                op.model_copy(update={"type": OperationType.set_value}),
                file_path=file_path,
            )
        )
    if op.formula is not None:
        child_results.append(
            _apply_set_formula(
                model,
                op.model_copy(update={"type": OperationType.set_formula}),
                file_path=file_path,
            )
        )

    errors = [result for result in child_results if result.status == "error"]
    if errors:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="add_item_child_op_failed",
            details={"child_results": [result.model_dump(mode="json") for result in child_results]},
        )

    details: Dict[str, Any] = {}
    if child_results:
        details["child_results"] = [result.model_dump(mode="json") for result in child_results]

    return OperationResult(op_type=op.type, item_id=op.item_id, status="ok", details=details)


def _apply_remove_item(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    del file_path
    if op.item_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_id_required",
        )

    try:
        item = model.get_item(op.item_id)
    except KeyError:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_not_found",
        )

    if op.item_id.startswith("bm.") and not op.force_remove:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="bm_row_protected",
        )

    graph = DependencyGraph()
    graph.build(model)
    dependents = set(graph.adj.get(op.item_id, set()))
    dependents.update(
        dependent_id
        for dependent_id, refs in graph.time_edges.items()
        if any(ref.id == op.item_id for ref in refs)
    )
    dependents.discard(op.item_id)
    if dependents:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="has_dependents",
            details={"dependents": sorted(dependents)},
        )

    location = _find_item_location(model, op.item_id)
    if location is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_not_found",
        )

    sheet_name, sheet, section, index = location
    removed_row = int(item.row)
    section.line_items.pop(index)
    if sheet.sheet_type not in {SheetType.valuation, SheetType.scenarios}:
        _shift_rows(_collect_sheet_items(model, sheet_name), at_or_after=removed_row, delta=-1)
    model.build_index()

    return OperationResult(op_type=op.type, item_id=op.item_id, status="ok")


def _apply_reorder_items(
    model: FinancialModel,
    op: Operation,
    *,
    file_path: str,
) -> OperationResult:
    del file_path
    if op.sheet_name is None or op.section_id is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="location_required",
        )
    if op.item_ids is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_ids_required",
        )

    try:
        sheet = model.sheets[op.sheet_name]
    except KeyError:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="sheet_not_found",
        )

    section = next((candidate for candidate in sheet.sections if candidate.id == op.section_id), None)
    if section is None:
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="section_not_found",
        )

    current_ids = [item.id for item in section.line_items]
    if sorted(current_ids) != sorted(op.item_ids) or len(current_ids) != len(op.item_ids):
        return OperationResult(
            op_type=op.type,
            item_id=op.item_id,
            status="error",
            reason="item_ids_not_permutation",
        )

    by_id = {item.id: item for item in section.line_items}
    first_row = min((int(item.row) for item in section.line_items), default=1)
    section.line_items = [by_id[item_id] for item_id in op.item_ids]
    for offset, item in enumerate(section.line_items):
        item.row = first_row + offset
    model.build_index()

    return OperationResult(op_type=op.type, item_id=op.item_id, status="ok")
