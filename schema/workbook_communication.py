from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from pydantic import BaseModel, ConfigDict, Field

from .workbook_communication_specs import (
    _CellSpec,
    _DirectCellSpec,
    _SCENARIO_CASES,
    _SCENARIO_DIRECT_CELL_SPECS,
    _VALUATION_DIRECT_CELL_SPECS,
    _VALUATION_SUMMARY_SPECS,
    _VISUAL_CLEANUP_SPECS,
    _VisualCleanupSpec as _VisualCleanupSpec,
)
from .workbook_communication_utils import (
    _artifact_ref,
    _first_mapping,
    _first_number,
    _get_path,
    _is_blankish,
    _is_formula,
    _normalize_label,
    _number_from_source,
    _number_or_none,
    _string_or_none,
    _values_equivalent,
)


WorkbookCommunicationStatus = Literal["ready", "actionable", "blocked"]
WorkbookCommunicationMaterializationStatus = Literal["materialized", "blocked", "noop"]
WorkbookCommunicationFieldStatus = Literal[
    "populated",
    "materializable",
    "formula_only",
    "missing_source",
    "missing_sheet",
    "missing_cell",
]


class WorkbookCommunicationSourceValue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    value: Any
    source_kind: str
    source_ref: str | None = None


class WorkbookCommunicationCandidateWrite(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    field: str
    label: str
    sheet: str
    cell: str
    value: Any
    source_kind: str
    source_ref: str | None = None
    current_formula: str | None = None
    current_display_value: Any = None
    reason: str


class WorkbookCommunicationFieldReport(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    field: str
    label: str
    sheet: str
    cell: str | None = None
    required: bool = True
    status: WorkbookCommunicationFieldStatus
    current_formula: str | None = None
    current_display_value: Any = None
    source_value: Any = None
    source_kind: str | None = None
    source_ref: str | None = None
    issue: str | None = None
    candidate_write: WorkbookCommunicationCandidateWrite | None = None


class WorkbookCommunicationReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    workbook_path: str
    workbook_exists: bool = True
    status: WorkbookCommunicationStatus
    fields: list[WorkbookCommunicationFieldReport] = Field(default_factory=list)
    candidate_writes: list[WorkbookCommunicationCandidateWrite] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)


class WorkbookCommunicationMaterializationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    workbook_path: str
    output_path: str | None = None
    status: WorkbookCommunicationMaterializationStatus
    pre_readiness: WorkbookCommunicationReadiness
    post_readiness: WorkbookCommunicationReadiness | None = None
    writes_applied: list[WorkbookCommunicationCandidateWrite] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)


def plan_workbook_communication_readiness(
    workbook_path: str | Path,
    source_values: Mapping[str, Any] | None = None,
) -> WorkbookCommunicationReadiness:
    """Plan code-owned workbook communication writes without mutating the workbook.

    The planner treats formulas without cached/static-display values as not ready
    for communication surfaces. If a typed artifact supplies the same field, the
    report emits a candidate write that a later finalization layer can apply.
    """

    path = Path(workbook_path)
    sources = _normalize_source_values(source_values or {})
    if not path.exists():
        return WorkbookCommunicationReadiness(
            workbook_path=str(path),
            workbook_exists=False,
            status="blocked",
            issues=["workbook_not_found"],
            summary={"blocked": 1},
        )

    formulas = load_workbook(path, data_only=False)
    cached = load_workbook(path, data_only=True)

    fields: list[WorkbookCommunicationFieldReport] = []
    if "Summary" not in formulas.sheetnames:
        fields.append(
            WorkbookCommunicationFieldReport(
                field="summary",
                label="Summary",
                sheet="Summary",
                status="missing_sheet",
                issue="Summary sheet is missing.",
            )
        )
    else:
        fields.extend(_summary_valuation_fields(formulas["Summary"], cached["Summary"], sources))
        fields.extend(_summary_scenario_target_fields(formulas["Summary"], cached["Summary"], sources))
    fields.extend(_direct_cell_fields(formulas, cached, sources, _VALUATION_DIRECT_CELL_SPECS))
    fields.extend(_direct_cell_fields(formulas, cached, sources, _SCENARIO_DIRECT_CELL_SPECS))

    candidate_writes = [field.candidate_write for field in fields if field.candidate_write is not None]
    required_blocked = _has_required_blockers(fields)
    cleanup_writes = [] if required_blocked else _visual_cleanup_writes(
        formulas,
        cached,
        protected_cells={(field.sheet, field.cell) for field in fields if field.cell},
    )
    candidate_writes.extend(cleanup_writes)
    summary = _summarize_fields(fields)
    if cleanup_writes:
        summary["visual_cleanup"] = len(cleanup_writes)
    issues = _issues_from_fields(fields)
    status = _readiness_status(fields, candidate_writes)
    return WorkbookCommunicationReadiness(
        workbook_path=str(path),
        status=status,
        fields=fields,
        candidate_writes=candidate_writes,
        issues=issues,
        summary=summary,
    )


def source_values_from_artifacts(
    *artifacts: str | Path | Mapping[str, Any],
) -> dict[str, WorkbookCommunicationSourceValue]:
    """Extract communication-ready values from FMS artifact payloads.

    The returned keys are stable planner source fields, such as
    ``summary.current_price`` and ``summary.scenario.base.target_price``.
    """

    values: dict[str, WorkbookCommunicationSourceValue] = {}
    for artifact in artifacts:
        payload, source_ref = _load_artifact_payload(artifact)
        _merge_current_model_values(values, payload, source_ref)
        _merge_price_target_values(values, payload, source_ref)
        _merge_valuation_method_values(values, payload, source_ref)
        _merge_scenario_values(values, payload, source_ref)
        _merge_expected_value_values(values, payload, source_ref)
    _derive_expected_return(values)
    return values


def materialize_workbook_communication(
    workbook_path: str | Path,
    source_values: Mapping[str, Any] | None = None,
    *,
    output_path: str | Path | None = None,
    allow_in_place: bool = False,
) -> WorkbookCommunicationMaterializationResult:
    """Write a communication-ready workbook copy from trusted candidate writes.

    By default this writes ``<stem>.communication.xlsx`` next to the source
    workbook. It replaces only contracted communication cells that the planner
    marked as source-backed ``materializable`` and records provenance in a
    hidden ``_FMS_Communication`` ledger sheet.
    """

    path = Path(workbook_path)
    pre_readiness = plan_workbook_communication_readiness(path, source_values)
    if pre_readiness.status == "blocked":
        return WorkbookCommunicationMaterializationResult(
            workbook_path=str(path),
            status="blocked",
            pre_readiness=pre_readiness,
            issues=pre_readiness.issues or ["workbook_communication_blocked"],
        )
    if not pre_readiness.candidate_writes:
        return WorkbookCommunicationMaterializationResult(
            workbook_path=str(path),
            output_path=str(path),
            status="noop",
            pre_readiness=pre_readiness,
            post_readiness=pre_readiness,
        )

    output = Path(output_path) if output_path is not None else _default_materialized_path(path)
    if output.resolve() == path.resolve() and not allow_in_place:
        return WorkbookCommunicationMaterializationResult(
            workbook_path=str(path),
            output_path=str(output),
            status="blocked",
            pre_readiness=pre_readiness,
            issues=["output_path_matches_input; set allow_in_place=True or choose a copy path"],
        )

    workbook = load_workbook(path, data_only=False)
    ledger = _reset_communication_ledger(workbook)
    writes_applied: list[WorkbookCommunicationCandidateWrite] = []
    for write in pre_readiness.candidate_writes:
        if write.sheet not in workbook.sheetnames:
            continue
        worksheet = workbook[write.sheet]
        cell = worksheet[write.cell]
        previous_formula = cell.value if _is_formula(cell.value) else None
        cell.value = write.value
        writes_applied.append(write)
        ledger.append(
            [
                write.field,
                write.label,
                write.sheet,
                write.cell,
                write.value,
                write.source_kind,
                write.source_ref,
                previous_formula,
                write.current_display_value,
                write.reason,
            ]
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output)
    post_readiness = plan_workbook_communication_readiness(output, source_values)
    status: WorkbookCommunicationMaterializationStatus = "materialized" if writes_applied else "noop"
    return WorkbookCommunicationMaterializationResult(
        workbook_path=str(path),
        output_path=str(output),
        status=status,
        pre_readiness=pre_readiness,
        post_readiness=post_readiness,
        writes_applied=writes_applied,
        issues=[],
    )


def _summary_valuation_fields(
    formulas: Worksheet,
    cached: Worksheet,
    sources: Mapping[str, WorkbookCommunicationSourceValue],
) -> list[WorkbookCommunicationFieldReport]:
    fields: list[WorkbookCommunicationFieldReport] = []
    for spec in _VALUATION_SUMMARY_SPECS:
        row = _find_label_row(formulas, spec.label)
        if row is None:
            fields.append(
                WorkbookCommunicationFieldReport(
                    field=spec.field,
                    label=spec.label,
                    sheet=formulas.title,
                    required=spec.required,
                    status="missing_cell",
                    issue=f"Could not find Summary row labelled {spec.label!r}.",
                )
            )
            continue
        fields.append(
            _field_report(
                spec=spec,
                formulas=formulas,
                cached=cached,
                cell=f"B{row}",
                source=sources.get(spec.source_field),
            )
        )
    return fields


def _summary_scenario_target_fields(
    formulas: Worksheet,
    cached: Worksheet,
    sources: Mapping[str, WorkbookCommunicationSourceValue],
) -> list[WorkbookCommunicationFieldReport]:
    target_column = _find_table_value_column(formulas, "Case", "Target Price")
    if target_column is None:
        return []

    fields: list[WorkbookCommunicationFieldReport] = []
    for case in _SCENARIO_CASES:
        row = _find_label_row(formulas, case, max_col=1)
        if row is None:
            continue
        label = f"{case.title()} Target Price"
        spec = _CellSpec(
            f"summary.scenario.{case}.target_price",
            label,
            f"summary.scenario.{case}.target_price",
            False,
        )
        fields.append(
            _field_report(
                spec=spec,
                formulas=formulas,
                cached=cached,
                cell=f"{target_column}{row}",
                source=sources.get(spec.source_field),
            )
        )
    return fields


def _direct_cell_fields(
    formulas: Any,
    cached: Any,
    sources: Mapping[str, WorkbookCommunicationSourceValue],
    specs: Iterable[_DirectCellSpec],
) -> list[WorkbookCommunicationFieldReport]:
    fields: list[WorkbookCommunicationFieldReport] = []
    for spec in specs:
        if spec.sheet not in formulas.sheetnames:
            fields.append(
                WorkbookCommunicationFieldReport(
                    field=spec.field,
                    label=spec.label,
                    sheet=spec.sheet,
                    cell=spec.cell,
                    required=spec.required,
                    status="missing_sheet",
                    issue=f"{spec.sheet} sheet is missing.",
                )
            )
            continue
        cell_spec = _CellSpec(spec.field, spec.label, spec.source_field, spec.required)
        fields.append(
            _field_report(
                spec=cell_spec,
                formulas=formulas[spec.sheet],
                cached=cached[spec.sheet],
                cell=spec.cell,
                source=sources.get(spec.source_field),
            )
        )
    return fields


def _visual_cleanup_writes(
    formulas: Any,
    cached: Any,
    *,
    protected_cells: set[tuple[str, str]],
) -> list[WorkbookCommunicationCandidateWrite]:
    writes: list[WorkbookCommunicationCandidateWrite] = []
    for spec in _VISUAL_CLEANUP_SPECS:
        if (spec.sheet, spec.cell) in protected_cells:
            continue
        if spec.sheet not in formulas.sheetnames or spec.sheet not in cached.sheetnames:
            continue
        formula_value = formulas[spec.sheet][spec.cell].value
        cached_value = cached[spec.sheet][spec.cell].value
        if not _is_formula(formula_value) or not _is_blankish(cached_value):
            continue
        writes.append(
            WorkbookCommunicationCandidateWrite(
                field=spec.field,
                label=spec.label,
                sheet=spec.sheet,
                cell=spec.cell,
                value=None,
                source_kind="presentation_cleanup",
                current_formula=formula_value,
                current_display_value=cached_value,
                reason="communication_copy_clears_noncontracted_formula_detail",
            )
        )
    return writes


def _field_report(
    *,
    spec: _CellSpec,
    formulas: Worksheet,
    cached: Worksheet,
    cell: str,
    source: WorkbookCommunicationSourceValue | None,
) -> WorkbookCommunicationFieldReport:
    formula_value = formulas[cell].value
    cached_value = cached[cell].value
    current_formula = formula_value if _is_formula(formula_value) else None
    current_display_value = cached_value if current_formula is not None else formula_value
    has_source = source is not None and not _is_blankish(source.value)
    has_display = not _is_blankish(current_display_value)

    if has_source and (not has_display or not _values_equivalent(source.value, current_display_value)):
        reason = (
            "typed_source_can_materialize_formula_only_display"
            if not has_display
            else "typed_source_updates_stale_display"
        )
        candidate = WorkbookCommunicationCandidateWrite(
            field=spec.field,
            label=spec.label,
            sheet=formulas.title,
            cell=cell,
            value=source.value,
            source_kind=source.source_kind,
            source_ref=source.source_ref,
            current_formula=current_formula,
            current_display_value=current_display_value,
            reason=reason,
        )
        return WorkbookCommunicationFieldReport(
            field=spec.field,
            label=spec.label,
            sheet=formulas.title,
            cell=cell,
            required=spec.required,
            status="materializable",
            current_formula=current_formula,
            current_display_value=current_display_value,
            source_value=source.value,
            source_kind=source.source_kind,
            source_ref=source.source_ref,
            candidate_write=candidate,
        )

    if has_display:
        return WorkbookCommunicationFieldReport(
            field=spec.field,
            label=spec.label,
            sheet=formulas.title,
            cell=cell,
            required=spec.required,
            status="populated",
            current_formula=current_formula,
            current_display_value=current_display_value,
            source_value=source.value if source else None,
            source_kind=source.source_kind if source else None,
            source_ref=source.source_ref if source else None,
        )

    status: WorkbookCommunicationFieldStatus = "formula_only" if current_formula is not None else "missing_source"
    issue = "Formula has no cached display value." if current_formula is not None else "No display value or typed source."
    return WorkbookCommunicationFieldReport(
        field=spec.field,
        label=spec.label,
        sheet=formulas.title,
        cell=cell,
        required=spec.required,
        status=status,
        current_formula=current_formula,
        current_display_value=current_display_value,
        issue=issue,
    )


def _normalize_source_values(
    source_values: Mapping[str, Any],
) -> dict[str, WorkbookCommunicationSourceValue]:
    normalized: dict[str, WorkbookCommunicationSourceValue] = {}
    for field, raw_value in source_values.items():
        if isinstance(raw_value, WorkbookCommunicationSourceValue):
            normalized[field] = raw_value
        elif isinstance(raw_value, Mapping):
            normalized[field] = WorkbookCommunicationSourceValue(
                value=raw_value.get("value"),
                source_kind=str(raw_value.get("source_kind") or "provided"),
                source_ref=_string_or_none(raw_value.get("source_ref")),
            )
        else:
            normalized[field] = WorkbookCommunicationSourceValue(
                value=raw_value,
                source_kind="provided",
            )
    return normalized


def _load_artifact_payload(artifact: str | Path | Mapping[str, Any]) -> tuple[Mapping[str, Any], str | None]:
    if isinstance(artifact, Mapping):
        return artifact, _artifact_ref(artifact)
    path = Path(artifact)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload, str(path)


def _merge_current_model_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    current_model = _first_mapping(
        _get_path(payload, ("verdict", "current_model")),
        _get_path(payload, ("current_model",)),
    )
    if not current_model:
        return
    ref = _string_or_none(current_model.get("price_target_id")) or _string_or_none(
        current_model.get("model_id")
    ) or source_ref
    current_price = _first_number(
        current_model.get("price_target_current_price"),
        current_model.get("current_price"),
        current_model.get("last_price"),
        current_model.get("stock_price"),
    )
    target_price = _first_number(
        current_model.get("last_price_target"),
        current_model.get("price_target"),
        current_model.get("target_price"),
    )
    expected_return = _first_number(
        current_model.get("price_target_expected_return"),
        current_model.get("expected_return"),
        current_model.get("return_pct"),
    )
    _set_number(values, "summary.current_price", current_price, "current_model_artifact", ref)
    _set_number(values, "valuation.current_price", current_price, "current_model_artifact", ref)
    _set_number(values, "summary.blended_target_price", target_price, "current_model_artifact", ref)
    _set_number(
        values,
        "valuation.blended_target_price",
        target_price,
        "current_model_artifact",
        ref,
    )
    _set_number(values, "summary.expected_return", expected_return, "current_model_artifact", ref)
    _set_number(values, "valuation.expected_return", expected_return, "current_model_artifact", ref)


def _merge_price_target_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    price_target = _first_mapping(
        _get_path(payload, ("verdict", "price_target")),
        _get_path(payload, ("typed_outputs", "price_target")),
        _get_path(payload, ("price_target",)),
    )
    if not price_target:
        return
    ref = _string_or_none(price_target.get("id")) or source_ref
    ranges = price_target.get("ranges") if isinstance(price_target.get("ranges"), Mapping) else {}
    target_price = _first_number(
        price_target.get("target_price"),
        price_target.get("price_target"),
        ranges.get("mid") if isinstance(ranges, Mapping) else None,
        ranges.get("base") if isinstance(ranges, Mapping) else None,
    )
    current_price = _first_number(
        price_target.get("current_price"),
        price_target.get("price_target_current_price"),
    )
    expected_return = _first_number(
        price_target.get("expected_return"),
        price_target.get("implied_return_pct"),
        price_target.get("return_pct"),
    )
    _set_number(values, "summary.current_price", current_price, "price_target_artifact", ref)
    _set_number(values, "valuation.current_price", current_price, "price_target_artifact", ref)
    _set_number(values, "summary.blended_target_price", target_price, "price_target_artifact", ref)
    _set_number(
        values,
        "valuation.blended_target_price",
        target_price,
        "price_target_artifact",
        ref,
    )
    _set_number(values, "summary.expected_return", expected_return, "price_target_artifact", ref)
    _set_number(values, "valuation.expected_return", expected_return, "price_target_artifact", ref)


def _merge_valuation_method_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    targets_by_method = _first_mapping(
        _get_path(payload, ("typed_outputs", "targets_by_method")),
        _get_path(payload, ("verdict", "targets_by_method")),
        _get_path(payload, ("verdict", "typed_outputs", "targets_by_method")),
    )
    workbook_write = _first_mapping(
        _get_path(payload, ("typed_outputs", "workbook_write")),
        _get_path(payload, ("verdict", "workbook_write")),
        _get_path(payload, ("typed_outputs", "verification")),
        _get_path(payload, ("verdict", "verification")),
    )
    if not targets_by_method and not workbook_write:
        return

    dcf = targets_by_method.get("dcf") if isinstance(targets_by_method, Mapping) else None
    forward_pe = targets_by_method.get("forward_pe") if isinstance(targets_by_method, Mapping) else None
    forward_ev_ebitda = (
        targets_by_method.get("forward_ev_ebitda") if isinstance(targets_by_method, Mapping) else None
    )
    dcf = dcf if isinstance(dcf, Mapping) else {}
    forward_pe = forward_pe if isinstance(forward_pe, Mapping) else {}
    forward_ev_ebitda = forward_ev_ebitda if isinstance(forward_ev_ebitda, Mapping) else {}
    effectiveness = (
        workbook_write.get("effectiveness_readback") if isinstance(workbook_write, Mapping) else None
    )
    fixed = workbook_write.get("fixed_cell_readback") if isinstance(workbook_write, Mapping) else None
    effectiveness = effectiveness if isinstance(effectiveness, Mapping) else {}
    fixed = fixed if isinstance(fixed, Mapping) else {}
    ref = source_ref

    _set_number(
        values,
        "valuation.dcf_price",
        _first_number(
            effectiveness.get("tpl.v.dcf.dcf_price"),
            effectiveness.get("tpl.v.dcf.dcf_price_summary"),
            dcf.get("target"),
        ),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.forward_pe_price",
        forward_pe.get("target"),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.forward_ev_ebitda_price",
        forward_ev_ebitda.get("target"),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.wacc",
        dcf.get("wacc_pct"),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.terminal_growth",
        _first_number(
            fixed.get("tpl.v.dcf.terminal_growth_base"),
            dcf.get("terminal_growth"),
            dcf.get("terminal_growth_rate"),
        ),
        "valuation_artifact",
        ref,
    )


def _merge_scenario_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    typed_outputs = _first_mapping(
        _get_path(payload, ("verdict", "typed_outputs")),
        _get_path(payload, ("typed_outputs",)),
        _get_path(payload, ("verdict", "fms_result", "typed_outputs")),
    )
    scenario_root = _first_mapping(
        typed_outputs.get("scenarios") if typed_outputs else None,
        _get_path(payload, ("verdict", "scenarios")),
        _get_path(payload, ("scenarios",)),
    )
    if not scenario_root:
        return
    for case, scenario in _iter_scenarios(scenario_root):
        if case not in _SCENARIO_CASES:
            continue
        target_price = _first_number(
            scenario.get("target_price"),
            scenario.get("price_target"),
            scenario.get("selected_scenario_valuation"),
            scenario.get("valuation"),
        )
        return_pct = _first_number(
            scenario.get("return_pct"),
            scenario.get("implied_return_pct"),
            scenario.get("expected_return"),
        )
        probability = _first_number(
            scenario.get("probability"),
            scenario.get("probability_weight"),
            scenario.get("weight"),
        )
        ref = _string_or_none(scenario.get("id")) or source_ref
        _set_number(
            values,
            f"summary.scenario.{case}.target_price",
            target_price,
            "scenario_pricing_artifact",
            ref,
        )
        _set_number(
            values,
            f"summary.scenario.{case}.return_pct",
            return_pct,
            "scenario_pricing_artifact",
            ref,
        )
        _set_number(
            values,
            f"summary.scenario.{case}.probability",
            probability,
            "scenario_pricing_artifact",
            ref,
        )


def _merge_expected_value_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    expected_value = _first_mapping(
        _get_path(payload, ("typed_outputs", "expected_value")),
        _get_path(payload, ("verdict", "expected_value")),
        _get_path(payload, ("expected_value",)),
    )
    if expected_value:
        _set_number(
            values,
            "summary.expected_value.price",
            expected_value.get("expected_price"),
            "expected_value_artifact",
            source_ref,
        )
        _set_number(
            values,
            "summary.expected_value.return_pct",
            expected_value.get("expected_return_pct"),
            "expected_value_artifact",
            source_ref,
        )
        _set_number(
            values,
            "summary.expected_value.return_to_risk",
            expected_value.get("return_to_risk"),
            "expected_value_artifact",
            source_ref,
        )

    probabilities = _first_mapping(
        _get_path(payload, ("typed_outputs", "scenario_probabilities")),
        _get_path(payload, ("verdict", "scenario_probabilities")),
        _get_path(payload, ("scenario_probabilities",)),
    )
    if not probabilities:
        return
    for case, scenario in _iter_scenarios(probabilities):
        if case not in _SCENARIO_CASES:
            continue
        ref = _string_or_none(scenario.get("claim_id")) or source_ref
        _set_number(
            values,
            f"summary.scenario.{case}.probability",
            _first_number(
                scenario.get("probability"),
                scenario.get("probability_weight"),
                scenario.get("weight"),
            ),
            "expected_value_artifact",
            ref,
        )


def _derive_expected_return(values: dict[str, WorkbookCommunicationSourceValue]) -> None:
    if "summary.expected_return" in values:
        return
    current = _number_from_source(values.get("summary.current_price"))
    target = _number_from_source(values.get("summary.blended_target_price"))
    if current is None or target is None or current == 0:
        return
    derived = WorkbookCommunicationSourceValue(
        value=(target / current) - 1.0,
        source_kind="derived_from_typed_artifact_values",
        source_ref=values.get(
            "summary.blended_target_price",
            values.get("summary.current_price"),
        ).source_ref,
    )
    values["summary.expected_return"] = derived
    values["valuation.expected_return"] = derived


def _iter_scenarios(scenario_root: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    if all(isinstance(value, Mapping) for value in scenario_root.values()):
        for key, value in scenario_root.items():
            yield _scenario_case(key, value), value
        return
    scenario_list = scenario_root.get("cases") or scenario_root.get("scenarios")
    if isinstance(scenario_list, list):
        for item in scenario_list:
            if isinstance(item, Mapping):
                yield _scenario_case(item.get("case") or item.get("name"), item), item


def _field_from_mapping(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _default_materialized_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.communication{path.suffix}")


def _reset_communication_ledger(workbook: Any) -> Any:
    title = "_FMS_Communication"
    if title in workbook.sheetnames:
        del workbook[title]
    ledger = workbook.create_sheet(title)
    ledger.sheet_state = "hidden"
    ledger.append(
        [
            "field",
            "label",
            "sheet",
            "cell",
            "value",
            "source_kind",
            "source_ref",
            "previous_formula",
            "previous_display_value",
            "reason",
        ]
    )
    return ledger


def _scenario_case(raw_case: Any, scenario: Mapping[str, Any]) -> str:
    candidate = raw_case or _field_from_mapping(scenario, "case", "name", "scenario")
    text = _normalize_label(candidate)
    if "bull" in text or "upside" in text:
        return "bull"
    if "bear" in text or "downside" in text:
        return "bear"
    if "base" in text or "central" in text:
        return "base"
    return text


def _set_number(
    values: dict[str, WorkbookCommunicationSourceValue],
    field: str,
    value: Any,
    source_kind: str,
    source_ref: str | None,
) -> None:
    number = _number_or_none(value)
    if number is None:
        return
    values[field] = WorkbookCommunicationSourceValue(
        value=number,
        source_kind=source_kind,
        source_ref=source_ref,
    )


def _readiness_status(
    fields: list[WorkbookCommunicationFieldReport],
    candidate_writes: list[WorkbookCommunicationCandidateWrite],
) -> WorkbookCommunicationStatus:
    if _has_required_blockers(fields):
        return "blocked"
    if candidate_writes:
        return "actionable"
    return "ready"


def _has_required_blockers(fields: list[WorkbookCommunicationFieldReport]) -> bool:
    return any(
        field.required
        and field.status in {"formula_only", "missing_source", "missing_sheet", "missing_cell"}
        for field in fields
    )


def _summarize_fields(fields: list[WorkbookCommunicationFieldReport]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for field in fields:
        summary[field.status] = summary.get(field.status, 0) + 1
    return summary


def _issues_from_fields(fields: list[WorkbookCommunicationFieldReport]) -> list[str]:
    issues: list[str] = []
    for field in fields:
        if field.issue:
            issues.append(f"{field.field}: {field.issue}")
    return issues


def _find_label_row(worksheet: Worksheet, label: str, *, max_col: int = 2) -> int | None:
    target = _normalize_label(label)
    for row in worksheet.iter_rows(max_col=max_col):
        for cell in row:
            if _normalize_label(cell.value) == target:
                return cell.row
    return None


def _find_table_value_column(worksheet: Worksheet, row_label: str, value_label: str) -> str | None:
    row_target = _normalize_label(row_label)
    value_target = _normalize_label(value_label)
    for row in worksheet.iter_rows():
        row_has_label = any(_normalize_label(cell.value) == row_target for cell in row)
        if not row_has_label:
            continue
        for cell in row:
            if _normalize_label(cell.value) == value_target:
                return cell.column_letter
    return None
