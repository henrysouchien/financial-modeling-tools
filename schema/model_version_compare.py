"""Deterministic, on-demand comparison of immutable model semantics.

Two reads live here. ``compare_model_versions`` and
``compare_model_version_candidate`` diff two ModelVersions.
``compare_workbook_assumptions`` reads a published version's workbook beside
the forecast assumptions that own its values. It lists each assumption cell the
workbook was given and names the cells where the workbook and the FAS row
disagree. A cell someone typed into becomes a proposed ``fms_set_driver``
judgment (Supersession ``re-thinking``, author human). The workbook is read and
never written: the divergence stays a finding until the human confirms it and
the door persists the successor.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from .build_model_product_contracts import (
    ModelSpec,
    ProvenanceMap,
    ScheduleInputAudit,
    ScheduleInputAuditArtifact,
)
from .driver_resolver import resolve_item_cells
from .forecast_control_profile import sia_generic_nonvaluation_forecast_profile
from .handoff import SeedCoverageRecord
from .model_ref import ModelVersionRef
from .model_semantic_document import (
    ModelSemanticDocument,
    RecordedValidationStatus,
    SemanticValueSnapshot,
    decode_model_semantic_document,
)
from .model_spec_decoder import (
    VERIFICATION_MODE_SINGLE_BOUNDARY,
    VerifiedModelSpecDecodeRequest,
    decode_verified_model_spec_compiler_input,
)
from .models import FormulaType


class _ComparisonContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelVersionIdentityComparison(_ComparisonContract):
    predecessor: ModelVersionRef
    successor: ModelVersionRef
    successor_spec_predecessor: ModelVersionRef | None = None
    successor_spec_idempotency_key: str | None = None
    successor_spec_linked: bool


class AssumptionDelta(_ComparisonContract):
    assumption_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    change: Literal["added", "removed", "changed"]
    period: int | None = None
    old_value: Any = None
    new_value: Any = None


class ScenarioReviewDelta(_ComparisonContract):
    scenario_name: str = Field(min_length=1)
    assumption_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    change: Literal["added", "removed", "changed"]
    period: int | None = None
    old_value: Any = None
    new_value: Any = None


class SemanticValueDelta(_ComparisonContract):
    item_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    period: int
    change: Literal[
        "value_changed",
        "state_transition",
        "item_added",
        "item_removed",
        "period_added",
        "period_removed",
    ]
    old_state: str | None = None
    new_state: str | None = None
    old_value: Any = None
    new_value: Any = None
    tolerance: float = Field(ge=0)


class ProvenanceDelta(_ComparisonContract):
    item_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    period: int
    old_lineage_status: str | None = None
    new_lineage_status: str | None = None
    old_evidence_ids: tuple[str, ...] = ()
    new_evidence_ids: tuple[str, ...] = ()
    old_assumption_ids: tuple[str, ...] = ()
    new_assumption_ids: tuple[str, ...] = ()
    old_source_refs: tuple[str, ...] = ()
    new_source_refs: tuple[str, ...] = ()
    old_compiler_origin: str | None = None
    new_compiler_origin: str | None = None


class ProvenanceDeltaSummary(_ComparisonContract):
    changed_cell_count: int = Field(ge=0)
    lineage_status_change_count: int = Field(ge=0)
    evidence_ref_change_count: int = Field(ge=0)
    assumption_ref_change_count: int = Field(ge=0)
    compiler_origin_change_count: int = Field(ge=0)


class ScheduleInputAuditDelta(_ComparisonContract):
    family: str = Field(min_length=1)
    row_id: str = Field(min_length=1)
    public_label: str = Field(min_length=1)
    old_state: str | None = None
    new_state: str | None = None
    old_missing_periods: tuple[int, ...] = ()
    new_missing_periods: tuple[int, ...] = ()
    old_origins: tuple[str, ...] = ()
    new_origins: tuple[str, ...] = ()


class ScheduleInputAuditDeltaSummary(_ComparisonContract):
    changed_row_count: int = Field(ge=0)


class ModelStatusDelta(_ComparisonContract):
    predecessor_status: RecordedValidationStatus
    successor_status: RecordedValidationStatus
    predecessor_blocker_count: int = Field(ge=0)
    successor_blocker_count: int = Field(ge=0)
    blocker_count_change: int


class ValuationEvidenceStatusDelta(_ComparisonContract):
    changed: bool
    predecessor: dict[str, Any]
    successor: dict[str, Any]


class ModelVersionComparison(_ComparisonContract):
    identity: ModelVersionIdentityComparison
    assumption_deltas: tuple[AssumptionDelta, ...] = ()
    scenario_review_deltas: tuple[ScenarioReviewDelta, ...] = ()
    semantic_value_deltas: tuple[SemanticValueDelta, ...] = ()
    provenance_summary: ProvenanceDeltaSummary
    provenance_deltas: tuple[ProvenanceDelta, ...] = ()
    schedule_input_audit_summary: ScheduleInputAuditDeltaSummary
    schedule_input_audit_deltas: tuple[ScheduleInputAuditDelta, ...] = ()
    status: ModelStatusDelta
    valuation_evidence_status: ValuationEvidenceStatusDelta


def _model_spec(document: ModelSemanticDocument) -> ModelSpec:
    compiler_input = document.render_state.compiler_inputs.model_spec
    decoded = decode_verified_model_spec_compiler_input(
        VerifiedModelSpecDecodeRequest(
            payload_mapping=compiler_input.payload,
            compiler_input_hash=compiler_input.content_hash,
            external_model_version_digest=None,
        )
    )
    if decoded.verification_mode != VERIFICATION_MODE_SINGLE_BOUNDARY:
        raise ValueError("semantic comparison requires single-boundary mode")
    return decoded.model_spec


@dataclass(frozen=True)
class _ComparisonInput:
    ref: ModelVersionRef
    spec: ModelSpec
    semantic_values: tuple[SemanticValueSnapshot, ...]
    provenance: ProvenanceMap
    schedule_input_audit: ScheduleInputAuditArtifact
    validation_status: RecordedValidationStatus
    validation_blocker_count: int
    valuation_evidence_status: dict[str, Any]


def _input(document: ModelSemanticDocument) -> _ComparisonInput:
    return _ComparisonInput(
        ref=_ref(document),
        spec=_model_spec(document),
        semantic_values=document.semantic_values,
        provenance=document.provenance,
        schedule_input_audit=document.schedule_input_audit,
        validation_status=document.validation_status,
        validation_blocker_count=document.validation_blocker_count,
        valuation_evidence_status=document.valuation_evidence_status.model_dump(
            mode="json"
        ),
    )


def _ref(document: ModelSemanticDocument) -> ModelVersionRef:
    return ModelVersionRef(
        model_id=document.model_id,
        version=document.model_version,
    )


def _driver_label(driver: str) -> str:
    """Render a driver key as presentation-safe text."""

    token = driver.rsplit(".", 1)[-1]
    rendered = token.replace("_", " ").strip()
    return rendered[:1].upper() + rendered[1:] if rendered else "Assumption"


def _assumption_label(assumption: Any) -> str:
    """Render an authored driver identity as presentation-safe text."""

    return _driver_label(str(assumption.driver))


def _comparison_curves(
    old_value: Any,
    new_value: Any,
) -> tuple[dict[int, Any], dict[int, Any]]:
    """Expand a scalar counterpart over the explicit curve's exact periods."""

    periods = {
        *(old_value if isinstance(old_value, dict) else ()),
        *(new_value if isinstance(new_value, dict) else ()),
    }

    def expand(value: Any) -> dict[int, Any]:
        if isinstance(value, dict):
            return {int(period): item for period, item in value.items()}
        if value is None:
            return {}
        return {int(period): value for period in periods}

    return expand(old_value), expand(new_value)


def _forecast_assumption_deltas(
    predecessor: ModelSpec,
    successor: ModelSpec,
) -> tuple[AssumptionDelta, ...]:
    old = {
        str(item.assumption_id): item
        for item in predecessor.forecast_assumptions
        if item.assumption_id is not None
    }
    new = {
        str(item.assumption_id): item
        for item in successor.forecast_assumptions
        if item.assumption_id is not None
    }
    deltas: list[AssumptionDelta] = []
    for assumption_id in sorted(set(old) | set(new)):
        prior = old.get(assumption_id)
        current = new.get(assumption_id)
        old_value = None if prior is None else (
            prior.value if prior.value is not None else prior.period_values
        )
        new_value = None if current is None else (
            current.value if current.value is not None else current.period_values
        )
        if prior is not None and current is not None and old_value == new_value:
            continue
        change = "added" if prior is None else "removed" if current is None else "changed"
        label_source = current or prior
        assert label_source is not None
        if isinstance(old_value, dict) or isinstance(new_value, dict):
            old_curve, new_curve = _comparison_curves(old_value, new_value)
            for period in sorted(set(old_curve) | set(new_curve)):
                prior_period_value = old_curve.get(period)
                current_period_value = new_curve.get(period)
                if prior_period_value == current_period_value:
                    continue
                deltas.append(
                    AssumptionDelta(
                        assumption_id=assumption_id,
                        label=_assumption_label(label_source),
                        change=change,
                        period=int(period),
                        old_value=prior_period_value,
                        new_value=current_period_value,
                    )
                )
        else:
            deltas.append(
                AssumptionDelta(
                    assumption_id=assumption_id,
                    label=_assumption_label(label_source),
                    change=change,
                    old_value=old_value,
                    new_value=new_value,
                )
            )
    return tuple(deltas)


def _scenario_review_deltas(
    predecessor: ModelSpec,
    successor: ModelSpec,
) -> tuple[ScenarioReviewDelta, ...]:
    def entries(spec: ModelSpec) -> dict[tuple[str, str], Any]:
        return {
            (review.scenario_name, override.assumption_id): override
            for review in spec.scenario_reviews
            for override in review.overrides
        }

    labels = {
        str(item.assumption_id): _assumption_label(item)
        for spec in (predecessor, successor)
        for item in spec.forecast_assumptions
        if item.assumption_id is not None
    }
    old = entries(predecessor)
    new = entries(successor)
    deltas: list[ScenarioReviewDelta] = []
    for key in sorted(set(old) | set(new)):
        prior = old.get(key)
        current = new.get(key)
        old_value = None if prior is None else prior.value
        new_value = None if current is None else current.value
        if prior is not None and current is not None and old_value == new_value:
            continue
        change = "added" if prior is None else "removed" if current is None else "changed"
        scenario_name, assumption_id = key
        if isinstance(old_value, dict) or isinstance(new_value, dict):
            old_curve, new_curve = _comparison_curves(old_value, new_value)
            for period in sorted(set(old_curve) | set(new_curve)):
                prior_period_value = old_curve.get(period)
                current_period_value = new_curve.get(period)
                if prior_period_value == current_period_value:
                    continue
                deltas.append(
                    ScenarioReviewDelta(
                        scenario_name=scenario_name,
                        assumption_id=assumption_id,
                        label=f"{scenario_name.title()} scenario — {labels.get(assumption_id, 'Assumption')}",
                        change=change,
                        period=int(period),
                        old_value=prior_period_value,
                        new_value=current_period_value,
                    )
                )
        else:
            deltas.append(
                ScenarioReviewDelta(
                    scenario_name=scenario_name,
                    assumption_id=assumption_id,
                    label=f"{scenario_name.title()} scenario — {labels.get(assumption_id, 'Assumption')}",
                    change=change,
                    old_value=old_value,
                    new_value=new_value,
                )
            )
    return tuple(deltas)


def _semantic_value_deltas(
    predecessor: _ComparisonInput,
    successor: _ComparisonInput,
) -> tuple[SemanticValueDelta, ...]:
    old = {(cell.semantic_item_id, cell.period): cell for cell in predecessor.semantic_values}
    new = {(cell.semantic_item_id, cell.period): cell for cell in successor.semantic_values}
    old_items = {item_id for item_id, _period in old}
    new_items = {item_id for item_id, _period in new}
    deltas: list[SemanticValueDelta] = []
    for key in sorted(set(old) | set(new)):
        prior = old.get(key)
        current = new.get(key)
        item_id, period = key
        label = (current or prior).semantic_label  # type: ignore[union-attr]
        tolerance = max(
            0.0 if prior is None else prior.tolerance,
            0.0 if current is None else current.tolerance,
        )
        if prior is None:
            change = "item_added" if item_id not in old_items else "period_added"
        elif current is None:
            change = "item_removed" if item_id not in new_items else "period_removed"
        elif prior.state != current.state:
            change = "state_transition"
        elif prior.state != "known":
            continue
        elif isinstance(prior.value, (int, float)) and not isinstance(prior.value, bool) and isinstance(current.value, (int, float)) and not isinstance(current.value, bool):
            if math.isclose(
                float(prior.value),
                float(current.value),
                rel_tol=tolerance,
                abs_tol=tolerance,
            ):
                continue
            change = "value_changed"
        elif prior.value != current.value:
            change = "value_changed"
        else:
            continue
        deltas.append(
            SemanticValueDelta(
                item_id=item_id,
                label=label,
                period=period,
                change=change,
                old_state=None if prior is None else prior.state,
                new_state=None if current is None else current.state,
                old_value=None if prior is None else prior.value,
                new_value=None if current is None else current.value,
                tolerance=tolerance,
            )
        )
    return tuple(deltas)


def _lineage_by_value(
    document: _ComparisonInput,
) -> dict[tuple[str, int], Any]:
    lineage_by_cell = {
        (cell.sheet, cell.cell): cell for cell in document.provenance.cells
    }
    return {
        (value.semantic_item_id, value.period): lineage_by_cell[(value.sheet, value.cell)]
        for value in document.semantic_values
        if (value.sheet, value.cell) in lineage_by_cell
    }


def _provenance_deltas(
    predecessor: _ComparisonInput,
    successor: _ComparisonInput,
) -> tuple[tuple[ProvenanceDelta, ...], ProvenanceDeltaSummary]:
    old = _lineage_by_value(predecessor)
    new = _lineage_by_value(successor)
    labels = {
        (cell.semantic_item_id, cell.period): cell.semantic_label
        for document in (predecessor, successor)
        for cell in document.semantic_values
    }
    deltas: list[ProvenanceDelta] = []
    status_changes = evidence_changes = assumption_changes = compiler_changes = 0
    for key in sorted(set(old) | set(new)):
        prior = old.get(key)
        current = new.get(key)
        old_status = None if prior is None else prior.lineage_status
        new_status = None if current is None else current.lineage_status
        old_evidence = () if prior is None else tuple(sorted(ref.evidence_id for ref in prior.evidence_refs))
        new_evidence = () if current is None else tuple(sorted(ref.evidence_id for ref in current.evidence_refs))
        old_assumptions = () if prior is None else tuple(sorted(prior.assumption_ids))
        new_assumptions = () if current is None else tuple(sorted(current.assumption_ids))
        old_sources = () if prior is None else tuple(sorted(str(ref) for ref in prior.source_refs))
        new_sources = () if current is None else tuple(sorted(str(ref) for ref in current.source_refs))
        old_compiler = (
            None if prior is None or prior.compiler_origin is None
            else prior.compiler_origin.kind
        )
        new_compiler = (
            None if current is None or current.compiler_origin is None
            else current.compiler_origin.kind
        )
        if (old_status, old_evidence, old_assumptions, old_sources, old_compiler) == (
            new_status,
            new_evidence,
            new_assumptions,
            new_sources,
            new_compiler,
        ):
            continue
        status_changes += int(old_status != new_status)
        evidence_changes += int(old_evidence != new_evidence or old_sources != new_sources)
        assumption_changes += int(old_assumptions != new_assumptions)
        compiler_changes += int(old_compiler != new_compiler)
        item_id, period = key
        deltas.append(
            ProvenanceDelta(
                item_id=item_id,
                label=labels[key],
                period=period,
                old_lineage_status=old_status,
                new_lineage_status=new_status,
                old_evidence_ids=old_evidence,
                new_evidence_ids=new_evidence,
                old_assumption_ids=old_assumptions,
                new_assumption_ids=new_assumptions,
                old_source_refs=old_sources,
                new_source_refs=new_sources,
                old_compiler_origin=old_compiler,
                new_compiler_origin=new_compiler,
            )
        )
    return tuple(deltas), ProvenanceDeltaSummary(
        changed_cell_count=len(deltas),
        lineage_status_change_count=status_changes,
        evidence_ref_change_count=evidence_changes,
        assumption_ref_change_count=assumption_changes,
        compiler_origin_change_count=compiler_changes,
    )


def _schedule_input_audit_deltas(
    predecessor: ScheduleInputAuditArtifact,
    successor: ScheduleInputAuditArtifact,
) -> tuple[tuple[ScheduleInputAuditDelta, ...], ScheduleInputAuditDeltaSummary]:
    old = (
        {(row.family, row.row_id): row for row in predecessor.rows}
        if isinstance(predecessor, ScheduleInputAudit)
        else {}
    )
    new = (
        {(row.family, row.row_id): row for row in successor.rows}
        if isinstance(successor, ScheduleInputAudit)
        else {}
    )
    deltas: list[ScheduleInputAuditDelta] = []
    for key in sorted(set(old) | set(new)):
        prior = old.get(key)
        current = new.get(key)
        old_origins = () if prior is None else tuple(
            sorted(f"{origin.kind}:{origin.reference}" for origin in prior.origins)
        )
        new_origins = () if current is None else tuple(
            sorted(f"{origin.kind}:{origin.reference}" for origin in current.origins)
        )
        old_shape = (
            None if prior is None else prior.state,
            () if prior is None else prior.missing_periods,
            old_origins,
        )
        new_shape = (
            None if current is None else current.state,
            () if current is None else current.missing_periods,
            new_origins,
        )
        if old_shape == new_shape:
            continue
        row = current or prior
        assert row is not None
        deltas.append(
            ScheduleInputAuditDelta(
                family=key[0],
                row_id=key[1],
                public_label=row.public_label,
                old_state=old_shape[0],
                new_state=new_shape[0],
                old_missing_periods=old_shape[1],
                new_missing_periods=new_shape[1],
                old_origins=old_origins,
                new_origins=new_origins,
            )
        )
    return tuple(deltas), ScheduleInputAuditDeltaSummary(
        changed_row_count=len(deltas)
    )


def _compare_inputs(
    predecessor: _ComparisonInput,
    successor: _ComparisonInput,
) -> ModelVersionComparison:
    provenance_deltas, provenance_summary = _provenance_deltas(predecessor, successor)
    audit_deltas, audit_summary = _schedule_input_audit_deltas(
        predecessor.schedule_input_audit,
        successor.schedule_input_audit,
    )
    return ModelVersionComparison(
        identity=ModelVersionIdentityComparison(
            predecessor=predecessor.ref,
            successor=successor.ref,
            successor_spec_predecessor=successor.spec.predecessor,
            successor_spec_idempotency_key=successor.spec.idempotency_key,
            successor_spec_linked=successor.spec.predecessor == predecessor.ref,
        ),
        assumption_deltas=_forecast_assumption_deltas(predecessor.spec, successor.spec),
        scenario_review_deltas=_scenario_review_deltas(predecessor.spec, successor.spec),
        semantic_value_deltas=_semantic_value_deltas(predecessor, successor),
        provenance_summary=provenance_summary,
        provenance_deltas=provenance_deltas,
        schedule_input_audit_summary=audit_summary,
        schedule_input_audit_deltas=audit_deltas,
        status=ModelStatusDelta(
            predecessor_status=predecessor.validation_status,
            successor_status=successor.validation_status,
            predecessor_blocker_count=predecessor.validation_blocker_count,
            successor_blocker_count=successor.validation_blocker_count,
            blocker_count_change=(
                successor.validation_blocker_count - predecessor.validation_blocker_count
            ),
        ),
        valuation_evidence_status=ValuationEvidenceStatusDelta(
            changed=(
                predecessor.valuation_evidence_status
                != successor.valuation_evidence_status
            ),
            predecessor=predecessor.valuation_evidence_status,
            successor=successor.valuation_evidence_status,
        ),
    )


def compare_model_version_candidate(
    predecessor: ModelSemanticDocument,
    *,
    successor_ref: ModelVersionRef,
    successor_spec: ModelSpec,
    semantic_values: tuple[SemanticValueSnapshot, ...],
    provenance: ProvenanceMap,
    schedule_input_audit: ScheduleInputAudit,
    validation_status: RecordedValidationStatus,
    validation_blocker_count: int,
    valuation_evidence_status: Any,
) -> ModelVersionComparison:
    """Compare a not-yet-persisted render while its workbook is being built."""

    predecessor = decode_model_semantic_document(predecessor)
    return _compare_inputs(
        _input(predecessor),
        _ComparisonInput(
            ref=ModelVersionRef.model_validate(successor_ref),
            spec=ModelSpec.model_validate(successor_spec),
            semantic_values=semantic_values,
            provenance=ProvenanceMap.model_validate(provenance),
            schedule_input_audit=ScheduleInputAudit.model_validate(
                schedule_input_audit
            ),
            validation_status=validation_status,
            validation_blocker_count=validation_blocker_count,
            valuation_evidence_status=valuation_evidence_status.model_dump(
                mode="json"
            ),
        ),
    )


def compare_model_versions(
    predecessor: ModelSemanticDocument,
    successor: ModelSemanticDocument,
) -> ModelVersionComparison:
    """Compute a stable comparison without creating a persisted carrier."""

    predecessor = decode_model_semantic_document(predecessor)
    successor = decode_model_semantic_document(successor)
    return _compare_inputs(_input(predecessor), _input(successor))


#: How a workbook cell differs from its FAS row. ``workbook_edit``: the cell no
#: longer holds what the ModelVersion rendered there, so someone typed into it,
#: and its value is a proposed successor row. ``fas_revised``: the cell still
#: holds the published value, but the FAS row has moved since; compiling and
#: publishing the current chain carries it.
WorkbookDivergenceKind = Literal["workbook_edit", "fas_revised"]


class WorkbookAssumptionCell(_ComparisonContract):
    """One workbook cell holding a forecast assumption's base value."""

    #: The FAS row that owns the cell: the forecast control whose lowering
    #: wrote it, which may be a different key from the compiler key.
    driver_key: str = Field(min_length=1)
    assumption_id: str | None = None
    label: str = Field(min_length=1)
    period: int
    sheet: str = Field(min_length=1)
    cell: str = Field(pattern=r"^[A-Z]+[1-9][0-9]*$")
    #: ``Sheet!A1``: the key of this cell's value in a workbook read.
    address: str = Field(min_length=1)
    #: What the ModelVersion rendered into the cell.
    published_value: Any = None
    #: The factor the compile multiplied the control's value by to render
    #: this cell, when one control fans out over several rows: ``sbc_pct``
    #: scaled by one line's share of the disclosed mix. Absent when the cell
    #: holds the control's value as written.
    lowering_scale: float | None = None


class WorkbookAssumptionDivergence(_ComparisonContract):
    """One assumption cell whose workbook value is not its FAS row's value."""

    driver_key: str = Field(min_length=1)
    assumption_id: str | None = None
    label: str = Field(min_length=1)
    period: int
    address: str = Field(min_length=1)
    kind: WorkbookDivergenceKind
    workbook_value: Any = None
    #: The FAS row's base value for the period; absent when the FAS has no row.
    fas_value: Any = None
    published_value: Any = None


class WorkbookAssumptionProposal(_ComparisonContract):
    """The ``fms_set_driver`` judgment one driver's workbook edits become.

    Nothing is written until the human confirms it and the door persists the
    successor; the caller adds the human's rationale.
    """

    driver_key: str = Field(min_length=1)
    values_by_year: dict[str, float]
    workbook_edit: ModelVersionRef


class WorkbookAssumptionComparison(_ComparisonContract):
    model_version: ModelVersionRef
    divergences: tuple[WorkbookAssumptionDivergence, ...] = ()
    proposals: tuple[WorkbookAssumptionProposal, ...] = ()
    #: Assumption cells not compared: the workbook read did not include them,
    #: or no row of the FAS owns them. They are unread, not in sync.
    unread: tuple[str, ...] = ()


def _owning_controls() -> dict[str, str]:
    """Each compiler key the forecast controls lower onto, and its control."""

    return {
        binding.projection_driver_key: control.control_id
        for control in sia_generic_nonvaluation_forecast_profile().controls
        for binding in control.target_bindings
    }


def _lowering_scale(lineage: object) -> float | None:
    """The disclosed-mix factor the compile applied to a fanned-out control.

    The compile records it on the row it scaled
    (``schema.build_company_grounded_schedules._scale_aggregate_sbc_rate``):
    the line's disclosed rate over the disclosed aggregate rate.
    """

    if not isinstance(lineage, dict) or "authored_value" not in lineage:
        return None
    return float(lineage["disclosed_rate"]) / float(lineage["aggregate_rate"])


def workbook_assumption_cells(
    document: ModelSemanticDocument,
) -> tuple[WorkbookAssumptionCell, ...]:
    """Every workbook cell of this ModelVersion that holds an assumption's base value.

    The compile's own seed-coverage manifest names where each base assumption
    was lowered. A scenario-flexed driver's base value is held on its base
    scenario row, because the driver's own row selects a case by formula. For
    every other driver, the value is held on the row the assumption was
    lowered onto. A cell counts only when its published lineage renders a
    constant. A formula the assumption feeds, such as an equation-owned
    control, is not a cell that holds the assumption.

    Each cell is reported under the forecast control that owns it, as the
    profile binds it. A control that fans out over several rows (``sbc_pct``
    over the disclosed SBC mix) carries the scale the compile applied, as
    recorded on the row. A row that a per-line control displaced belongs to
    that control alone.
    """

    document = decode_model_semantic_document(document)
    owners = _owning_controls()
    records = tuple(
        record
        for record in document.render_state.seed_coverage_manifest
        if isinstance(record, SeedCoverageRecord)
    )
    base_rows = {
        record.canonical_driver_key: record
        for record in records
        if record.source_kind == "projection_override" and record.scenario == "base"
    }
    lineage = {(cell.sheet, cell.cell): cell for cell in document.provenance.cells}
    cells: list[WorkbookAssumptionCell] = []
    for record in records:
        if record.source_kind != "model_spec_assumption":
            continue
        owner = owners.get(record.canonical_driver_key, record.canonical_driver_key)
        target = base_rows.get(record.canonical_driver_key, record)
        item = document.model.get_item(target.resolved_item_id)
        values = {} if item.values is None else item.values.values
        overrides = item.overrides or {}
        for sheet, cell, period in resolve_item_cells(
            document.model, target.resolved_item_id, list(target.periods)
        ):
            rendered = lineage.get((sheet, cell))
            if rendered is None or (
                rendered.formula is not None
                and rendered.formula.type is not FormulaType.constant
            ):
                continue
            written = overrides.get(period)
            row_lineage = None if written is None else written.params.get("lineage")
            if isinstance(row_lineage, dict) and row_lineage.get("control") not in (
                None,
                owner,
            ):
                continue
            published = values.get(period)
            cells.append(
                WorkbookAssumptionCell(
                    driver_key=owner,
                    assumption_id=record.assumption_id,
                    label=_driver_label(owner),
                    period=period,
                    sheet=sheet,
                    cell=cell,
                    address=f"{sheet}!{cell}",
                    published_value=None if published is None else published.value,
                    lowering_scale=_lowering_scale(row_lineage),
                )
            )
    return tuple(sorted(cells, key=lambda cell: (cell.driver_key, cell.period, cell.address)))


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _workbook_value(value: object) -> object:
    """A workbook read's cell value: numeric text is a number, blank is absent."""

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return text
    if _is_number(value):
        return float(value)  # type: ignore[arg-type]
    return value


def _same_value(left: object, right: object) -> bool:
    if _is_number(left) and _is_number(right):
        return math.isclose(
            float(left),  # type: ignore[arg-type]
            float(right),  # type: ignore[arg-type]
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    return left == right


class AssumptionBaseValue(Protocol):
    """What the read needs of a FAS row: its driver key and base values."""

    @property
    def driver(self) -> str: ...

    @property
    def value(self) -> object: ...

    @property
    def period_values(self) -> Mapping[int, float] | None: ...


def _fas_value(
    assumption: AssumptionBaseValue,
    period: int,
    lowering_scale: float | None,
) -> object:
    """The FAS row's value for the cell, through the lowering the compile applied."""

    value = (
        assumption.period_values.get(period)
        if assumption.period_values is not None
        else assumption.value
    )
    if lowering_scale is None or not _is_number(value):
        return value
    return float(value) * lowering_scale  # type: ignore[arg-type]


def compare_workbook_assumptions(
    document: ModelSemanticDocument,
    *,
    forecast_assumptions: Iterable[AssumptionBaseValue],
    workbook_values: Mapping[str, object],
) -> WorkbookAssumptionComparison:
    """Read a published workbook's assumption cells beside their FAS rows.

    ``forecast_assumptions`` are the bound ForecastAssumptionSet's rows (its
    ``driver_assumptions``' ``assumption``). ``workbook_values`` maps a cell's
    ``address`` (``Sheet!A1``) to the value the live workbook holds there.
    Each cell is compared with its owning FAS row's value, through the scale
    the compile applied to a fanned-out control. A cell whose workbook value
    equals that value is in sync. A diverging cell is a ``workbook_edit`` when
    the workbook no longer holds what was published, and ``fas_revised``
    otherwise. A cell no FAS row owns is ``unread``. Each driver's numeric
    workbook edits become one proposal when the row holds the cell as
    written. One line of a fanned-out control is not that control's value,
    so an edit there is a divergence with no proposal.
    """

    document = decode_model_semantic_document(document)
    model_version = _ref(document)
    rows = {str(row.driver): row for row in forecast_assumptions}
    divergences: list[WorkbookAssumptionDivergence] = []
    unread: list[str] = []
    edits: dict[str, dict[str, float]] = {}
    for cell in workbook_assumption_cells(document):
        row = rows.get(cell.driver_key)
        if cell.address not in workbook_values or row is None:
            unread.append(cell.address)
            continue
        workbook_value = _workbook_value(workbook_values[cell.address])
        fas_value = _fas_value(row, cell.period, cell.lowering_scale)
        if _same_value(workbook_value, fas_value):
            continue
        edited = not _same_value(workbook_value, cell.published_value)
        divergences.append(
            WorkbookAssumptionDivergence(
                driver_key=cell.driver_key,
                assumption_id=cell.assumption_id,
                label=cell.label,
                period=cell.period,
                address=cell.address,
                kind="workbook_edit" if edited else "fas_revised",
                workbook_value=workbook_value,
                fas_value=fas_value,
                published_value=cell.published_value,
            )
        )
        if (
            edited
            and cell.lowering_scale is None
            and isinstance(workbook_value, float)
            and math.isfinite(workbook_value)
        ):
            edits.setdefault(cell.driver_key, {})[str(cell.period)] = workbook_value
    return WorkbookAssumptionComparison(
        model_version=model_version,
        divergences=tuple(divergences),
        proposals=tuple(
            WorkbookAssumptionProposal(
                driver_key=driver_key,
                values_by_year=values_by_year,
                workbook_edit=model_version,
            )
            for driver_key, values_by_year in sorted(edits.items())
        ),
        unread=tuple(unread),
    )


__all__ = [
    "AssumptionBaseValue",
    "AssumptionDelta",
    "ModelStatusDelta",
    "ModelVersionComparison",
    "ModelVersionIdentityComparison",
    "ProvenanceDelta",
    "ProvenanceDeltaSummary",
    "ScenarioReviewDelta",
    "ScheduleInputAuditDelta",
    "ScheduleInputAuditDeltaSummary",
    "SemanticValueDelta",
    "ValuationEvidenceStatusDelta",
    "WorkbookAssumptionCell",
    "WorkbookAssumptionComparison",
    "WorkbookAssumptionDivergence",
    "WorkbookAssumptionProposal",
    "WorkbookDivergenceKind",
    "compare_model_version_candidate",
    "compare_model_versions",
    "compare_workbook_assumptions",
    "workbook_assumption_cells",
]
