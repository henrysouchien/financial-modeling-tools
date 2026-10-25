"""Annual render inputs and the writes appended after validation.

Called only by the build composition root (``finalize_compiled_model`` in
``schema/build.py``): the compiler-input payload for the render state, the
annual render context (version identity and required-unavailable cells), the
Valuation evidence status block, and the Summary verdict block with the
published bytes it is serialized into.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import hashlib
from typing import Any

from .annual_presentation_guard import (
    evaluate_annual_presentation,
    read_annual_presentation_cells as _read_annual_presentation_cells,
)
from .build_findings import blocking_readiness_issues, finding_subject_items
from .build_model_product_contracts import ModelSpec, SourceBundle
from .build_offline_validation import workbook_readback_mismatches
from .build_offline_validation import lag_input_gap_readers, owed_history_cells, owned_cell_gaps
from .excel_writer import render_xlsx_bytes
from .formatter import ModelFormatter
from .handle import ComputedValues
from .model_kind_dispatch import declared_semantic_unit_plane
from .model_readiness import ModelProjectionReadiness
from .model_readiness_quality import ModelQualityReadiness
from .model_readiness_scenario_bridge import ModelScenarioBridgeReadiness
from .model_ref import ModelStamp, ModelVersionRef
from .model_semantic_document import ModelSemanticDocument, semantic_value_snapshots
from .models import FinancialModel
from .projected_financial_integrity import evaluate_projected_financial_integrity
from .renderer import finalize_render_contract
from .renderer_columns import item_owns_cell
from .renderer_summary import append_summary_findings
from .renderer_types import AnnualRenderContext, CellWrite, RenderPlan


def _annual_render_context(
    *,
    model: FinancialModel,
    source_bundle: SourceBundle,
    computed_values: ComputedValues,
    projection_readiness: ModelProjectionReadiness,
    model_quality_readiness: ModelQualityReadiness,
    scenario_bridge_readiness: ModelScenarioBridgeReadiness,
    scenario_results: dict[str, object],
    version_identity: ModelVersionRef | None,
    predecessor_semantic_document: ModelSemanticDocument | None,
) -> AnnualRenderContext:
    """The annual render inputs: version identity and required-unavailable cells.

    A required cell the row owns (``item_owns_cell``) with no compiled value renders
    ``NA()``; a coordinate outside the row's ``formula_periods`` is no cell and is not
    required. The verdict and its findings are appended to the Summary after validation.
    """

    projection_periods = tuple(model.time_structure.projection_periods)
    required: set[tuple[str, int]] = set()
    integrity = evaluate_projected_financial_integrity(
        model,
        computed_values=computed_values,
        projection_periods=list(projection_periods),
        semantic_units=declared_semantic_unit_plane(source_bundle),
    )
    for domain in model_quality_readiness.domains.values():
        for item_id in domain.required_items:
            for period in projection_periods:
                if period not in computed_values.get(item_id, {}):
                    required.add((item_id, int(period)))
    for item_id in integrity.required_item_ids:
        for period in projection_periods:
            if period not in computed_values.get(item_id, {}):
                required.add((item_id, int(period)))
    if scenario_bridge_readiness.owners:
        for owner in scenario_bridge_readiness.owners:
            for period in projection_periods:
                if period not in computed_values.get(owner.owner_id, {}):
                    required.add((owner.owner_id, int(period)))
    for issue in blocking_readiness_issues(
        projection_readiness=projection_readiness,
        model_quality_readiness=model_quality_readiness,
        scenario_bridge_readiness=scenario_bridge_readiness,
        scenario_results=scenario_results,
        projection_periods=projection_periods,
    ):
        item_id = getattr(issue, "item_id", None) or getattr(issue, "owner_id", None)
        if item_id:
            for period in getattr(issue, "missing_periods", ()):
                required.add((str(item_id), int(period)))
    for finding in integrity.findings:
        if finding.item_id:
            for period in finding.periods:
                required.add((str(finding.item_id), int(period)))
    # A source-bundle data gap off the rendered axis leaves its readers'
    # first historical cells with no formula to render; each such cell with
    # no value is required-unavailable and displays #N/A beside the gap
    # (ruled 2026-09-29; the derivation is ``lag_input_gap_readers``). The
    # gap is the cell's claim on its coordinate (``item_owns_cell``).
    lag_gaps = lag_input_gap_readers(model, source_bundle)
    for item_id, period in lag_gaps:
        if period not in computed_values.get(item_id, {}):
            required.add((item_id, period))
    # The history a role with an authored forecast still owes is the same
    # class: its blank cell is the one root of every reader's non-known cell,
    # required-unavailable so it carries the finding those readers fold into
    # (``owed_history_cells``).
    for item_id, period in owed_history_cells(model, source_bundle):
        if period not in computed_values.get(item_id, {}):
            required.add((item_id, period))
    if predecessor_semantic_document is not None:
        for sv in predecessor_semantic_document.semantic_values:
            if (
                sv.state == "missing"
                and sv.period
                and sv.semantic_item_id in model._index
                and computed_values.get(sv.semantic_item_id, {}).get(int(sv.period)) is None
            ):
                pair = (str(sv.semantic_item_id), int(sv.period))
                required.add(pair)

    cell_gaps = owned_cell_gaps(model, source_bundle)

    predecessor_version = (
        predecessor_semantic_document.model_version
        if predecessor_semantic_document is not None
        else None
    )
    version_label = (
        "Working build (uncommitted)"
        if version_identity is None
        else f"Version {version_identity.version}"
    )
    if version_identity is not None and predecessor_version is not None:
        version_label += f" (successor of Version {predecessor_version})"
    return AnnualRenderContext(
        version_label=version_label,
        version_number=(
            None if version_identity is None else version_identity.version
        ),
        predecessor_version_number=predecessor_version,
        required_unavailable=frozenset(
            (item_id, period)
            for item_id, period in required
            if item_id in model._index
            and item_owns_cell(model, model._index[item_id], period, gaps=cell_gaps)
        ),
    )


def _append_verdict_and_reserialize(
    *,
    model: FinancialModel,
    plan: RenderPlan,
    validation: Any,
    bytes0_annual_presentation: Any,
    semantic_base_document: ModelSemanticDocument | None,
    allocated_ref: ModelVersionRef,
    model_spec: ModelSpec,
    provenance: Any,
    valuation_evidence_status: Any,
    schedule_input_audit: Any,
    render_state: Any,
    seed_coverage_manifest: Sequence[Mapping[str, Any]],
    formatter: ModelFormatter,
    finalized_at: datetime,
) -> tuple[bytes, Any]:
    """Append the Summary verdict block and serialize the published bytes₁.

    The appended writes are literals and no renderer formula references the
    Summary, so bytes₁ needs no recalculation: only an exact readback of the
    appended writes and the annual guard. The guard's blockers on bytes₁ must
    equal those on bytes₀; a difference means the verdict block itself
    tripped presentation, a compiler defect, not a finding.
    """

    from .render_state import render_state_hash

    writes_before = len(plan.writes)
    append_summary_findings(
        plan,
        model,
        validation.status,
        validation.findings,
        formatter,
        subject_items=finding_subject_items(model_spec, seed_coverage_manifest),
    )
    if semantic_base_document is not None:
        from .model_version_compare import compare_model_version_candidate
        from .renderer_summary import append_model_version_comparison

        comparison = compare_model_version_candidate(
            semantic_base_document,
            successor_ref=allocated_ref,
            successor_spec=model_spec,
            semantic_values=semantic_value_snapshots(plan),
            provenance=provenance,
            validation_status=validation.status,
            validation_blocker_count=validation.error_count,
            valuation_evidence_status=valuation_evidence_status,
            schedule_input_audit=schedule_input_audit,
        )
        append_model_version_comparison(
            plan=plan,
            comparison=comparison,
            formatter=formatter,
        )
    finalize_render_contract(plan)
    appended = {
        (write.sheet, str(write.cell).upper()): write.value
        for write in plan.writes[writes_before:]
    }
    workbook_content = render_xlsx_bytes(
        plan,
        finalized_at=finalized_at,
        stamp=ModelStamp(
            model_id=allocated_ref.model_id,
            version=allocated_ref.version,
            build_id=render_state["identity"]["build_id"],
        ),
    )
    mismatches = workbook_readback_mismatches(workbook_content, appended)
    if mismatches:
        sheet, cell, expected, observed = mismatches[0]
        raise RuntimeError(
            f"Summary verdict block read back {len(mismatches)} mismatch(es); "
            f"first {sheet}!{cell}: expected {expected!r}, observed {observed!r}"
        )
    annual_presentation = evaluate_annual_presentation(
        model,
        render_plan=plan,
        cell_readback=_read_annual_presentation_cells(workbook_content),
        workbook_sha256=hashlib.sha256(workbook_content).hexdigest(),
        render_state_hash=render_state_hash(render_state),
        formatter=formatter,
    )
    if tuple(annual_presentation.blockers) != tuple(
        bytes0_annual_presentation.blockers
    ):
        raise RuntimeError(
            "the Summary verdict block changed the annual presentation guard's "
            "blockers; the compiler rendered presentation residue"
        )
    return workbook_content, annual_presentation


def _append_valuation_evidence_status_block(
    plan: RenderPlan,
    status: Any,
) -> None:
    """Render the fixed non-financial valuation evidence surface."""

    from .build_model_product_contracts import (
        ValuationEvidenceStatus,
    )
    from .valuation_evidence_derivations import (
        valuation_evidence_status_public_projection,
    )

    artifact = ValuationEvidenceStatus.model_validate(status)
    public = valuation_evidence_status_public_projection(artifact)
    writes = {
        "D76": "Selected methods",
        "D78": "EV/Adj-EBITDA selected method",
        "D84": "Peer evidence status",
        "D85": "Method",
        "E85": "Status",
        "F85": "Fiscal basis",
        "G85": "Selected multiple",
        "H85": "Evidence detail",
        "D88": "Source bundle",
        "E88": public["bundle_digest"],
    }
    from .thesis import SCENARIO_MULTIPLE_TYPE_LABELS

    for row_number, row in zip((86, 87), public["methods"], strict=True):
        writes.update(
            {
                f"D{row_number}": SCENARIO_MULTIPLE_TYPE_LABELS[row["method"]],
                f"E{row_number}": row["status"].title(),
                f"F{row_number}": str(row["fiscal_basis"]).upper(),
                f"G{row_number}": row["selected_value"],
                f"H{row_number}": row["detail"],
            }
        )
    occupied = {(write.sheet, write.cell) for write in plan.writes}
    collisions = sorted(
        cell for cell in writes if ("Valuation", cell) in occupied
    )
    if collisions:
        raise ValueError(
            f"valuation evidence status block collides with rendered cells: {collisions}"
        )
    plan.writes.extend(
        CellWrite(sheet="Valuation", cell=cell, value=value)
        for cell, value in writes.items()
    )


def _render_compiler_inputs(
    *,
    source_bundle: SourceBundle,
    source_bundle_payload: Mapping[str, Any],
    source_bundle_content_sha256: str,
    model_spec: ModelSpec,
    source_bundle_id: str | None = None,
    source_bundle_identity_digest: str | None = None,
) -> dict[str, object]:
    from .handoff import CompilerInput
    from .render_state import compiler_input_content_hash

    compiler_inputs: dict[str, object] = {
        "source_bundle": CompilerInput(
            ticker=source_bundle.ticker,
            content_hash=f"sha256:{source_bundle_content_sha256}",
            payload=dict(source_bundle_payload),
            bundle_id=source_bundle_id,
            identity_digest=source_bundle_identity_digest,
        ).model_dump(mode="json"),
        "model_spec": CompilerInput(
            ticker=model_spec.ticker,
            content_hash=compiler_input_content_hash(model_spec),
            payload=model_spec.model_dump(mode="json"),
        ).model_dump(mode="json"),
    }
    return compiler_inputs
