"""Compile accepted model inputs into deterministic workbook artifacts.

``compile_model_intermediate`` interprets a ``SourceBundle`` plus ``ModelSpec``
and delegates focused stages to the sibling ``build_*.py`` modules.
``finalize_compiled_model`` binds a caller-allocated model identity to exact
rendered bytes. This module
does not authenticate a mutation or commit durable product state--those are
FMS responsibilities described in ``api/fms/README.md``.

Owner and pipeline guide: ``schema/README.md``.
"""

from __future__ import annotations

import calendar
from collections.abc import Iterable, Sequence
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from dataclasses import dataclass, field
import logging
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, List, Mapping, Optional, cast

from pydantic import BaseModel


from .build_diagnostics import (
    DiagnosticReport,
    run_build_diagnostic,
)
from .build_time_structure import remap_time_structure
from .build_population import (
    populate_historicals,
    source_evidence_by_line,
)
from .build_mbc_entry import (
    _apply_model_spec_assumptions,
)
from .build_historical_driver_seeds import (
    apply_model_spec_historical_driver_seeds,
)
from .build_bm_node_history import (
    apply_business_model_node_history,
    bm_required_series,
    bm_series_coverage_findings,
    opening_balances,
)
from .build_required_lines import (
    apply_required_line_identity,
    derive_required_line_plan,
    required_formula_input_findings,
    required_line_population_findings,
)
from .required_set import derive_required_set
from .build_historical_foundation_facts import (
    apply_model_spec_historical_foundation_facts,
)
from .build_company_grounded_schedules import (
    apply_company_grounded_schedule_policies,
    apply_not_separately_presented_periods,
    sbc_per_line_row_split_undisclosed_findings,
)
from .model_spec_forecast_authority import (
    project_model_spec_forecast_driver_coverage,
)
from .build_formula_first import (
    apply_formula_first,
)
from .build_company_info import (
    update_company_info,
)
from .build_valuation_inputs import (
    normalize_zero_debt_valuation_readiness,
    populate_valuation_inputs,
    suppress_unauthorized_relative_valuation_outputs,
)
from .valuation_schema_invariant import assert_valuation_template_schema
from .versioned_artifact import canonical_json_bytes
from .build_validation_inputs import (
    _make_validation_input,
)
from .build_statement_windows import (
    StatementWindowBuildPlan,
    compile_statement_window_build_plan,
)
from .build_statement_window_validation import (
    statement_window_population_years,
    statement_window_validation_findings,
    validate_statement_window_post_compute,
    validate_statement_window_pre_population,
)
from .annual_presentation_guard import (
  evaluate_annual_presentation,
    read_annual_presentation_cells as _read_annual_presentation_cells,
)
from .build_types import (
    BuildResult,
    PopulateStats,
)
from .build_model_product_contracts import (
    ArtifactLocation,
    BuildDiagnostic,
    HandoffBuildModelVersionOrigin,
    ModelSpec,
    SourceBundle,
    SuccessorModelVersionOrigin,
    decode_source_bundle_payload,
    resolve_model_spec_business_model_revision,
)
from .valuation_evidence_validation import (
    validate_source_bundle_binding_and_selection,
    validate_source_bundle_valuation_evidence,
)
from .build_offline_validation import validate_offline_build_artifacts
from .valuation_evidence_derivations import (
    derive_covered_market_inputs,
    derive_valuation_evidence_status,
)
from .schedule_input_audit import build_schedule_input_audit
from .business_model_revision import BusinessModelRevision
from .build_projection_seeds import (
    compile_candidate_model,
    ControlSnapshotProjectionResult,
)
from .control_snapshot_projection import (
    CanonicalProjectionCurve,
)
from .scenario_bindings import (
    lower_model_spec_direct_scenario_inputs,
    lower_model_spec_scenario_curves,
    partition_model_spec_scenario_workbook_targets,
    scenario_case_gaps,
    scenario_gap_findings,
    validate_scenario_bindings,
)
from .model_kind_dispatch import (
    declared_capabilities,
    declared_control_binding,
    declared_eps_publication_basis,
    declared_grounded_semantic_rows,
    declared_provisional_valuation_defaults,
    declared_scenario_controls,
    declared_schedule_completions,
    declared_semantic_row_materializations,
    declared_sensitivity_bindings,
    declared_share_conversion,
    declared_style_guide,
)
from .build_cash_historicals import (
    _normalize_projected_cash_roll_forward,
    _normalize_projected_balance_sheet_roll_forward,
    _normalize_three_statement_spine,
)
from .build_non_gaap_addbacks import (
    _seed_projected_non_gaap_addbacks,
)
from .build_working_capital_policy import (
    _apply_projected_inventory_policy,
    _complete_working_capital_schedule,
)
from .build_financial_schedules import (
    _complete_financial_schedules,
)
from .assumption_value_semantics import finite_number
from .thesis_shared_slice import Assumption, Valuation
from .build_scenarios import (
    _bind_declared_model_spec_controls,
    _bind_model_spec_control_units,
    _finalize_authenticated_dependents,
    _lower_scenario_case_probabilities,
    _materialize_declared_share_conversion,
    _compute_model_spec_sensitivity_results,
    _populate_scenario_eps,
    _populate_thesis_snapshot,
    _populate_scenario_inputs,
    compute_scenario_outputs,
    scenario_results_payload,
)
from .build_semantic_rows_balance_bridge import (
    BALANCE_BRIDGE_NOTE,
    _materialize_balance_bridges,
)
from .build_semantic_rows import (
    SemanticRowsResult,
    semantic_row_policy_findings,
)
from .build_semantic_rows_tax import _materialize_tax_schedule
from .dependency_graph import (
    DependencyGraph,
    DependencyGraphResults,
    formula_owned_recompute_ids,
)
from .handle import ComputedValues
from .excel_writer import (
    render_xlsx_bytes,
)
from .formatter import ModelFormatter, formatter_for_declared_style_guide
from .model_build_context import MbcDriverSpec
from .model_readiness import (
    ModelProjectionReadiness,
    compute_model_projection_readiness,
)
from .model_readiness_quality import (
    ModelQualityReadiness,
    compute_model_quality_readiness,
)
from .model_readiness_scenario_bridge import (
    ModelScenarioBridgeReadiness,
    compute_model_scenario_bridge_readiness,
)
from .model_readiness_scenario_output import (
    ModelScenarioOutputReadiness,
    compute_model_scenario_output_readiness,
)
from .model_readiness_valuation import ValuationInputReadiness
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
)
from .model_semantics import canonical_workbook_semantics_hash
from .model_ref import ModelStamp, ModelVersionRef
from .model_semantic_document import (
    ModelSemanticDocument,
    decode_model_semantic_document,
    semantic_value_snapshots,
)
from .statement_windows import (
    StatementMappingSourceUnavailableError,
    StatementWindowsResult,
    compile_statement_windows,
    enforce_window_boundaries,
    require_window_source_material,
)
from .statement_topology import (
    StatementTopology,
    build_statement_topology,
    financials_payloads_fingerprint,
)
from .renderer import (
    finalize_render_contract,
    reconcile_render_contract,
    render_model,
)
from .renderer_lineage import append_lineage_index
from .build_findings import (
    CompileStageEvidence,
    business_model_validation_findings,
    make_finding,
)
from .finding_vocabulary import Finding
from .segments import (
    SegmentProfile,
    populate_segment_historicals,
)
from .provider_port.registry import (
    ProviderId,
    ProviderRegistry,
)
from .templates import load_sia_generic_template
from .templates.template_builder import _install_residual_histories
from .validation_input import ValidationInput
from .build_intermediate_seal import (
    _seal_compiled_intermediate,
    _verified_compiled_intermediate,
)
from .build_population import (
    _selected_statement_population_inputs,
    _statement_population_source,
)
from .build_presentation_residue import _resolve_presentation_template_residue
from .renderer_annual_context import (
    _annual_render_context,
    _append_valuation_evidence_status_block,
    _append_verdict_and_reserialize,
    _render_compiler_inputs,
)


_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT = 0.01
_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M = 0.1




def load_template() -> FinancialModel:
    """Load the bundled generic SIA template and build its item index."""

    model = load_sia_generic_template()
    model.build_index()
    return model










def _computed_base_results(model: FinancialModel) -> DependencyGraphResults:
    graph = DependencyGraph()
    graph.build(model)
    return graph.compute({}, recompute=formula_owned_recompute_ids(model))


def _assumption_period_values(
    assumption: Assumption,
    forecast_periods: tuple[int, ...],
) -> dict[int, float] | None:
    """The assumption's curve, or ``None`` when its scalar is not a number.

    A scalar ``value`` spans the forecast axis; ``finite_number`` owns which
    authored scalars are numbers (not ``bool``, ``str`` or non-finite).
    """

    if assumption.value is None:
        return dict(assumption.period_values or {})
    number = finite_number(assumption.value)
    if number is None:
        return None
    return {period: number for period in forecast_periods}


def _model_spec_drivers(
    forecast_assumptions: Sequence[Assumption],
    forecast_periods: tuple[int, ...],
) -> tuple[dict[str, MbcDriverSpec], tuple[Finding, ...]]:
    """The forecast drivers the mechanics takes, and the assumptions it cannot.

    An assumption with no typed value, a scalar value that is not a finite
    number, no unit, a second row for a driver already bound, or a curve short
    of the forecast axis is not bound; it is the ``error`` Finding
    ``forecast_assumption_incomplete`` on its driver.
    """

    drivers: dict[str, MbcDriverSpec] = {}
    findings: list[Finding] = []
    for assumption in forecast_assumptions:
        period_values = _assumption_period_values(assumption, forecast_periods)
        if assumption.value is None and assumption.period_values is None:
            problem = "has no typed value"
        elif period_values is None:
            problem = (
                f"has a {type(assumption.value).__name__} value that is not a "
                "finite number"
            )
        elif assumption.unit is None:
            problem = "has no unit"
        elif assumption.driver in drivers:
            problem = "repeats a driver another assumption already binds"
        elif tuple(sorted(period_values)) != forecast_periods:
            problem = (
                f"covers {sorted(period_values)}, not the forecast axis "
                f"{list(forecast_periods)}"
            )
        else:
            drivers[assumption.driver] = MbcDriverSpec(
                assumption_id=assumption.assumption_id,
                period_values=period_values,
                unit=assumption.unit,
                periods=list(forecast_periods),
                rationale=assumption.rationale,
                confidence=assumption.confidence,
            )
            continue
        findings.append(
            make_finding(
                "forecast_assumption_incomplete",
                message=(
                    f"forecast assumption {assumption.assumption_id!r} for driver "
                    f"{assumption.driver!r} {problem}, so the model does not "
                    "take it; repair the assumption in the forecast-assumption set"
                ),
                item_id=assumption.driver,
                observed={"assumption_id": assumption.assumption_id},
            )
        )
    return drivers, tuple(findings)


def _merge_model_spec_valuation_controls(
    controls: tuple[Valuation, ...],
) -> Valuation | None:
    """Project compatible authored controls into one compiler input."""

    if not controls:
        return None
    merged: dict[str, Any] = {}
    methods: list[str] = []
    rationales: list[str] = []
    source_refs: list[Any] = []
    for control in controls:
        if control.method is not None:
            methods.append(control.method)
        if control.rationale:
            rationales.append(control.rationale)
        source_refs.extend(control.source_refs)
        for field_name, value in control.model_dump(
            mode="python",
            exclude_none=True,
            exclude={"method", "rationale", "source_refs"},
        ).items():
            previous = merged.get(field_name)
            if previous is not None and previous != value:
                raise ValueError(
                    "incomplete_model_spec: conflicting valuation controls "
                    f"for {field_name}"
                )
            merged[field_name] = value
    if methods:
        merged["method"] = "dcf" if "dcf" in methods else methods[0]
    if rationales:
        merged["rationale"] = "; ".join(dict.fromkeys(rationales))
    if source_refs:
        merged["source_refs"] = list(dict.fromkeys(source_refs))
    return Valuation.model_validate(merged)


@dataclass(frozen=True, slots=True)
class CompiledModelIntermediate:
    """Sealed, ref-independent output of deterministic model compilation."""

    content_digest: str
    source_bundle: SourceBundle
    model_spec: ModelSpec
    model: FinancialModel
    typed_analysis_values: dict[str, dict[int, Any]]
    computed_values: ComputedValues
    projection_readiness: ModelProjectionReadiness
    model_quality_readiness: ModelQualityReadiness
    scenario_results: dict[str, object]
    valuation_results: dict[str, object]
    schedule_input_audit: Any
    valuation_evidence_status: Any
    model_spec_valuation: Valuation | None
    seed_coverage_manifest: Sequence[Mapping[str, Any]]
    #: Compile failures the company-grounded schedule policies report at
    #: their own rows; validation reports them beside the source diagnostics.
    schedule_policy_diagnostics: tuple[BuildDiagnostic, ...]
    #: What the compile reports instead of raising (Finding rows of the one
    #: severity map): business-model series coverage (R2) and node history,
    #: required lines, statement windows and forecast assumptions. Validation
    #: reports them with the other compile-stage findings.
    compile_findings: tuple[Finding, ...]
    #: The bound judgment artifacts' revision findings (R7), computed by the
    #: build lifecycle (``judgment_revision_findings``); validation reports
    #: them with the compile findings.
    judgment_findings: tuple[Finding, ...]
    canonical_control_curves: tuple[CanonicalProjectionCurve, ...]
    direct_scenario_control_ids: tuple[str, ...]
    ticker: str
    stats: PopulateStats
    diagnostic: DiagnosticReport | None
    segment_profile: SegmentProfile | None
    compiled_registry: Any | None
    derivable_items: dict[str, set[int]]
    overrides_applied_count: int
    control_snapshot_projection: ControlSnapshotProjectionResult
    semantic_rows_result: SemanticRowsResult
    statement_windows_result: StatementWindowsResult
    scenario_output_readiness: ModelScenarioOutputReadiness
    scenario_bridge_readiness: ModelScenarioBridgeReadiness
    sensitivity_results: dict[str, object]
    valuation_input_readiness: ValuationInputReadiness
    provider_registry: ProviderRegistry | None
    scenario_source_records: Sequence[Mapping[str, Any]] = ()
    evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] = field(default_factory=dict)
    #: Compiled lines of the BusinessModel nodes whose history is unresolved
    #: (``BmNodeHistoryResult.unresolved_items``): their ``business_model_node_unresolved``
    #: finding is the one record of the missing history, and finalization stamps their
    #: unwritten historical cells with that typed reason.
    unresolved_node_items: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            len(self.content_digest) != 64
            or self.content_digest != self.content_digest.lower()
            or any(
                character not in "0123456789abcdef"
                for character in self.content_digest
            )
        ):
            raise ValueError("intermediate content_digest must be lowercase sha256 hex")


@dataclass(frozen=True, slots=True)
class FinalizedArtifactPayload:
    location: str
    sha256: str
    content: bytes

    def __post_init__(self) -> None:
        if not self.location or self.location != self.location.strip():
            raise ValueError("artifact location must be canonical nonempty text")
        # Data integrity: the publisher accepts an already-stored file by this
        # digest alone while it validates ``content`` in memory
        # (``_publish_immutable_artifact``/``_validate_finalized_artifacts`` in
        # ``api/research/normal_handoff_model_publisher.py``), so a digest that is
        # not this content's would commit a ModelVersion whose validated bytes
        # are not its stored bytes. No agent value reaches it (``payload`` in
        # ``finalize_compiled_model`` derives the digest); its next call is
        # ``finalize_compiled_model`` again from the same sealed intermediate.
        if hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise ValueError("artifact digest must match exact content bytes")

    @property
    def artifact_location(self) -> ArtifactLocation:
        return ArtifactLocation(location=self.location, sha256=self.sha256)


BuildFinalizationOrigin = (
    HandoffBuildModelVersionOrigin | SuccessorModelVersionOrigin
)


@dataclass(frozen=True, slots=True)
class FinalizedBuildArtifacts:
    intermediate_digest: str
    allocated_ref: ModelVersionRef
    sequence_predecessor: ModelVersionRef | None
    semantic_base: ModelVersionRef | None
    origin: BuildFinalizationOrigin
    source_bundle_id: str
    source_bundle_identity_digest: str
    finalized_at: datetime
    workbook: FinalizedArtifactPayload
    semantic_sidecar: FinalizedArtifactPayload
    provenance_map: FinalizedArtifactPayload
    validation_report: FinalizedArtifactPayload
    build_result: BuildResult
    # Not artifact bytes: what finalization computed on the way, kept out of
    # ``files`` so content-addressed bytes stay byte-identical across calls.
    # ``recalculated_values`` is the external recalculation's numeric semantic
    # cells (item id -> period -> value); ``stage_seconds`` is wall time for
    # ``render``, ``provenance``, ``recalc`` and ``validation`` (recalc
    # included); ``recalculation_bytes`` is the workbook bytes the
    # recalculation worker read (``input``) and the output bytes it wrote
    # (``output``).
    recalculated_values: dict[str, dict[int, float]]
    stage_seconds: dict[str, float]
    recalculation_bytes: dict[str, int]

    @property
    def files(self) -> tuple[FinalizedArtifactPayload, ...]:
        return (
            self.workbook,
            self.semantic_sidecar,
            self.provenance_map,
            self.validation_report,
        )


def compile_model_intermediate(
    source_bundle: SourceBundle,
    model_spec: ModelSpec,
    *,
    business_model_revision: BusinessModelRevision,
    validation_mode: bool = False,
    run_diagnostics: bool = False,
    provider_registry: ProviderRegistry | None = None,
    judgment_findings: Sequence[Finding] = (),
) -> CompiledModelIntermediate:
    """Compile one canonical SourceBundle and ModelSpec without rendering."""

    source_bundle = (
        source_bundle
        if isinstance(source_bundle, SourceBundle)
        else decode_source_bundle_payload(source_bundle)
    )
    model_spec = ModelSpec.model_validate(model_spec)
    validate_source_bundle_valuation_evidence(source_bundle)
    validate_source_bundle_binding_and_selection(source_bundle, model_spec)
    # Read capabilities once from the declared compile inputs so every stage
    # dispatches from the same authority. Absence is feature-off: a bundle
    # without a capability record stays on the generic arm throughout.
    capabilities = declared_capabilities(source_bundle)
    grounded_semantic_rows = declared_grounded_semantic_rows(capabilities)
    semantic_row_materializations = declared_semantic_row_materializations(
        capabilities
    )
    sensitivity_bindings = declared_sensitivity_bindings(capabilities)
    scenario_controls = declared_scenario_controls(capabilities)
    share_conversion = declared_share_conversion(capabilities)
    schedule_completions = declared_schedule_completions(capabilities)
    control_binding = declared_control_binding(capabilities)
    eps_publication_basis = declared_eps_publication_basis(capabilities)
    provisional_valuation_defaults = declared_provisional_valuation_defaults(
        capabilities
    )
    validate_scenario_bindings(model_spec, scenario_controls=scenario_controls)
    scenario_gaps = scenario_case_gaps(model_spec)
    unresolved_cases = frozenset(gap.scenario for gap in scenario_gaps)
    pinned_market_inputs = derive_covered_market_inputs(
        source_bundle.valuation_observations,
        source_bundle.cutoff_at,
        source_bundle.market_calendar,
        source_bundle.fiscal_calendar,
        requirements=source_bundle.valuation_requirements,
        focal_ticker=source_bundle.coverage_profile.focal_ticker,
        sources=source_bundle.sources,
        retrieved_at=source_bundle.retrieved_at,
    )
    valuation_evidence_status = derive_valuation_evidence_status(
        source_bundle,
        None
        if model_spec.valuation_policy is None
        else model_spec.valuation_policy.peer_policy,
    )
    business_model_revision = resolve_model_spec_business_model_revision(
        model_spec,
        business_model_revision,
    )
    if source_bundle.ticker != model_spec.ticker:
        raise ValueError("SourceBundle ticker must equal ModelSpec ticker")
    historical_axis = model_spec.fiscal_axis.historical_periods
    forecast_axis = model_spec.fiscal_axis.forecast_periods
    ticker = model_spec.ticker
    company_name = business_model_revision.business_model.company.name
    most_recent_fy = historical_axis[-1].fiscal_year
    n_historical = len(historical_axis)
    n_projection = len(forecast_axis)
    latest_period_end = max(
        (
            period.period_end
            for period in source_bundle.reported_periods
            if period.fiscal_year == most_recent_fy
        ),
        default=None,
    )
    fiscal_year_end = (
        calendar.month_name[latest_period_end.month]
        if latest_period_end is not None
        else "June"
    )
    business_model = business_model_revision.business_model
    sector = business_model.company.sector
    source: ProviderId | None
    canonical_control_curves: tuple[CanonicalProjectionCurve, ...] = ()
    scenario_compute_inputs = tuple(
        scenario for scenario in model_spec.scenarios
        if scenario.name not in unresolved_cases
    )
    direct_scenario_control_ids: tuple[str, ...] = ()
    # Scenario-curve lowering is authorized only by the model kind's declared
    # workbook targets. A kind that declares none resolves selectors from the
    # artifact path below instead.
    if scenario_controls is not None and scenario_controls.workbook_targets:
        canonical_control_curves = lower_model_spec_scenario_curves(
            model_spec,
            workbook_targets=scenario_controls.workbook_target_map(),
            scenario_controls=scenario_controls,
        )
    model_spec_drivers, forecast_assumption_findings = _model_spec_drivers(
        model_spec.forecast_assumptions,
        tuple(int(period.fiscal_year) for period in forecast_axis),
    )
    #: What the compile reports instead of raising; validation reads it as
    #: compile-stage findings (`CompiledModelIntermediate.compile_findings`).
    compile_findings: list[Finding] = [
        *forecast_assumption_findings,
        *scenario_gap_findings(model_spec),
    ]
    model = load_template()
    profile: Optional[SegmentProfile] = None

    registry = provider_registry

    from .business_model_compiler import (
        bind_driver_assumption_plan,
        compile_business_model,
    )

    compiled_registry = bind_driver_assumption_plan(
        compile_business_model(model, business_model),
        business_model_revision.driver_assumption_plan,
    )
    profile = compiled_registry.segment_profile
    remap_time_structure(
        model,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        n_projection=n_projection,
    )

    statement_source_material = source_bundle.statement_source_material
    persisted_statement_windows = (
        {}
        if statement_source_material is None
        else statement_source_material.windows_by_id
    )
    persisted_semantic_rows = {}
    from .render_state import compiler_input_content_hash
    source_store_hash = compiler_input_content_hash(model_spec)

    statement_compilation = compile_statement_windows(
        ticker=ticker,
        statement_windows=persisted_statement_windows,
        existing_semantic_rows=persisted_semantic_rows,
        source_store_hash=source_store_hash,
        provider_registry=registry,
    )
    window_topology: StatementTopology | None = None
    boundary_resolutions: tuple[dict[str, Any], ...] = ()
    if statement_compilation.section_plans:
        assert statement_source_material is not None
        required_years = tuple(
            range(most_recent_fy - n_historical + 1, most_recent_fy + 1)
        )
        frozen_financials_payloads = statement_source_material.frozen_payloads
        payload_years = set(frozen_financials_payloads)
        require_window_source_material(
            ticker=ticker,
            section_plans=statement_compilation.section_plans,
            payload_years=payload_years,
            requested_years=required_years,
        )
        raw_payload_fingerprint = financials_payloads_fingerprint(
            frozen_financials_payloads
        )
        if statement_source_material.payloads_fingerprint != raw_payload_fingerprint:
            raise StatementMappingSourceUnavailableError(
                "Pinned statement source material and its payload snapshot do "
                "not share the same content fingerprint"
            )
        window_topology = build_statement_topology(frozen_financials_payloads)
        if window_topology.source_payloads_fingerprint != raw_payload_fingerprint:
            raise StatementMappingSourceUnavailableError(
                "Statement topology and immutable raw financials payload snapshot "
                "do not share the same content fingerprint"
            )
        # Every stored PresentationLineRef the boundary pass reads is re-minted
        # against these trees; a ref the pin does not reproduce is a typed
        # refusal, never a resolved boundary.
        pinned_topologies = {
            int(year): window_topology for year in frozen_financials_payloads
        }
        for window_id, plan in sorted(statement_compilation.section_plans.items()):
            boundary_outcome = enforce_window_boundaries(
                plan.window,
                pinned_topologies,
                current_links=statement_source_material.correspondence_links,
                current_breaks=statement_source_material.declared_breaks,
                requested_years=required_years,
            )
            boundary_resolutions += boundary_outcome.resolutions
            compile_findings.extend(boundary_outcome.findings)
        statement_compilation.report.boundary_resolutions = [
            dict(row) for row in boundary_resolutions
        ]
    # BM-node history binds through the identity channel below rather than
    # minting custom concepts or accepting inline values. A pinned disclosed
    # series claims identity via `kpi_source`, ratio nodes use registered
    # formulas, and anything else is `business_model_node_unresolved`.
    overrides_applied_count = 0
    statement_build_plan: StatementWindowBuildPlan | None = None
    statement_windows_result = StatementWindowsResult()
    if statement_compilation.section_plans:
        assert window_topology is not None
        statement_build_plan = compile_statement_window_build_plan(
            ticker=ticker,
            model=model,
            compiled_statement_windows=statement_compilation,
        )
        model = statement_build_plan.prepared_model
        compile_findings.extend(statement_build_plan.findings)
        compile_findings.extend(
            semantic_row_policy_findings(
                statement_build_plan.materialization["materialized"]
            )
        )
        # The window build plan replaces the FinancialModel object with a
        # derivation of the already-compiled model, so the registry's id maps
        # remain valid but its model witness goes stale (the node-history
        # identity check refuses exactly that). Re-witness rather than
        # recompile: the first compile consumed the template prototypes, so a
        # second compile_business_model over the prepared model would refuse
        # on the missing prototype rows.
        compiled_registry.validation_model = model
        statement_windows_result = validate_statement_window_pre_population(
            compiled_statement_windows=statement_compilation,
            build_plan=statement_build_plan,
            statement_topology=window_topology,
        )

    required_set = derive_required_set(
        source_bundle.requirements,
        business_model_revision,
        tuple(
            f"FY{int(period)}"
            for period in model.time_structure.historical_periods
        ),
    )
    required_line_plan = derive_required_line_plan(
        required_set,
        source_bundle.coverage,
    )
    statement_population_eligible_years = (
        statement_window_population_years(
            compiled_statement_windows=statement_compilation,
            build_plan=statement_build_plan,
            pre_result=statement_windows_result,
        )
        if statement_build_plan is not None
        else None
    )

    statement_population_inputs = _selected_statement_population_inputs(
        source_bundle
    )
    evidence_by_line = source_evidence_by_line(source_bundle.evidence)
    source, source_findings = _statement_population_source(
        statement_population_inputs
    )
    compile_findings.extend(source_findings)
    stats = populate_historicals(
        model,
        source=source,
        ticker=ticker,
        statement_facts=statement_population_inputs,
        reported_periods=source_bundle.reported_periods,
        required_line_plan=required_line_plan,
        evidence_by_line=evidence_by_line,
    )
    semantic_rows_result = SemanticRowsResult()
    if profile and profile.segments:
        historical_periods = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))
        populate_segment_historicals(model, profile, historical_periods)
    model_spec_seed_coverage: list[dict[str, Any]] = []
    artifact_forecast_coverage_by_driver_id: dict[str, tuple[int, ...]] = {}
    # Populate required statement and schedule lines by pinned identity before
    # projected assumptions, schedule completion, and the HFF fact pass consume
    # them. BM-node history uses the same identity/formula/refusal channel, so
    # there is no parallel authored join or anchor map.
    compile_findings.extend(apply_required_line_identity(
        model,
        observations=(
            *statement_population_inputs,
            *source_bundle.kpi_observations,
        ),
        plan=required_line_plan,
        required_set=required_set,
        coverage=source_bundle.coverage,
        evidence_by_line=evidence_by_line,
        ticker=ticker,
        historical_periods=[
            int(period) for period in model.time_structure.historical_periods
        ],
    ))
    pin_opening_balances = opening_balances(
        source_bundle,
        first_historical_year=min(
            int(period) for period in model.time_structure.historical_periods
        ),
    )
    # A statement residual's history closes against the filed statement values the identity
    # channel just wrote; it is statement construction, never the node history's.
    _install_residual_histories(model)
    node_history = apply_business_model_node_history(
        model,
        source_bundle,
        business_model_revision=business_model_revision,
        compiled_registry=compiled_registry,
        required_set=required_set,
        opening_balances=pin_opening_balances,
    )
    model_spec_seed_coverage.extend(node_history.seed_coverage)
    compile_findings.extend(node_history.findings)
    compile_findings.extend(bm_series_coverage_findings(
        business_model_revision.business_model,
        required=bm_required_series(required_set.rows),
    ))
    compile_findings.extend(
        business_model_validation_findings(business_model_revision)
    )
    # Accepted HFF seeds are the primary authority for historical opening values
    # required by exact BusinessModel self[-1] growth nodes, with cited
    # BusinessModel historical_observations filling remaining unseeded historical
    # years on compiled node and direct-segment lines.
    model_spec_seed_coverage.extend(
        apply_model_spec_historical_driver_seeds(
            model,
            model_spec,
            business_model_revision=business_model_revision,
            compiled_registry=compiled_registry,
        )
    )
    financial_schedule_result = None
    working_capital_schedule_result = None
    if model_spec_drivers:
        # Complete declared strict-schedule targets first so every row owner
        # exists at the single assumption-lowering boundary; the declared
        # control-binding mode then selects the binding pass.
        if "financial_schedules" in schedule_completions:
            financial_schedule_result = _complete_financial_schedules(model)
        if "working_capital" in schedule_completions:
            working_capital_schedule_result = _complete_working_capital_schedule(
                model
            )
        if control_binding == "registry_bound":
            _bind_declared_model_spec_controls(
                model,
                compiled_registry,
                forecast_control_targets=(
                    {}
                    if scenario_controls is None
                    else scenario_controls.forecast_control_target_map()
                ),
                semantic_rows=semantic_row_materializations,
                grounded_semantic_rows=grounded_semantic_rows,
            )
        else:
            _bind_model_spec_control_units(model)
        applied_forecast_coverage = _apply_model_spec_assumptions(
            model,
            model_spec_drivers,
            compiled_registry,
            business_model,
            equation_owned_controls=(
                {}
                if share_conversion is None
                else share_conversion.equation_owned_control_map()
            ),
        )
        model_spec_seed_coverage.extend(applied_forecast_coverage)
        artifact_forecast_coverage_by_driver_id = (
            project_model_spec_forecast_driver_coverage(
                model_spec,
                business_model_revision,
                compiled_registry=compiled_registry,
                model_spec_seed_coverage=applied_forecast_coverage,
            )
        )
        # A4: the declared working-capital completion re-runs after the
        # binding pass rewired its rows.  A model kind that declares no
        # workbook targets instead resolves its scenario selectors from the
        # artifact, through the compiled driver plan.
        if "working_capital" in schedule_completions:
            working_capital_schedule_result = _complete_working_capital_schedule(
                model
            )
        if scenario_controls is None or not scenario_controls.workbook_targets:
            canonical_scenario_targets, direct_scenario_targets = (
                partition_model_spec_scenario_workbook_targets(
                    model_spec,
                    model=model,
                    compiled_registry=compiled_registry,
                )
            )
            canonical_control_curves = lower_model_spec_scenario_curves(
                model_spec,
                workbook_targets=canonical_scenario_targets,
                allow_direct_targets=True,
            )
            scenario_compute_inputs = lower_model_spec_direct_scenario_inputs(
                model_spec,
                direct_targets=direct_scenario_targets,
            )
            direct_scenario_control_ids = tuple(sorted(direct_scenario_targets))
    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    _normalize_projected_cash_roll_forward(model)
    _normalize_projected_balance_sheet_roll_forward(model)
    _normalize_three_statement_spine(model)
    if financial_schedule_result is None:
        financial_schedule_result = _complete_financial_schedules(model)
    if working_capital_schedule_result is None:
        working_capital_schedule_result = _complete_working_capital_schedule(model)
    # The legacy SourceBundle can omit exact schedule disclosures that are
    # retained by the accepted HFF.  Schedule completion above materializes
    # every code-owned target; reconcile and apply the HFF facts before the
    # forward formula pass derives rates and openings from them.
    model_spec_seed_coverage.extend(
        apply_model_spec_historical_foundation_facts(model, model_spec)
    )
    tax_schedule_result = _materialize_tax_schedule(model)
    for item_id in tax_schedule_result.get("materialized", []):
        logging.info("Materialized canonical tax schedule row %s", item_id)

    apply_not_separately_presented_periods(model, required_set.rows)
    from .build_financial_schedules import apply_disclosed_interest_bridge
    apply_disclosed_interest_bridge(model, source_bundle.historical_basis_facts)
    # A pinned filing subtotal its template children do not reproduce keeps
    # the filed value and is reported here as a Finding, never a raise.
    derivable_items = apply_formula_first(
        model,
        historical_periods,
        findings=compile_findings,
    )
    semantic_rows_result = _materialize_balance_bridges(
        model,
        ticker,
        historical_periods,
        semantic_rows_result,
        business_model=business_model,
    )
    compile_findings.extend(
        semantic_row_policy_findings(semantic_rows_result.materialized)
    )
    # The subtotal bridge can create the stable client-funds obligation row
    # mid-compile, so identity fill runs again here and overwrites any
    # bridge-derived residual with the exact pinned observation. A reported
    # balance must never be relabeled as a compiler-derived subtotal residual.
    # The required-line pass then reports required_column_absent_from_pin
    # for any pin-supported required line + period that no channel
    # populated; a registered template formula riding an unpopulated
    # required input is formula_input_missing. Both are Findings.
    compile_findings.extend(apply_required_line_identity(
        model,
        observations=(
            *statement_population_inputs,
            *source_bundle.kpi_observations,
        ),
        plan=required_line_plan,
        required_set=required_set,
        coverage=source_bundle.coverage,
        evidence_by_line=evidence_by_line,
        ticker=ticker,
        historical_periods=historical_periods,
        overwrite_notes=frozenset(
            {BALANCE_BRIDGE_NOTE}
        ),
    ))
    compile_findings.extend(required_line_population_findings(
        model,
        plan=required_line_plan,
        ticker=ticker,
        historical_periods=historical_periods,
        formula_satisfied=derivable_items,
        population_eligible_years=statement_population_eligible_years,
    ))
    compile_findings.extend(required_formula_input_findings(
        model,
        plan=required_line_plan,
        historical_periods=historical_periods,
        formula_satisfied=derivable_items,
        population_eligible_years=statement_population_eligible_years,
    ))
    update_company_info(
        model,
        ticker=ticker,
        name=company_name,
        fye=fiscal_year_end,
        sector=sector,
        created_at=None,
    )
    model.build_index()
    model_spec_valuation = _merge_model_spec_valuation_controls(
        model_spec.valuation_controls
    )
    from .build_valuation_inputs import retire_unused_cost_of_debt
    from .required_role_catalog import load_required_role_catalog
    from .role_disposition import items_by_role, resolve_build_dispositions
    role_items = items_by_role(
        load_required_role_catalog(), ticker=str(model.company.ticker)
    )
    model.build_index()
    role_dispositions = resolve_build_dispositions(
        source_bundle.coverage_profile.role_dispositions,
        model,
        role_items,
        projected=tuple(model.time_structure.projection_periods),
    )
    # Pass order, identical to model_build.target_build.construct_document:
    #   1. apply_company_grounded_schedule_policies
    #   2. populate_valuation_inputs
    #   3. suppress_unauthorized_relative_valuation_outputs (when a valuation policy binding is declared)
    #   4. retire_unused_cost_of_debt
    company_grounded_schedule_result = apply_company_grounded_schedule_policies(
        model,
        business_model_revision=business_model_revision,
        compiled_registry=compiled_registry,
        historical_basis_facts=source_bundle.historical_basis_facts,
        grounded_semantic_rows=grounded_semantic_rows,
        opening_balances=pin_opening_balances,
        role_dispositions=role_dispositions,
        role_items=role_items,
    )
    valuation_input_readiness = populate_valuation_inputs(
        model,
        valuation=model_spec_valuation,
        pinned_market_inputs=pinned_market_inputs,
        valuation_policy=model_spec.valuation_policy,
        valuation_family_authorizations=(
            model_spec.valuation_family_authorizations
        ),
    )
    if model_spec.valuation_policy_binding is not None:
        suppress_unauthorized_relative_valuation_outputs(
            model,
            valuation_policy=model_spec.valuation_policy,
            valuation_family_authorizations=(
                model_spec.valuation_family_authorizations
            ),
        )
    retire_unused_cost_of_debt(
        model, valuation_input_readiness, role_dispositions=role_dispositions
    )
    for item_id in company_grounded_schedule_result.get("wired", ()):
        logging.info("Applied company-grounded schedule policy for %s", item_id)
    model_spec_seed_coverage.extend(
        dict(row)
        for row in company_grounded_schedule_result.get(
            "source_fact_coverage", ()
        )
        if isinstance(row, dict)
    )
    # Repurchase conversion and provisional valuation anchors are enabled only
    # when the model kind declares them.
    if share_conversion is not None:
        _materialize_declared_share_conversion(
            model, pinned_market_inputs, share_conversion
        )
    if provisional_valuation_defaults:
        first_projection_period = int(model.time_structure.projection_periods[0])
        for declared_default in provisional_valuation_defaults:
            anchor = model.get_item(declared_default.anchor_item_id)
            anchor.unit = declared_default.unit
            anchor.projected = FormulaSpec(
                type=FormulaType.constant,
                params={"value": declared_default.value},
                note=(
                    "provisional_valuation_default_requires_user_review"
                ),
            )
            anchor.formula_periods = [first_projection_period]
    candidate_compile = compile_candidate_model(
        model,
        None,
        canonical_control_curves,
        compiled_registry,
        most_recent_fy,
    )
    model = candidate_compile.model
    control_snapshot_projection = candidate_compile.control_snapshot_projection
    valuation_input_readiness = normalize_zero_debt_valuation_readiness(
        model,
        candidate_compile.computed_values,
        valuation_input_readiness,
    )
    compile_findings.extend(
        sbc_per_line_row_split_undisclosed_findings(
            model, candidate_compile.computed_values
        )
    )
    # Finalize the declared software and historical presentation links.
    authenticated_dependents = (
        None if share_conversion is None else share_conversion.authenticated_dependents
    )
    if authenticated_dependents is not None:
        _finalize_authenticated_dependents(
            model,
            grounded_semantic_rows=grounded_semantic_rows,
        )
    _lower_scenario_case_probabilities(model, model_spec.scenario_set_binding)
    # seed_coverage_manifest is assembled after scenario EPS population below.
    non_gaap_addback_seed_result = _seed_projected_non_gaap_addbacks(model)
    for seeded_item_id in non_gaap_addback_seed_result.get("seeded", []):
        logging.info("Seeded projected non-GAAP add-back fallback for %s", seeded_item_id)
    for skipped in non_gaap_addback_seed_result.get("skipped", []):
        logging.warning(
            "Skipped projected non-GAAP add-back fallback for %s: %s",
            skipped.get("item_id"),
            skipped.get("reason"),
        )
    inventory_policy_result = _apply_projected_inventory_policy(model)
    for seeded_item_id in inventory_policy_result.get("seeded", []):
        logging.info("Seeded projected inventory policy fallback for %s", seeded_item_id)
    for extended_item_id in inventory_policy_result.get("extended", []):
        logging.info("Extended projected inventory policy horizon for %s", extended_item_id)
    for skipped in inventory_policy_result.get("skipped", []):
        logging.warning(
            "Skipped projected inventory policy repair for %s: %s",
            skipped.get("item_id"),
            skipped.get("reason"),
        )
    if any(curve.domain == "scenario" for curve in canonical_control_curves):
        _populate_scenario_inputs(
            model,
            scenario_compute_inputs,
            authenticated_scenario_curves=True,
        )
    else:
        _populate_scenario_inputs(model, scenario_compute_inputs)
    scenario_output_values, scenario_output_item_ids = compute_scenario_outputs(
        model, unresolved_cases=unresolved_cases,
    )
    scenario_source_records = _populate_scenario_eps(
        model,
        {
            case: dict(
                outputs.get(eps_publication_basis, {})
            )
            for case, outputs in scenario_output_values.items()
        },
        scenario_output_item_ids.get("base", {}).get(eps_publication_basis),
        existing_seed_coverage=model_spec_seed_coverage,
        base_eval_reads=getattr(scenario_output_values, "base_eval_reads", None),
    )
    seed_coverage_manifest = _build_seed_coverage_manifest(
        model,
        business_model=business_model,
        compiled_registry=compiled_registry,
        model_spec_seed_coverage=model_spec_seed_coverage,
        projection_seed_coverage=control_snapshot_projection.coverage,
    )
    _populate_thesis_snapshot(model, scenario_output_values)
    scenario_results = scenario_results_payload(scenario_output_values, unresolved_cases)
    model.build_index()
    computed_values = _computed_base_results(model)
    sensitivity_results = _compute_model_spec_sensitivity_results(
        model,
        model_spec,
        compiled_registry,
        scenario_controls=scenario_controls,
        sensitivity_bindings=sensitivity_bindings,
    )
    valuation_result_item_ids = ["tpl.v.dcf.equity_value_per_share"]
    if model_spec.valuation_policy_binding is None:
        valuation_result_item_ids.extend(
            (
                "tpl.v.forward_pe.method_price",
                "tpl.v.forward_ev_ebitda.method_price",
                "tpl.v.blended_target.blended_price",
            )
        )
    else:
        authorized_methods = {
            item.method for item in model_spec.valuation_family_authorizations
        }
        if "forward_pe" in authorized_methods:
            valuation_result_item_ids.append("tpl.v.forward_pe.method_price")
        if "ev_adjusted_ebitda" in authorized_methods:
            valuation_result_item_ids.append(
                "tpl.v.forward_ev_ebitda.method_price"
            )
    if statement_build_plan is not None:
        assert window_topology is not None
        statement_windows_result = validate_statement_window_post_compute(
            model,
            compiled_statement_windows=statement_compilation,
            build_plan=statement_build_plan,
            statement_topology=window_topology,
            stats=stats,
            computed_values=computed_values,
            pre_result=statement_windows_result,
        )
        compile_findings.extend(
            statement_window_validation_findings(statement_windows_result)
        )
    projection_readiness = compute_model_projection_readiness(
        model,
        compiled_registry=compiled_registry,
        segment_profile=profile if compiled_registry is None else None,
        canonical_control_curves=tuple(
            curve
            for curve in canonical_control_curves
            if curve.domain == "forecast"
        ),
        artifact_forecast_coverage_by_driver_id=(
            artifact_forecast_coverage_by_driver_id
        ),
        computed_values=computed_values,
        guard_config=None,
        driver_assumption_plan=business_model_revision.driver_assumption_plan,
    )
    scenario_output_readiness = compute_model_scenario_output_readiness(
        model,
        computed_values=computed_values,
    )
    scenario_bridge_readiness = compute_model_scenario_bridge_readiness(model)
    model_quality_readiness = compute_model_quality_readiness(
        model,
        computed_values=computed_values,
        valuation_input_readiness=valuation_input_readiness,
        segment_profile=profile,
        guard_config=None,
    )
    diagnostic: DiagnosticReport | None = None
    validation_input: ValidationInput | None = None
    if validation_mode:
        validation_input = _make_validation_input(
            statement_facts=source_bundle.statement_facts,
            required_line_plan=required_line_plan,
            historical_periods=historical_periods,
            served_source_by_role_year=stats.served_source_by_role_year,
        )
    should_run_diagnostics = source == "edgar" or validation_mode or run_diagnostics
    if should_run_diagnostics:
        try:
            diagnostic = run_build_diagnostic(
                model,
                ticker=ticker,
                fy=most_recent_fy,
                stats=stats,
                derivable_items=derivable_items,
                validation_input=validation_input,
                provider_registry=registry,
                segment_profile=profile,
                generated_at=source_bundle.retrieved_at.astimezone(
                    timezone.utc
                ).isoformat(),
            )
        except Exception:
            logging.exception(
                "Build diagnostic failed for %s fy=%s",
                ticker,
                most_recent_fy,
            )
    _resolve_presentation_template_residue(
        model,
        ticker=ticker,
        composition_slots={
            req.model_item: req.required_role
            for req in source_bundle.requirements
            if req.family == "operating" and req.model_item != req.required_role
        },
    )
    # One final immutable typed matrix, after every schedule/policy/scenario
    # mutation. Valuation extraction and schedule auditing share this pass.
    analysis_graph = DependencyGraph()
    analysis_graph.build(model)
    typed_analysis_values = analysis_graph.compute_typed({})
    valuation_results = {}
    for item_id in valuation_result_item_ids:
        if item_id not in model._index:
            continue
        valuation_results[item_id] = {
            period: {
                "state": value.state,
                "value": value.value if value.state == "known" else None,
                "reason": getattr(value, "primary_reason", None),
            }
            for period, value in typed_analysis_values.get(item_id, {}).items()
        }
    schedule_input_audit = build_schedule_input_audit(
        model=model,
        typed_values=typed_analysis_values,
        projection_periods=tuple(int(value) for value in model.time_structure.projection_periods),
        working_capital_required=tuple(
            item_id
            for item_id in ("tpl.fm.balance_sheet.current_asset_3",)
            if item_id in model._index
        ),
    )
    for row in schedule_input_audit.rows:
        if row.state != "missing":
            continue
        prefix = "Working-capital" if row.family == "working_capital" else "Financial"
        logging.warning(
            "%s schedule dependency remains Missing: %s",
            prefix,
            row.row_id,
        )
    return _seal_compiled_intermediate(CompiledModelIntermediate(
        content_digest="0" * 64,
        source_bundle=source_bundle,
        model_spec=model_spec,
        model=model,
        typed_analysis_values=typed_analysis_values,
        computed_values=computed_values,
        projection_readiness=projection_readiness,
        model_quality_readiness=model_quality_readiness,
        scenario_results=scenario_results,
        valuation_results=valuation_results,
        schedule_input_audit=schedule_input_audit,
        valuation_evidence_status=valuation_evidence_status,
        model_spec_valuation=model_spec_valuation,
        seed_coverage_manifest=seed_coverage_manifest,
        schedule_policy_diagnostics=cast(
            tuple[BuildDiagnostic, ...],
            company_grounded_schedule_result["diagnostics"],
        ),
        compile_findings=tuple(compile_findings),
        judgment_findings=tuple(judgment_findings),
        canonical_control_curves=canonical_control_curves,
        direct_scenario_control_ids=direct_scenario_control_ids,
        ticker=ticker,
        stats=stats,
        diagnostic=diagnostic,
        segment_profile=profile,
        compiled_registry=compiled_registry,
        derivable_items=derivable_items,
        overrides_applied_count=overrides_applied_count,
        control_snapshot_projection=control_snapshot_projection,
        semantic_rows_result=semantic_rows_result,
        statement_windows_result=statement_windows_result,
        scenario_output_readiness=scenario_output_readiness,
        scenario_bridge_readiness=scenario_bridge_readiness,
        sensitivity_results=sensitivity_results,
        valuation_input_readiness=valuation_input_readiness,
        provider_registry=registry,
        scenario_source_records=scenario_source_records,
        evaluation_reads=analysis_graph.last_eval_reads,
        unresolved_node_items=tuple(node_history.unresolved_items),
    ))


def _predecessor_document_seed_rows(
    semantic_base_document: ModelSemanticDocument | None,
    model: FinancialModel,
    model_spec: ModelSpec,
    *,
    computed_values: Mapping[str, Mapping[int, float | None]] | None = None,
    seed_coverage_manifest: Iterable[Mapping[str, Any]] = (),
) -> tuple[dict[str, Any], ...]:
    """Retain predecessor evidenced absences that remain absent and unseeded in the successor."""
    del model_spec
    if semantic_base_document is None:
        return ()
    seeded_cells = {
        (str(row.get("resolved_item_id")), int(period))
        for row in seed_coverage_manifest
        if row.get("resolved_item_id")
        for period in (row.get("periods") or ())
    }

    def _remains_absent_and_unseeded(item_id: str, period: int) -> bool:
        if (item_id, period) in seeded_cells:
            return False
        if computed_values is not None and computed_values.get(item_id, {}).get(period) is not None:
            return False
        item = model._index.get(item_id)
        if (
            item is not None
            and item.values is not None
            and (cell_val := item.values.values.get(period)) is not None
            and cell_val.value is not None
        ):
            return False
        return True

    rows: list[dict[str, Any]] = []
    coord_map = {
        (str(sv.sheet), str(sv.cell).upper()): (
            str(sv.semantic_item_id),
            int(sv.period),
        )
        for sv in semantic_base_document.semantic_values
        if sv.period
    }
    for cell in semantic_base_document.provenance.cells:
        if cell.lineage_status != "source_absence_evidenced":
            continue
        identity = coord_map.get((str(cell.sheet), str(cell.cell).upper()))
        if identity is None:
            continue
        item_id, period = identity
        if _remains_absent_and_unseeded(item_id, period):
            rows.append({
                "source_kind": "document_absence",
                "resolved_item_id": item_id,
                "periods": [period],
            })
    for item_id, item in model._index.items():
        for period, spec in (item.overrides or {}).items():
            if getattr(spec, "note", None) == "derived.effective_yield_minus_sofr":
                rows.append({
                    "source_kind": "document_construction",
                    "resolved_item_id": item_id,
                    "source_id": "valuation_inputs",
                    "periods": [int(period)],
                })
    return tuple(rows)


def render_intermediate_workbook_bytes(
    intermediate: CompiledModelIntermediate,
    *,
    semantic_base_document: ModelSemanticDocument | None = None,
    formatter: Optional[ModelFormatter] = None,
) -> bytes:
    """Render formula workbook bytes for external recalculation before version allocation."""

    intermediate = _verified_compiled_intermediate(intermediate)
    semantic_base_document = (
        None
        if semantic_base_document is None
        else decode_model_semantic_document(
            deepcopy(semantic_base_document)
        )
    )
    source_bundle = intermediate.source_bundle
    resolved_formatter = (
        formatter
        if formatter is not None
        else formatter_for_declared_style_guide(
            declared_style_guide(declared_capabilities(source_bundle))
        )
    )
    model = FinancialModel.model_validate(
        intermediate.model.model_dump(mode="python")
    )
    model.build_index()
    annual_render_context = _annual_render_context(
        model=model,
        source_bundle=source_bundle,
        computed_values=intermediate.computed_values,
        projection_readiness=intermediate.projection_readiness,
        model_quality_readiness=intermediate.model_quality_readiness,
        scenario_bridge_readiness=intermediate.scenario_bridge_readiness,
        scenario_results=intermediate.scenario_results,
        version_identity=None,
        predecessor_semantic_document=semantic_base_document,
    )
    plan = render_model(
        model,
        formatter=resolved_formatter,
        annual_context=annual_render_context,
    )
    _append_valuation_evidence_status_block(
        plan,
        intermediate.valuation_evidence_status,
    )
    reconcile_render_contract(
        model,
        plan,
        formatter=resolved_formatter,
        annual_context=annual_render_context,
    )
    finalize_render_contract(plan)
    return render_xlsx_bytes(
        plan,
        finalized_at=source_bundle.retrieved_at,
        stamp=ModelStamp(model_id="model-1", version=1, build_id="sha256:" + "0" * 64),
    )


def _provenance_document(
    model: FinancialModel, model_spec: ModelSpec, unresolved_node_items: Sequence[str]
) -> Any:
    """The document ``finalize_compiled_model`` projects provenance from: the model, no semantic
    rows, and the unwritten historical cells of each unresolved BusinessModel node stamped
    ``business_model_node_unresolved`` by ``mark_unresolved_node_history`` (the stamp
    ``mark_missing_inputs`` gives them on the target path), so the node's
    ``business_model_node_unresolved`` finding is the one record of its missing history."""

    from model_build import document, target_build

    doc = document.ModelSheet(model=model, model_spec=model_spec, semantic_values=(), source="")
    target_build.mark_unresolved_node_history(
        target_build._document_structure(doc), unresolved_node_items
    )
    return doc


def finalize_compiled_model(
    intermediate: CompiledModelIntermediate,
    *,
    allocated_ref: ModelVersionRef,
    sequence_predecessor: ModelVersionRef | None,
    semantic_base: ModelVersionRef | None,
    origin: BuildFinalizationOrigin,
    source_bundle_id: str,
    source_bundle_identity_digest: str,
    finalized_at: datetime,
    semantic_base_document: ModelSemanticDocument | None = None,
    formatter: Optional[ModelFormatter] = None,
    native: Any | None = None,
) -> FinalizedBuildArtifacts:
    """Bind one sealed compile to exact immutable, in-memory artifact bytes."""

    intermediate = _verified_compiled_intermediate(intermediate)
    allocated_ref = ModelVersionRef.model_validate(allocated_ref)
    sequence_predecessor = (
        None
        if sequence_predecessor is None
        else ModelVersionRef.model_validate(sequence_predecessor)
    )
    semantic_base = (
        None
        if semantic_base is None
        else ModelVersionRef.model_validate(semantic_base)
    )
    if finalized_at.tzinfo is None or finalized_at.utcoffset() is None:
        raise ValueError("finalized_at must be timezone-aware")
    canonical_finalized_at = finalized_at.astimezone(timezone.utc)
    semantic_base_document = (
        None
        if semantic_base_document is None
        else decode_model_semantic_document(
            deepcopy(semantic_base_document)
        )
    )
    if isinstance(origin, HandoffBuildModelVersionOrigin):
        origin = HandoffBuildModelVersionOrigin.model_validate(origin)
        if semantic_base is not None or semantic_base_document is not None:
            raise ValueError("handoff finalization forbids successor semantic state")
        if intermediate.model_spec.predecessor is not None:
            raise ValueError("handoff ModelSpec must not carry a semantic predecessor")
    elif isinstance(origin, SuccessorModelVersionOrigin):
        origin = SuccessorModelVersionOrigin.model_validate(origin)
        if semantic_base is None or semantic_base_document is None:
            raise ValueError(
                "successor finalization requires semantic base and document"
            )
        if origin.semantic_base != semantic_base:
            raise ValueError("successor origin must match the semantic base")
        if intermediate.model_spec.predecessor != semantic_base:
            raise ValueError(
                "successor ModelSpec predecessor must match the semantic base"
            )
        document_ref = ModelVersionRef(
            model_id=semantic_base_document.model_id,
            version=semantic_base_document.model_version,
        )
        if document_ref != semantic_base:
            raise ValueError("semantic base document must match the semantic base")
    else:
        raise TypeError("origin must be a handoff or successor origin")

    # CompilerInput performs the canonical pair validation before rendering.
    source_bundle_id = str(source_bundle_id)
    source_bundle_identity_digest = str(source_bundle_identity_digest)

    source_bundle = intermediate.source_bundle
    model_spec = intermediate.model_spec
    # Resolve the model kind's declared style guide. An explicit formatter is a
    # caller override, while a kind that declares no guide uses the authored
    # library default.
    resolved_formatter = (
        formatter
        if formatter is not None
        else formatter_for_declared_style_guide(
            declared_style_guide(declared_capabilities(source_bundle))
        )
    )
    render_compiler_inputs = _render_compiler_inputs(
        source_bundle=source_bundle,
        model_spec=model_spec,
        source_bundle_id=source_bundle_id,
        source_bundle_identity_digest=source_bundle_identity_digest,
    )
    model = FinancialModel.model_validate(
        intermediate.model.model_dump(mode="python")
    )
    model.build_index()
    model.metadata.created_at = canonical_finalized_at.isoformat()
    typed_analysis_values = intermediate.typed_analysis_values
    computed_values = intermediate.computed_values
    projection_readiness = intermediate.projection_readiness
    model_quality_readiness = intermediate.model_quality_readiness
    scenario_results = intermediate.scenario_results
    valuation_results = intermediate.valuation_results
    schedule_input_audit = intermediate.schedule_input_audit
    valuation_evidence_status = intermediate.valuation_evidence_status
    model_spec_valuation = intermediate.model_spec_valuation
    seed_coverage_manifest = intermediate.seed_coverage_manifest
    scenario_source_records = intermediate.scenario_source_records
    evaluation_reads = intermediate.evaluation_reads
    canonical_control_curves = intermediate.canonical_control_curves
    direct_scenario_control_ids = intermediate.direct_scenario_control_ids
    ticker = intermediate.ticker
    stats = intermediate.stats
    diagnostic = intermediate.diagnostic
    profile = intermediate.segment_profile
    compiled_registry = intermediate.compiled_registry
    derivable_items = intermediate.derivable_items
    overrides_applied_count = intermediate.overrides_applied_count
    control_snapshot_projection = intermediate.control_snapshot_projection
    semantic_rows_result = intermediate.semantic_rows_result
    statement_windows_result = intermediate.statement_windows_result
    scenario_output_readiness = intermediate.scenario_output_readiness
    scenario_bridge_readiness = intermediate.scenario_bridge_readiness
    sensitivity_results = intermediate.sensitivity_results
    valuation_input_readiness = intermediate.valuation_input_readiness
    registry = intermediate.provider_registry

    render_started = perf_counter()
    annual_render_context = _annual_render_context(
        model=model,
        source_bundle=source_bundle,
        computed_values=computed_values,
        projection_readiness=projection_readiness,
        model_quality_readiness=model_quality_readiness,
        scenario_bridge_readiness=scenario_bridge_readiness,
        scenario_results=scenario_results,
        version_identity=allocated_ref,
        predecessor_semantic_document=semantic_base_document,
    )
    plan = render_model(
        model,
        formatter=resolved_formatter,
        annual_context=annual_render_context,
    )
    _append_valuation_evidence_status_block(
        plan,
        valuation_evidence_status,
    )
    reconcile_render_contract(
        model,
        plan,
        formatter=resolved_formatter,
        annual_context=annual_render_context,
    )
    from .render_state import compiler_input_content_hash

    provenance_started = perf_counter()
    from model_build import target_build, document
    doc = _provenance_document(model, model_spec, intermediate.unresolved_node_items)
    ours = document.recalc(doc)
    document_seed_rows = _predecessor_document_seed_rows(
        semantic_base_document,
        model,
        model_spec,
        computed_values=computed_values,
        seed_coverage_manifest=(
            *seed_coverage_manifest,
            *scenario_source_records,
        ),
    )
    preview_provenance = target_build.project_provenance_map(
        doc,
        ours,
        plan,
        source_bundle_digest=compiler_input_content_hash(
            source_bundle
        ).removeprefix("sha256:"),
        model_spec_digest=compiler_input_content_hash(
            model_spec
        ).removeprefix("sha256:"),
        source_bundle=source_bundle,
        model_spec=model_spec,
        seed_coverage_manifest=(
            *seed_coverage_manifest,
            *scenario_source_records,
            *document_seed_rows,
        ),
        evaluation_reads=evaluation_reads,
    )
    provenance_seconds = perf_counter() - provenance_started
    schedule_input_audit = build_schedule_input_audit(
        model=model,
        typed_values=typed_analysis_values,
        projection_periods=tuple(int(value) for value in model.time_structure.projection_periods),
        working_capital_required=tuple(
            item_id
            for item_id in ("tpl.fm.balance_sheet.current_asset_3",)
            if item_id in model._index
        ),
        provenance=preview_provenance,
        semantic_cells=tuple(plan.semantic_cells),
    )
    append_lineage_index(
        plan=plan,
        provenance=preview_provenance,
        model=model,
        formatter=resolved_formatter,
        declinations={
            declination.declination_id: declination
            for declination in model_spec.declinations
        },
    )
    finalize_render_contract(plan)
    assert_valuation_template_schema(
        model,
        origin="build_model",
        workbook_path=None,
        module_path=__file__,
    )

    workbook_semantics_hash = canonical_workbook_semantics_hash(
        ticker=ticker,
        control_curves=canonical_control_curves or (),
        valuation=model_spec_valuation,
    )
    from .render_state import construct_model_render_state, render_state_hash

    render_state = construct_model_render_state(
        model_id=allocated_ref.model_id,
        model_version=allocated_ref.version,
        workbook_semantics_hash=workbook_semantics_hash,
        compiler_inputs=render_compiler_inputs,
        seed_coverage_manifest=seed_coverage_manifest,
    )
    # Bytes₀: the workbook without the Summary verdict block. Validation and
    # the annual guard read these bytes; the verdict they produce is then
    # appended (literal writes only) and bytes₁ is the published workbook.
    workbook_content = render_xlsx_bytes(
        plan,
        finalized_at=canonical_finalized_at,
        stamp=ModelStamp(
            model_id=allocated_ref.model_id,
            version=allocated_ref.version,
            build_id=render_state["identity"]["build_id"],
        ),
    )
    annual_presentation = evaluate_annual_presentation(
        model,
        render_plan=plan,
        cell_readback=_read_annual_presentation_cells(workbook_content),
        workbook_sha256=hashlib.sha256(workbook_content).hexdigest(),
        render_state_hash=render_state_hash(render_state),
        formatter=resolved_formatter,
    )
    render_seconds = perf_counter() - render_started - provenance_seconds

    from .render_state import compiler_input_content_hash

    provenance_started = perf_counter()
    from model_build import target_build, document
    doc = _provenance_document(model, model_spec, intermediate.unresolved_node_items)
    ours = document.recalc(doc)
    from .build_offline_validation import provenance_diagnostics_for_map
    from .build_findings import historical_foundation_evidence_findings

    provenance = target_build.project_provenance_map(
        doc,
        ours,
        plan,
        source_bundle_digest=compiler_input_content_hash(
            source_bundle
        ).removeprefix("sha256:"),
        model_spec_digest=compiler_input_content_hash(
            model_spec
        ).removeprefix("sha256:"),
        source_bundle=source_bundle,
        model_spec=model_spec,
        seed_coverage_manifest=(
            *seed_coverage_manifest,
            *scenario_source_records,
            *document_seed_rows,
        ),
        evaluation_reads=evaluation_reads,
    )
    provenance_diagnostics = provenance_diagnostics_for_map(
        provenance,
        render_plan=plan,
        model=model,
    )
    provenance_seconds += perf_counter() - provenance_started
    validation_started = perf_counter()
    offline_validation = validate_offline_build_artifacts(
        model=model,
        render_plan=plan,
        workbook_path=workbook_content,
        native=native,
        source_bundle=source_bundle.model_copy(
            update={
                "diagnostics": (
                    *source_bundle.diagnostics,
                    *intermediate.schedule_policy_diagnostics,
                )
            }
        ),
        model_spec=model_spec,
        provenance=provenance,
        provenance_diagnostics=tuple(provenance_diagnostics),
        annual_presentation=annual_presentation,
        validated_at=canonical_finalized_at,
        compile_stage=CompileStageEvidence(
            projection_readiness=projection_readiness,
            model_quality_readiness=model_quality_readiness,
            scenario_bridge_readiness=scenario_bridge_readiness,
            scenario_results=scenario_results,
            valuation_input_readiness=valuation_input_readiness,
            schedule_input_audit=schedule_input_audit,
            diagnostic_report=diagnostic,
            required_unavailable=annual_render_context.required_unavailable,
            compile_findings=(
                *intermediate.compile_findings,
                *historical_foundation_evidence_findings(source_bundle, model_spec),
            ),
            judgment_findings=intermediate.judgment_findings,
        ),
        bound_scenario_control_ids=tuple(
            sorted(
                set(direct_scenario_control_ids)
                | {
                    curve.control_id
                    for curve in canonical_control_curves
                    if curve.domain == "scenario"
                }
            )
        ),
    )
    validation_seconds = perf_counter() - validation_started
    validation = offline_validation.report

    publish_render_started = perf_counter()
    workbook_content, annual_presentation = _append_verdict_and_reserialize(
        model=model,
        plan=plan,
        validation=validation,
        bytes0_annual_presentation=annual_presentation,
        semantic_base_document=semantic_base_document,
        allocated_ref=allocated_ref,
        model_spec=model_spec,
        provenance=preview_provenance,
        valuation_evidence_status=valuation_evidence_status,
        schedule_input_audit=schedule_input_audit,
        render_state=render_state,
        seed_coverage_manifest=seed_coverage_manifest,
        formatter=resolved_formatter,
        finalized_at=canonical_finalized_at,
    )
    render_seconds += perf_counter() - publish_render_started

    build_result = BuildResult(
        model=model,
        render_plan=plan,
        output_path=None,
        stats=stats,
        annual_presentation=annual_presentation,
        render_state=render_state,
        diagnostic=diagnostic,
        segment_profile=profile,
        compiled_registry=compiled_registry,
        derivable_items=derivable_items,
        overrides_applied=overrides_applied_count,
        control_snapshot_projection=control_snapshot_projection,
        semantic_rows=semantic_rows_result,
        statement_windows=statement_windows_result,
        projection_readiness=projection_readiness,
        scenario_output_readiness=scenario_output_readiness,
        scenario_bridge_readiness=scenario_bridge_readiness,
        scenario_results=scenario_results,
        sensitivity_results=sensitivity_results,
        valuation_results=valuation_results,
        valuation_evidence_status=valuation_evidence_status,
        schedule_input_audit=schedule_input_audit,
        model_quality_readiness=model_quality_readiness,
        valuation_input_readiness=valuation_input_readiness,
        workbook_semantics_hash=workbook_semantics_hash,
        render_compiler_inputs=render_compiler_inputs,
        seed_coverage_manifest=seed_coverage_manifest,
        scenario_source_records=scenario_source_records,
        evaluation_reads=evaluation_reads,
        provider_registry=registry,
    )

    if (
        allocated_ref.model_id in {".", ".."}
        or Path(allocated_ref.model_id).is_absolute()
        or "/" in allocated_ref.model_id
        or "\\" in allocated_ref.model_id
        or Path(allocated_ref.model_id).name != allocated_ref.model_id
    ):
        raise ValueError("model_id must be one safe path segment")
    bundle_location = (
        f"model-versions/{allocated_ref.model_id}/v{allocated_ref.version}"
    )

    def payload(name: str, content: bytes) -> FinalizedArtifactPayload:
        return FinalizedArtifactPayload(
            location=f"{bundle_location}/{name}",
            sha256=hashlib.sha256(content).hexdigest(),
            content=content,
        )

    workbook = payload("model.xlsx", workbook_content)
    def encoded_json(value: object) -> bytes:
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        if isinstance(value, str):
            return json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        return canonical_json_bytes(value)

    def encoded_array(values: Iterable[object]) -> bytes:
        payload = bytearray(b"[")
        for index, value in enumerate(values):
            if index:
                payload.extend(b",")
            payload.extend(encoded_json(value))
        payload.extend(b"]")
        return bytes(payload)

    def encoded_object(fields_by_name: Mapping[str, bytes]) -> bytes:
        payload = bytearray(b"{")
        for index, field_name in enumerate(sorted(fields_by_name)):
            if index:
                payload.extend(b",")
            payload.extend(
                json.dumps(
                    field_name,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            payload.extend(b":")
            payload.extend(fields_by_name[field_name])
        payload.extend(b"}")
        return bytes(payload)

    # Stream the repeated cell collection so canonical serialization never
    # materializes the 25+ MiB provenance document as both a Python mapping and
    # an encoded string at once.
    provenance_content = encoded_object(
        {
            "schema_version": encoded_json(provenance.schema_version),
            "source_bundle_digest": encoded_json(
                provenance.source_bundle_digest
            ),
            "model_spec_digest": encoded_json(provenance.model_spec_digest),
            "cells": encoded_array(provenance.cells),
        }
    )
    provenance_map = payload("provenance.json", provenance_content)
    validation_content = canonical_json_bytes(
        validation.model_dump(mode="json")
    )
    validation_report = payload("validation.json", validation_content)
    # Every nested value below has already crossed its exact typed compiler
    # boundary.  Reconstructing the full Pydantic graph here needlessly copies
    # the complete FinancialModel and ProvenanceMap at the finalization memory
    # peak.  Trusted construction avoids that duplicate graph while retaining
    # the document's cross-field identity/status invariant before persistence.
    from .handoff import ModelRenderState

    semantic_document = ModelSemanticDocument.model_construct(
        schema_version="3.3",
        model_id=allocated_ref.model_id,
        model_version=allocated_ref.version,
        ticker=source_bundle.ticker,
        render_state=ModelRenderState.model_validate(render_state),
        model=model,
        provenance_map=provenance_map.artifact_location,
        provenance=provenance,
        semantic_values=semantic_value_snapshots(plan),
        validation_status=validation.status,
        validation_blocker_count=validation.error_count,
        validation_warning_count=validation.uncleared_warning_count,
        valuation_evidence_status=valuation_evidence_status,
        schedule_input_audit=schedule_input_audit,
    )
    semantic_document.enforce_identity()
    semantic_content = encoded_object(
        {
            "schema_version": encoded_json(semantic_document.schema_version),
            "model_id": encoded_json(semantic_document.model_id),
            "model_version": encoded_json(semantic_document.model_version),
            "ticker": encoded_json(semantic_document.ticker),
            "render_state": encoded_json(semantic_document.render_state),
            "model": encoded_json(semantic_document.model),
            "provenance_map": encoded_json(semantic_document.provenance_map),
            "provenance": provenance_content,
            "semantic_values": encoded_array(semantic_document.semantic_values),
            "validation_status": encoded_json(
                semantic_document.validation_status
            ),
            "validation_blocker_count": encoded_json(
                semantic_document.validation_blocker_count
            ),
            "validation_warning_count": encoded_json(
                semantic_document.validation_warning_count
            ),
            "valuation_evidence_status": encoded_json(
                semantic_document.valuation_evidence_status
            ),
            "schedule_input_audit": encoded_json(
                semantic_document.schedule_input_audit
            ),
        }
    )
    semantic_sidecar = payload("semantic.json", semantic_content)
    return FinalizedBuildArtifacts(
        intermediate_digest=intermediate.content_digest,
        allocated_ref=allocated_ref,
        sequence_predecessor=sequence_predecessor,
        semantic_base=semantic_base,
        origin=origin,
        source_bundle_id=source_bundle_id,
        source_bundle_identity_digest=source_bundle_identity_digest,
        finalized_at=canonical_finalized_at,
        workbook=workbook,
        semantic_sidecar=semantic_sidecar,
        provenance_map=provenance_map,
        validation_report=validation_report,
        build_result=build_result,
        recalculated_values=offline_validation.recalculated_values,
        stage_seconds={
            "render": render_seconds,
            "provenance": provenance_seconds,
            "recalc": offline_validation.recalculation_wall_seconds,
            "validation": validation_seconds,
        },
        recalculation_bytes={
            "input": offline_validation.recalculation_input_bytes,
            "output": offline_validation.recalculation_output_bytes,
        },
    )


def _build_seed_coverage_manifest(
    model: FinancialModel,
    *,
    business_model: Any | None,
    compiled_registry: Any | None,
    model_spec_seed_coverage: Iterable[Mapping[str, Any]],
    projection_seed_coverage: Iterable[Mapping[str, Any]],
) -> list[dict[str, object]]:
    del model, business_model, compiled_registry
    return [
        dict(row)
        for row in [*model_spec_seed_coverage, *projection_seed_coverage]
    ]


__all__ = [
    "CompiledModelIntermediate",
    "compile_model_intermediate",
    "load_template",
    "finalize_compiled_model",
    "render_intermediate_workbook_bytes",
]
