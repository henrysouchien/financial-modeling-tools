from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .control_snapshot_projection import CanonicalProjectionCurve
from .dependency_graph import DependencyGraph as DependencyGraph
from .growth_carry_forward_guard import (
    GrowthCarryForwardFinding,
    growth_carry_forward_findings,
)
from .forecast_contribution import durable_driver_plan_coverage
from .handle import ComputedValues
from .model_readiness_common import (
    _computed_values,
    _has_any_value,
    _historical_periods,
    _is_present,
    _latest_numeric_value as _latest_numeric_value,
    _missing_periods,
    _nearly_equal as _nearly_equal,
    _normalize_computed_values,
    _projection_periods,
)
from .model_readiness_valuation import (
    ValuationInputReadiness,
    ValuationInputReadinessFlag as ValuationInputReadinessFlag,
    ValuationInputReadinessSeverity,
    ValuationInputReadinessStatus,
    _MARKET_BETA_ANCHOR as _MARKET_BETA_ANCHOR,
    _VALUATION_ECONOMIC_INPUTS as _VALUATION_ECONOMIC_INPUTS,
    _VALUATION_PLACEHOLDER_VALUES as _VALUATION_PLACEHOLDER_VALUES,
    _VALUATION_REQUIRED_INPUTS as _VALUATION_REQUIRED_INPUTS,
    compute_valuation_input_readiness,
)
from .model_readiness_quality import (
    ModelQualityDomain,
    ModelQualityDomainReadiness,
    ModelQualityReadiness,
    ModelQualityReadinessIssue,
    ModelQualityReadinessSeverity,
    ModelQualityReadinessStatus,
    _upstream_item_ids,
    compute_model_quality_readiness,
)
from .model_readiness_scenario_output import (
    ModelScenarioOutputFieldReadiness,
    ModelScenarioOutputReadiness,
    ModelScenarioOutputReadinessIssue,
    ScenarioOutputReadinessIssueCategory,
    ScenarioOutputReadinessIssueCode,
    ScenarioOutputReadinessScope,
    ScenarioOutputReadinessSeverity,
    ScenarioOutputReadinessStatus,
    compute_model_scenario_output_readiness,
)
from .model_readiness_scenario_bridge import (
    ModelScenarioBridgeOwnerReadiness,
    ModelScenarioBridgeReadiness,
    ModelScenarioBridgeReadinessIssue,
    ScenarioBridgeReadinessIssueCategory,
    ScenarioBridgeReadinessIssueCode,
    ScenarioBridgeReadinessScope,
    ScenarioBridgeReadinessSeverity,
    ScenarioBridgeReadinessStatus,
    compute_model_scenario_bridge_readiness,
)
from .models import FinancialModel, FormulaSpec, ItemType, LineItemRef
from .refs import line_item_ref_from_obj

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry
    from .segments import SegmentProfile


ProjectionReadinessStatus = Literal["ready", "incomplete", "blocked", "unknown"]
ProjectionReadinessSeverity = Literal["blocking", "warning"]
ProjectionReadinessScope = Literal["build_contract", "workbook_formula"]
ProjectionReadinessIssueCategory = Literal[
    "seedable", "operational_warning", "blocking"
]
ProjectionReadinessIssueCode = Literal[
    "primary_projection_item_missing",
    "primary_projection_missing",
    "primary_projection_incomplete",
    "bm_driver_projection_seed_missing",
    "bm_segment_projection_missing",
    "segment_history_basis_mixed",
    "segment_history_basis_unverified",
    "formula_rollup_missing_inputs",
    "required_driver_projection_missing",
    "driver_projection_period_gap",
    "stale_render_state",
]


class ModelProjectionReadinessIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: ProjectionReadinessIssueCode
    severity: ProjectionReadinessSeverity
    category: ProjectionReadinessIssueCategory | None = None
    detail: str
    item_id: str | None = None
    segment_id: str | None = None
    segment_name: str | None = None
    missing_periods: list[int] = Field(default_factory=list)
    related_item_ids: list[str] = Field(default_factory=list)
    unowned_periods: list[int] = Field(default_factory=list)
    carried_rate: float | None = None
    bound: float | None = None
    scenario_row_item_id: str | None = None
    fix: str | None = None

    @model_validator(mode="after")
    def _populate_category(self) -> "ModelProjectionReadinessIssue":
        if self.category is None:
            self.category = _issue_category(self.code, self.severity)
        return self


class ModelProjectionReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: ProjectionReadinessStatus = "unknown"
    scope: ProjectionReadinessScope = "workbook_formula"
    projection_periods: list[int] = Field(default_factory=list)
    primary_item_id: str | None = None
    issues: list[ModelProjectionReadinessIssue] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    summary: str | None = None


def compute_model_projection_readiness(
    model: FinancialModel,
    *,
    compiled_registry: "CompiledDriverRegistry | None" = None,
    segment_profile: "SegmentProfile | None" = None,
    canonical_control_curves: tuple[CanonicalProjectionCurve, ...] = (),
    artifact_forecast_coverage_by_driver_id: Mapping[
        str, Sequence[int]
    ] | None = None,
    computed_values: ComputedValues | None = None,
    guard_config: Any | None = None,
    driver_assumption_plan: Any | None = None,
    render_state: Any | None = None,
    current_identity: Any | None = None,
) -> ModelProjectionReadiness:
    """Return an agent-facing projection-readiness summary for a built model."""

    model.build_index()
    scope: ProjectionReadinessScope = (
        "build_contract"
        if compiled_registry is not None
        or segment_profile is not None
        or canonical_control_curves
        or artifact_forecast_coverage_by_driver_id
        else "workbook_formula"
    )
    limitations = _readiness_limitations(
        compiled_registry=compiled_registry,
    )
    projection_periods = _projection_periods(model)
    if not projection_periods:
        return ModelProjectionReadiness(
            status="unknown",
            scope=scope,
            limitations=limitations,
            summary="model has no projection periods",
        )

    values = computed_values if computed_values is not None else _computed_values(model)
    issues: list[ModelProjectionReadinessIssue] = []

    primary_item_id = _primary_revenue_item_id(model)
    if primary_item_id is None:
        issues.append(
            ModelProjectionReadinessIssue(
                code="primary_projection_item_missing",
                severity="blocking",
                detail="no primary total revenue item was found in the model",
                missing_periods=list(projection_periods),
            )
        )
    else:
        missing = _missing_periods(values.get(primary_item_id, {}), projection_periods)
        if missing == projection_periods:
            issues.append(
                ModelProjectionReadinessIssue(
                    code="primary_projection_missing",
                    severity="blocking",
                    detail=f"primary revenue item {primary_item_id!r} has no projected values",
                    item_id=primary_item_id,
                    missing_periods=missing,
                )
            )
        elif missing:
            issues.append(
                ModelProjectionReadinessIssue(
                    code="primary_projection_incomplete",
                    severity="warning",
                    detail=f"primary revenue item {primary_item_id!r} is missing some projected values",
                    item_id=primary_item_id,
                    missing_periods=missing,
                )
            )

    if compiled_registry is not None:
        issues.extend(
            _compiled_bm_projection_issues(
                model,
                compiled_registry=compiled_registry,
                values=values,
                projection_periods=projection_periods,
                guard_config=guard_config,
            )
        )
        _downgrade_primary_missing_for_seedable_bm(issues)
    elif segment_profile is not None:
        issues.extend(
            _segment_profile_basis_issues(
                model,
                segment_profile=segment_profile,
            )
        )

    durable_issues = _canonical_driver_coverage_issues(
        driver_assumption_plan=driver_assumption_plan,
        canonical_control_curves=canonical_control_curves,
        artifact_forecast_coverage_by_driver_id=(
            artifact_forecast_coverage_by_driver_id
        ),
        render_state=render_state,
        current_identity=current_identity,
        projection_periods=projection_periods,
    )
    issues = _dedup_compiled_issues(issues, durable_issues)
    issues.extend(durable_issues)
    issues.extend(
        _formula_rollup_missing_input_issues(model, values, projection_periods)
    )

    if any(issue.severity == "blocking" for issue in issues):
        status: ProjectionReadinessStatus = "blocked"
    elif issues:
        status = "incomplete"
    else:
        status = "ready"

    return ModelProjectionReadiness(
        status=status,
        scope=scope,
        projection_periods=list(projection_periods),
        primary_item_id=primary_item_id,
        issues=issues,
        limitations=limitations,
        summary=_readiness_summary(status, issues),
    )


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _render_identity_mismatches(
    *, driver_assumption_plan: Any, render_state: Any, current_identity: Any
) -> list[str]:
    if render_state is None:
        return []
    identity = _field(render_state, "identity")
    compiler = _field(render_state, "compiler_inputs")
    if identity is None or compiler is None:
        return ["partial_render_state"]
    mismatches: list[str] = []
    plan_bm = str(_field(driver_assumption_plan, "business_model_id", "") or "")
    plan_revision = str(_field(driver_assumption_plan, "revision", "") or "")
    if plan_bm and plan_bm != str(_field(identity, "business_model_id", "") or ""):
        mismatches.append("business_model_id")
    if plan_revision and plan_revision != str(
        _field(identity, "business_model_revision", "") or ""
    ):
        mismatches.append("business_model_revision")
    comparisons = {
        "build_id": "build_id",
        "model_id": "model_id",
        "model_version": "model_version",
        "model_build_context_id": "model_build_context_id",
        "model_build_context_version": "model_build_context_version",
        "business_model_id": "business_model_id",
        "business_model_revision": "business_model_revision",
        "segment_profile_snapshot_ref": "segment_profile_snapshot_ref",
    }
    if current_identity is not None:
        for current_name, identity_name in comparisons.items():
            expected = _field(current_identity, current_name)
            if expected is not None and expected != _field(identity, identity_name):
                mismatches.append(current_name)
    for name in (
        "model_build_context_id",
        "model_build_context_version",
    ):
        if _field(identity, name) != _field(compiler, name):
            mismatches.append(name)
    business_model_ref = _field(compiler, "business_model_ref")
    segment_profile_ref = _field(compiler, "segment_profile_snapshot_ref")
    if _field(identity, "business_model_id") != _field(
        business_model_ref, "business_model_id"
    ):
        mismatches.append("business_model_id")
    if _field(identity, "business_model_revision") != _field(
        business_model_ref, "revision"
    ):
        mismatches.append("business_model_revision")
    if _field(identity, "segment_profile_snapshot_ref") != _field(
        segment_profile_ref, "snapshot_id"
    ):
        mismatches.append("segment_profile_snapshot_ref")
    records = list(_field(render_state, "seed_coverage_manifest", []) or [])
    build_id = _field(identity, "build_id")
    if any(_field(record, "build_identity") != build_id for record in records):
        mismatches.append("seed_coverage_build_id")
    return sorted(set(mismatches))


def _canonical_driver_coverage_issues(
    *,
    driver_assumption_plan: Any,
    canonical_control_curves: tuple[CanonicalProjectionCurve, ...],
    artifact_forecast_coverage_by_driver_id: Mapping[
        str, Sequence[int]
    ] | None,
    render_state: Any,
    current_identity: Any,
    projection_periods: list[int],
) -> list[ModelProjectionReadinessIssue]:
    if driver_assumption_plan is None:
        return []
    mismatches = _render_identity_mismatches(
        driver_assumption_plan=driver_assumption_plan,
        render_state=render_state,
        current_identity=current_identity,
    )
    if mismatches:
        return [
            ModelProjectionReadinessIssue(
                code="stale_render_state",
                severity="blocking",
                detail="render-state identity does not match the current accepted model: "
                + ", ".join(mismatches),
                fix="rebuild the model from the accepted BusinessModel and segment snapshot",
            )
        ]
    coverage = durable_driver_plan_coverage(
        driver_assumption_plan=driver_assumption_plan,
        canonical_control_curves=canonical_control_curves,
        projection_periods=projection_periods,
    )
    covered_periods_by_driver_id = {
        driver_id: set(periods)
        for driver_id, periods in coverage.covered_periods_by_driver_id.items()
    }
    artifact_coverage = artifact_forecast_coverage_by_driver_id or {}
    unknown_driver_ids = set(artifact_coverage) - set(coverage.required_driver_ids)
    if unknown_driver_ids:
        raise ValueError(
            "artifact forecast coverage driver_id is absent from the accepted plan: "
            f"{tuple(sorted(unknown_driver_ids))!r}"
        )
    for driver_id, periods in artifact_coverage.items():
        covered_periods_by_driver_id[driver_id].update(
            str(int(period)) for period in periods
        )
    issues: list[ModelProjectionReadinessIssue] = []
    required_periods = set(coverage.required_periods)
    for key in coverage.required_driver_ids:
        durable_periods = covered_periods_by_driver_id[key]
        missing = sorted(int(period) for period in required_periods - durable_periods)
        if not durable_periods:
            issues.append(
                ModelProjectionReadinessIssue(
                    code="required_driver_projection_missing",
                    severity="blocking",
                    detail=(
                        f"required driver {key!r} has no canonical forecast-control "
                        "curve"
                    ),
                    item_id=key,
                    missing_periods=list(projection_periods),
                )
            )
        elif missing:
            issues.append(
                ModelProjectionReadinessIssue(
                    code="driver_projection_period_gap",
                    severity="blocking",
                    detail=(
                        f"required driver {key!r} has a canonical forecast-control "
                        "period gap"
                    ),
                    item_id=key,
                    missing_periods=missing,
                )
            )
    return issues


def _dedup_compiled_issues(
    existing: list[ModelProjectionReadinessIssue],
    durable: list[ModelProjectionReadinessIssue],
) -> list[ModelProjectionReadinessIssue]:
    blocking_gaps = [
        issue
        for issue in durable
        if issue.severity == "blocking"
        and issue.code
        in {"required_driver_projection_missing", "driver_projection_period_gap"}
    ]
    if not blocking_gaps:
        return existing
    result: list[ModelProjectionReadinessIssue] = []
    for issue in existing:
        if (
            issue.code != "bm_driver_projection_seed_missing"
            or issue.severity == "blocking"
        ):
            result.append(issue)
            continue
        issue_periods = set(issue.missing_periods)
        related = set(issue.related_item_ids)
        superseded = any(
            (not issue_periods or bool(issue_periods & set(gap.missing_periods)))
            and (gap.item_id in related or not related)
            for gap in blocking_gaps
        )
        if not superseded:
            result.append(issue)
    return result


def _primary_revenue_item_id(model: FinancialModel) -> str | None:
    for item_id in (
        "tpl.fm.income_statement.total_revenue",
        "tpl.a.revenue_drivers.total_revenue",
    ):
        if item_id in model._index:
            return item_id
    return None


def _compiled_bm_projection_issues(
    model: FinancialModel,
    *,
    compiled_registry: "CompiledDriverRegistry",
    values: ComputedValues,
    projection_periods: list[int],
    guard_config: Any | None = None,
) -> list[ModelProjectionReadinessIssue]:
    issues: list[ModelProjectionReadinessIssue] = []
    historical_periods = _historical_periods(model)
    segment_profile = compiled_registry.segment_profile
    growth_carry_forward_by_segment = {
        finding.segment_id: finding
        for finding in growth_carry_forward_findings(
            model,
            values=values,
            projection_periods=projection_periods,
            guard_config=guard_config,
        )
    }

    for segment_id, segment_index in sorted(
        compiled_registry.segment_mapping.items(), key=lambda item: item[1]
    ):
        segment_info = (
            segment_profile.segments[int(segment_index) - 1]
            if 0 < int(segment_index) <= len(segment_profile.segments)
            else None
        )
        revenue_id = (
            getattr(segment_info, "item_ids", {}).get("revenue")
            if segment_info is not None
            else None
        ) or f"bm.{segment_id}.__rev"
        if revenue_id not in model._index:
            continue

        issues.extend(
            _segment_history_basis_issues(
                segment_info,
                historical_periods=historical_periods,
                revenue_id=revenue_id,
                segment_id=segment_id,
            )
        )

        revenue_values = values.get(revenue_id, {})
        has_actuals = _has_any_value(revenue_values, historical_periods)
        driver_gaps = _segment_driver_projection_gaps(
            model,
            compiled_registry=compiled_registry,
            values=values,
            segment_id=segment_id,
            projection_periods=projection_periods,
        )
        structural_gaps, seedable_gaps = _split_segment_driver_gaps(
            model,
            compiled_registry=compiled_registry,
            values=values,
            segment_id=segment_id,
            gaps=driver_gaps,
            historical_periods=historical_periods,
        )
        growth_carry_forward_finding = growth_carry_forward_by_segment.get(segment_id)
        if has_actuals and structural_gaps:
            issues.append(
                ModelProjectionReadinessIssue(
                    code="bm_driver_projection_seed_missing",
                    severity="blocking",
                    detail=(
                        f"segment {segment_id!r} has historical revenue actuals, but its "
                        "BusinessModel driver rows do not have source-backed baselines, "
                        "overrides, or MBC seeds"
                    ),
                    item_id=revenue_id,
                    segment_id=segment_id,
                    segment_name=getattr(segment_info, "name", None),
                    missing_periods=_union_missing_periods(structural_gaps),
                    related_item_ids=list(structural_gaps),
                )
            )
        elif has_actuals and seedable_gaps and growth_carry_forward_finding is None:
            issues.append(
                ModelProjectionReadinessIssue(
                    code="bm_driver_projection_seed_missing",
                    severity="warning",
                    detail=(
                        f"segment {segment_id!r} has source-backed historical driver rows, "
                        "but base-case projection assumptions have not been written yet"
                    ),
                    item_id=revenue_id,
                    segment_id=segment_id,
                    segment_name=getattr(segment_info, "name", None),
                    missing_periods=_union_missing_periods(seedable_gaps),
                    related_item_ids=list(seedable_gaps),
                )
            )
        if growth_carry_forward_finding is not None:
            issues.append(
                _growth_carry_forward_projection_issue(
                    growth_carry_forward_finding,
                    segment_name=getattr(segment_info, "name", None),
                )
            )
        missing = _missing_periods(revenue_values, projection_periods)
        if not has_actuals or missing != projection_periods:
            continue

        related_item_ids = _segment_projection_related_item_ids(
            model,
            compiled_registry=compiled_registry,
            values=values,
            segment_id=segment_id,
            projection_periods=projection_periods,
        )
        issues.append(
            ModelProjectionReadinessIssue(
                code="bm_segment_projection_missing",
                severity="blocking"
                if structural_gaps or not seedable_gaps
                else "warning",
                detail=(
                    (
                        f"segment {segment_id!r} has historical revenue actuals but no projected revenue; "
                        "the BM decomposition path is not seeded for projections"
                    )
                    if structural_gaps or not seedable_gaps
                    else (
                        f"segment {segment_id!r} has historical revenue actuals but no projected revenue "
                        "until base-case projection assumptions are written"
                    )
                ),
                item_id=revenue_id,
                segment_id=segment_id,
                segment_name=getattr(segment_info, "name", None),
                missing_periods=missing,
                related_item_ids=related_item_ids,
            )
        )
    return issues


def _growth_carry_forward_projection_issue(
    finding: GrowthCarryForwardFinding,
    *,
    segment_name: str | None = None,
) -> ModelProjectionReadinessIssue:
    if finding.scenario_row_item_id is not None:
        detail = (
            f"segment {finding.segment_id!r} reads scenario row "
            f"{finding.scenario_row_item_id!r} via offset_scenario, but "
            f"{len(finding.unowned_periods)} projection period(s) still carry forward "
            f"{finding.effective_growth_item_id!r} at {finding.carried_rate:g}, above the "
            f"{finding.bound:g} guardrail"
        )
    else:
        detail = (
            f"segment {finding.segment_id!r} has partially authored revenue-growth assumptions, "
            f"but {len(finding.unowned_periods)} projection period(s) still carry forward "
            f"{finding.effective_growth_item_id!r} at {finding.carried_rate:g}, above the "
            f"{finding.bound:g} guardrail"
        )
    return ModelProjectionReadinessIssue(
        code="bm_driver_projection_seed_missing",
        severity="blocking",
        category="seedable",
        detail=detail,
        item_id=finding.driver_item_id,
        segment_id=finding.segment_id,
        segment_name=segment_name,
        missing_periods=list(finding.unowned_periods),
        related_item_ids=[finding.effective_growth_item_id],
        unowned_periods=list(finding.unowned_periods),
        carried_rate=finding.carried_rate,
        bound=finding.bound,
        scenario_row_item_id=finding.scenario_row_item_id,
        fix=finding.fix,
    )


def _segment_history_basis_issues(
    segment_info: Any,
    *,
    historical_periods: list[int],
    revenue_id: str,
    segment_id: str,
) -> list[ModelProjectionReadinessIssue]:
    observations = getattr(segment_info, "revenue_observations", None) or {}
    if not observations:
        return []
    historical_set = {int(period) for period in historical_periods}
    mixed_periods: list[int] = []
    unknown_periods: list[int] = []
    mixed_notes: list[str] = []
    prior_observation: Any | None = None
    for _raw_year, observation in sorted(
        (
            (int(getattr(observation, "fiscal_year", year)), observation)
            for year, observation in observations.items()
        ),
        key=lambda item: item[0],
    ):
        fiscal_year = int(getattr(observation, "fiscal_year", _raw_year))
        if fiscal_year not in historical_set:
            prior_observation = observation
            continue
        comparable, note = _effective_segment_observation_comparability(
            prior_observation, observation
        )
        if comparable == "not_applicable":
            prior_observation = observation
            continue
        if comparable == "not_comparable":
            mixed_periods.append(fiscal_year)
            if note:
                mixed_notes.append(str(note))
        elif comparable == "unknown":
            unknown_periods.append(fiscal_year)
        prior_observation = observation

    issues: list[ModelProjectionReadinessIssue] = []
    segment_name = getattr(segment_info, "name", None)
    if mixed_periods:
        detail = (
            f"segment {segment_id!r} has historical revenue observations on mixed reporting/recast bases; "
            "do not derive YoY growth from adjacent segment years until the basis is reconciled"
        )
        if mixed_notes:
            detail = f"{detail}: {'; '.join(sorted(set(mixed_notes)))}"
        issues.append(
            ModelProjectionReadinessIssue(
                code="segment_history_basis_mixed",
                severity="blocking",
                detail=detail,
                item_id=revenue_id,
                segment_id=segment_id,
                segment_name=segment_name,
                missing_periods=mixed_periods,
                related_item_ids=[revenue_id],
            )
        )
    if unknown_periods and not mixed_periods:
        issues.append(
            ModelProjectionReadinessIssue(
                code="segment_history_basis_unverified",
                severity="warning",
                detail=(
                    f"segment {segment_id!r} has historical revenue observations without enough "
                    "basis provenance to prove adjacent-year comparability"
                ),
                item_id=revenue_id,
                segment_id=segment_id,
                segment_name=segment_name,
                missing_periods=unknown_periods,
                related_item_ids=[revenue_id],
            )
        )
    return issues


def _segment_profile_basis_issues(
    model: FinancialModel,
    *,
    segment_profile: "SegmentProfile",
) -> list[ModelProjectionReadinessIssue]:
    historical_periods = _historical_periods(model)
    issues: list[ModelProjectionReadinessIssue] = []
    for index, segment in enumerate(segment_profile.segments, start=1):
        item_ids = getattr(segment, "item_ids", None) or {}
        revenue_id = item_ids.get("revenue_fm") or item_ids.get("revenue")
        if revenue_id is None:
            revenue_id = f"tpl.fm.income_statement.business_segment_{index}_revenue"
        if revenue_id not in model._index:
            continue
        issues.extend(
            _segment_history_basis_issues(
                segment,
                historical_periods=historical_periods,
                revenue_id=revenue_id,
                segment_id=f"segment_{index}",
            )
        )
    return issues


def _effective_segment_observation_comparability(
    prior: Any | None,
    observation: Any,
) -> tuple[str, str | None]:
    if prior is None:
        return "not_applicable", getattr(observation, "comparability_note", None)
    prior_basis = getattr(prior, "basis_key", None)
    basis = getattr(observation, "basis_key", None)
    explicit = getattr(observation, "comparable_with_prior", "unknown")
    note = getattr(observation, "comparability_note", None)
    if prior_basis and basis:
        if prior_basis != basis:
            return (
                "not_comparable",
                note or f"segment basis changed from {prior_basis!r} to {basis!r}",
            )
        if explicit == "not_comparable":
            return "not_comparable", note
        return "comparable", note
    if explicit == "not_comparable":
        return "not_comparable", note
    return (
        "unknown",
        note or "segment basis provenance is missing for adjacent-year comparability",
    )


def _split_segment_driver_gaps(
    model: FinancialModel,
    *,
    compiled_registry: "CompiledDriverRegistry",
    values: ComputedValues,
    segment_id: str,
    gaps: dict[str, list[int]],
    historical_periods: list[int],
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    structural: dict[str, list[int]] = {}
    seedable: dict[str, list[int]] = {}
    rate_item_ids = _segment_rate_item_ids(compiled_registry, segment_id)
    for item_id, missing in gaps.items():
        try:
            model.get_item(item_id)
        except KeyError:
            structural[item_id] = missing
            continue
        if item_id in rate_item_ids or _has_any_value(
            values.get(item_id, {}), historical_periods
        ):
            seedable[item_id] = missing
        else:
            structural[item_id] = missing
    return structural, seedable


def _segment_rate_item_ids(
    compiled_registry: "CompiledDriverRegistry",
    segment_id: str,
) -> set[str]:
    prefix = f"{segment_id}."
    return {
        item_id
        for driver_key, item_id in compiled_registry.driver_keys.items()
        if driver_key.startswith(prefix) and "." in driver_key[len(prefix) :]
    }


def _downgrade_primary_missing_for_seedable_bm(
    issues: list[ModelProjectionReadinessIssue],
) -> None:
    has_seedable_bm_gap = any(
        issue.severity == "warning"
        and issue.code
        in {"bm_driver_projection_seed_missing", "bm_segment_projection_missing"}
        for issue in issues
    )
    has_structural_bm_block = any(
        issue.severity == "blocking"
        and issue.code
        in {"bm_driver_projection_seed_missing", "bm_segment_projection_missing"}
        for issue in issues
    )
    if not has_seedable_bm_gap or has_structural_bm_block:
        return
    for issue in issues:
        if issue.code == "primary_projection_missing" and issue.severity == "blocking":
            issue.severity = "warning"
            issue.category = _issue_category(issue.code, issue.severity)
            issue.detail = (
                f"primary revenue item {issue.item_id!r} has no projected values "
                "until seedable BusinessModel projection assumptions are written"
            )


def _segment_driver_projection_gaps(
    model: FinancialModel,
    *,
    compiled_registry: "CompiledDriverRegistry",
    values: ComputedValues,
    segment_id: str,
    projection_periods: list[int],
) -> dict[str, list[int]]:
    prefix = f"{segment_id}."
    revenue_id = f"bm.{segment_id}.__rev"
    revenue_upstream_ids = _upstream_item_ids(model, revenue_id)
    candidate_ids: dict[str, None] = {}
    for registry_key, item_id in sorted(compiled_registry.node_items.items()):
        if registry_key.startswith(prefix):
            candidate_ids[item_id] = None
    for registry_key, item_id in sorted(compiled_registry.driver_keys.items()):
        if registry_key.startswith(prefix):
            candidate_ids[item_id] = None

    gaps: dict[str, list[int]] = {}
    for item_id in candidate_ids:
        if item_id not in revenue_upstream_ids:
            continue
        if item_id not in model._index:
            continue
        item = model.get_item(item_id)
        if item.item_type != ItemType.input:
            continue
        missing = _missing_periods(values.get(item_id, {}), projection_periods)
        if missing:
            gaps[item_id] = missing
    return gaps


def _union_missing_periods(gaps: dict[str, list[int]]) -> list[int]:
    periods: set[int] = set()
    for missing in gaps.values():
        periods.update(int(period) for period in missing)
    return sorted(periods)


def _segment_projection_related_item_ids(
    model: FinancialModel,
    *,
    compiled_registry: "CompiledDriverRegistry",
    values: ComputedValues,
    segment_id: str,
    projection_periods: list[int],
) -> list[str]:
    prefix = f"{segment_id}."
    blank_ids: list[str] = []
    for registry_key, item_id in sorted(compiled_registry.node_items.items()):
        if not registry_key.startswith(prefix) or item_id not in model._index:
            continue
        item = model.get_item(item_id)
        if item.item_type != ItemType.input and not item_id.endswith(
            ".segment_revenue"
        ):
            continue
        if not _has_any_value(values.get(item_id, {}), projection_periods):
            blank_ids.append(item_id)
    return blank_ids


_ROLLUP_COLLAPSE_ANCHORS: dict[str, tuple[str, ...]] = {
    "tpl.fm.adjusted_earnings.adjusted_operating_income": (
        "tpl.fm.adjusted_earnings.adjusted_ebitda",
        "tpl.fm.income_statement.operating_income",
    ),
}
_ROLLUP_REQUIRED_INPUT_ANCHORS: dict[str, tuple[str, ...]] = {
    "tpl.fm.income_statement.total_operating_expenses": (
        "tpl.fm.income_statement.operating_income",
        "tpl.fm.income_statement.total_revenue",
    ),
}


def _formula_rollup_missing_input_issues(
    model: FinancialModel,
    values: ComputedValues,
    projection_periods: list[int],
) -> list[ModelProjectionReadinessIssue]:
    issues: list[ModelProjectionReadinessIssue] = []
    active_periods = _historical_periods(model) + list(projection_periods)

    for item_id, anchor_ids in _ROLLUP_COLLAPSE_ANCHORS.items():
        if item_id not in model._index:
            continue
        item = model.get_item(item_id)
        if item.projected is None:
            continue

        direct_ref_ids = sorted(_formula_ref_item_ids(item.projected))
        missing_by_ref = {
            ref_id: missing
            for ref_id in direct_ref_ids
            if (missing := _missing_periods(values.get(ref_id, {}), projection_periods))
        }
        if not missing_by_ref:
            continue

        output_values = values.get(item_id, {})
        collapsed_periods: list[int] = []
        for period in projection_periods:
            output_value = output_values.get(period)
            if _is_material_nonzero(output_value):
                continue
            if not any(period in missing for missing in missing_by_ref.values()):
                continue
            if any(
                _is_material_nonzero(values.get(anchor_id, {}).get(period))
                for anchor_id in anchor_ids
            ):
                collapsed_periods.append(period)

        if not collapsed_periods:
            continue

        missing_ref_ids = [
            ref_id
            for ref_id, missing in missing_by_ref.items()
            if any(period in missing for period in collapsed_periods)
        ]
        issues.append(
            ModelProjectionReadinessIssue(
                code="formula_rollup_missing_inputs",
                severity="blocking",
                detail=(
                    f"derived rollup {item_id!r} collapses while upstream operating metrics "
                    f"are present; missing formula inputs: {', '.join(missing_ref_ids)}"
                ),
                item_id=item_id,
                missing_periods=collapsed_periods,
                related_item_ids=sorted(set(missing_ref_ids).union(anchor_ids)),
            )
        )

    for item_id, anchor_ids in _ROLLUP_REQUIRED_INPUT_ANCHORS.items():
        if item_id not in model._index:
            continue
        item = model.get_item(item_id)
        if item.projected is None:
            continue

        direct_ref_ids = sorted(
            ref_id
            for ref_id in _formula_ref_item_ids(item.projected)
            if _has_any_value(values.get(ref_id, {}), active_periods)
        )
        missing_by_ref = {
            ref_id: missing
            for ref_id in direct_ref_ids
            if (missing := _missing_periods(values.get(ref_id, {}), projection_periods))
        }
        if not missing_by_ref:
            continue

        output_values = values.get(item_id, {})
        incomplete_periods: list[int] = []
        for period in projection_periods:
            if not any(period in missing for missing in missing_by_ref.values()):
                continue
            if _is_material_nonzero(output_values.get(period)) or any(
                _is_material_nonzero(values.get(anchor_id, {}).get(period))
                for anchor_id in anchor_ids
            ):
                incomplete_periods.append(period)

        if not incomplete_periods:
            continue

        missing_ref_ids = [
            ref_id
            for ref_id, missing in missing_by_ref.items()
            if any(period in missing for period in incomplete_periods)
        ]
        issues.append(
            ModelProjectionReadinessIssue(
                code="formula_rollup_missing_inputs",
                severity="blocking",
                detail=(
                    f"derived rollup {item_id!r} has projected output while required "
                    f"formula inputs are missing: {', '.join(missing_ref_ids)}"
                ),
                item_id=item_id,
                missing_periods=incomplete_periods,
                related_item_ids=sorted(set(missing_ref_ids).union(anchor_ids)),
            )
        )

    return issues


def _formula_ref_item_ids(spec: FormulaSpec) -> set[str]:
    item_ids: set[str] = set()

    def visit(obj: Any) -> None:
        if isinstance(obj, LineItemRef):
            item_ids.add(obj.id)
            return
        if isinstance(obj, dict):
            coerced = line_item_ref_from_obj(obj)
            if coerced is not None:
                item_ids.add(coerced.id)
                return
            for value in obj.values():
                visit(value)
            return
        if isinstance(obj, (list, tuple)):
            for value in obj:
                visit(value)

    visit(spec.params)
    return item_ids


def _is_material_nonzero(value: Any) -> bool:
    return _is_present(value) and abs(float(value)) > 1e-9


def _issue_category(
    code: ProjectionReadinessIssueCode,
    severity: ProjectionReadinessSeverity,
) -> ProjectionReadinessIssueCategory:
    if severity == "blocking":
        return "blocking"
    if code in {
        "primary_projection_missing",
        "bm_driver_projection_seed_missing",
        "bm_segment_projection_missing",
    }:
        return "seedable"
    if code in {
        "segment_history_basis_unverified",
    }:
        return "operational_warning"
    return "blocking"


def _readiness_limitations(
    *,
    compiled_registry: "CompiledDriverRegistry | None",
) -> list[str]:
    limitations: list[str] = []
    if compiled_registry is None:
        limitations.append("bm_segment_projection_missing requires the build registry")
    return limitations


def _readiness_summary(
    status: ProjectionReadinessStatus, issues: list[ModelProjectionReadinessIssue]
) -> str:
    if status == "ready":
        return "projection formulas resolve for the primary model outputs"
    if not issues:
        return "projection readiness could not be determined"
    blocking = sum(1 for issue in issues if issue.severity == "blocking")
    warnings = len(issues) - blocking
    return f"{status}: {blocking} blocking issue(s), {warnings} warning issue(s)"


__all__ = [
    "ModelQualityDomain",
    "ModelQualityDomainReadiness",
    "ModelQualityReadiness",
    "ModelQualityReadinessIssue",
    "ModelQualityReadinessSeverity",
    "ModelQualityReadinessStatus",
    "ModelProjectionReadiness",
    "ModelProjectionReadinessIssue",
    "ModelScenarioBridgeOwnerReadiness",
    "ModelScenarioBridgeReadiness",
    "ModelScenarioBridgeReadinessIssue",
    "ModelScenarioOutputFieldReadiness",
    "ModelScenarioOutputReadiness",
    "ModelScenarioOutputReadinessIssue",
    "ProjectionReadinessIssueCategory",
    "ProjectionReadinessIssueCode",
    "ProjectionReadinessScope",
    "ProjectionReadinessSeverity",
    "ProjectionReadinessStatus",
    "ScenarioBridgeReadinessIssueCategory",
    "ScenarioBridgeReadinessIssueCode",
    "ScenarioBridgeReadinessScope",
    "ScenarioBridgeReadinessSeverity",
    "ScenarioBridgeReadinessStatus",
    "ScenarioOutputReadinessIssueCategory",
    "ScenarioOutputReadinessIssueCode",
    "ScenarioOutputReadinessScope",
    "ScenarioOutputReadinessSeverity",
    "ScenarioOutputReadinessStatus",
    "ValuationInputReadiness",
    "ValuationInputReadinessSeverity",
    "ValuationInputReadinessStatus",
    "compute_model_quality_readiness",
    "compute_model_projection_readiness",
    "compute_model_scenario_bridge_readiness",
    "compute_model_scenario_output_readiness",
    "compute_valuation_input_readiness",
]
