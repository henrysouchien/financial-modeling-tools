from __future__ import annotations

from typing import Any, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .dependency_graph import DependencyGraph as DependencyGraph
from .model_readiness_common import (
    _computed_values,
    _has_any_value,
    _historical_periods,
    _is_present,
    _latest_numeric_value as _latest_numeric_value,
    _missing_periods,
    _nearly_equal as _nearly_equal,
    _normalize_computed_values,  # noqa: F401 - compatibility alias for schema.model_readiness imports
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
    _valuation_input_status,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    compute_valuation_input_readiness,
)
from .model_readiness_quality import (
    ModelQualityDomain,
    ModelQualityDomainReadiness,
    ModelQualityReadiness,
    ModelQualityReadinessDomain,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    ModelQualityReadinessIssue,
    ModelQualityReadinessSeverity,
    ModelQualityReadinessStatus,
    _dependency_issue,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _inventory_material,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _model_quality_summary,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _quality_any_projection_issue,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _quality_domain,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _quality_projection_issue,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _segment_basis_quality_domain,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _share_count_quality_domain,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _upstream_item_ids,
    _valuation_quality_domain,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _working_capital_quality_domain,  # noqa: F401 - compatibility alias for schema.model_readiness imports
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
    _SCENARIO_OUTPUT_REQUIREMENTS,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_output_issue_category,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_output_summary,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _select_scenario_output_item_id,  # noqa: F401 - compatibility alias for schema.model_readiness imports
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
    _DEFAULT_SCENARIO_BRIDGE_TARGET_ID,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _SCENARIO_CASE_PATTERNS,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _SCENARIO_CASES,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _find_item_location,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _find_scenario_value_row,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _graph_downstream_ids,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _is_offset_scenario_owner,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _normalize_scenario_label,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _offset_anchor_ids,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_bridge_anchor_id,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_bridge_case_row_ids,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_bridge_issue_category,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_bridge_owners,  # noqa: F401 - compatibility alias for schema.model_readiness imports
    _scenario_bridge_summary,  # noqa: F401 - compatibility alias for schema.model_readiness imports
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
ProjectionReadinessIssueCategory = Literal["seedable", "operational_warning", "blocking"]
ProjectionReadinessIssueCode = Literal[
    "primary_projection_item_missing",
    "primary_projection_missing",
    "primary_projection_incomplete",
    "bm_driver_projection_seed_missing",
    "bm_segment_projection_missing",
    "segment_history_basis_mixed",
    "segment_history_basis_unverified",
    "projection_override_validation_error",
    "stale_projection_overrides",
    "projection_override_warnings",
    "formula_rollup_missing_inputs",
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
    seed_projections: Any | None = None,
    computed_values: dict[str, dict[int, float]] | None = None,
) -> ModelProjectionReadiness:
    """Return an agent-facing projection-readiness summary for a built model."""

    model.build_index()
    scope: ProjectionReadinessScope = (
        "build_contract"
        if compiled_registry is not None or segment_profile is not None or seed_projections is not None
        else "workbook_formula"
    )
    limitations = _readiness_limitations(
        compiled_registry=compiled_registry,
        seed_projections=seed_projections,
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

    issues.extend(_seed_projection_issues(seed_projections))
    issues.extend(_formula_rollup_missing_input_issues(model, values, projection_periods))

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
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
) -> list[ModelProjectionReadinessIssue]:
    issues: list[ModelProjectionReadinessIssue] = []
    historical_periods = _historical_periods(model)
    segment_profile = compiled_registry.segment_profile

    for segment_id, segment_index in sorted(compiled_registry.segment_mapping.items(), key=lambda item: item[1]):
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
        elif has_actuals and seedable_gaps:
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
                severity="blocking" if structural_gaps or not seedable_gaps else "warning",
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
        ((int(getattr(observation, "fiscal_year", year)), observation) for year, observation in observations.items()),
        key=lambda item: item[0],
    ):
        fiscal_year = int(getattr(observation, "fiscal_year", _raw_year))
        if fiscal_year not in historical_set:
            prior_observation = observation
            continue
        comparable, note = _effective_segment_observation_comparability(prior_observation, observation)
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
            return "not_comparable", note or f"segment basis changed from {prior_basis!r} to {basis!r}"
        if explicit == "not_comparable":
            return "not_comparable", note
        return "comparable", note
    if explicit == "not_comparable":
        return "not_comparable", note
    return "unknown", note or "segment basis provenance is missing for adjacent-year comparability"


def _split_segment_driver_gaps(
    model: FinancialModel,
    *,
    compiled_registry: "CompiledDriverRegistry",
    values: dict[str, dict[int, float]],
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
        if item_id in rate_item_ids or _has_any_value(values.get(item_id, {}), historical_periods):
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
        if driver_key.startswith(prefix)
        and "." in driver_key[len(prefix):]
    }


def _downgrade_primary_missing_for_seedable_bm(
    issues: list[ModelProjectionReadinessIssue],
) -> None:
    has_seedable_bm_gap = any(
        issue.severity == "warning"
        and issue.code in {"bm_driver_projection_seed_missing", "bm_segment_projection_missing"}
        for issue in issues
    )
    has_structural_bm_block = any(
        issue.severity == "blocking"
        and issue.code in {"bm_driver_projection_seed_missing", "bm_segment_projection_missing"}
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
    values: dict[str, dict[int, float]],
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
    values: dict[str, dict[int, float]],
    segment_id: str,
    projection_periods: list[int],
) -> list[str]:
    prefix = f"{segment_id}."
    blank_ids: list[str] = []
    for registry_key, item_id in sorted(compiled_registry.node_items.items()):
        if not registry_key.startswith(prefix) or item_id not in model._index:
            continue
        item = model.get_item(item_id)
        if item.item_type != ItemType.input and not item_id.endswith(".segment_revenue"):
            continue
        if not _has_any_value(values.get(item_id, {}), projection_periods):
            blank_ids.append(item_id)
    return blank_ids


def _seed_projection_issues(seed_projections: Any | None) -> list[ModelProjectionReadinessIssue]:
    if seed_projections is None:
        return []

    validation_error = getattr(seed_projections, "validation_error", None)
    if validation_error:
        return [
            ModelProjectionReadinessIssue(
                code="projection_override_validation_error",
                severity="blocking",
                detail=str(validation_error),
            )
        ]

    issues: list[ModelProjectionReadinessIssue] = []
    total_rate_keys = int(getattr(seed_projections, "total_rate_keys", 0) or 0)
    orphans = list(getattr(seed_projections, "orphans", []) or [])
    if total_rate_keys and orphans:
        item_miss_reasons = {"bm_key_not_in_registry", "tpl_item_not_found", "item_not_found"}
        item_miss_orphans = [
            orphan for orphan in orphans if getattr(orphan, "reason", None) in item_miss_reasons
        ]
        if item_miss_orphans:
            ratio = len(item_miss_orphans) / total_rate_keys
            severity: ProjectionReadinessSeverity = "blocking" if ratio > 0.5 else "warning"
            issues.append(
                ModelProjectionReadinessIssue(
                    code="stale_projection_overrides",
                    severity=severity,
                    detail=(
                        f"{len(item_miss_orphans)}/{total_rate_keys} projection override keys "
                        "do not exist in this build"
                    ),
                    related_item_ids=[str(getattr(orphan, "rate_key", "")) for orphan in item_miss_orphans[:20]],
                )
            )

    warnings = list(getattr(seed_projections, "warnings", []) or [])
    if warnings:
        issues.append(
            ModelProjectionReadinessIssue(
                code="projection_override_warnings",
                severity="warning",
                detail=f"{len(warnings)} projection override warning(s) were emitted during build",
                related_item_ids=[str(getattr(warning, "rate_key", "")) for warning in warnings[:20]],
            )
        )
    return issues


_ROLLUP_COLLAPSE_ANCHORS: dict[str, tuple[str, ...]] = {
    "tpl.fm.adjusted_earnings.adjusted_operating_income": (
        "tpl.fm.adjusted_earnings.adjusted_ebitda",
        "tpl.fm.income_statement.operating_income",
    ),
}


def _formula_rollup_missing_input_issues(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
) -> list[ModelProjectionReadinessIssue]:
    issues: list[ModelProjectionReadinessIssue] = []

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
            if any(_is_material_nonzero(values.get(anchor_id, {}).get(period)) for anchor_id in anchor_ids):
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
        "stale_projection_overrides",
        "projection_override_warnings",
        "segment_history_basis_unverified",
    }:
        return "operational_warning"
    return "blocking"


def _readiness_limitations(
    *,
    compiled_registry: "CompiledDriverRegistry | None",
    seed_projections: Any | None,
) -> list[str]:
    limitations: list[str] = []
    if compiled_registry is None:
        limitations.append("bm_segment_projection_missing requires the build registry")
    if seed_projections is None:
        limitations.append("stale_projection_overrides requires build seed-projection metadata")
    return limitations


def _readiness_summary(status: ProjectionReadinessStatus, issues: list[ModelProjectionReadinessIssue]) -> str:
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
