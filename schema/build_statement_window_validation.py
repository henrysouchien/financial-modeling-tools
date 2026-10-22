"""Structural validators for reviewed multi-basis windows.

Each year is validated against its reviewed basis segment. Pre-population
checks topology usability and row presence; post-compute checks required-role
service, active formula membership, excluded rows, and section reconciliation.
Only the windows the build plan applied are validated; a window it did not
apply is a blocking ``build_plan`` gap on the result (its Finding is the
plan's). A blocking gap never raises: the result carries every gap with
``enforcement_status="fail"``, and :func:`statement_window_validation_findings`
reads the validation gaps as ``statement_window_validation_failed`` Findings.

Producer selection and adapter-asserted filing agreement do not live here.
The bundle producer owns that pin policy and emits the current non-blocking
assertion diagnostic rung, so this validator must not recreate a binding
provider or per-row filing-value assertion.
"""

from __future__ import annotations

import copy
from dataclasses import asdict
import math
from typing import TYPE_CHECKING, Any, Literal, Mapping

from .build_findings import make_finding
from .build_statement_windows import (
    StatementWindowBuildPlan,
    StatementWindowPlanIssue,
    inspect_statement_window_formula_membership,
)
from .finding_vocabulary import Finding
from .handle import ComputedValues
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
)
from .multi_basis_window import BasisSegment, SegmentRowBinding
from .statement_tolerance_profiles import (
    exceeds_reconciliation_tolerance,
    is_material_unmapped_child,
    require_tolerance_profile,
)
from .statement_topology import StatementTopology, TopologyEdge
from .statement_windows import (
    CompiledStatementWindows,
    CompiledWindowPlan,
    StatementWindowsResult,
)

if TYPE_CHECKING:
    from .build_types import PopulateStats


GapStage = Literal["build_plan", "pre_population", "post_compute"]


def _new_result(
    compiled: CompiledStatementWindows,
    build_plan: StatementWindowBuildPlan,
    topology: StatementTopology,
) -> StatementWindowsResult:
    result = copy.deepcopy(compiled.report)
    result.population_dispositions = [
        asdict(entry) for entry in build_plan.population_dispositions
    ]
    result.materialized = copy.deepcopy(
        list(build_plan.materialization.get("materialized", []))
    )
    result.suppressed_formula_members = [
        asdict(entry) for entry in build_plan.formula_suppressions
    ]
    result.discarded_duplicate_observations = [
        observation.to_dict()
        for observation in topology.discarded_duplicate_observations
    ]
    result.enforcement_status = "not_run"
    return result


def _record_blocker(
    result: StatementWindowsResult,
    code: str,
    detail: str,
    *,
    stage: GapStage,
    window_id: str,
    year: int | None = None,
    concept_id: str | None = None,
    item_id: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> None:
    issue: dict[str, Any] = {
        "code": code,
        "stage": stage,
        "window_id": window_id,
        "detail": detail,
        "blocking": True,
    }
    if year is not None:
        issue["year"] = int(year)
    if concept_id is not None:
        issue["concept_id"] = concept_id
    if item_id is not None:
        issue["item_id"] = item_id
    if extra:
        issue.update(copy.deepcopy(dict(extra)))
    result.gaps.append(issue)


def _record_plan_issue(
    result: StatementWindowsResult,
    issue: StatementWindowPlanIssue,
    *,
    stage: GapStage,
) -> None:
    _record_blocker(
        result,
        issue.issue_code,
        issue.detail,
        stage=stage,
        window_id=issue.window_id,
        year=issue.period,
        item_id=issue.item_id,
    )


def _has_blocker(result: StatementWindowsResult) -> bool:
    return any(issue.get("blocking", True) for issue in result.gaps)


def _applied_section_plans(
    compiled: CompiledStatementWindows,
    build_plan: StatementWindowBuildPlan,
) -> list[tuple[str, CompiledWindowPlan]]:
    """The windows the build plan applied, in window order."""

    return [
        (window_id, compiled.section_plans[window_id])
        for window_id in build_plan.applied_window_ids
    ]


def statement_window_validation_findings(
    result: StatementWindowsResult,
) -> tuple[Finding, ...]:
    """``statement_window_validation_failed`` for the blocking validation gaps.

    Read the final result once: the post-compute result carries the
    pre-population gaps. ``build_plan`` gaps are not read here — the plan's
    own Findings (``StatementWindowBuildPlan.findings``) report them. Gaps that
    share a Finding identity (the gap's item, else its window, and its year)
    are one Finding naming every gap, so collapsing findings loses no message.
    """

    grouped: dict[tuple[str, tuple[int, ...]], list[Mapping[str, Any]]] = {}
    for gap in result.gaps:
        if not gap.get("blocking", True) or gap["stage"] == "build_plan":
            continue
        item_id = str(gap.get("item_id") or gap["window_id"])
        periods = () if gap.get("year") is None else (int(gap["year"]),)
        grouped.setdefault((item_id, periods), []).append(gap)
    findings: list[Finding] = []
    for (item_id, periods), gaps in grouped.items():
        window_ids = sorted({str(gap["window_id"]) for gap in gaps})
        findings.append(
            make_finding(
                "statement_window_validation_failed",
                message=(
                    f"Statement window {', '.join(window_ids)} failed validation: "
                    + "; ".join(f"{gap['code']}: {gap['detail']}" for gap in gaps)
                ),
                item_id=item_id,
                periods=periods,
                observed={
                    "issue_code": ", ".join(
                        sorted({str(gap["code"]) for gap in gaps})
                    ),
                    "stage": ", ".join(sorted({str(gap["stage"]) for gap in gaps})),
                    "window_id": ", ".join(window_ids),
                },
            )
        )
    return tuple(findings)


def _segment_for_year(plan: CompiledWindowPlan, year: int) -> BasisSegment:
    segment = plan.window.segment_for(int(year))
    if segment is None:  # pragma: no cover - the window check runs first
        raise ValueError(
            f"statement window {plan.window_id!r} covers no segment for FY{year}"
        )
    return segment


def _edge_index(edges: tuple[TopologyEdge, ...]) -> dict[str, list[TopologyEdge]]:
    indexed: dict[str, list[TopologyEdge]] = {}
    for edge in edges:
        indexed.setdefault(edge.child_tag, []).append(edge)
    return indexed


def _presentation_edges(
    topology: StatementTopology,
    segment: BasisSegment,
    year: int,
) -> tuple[TopologyEdge, ...]:
    edges: list[TopologyEdge] = []
    for parent_tag in segment.topology_roles.presentation_parent_tags:
        edges.extend(
            topology.presentation_children_of(
                parent_tag,
                year,
                presentation_roles=segment.topology_roles.presentation_roles,
            )
        )
    return tuple(edges)


def _covered_descendant_edges(
    topology: StatementTopology,
    segment: BasisSegment,
    *,
    selected_tag: str,
    year: int,
    same_role_calculation_roles: list[str],
) -> tuple[TopologyEdge, ...]:
    """Traverse same-role coverage plus reviewed transitions at the selected QName."""

    policy = segment.topology_roles
    traversals = [
        topology.descendants_of(
            selected_tag,
            year,
            edge_kind="calculation",
            roles=same_role_calculation_roles,
        )
    ]
    if policy.coverage_calculation_roles:
        traversals.append(
            topology.descendants_of(
                selected_tag,
                year,
                edge_kind="calculation",
                roles=list(policy.coverage_calculation_roles),
            )
        )
    by_edge = {
        (edge.role, edge.parent_tag, edge.child_tag): edge
        for traversal in traversals
        for edge in traversal
    }
    return tuple(by_edge[key] for key in sorted(by_edge))


def _row_is_expected(
    *,
    presence: str,
    tag: str,
    calculation_index: Mapping[str, list[TopologyEdge]],
    presentation_index: Mapping[str, list[TopologyEdge]],
) -> bool:
    if presence == "presented_all_years":
        return True
    return bool(calculation_index.get(tag) or presentation_index.get(tag))


def _basis_reasons(
    *,
    edge_kind: str,
    edges: tuple[TopologyEdge, ...],
    segment: BasisSegment,
    topology: StatementTopology,
    year: int,
    calculation_index: Mapping[str, list[TopologyEdge]],
    presentation_index: Mapping[str, list[TopologyEdge]],
) -> list[str]:
    reasons: list[str] = []
    if not edges:
        reasons.append("no_reviewed_edges")
    edge_index = _edge_index(edges)
    for row_role, disposition in sorted(segment.rows.items()):
        if not isinstance(disposition, SegmentRowBinding):
            # A DECLARED absence is not a missing edge: the segment says this
            # role has no line in these filings, so its absence can never make
            # a basis unusable.
            continue
        tag = disposition.tag
        if not _row_is_expected(
            presence=disposition.presence,
            tag=tag,
            calculation_index=calculation_index,
            presentation_index=presentation_index,
        ):
            continue
        matches = edge_index.get(tag, [])
        if len(matches) != 1:
            reasons.append(f"{row_role}:edge_count={len(matches)}")
            continue
        source_value = matches[0].source_value
        if source_value is None or source_value.normalized_value is None:
            reasons.append(f"{row_role}:value_unresolved")
        if edge_kind == "calculation" and matches[0].weight is None:
            reasons.append(f"{row_role}:weight_unresolved")
    total_value = topology.source_value(segment.section_total_tag, year)
    if total_value is None or total_value.normalized_value is None:
        reasons.append("section_total:value_unresolved")
    return reasons


def _mapped_topology_value(normalized_value: float | None) -> float | None:
    """Return the pin-normalized value; producer adapters own sign normalization."""

    return None if normalized_value is None else float(normalized_value)


def validate_statement_window_pre_population(
    *,
    compiled_statement_windows: CompiledStatementWindows,
    build_plan: StatementWindowBuildPlan,
    statement_topology: StatementTopology,
) -> StatementWindowsResult:
    """Validate targets, topology, and formula plans before any source write.

    Each build-plan issue is a blocking ``build_plan`` gap; only the applied
    windows are validated.
    """

    result = _new_result(
        compiled_statement_windows,
        build_plan,
        statement_topology,
    )
    for issue in build_plan.issues:
        _record_plan_issue(result, issue, stage="build_plan")
    historical_years = sorted(
        {
            entry.period
            for entry in build_plan.formula_membership
            if entry.graph_kind == "historical"
        }
    )
    for window_id, section_plan in _applied_section_plans(
        compiled_statement_windows,
        build_plan,
    ):
        window = section_plan.window
        profile = require_tolerance_profile(
            window.tolerance_profile_id,
            statement=window.statement,
        )
        window_coverage = result.coverage.setdefault(window_id, {})
        by_year = window_coverage.setdefault("by_year", {})
        # The years this build actually covers, declared by the producer beside
        # the window it covers them under. The window can be longer than the
        # build (a five-year window feeding a two-year model is ordinary), so
        # "coverage equals the declared window" is no longer the closure —
        # "coverage equals the years this build declared it covered" is.
        for entry in result.compiled:
            if entry.get("window_id") == window_id:
                entry["covered_fiscal_years"] = list(historical_years)
        for year in historical_years:
            segment = _segment_for_year(section_plan, year)
            calculation_edges = statement_topology.additive_children_of(
                segment.topology_roles.calculation_parent_tag,
                year,
                calculation_roles=list(
                    segment.topology_roles.basis_calculation_roles
                ),
            )
            presentation_edges = _presentation_edges(
                statement_topology,
                segment,
                year,
            )
            calculation_index = _edge_index(calculation_edges)
            presentation_index = _edge_index(presentation_edges)
            calculation_reasons = _basis_reasons(
                edge_kind="calculation",
                edges=calculation_edges,
                segment=segment,
                topology=statement_topology,
                year=year,
                calculation_index=calculation_index,
                presentation_index=presentation_index,
            )
            presentation_reasons = _basis_reasons(
                edge_kind="presentation",
                edges=presentation_edges,
                segment=segment,
                topology=statement_topology,
                year=year,
                calculation_index=calculation_index,
                presentation_index=presentation_index,
            )
            if not calculation_reasons:
                basis = "calculation"
                basis_edges = calculation_edges
            elif not presentation_reasons:
                basis = "presentation_fallback"
                basis_edges = presentation_edges
            else:
                basis = "unavailable"
                basis_edges = ()

            total_source_value = statement_topology.source_value(
                segment.section_total_tag,
                year,
            )
            year_result: dict[str, Any] = {
                "segment": {
                    "from_fy": segment.from_fy,
                    "through_fy": segment.through_fy,
                    "basis_kind": segment.basis.kind,
                    "section_total_tag": segment.section_total_tag,
                },
                "topology_basis": basis,
                "calculation_unusable_reasons": calculation_reasons,
                "presentation_unusable_reasons": presentation_reasons,
                "reported_total": (
                    None
                    if total_source_value is None
                    else _mapped_topology_value(total_source_value.normalized_value)
                ),
                "selected_rows": [],
                "covered_descendants": [],
                "reconciliation": {"status": "not_run"},
            }
            by_year[str(year)] = year_result
            if basis == "unavailable":
                _record_blocker(
                    result,
                    "topology_basis_unavailable",
                    "neither reviewed calculation nor presentation topology is structurally usable",
                    stage="pre_population",
                    window_id=window_id,
                    year=year,
                    extra={
                        "calculation_reasons": calculation_reasons,
                        "presentation_reasons": presentation_reasons,
                    },
                )
                continue

            basis_index = _edge_index(basis_edges)
            eligible_tags: set[str] = set()
            covered_tags: set[str] = set()
            selected_tag_to_concept: dict[str, str] = {}
            for row_role in sorted(window.bound_roles):
                disposition = segment.rows.get(row_role)
                if not isinstance(disposition, SegmentRowBinding):
                    # Declared absent in THIS segment while bound in another:
                    # a typed outcome, never a zero and never a blocker.
                    year_result["selected_rows"].append(
                        {
                            "concept_id": row_role,
                            "tag": None,
                            "disposition": "not_presented",
                            "presence_policy": "not_presented",
                            "eligible": False,
                            "value": None,
                            "role": None,
                            "weight": None,
                        }
                    )
                    continue
                tag = disposition.tag
                selected_tag_to_concept[tag] = row_role
                expected = _row_is_expected(
                    presence=disposition.presence,
                    tag=tag,
                    calculation_index=calculation_index,
                    presentation_index=presentation_index,
                )
                matches = basis_index.get(tag, [])
                source_value = (
                    matches[0].source_value
                    if len(matches) == 1
                    else statement_topology.source_value(tag, year)
                )
                eligible = bool(
                    expected
                    and len(matches) == 1
                    and source_value is not None
                    and source_value.normalized_value is not None
                )
                if eligible:
                    eligible_tags.add(tag)
                year_result["selected_rows"].append(
                    {
                        "concept_id": row_role,
                        "tag": tag,
                        "disposition": "bound",
                        "presence_policy": disposition.presence,
                        "eligible": eligible,
                        "value": (
                            None
                            if source_value is None
                            else _mapped_topology_value(
                                source_value.normalized_value
                            )
                        ),
                        "role": matches[0].role if len(matches) == 1 else None,
                        "weight": matches[0].weight if len(matches) == 1 else None,
                    }
                )
                if expected and not eligible:
                    _record_blocker(
                        result,
                        "topology_relationship_mismatch",
                        "selected fact is absent or ambiguous in the chosen topology basis",
                        stage="pre_population",
                        window_id=window_id,
                        year=year,
                        concept_id=row_role,
                        extra={"tag": tag, "topology_basis": basis},
                    )
                if (
                    basis == "calculation"
                    and eligible
                    and matches[0].weight != disposition.expected_weight
                ):
                    _record_blocker(
                        result,
                        "calculation_weight_mismatch",
                        "selected additive edge does not have the reviewed invariant weight",
                        stage="pre_population",
                        window_id=window_id,
                        year=year,
                        concept_id=row_role,
                        extra={
                            "tag": tag,
                            "actual_weight": matches[0].weight,
                            "expected_weight": disposition.expected_weight,
                        },
                    )
                if disposition.coverage == "subtree" and eligible:
                    calculation_matches = calculation_index.get(tag, [])
                    same_role_calculation_roles = (
                        [calculation_matches[0].role]
                        if len(calculation_matches) == 1
                        else list(segment.topology_roles.basis_calculation_roles)
                    )
                    descendants = _covered_descendant_edges(
                        statement_topology,
                        segment,
                        selected_tag=tag,
                        year=year,
                        same_role_calculation_roles=same_role_calculation_roles,
                    )
                    descendant_tags = sorted({edge.child_tag for edge in descendants})
                    covered_tags.update(descendant_tags)
                    year_result["covered_descendants"].append(
                        {
                            "concept_id": row_role,
                            "ancestor_tag": tag,
                            "descendant_tags": descendant_tags,
                        }
                    )

            for covered_tag in sorted(covered_tags & eligible_tags):
                _record_blocker(
                    result,
                    "ancestor_descendant_overlap",
                    "a covered descendant is also an eligible additive selected row",
                    stage="pre_population",
                    window_id=window_id,
                    year=year,
                    concept_id=selected_tag_to_concept.get(covered_tag),
                    extra={"tag": covered_tag},
                )

            if total_source_value is not None:
                reported_total = total_source_value.normalized_value
                if reported_total is not None:
                    selected_tags = set(selected_tag_to_concept)
                    for edge in basis_edges:
                        if edge.child_tag in selected_tags:
                            continue
                        normalized = edge.source_value.normalized_value
                        if normalized is None or not is_material_unmapped_child(
                            normalized,
                            reported_total,
                            profile=profile,
                        ):
                            continue
                        _record_blocker(
                            result,
                            "unmapped_direct_child",
                            "material direct calculation child is outside the authoritative basis",
                            stage="pre_population",
                            window_id=window_id,
                            year=year,
                            extra={
                                "tag": edge.child_tag,
                                "value": normalized,
                                "role": edge.role,
                            },
                        )

    if _has_blocker(result):
        result.enforcement_status = "fail"
    return result



def statement_window_population_years(
    *,
    compiled_statement_windows: CompiledStatementWindows,
    build_plan: StatementWindowBuildPlan,
    pre_result: StatementWindowsResult,
) -> dict[str, frozenset[int]]:
    """Return per-concept years eligible for model writes after topology review.

    Only the applied windows' roles are named: a role absent here is not
    narrowed by any window.
    """

    eligible: dict[str, set[int]] = {}
    for window_id, section_plan in _applied_section_plans(
        compiled_statement_windows,
        build_plan,
    ):
        by_year = pre_result.coverage[window_id]["by_year"]
        total_id = section_plan.section_total_required_role
        eligible.setdefault(total_id, set()).update(int(year) for year in by_year)
        for year, year_result in by_year.items():
            for row in year_result.get("selected_rows", []):
                concept_id = str(row["concept_id"])
                eligible.setdefault(concept_id, set())
                if row.get("eligible"):
                    eligible[concept_id].add(int(year))
    return {
        concept_id: frozenset(sorted(years))
        for concept_id, years in sorted(eligible.items())
    }


def _active_period_spec(
    model: FinancialModel,
    item: LineItem,
    period: int,
) -> tuple[FormulaSpec | None, bool]:
    override = (item.overrides or {}).get(int(period))
    if override is not None:
        return override, True
    if item.formula_periods is not None and int(period) not in {
        int(active_period) for active_period in item.formula_periods
    }:
        return None, False
    historical_periods = {
        int(active_period)
        for active_period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
        )
    }
    projection_periods = {
        int(active_period)
        for active_period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
        )
    }
    if int(period) in historical_periods:
        return item.historical, False
    if int(period) in projection_periods:
        return item.projected, False
    return item.projected or item.historical, False


def _effective_value(
    model: FinancialModel,
    item: LineItem,
    period: int,
    computed_values: ComputedValues,
) -> float | None:
    active_spec, from_override = _active_period_spec(model, item, period)
    if (
        active_spec is not None
        and from_override
        and active_spec.type is FormulaType.constant
    ):
        value = active_spec.params.get("value")
        return None if value is None else float(value)
    if active_spec is not None and active_spec.type is not FormulaType.constant:
        value = computed_values.get(item.id, {}).get(int(period))
        return None if value is None else float(value)
    if item.values is not None:
        cell = item.values.values.get(int(period))
        if cell is not None and cell.value is not None:
            return float(cell.value)
    value = computed_values.get(item.id, {}).get(int(period))
    return None if value is None else float(value)


def _year_binding_rows(
    section_plan: CompiledWindowPlan,
    year: int,
) -> list[tuple[str, str]]:
    """Return ``(required_role, presence)`` for one basis year."""

    window = section_plan.window
    segment = _segment_for_year(section_plan, year)
    rows = [
        (
            section_plan.section_total_required_role,
            "presented_all_years",
        )
    ]
    for row_role in sorted(window.bound_roles):
        disposition = segment.rows.get(row_role)
        if isinstance(disposition, SegmentRowBinding):
            rows.append((row_role, disposition.presence))
        else:
            rows.append((row_role, "not_presented"))
    return rows


def validate_statement_window_post_compute(
    model: FinancialModel,
    *,
    compiled_statement_windows: CompiledStatementWindows,
    build_plan: StatementWindowBuildPlan,
    statement_topology: StatementTopology,
    stats: PopulateStats,
    computed_values: ComputedValues,
    pre_result: StatementWindowsResult,
) -> StatementWindowsResult:
    """Validate required values, active graphs, excluded rows, and exact rollups."""

    result = copy.deepcopy(pre_result)
    # Row checks read the model the policies left. The plan is frozen before
    # population; the company-grounded policies may then take a source-empty
    # row out of the model (a role-less placeholder, or an optional face row
    # the issuer's profile does not declare), and a row that is gone carries no
    # value, override or contribution to check.
    model_item_ids = {
        item.id
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    }
    expected_membership = {
        (entry.window_id, entry.graph_kind, entry.period): entry
        for entry in build_plan.formula_membership
    }
    actual_membership_entries, inspection_issues = (
        inspect_statement_window_formula_membership(
            model,
            compiled_statement_windows=compiled_statement_windows,
            build_plan=build_plan,
        )
    )
    for issue in inspection_issues:
        _record_plan_issue(result, issue, stage="post_compute")
    uninspected_window_ids = {issue.window_id for issue in inspection_issues}

    for actual in actual_membership_entries:
        key = (actual.window_id, actual.graph_kind, actual.period)
        expected = expected_membership.get(key)
        membership_matches = expected is not None and not (
            actual.selected_paths != expected.selected_paths
            or actual.active_member_item_ids != expected.active_member_item_ids
            or actual.suppressed_member_item_ids
            != expected.suppressed_member_item_ids
        )
        if not membership_matches:
            _record_blocker(
                result,
                "formula_membership_drift",
                "active formula membership differs from the enforced pre-population plan",
                stage="post_compute",
                window_id=actual.window_id,
                year=actual.period,
                extra={"graph_kind": actual.graph_kind},
            )
        mapping_coverage = result.coverage.setdefault(actual.window_id, {})
        if actual.graph_kind == "historical":
            year_result = mapping_coverage.setdefault("by_year", {}).setdefault(
                str(actual.period),
                {},
            )
            year_result["formula_membership"] = {
                "status": "pass" if membership_matches else "fail",
                "selected_paths": [asdict(path) for path in actual.selected_paths],
                "constant_overrides": list(actual.constant_overrides),
            }
        else:
            mapping_coverage.setdefault("projection_formula_membership", {})[
                str(actual.period)
            ] = {
                "status": "pass" if membership_matches else "fail",
                "selected_paths": [asdict(path) for path in actual.selected_paths],
                "constant_overrides": list(actual.constant_overrides),
            }

    actual_keys = {
        (entry.window_id, entry.graph_kind, entry.period)
        for entry in actual_membership_entries
    }
    inspected_expected_keys = {
        key for key in expected_membership if key[0] not in uninspected_window_ids
    }
    for missing_key in sorted(inspected_expected_keys - actual_keys):
        _record_blocker(
            result,
            "formula_membership_plan_missing",
            "an enforced formula-membership period was not inspectable post-compute",
            stage="post_compute",
            window_id=missing_key[0],
            year=missing_key[2],
            extra={"graph_kind": missing_key[1]},
        )


    historical_years = sorted(
        {
            entry.period
            for entry in build_plan.formula_membership
            if entry.graph_kind == "historical"
        }
    )
    for window_id, section_plan in _applied_section_plans(
        compiled_statement_windows,
        build_plan,
    ):
        window = section_plan.window
        profile = require_tolerance_profile(
            window.tolerance_profile_id,
            statement=window.statement,
        )
        by_year = result.coverage[window_id]["by_year"]
        for year in historical_years:
            year_result = by_year[str(year)]
            eligible_roles = {
                row["concept_id"]
                for row in year_result.get("selected_rows", [])
                if row.get("eligible")
            }
            for required_role, presence in _year_binding_rows(section_plan, year):
                required = (
                    required_role == section_plan.section_total_required_role
                    or presence == "presented_all_years"
                    or required_role in eligible_roles
                )
                served = stats.served_source_by_role_year.get(
                    required_role,
                    {},
                ).get(year)
                if served is None:
                    if required:
                        _record_blocker(
                            result,
                            "required_fact_unserved",
                            "required or presented role was not served",
                            stage="post_compute",
                            window_id=window_id,
                            year=year,
                            concept_id=required_role,
                        )
                    continue
                item = model.get_item(build_plan.item_id_by_role[required_role])
                effective_value = _effective_value(model, item, year, computed_values)
                if required and (
                    effective_value is None or not math.isfinite(effective_value)
                ):
                    _record_blocker(
                        result,
                        "required_fact_unserved",
                        "required or presented role has no finite effective value",
                        stage="post_compute",
                        window_id=window_id,
                        year=year,
                        concept_id=required_role,
                        item_id=item.id,
                    )
            selected_sum = 0.0
            selected_values: dict[str, float] = {}
            for row in year_result.get("selected_rows", []):
                if not row.get("eligible"):
                    continue
                required_role = str(row["concept_id"])
                item = model.get_item(build_plan.item_id_by_role[required_role])
                value = _effective_value(model, item, year, computed_values)
                if value is None or not math.isfinite(value):
                    continue
                selected_values[required_role] = value
                selected_sum += value

            reported_total = year_result.get("reported_total")
            if reported_total is not None:
                gap = selected_sum - float(reported_total)
                mismatch = exceeds_reconciliation_tolerance(
                    gap,
                    float(reported_total),
                    profile=profile,
                )
                year_result["reconciliation"] = {
                    "status": "fail" if mismatch else "pass",
                    "reported_total": float(reported_total),
                    "selected_sum": selected_sum,
                    "gap": gap,
                    "tolerance_profile_id": profile.profile_id,
                    "absolute_tolerance_m": profile.reconciliation_absolute_tolerance_m,
                    "relative_tolerance": profile.reconciliation_relative_tolerance,
                    "selected_values": selected_values,
                }
                if mismatch:
                    _record_blocker(
                        result,
                        "section_reconciliation_mismatch",
                        "eligible selected rows do not reconcile to the reported section total",
                        stage="post_compute",
                        window_id=window_id,
                        year=year,
                        extra={
                            "reported_total": float(reported_total),
                            "selected_sum": selected_sum,
                            "gap": gap,
                        },
                    )

            for row in year_result.get("selected_rows", []):
                if row.get("eligible"):
                    continue
                required_role = str(row["concept_id"])
                item = model.get_item(build_plan.item_id_by_role[required_role])
                override = (item.overrides or {}).get(year)
                cell = (
                    None
                    if item.values is None
                    else item.values.values.get(year)
                )
                if override is not None or cell is not None:
                    _record_blocker(
                        result,
                        "population_disposition_violation",
                        "per-year ineligible optional row acquired a stored historical cell",
                        stage="post_compute",
                        window_id=window_id,
                        year=year,
                        concept_id=required_role,
                        item_id=item.id,
                    )
                value = _effective_value(model, item, year, computed_values)
                if value is None or (math.isfinite(value) and abs(value) <= 1e-9):
                    continue
                _record_blocker(
                    result,
                    "excluded_row_contributes",
                    "optional fact absent from the reviewed section acquired a value",
                    stage="post_compute",
                    window_id=window_id,
                    year=year,
                    concept_id=required_role,
                    item_id=item.id,
                    extra={"value": value},
                )

        if historical_years:
            latest_year = max(historical_years)
            latest_result = by_year[str(latest_year)]
            latest_ineligible = {
                str(row["concept_id"])
                for row in latest_result.get("selected_rows", [])
                if not row.get("eligible")
            }
            projection_periods = sorted(
                {
                    entry.period
                    for entry in build_plan.formula_membership
                    if entry.window_id == window_id
                    and entry.graph_kind == "projected"
                }
            )
            for required_role in sorted(latest_ineligible):
                item = model.get_item(build_plan.item_id_by_role[required_role])
                for period in projection_periods:
                    value = _effective_value(model, item, period, computed_values)
                    if value is None or (
                        math.isfinite(value) and abs(value) <= 1e-9
                    ):
                        continue
                    _record_blocker(
                        result,
                        "excluded_row_contributes",
                        "latest-basis optional absence contributes to a projection",
                        stage="post_compute",
                        window_id=window_id,
                        year=period,
                        concept_id=required_role,
                        item_id=item.id,
                        extra={"value": value, "basis_year": latest_year},
                    )

    historical_period_set = set(historical_years)
    for disposition in build_plan.population_dispositions:
        if (
            disposition.disposition == "populate"
            or disposition.item_id not in model_item_ids
        ):
            continue
        item = model.get_item(disposition.item_id)
        for year in sorted(historical_period_set):
            override = (item.overrides or {}).get(year)
            cell = (
                None
                if item.values is None
                else item.values.values.get(year)
            )
            if override is None and cell is None:
                continue
            _record_blocker(
                result,
                "population_disposition_violation",
                "non-populate row acquired a stored historical cell or override",
                stage="post_compute",
                window_id=disposition.window_id,
                year=year,
                concept_id=disposition.required_role,
                item_id=disposition.item_id,
            )

    suppressed_item_ids = {
        item_id
        for period_plan in build_plan.formula_membership
        for item_id in period_plan.suppressed_member_item_ids
    } & model_item_ids
    active_periods = sorted(
        {
            entry.period for entry in build_plan.formula_membership
        }
    )
    for item_id in sorted(suppressed_item_ids):
        item = model.get_item(item_id)
        for period in active_periods:
            value = _effective_value(model, item, period, computed_values)
            if value is None or (math.isfinite(value) and abs(value) <= 1e-9):
                continue
            window_id = next(
                (
                    entry.window_id
                    for entry in build_plan.population_dispositions
                    if entry.item_id == item_id
                ),
                "unknown",
            )
            _record_blocker(
                result,
                "excluded_row_contributes",
                "formula-suppressed row acquired a non-zero or non-finite value",
                stage="post_compute",
                window_id=window_id,
                year=period,
                item_id=item_id,
                extra={"value": value},
            )

    result.enforcement_status = "fail" if _has_blocker(result) else "pass"
    return result


__all__ = [
    "statement_window_population_years",
    "statement_window_validation_findings",
    "validate_statement_window_post_compute",
    "validate_statement_window_pre_population",
]
