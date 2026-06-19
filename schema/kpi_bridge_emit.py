from __future__ import annotations

from collections import defaultdict
from typing import Any

from .kpi_bridge_report import (
    AlternativeSegmentSet,
    AxisInventoryEntry,
    BridgeReport,
    BridgeSummary,
    GlobalFinding,
    NodeFinding,
    RejectedObservation,
    SegmentFinding,
)
from .kpi_bridge_utils import (
    TOLERANCE_ERROR,
    TOLERANCE_WARNING,
    _AXIS_PRIORITY,
    _NodeBundle,
    _SegmentBundle,
    _axis_qname,
    _count_error_warning_findings,
    _group_observations,
    _is_revenue_like_node,
    _is_single_node_revenue_fallback,
    _kpi_frequency,
    _kpi_source,
    _map_behavior,
    _period_key_recency,
    _segment_id_for_observation,
    _segment_id_from_member,
    _segment_label,
    _segment_match_name,
    _single_axis_qname,
)
from .kpi_bridge_utils import (
    _encode_period_key,
    _snake_case,
    _title_label,
    _unique_node_id,
    _unique_preserve_order,
)


def _translate_to_driver_node(
    group: list[dict[str, Any]],
    *,
    segment_id: str,
    used_node_ids: set[str],
    node_findings: list[NodeFinding],
) -> _NodeBundle:
    first = group[0]
    metric_name = str(first.get("metric_name_normalized") or first.get("metric_name") or "kpi")
    node_id = _unique_node_id(_snake_case(metric_name), used_node_ids)
    values: dict[str, float] = {}

    first_behavior = first.get("behavior")
    for obs in group:
        for value in obs.get("values") or []:
            normalized = value.get("value_normalized")
            if normalized is None:
                continue
            period_key = _encode_period_key(
                value.get("period_frame"),
                value.get("period"),
                value.get("period_end"),
                value.get("range_bound"),
            )
            numeric = float(normalized)
            if period_key in values and values[period_key] != numeric:
                node_findings.append(
                    NodeFinding(
                        segment_id=segment_id,
                        node_id=node_id,
                        kind="period_value_conflict",
                        severity="error",
                        detail="Multiple observations had different values for the same compound period key; first value kept",
                        period_key=period_key,
                        raw_value_first=values[period_key],
                        raw_value_conflicting=numeric,
                    )
                )
                continue
            values.setdefault(period_key, numeric)

        if obs is not first and obs.get("behavior") != first_behavior:
            node_findings.append(
                NodeFinding(
                    segment_id=segment_id,
                    node_id=node_id,
                    kind="observation_metadata_conflict",
                    severity="warning",
                    detail="Multiple grouped observations differed on behavior; first observation kept",
                )
            )

    dropped_factors = _unique_preserve_order(
        factor for obs in group for factor in (obs.get("_bridge_dropped_factors") or [])
    )
    if dropped_factors:
        node_findings.append(
            NodeFinding(
                segment_id=segment_id,
                node_id=node_id,
                kind="factors_filtered",
                severity="warning",
                detail=f"Dropped unmapped factors: {dropped_factors}",
            )
        )

    node = {
        "id": node_id,
        "label": _title_label(metric_name),
        "factors": list(first.get("_bridge_factors") or []),
        "unit": first.get("_bridge_unit"),
        "driver": None,
        "compile_to": {
            "target_type": "commentary",
            "sheet": "Assumptions",
            "section_id": None,
            "existing_driver_key": None,
        },
        "kpi": True,
        "kpi_source": _kpi_source(first),
        "kpi_frequency": _kpi_frequency(group),
        "behavior": _map_behavior(first),
        "management_target": first.get("management_target"),
        "note": None,
        "children": None,
    }
    node_findings.append(
        NodeFinding(
            segment_id=segment_id,
            node_id=node_id,
            kind="driver_assignment_pending",
            severity="warning",
            detail="v1 sets driver=None + commentary; analyst chooses GrowthExpr/ProductExpr/etc.",
        )
    )
    return _NodeBundle(
        node=node,
        values=values,
        metric_kind=first.get("metric_kind"),
        metric_name_normalized=metric_name,
        segment_id=segment_id,
    )


def _assemble_segments(
    *,
    primary_axis: str | None,
    primary_axis_qname: str | None,
    axis_buckets: dict[str, list[dict[str, Any]]],
    consolidated_unmapped: list[dict[str, Any]],
    node_findings: list[NodeFinding],
    segment_findings: list[SegmentFinding],
    observed_values_per_node: dict[str, dict[str, float]],
) -> list[_SegmentBundle]:
    bundles: list[_SegmentBundle] = []

    if primary_axis is not None:
        by_member: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for obs in axis_buckets[primary_axis]:
            if primary_axis_qname is not None and _axis_qname(obs) != primary_axis_qname:
                continue
            member = (obs.get("segment_hint") or {}).get("edgar_member")
            if member:
                by_member[member].append(obs)
        for member, member_observations in by_member.items():
            bundles.append(
                _segment_bundle_from_observations(
                    segment_id=_segment_id_from_member(member),
                    label=_segment_label(member_observations),
                    match_name=_segment_match_name(member_observations),
                    edgar_member=member,
                    axis=primary_axis,
                    observations=member_observations,
                    node_findings=node_findings,
                    observed_values_per_node=observed_values_per_node,
                )
            )

    if consolidated_unmapped:
        segment_findings.append(
            SegmentFinding(
                segment_id="consolidated_unmapped",
                kind="segment_without_edgar_member",
                severity="warning",
                detail="Label-only observations were assigned to the consolidated_unmapped review segment",
            )
        )
        bundles.append(
            _segment_bundle_from_observations(
                segment_id="consolidated_unmapped",
                label="Consolidated Unmapped",
                match_name="consolidated unmapped",
                edgar_member=None,
                axis=None,
                observations=consolidated_unmapped,
                node_findings=node_findings,
                observed_values_per_node=observed_values_per_node,
            )
        )

    return bundles


def _assemble_alternative_segment_sets(
    *,
    primary_axis: str | None,
    axis_buckets: dict[str, list[dict[str, Any]]],
) -> dict[str, AlternativeSegmentSet]:
    alternatives: dict[str, AlternativeSegmentSet] = {}
    for axis, observations in axis_buckets.items():
        if axis == primary_axis or not observations:
            continue
        members = _unique_preserve_order(
            (obs.get("segment_hint") or {}).get("edgar_member")
            for obs in observations
            if (obs.get("segment_hint") or {}).get("edgar_member")
        )
        members_by_axis: dict[str, list[str]] = {}
        for obs in observations:
            axis_qname = _axis_qname(obs)
            member = (obs.get("segment_hint") or {}).get("edgar_member")
            if not axis_qname or not member:
                continue
            members_by_axis.setdefault(axis_qname, []).append(member)
        members_by_axis = {
            axis_qname: _unique_preserve_order(axis_members)
            for axis_qname, axis_members in sorted(members_by_axis.items())
        }
        samples: list[dict[str, Any]] = []
        sample_findings: list[NodeFinding] = []
        for group in list(_group_observations(observations).values())[:3]:
            bundle = _translate_to_driver_node(
                group,
                segment_id=_segment_id_for_observation(group[0]),
                used_node_ids=set(),
                node_findings=sample_findings,
            )
            samples.append(bundle.node)
        alternatives[axis] = AlternativeSegmentSet(
            axis=axis,  # type: ignore[arg-type]
            edgar_members=members,
            edgar_members_by_axis=members_by_axis,
            node_count=len(_group_observations(observations)),
            rationale=f"Primary axis = {primary_axis}; {axis} observations available as alternative grouping",
            sample_driver_nodes=samples,
        )
    return alternatives


def _build_axis_inventory(
    *,
    axis_buckets: dict[str, list[dict[str, Any]]],
    primary_axis: str | None,
    primary_axis_qname: str | None,
) -> list[AxisInventoryEntry]:
    inventory: list[AxisInventoryEntry] = []
    for axis in _AXIS_PRIORITY:
        observations = axis_buckets[axis]
        if not observations:
            continue

        by_qname: dict[str | None, list[dict[str, Any]]] = defaultdict(list)
        for obs in observations:
            by_qname[_axis_qname(obs)].append(obs)

        for axis_qname, grouped in sorted(by_qname.items(), key=lambda item: item[0] or ""):
            labels: dict[str, str] = {}
            members = _unique_preserve_order(
                (obs.get("segment_hint") or {}).get("edgar_member")
                for obs in grouped
                if (obs.get("segment_hint") or {}).get("edgar_member")
            )
            for obs in grouped:
                segment_hint = obs.get("segment_hint") or {}
                member = segment_hint.get("edgar_member")
                label = segment_hint.get("segment_label")
                if member and label:
                    labels.setdefault(member, str(label))

            selected = axis == primary_axis and (
                primary_axis_qname is None or axis_qname == primary_axis_qname
            )
            inventory.append(
                AxisInventoryEntry(
                    axis=axis,  # type: ignore[arg-type]
                    axis_qname=axis_qname,
                    members=members,
                    member_labels=labels,
                    observation_count=len(grouped),
                    node_count=len(_group_observations(grouped)),
                    selected_as_primary=selected,
                )
            )
    return inventory


def _compute_revenue_shares(
    *,
    primary_axis: str | None,
    segment_bundles: list[_SegmentBundle],
    consolidated_revenue_totals: dict[str, float],
    segment_findings: list[SegmentFinding],
    global_findings: list[GlobalFinding],
) -> tuple[bool, str | None]:
    if primary_axis is None:
        if len(segment_bundles) == 1 and segment_bundles[0].axis is None:
            segment_bundles[0].draft["revenue_share"] = 1.0
            return True, None
        return False, None

    primary_bundles = [bundle for bundle in segment_bundles if bundle.axis == primary_axis]
    if not primary_bundles:
        return False, None

    revenue_nodes: dict[str, _NodeBundle] = {}
    for bundle in primary_bundles:
        strict_candidates = [node for node in bundle.nodes if _is_revenue_like_node(node)]
        if len(strict_candidates) > 1:
            segment_findings.append(
                SegmentFinding(
                    segment_id=bundle.draft["id"],
                    edgar_member=bundle.draft.get("edgar_member"),
                    axis=primary_axis,
                    kind="multiple_revenue_candidates",
                    severity="error",
                    detail=f"Multiple revenue-like nodes found: {[node.node['id'] for node in strict_candidates]}",
                )
            )
            continue
        if len(strict_candidates) == 1:
            revenue_nodes[bundle.draft["id"]] = strict_candidates[0]
            continue

        fallback_candidates = [node for node in bundle.nodes if _is_single_node_revenue_fallback(node)]
        if len(fallback_candidates) == 1 and len(bundle.nodes) == 1:
            revenue_nodes[bundle.draft["id"]] = fallback_candidates[0]
            if fallback_candidates[0].node.get("unit") != "dollars":
                segment_findings.append(
                    SegmentFinding(
                        segment_id=bundle.draft["id"],
                        edgar_member=bundle.draft.get("edgar_member"),
                        axis=primary_axis,
                        kind="revenue_candidate_unit_assumed",
                        severity="warning",
                        detail=(
                            "Single absolute segment KPI used for revenue share despite "
                            f"unit={fallback_candidates[0].node.get('unit')!r}"
                        ),
                    )
                )
            continue

        segment_findings.append(
            SegmentFinding(
                segment_id=bundle.draft["id"],
                edgar_member=bundle.draft.get("edgar_member"),
                axis=primary_axis,
                kind="missing_segment_revenue",
                severity="error",
                detail="No unique revenue-like node was available for revenue share calculation",
            )
        )

    if len(revenue_nodes) != len(primary_bundles):
        return False, None

    if not consolidated_revenue_totals:
        global_findings.append(
            GlobalFinding(
                kind="missing_total_revenue",
                severity="error",
                detail="No consolidated total revenue observation was available for revenue share calculation",
            )
        )
        return False, None

    candidate_keys = sorted(
        consolidated_revenue_totals.keys(),
        key=_period_key_recency,
        reverse=True,
    )
    basis_key = next(
        (
            key
            for key in candidate_keys
            if all(revenue_nodes[bundle.draft["id"]].values.get(key) is not None for bundle in primary_bundles)
        ),
        None,
    )
    if basis_key is None:
        global_findings.append(
            GlobalFinding(
                kind="missing_basis_period_for_share",
                severity="error",
                detail="No period key exists in consolidated totals and every primary-axis segment revenue node",
            )
        )
        return False, None

    total_val = consolidated_revenue_totals.get(basis_key)
    if total_val is None:
        global_findings.append(
            GlobalFinding(
                kind="missing_total_revenue",
                severity="error",
                detail=f"Consolidated total revenue missing for basis period {basis_key}",
            )
        )
        return False, basis_key

    for bundle in primary_bundles:
        revenue_node = revenue_nodes[bundle.draft["id"]]
        seg_val = revenue_node.values.get(basis_key)
        if seg_val is not None and total_val is not None:
            bundle.draft["revenue_share"] = round(seg_val / total_val, 4)
        else:
            bundle.draft["revenue_share"] = 0.0
            segment_findings.append(
                SegmentFinding(
                    segment_id=bundle.draft["id"],
                    edgar_member=bundle.draft.get("edgar_member"),
                    axis=primary_axis,
                    kind="missing_segment_revenue",
                    severity="error",
                    detail=f"Segment revenue missing for basis period {basis_key}",
                )
            )

    share_sum = sum(bundle.draft["revenue_share"] for bundle in primary_bundles)
    deviation = abs(share_sum - 1.0)
    if deviation > TOLERANCE_ERROR:
        for bundle in primary_bundles:
            bundle.draft["revenue_share"] = 0.0
        global_findings.append(
            GlobalFinding(
                kind="unresolved_revenue_share",
                severity="error",
                detail=f"share_sum={share_sum:.4f}, deviation={deviation:.4f}, tolerance={TOLERANCE_ERROR}",
            )
        )
        return False, basis_key
    if deviation > TOLERANCE_WARNING:
        global_findings.append(
            GlobalFinding(
                kind="revenue_share_drift",
                severity="warning",
                detail=f"share_sum={share_sum:.4f}, deviation={deviation:.4f}",
            )
        )
    return True, basis_key


def _emit_bridge_report(
    *,
    ticker: str,
    filing_id: str | None,
    primary_axis: str | None,
    primary_axis_qname: str | None,
    axis_choice_needed: bool,
    total_observations: int,
    observations_translated: int,
    rejected: list[RejectedObservation],
    rejected_by_reason: dict[str, int],
    segments_created: int,
    nodes_created: int,
    revenue_share_resolved: bool,
    revenue_share_basis_period_key: str | None,
    node_findings: list[NodeFinding],
    segment_findings: list[SegmentFinding],
    global_findings: list[GlobalFinding],
    axis_inventory: list[AxisInventoryEntry],
    alternative_segment_sets: dict[str, AlternativeSegmentSet],
    consolidated_revenue_totals: dict[str, float],
    observed_values_per_node: dict[str, dict[str, float]],
) -> BridgeReport:
    errors, warnings = _count_error_warning_findings(
        rejected,
        node_findings,
        segment_findings,
        global_findings,
    )
    if errors:
        readiness_status = "blocked"
    elif axis_choice_needed:
        readiness_status = "needs_axis_choice"
    elif any(finding.kind == "driver_assignment_pending" for finding in node_findings):
        readiness_status = "needs_driver_assignment"
    else:
        readiness_status = "ready_for_compile"

    if total_observations == 0 or segments_created == 0:
        readiness_status = "blocked"

    return BridgeReport(
        ticker=ticker,
        filing_id=filing_id,
        primary_axis=primary_axis,  # type: ignore[arg-type]
        primary_axis_qname=primary_axis_qname,
        readiness_status=readiness_status,  # type: ignore[arg-type]
        summary=BridgeSummary(
            total_observations_in_catalog=total_observations,
            observations_translated=observations_translated,
            observations_rejected=len(rejected),
            rejected_by_reason=rejected_by_reason,
            segments_created=segments_created,
            nodes_created=nodes_created,
            revenue_share_resolved=revenue_share_resolved,
            revenue_share_basis_period_key=revenue_share_basis_period_key,
            errors=errors,
            warnings=warnings,
        ),
        rejected_observations=rejected,
        findings_per_node=node_findings,
        findings_per_segment=segment_findings,
        global_findings=global_findings,
        axis_inventory=axis_inventory,
        alternative_segment_sets=alternative_segment_sets,  # type: ignore[arg-type]
        consolidated_revenue_totals=consolidated_revenue_totals,
        observed_values_per_node=observed_values_per_node,
    )


def _segment_bundle_from_observations(
    *,
    segment_id: str,
    label: str,
    match_name: str,
    edgar_member: str | None,
    axis: str | None,
    observations: list[dict[str, Any]],
    node_findings: list[NodeFinding],
    observed_values_per_node: dict[str, dict[str, float]],
) -> _SegmentBundle:
    used_node_ids: set[str] = set()
    nodes: list[_NodeBundle] = []
    for group in _group_observations(observations).values():
        bundle = _translate_to_driver_node(
            group,
            segment_id=segment_id,
            used_node_ids=used_node_ids,
            node_findings=node_findings,
        )
        nodes.append(bundle)
        observed_values_per_node[f"{segment_id}:{bundle.node['id']}"] = bundle.values

    draft = {
        "id": segment_id,
        "label": label,
        "match_name": match_name,
        "edgar_member": edgar_member,
        "revenue_share": 0.0,
        "revenue_model": {
            "type": "blended",
            "decomposition": [bundle.node for bundle in nodes],
        },
        "unit_economics": None,
        "cost_overrides": None,
    }
    edgar_axis = _single_axis_qname(observations)
    if edgar_axis is not None:
        draft["edgar_axis"] = edgar_axis
    return _SegmentBundle(draft=draft, nodes=nodes, axis=axis)


__all__ = [
    "_assemble_alternative_segment_sets",
    "_assemble_segments",
    "_build_axis_inventory",
    "_compute_revenue_shares",
    "_emit_bridge_report",
    "_segment_bundle_from_observations",
    "_translate_to_driver_node",
]
