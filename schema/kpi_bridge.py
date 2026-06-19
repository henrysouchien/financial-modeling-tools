# ruff: noqa: F401
from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from .kpi_bridge_axis_hints import product_axis_members_for_ticker
from .kpi_bridge_completion import (
    _commentary_reference_findings,
    _duplicate_field_findings,
    _leaf_driver_completion_findings,
    _walk_nodes,
    validate_business_model_draft_completion,
)
from .kpi_bridge_emit import (
    _assemble_alternative_segment_sets,
    _assemble_segments,
    _build_axis_inventory,
    _compute_revenue_shares,
    _emit_bridge_report,
    _segment_bundle_from_observations,
    _translate_to_driver_node,
)
from .kpi_bridge_report import (
    AlternativeSegmentSet,
    AxisInventoryEntry,
    BridgeReport,
    CompletionFinding,
    GlobalFinding,
    NodeFinding,
    RejectedObservation,
    SegmentFinding,
)
from .kpi_bridge_utils import (
    TOLERANCE_ERROR,
    TOLERANCE_WARNING,
    _ALLOWED_BEHAVIORS,
    _ALLOWED_FACTORS,
    _AXIS_PRIORITY,
    _BUSINESS_SEGMENT_SUFFIXES,
    _GEOGRAPHY_MEMBER_NAMES,
    _GEOGRAPHY_SUFFIXES,
    _NodeBundle,
    _REJECTION_REASONS,
    _SegmentBundle,
    _UNIT_MAP,
    _all_normalized_values_missing,
    _axis_choice_needed,
    _axis_qname,
    _count_error_warning_findings,
    _decode_period_key,
    _decode_period_part,
    _encode_period_key,
    _emit_business_model_draft,
    _first_nonempty,
    _group_observations,
    _is_revenue_like_node,
    _is_single_node_revenue_fallback,
    _is_total_revenue_observation,
    _kpi_frequency,
    _kpi_source,
    _local_member_name,
    _map_behavior,
    _period_key_recency,
    _period_part,
    _pick_primary_axis,
    _resolve_primary_axis,
    _segment_id_for_observation,
    _segment_id_from_member,
    _segment_label,
    _segment_match_name,
    _snake_case,
    _single_axis_qname,
    _title_label,
    _unique_node_id,
    _unique_preserve_order,
    _values_by_period_key,
)


def bridge_kpi_catalog(
    catalog_path: str | Path,
    ticker: str,
    template_id: str | None = None,
    primary_axis_qname: str | None = None,
    primary_axis_family: str | None = None,
) -> tuple[dict[str, Any], BridgeReport]:
    """Translate an Edgar KPI catalog JSONL into a BusinessModel-shaped draft."""

    observations = _load_catalog(catalog_path)
    ticker_normalized = str(ticker or "").upper()
    filing_id = _first_nonempty(obs.get("filing_id") for obs in observations)
    kept, rejected, rejected_by_reason, prose_rejected = _filter_observations(observations)

    global_findings: list[GlobalFinding] = []
    segment_findings: list[SegmentFinding] = []
    node_findings: list[NodeFinding] = []
    observed_values_per_node: dict[str, dict[str, float]] = {}
    consolidated_revenue_totals: dict[str, float] = {}

    if prose_rejected:
        global_findings.append(
            GlobalFinding(
                kind="prose_path_blocked",
                severity="info",
                detail=(
                    f"{prose_rejected} prose observations skipped because upstream "
                    "0d03c09 blocks the prose extraction path"
                ),
            )
        )

    axis_buckets: dict[str, list[dict[str, Any]]] = {axis: [] for axis in _AXIS_PRIORITY}
    consolidated_unmapped: list[dict[str, Any]] = []

    for obs in kept:
        axis = _classify_axis(obs, ticker_normalized)
        obs["_bridge_axis"] = axis
        if axis == "axis_ambiguous":
            segment_hint = obs.get("segment_hint") or {}
            edgar_member = segment_hint.get("edgar_member")
            segment_findings.append(
                SegmentFinding(
                    segment_id=_segment_id_for_observation(obs),
                    edgar_member=edgar_member,
                    axis="axis_ambiguous",
                    kind="axis_ambiguous",
                    severity="error",
                    detail=f"Could not classify XBRL member {edgar_member!r}; analyst must choose an axis",
                )
            )
            continue

        if axis is None:
            if _is_total_revenue_observation(obs):
                consolidated_revenue_totals.update(_values_by_period_key(obs))
            consolidated_unmapped.append(obs)
            continue

        axis_buckets[axis].append(obs)

    primary_axis, selected_axis_qname, selection_reason = _resolve_primary_axis(
        axis_buckets,
        consolidated_unmapped,
        primary_axis_qname=primary_axis_qname,
        primary_axis_family=primary_axis_family,
    )
    axis_choice_needed = (
        _axis_choice_needed(axis_buckets)
        and selection_reason == "default"
    )
    if selection_reason != "default" and primary_axis is not None:
        selector = selected_axis_qname or primary_axis
        global_findings.append(
            GlobalFinding(
                kind="primary_axis_chosen_by_override",
                severity="info",
                detail=f"Primary axis selected by explicit override: {selector!r}",
            )
        )
    elif primary_axis and sum(1 for axis in _AXIS_PRIORITY if axis_buckets[axis]) > 1:
        alternatives = [axis for axis in _AXIS_PRIORITY if axis != primary_axis and axis_buckets[axis]]
        global_findings.append(
            GlobalFinding(
                kind="primary_axis_chosen_by_priority",
                severity="info",
                detail=(
                    f"Primary axis {primary_axis!r} chosen by SEGMENT_AXES_PRIORITY; "
                    f"alternatives: {alternatives}"
                ),
            )
        )

    segment_bundles = _assemble_segments(
        primary_axis=primary_axis,
        primary_axis_qname=selected_axis_qname,
        axis_buckets=axis_buckets,
        consolidated_unmapped=consolidated_unmapped,
        node_findings=node_findings,
        segment_findings=segment_findings,
        observed_values_per_node=observed_values_per_node,
    )
    alternative_segment_sets = _assemble_alternative_segment_sets(
        primary_axis=primary_axis,
        axis_buckets=axis_buckets,
    )
    axis_inventory = _build_axis_inventory(
        axis_buckets=axis_buckets,
        primary_axis=primary_axis,
        primary_axis_qname=selected_axis_qname,
    )
    revenue_share_resolved, basis_key = _compute_revenue_shares(
        primary_axis=primary_axis,
        segment_bundles=segment_bundles,
        consolidated_revenue_totals=consolidated_revenue_totals,
        segment_findings=segment_findings,
        global_findings=global_findings,
    )

    draft = _emit_business_model_draft(
        ticker=ticker_normalized,
        template_id=template_id,
        filing_id=filing_id,
        segment_bundles=segment_bundles,
    )

    nodes_created = sum(len(bundle.nodes) for bundle in segment_bundles)
    observations_translated = sum(len(bundle.nodes) for bundle in segment_bundles)
    report = _emit_bridge_report(
        ticker=ticker_normalized,
        filing_id=filing_id,
        primary_axis=primary_axis,
        primary_axis_qname=selected_axis_qname,
        axis_choice_needed=axis_choice_needed,
        total_observations=len(observations),
        observations_translated=observations_translated,
        rejected=rejected,
        rejected_by_reason=rejected_by_reason,
        segments_created=len(segment_bundles),
        nodes_created=nodes_created,
        revenue_share_resolved=revenue_share_resolved,
        revenue_share_basis_period_key=basis_key,
        node_findings=node_findings,
        segment_findings=segment_findings,
        global_findings=global_findings,
        axis_inventory=axis_inventory,
        alternative_segment_sets=alternative_segment_sets,
        consolidated_revenue_totals=consolidated_revenue_totals,
        observed_values_per_node=observed_values_per_node,
    )
    return draft, report


def _load_catalog(catalog_path: str | Path) -> list[dict[str, Any]]:
    path = Path(catalog_path).expanduser()
    observations: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"Catalog line {index} is not a JSON object")
            payload["_catalog_index"] = index
            observations.append(payload)
    return observations


def _filter_observations(
    observations: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[RejectedObservation], dict[str, int], int]:
    kept: list[dict[str, Any]] = []
    rejected: list[RejectedObservation] = []
    rejected_by_reason = {reason: 0 for reason in _REJECTION_REASONS}
    prose_rejected = 0

    for obs in observations:
        reason: str | None = None
        severity = "info"
        detail: str | None = None
        source_type = obs.get("source_type")
        source_priority = obs.get("source_priority")

        if obs.get("grounded") is not True:
            reason = "ungrounded"
        elif source_type == "prose" or (source_type == "merged" and source_priority == "prose"):
            reason = "prose_path_blocked_by_upstream_regression"
            detail = "Upstream 0d03c09 blocks deterministic prose extraction in v1"
            prose_rejected += 1
        elif obs.get("metric_kind") == "growth" and _all_normalized_values_missing(obs):
            reason = "qualitative_trend_only"
        elif _all_normalized_values_missing(obs):
            reason = "no_normalized_values"
        else:
            raw_factors = [str(factor) for factor in (obs.get("factors") or [])]
            mapped_factors = _unique_preserve_order(f for f in raw_factors if f in _ALLOWED_FACTORS)
            dropped_factors = [factor for factor in raw_factors if factor not in _ALLOWED_FACTORS]
            if not mapped_factors:
                reason = "factors_unmappable"
                severity = "warning"
                detail = f"No catalog factors mapped to BusinessModel Factor literals: {raw_factors}"
            else:
                unit = _UNIT_MAP.get(str(obs.get("unit") or ""))
                if unit is None:
                    reason = "unit_unmapped"
                    severity = "warning"
                    detail = f"Catalog unit {obs.get('unit')!r} is not mapped to schema.models.Unit"
                else:
                    obs["_bridge_factors"] = mapped_factors
                    obs["_bridge_dropped_factors"] = dropped_factors
                    obs["_bridge_unit"] = unit
                    kept.append(obs)
                    continue

        if reason is None:
            kept.append(obs)
            continue

        rejected_by_reason[reason] += 1
        rejected.append(
            RejectedObservation(
                metric_name_normalized=obs.get("metric_name_normalized"),
                reason=reason,
                severity=severity,  # type: ignore[arg-type]
                metric_kind=obs.get("metric_kind"),
                direction_observed=obs.get("direction"),
                source_type=source_type,
                filing_id=obs.get("filing_id"),
                detail=detail,
            )
        )

    return kept, rejected, rejected_by_reason, prose_rejected


def _classify_axis(obs: dict[str, Any], ticker: str) -> str | None:
    segment_hint = obs.get("segment_hint") or {}
    axis_family = segment_hint.get("axis_family")
    if axis_family == "segment":
        return "business_segment"
    if axis_family in _AXIS_PRIORITY or axis_family == "axis_ambiguous":
        return axis_family

    axis_from_qname = _axis_family_from_qname(segment_hint.get("edgar_axis"))
    if axis_from_qname is not None:
        return axis_from_qname

    edgar_member = segment_hint.get("edgar_member")
    if not edgar_member:
        return None

    local_member = _local_member_name(edgar_member)
    geography = segment_hint.get("geography")
    if geography is not None and str(geography).lower() != "global":
        return "geography"
    if local_member.endswith(_GEOGRAPHY_SUFFIXES) or local_member in _GEOGRAPHY_MEMBER_NAMES:
        return "geography"
    if segment_hint.get("product") is not None:
        return "product"
    if local_member.endswith("ProductMember") or local_member in product_axis_members_for_ticker(ticker):
        return "product"
    if local_member.endswith(_BUSINESS_SEGMENT_SUFFIXES):
        return "business_segment"
    if local_member.endswith("Member"):
        return "axis_ambiguous"
    return None


def _axis_family_from_qname(axis_qname: Any) -> str | None:
    local_axis = _local_member_name(str(axis_qname or "")).lower()
    if not local_axis:
        return None
    if any(token in local_axis for token in ("product", "service", "brand", "category")):
        return "product"
    if any(token in local_axis for token in ("geograph", "country", "region", "area", "market")):
        return "geography"
    if "segment" in local_axis:
        return "business_segment"
    return None


__all__ = [
    "TOLERANCE_ERROR",
    "TOLERANCE_WARNING",
    "bridge_kpi_catalog",
    "validate_business_model_draft_completion",
    "_classify_axis",
    "_compute_revenue_shares",
    "_decode_period_key",
    "_encode_period_key",
    "_filter_observations",
    "_group_observations",
    "_load_catalog",
    "_period_key_recency",
    "_pick_primary_axis",
    "_translate_to_driver_node",
]
