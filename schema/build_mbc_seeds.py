"""Business-model inline seed helpers for schema build orchestration."""

from __future__ import annotations

import logging
import math
import sys
from typing import TYPE_CHECKING, Any

from .model_build_context import SegmentConfig
from .segments import revenue_observations_to_values as _revenue_observations_to_values_fallback

if TYPE_CHECKING:
    from .business_model import BusinessModel


_SEGMENT_REVENUE_KPI_SOURCE_TAGS = frozenset({
    "revenuefromcontractwithcustomerexcludingassessedtax",
    "revenues",
    "salesrevenuegoodsnet",
    "salesrevenuenet",
})


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        from . import build as parent
    return getattr(parent, name, fallback)


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build helper '{name}' is unavailable")
    return helper


def _bm_segment_snapshot_inline_values(
    business_model: "BusinessModel | None",
    segment_config: SegmentConfig | None,
) -> dict[str, dict[str, float]]:
    if business_model is None or segment_config is None:
        return {}
    snapshot = segment_config.segment_profile_snapshot
    axis = segment_config.axis or snapshot.axis_used
    if not axis:
        return {}

    tags_equivalent = _required_parent_attr("_tags_equivalent")
    iter_business_model_nodes = _parent_attr("_iter_business_model_nodes", _iter_business_model_nodes)
    is_segment_revenue_kpi_node = _parent_attr(
        "_is_segment_revenue_kpi_node",
        _is_segment_revenue_kpi_node,
    )
    revenue_observations_to_values = _parent_attr(
        "revenue_observations_to_values",
        _revenue_observations_to_values_fallback,
    )

    snapshot_by_member = {
        str(segment.edgar_member or "").strip(): segment
        for segment in snapshot.segments
        if str(segment.edgar_member or "").strip()
    }
    if not snapshot_by_member:
        return {}

    inline_values: dict[str, dict[str, float]] = {}
    for segment in business_model.segments:
        if not (segment.edgar_axis and segment.edgar_member):
            # Absorb (multi-member) segment: the segment names no single member,
            # but each member-backed revenue node carries its own edgar_member.
            # Seed each such node from its member's snapshot observations. This is
            # id-agnostic on purpose — the blended nodes are named e.g.
            # `recurring_revenue`/`implementation_revenue`, not `segment_revenue`,
            # so `_is_segment_revenue_kpi_node` (id-gated) must NOT be used here.
            if segment.absorbs:
                _emit_absorbed_member_inline_values(
                    segment,
                    snapshot_by_member,
                    axis,
                    inline_values=inline_values,
                    tags_equivalent=tags_equivalent,
                    iter_business_model_nodes=iter_business_model_nodes,
                    revenue_observations_to_values=revenue_observations_to_values,
                )
            continue
        if not tags_equivalent(str(segment.edgar_axis), str(axis)):
            continue
        snapshot_segment = next(
            (
                candidate
                for member, candidate in snapshot_by_member.items()
                if tags_equivalent(member, str(segment.edgar_member))
            ),
            None,
        )
        if snapshot_segment is None or not snapshot_segment.revenue_observations:
            continue
        for node in iter_business_model_nodes(segment.revenue_model.decomposition):
            if not is_segment_revenue_kpi_node(node):
                continue
            inline_values[f"{segment.id}:{node.id}"] = {
                str(int(year)): float(value)
                for year, value in sorted(
                    revenue_observations_to_values(snapshot_segment.revenue_observations).items()
                )
                if value is not None and math.isfinite(float(value))
            }
    return {
        key: values
        for key, values in inline_values.items()
        if values
    }


def _absorbed_claim_members(segment: object) -> list[str]:
    members: list[str] = []
    for claim in getattr(segment, "absorbs", None) or []:
        member = str(getattr(claim, "member", "") or "").strip()
        name = str(getattr(claim, "name", "") or "").strip()
        members.append(member or name)
    return [member for member in members if member]


def _bm_revenue_share_inline_values(
    business_model: "BusinessModel | None",
    *,
    fmp_data: dict | None,
    most_recent_fy: int,
    n_historical: int,
) -> dict[str, dict[str, float]]:
    if business_model is None or not isinstance(fmp_data, dict):
        return {}

    fmp_total_revenue_by_year = _parent_attr(
        "_fmp_total_revenue_by_year",
        _fmp_total_revenue_by_year,
    )
    iter_business_model_nodes = _parent_attr("_iter_business_model_nodes", _iter_business_model_nodes)
    is_segment_revenue_kpi_node = _parent_attr(
        "_is_segment_revenue_kpi_node",
        _is_segment_revenue_kpi_node,
    )

    total_revenue_by_year = fmp_total_revenue_by_year(
        fmp_data,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
    )
    if not total_revenue_by_year:
        return {}

    inline_values: dict[str, dict[str, float]] = {}
    for segment in business_model.segments:
        try:
            share = float(segment.revenue_share)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(share) or share <= 0:
            continue
        for node in iter_business_model_nodes(segment.revenue_model.decomposition):
            if not is_segment_revenue_kpi_node(node):
                continue
            inline_values[f"{segment.id}:{node.id}"] = {
                str(year): float(total_revenue) * share
                for year, total_revenue in sorted(total_revenue_by_year.items())
            }

    return inline_values


def _fmp_total_revenue_by_year(
    fmp_data: dict,
    *,
    most_recent_fy: int,
    n_historical: int,
) -> dict[int, float]:
    build_fmp_lookup = _required_parent_attr("_build_fmp_lookup")
    lookup = build_fmp_lookup(fmp_data)
    income_statement = lookup.get("income_statement") or {}
    if not income_statement:
        return {}

    allowed_years = set(range(int(most_recent_fy) - int(n_historical) + 1, int(most_recent_fy) + 1))
    revenue_fields = ("revenue", "totalRevenue", "total_revenue")
    values: dict[int, float] = {}
    for year, record in sorted(income_statement.items()):
        if year not in allowed_years:
            continue
        raw_value = next((record.get(field) for field in revenue_fields if record.get(field) is not None), None)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        values[int(year)] = value / 1_000_000.0
    return values


def _iter_business_model_nodes(nodes: list[Any]):
    for node in nodes:
        yield node
        yield from _iter_business_model_nodes(getattr(node, "children", None) or [])


def _is_segment_revenue_kpi_node(node: Any) -> bool:
    if str(getattr(node, "id", "") or "") != "segment_revenue":
        return False
    return _is_revenue_kpi_source_node(node)


def _is_revenue_kpi_source_node(node: Any) -> bool:
    """Contract-revenue KPI node, id-agnostic (for absorb-segment member nodes)."""
    if not bool(getattr(node, "kpi", False)):
        return False
    source = str(getattr(node, "kpi_source", "") or "").rsplit(":", 1)[-1].lower()
    source_tags = _parent_attr(
        "_SEGMENT_REVENUE_KPI_SOURCE_TAGS",
        _SEGMENT_REVENUE_KPI_SOURCE_TAGS,
    )
    return source in source_tags


def _emit_absorbed_member_inline_values(
    segment: Any,
    snapshot_by_member: dict[str, Any],
    axis: Any,
    *,
    inline_values: dict[str, dict[str, float]],
    tags_equivalent: Any,
    iter_business_model_nodes: Any,
    revenue_observations_to_values: Any,
) -> None:
    """Seed each member-tagged contract-revenue node of an absorb segment from its
    own EDGAR member's snapshot observations (recurring -> RecurringFeesMember, etc.),
    instead of every node falling back to the consolidated total (the 2x double-count).
    Non-member nodes (e.g. interest_income) are left to their own kpi_source fetch.
    """
    for node in iter_business_model_nodes(segment.revenue_model.decomposition):
        node_member = str(getattr(node, "edgar_member", None) or "").strip()
        if not node_member:
            continue
        if not _is_revenue_kpi_source_node(node):
            continue
        node_axis = str(getattr(node, "edgar_axis", None) or "").strip()
        # Axis cross-check: a mis-axied member must not silently map (mirror line 83).
        if node_axis and axis and not tags_equivalent(node_axis, str(axis)):
            continue
        snapshot_segment = next(
            (
                candidate
                for member, candidate in snapshot_by_member.items()
                if tags_equivalent(member, node_member)
            ),
            None,
        )
        if snapshot_segment is None or not snapshot_segment.revenue_observations:
            continue
        inline_values[f"{segment.id}:{node.id}"] = {
            str(int(year)): float(value)
            for year, value in sorted(
                revenue_observations_to_values(snapshot_segment.revenue_observations).items()
            )
            if value is not None and math.isfinite(float(value))
        }
