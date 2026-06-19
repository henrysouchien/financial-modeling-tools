from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Iterable, get_args

from .business_model import Factor as BusinessModelFactor
from .business_model import NodeBehavior


TOLERANCE_ERROR = 0.02
TOLERANCE_WARNING = 0.005

_AXIS_PRIORITY: tuple[str, ...] = ("business_segment", "product", "geography")
_ALLOWED_FACTORS = frozenset(get_args(BusinessModelFactor))
_ALLOWED_BEHAVIORS = frozenset(get_args(NodeBehavior))
_REJECTION_REASONS = (
    "ungrounded",
    "prose_path_blocked_by_upstream_regression",
    "qualitative_trend_only",
    "no_normalized_values",
    "factors_unmappable",
    "unit_unmapped",
)
_UNIT_MAP = {
    "dollars": "dollars",
    "percentage": "percentage",
    "count": "count",
    "ratio": "ratio",
    "per_share": "per_share",
    "days": "days",
    "multiple": "multiple",
}
_TOTAL_REVENUE_NAMES = {
    "total net sales",
    "total revenue",
    "total revenues",
    "net revenue",
    "total net revenue",
}
_REVENUE_SEMANTICS_RE = re.compile(r"\b(revenue|sales|net sales|net revenue)\b")
_GEOGRAPHY_MEMBER_NAMES = {
    "AmericasSegmentMember",
    "EuropeSegmentMember",
    "AsiaSegmentMember",
    "JapanSegmentMember",
    "ChinaSegmentMember",
    "GreaterChinaSegmentMember",
    "RestOfAsiaPacificSegmentMember",
}
_GEOGRAPHY_SUFFIXES = (
    "CountryMember",
    "RegionMember",
    "InternationalMember",
    "DomesticMember",
)
_BUSINESS_SEGMENT_SUFFIXES = (
    "OperatingSegmentMember",
    "ReportableSegmentMember",
    "BusinessSegmentMember",
)
_FRAME_PRIORITY = {
    "annual": 4,
    "quarterly": 3,
    "ttm": 2,
    "ytd": 1,
    "point_in_time": 0,
}


@dataclass
class _NodeBundle:
    node: dict[str, Any]
    values: dict[str, float]
    metric_kind: str | None
    metric_name_normalized: str
    segment_id: str


@dataclass
class _SegmentBundle:
    draft: dict[str, Any]
    nodes: list[_NodeBundle]
    axis: str | None


def _encode_period_key(
    period_frame: Any,
    period: Any,
    period_end: Any,
    range_bound: Any,
) -> str:
    return ":".join(_period_part(value) for value in (period_frame, period, period_end, range_bound))


def _decode_period_key(period_key: str) -> tuple[str | None, int | None, str | None, str | None]:
    parts = period_key.split(":", 3)
    if len(parts) != 4:
        raise ValueError(f"Invalid period key: {period_key!r}")
    frame, period, period_end, range_bound = (_decode_period_part(part) for part in parts)
    period_int: int | None
    if period is None:
        period_int = None
    else:
        try:
            period_int = int(period)
        except ValueError:
            period_int = None
    return frame, period_int, period_end, range_bound


def _period_key_recency(period_key: str) -> tuple[datetime, int, int]:
    period_frame, period, period_end, _range_bound = _decode_period_key(period_key)
    parsed_period_end = datetime.min
    if period_end:
        try:
            parsed_period_end = datetime.fromisoformat(period_end)
        except ValueError:
            parsed_period_end = datetime.min
    period_int = int(period) if period is not None else -10**18
    frame_priority = _FRAME_PRIORITY.get(str(period_frame or ""), -1)
    return parsed_period_end, period_int, frame_priority


def _pick_primary_axis(
    axis_buckets: dict[str, list[dict[str, Any]]],
    consolidated_unmapped: list[dict[str, Any]],
) -> str | None:
    for axis in _AXIS_PRIORITY:
        if axis_buckets[axis]:
            return axis
    if consolidated_unmapped:
        return None
    return None


def _resolve_primary_axis(
    axis_buckets: dict[str, list[dict[str, Any]]],
    consolidated_unmapped: list[dict[str, Any]],
    *,
    primary_axis_qname: str | None,
    primary_axis_family: str | None,
) -> tuple[str | None, str | None, str]:
    requested_qname = str(primary_axis_qname or "").strip() or None
    requested_family = str(primary_axis_family or "").strip() or None
    if requested_qname and requested_family:
        raise ValueError("primary_axis_qname and primary_axis_family are mutually exclusive")

    if requested_qname:
        matches: list[tuple[str, list[dict[str, Any]]]] = []
        for axis in _AXIS_PRIORITY:
            observations = [
                obs for obs in axis_buckets[axis]
                if _axis_qname(obs) == requested_qname
            ]
            if observations:
                matches.append((axis, observations))
        if not matches:
            available = sorted({
                qname
                for observations in axis_buckets.values()
                for obs in observations
                if (qname := _axis_qname(obs))
            })
            raise ValueError(
                f"primary_axis_qname {requested_qname!r} is not present in the catalog; "
                f"available_axis_qnames={available}"
            )
        families = {axis for axis, _observations in matches}
        if len(families) > 1:
            raise ValueError(
                f"primary_axis_qname {requested_qname!r} maps to multiple axis families: "
                f"{sorted(families)}"
            )
        return matches[0][0], requested_qname, "qname_override"

    if requested_family:
        if requested_family not in _AXIS_PRIORITY:
            raise ValueError(
                f"primary_axis_family must be one of {list(_AXIS_PRIORITY)}, got {requested_family!r}"
            )
        if not axis_buckets[requested_family]:
            available = [axis for axis in _AXIS_PRIORITY if axis_buckets[axis]]
            raise ValueError(
                f"primary_axis_family {requested_family!r} is not present in the catalog; "
                f"available_axis_families={available}"
            )
        return (
            requested_family,
            _single_axis_qname(axis_buckets[requested_family]),
            "family_override",
        )

    primary_axis = _pick_primary_axis(axis_buckets, consolidated_unmapped)
    return (
        primary_axis,
        _single_axis_qname(axis_buckets[primary_axis]) if primary_axis is not None else None,
        "default",
    )


def _axis_qname(obs: dict[str, Any]) -> str | None:
    raw = (obs.get("segment_hint") or {}).get("edgar_axis")
    qname = str(raw or "").strip()
    return qname or None


def _single_axis_qname(observations: list[dict[str, Any]]) -> str | None:
    qnames = _unique_preserve_order(_axis_qname(obs) for obs in observations if _axis_qname(obs))
    return qnames[0] if len(qnames) == 1 else None


def _group_observations(observations: Iterable[dict[str, Any]]) -> dict[tuple[str, str | None], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for obs in observations:
        segment_hint = obs.get("segment_hint") or {}
        segment_key = segment_hint.get("edgar_member") or segment_hint.get("segment_label_normalized")
        grouped[(str(obs.get("metric_name_normalized") or ""), segment_key)].append(obs)
    return grouped


def _values_by_period_key(obs: dict[str, Any]) -> dict[str, float]:
    values: dict[str, float] = {}
    for value in obs.get("values") or []:
        normalized = value.get("value_normalized")
        if normalized is None:
            continue
        values[
            _encode_period_key(
                value.get("period_frame"),
                value.get("period"),
                value.get("period_end"),
                value.get("range_bound"),
            )
        ] = float(normalized)
    return values


def _emit_business_model_draft(
    *,
    ticker: str,
    template_id: str | None,
    filing_id: str | None,
    segment_bundles: list[_SegmentBundle],
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "created_by": "kpi_bridge",
        "review_basis_filing": filing_id,
        "source_materials": [filing_id] if filing_id else [],
    }
    if template_id:
        metadata["revision"] = f"kpi_bridge:{template_id}"
    return {
        "schema_version": "1.0",
        "company": {
            "ticker": ticker,
            "name": ticker,
            "sector": None,
            "industry": None,
            "business_type": None,
        },
        "segments": [bundle.draft for bundle in segment_bundles],
        "consolidated": None,
        "profitability_targets": None,
        "recommended_depth": None,
        "decisions_log": [],
        "metadata": metadata,
    }


def _is_total_revenue_observation(obs: dict[str, Any]) -> bool:
    segment_hint = obs.get("segment_hint") or {}
    if segment_hint.get("edgar_member"):
        return False
    metric_name = str(obs.get("metric_name_normalized") or "").lower()
    return (
        "volume" in (obs.get("_bridge_factors") or [])
        and obs.get("_bridge_unit") == "dollars"
        and metric_name in _TOTAL_REVENUE_NAMES
    )


def _is_revenue_like_node(bundle: _NodeBundle) -> bool:
    node = bundle.node
    metric_name = bundle.metric_name_normalized.lower()
    return (
        "volume" in (node.get("factors") or [])
        and node.get("unit") == "dollars"
        and bundle.metric_kind == "absolute"
        and bool(_REVENUE_SEMANTICS_RE.search(metric_name))
    )


def _is_single_node_revenue_fallback(bundle: _NodeBundle) -> bool:
    return (
        "volume" in (bundle.node.get("factors") or [])
        and bundle.metric_kind == "absolute"
        and bool(bundle.values)
        and bundle.node.get("unit") in {"dollars", "count"}
    )


def _axis_choice_needed(axis_buckets: dict[str, list[dict[str, Any]]]) -> bool:
    non_geo_counts = [
        len(axis_buckets[axis])
        for axis in ("business_segment", "product")
        if axis_buckets[axis]
    ]
    if len(non_geo_counts) < 2:
        return False
    high = max(non_geo_counts)
    low = min(non_geo_counts)
    return high > 0 and (low / high) >= 0.7


def _count_error_warning_findings(*groups: Iterable[Any]) -> tuple[int, int]:
    errors = 0
    warnings = 0
    for group in groups:
        for finding in group:
            if finding.severity == "error":
                errors += 1
            elif finding.severity == "warning":
                warnings += 1
    return errors, warnings


def _all_normalized_values_missing(obs: dict[str, Any]) -> bool:
    values = obs.get("values") or []
    return all(value.get("value_normalized") is None for value in values)


def _map_behavior(obs: dict[str, Any]) -> str | None:
    behavior = obs.get("behavior")
    if behavior in _ALLOWED_BEHAVIORS:
        return behavior
    direction = str(obs.get("direction") or "").lower()
    if direction in {"increased", "increasing", "growth", "grew", "up"}:
        return "growing"
    if direction in {"decreased", "decreasing", "declined", "down"}:
        return "declining"
    if direction in {"flat", "unchanged", "stable"}:
        return "roughly_flat"
    return None


def _kpi_frequency(group: list[dict[str, Any]]) -> str:
    frames = {
        value.get("period_frame")
        for obs in group
        for value in (obs.get("values") or [])
        if value.get("value_normalized") is not None
    }
    return "annual" if frames == {"annual"} else "ad_hoc"


def _kpi_source(obs: dict[str, Any]) -> str | None:
    filing_id = obs.get("filing_id")
    provenance = obs.get("provenance") or []
    span = None
    if provenance and isinstance(provenance[0], dict):
        span = provenance[0].get("span")
    if filing_id and span:
        return f"{filing_id}: {str(span)[:120]}"
    if filing_id:
        return str(filing_id)
    return str(span)[:120] if span else None


def _segment_id_for_observation(obs: dict[str, Any]) -> str:
    segment_hint = obs.get("segment_hint") or {}
    if segment_hint.get("edgar_member"):
        return _segment_id_from_member(segment_hint["edgar_member"])
    if segment_hint.get("segment_label_normalized"):
        return _snake_case(segment_hint["segment_label_normalized"])
    return "consolidated_unmapped"


def _segment_id_from_member(edgar_member: str) -> str:
    local = _local_member_name(edgar_member)
    if local.endswith("Member"):
        local = local[: -len("Member")]
    return _snake_case(local)


def _segment_label(observations: list[dict[str, Any]]) -> str:
    segment_hint = observations[0].get("segment_hint") or {}
    return (
        segment_hint.get("segment_label")
        or _title_label(segment_hint.get("segment_label_normalized"))
        or _title_label(observations[0].get("metric_name_normalized"))
        or "Segment"
    )


def _segment_match_name(observations: list[dict[str, Any]]) -> str:
    segment_hint = observations[0].get("segment_hint") or {}
    return (
        segment_hint.get("segment_label_normalized")
        or str(segment_hint.get("segment_label") or "").strip().lower()
        or str(observations[0].get("metric_name_normalized") or "").strip().lower()
        or "segment"
    )


def _local_member_name(edgar_member: str) -> str:
    return str(edgar_member).split(":")[-1]


def _snake_case(value: Any) -> str:
    raw = str(value or "").strip()
    raw = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw)
    raw = re.sub(r"[^A-Za-z0-9]+", "_", raw).strip("_").lower()
    raw = re.sub(r"_+", "_", raw)
    if not raw:
        raw = "kpi"
    if not raw[0].isalpha():
        raw = f"kpi_{raw}"
    return raw


def _unique_node_id(base: str, used: set[str]) -> str:
    candidate = base
    suffix = 2
    while candidate in used:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def _title_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("_", " ")).title()


def _unique_preserve_order(values: Iterable[Any]) -> list[Any]:
    seen: set[Any] = set()
    unique: list[Any] = []
    for value in values:
        if value is None or value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def _first_nonempty(values: Iterable[Any]) -> Any | None:
    for value in values:
        if value:
            return value
    return None


def _period_part(value: Any) -> str:
    if value is None:
        return "null"
    return str(value)


def _decode_period_part(value: str) -> str | None:
    return None if value == "null" else value


__all__ = [
    "TOLERANCE_ERROR",
    "TOLERANCE_WARNING",
    "_ALLOWED_BEHAVIORS",
    "_ALLOWED_FACTORS",
    "_AXIS_PRIORITY",
    "_BUSINESS_SEGMENT_SUFFIXES",
    "_GEOGRAPHY_MEMBER_NAMES",
    "_GEOGRAPHY_SUFFIXES",
    "_NodeBundle",
    "_REJECTION_REASONS",
    "_SegmentBundle",
    "_UNIT_MAP",
    "_all_normalized_values_missing",
    "_axis_choice_needed",
    "_axis_qname",
    "_count_error_warning_findings",
    "_decode_period_key",
    "_decode_period_part",
    "_encode_period_key",
    "_emit_business_model_draft",
    "_first_nonempty",
    "_group_observations",
    "_is_revenue_like_node",
    "_is_single_node_revenue_fallback",
    "_is_total_revenue_observation",
    "_kpi_frequency",
    "_kpi_source",
    "_local_member_name",
    "_map_behavior",
    "_period_key_recency",
    "_period_part",
    "_pick_primary_axis",
    "_resolve_primary_axis",
    "_segment_id_for_observation",
    "_segment_id_from_member",
    "_segment_label",
    "_segment_match_name",
    "_snake_case",
    "_single_axis_qname",
    "_title_label",
    "_unique_node_id",
    "_unique_preserve_order",
    "_values_by_period_key",
]
