from __future__ import annotations

from collections import defaultdict
import logging
from typing import Dict, List

from .segment_profile_helpers import (
    SegmentInfo,
    SegmentProfile,
    _derived_component_basis_by_year,
    _derived_revenue_observations,
    segment_revenue_values,
)


def apply_segment_overrides(discovered: SegmentProfile, mapping: List[Dict]) -> SegmentProfile:
    """Apply caller-provided naming, KPI label, and ordering overrides."""

    discovered_by_member = {
        segment.edgar_member: segment
        for segment in discovered.segments
        if segment.edgar_member
    }
    matched_members: set[str] = set()
    segments: List[SegmentInfo] = []
    other_values: Dict[int, float] = defaultdict(float)
    other_basis_parts_by_year: Dict[int, set[str]] = defaultdict(set)

    for entry in list(mapping or []):
        member = str(entry.get("edgar_member") or "").strip()
        if not member:
            continue
        if member in matched_members:
            logging.warning("Ignoring duplicate segment_mapping entry for '%s'", member)
            continue

        discovered_segment = discovered_by_member.get(member)
        if discovered_segment is None:
            logging.warning("Ignoring unmatched segment_mapping entry for '%s'", member)
            continue

        matched_members.add(member)
        segments.append(
            SegmentInfo(
                name=str(entry.get("name") or discovered_segment.name),
                edgar_member=discovered_segment.edgar_member,
                revenue_observations=dict(discovered_segment.revenue_observations or {}),
                volume_label=entry.get("volume_label") or discovered_segment.volume_label,
                price_label=entry.get("price_label") or discovered_segment.price_label,
            )
        )

    for segment in discovered.segments:
        if segment.edgar_member and segment.edgar_member in matched_members:
            continue
        for year, value in segment_revenue_values(segment).items():
            other_values[int(year)] += float(value)
            observation = (segment.revenue_observations or {}).get(int(year))
            basis_key = getattr(observation, "basis_key", None)
            if basis_key:
                other_basis_parts_by_year[int(year)].add(str(basis_key))

    if any(abs(value) > 1e-9 for value in other_values.values()):
        other_basis_by_year = _derived_component_basis_by_year(
            "derived_other",
            other_values,
            other_basis_parts_by_year,
        )
        segments.append(
            SegmentInfo(
                name="Other",
                revenue_observations=_derived_revenue_observations(
                    other_values,
                    source="derived_other",
                    note="Aggregated from unmatched segment members after caller override.",
                    basis_by_year=other_basis_by_year,
                ),
            )
        )

    return SegmentProfile(
        ticker=discovered.ticker,
        segments=segments,
        source="caller_override",
        axis_used=discovered.axis_used,
        total_revenue_check=dict(discovered.total_revenue_check or {}),
    )
