"""Segment profile data types and revenue observation helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence

from .model_build_context import SegmentRevenueObservation
from .segment_fact_helpers import _annotate_revenue_comparability


@dataclass
class SegmentInfo:
    name: str
    edgar_member: Optional[str] = None
    revenue_observations: Optional[Dict[int, SegmentRevenueObservation]] = None
    volume_label: Optional[str] = None
    price_label: Optional[str] = None
    item_ids: Optional[Dict[str, str]] = None


@dataclass
class SegmentProfile:
    ticker: str
    segments: List[SegmentInfo]
    source: str
    axis_used: Optional[str] = None
    total_revenue_check: Optional[Dict[int, float]] = None


def segment_revenue_values(segment: SegmentInfo) -> Dict[int, float]:
    """Return model-ready revenue values derived from typed observations."""

    observations = segment.revenue_observations or {}
    values: Dict[int, float] = {}
    for raw_year, observation in observations.items():
        year = int(getattr(observation, "fiscal_year", raw_year))
        value = getattr(observation, "value", None)
        if value is None:
            continue
        values[year] = float(value)
    return dict(sorted(values.items()))


def segment_revenue_observation_list(segment: SegmentInfo) -> list[SegmentRevenueObservation] | None:
    observations = segment.revenue_observations or {}
    if not observations:
        return None
    return [
        observation
        for _year, observation in sorted(
            ((int(year), observation) for year, observation in observations.items()),
            key=lambda item: item[0],
        )
    ]


def revenue_observations_to_values(
    observations: Sequence[SegmentRevenueObservation] | None,
) -> Dict[int, float]:
    values: Dict[int, float] = {}
    for observation in observations or []:
        values[int(observation.fiscal_year)] = float(observation.value)
    return dict(sorted(values.items()))


def segment_revenue_observations_from_snapshot(
    snapshot_segment: object,
) -> Dict[int, SegmentRevenueObservation] | None:
    observations = getattr(snapshot_segment, "revenue_observations", None)
    if not observations:
        return None
    return {
        int(observation.fiscal_year): observation
        for observation in sorted(observations, key=lambda item: int(item.fiscal_year))
    }


def _derived_revenue_observations(
    values: Dict[int, float],
    *,
    source: str,
    note: str,
    basis_by_year: Mapping[int, str] | None = None,
) -> Dict[int, SegmentRevenueObservation]:
    observations: Dict[int, SegmentRevenueObservation] = {}
    for year, value in sorted(values.items()):
        observations[int(year)] = SegmentRevenueObservation(
            fiscal_year=int(year),
            value=float(value),
            source=source,
            basis_key=(basis_by_year or {}).get(int(year)),
            comparable_with_prior="unknown",
            comparability_note=note,
        )
    return _annotate_revenue_comparability(observations)


def _derived_component_basis_by_year(
    source: str,
    values: Dict[int, float],
    basis_parts_by_year: Dict[int, set[str]],
) -> Dict[int, str]:
    basis_by_year: Dict[int, str] = {}
    for year, value in sorted(values.items()):
        if abs(float(value)) <= 1e-9:
            continue
        parts = sorted(part for part in basis_parts_by_year.get(int(year), set()) if part)
        if not parts:
            continue
        basis_by_year[int(year)] = f"{source}:" + " + ".join(parts)
    return basis_by_year


@dataclass
class MultiAxisResult:
    ticker: str
    profiles: List[SegmentProfile]
    total_revenue_check: Optional[Dict[int, float]] = None
    payloads_by_year: Dict[int, dict] = field(default_factory=dict)


@dataclass
class ExpandResult:
    segments_created: int
    items_added: int
    items_relabeled: int


def _segment_sort_key(segment: SegmentInfo) -> tuple[float, str]:
    values = list(segment_revenue_values(segment).values())
    average = sum(float(value) for value in values) / len(values) if values else float("-inf")
    return (average, segment.name)


__all__ = [
    "ExpandResult",
    "MultiAxisResult",
    "SegmentInfo",
    "SegmentProfile",
    "_derived_component_basis_by_year",
    "_derived_revenue_observations",
    "_segment_sort_key",
    "revenue_observations_to_values",
    "segment_revenue_observation_list",
    "segment_revenue_observations_from_snapshot",
    "segment_revenue_values",
]
