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
    edgar_members: Optional[List[str]] = None
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


def aggregate_segment_revenue_observations(
    snapshot_segments: Sequence[object],
    *,
    axis_used: str | None = None,
) -> Dict[int, SegmentRevenueObservation] | None:
    ordered_segments = sorted(
        snapshot_segments,
        key=lambda segment: int(getattr(segment, "segment_index", 0) or 0),
    )
    observations_by_segment: list[tuple[object, dict[int, SegmentRevenueObservation]]] = []
    for snapshot_segment in ordered_segments:
        observations = segment_revenue_observations_from_snapshot(snapshot_segment) or {}
        if observations:
            observations_by_segment.append((snapshot_segment, observations))
    if not observations_by_segment:
        return None

    all_years = sorted(
        {
            int(year)
            for _snapshot_segment, observations in observations_by_segment
            for year in observations
        }
    )
    full_contributor_keys = tuple(_segment_contributor_key(segment) for segment in ordered_segments)
    aggregated: Dict[int, SegmentRevenueObservation] = {}
    prior_contributor_keys: tuple[str, ...] | None = None
    prior_basis_key: str | None = None
    normalized_axis = str(axis_used or "").strip() or None

    for year in all_years:
        present: list[tuple[object, SegmentRevenueObservation]] = [
            (snapshot_segment, observations[int(year)])
            for snapshot_segment, observations in observations_by_segment
            if int(year) in observations
        ]
        if not present:
            continue
        _validate_aggregated_observation_axes(year, present, axis_used=normalized_axis)
        scale = _aggregated_observation_scale(year, present)
        contributor_keys = tuple(_segment_contributor_key(segment) for segment, _observation in present)
        basis_parts = sorted(
            {
                str(observation.basis_key).strip()
                for _segment, observation in present
                if str(observation.basis_key or "").strip()
            }
        )
        basis_key = "reconciliation:" + " + ".join(basis_parts or contributor_keys)
        partial = contributor_keys != full_contributor_keys
        note = _aggregated_observation_note(
            present,
            full_segments=ordered_segments,
            basis_parts=basis_parts,
            partial=partial,
        )
        if prior_contributor_keys is None:
            comparable = "unknown" if partial else "not_applicable"
        elif partial or contributor_keys != prior_contributor_keys or (
            prior_basis_key and basis_key and prior_basis_key != basis_key
        ):
            comparable = "not_comparable"
        else:
            comparable = "comparable"

        tags = {
            str(observation.tag).strip()
            for _segment, observation in present
            if str(observation.tag or "").strip()
        }
        aggregated[int(year)] = SegmentRevenueObservation(
            fiscal_year=int(year),
            value=sum(float(observation.value) for _segment, observation in present),
            source="reconciliation",
            tag=next(iter(tags)) if len(tags) == 1 else None,
            scale=scale,
            axis=normalized_axis or _single_observation_axis(present),
            basis_key=basis_key,
            comparable_with_prior=comparable,
            comparability_note=note,
        )
        prior_contributor_keys = contributor_keys
        prior_basis_key = basis_key

    return aggregated or None


def _validate_aggregated_observation_axes(
    year: int,
    present: Sequence[tuple[object, SegmentRevenueObservation]],
    *,
    axis_used: str | None,
) -> None:
    axes = {
        str(observation.axis).strip()
        for _segment, observation in present
        if str(observation.axis or "").strip()
    }
    if len(axes) > 1:
        members = ", ".join(_segment_contributor_key(segment) for segment, _observation in present)
        raise ValueError(
            f"cannot aggregate absorbed observations for fiscal_year={year}: "
            f"multiple axes {sorted(axes)} for members [{members}]"
        )
    if axis_used and axes and axes != {axis_used}:
        members = ", ".join(_segment_contributor_key(segment) for segment, _observation in present)
        raise ValueError(
            f"cannot aggregate absorbed observations for fiscal_year={year}: "
            f"observation axis {sorted(axes)} does not match snapshot axis {axis_used!r} "
            f"for members [{members}]"
        )


def _aggregated_observation_scale(
    year: int,
    present: Sequence[tuple[object, SegmentRevenueObservation]],
) -> str | None:
    scales = [str(observation.scale).strip() if observation.scale is not None else None for _segment, observation in present]
    present_scales = {scale for scale in scales if scale}
    members = ", ".join(_segment_contributor_key(segment) for segment, _observation in present)
    if present_scales and any(scale is None or scale == "" for scale in scales):
        raise ValueError(
            f"cannot aggregate absorbed observations for fiscal_year={year}: "
            f"mixed present/absent scales for members [{members}]"
        )
    if len(present_scales) > 1:
        raise ValueError(
            f"cannot aggregate absorbed observations for fiscal_year={year}: "
            f"unequal scales {sorted(present_scales)} for members [{members}]"
        )
    return next(iter(present_scales)) if present_scales else None


def _aggregated_observation_note(
    present: Sequence[tuple[object, SegmentRevenueObservation]],
    *,
    full_segments: Sequence[object],
    basis_parts: Sequence[str],
    partial: bool,
) -> str:
    contributors = ", ".join(_segment_contributor_label(segment) for segment, _observation in present)
    full_claims = ", ".join(_segment_contributor_label(segment) for segment in full_segments)
    prefix = "absorbed revenue partial-year contributors" if partial else "absorbed revenue contributors"
    note = f"{prefix}: {contributors}; full claim set: {full_claims}"
    if basis_parts:
        note += "; basis_keys: " + " + ".join(basis_parts)
    return note


def _single_observation_axis(
    present: Sequence[tuple[object, SegmentRevenueObservation]],
) -> str | None:
    axes = [
        str(observation.axis).strip()
        for _segment, observation in present
        if str(observation.axis or "").strip()
    ]
    return axes[0] if axes else None


def _segment_contributor_label(snapshot_segment: object) -> str:
    name = str(getattr(snapshot_segment, "name", "") or "").strip()
    member = str(getattr(snapshot_segment, "edgar_member", "") or "").strip()
    if name and member:
        return f"{name} ({member})"
    return name or member or f"segment_index={getattr(snapshot_segment, 'segment_index', '')}"


def _segment_contributor_key(snapshot_segment: object) -> str:
    member = str(getattr(snapshot_segment, "edgar_member", "") or "").strip()
    if member:
        return member
    name = str(getattr(snapshot_segment, "name", "") or "").strip()
    if name:
        return name
    return f"segment_index={getattr(snapshot_segment, 'segment_index', '')}"


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
    "aggregate_segment_revenue_observations",
    "_derived_component_basis_by_year",
    "_derived_revenue_observations",
    "_segment_sort_key",
    "revenue_observations_to_values",
    "segment_revenue_observation_list",
    "segment_revenue_observations_from_snapshot",
    "segment_revenue_values",
]
