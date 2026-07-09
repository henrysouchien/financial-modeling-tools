from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from .business_model import BusinessModel
from .business_model_compiler_errors import BusinessModelCompileError
from .model_build_context import SegmentProfileSnapshot
from .segments import (
    SegmentInfo,
    SegmentProfile,
    segment_revenue_observations_from_snapshot,
)
from .segment_profile_helpers import aggregate_segment_revenue_observations


_NORMALIZE_NAME_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class _SegmentCompilePlan:
    segment: Any | None
    segment_index: int
    info: SegmentInfo
    segment_id: str | None = None
    unmanaged_snapshot: bool = False


def _reconcile_segments(
    business_model: BusinessModel,
    edgar_snapshot: SegmentProfileSnapshot | None,
) -> tuple[list[_SegmentCompilePlan], dict[str, int], SegmentProfile]:
    if edgar_snapshot is None:
        plans = [
            _SegmentCompilePlan(
                segment=segment,
                segment_index=index,
                info=SegmentInfo(
                    name=segment.label,
                    edgar_member=segment.edgar_member,
                ),
            )
            for index, segment in enumerate(business_model.segments, start=1)
        ]
        return (
            plans,
            {segment.id: index for index, segment in enumerate(business_model.segments, start=1)},
            SegmentProfile(
                ticker=business_model.company.ticker,
                segments=[plan.info for plan in plans],
                source="caller_override",
            ),
        )

    snapshot_by_name: dict[str, Any] = {}
    snapshot_by_index: dict[int, Any] = {}
    snapshot_by_member: dict[str, Any] = {}
    for snapshot_segment in edgar_snapshot.segments:
        normalized = _normalize_name(snapshot_segment.name)
        if normalized in snapshot_by_name:
            raise BusinessModelCompileError(
                f"snapshot contains duplicate segment names after normalization: {snapshot_segment.name!r}"
            )
        if snapshot_segment.segment_index in snapshot_by_index:
            raise BusinessModelCompileError(
                f"snapshot contains duplicate segment_index values: {snapshot_segment.segment_index}"
            )
        snapshot_by_name[normalized] = snapshot_segment
        snapshot_by_index[int(snapshot_segment.segment_index)] = snapshot_segment
        member = str(getattr(snapshot_segment, "edgar_member", "") or "").strip()
        if member:
            if member in snapshot_by_member:
                raise BusinessModelCompileError(
                    f"snapshot contains duplicate edgar_member values: {member!r}"
                )
            snapshot_by_member[member] = snapshot_segment

    plans: list[_SegmentCompilePlan] = []
    matched_snapshot_indices: set[int] = set()
    matched_snapshot_owner: dict[int, str] = {}
    segment_mapping: dict[str, int] = {}
    used_segment_ids = {str(segment.id) for segment in business_model.segments}
    next_supplemental_index = max(snapshot_by_index) + 1 if snapshot_by_index else 1

    for segment in business_model.segments:
        normalized = _normalize_name(segment.match_name)
        snapshot_segment = snapshot_by_name.get(normalized)
        if snapshot_segment is None:
            if segment.edgar_axis or segment.edgar_member:
                raise BusinessModelCompileError(
                    f"no EDGAR snapshot match for BM segment {segment.id!r} (match_name={segment.match_name!r})"
                )
            if segment.absorbs:
                resolved_segments = _resolve_absorbed_snapshot_segments(
                    segment,
                    snapshot_by_member=snapshot_by_member,
                    snapshot_by_name=snapshot_by_name,
                )
                segment_index = min(int(candidate.segment_index) for candidate in resolved_segments)
                _claim_snapshot_segments(
                    segment,
                    resolved_segments,
                    matched_snapshot_owner=matched_snapshot_owner,
                    matched_snapshot_indices=matched_snapshot_indices,
                )
                segment_mapping[segment.id] = segment_index
                plans.append(
                    _SegmentCompilePlan(
                        segment=segment,
                        segment_index=segment_index,
                        info=_absorbed_segment_info(
                            segment,
                            resolved_segments,
                            axis_used=edgar_snapshot.axis_used,
                        ),
                        segment_id=segment.id,
                    )
                )
                continue
            # A BusinessModel can include source-backed revenue streams that
            # are not EDGAR axis members, e.g. interest income in total revenue.
            segment_index = next_supplemental_index
            next_supplemental_index += 1
            segment_mapping[segment.id] = segment_index
            plans.append(
                _SegmentCompilePlan(
                    segment=segment,
                    segment_index=segment_index,
                    info=SegmentInfo(
                        name=segment.label,
                        edgar_member=None,
                    ),
                    segment_id=segment.id,
                )
            )
            continue
        if segment.edgar_member and snapshot_segment.edgar_member and segment.edgar_member != snapshot_segment.edgar_member:
            raise BusinessModelCompileError(
                "EDGAR member conflict for BM segment "
                f"{segment.id!r}: business_model={segment.edgar_member!r}, "
                f"snapshot={snapshot_segment.edgar_member!r}"
            )

        if segment.absorbs:
            resolved_segments = _unique_snapshot_segments(
                [
                    snapshot_segment,
                    *_resolve_absorbed_snapshot_segments(
                        segment,
                        snapshot_by_member=snapshot_by_member,
                        snapshot_by_name=snapshot_by_name,
                    ),
                ]
            )
            segment_index = min(int(candidate.segment_index) for candidate in resolved_segments)
            _claim_snapshot_segments(
                segment,
                resolved_segments,
                matched_snapshot_owner=matched_snapshot_owner,
                matched_snapshot_indices=matched_snapshot_indices,
            )
            segment_mapping[segment.id] = segment_index
            plans.append(
                _SegmentCompilePlan(
                    segment=segment,
                    segment_index=segment_index,
                    info=_absorbed_segment_info(
                        segment,
                        resolved_segments,
                        axis_used=edgar_snapshot.axis_used,
                    ),
                    segment_id=segment.id,
                )
            )
            continue

        _claim_snapshot_segments(
            segment,
            [snapshot_segment],
            matched_snapshot_owner=matched_snapshot_owner,
            matched_snapshot_indices=matched_snapshot_indices,
        )
        segment_mapping[segment.id] = int(snapshot_segment.segment_index)
        plans.append(
            _SegmentCompilePlan(
                segment=segment,
                segment_index=int(snapshot_segment.segment_index),
                info=SegmentInfo(
                    name=segment.label,
                    edgar_member=segment.edgar_member or snapshot_segment.edgar_member,
                    revenue_observations=segment_revenue_observations_from_snapshot(snapshot_segment),
                    volume_label=snapshot_segment.volume_label,
                    price_label=snapshot_segment.price_label,
                ),
                segment_id=segment.id,
            )
        )

    unmatched_snapshot_segments = [
        snapshot_segment
        for snapshot_segment in edgar_snapshot.segments
        if int(snapshot_segment.segment_index) not in matched_snapshot_indices
    ]
    residual_snapshot_segments = [
        snapshot_segment
        for snapshot_segment in unmatched_snapshot_segments
        if _is_unmanaged_residual_snapshot_segment(snapshot_segment)
    ]
    residual_snapshot_segment_ids = {id(snapshot_segment) for snapshot_segment in residual_snapshot_segments}
    blocking_unmatched_snapshot_names = sorted(
        snapshot_segment.name
        for snapshot_segment in unmatched_snapshot_segments
        if id(snapshot_segment) not in residual_snapshot_segment_ids
    )
    for snapshot_segment in residual_snapshot_segments:
        segment_id = _unmanaged_snapshot_segment_id(
            snapshot_segment.name,
            int(snapshot_segment.segment_index),
            used_segment_ids,
        )
        plans.append(
            _SegmentCompilePlan(
                segment=None,
                segment_index=int(snapshot_segment.segment_index),
                info=SegmentInfo(
                    name=snapshot_segment.name,
                    edgar_member=None,
                    revenue_observations=segment_revenue_observations_from_snapshot(snapshot_segment),
                    volume_label=snapshot_segment.volume_label,
                    price_label=snapshot_segment.price_label,
                ),
                segment_id=segment_id,
                unmanaged_snapshot=True,
            )
        )
    unmatched_snapshot_names = blocking_unmatched_snapshot_names
    if unmatched_snapshot_names:
        raise BusinessModelCompileError(
            f"EDGAR snapshot has unmatched segments: {unmatched_snapshot_names}"
        )

    plans.sort(key=lambda plan: plan.segment_index)
    if (
        not plans
        or plans[0].segment_index != 1
        or plans[0].segment is None
        or plans[0].unmanaged_snapshot
    ):
        raise BusinessModelCompileError(
            "EDGAR reconciliation must produce a managed primary segment with segment_index == 1"
        )

    return (
        plans,
        segment_mapping,
        SegmentProfile(
            ticker=business_model.company.ticker,
            segments=[plan.info for plan in plans],
            source=edgar_snapshot.source,
            axis_used=edgar_snapshot.axis_used,
            total_revenue_check=dict(edgar_snapshot.total_revenue_check) if edgar_snapshot.total_revenue_check else None,
        ),
    )


def _is_unmanaged_residual_snapshot_segment(snapshot_segment: Any) -> bool:
    if str(getattr(snapshot_segment, "edgar_member", "") or "").strip():
        return False
    if _normalize_name(getattr(snapshot_segment, "name", "")) not in {
        "other",
        "other segments",
        "all other",
    }:
        return False
    observations = list(getattr(snapshot_segment, "revenue_observations", None) or [])
    if not observations:
        return False
    return all(str(getattr(observation, "source", "") or "") == "derived_other" for observation in observations)


def _resolve_absorbed_snapshot_segments(
    segment: Any,
    *,
    snapshot_by_member: dict[str, Any],
    snapshot_by_name: dict[str, Any],
) -> list[Any]:
    resolved: list[Any] = []
    for claim in segment.absorbs or []:
        member = str(getattr(claim, "member", "") or "").strip()
        name = str(getattr(claim, "name", "") or "").strip()
        by_member = snapshot_by_member.get(member) if member else None
        by_name = snapshot_by_name.get(_normalize_name(name)) if name else None
        if by_member is not None and by_name is not None and int(by_member.segment_index) != int(by_name.segment_index):
            raise BusinessModelCompileError(
                f"absorbed claim for BM segment {segment.id!r} resolves to different EDGAR "
                f"segments by member and name: member={member!r}, name={name!r}"
            )
        resolved_segment = by_member or by_name
        if resolved_segment is None:
            raise BusinessModelCompileError(
                f"absorbed claim for BM segment {segment.id!r} could not resolve to EDGAR snapshot: "
                f"name={name!r}, member={member!r}"
            )
        resolved.append(resolved_segment)
    return _unique_snapshot_segments(resolved)


def _unique_snapshot_segments(snapshot_segments: list[Any]) -> list[Any]:
    unique: dict[int, Any] = {}
    for snapshot_segment in snapshot_segments:
        unique[int(snapshot_segment.segment_index)] = snapshot_segment
    return sorted(unique.values(), key=lambda segment: int(segment.segment_index))


def _claim_snapshot_segments(
    segment: Any,
    snapshot_segments: list[Any],
    *,
    matched_snapshot_owner: dict[int, str],
    matched_snapshot_indices: set[int],
) -> None:
    for snapshot_segment in snapshot_segments:
        segment_index = int(snapshot_segment.segment_index)
        owner = matched_snapshot_owner.get(segment_index)
        if owner is not None and owner != segment.id:
            raise BusinessModelCompileError(
                f"EDGAR snapshot segment {snapshot_segment.name!r} is claimed by multiple BM segments: "
                f"{owner!r} and {segment.id!r}"
            )
        matched_snapshot_owner[segment_index] = str(segment.id)
        matched_snapshot_indices.add(segment_index)


def _absorbed_segment_info(
    segment: Any,
    resolved_segments: list[Any],
    *,
    axis_used: str | None,
) -> SegmentInfo:
    ordered_segments = sorted(resolved_segments, key=lambda candidate: int(candidate.segment_index))
    edgar_members = [
        str(candidate.edgar_member).strip()
        for candidate in ordered_segments
        if str(getattr(candidate, "edgar_member", "") or "").strip()
    ]
    try:
        revenue_observations = aggregate_segment_revenue_observations(
            ordered_segments,
            axis_used=axis_used,
        )
    except ValueError as exc:
        raise BusinessModelCompileError(str(exc)) from exc
    return SegmentInfo(
        name=segment.label,
        edgar_member=edgar_members[0] if len(edgar_members) == 1 else None,
        edgar_members=edgar_members or None,
        revenue_observations=revenue_observations,
        volume_label=_common_snapshot_label(ordered_segments, "volume_label"),
        price_label=_common_snapshot_label(ordered_segments, "price_label"),
    )


def _common_snapshot_label(snapshot_segments: list[Any], attr: str) -> str | None:
    values = [getattr(segment, attr, None) for segment in snapshot_segments]
    if not values or any(value is None for value in values):
        return None
    normalized = {str(value) for value in values}
    if len(normalized) != 1:
        return None
    return str(values[0])


def _unmanaged_snapshot_segment_id(name: str, segment_index: int, used_segment_ids: set[str]) -> str:
    normalized = _NORMALIZE_NAME_RE.sub("_", str(name or "").strip().casefold()).strip("_")
    normalized = "_".join(part for part in normalized.split("_") if part) or "segment"
    base = f"unmodeled_{normalized}_{int(segment_index)}"
    candidate = base
    suffix = 2
    while candidate in used_segment_ids:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used_segment_ids.add(candidate)
    return candidate


def _normalize_name(value: str) -> str:
    normalized = _NORMALIZE_NAME_RE.sub(" ", str(value or "").strip().casefold())
    return " ".join(normalized.split())
