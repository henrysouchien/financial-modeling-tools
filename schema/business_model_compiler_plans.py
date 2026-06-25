from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from .business_model import BusinessModel
from .business_model_compiler_errors import BusinessModelCompileError
from .model_build_context import SegmentProfileSnapshot
from .segments import SegmentInfo, SegmentProfile, segment_revenue_observations_from_snapshot


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

    plans: list[_SegmentCompilePlan] = []
    matched_snapshot_names: set[str] = set()
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
        if normalized in matched_snapshot_names:
            raise BusinessModelCompileError(f"snapshot segment {snapshot_segment.name!r} matched more than once")
        if segment.edgar_member and snapshot_segment.edgar_member and segment.edgar_member != snapshot_segment.edgar_member:
            raise BusinessModelCompileError(
                "EDGAR member conflict for BM segment "
                f"{segment.id!r}: business_model={segment.edgar_member!r}, "
                f"snapshot={snapshot_segment.edgar_member!r}"
            )

        matched_snapshot_names.add(normalized)
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
        if _normalize_name(snapshot_segment.name) not in matched_snapshot_names
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
    if not plans or plans[0].segment_index != 1:
        raise BusinessModelCompileError("EDGAR reconciliation must produce a primary segment with segment_index == 1")

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
