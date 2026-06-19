from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import Field, model_validator

from .thesis_shared_slice import _ContractModel


Severity = Literal["info", "warning", "error"]
BridgeAxis = Literal["business_segment", "product", "geography"]
BridgeReadinessStatus = Literal[
    "ready_for_compile",
    "needs_driver_assignment",
    "needs_axis_choice",
    "blocked",
]
_AXIS_QNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z][A-Za-z0-9_-]*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class BridgeSummary(_ContractModel):
    total_observations_in_catalog: int = 0
    observations_translated: int = 0
    observations_rejected: int = 0
    rejected_by_reason: dict[str, int] = Field(default_factory=dict)
    segments_created: int = 0
    nodes_created: int = 0
    revenue_share_resolved: bool = False
    revenue_share_basis_period_key: str | None = None
    errors: int = 0
    warnings: int = 0


class RejectedObservation(_ContractModel):
    metric_name_normalized: str | None = None
    reason: str
    severity: Severity
    metric_kind: str | None = None
    direction_observed: str | None = None
    source_type: str | None = None
    filing_id: str | None = None
    detail: str | None = None


class NodeFinding(_ContractModel):
    segment_id: str | None = None
    node_id: str | None = None
    kind: str
    severity: Severity
    detail: str | None = None
    period_key: str | None = None
    raw_value_first: Any | None = None
    raw_value_conflicting: Any | None = None


class SegmentFinding(_ContractModel):
    segment_id: str | None = None
    edgar_member: str | None = None
    axis: str | None = None
    kind: str
    severity: Severity
    detail: str | None = None


class GlobalFinding(_ContractModel):
    kind: str
    severity: Severity
    detail: str | None = None


class AlternativeSegmentSet(_ContractModel):
    axis: BridgeAxis
    edgar_members: list[str] = Field(default_factory=list)
    edgar_members_by_axis: dict[str, list[str]] = Field(default_factory=dict)
    node_count: int = 0
    rationale: str
    sample_driver_nodes: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _autofill_flat_members(cls, data):
        # R4 #5 + R5 nit + R6 nit: auto-fill flat list from structured dict when
        # flat is empty. `mode="before"` runs on raw input dict; copy before
        # mutation (don't surprise the caller). Guard isinstance so invalid input
        # (non-dict) falls to normal Pydantic validation rather than a validator-
        # side AttributeError.
        if not isinstance(data, dict):
            return data
        structured = data.get("edgar_members_by_axis") or {}
        if not isinstance(structured, dict):
            return data  # let Pydantic surface the type error
        flat = data.get("edgar_members") or []
        if structured and not flat:
            data = {**data}  # copy before mutation
            data["edgar_members"] = sorted({
                m
                for members in structured.values()
                if isinstance(members, (list, tuple))  # guard before iterating
                for m in members
            })
        return data

    @model_validator(mode="after")
    def _validate_axis_keys(self) -> "AlternativeSegmentSet":
        for axis_qname in self.edgar_members_by_axis:
            if not _AXIS_QNAME_RE.match(axis_qname):
                raise ValueError(
                    f"edgar_members_by_axis key {axis_qname!r} must be an XBRL QName"
                )
        # Cross-check: flat edgar_members must equal the union of structured values
        # when both are populated (defensive - bridge should keep them in sync).
        if self.edgar_members_by_axis and self.edgar_members:
            flat_from_structured = sorted({
                m for members in self.edgar_members_by_axis.values() for m in members
            })
            if sorted(set(self.edgar_members)) != flat_from_structured:
                raise ValueError(
                    "edgar_members and edgar_members_by_axis are inconsistent"
                )
        return self


class AxisInventoryEntry(_ContractModel):
    axis: BridgeAxis
    axis_qname: str | None = None
    members: list[str] = Field(default_factory=list)
    member_labels: dict[str, str] = Field(default_factory=dict)
    observation_count: int = 0
    node_count: int = 0
    selected_as_primary: bool = False

    @model_validator(mode="after")
    def _validate_axis_qname(self) -> "AxisInventoryEntry":
        if self.axis_qname is not None and not _AXIS_QNAME_RE.match(self.axis_qname):
            raise ValueError(
                f"axis_qname {self.axis_qname!r} must be an XBRL QName"
            )
        return self


class BridgeReport(_ContractModel):
    ticker: str
    filing_id: str | None = None
    generated_at: str = Field(default_factory=utc_now_iso)
    bridge_version: str = "1.0"
    primary_axis: BridgeAxis | None = None
    primary_axis_qname: str | None = None
    readiness_status: BridgeReadinessStatus
    summary: BridgeSummary = Field(default_factory=BridgeSummary)
    rejected_observations: list[RejectedObservation] = Field(default_factory=list)
    findings_per_node: list[NodeFinding] = Field(default_factory=list)
    findings_per_segment: list[SegmentFinding] = Field(default_factory=list)
    global_findings: list[GlobalFinding] = Field(default_factory=list)
    axis_inventory: list[AxisInventoryEntry] = Field(default_factory=list)
    alternative_segment_sets: dict[BridgeAxis, AlternativeSegmentSet] = Field(default_factory=dict)
    consolidated_revenue_totals: dict[str, float] = Field(default_factory=dict)
    observed_values_per_node: dict[str, dict[str, float]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_primary_axis_qname(self) -> "BridgeReport":
        if self.primary_axis_qname is not None and not _AXIS_QNAME_RE.match(self.primary_axis_qname):
            raise ValueError(
                f"primary_axis_qname {self.primary_axis_qname!r} must be an XBRL QName"
            )
        return self

    def headline(self) -> str:
        primary = self.primary_axis or "none"
        return (
            f"{self.ticker} KPI bridge: {self.readiness_status}; "
            f"primary_axis={primary}; nodes={self.summary.nodes_created}; "
            f"errors={self.summary.errors}; warnings={self.summary.warnings}"
        )


class CompletionFinding(_ContractModel):
    kind: str
    severity: Severity
    detail: str
    segment_id: str | None = None
    node_id: str | None = None


__all__ = [
    "AlternativeSegmentSet",
    "AxisInventoryEntry",
    "BridgeAxis",
    "BridgeReadinessStatus",
    "BridgeReport",
    "BridgeSummary",
    "CompletionFinding",
    "GlobalFinding",
    "NodeFinding",
    "RejectedObservation",
    "SegmentFinding",
    "Severity",
]
