"""Stored records the thesis report doors persist as their typed payload.

``fms.modules.thesis_link`` constructs :class:`ThesisLinkBindingRecord` from the
admitted judgment and the rows it bound; ``fms.modules.thesis_consultation``
constructs :class:`ThesisConsultationRecord` from the admitted judgment, the
projection it built and what persistence stored. Each record's schema validates
what the door built; the agent never submits one.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue

from .thesis import ThesisLink
from .thesis_shared_slice import DataGap, SourceRecord, _ContractModel


class ThesisLinkFailure(_ContractModel):
    """A constructed link the door could not write, with the reason."""

    thesis_link_id: str | None = None
    thesis_point_id: str | None = None
    reason: Literal["cross_thesis_move_rejected", "upsert_failed"]
    existing_thesis_id: str | None = None
    target_thesis_id: str | None = None
    error_type: str | None = None
    message: str | None = None


class ThesisLinkUnresolvedClaim(_ContractModel):
    """A thesis claim that has no model anchor yet, and who could supply one."""

    thesis_point_id: str | None = None
    thesis_text: str | None = None
    missing_anchor: str | None = None
    next_producer: str | None = None
    next_tool: str | None = None


class ThesisLinkDataGap(_ContractModel):
    data_gap_id: str = Field(min_length=1)
    thesis_point_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    next_producer: str | None = None
    status: Literal["open"] = "open"
    source: Literal["thesis-link"] = "thesis-link"


class ThesisLinkScorecardSummary(_ContractModel):
    status: Literal["success", "failed", "not_run"]
    reason: str | None = None
    code: str | None = None
    error_type: str | None = None
    scorecard_id: str | None = None
    summary_status: str | None = None
    entry_count: int | None = None
    confirmed_count: int | None = None
    challenged_count: int | None = None
    disconfirmed_count: int | None = None


class ThesisLinkBindingRecord(_ContractModel):
    """What one thesis-link run bound, failed, and left unresolved."""

    links: list[ThesisLink] = Field(default_factory=list)
    validated_links: list[ThesisLink] = Field(default_factory=list)
    applied_link_ids: list[str] = Field(default_factory=list)
    failed_links: list[ThesisLinkFailure] = Field(default_factory=list)
    unresolved: list[ThesisLinkUnresolvedClaim] = Field(default_factory=list)
    data_gaps: list[ThesisLinkDataGap] = Field(default_factory=list)
    scorecard: ThesisLinkScorecardSummary


class ThesisConsultationProjectionOutcome(_ContractModel):
    """Whether the typed Thesis projection was written, and what persistence stored."""

    status: Literal["applied", "skipped"]
    reason: str | None = None
    thesis_id: str | None = None
    written_thesis_version: int | None = None
    thesis_projection: dict[str, JsonValue] = Field(default_factory=dict)
    decision_log_entry: dict[str, JsonValue] | None = None


class ThesisConsultationRecord(_ContractModel):
    """One thesis-consultation run: the verdict and the typed Thesis sections it wrote."""

    ticker: str = Field(min_length=1)
    caller: str = Field(min_length=1)
    verdict: Literal["THESIS_WRITTEN", "INSUFFICIENT_DATA"]
    confidence: Literal["HIGH", "MEDIUM", "LOW"] | None = None
    summary: str = Field(min_length=1)
    data_gaps: list[DataGap] = Field(default_factory=list)
    recommended_next_action: str | None = None
    #: ``ThesisProjection.model_dump(mode="json", exclude_none=True)``: the sections written.
    thesis_projection: dict[str, JsonValue] = Field(default_factory=dict)
    sources: list[SourceRecord] = Field(default_factory=list)
    projection: ThesisConsultationProjectionOutcome | None = None
