from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from schema._insights_shared import _FROZEN_CONTRACT


ResearchBriefStatus = Literal["pending", "ready", "failed"]
ResearchSlotKey = Literal["differentiated_view", "path", "valuation"]


class ResearchInsightCandidate(BaseModel):
    """One scored candidate emitted by a single generator."""

    model_config = _FROZEN_CONTRACT

    slot_key: ResearchSlotKey
    headline: str = Field(..., min_length=1, max_length=240)
    evidence: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    relevance_score: float = Field(..., ge=0.0, le=1.0)
    conviction_score: float = Field(..., ge=0.0, le=1.0)
    why: str = Field(..., min_length=1)
    generator_id: str


class ResearchBriefSlot(BaseModel):
    """Selected slot content for the brief. Empty when no candidate is available."""

    model_config = _FROZEN_CONTRACT

    slot_key: ResearchSlotKey
    selected: ResearchInsightCandidate | None = None
    candidates_considered: int = 0


class ResearchEditorialMetadata(BaseModel):
    model_config = _FROZEN_CONTRACT

    schema_version: Literal["1.0"] = "1.0"
    generated_at: float
    handoff_id: int
    process_template_id: str | None = None
    partial_failure: bool = False
    degraded: bool = False
    fallback_template: bool = False
    error_summary: dict[str, str] = Field(default_factory=dict)


class ResearchBrief(BaseModel):
    """Top-level brief envelope returned by the research editorial endpoint."""

    model_config = _FROZEN_CONTRACT

    schema_version: Literal["1.0"] = "1.0"
    status: ResearchBriefStatus
    headline: str | None = None
    differentiated_view_slot: ResearchBriefSlot | None = None
    path_slot: ResearchBriefSlot | None = None
    valuation_slot: ResearchBriefSlot | None = None
    editorial_metadata: ResearchEditorialMetadata | None = None
    error_code: str | None = None

    @model_validator(mode="after")
    def _validate_status_payload(self) -> "ResearchBrief":
        if self.status == "ready":
            missing_fields = [
                field_name
                for field_name in (
                    "headline",
                    "differentiated_view_slot",
                    "path_slot",
                    "valuation_slot",
                    "editorial_metadata",
                )
                if getattr(self, field_name) is None
            ]
            if missing_fields:
                raise ValueError(
                    f"ready research brief missing required fields: {', '.join(missing_fields)}"
                )

        if self.status == "failed" and self.error_code is None:
            raise ValueError("failed research brief requires error_code")

        return self


__all__ = [
    "ResearchBrief",
    "ResearchBriefSlot",
    "ResearchBriefStatus",
    "ResearchEditorialMetadata",
    "ResearchInsightCandidate",
    "ResearchSlotKey",
]
