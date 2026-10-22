"""The stored typed outputs of a business-quality-assessment sidecar.

The business-quality door (``api/fms/modules/business_quality``) constructs this
model from the admitted judgment and persists its ``model_dump``; the agent never
submits it. A construction that does not validate is the door's defect, raised
with the field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue


BusinessQualityVerdict = Literal["HIGH_QUALITY", "MIXED", "LOW_QUALITY", "INSUFFICIENT_DATA"]


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BusinessQualityOutputs(_StoredContract):
    ticker: str = Field(min_length=1)
    verdict: BusinessQualityVerdict
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    #: Pillar and composite scores on the 1-5 scale, keyed by pillar.
    pillar_scores: dict[str, float]
    evidence_by_pillar: dict[str, JsonValue]
    source_refs: tuple[str, ...] = ()
    sources: tuple[dict[str, JsonValue], ...] = ()
    data_gaps: tuple[JsonValue, ...] = ()
    recommended_next_action: str | None = None
    #: The applied Thesis projection outcome, once the door has written it.
    thesis_projection: dict[str, JsonValue] | None = None


__all__ = ["BusinessQualityOutputs", "BusinessQualityVerdict"]
