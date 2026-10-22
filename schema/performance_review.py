"""The stored typed outputs of a performance-review sidecar.

The performance-review door (``api/fms/modules/performance_review.py``)
constructs this model from the admitted review judgment and persists its
``model_dump``; the agent never submits it. A construction that does not
validate is the door's defect, raised with the field named, never a demand on
the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue


PerformanceReviewVerdict = Literal["PERFORMANCE_REVIEW_COMPLETE", "OPEN_POSITIONS_ONLY", "INSUFFICIENT_DATA"]
PerformanceReviewConfidence = Literal["HIGH", "MEDIUM", "LOW"]


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PerformanceReviewSummary(_StoredContract):
    scope: Literal["portfolio"] = "portfolio"
    scope_label: str = Field(min_length=1)
    period: str | None = None
    verdict: PerformanceReviewVerdict
    #: Absent only when the stated confidence read as none of the three levels.
    confidence: PerformanceReviewConfidence | None = None
    summary: str = Field(min_length=1)
    closed_trade_count: int | float | None = None
    open_position_count: int | float | None = None


class PerformanceReviewOutputs(_StoredContract):
    review: PerformanceReviewSummary
    closed_trade_scorecard: list[dict[str, JsonValue]] = Field(default_factory=list)
    pattern_summary: JsonValue = None
    thesis_accuracy: JsonValue = None
    open_positions: list[dict[str, JsonValue]] = Field(default_factory=list)
    benchmark_comparison: dict[str, JsonValue] = Field(default_factory=dict)
    action_items: list[JsonValue] = Field(default_factory=list)
    sources: list[dict[str, JsonValue]] = Field(default_factory=list)
    data_gaps: list[JsonValue] = Field(default_factory=list)
    recommended_next_action: str | None = None


__all__ = [
    "PerformanceReviewConfidence",
    "PerformanceReviewOutputs",
    "PerformanceReviewSummary",
    "PerformanceReviewVerdict",
]
