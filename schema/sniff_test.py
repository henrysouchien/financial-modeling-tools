"""The stored typed outputs of a sniff-test sidecar.

The sniff-test door (``api/fms/modules/sniff_test``) constructs this model from
the admitted judgment and persists its ``model_dump``; the agent never submits
it. A construction that does not validate is the door's defect, raised with the
field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue


SniffTestVerdict = Literal["PROCEED", "PARK", "DISMISS", "INSUFFICIENT_DATA"]
SniffTestCaseType = Literal[
    "quality_compounder",
    "quality_transition",
    "value_recovery",
    "commodity_cycle",
    "special_situation",
    "growth_secular",
    "priced_to_perfection",
    "risk_or_avoid",
    "unclear",
]


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SniffTestWarningFlag(_StoredContract):
    flag: str = Field(min_length=1)
    severity: Literal["SEVERE", "MATERIAL", "ELEVATED"] | None = None
    evidence: str | None = None
    receding: bool | None = None
    receding_rationale: str | None = None


class SniffTestOutputs(_StoredContract):
    ticker: str = Field(min_length=1)
    verdict: SniffTestVerdict
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    case_type: SniffTestCaseType
    #: The first-pass business frame keys the analyst stated (product, revenue_model, ...).
    business_frame: dict[str, str]
    warning_flags: tuple[SniffTestWarningFlag, ...] = ()
    dealbreakers: tuple[str, ...] = ()
    dismiss_rationale: str | None = None
    trajectory_read: dict[str, JsonValue]
    setup_read: dict[str, JsonValue]
    what_is_special: str | None = None
    follow_up_question: str | None = None
    park_trigger: str | None = None
    recommended_track: str | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    sources: tuple[dict[str, JsonValue], ...] = ()
    recommended_next_action: str | None = None


__all__ = [
    "SniffTestCaseType",
    "SniffTestOutputs",
    "SniffTestVerdict",
    "SniffTestWarningFlag",
]
