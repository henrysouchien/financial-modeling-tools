"""The stored typed outputs of a ticker-triage sidecar.

The ticker-triage door (``api/fms/modules/ticker_triage.py``) constructs this
model from the admitted triage judgment and persists its ``model_dump``; the
agent never submits it. A construction that does not validate is the door's
defect, raised with the field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue


TriageLevel = Literal["HIGH", "MEDIUM", "LOW"]
TriageVerdict = Literal["recommendation", "watchlist", "dismiss"]


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TickerTriageAnalysisSection(_StoredContract):
    heading: str = Field(min_length=1)
    body: JsonValue = None


class TickerTriageOutputs(_StoredContract):
    ticker: str = Field(min_length=1)
    skill: str = Field(min_length=1)
    verdict: str = Field(min_length=1)
    confidence: TriageLevel
    one_line_summary: str = Field(min_length=1)
    #: Absent only for an insufficient-data verdict.
    conviction: int | None = Field(default=None, ge=1, le=5)
    #: The ticker-memory verdict; absent only for an insufficient-data verdict.
    triage_verdict: TriageVerdict | None = None
    bound_template_id: str = Field(min_length=1)
    priority_rating: TriageLevel | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    #: The ticker-memory attributes the run projects, with the run clock bound.
    memory_projection: dict[str, str]
    analysis_sections: tuple[TickerTriageAnalysisSection, ...] = ()
    #: Server-resolved intake evidence for the research file, when the run has one.
    evidence: dict[str, JsonValue] | None = None


__all__ = ["TickerTriageAnalysisSection", "TickerTriageOutputs", "TriageLevel", "TriageVerdict"]
