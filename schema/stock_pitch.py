"""The stored typed outputs of a stock-pitch sidecar.

The stock-pitch report door (``api/fms/modules/stock_pitch``) constructs this
model from the admitted judgment and persists its ``model_dump`` under
``typed_outputs.stock_pitch``; the agent never submits it. It is stored for every
verdict and carries every pitch section the analyst wrote; a pitch verdict carries
every canonical section. A construction that does not validate is the door's
defect, raised with the field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .deliverable_binding import DeliverableModelBinding


StockPitchVerdict = Literal[
    "PITCH_DELIVERABLE",
    "PITCH_NEEDS_COMPRESSION",
    "THESIS_INSUFFICIENT_FOR_PITCH",
    "INSUFFICIENT_DATA",
]
#: The verdicts that circulate a pitch; each carries every canonical section.
PITCH_VERDICTS = frozenset({"PITCH_DELIVERABLE", "PITCH_NEEDS_COMPRESSION"})
CANONICAL_PITCH_SECTIONS = (
    "recommendation",
    "business_overview",
    "investment_thesis",
    "financial_summary",
    "valuation",
    "scenario_analysis",
    "key_risks",
    "q_and_a_preparation",
    "diligence_checklist",
    "summary_monitoring",
    "elevator_pitch",
)
StockPitchConfidence = Literal["HIGH", "MEDIUM", "LOW"]
#: One pitch section as the analyst wrote it: prose, bullets, or a checklist mapping.
PitchSection = str | list[JsonValue] | dict[str, JsonValue]

#: The 90-second elevator pitch's word budget; a pitch over it needs compression.
ELEVATOR_PITCH_WORD_LIMIT = 275


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _absent(value: object) -> bool:
    return value is None


class StockPitchBody(_StoredContract):
    recommendation: PitchSection | None = Field(default=None, exclude_if=_absent)
    business_overview: PitchSection | None = Field(default=None, exclude_if=_absent)
    investment_thesis: PitchSection | None = Field(default=None, exclude_if=_absent)
    financial_summary: PitchSection | None = Field(default=None, exclude_if=_absent)
    valuation: PitchSection | None = Field(default=None, exclude_if=_absent)
    scenario_analysis: PitchSection | None = Field(default=None, exclude_if=_absent)
    key_risks: PitchSection | None = Field(default=None, exclude_if=_absent)
    q_and_a_preparation: PitchSection | None = Field(default=None, exclude_if=_absent)
    diligence_checklist: PitchSection | None = Field(default=None, exclude_if=_absent)
    summary_monitoring: PitchSection | None = Field(default=None, exclude_if=_absent)
    elevator_pitch: PitchSection | None = Field(default=None, exclude_if=_absent)
    upside: PitchSection | None = Field(default=None, exclude_if=_absent)
    downside: PitchSection | None = Field(default=None, exclude_if=_absent)
    catalysts: PitchSection | None = Field(default=None, exclude_if=_absent)


class StockPitchSourceArtifact(_StoredContract):
    """One upstream artifact the pitch cites: its producer skill and its path or id."""

    skill: str = Field(min_length=1)
    path: str | None = Field(default=None, exclude_if=_absent)
    artifact_id: str | None = Field(default=None, exclude_if=_absent)

    @model_validator(mode="after")
    def _names_the_artifact(self) -> "StockPitchSourceArtifact":
        if not (self.path or self.artifact_id):
            raise ValueError("a cited source artifact names its path or artifact_id")
        return self


class StockPitch(_StoredContract):
    ticker: str = Field(min_length=1)
    audience: str = Field(min_length=1)
    verdict: StockPitchVerdict
    confidence: StockPitchConfidence
    summary: str = Field(min_length=1)
    elevator_pitch_word_count: int | None = Field(default=None, ge=0)
    pitch: StockPitchBody
    source_artifacts: tuple[StockPitchSourceArtifact, ...] = ()
    data_gaps: tuple[JsonValue, ...] = ()
    #: The one ModelVersion the pitch was computed from (W5d); ``None`` when the
    #: judgment named none the door could read, recorded as a finding.
    model_binding: DeliverableModelBinding | None = None

    @model_validator(mode="after")
    def _pitch_verdict_is_a_whole_pitch(self) -> "StockPitch":
        if self.verdict not in PITCH_VERDICTS:
            return self
        missing = [name for name in CANONICAL_PITCH_SECTIONS if getattr(self.pitch, name) is None]
        if missing:
            raise ValueError(f"a {self.verdict} pitch carries every canonical section; missing {', '.join(missing)}")
        if (
            self.verdict == "PITCH_DELIVERABLE"
            and self.elevator_pitch_word_count is not None
            and self.elevator_pitch_word_count > ELEVATOR_PITCH_WORD_LIMIT
        ):
            raise ValueError(
                f"a PITCH_DELIVERABLE elevator pitch is at most {ELEVATOR_PITCH_WORD_LIMIT} words"
            )
        return self


__all__ = [
    "CANONICAL_PITCH_SECTIONS",
    "ELEVATOR_PITCH_WORD_LIMIT",
    "PITCH_VERDICTS",
    "PitchSection",
    "StockPitch",
    "StockPitchBody",
    "StockPitchConfidence",
    "StockPitchSourceArtifact",
    "StockPitchVerdict",
]
