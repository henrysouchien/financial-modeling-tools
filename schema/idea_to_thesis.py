"""The stored typed outputs of an idea-to-thesis sidecar.

The idea-to-thesis door (``api/fms/modules/idea_to_thesis``) constructs these
models from the admitted judgment and persists their ``model_dump``; the agent
never submits them. :class:`ThesisProjection` is the typed Thesis sections the
door writes through repository shared-slice persistence; each section is the
Thesis section model itself. A construction that does not validate is the door's
defect, raised with the field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from .model_ref import ModelVersionRef
from .thesis import QuantitativeFraming
from .thesis_shared_slice import (
    Assumption,
    BusinessOverview,
    Catalyst,
    ConsensusView,
    DataGap,
    DifferentiatedViewClaim,
    IndustryAnalysis,
    InvalidationTrigger,
    Peer,
    Risk,
    ThesisField,
    Valuation,
)


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ThesisProjection(_StoredContract):
    """The typed Thesis sections one idea-to-thesis write projects; an absent section is ``None``."""

    thesis: ThesisField | None = None
    business_overview: BusinessOverview | None = None
    consensus_view: ConsensusView | None = None
    quantitative_framing: QuantitativeFraming | None = None
    valuation: Valuation | None = None
    industry_analysis: IndustryAnalysis | None = None
    model_ref: ModelVersionRef | None = None
    differentiated_view: list[DifferentiatedViewClaim] | None = None
    catalysts: list[Catalyst] | None = None
    risks: list[Risk] | None = None
    invalidation_triggers: list[InvalidationTrigger] | None = None
    peers: list[Peer] | None = None
    assumptions: list[Assumption] | None = None
    data_gaps: list[DataGap] | None = None


#: The section keys a :class:`ThesisProjection` carries, derived from the model.
THESIS_PROJECTION_SECTIONS = frozenset(ThesisProjection.model_fields)


class ThesisProjectionPersistence(_StoredContract):
    """What repository persistence stored for the projection, as it returned it."""

    status: Literal["applied"]
    reason: str | None = None
    thesis_id: str | None = None
    written_thesis_version: int | None = None
    thesis_projection: dict[str, JsonValue]
    decision_log_entry: dict[str, JsonValue] | None = None


class IdeaToThesisOutputs(_StoredContract):
    ticker: str = Field(min_length=1)
    idea_id: str = Field(min_length=1)
    verdict: Literal["THESIS_WRITTEN", "INSUFFICIENT_DATA"]
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    summary: str = Field(min_length=1)
    data_gaps: list[JsonValue] = Field(default_factory=list)
    recommended_next_action: str | None = None
    #: ``ThesisProjection.model_dump(mode="json", exclude_none=True)``: the sections written.
    thesis_projection: dict[str, JsonValue] = Field(default_factory=dict)
    projection: ThesisProjectionPersistence | None = None
    #: What the door noticed admitting and constructing the judgment: ``{index?, field, code, message, next_call}``.
    findings: list[dict[str, JsonValue]] = Field(default_factory=list)


__all__ = [
    "IdeaToThesisOutputs",
    "THESIS_PROJECTION_SECTIONS",
    "ThesisProjection",
    "ThesisProjectionPersistence",
]
