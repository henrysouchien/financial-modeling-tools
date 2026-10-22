"""The one Revision field shape the five judgment artifacts share (R3).

Store (A). A judgment change is a successor artifact, never an edit. The
successor BusinessModelRevision, HistoricalFoundation, ForecastAssumptionSet,
ScenarioSet or ValuationPolicy names its predecessor by ``predecessor_ref``
(each artifact's own typed ref), and every judgment row it changes or drops
carries one :class:`Supersession` in the artifact's ``supersessions``: the prior
row, what triggered the change, the reason (including whether an Actual read
as noise or signal), who authored it (the agent, or the human whose workbook
edit the door carried) and the date the door stamped. The prior row survives
on the predecessor.

Absence is never a refusal here. An artifact written before these fields
decodes unchanged (both fields default empty and drop out of the dump, so old
content digests hold), and a supersession without its reason or citation still
validates: those are the ``judgment_reason_missing``,
``judgment_citation_unresolved``, ``judgment_cites_derived_view`` and
``supersession_missing`` Findings ``schema/build_findings.py`` produces.
``history(subject)`` (``schema/judgment_history.py``) walks the chain.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .model_ref import ModelVersionRef


#: What made the analyst revisit a judgment (plan §4.1): a reported Actual, new
#: Evidence, a Claim, or re-thinking — a loop re-entry, whose ``trigger_ref`` is
#: the Finding or view that prompted it.
RevisionTrigger = Literal["actual", "evidence", "claim", "re-thinking"]
REVISION_TRIGGERS: Final[tuple[str, ...]] = (
    "actual",
    "evidence",
    "claim",
    "re-thinking",
)
#: Whether the trigger moved the belief (signal) or was judged noise.
TriggerReading = Literal["noise", "signal"]
#: Who authored the revised row: the agent, or the human whose own edit (a
#: forecast-assumption cell typed into a published workbook) the door carried.
RevisionAuthor = Literal["agent", "human"]


class Supersession(BaseModel):
    """Why one predecessor judgment row was changed or dropped."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    schema_version: Literal["judgment-supersession.v1"] = "judgment-supersession.v1"
    #: The superseded row's key on the predecessor artifact: a FAS driver key,
    #: an HFF normalization decision label, a BMR plan entry's ``driver_id``.
    prior_row: str = Field(min_length=1)
    trigger: RevisionTrigger
    #: The cited trigger: the Actual's fact id, the evidence id, the Claim id,
    #: or the Finding / view id a re-thinking answers.
    trigger_ref: str | None = None
    #: Why the judgment changed; a missing reason is ``judgment_reason_missing``.
    reason: str | None = None
    #: Noise or signal; part of the reason when the trigger is an Actual.
    reading: TriggerReading | None = None
    #: Stamped by the door that wrote the successor, never authored. ``agent``
    #: drops out of the dump, so a supersession written before the field keeps
    #: its content digest.
    author: RevisionAuthor = Field(
        default="agent",
        exclude_if=lambda value: value == "agent",
    )
    #: Stamped by the door that wrote the successor, never authored.
    revised_at: datetime

    @field_validator("trigger_ref", "reason", mode="before")
    @classmethod
    def _blank_is_absent(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("revised_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("revised_at must be timezone-aware")
        return value


def canonical_supersessions(
    rows: Iterable[Supersession],
) -> tuple[Supersession, ...]:
    """The stored order: sorted by prior row."""

    return tuple(sorted(rows, key=lambda row: row.prior_row))


def validate_revision_fields(
    predecessor_ref: object | None,
    supersessions: tuple[Supersession, ...],
) -> None:
    """The shape every artifact carrying the Revision fields keeps.

    Only the fields' own coherence: a supersession names a row of the
    predecessor, so it needs one, and one row is superseded at most once.
    """

    prior_rows = tuple(row.prior_row for row in supersessions)
    if prior_rows != tuple(sorted(set(prior_rows))):
        raise ValueError("supersessions must be sorted and unique by prior_row")
    if supersessions and predecessor_ref is None:
        raise ValueError("a supersession requires the artifact's predecessor_ref")


def workbook_edit_trigger_ref(model_version: ModelVersionRef) -> str:
    """A human workbook edit's ``trigger_ref``: the published version it was typed into."""

    return f"workbook:{model_version.model_id}@{model_version.version}"


__all__ = [
    "REVISION_TRIGGERS",
    "RevisionAuthor",
    "RevisionTrigger",
    "Supersession",
    "TriggerReading",
    "canonical_supersessions",
    "validate_revision_fields",
    "workbook_edit_trigger_ref",
]
