"""Claim and Reference: what someone said about a number, and what a value is held against.

Store contracts on the SourceBundle input plane, siblings of
``HistoricalBasisFact`` (``schema/build_model_product_contracts.py``).  A
``Claim`` is retrieved, never formed: management guidance printed in an
earnings release, or a street consensus a provider snapshot reports.  It is
never a Fact — its ``as_of`` is the date the author said it, distinct from the
period it is about and from the time the bytes were fetched.  A ``Reference``
is the number a model value is compared against: the pinned price, or a
pointer at the Claim that serves as the consensus or guidance reference (a
Claim is not copied into a second numeric row).

Rows are minted by code from bytes the capture door already pins
(``research.model_source_capture``) and are immutable: a later print is a new
row with a later ``as_of``, never an edit of the earlier one.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from industry_slice_core.thesis_shared_sources import SourceId


ClaimAuthor = Literal["management", "consensus", "named_analyst"]
ReferenceKind = Literal["price", "consensus", "guidance", "hurdle", "entry"]

#: Reference kinds whose number is a Claim's; the Reference points at it.
CLAIM_BACKED_REFERENCE_KINDS: frozenset[str] = frozenset({"consensus", "guidance"})


class _CollectedRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


def collected_row_id(prefix: Literal["claim", "reference"], *parts: str) -> str:
    """A stable row id from the exact identity parts its minter names."""

    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


class ClaimPeriod(_CollectedRow):
    """The fiscal period a claimed number is about."""

    fiscal_year: int = Field(ge=1900, le=2200)
    fiscal_quarter: int | None = Field(default=None, ge=1, le=4)
    #: The period end the source states, when it states one.
    period_end: date | None = None

    def label(self) -> str:
        if self.fiscal_quarter is None:
            return f"FY{self.fiscal_year}"
        return f"Q{self.fiscal_quarter} FY{self.fiscal_year}"


class Claim(_CollectedRow):
    """One dated statement of a number by its author: a point or a low/high range."""

    claim_id: str = Field(min_length=1)
    issuer: str = Field(min_length=1)
    #: The driver or line the number is about, as the source names it.
    subject: str = Field(min_length=1)
    period: ClaimPeriod
    value: Decimal | None = None
    low: Decimal | None = None
    high: Decimal | None = None
    unit: str = Field(min_length=1)
    author: ClaimAuthor
    analyst_name: str | None = Field(default=None, min_length=1)
    #: The date the author made the statement (a release's acceptance date, a
    #: consensus snapshot's date) — not the period end, not the fetch time.
    as_of: date
    source_id: SourceId
    locator: str = Field(min_length=1)

    @model_validator(mode="after")
    def _shape(self) -> "Claim":
        point = self.value is not None
        ranged = self.low is not None or self.high is not None
        if point == ranged:
            raise ValueError("a claim states either a point value or a low/high range")
        if ranged and (self.low is None or self.high is None or self.low > self.high):
            raise ValueError("a claimed range states low <= high")
        if (self.author == "named_analyst") != (self.analyst_name is not None):
            raise ValueError("analyst_name is required for, and only for, a named analyst")
        return self


class Reference(_CollectedRow):
    """One number a model value is held against, with its source and date."""

    reference_id: str = Field(min_length=1)
    kind: ReferenceKind
    issuer: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    period: ClaimPeriod | None = None
    #: The number itself for price, hurdle and entry; absent for a
    #: Claim-backed kind, whose number is the Claim's.
    value: Decimal | None = None
    unit: str | None = Field(default=None, min_length=1)
    as_of: date
    source_id: SourceId
    locator: str = Field(min_length=1)
    claim_id: str | None = Field(default=None, min_length=1)
    #: The pinned ``ValuationObservation`` a price Reference projects.
    observation_id: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _shape(self) -> "Reference":
        if self.kind in CLAIM_BACKED_REFERENCE_KINDS:
            if self.claim_id is None or self.value is not None or self.unit is not None:
                raise ValueError(
                    f"a {self.kind} reference points at a claim and carries no number"
                )
        elif self.claim_id is not None or self.value is None or self.unit is None:
            raise ValueError(f"a {self.kind} reference carries its own value and unit")
        if (self.kind == "price") != (self.observation_id is not None):
            raise ValueError("observation_id is required for, and only for, a price reference")
        return self


__all__ = [
    "CLAIM_BACKED_REFERENCE_KINDS",
    "Claim",
    "ClaimAuthor",
    "ClaimPeriod",
    "Reference",
    "ReferenceKind",
    "collected_row_id",
]
