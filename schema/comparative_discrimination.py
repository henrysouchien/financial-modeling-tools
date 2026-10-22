"""Drift-vs-restatement discrimination (Stage C, step C-1).

Two filings can speak about one period **P**: filing **A** (the fiscal-year-P
10-K) presents P as its current column; filing **B** (the fiscal-year-P+1 10-K)
presents P as its comparative column. This module reads that comparative side —
already on the ``/api/financials`` wire with zero producer change — and routes
what it finds:

* **presentation drift** goes to this plane's proposal port (a link, admitted
  by a human);
* **value revision** goes to the restatement producer lane (a vintage lineage,
  produced there);
* **ambiguity goes nowhere by invention** — it is a typed conflict with all
  candidates carried.

**No arm of this module writes a link, a vintage, or a lineage.** The output
type is closed; candidates become facts only through their owners.

Axis ownership, stated because it is the whole point of the seam
(mechanism §6.2, producer contract §3.3): this module owns the **slot /
presentation-identity axis** and the classification semantics. It **delegates
the value axis** to the restatement producer contract's declared-precision
predicate — values are compared in absolute units after applying each fact's
declared scale, and two values are equal iff they agree at the coarser of the
two facts' declared precisions. There is **no materiality epsilon**: a pair
equal under a tolerance profile but unequal at declared precision is unequal,
and is emitted. Tolerance profiles stamp advisory severity on facts already
emitted, never equality.

Because that predicate's single owning implementation lives producer-side, the
implementation here is a **conforming copy**, pinned by the shared conformance
vector ``tests/fixtures/correspondence/declared_precision_predicate.v1.json``
(checked into both repos byte-identical; a vector change is a contract change
and cuts the fixture version in both).

Until the restatement producer lands, restatement candidates are recorded
discriminator terminals with no consumer — the Part C replay gate stays open.
Stated, not hidden.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .correspondence import (
    IdentityDrifted,
    IdentityPart,
    PresentationLineRef,
    presentation_identity_unchanged,
)
from .statement_tolerance_profiles import (
    AdvisorySeverity,
    StatementToleranceProfile,
    advisory_severity,
)
from .statement_topology import normalize_topology_role


class _ComparativeContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _require_non_blank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


# --------------------------------------------------------------------------
# the declared-precision predicate (conforming copy; producer contract §3.3)
# --------------------------------------------------------------------------


#: Declared scale token -> decimal exponent. Mirrors
#: ``schema/source_values.py``'s ``_NAMED_SCALE_EXPONENT`` so a declared scale
#: means the same thing on both sides of the seam.
_SCALE_EXPONENT: Mapping[str, int] = {
    "hundredths": -2,
    "units": 0,
    "": 0,
    "thousands": 3,
    "millions": 6,
    "billions": 9,
    "trillions": 12,
}


class DeclaredPrecisionError(ValueError):
    """The pair cannot be compared at declared precision — fail closed.

    Raised, never swallowed: a comparison whose declared scale or unit the pin
    does not carry has no honest answer, and answering "equal" would suppress a
    restatement. The repair target is the pin/producer payload.
    """

    def __init__(self, code: str, message: str, *, repair_target: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.repair_target = repair_target


class DeclaredValue(_ComparativeContract):
    """A reported number plus the scale and unit it was declared under."""

    value: Decimal
    scale: str
    unit: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, value: Any) -> Decimal:
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"value {value!r} is not a decimal number") from exc

    @field_validator("scale")
    @classmethod
    def _normalize_scale(cls, value: str) -> str:
        return str(value or "").strip().lower()

    @field_validator("unit")
    @classmethod
    def _normalize_unit(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        return normalized or None

    def _scale_exponent(self) -> int:
        exponent = _SCALE_EXPONENT.get(self.scale)
        if exponent is None:
            raise DeclaredPrecisionError(
                "declared_precision_unavailable",
                f"declared scale {self.scale!r} is not a known XBRL scale",
                repair_target=(
                    "prepare lane: rebuild the pin from payloads declaring a known "
                    "scale for this fact"
                ),
            )
        return exponent

    def absolute(self) -> Decimal:
        """The value in absolute units, after applying its declared scale."""

        return self.value.scaleb(self._scale_exponent())

    def precision_exponent(self) -> int:
        """The decimal exponent of this fact's declared precision, in absolute units.

        A fact declared ``2.77`` under scale ``millions`` is precise to 0.01M —
        10^4 absolute units — not to 10^6: the declared scale is a multiplier,
        the reported digits are the precision. Fractional digits therefore
        *refine* the scale.

        Integers are read at their literal precision (exponent 0 relative to the
        scale) rather than by inferring significance from trailing zeros, which
        XBRL leaves ambiguous. That reading is the fail-closed one: a finer
        precision emits more deltas, and the contract's rule is that everything
        past declared precision is a delta and is emitted — never suppressed.
        """

        return self._scale_exponent() + min(0, int(self.value.as_tuple().exponent))


def values_equal_at_declared_precision(left: DeclaredValue, right: DeclaredValue) -> bool:
    """The producer contract §3.3 predicate, conforming copy.

    Equal iff the two values agree at the **coarser** of the two declared
    precisions, after normalizing both to absolute units. Different declared
    units are never equal. Nothing here is a tolerance: the only "equality with
    slack" permitted is declared-precision normalization, which is XBRL
    semantics, not judgment.
    """

    if left.unit is not None and right.unit is not None and left.unit != right.unit:
        return False
    if (left.unit is None) != (right.unit is None):
        raise DeclaredPrecisionError(
            "declared_precision_unavailable",
            "one side declares a unit and the other does not",
            repair_target=(
                "prepare lane: rebuild the pin from payloads declaring xbrl_unit "
                "on both facts"
            ),
        )
    coarser = max(left.precision_exponent(), right.precision_exponent())
    quantum = Decimal(1).scaleb(coarser)
    return (left.absolute() / quantum).quantize(Decimal(1)) == (
        right.absolute() / quantum
    ).quantize(Decimal(1))


# --------------------------------------------------------------------------
# §6.1 the comparative reader
# --------------------------------------------------------------------------


class ComparativeCell(_ComparativeContract):
    """One (role, tag) line of filing B, carrying both of its period columns.

    ``StatementTopology`` keys one ``SourceValue`` per (year, tag) and cannot
    hold two vintages of one period, so this is a **separate pure fold** over
    the same frozen payloads — no topology mutation, no producer change.
    """

    role: str
    tag: str
    current_period_value: DeclaredValue | None = None
    prior_period_value: DeclaredValue | None = None
    current_accession: str | None = None
    prior_accession: str | None = None
    period_type: str | None = None

    @field_validator("role")
    @classmethod
    def _normalize_role(cls, value: str) -> str:
        normalized = normalize_topology_role(value)
        if not normalized:
            raise ValueError("role must normalize to a non-empty topology role")
        return normalized

    @field_validator("tag")
    @classmethod
    def _tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="tag")

    @property
    def has_independent_prior_vintage(self) -> bool:
        """The vintage-independence guard (mechanism §6.1 consumption rule 1).

        The producer defaults the prior filing ref to the current one when no
        prior filing exists; therefore ``prior.accession == current.accession``
        means **no independent prior vintage** — never a restatement observation.
        """

        return (
            self.current_accession is not None
            and self.prior_accession is not None
            and self.prior_accession != self.current_accession
        )


def _declared_value(fact: Mapping[str, Any], key: str) -> DeclaredValue | None:
    raw = fact.get(key)
    if raw is None:
        return None
    return DeclaredValue(
        value=raw,
        scale=fact.get("scale") or "",
        unit=fact.get("xbrl_unit"),
    )


def comparative_lines(
    payload: Mapping[str, Any] | None,
) -> Mapping[tuple[str, str], ComparativeCell]:
    """Fold filing B's frozen payload into ``{(role, tag) -> ComparativeCell}``.

    Same scope rule as the topology fold: only consolidated, non-dimensional
    face rows enter (``axis_key in (None, "", "__NONE__")``). Dimensional and
    member-scoped series stay in the issuer-series identity channel, not here.

    Reads the **canonical** wire fields ``current_period_value`` /
    ``prior_period_value``. The legacy ``current_value``-first probe is the
    topology fold's read fallback for older captured payloads and is
    deliberately not carried into new Stage C code.

    Pure over its declared input; no clock, no session, no I/O.
    """

    cells: dict[tuple[str, str], ComparativeCell] = {}
    facts = payload.get("facts") if isinstance(payload, Mapping) else None
    if not isinstance(facts, (list, tuple)):
        return cells
    for fact in facts:
        if not isinstance(fact, Mapping):
            continue
        if fact.get("axis_key") not in (None, "", "__NONE__"):
            continue
        tag = str(fact.get("tag") or "").strip()
        if not tag:
            continue
        source_ref = fact.get("source_ref")
        current_ref = (
            source_ref.get("current") if isinstance(source_ref, Mapping) else None
        )
        prior_ref = source_ref.get("prior") if isinstance(source_ref, Mapping) else None
        period_type = fact.get("period_type")
        hierarchy = fact.get("presentation_hierarchy")
        if isinstance(hierarchy, Mapping):
            entries = [hierarchy]
        elif isinstance(hierarchy, (list, tuple)):
            entries = [entry for entry in hierarchy if isinstance(entry, Mapping)]
        else:
            entries = []
        for entry in entries:
            role = normalize_topology_role(entry.get("role"))
            if not role:
                continue
            key = (role, tag)
            candidate = ComparativeCell(
                role=role,
                tag=tag,
                current_period_value=_declared_value(fact, "current_period_value"),
                prior_period_value=_declared_value(fact, "prior_period_value"),
                current_accession=(
                    str(current_ref.get("accession"))
                    if isinstance(current_ref, Mapping) and current_ref.get("accession")
                    else None
                ),
                prior_accession=(
                    str(prior_ref.get("accession"))
                    if isinstance(prior_ref, Mapping) and prior_ref.get("accession")
                    else None
                ),
                period_type=(
                    str(period_type).strip().lower() if period_type is not None else None
                ),
            )
            existing = cells.get(key)
            cells[key] = (
                candidate
                if existing is None
                else _preferred_cell(existing, candidate)
            )
    return cells


def _declared_precision_or_coarsest(value: DeclaredValue | None) -> int:
    """Precision exponent for arbitration; an unusable scale sorts as coarsest."""

    if value is None:
        return _COARSEST_PRECISION
    try:
        return value.precision_exponent()
    except DeclaredPrecisionError:
        return _COARSEST_PRECISION


_COARSEST_PRECISION = 10**6


def _preferred_cell(existing: ComparativeCell, candidate: ComparativeCell) -> ComparativeCell:
    """Arbitrate two observations of one (role, tag), finer declared precision winning.

    Mirrors the topology fold's duplicate arbitration
    (``choose_preferred_source_value``'s scale precedence,
    ``schema/statement_topology.py``): the fold must not depend on the order
    facts happen to appear in the payload, and the coarser observation must not
    displace the finer one just by arriving first.
    """

    if existing == candidate:
        return existing
    if _declared_precision_or_coarsest(
        candidate.current_period_value
    ) < _declared_precision_or_coarsest(existing.current_period_value):
        return candidate
    return existing


# --------------------------------------------------------------------------
# §6.2 the closed output type
# --------------------------------------------------------------------------


class ValueAgreement(_ComparativeContract):
    """How the two vintages of one period's value compare, and under what rule."""

    equal_at_declared_precision: bool
    as_filed: DeclaredValue
    comparative: DeclaredValue


class Continuation(_ComparativeContract):
    """Row 1 — identical slot, no value event; identity already covers it.

    ``value_agreement`` is ``None`` when the value axis had nothing to say — no
    independent prior vintage, or no comparative value on the wire. §6.2's table
    has no arm for "identical slot, silent value axis", and the honest reading
    is the same one ``DriftCandidate`` takes: the slot axis says "unchanged" and
    the value axis says nothing, so no agreement is fabricated. A caller that
    needs "equal" distinguished from "unobservable" reads this field rather than
    inferring equality from the arm.
    """

    kind: Literal["continuation"] = "continuation"
    period: int
    value_agreement: ValueAgreement | None = None


class DriftCandidate(_ComparativeContract):
    """Row 2 — same fact under a moved presentation line. To the proposal port.

    ``value_agreement`` is ``None`` when the value axis had nothing to say — no
    independent prior vintage, or no comparative value on the wire. That is
    recorded honestly rather than fabricated as agreement: the drift is on the
    slot axis and stands on its own.
    """

    kind: Literal["drift_candidate"] = "drift_candidate"
    period: int
    from_line: PresentationLineRef
    to_line: PresentationLineRef
    drift_parts: frozenset[IdentityPart] = Field(min_length=1)
    value_agreement: ValueAgreement | None = None


class RestatementCandidate(_ComparativeContract):
    """Row 3 — same slot, revised value. To the restatement producer lane.

    ``advisory_severity`` is a tolerance-profile stamp on a fact **already
    emitted**; it is never an equality gate and never suppresses this candidate.
    """

    kind: Literal["restatement_candidate"] = "restatement_candidate"
    period: int
    restates_accession: str
    restated_by_accession: str
    as_filed_value: DeclaredValue
    restated_value: DeclaredValue
    advisory_severity: AdvisorySeverity | None = None


class DriftAndRestatement(_ComparativeContract):
    """Row 4 — decompose. One candidate per plane, each routed to its own owner.

    Neither half is inferred from the other. The vintage half lands producer-side
    as a typed conflict until the correspondence link is admitted, after which
    the pair is re-discriminated and lineage supersedes the conflict.
    """

    kind: Literal["drift_and_restatement"] = "drift_and_restatement"
    period: int
    drift: DriftCandidate
    restatement: RestatementCandidate


class PresentationAbsence(_ComparativeContract):
    """Row 5 (non-conflict half) — B presents no slot for this row.

    A window/segment question: a declared ``not_presented`` disposition or a
    break candidate. Emitted as an explicit terminal rather than as silence, so
    the caller cannot mistake "no observation" for "nothing happened".
    """

    kind: Literal["presentation_absence"] = "presentation_absence"
    period: int
    row_tag: str
    statement_role: str


class ComparativeConflict(_ComparativeContract):
    """Rows 5-6 — typed conflict. The discriminator never invents."""

    kind: Literal["comparative_conflict"] = "comparative_conflict"
    conflict: Literal["comparative_match_ambiguous", "comparative_line_unmatched"]
    period: int
    detail: str
    candidates: tuple[str, ...] = ()
    as_filed_accession: str | None = None
    comparative_accession: str | None = None

    @field_validator("detail")
    @classmethod
    def _detail_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="detail")

    @property
    def repair_target(self) -> str:
        return "proposal queue: a human takes it up; the discriminator never invents"


ComparativeObservation = Annotated[
    Continuation
    | DriftCandidate
    | RestatementCandidate
    | DriftAndRestatement
    | PresentationAbsence
    | ComparativeConflict,
    Field(discriminator="kind"),
]


# --------------------------------------------------------------------------
# §6.2 the decision table
# --------------------------------------------------------------------------


class AsFiledLine(_ComparativeContract):
    """Filing A's line for period P, in A's own current column."""

    line: PresentationLineRef
    value: DeclaredValue


class ComparativeCandidate(_ComparativeContract):
    """One candidate slot in filing B for period P, with B's comparative column."""

    line: PresentationLineRef
    cell: ComparativeCell

    @model_validator(mode="after")
    def _validate_candidate(self) -> "ComparativeCandidate":
        if self.cell.tag != self.line.tag:
            raise ValueError("candidate cell and line must describe the same tag")
        return self


class UnmatchedComparativeValue(_ComparativeContract):
    """A comparative value in B that no candidate slot claims."""

    tag: str
    cell: ComparativeCell

    @field_validator("tag")
    @classmethod
    def _tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="tag")


def discriminate_comparative_observation(
    *,
    period: int,
    as_filed: AsFiledLine,
    candidates: tuple[ComparativeCandidate, ...],
    unmatched_comparative_values: tuple[UnmatchedComparativeValue, ...] = (),
    reported_total: float | None = None,
    tolerance_profile: StatementToleranceProfile | None = None,
) -> (
    Continuation
    | DriftCandidate
    | RestatementCandidate
    | DriftAndRestatement
    | PresentationAbsence
    | ComparativeConflict
):
    """Classify one (period, section row candidate) pair. Pure, total, closed.

    Candidate *matching* is the caller's (the proposal port's candidate-feature
    machinery at step C-3): this function classifies what it is given, so no
    matching heuristic can outrank parsed structure inside the classifier.

    ``reported_total`` and ``tolerance_profile`` are optional and feed the
    **advisory** severity stamp only (``reported_total`` in the model's
    normalized unit, millions). Omitting them changes no classification.

    The six rows of mechanism §6.2, in order:

    ==== ================== =================== =====================================
    row  slot (B vs A)      value               classification
    ==== ================== =================== =====================================
    1    identical          equal               ``Continuation``
    2    drifted            equal               ``DriftCandidate``
    3    identical          unequal             ``RestatementCandidate``
    4    drifted            unequal             ``DriftAndRestatement``
    5    absent in B        --                  ``PresentationAbsence`` / conflict
    6    many slots match   any                 ``ComparativeConflict`` (ambiguous)
    ==== ================== =================== =====================================
    """

    if len(candidates) > 1:
        return ComparativeConflict(
            conflict="comparative_match_ambiguous",
            period=int(period),
            detail=(
                f"{len(candidates)} candidate slots match {as_filed.line.tag!r} for "
                f"period {int(period)}"
            ),
            candidates=tuple(candidate.line.tag for candidate in candidates),
            as_filed_accession=as_filed.line.accession,
            comparative_accession=candidates[0].line.accession,
        )

    if not candidates:
        if unmatched_comparative_values:
            orphan = unmatched_comparative_values[0]
            return ComparativeConflict(
                conflict="comparative_line_unmatched",
                period=int(period),
                detail=(
                    f"no slot in the comparative filing matches "
                    f"{as_filed.line.tag!r}, but comparative value(s) reappear "
                    f"under {orphan.tag!r}"
                ),
                candidates=tuple(
                    value.tag for value in unmatched_comparative_values
                ),
                as_filed_accession=as_filed.line.accession,
                comparative_accession=orphan.cell.current_accession,
            )
        return PresentationAbsence(
            period=int(period),
            row_tag=as_filed.line.tag,
            statement_role=as_filed.line.statement_role,
        )

    candidate = candidates[0]
    comparison = presentation_identity_unchanged(as_filed.line, candidate.line)
    drifted = isinstance(comparison, IdentityDrifted)

    comparative_value = candidate.cell.prior_period_value
    independent_vintage = candidate.cell.has_independent_prior_vintage

    if comparative_value is None or not independent_vintage:
        # No independent prior vintage (or no comparative value at all) means the
        # value axis has nothing to say — never a restatement observation, and
        # never a fabricated claim of agreement. The slot axis still speaks.
        if drifted:
            return DriftCandidate(
                period=int(period),
                from_line=as_filed.line,
                to_line=candidate.line,
                drift_parts=comparison.parts,
                value_agreement=None,
            )
        # Identical slot with a silent value axis: "no event", stated with the
        # value axis's silence carried rather than implied to be agreement.
        return Continuation(period=int(period), value_agreement=None)

    equal = values_equal_at_declared_precision(as_filed.value, comparative_value)
    agreement = ValueAgreement(
        equal_at_declared_precision=equal,
        as_filed=as_filed.value,
        comparative=comparative_value,
    )

    if equal:
        if not drifted:
            return Continuation(period=int(period), value_agreement=agreement)
        return DriftCandidate(
            period=int(period),
            from_line=as_filed.line,
            to_line=candidate.line,
            drift_parts=comparison.parts,
            value_agreement=agreement,
        )

    severity: AdvisorySeverity | None = None
    if tolerance_profile is not None and reported_total is not None:
        delta = float(
            comparative_value.absolute() - as_filed.value.absolute()
        ) / 1_000_000.0
        severity = advisory_severity(delta, reported_total, tolerance_profile)

    restatement = RestatementCandidate(
        period=int(period),
        restates_accession=as_filed.line.accession,
        restated_by_accession=candidate.line.accession,
        as_filed_value=as_filed.value,
        restated_value=comparative_value,
        advisory_severity=severity,
    )
    if not drifted:
        return restatement
    return DriftAndRestatement(
        period=int(period),
        drift=DriftCandidate(
            period=int(period),
            from_line=as_filed.line,
            to_line=candidate.line,
            drift_parts=comparison.parts,
            value_agreement=agreement,
        ),
        restatement=restatement,
    )


__all__ = [
    "AsFiledLine",
    "ComparativeCandidate",
    "ComparativeCell",
    "ComparativeConflict",
    "ComparativeObservation",
    "Continuation",
    "DeclaredPrecisionError",
    "DeclaredValue",
    "DriftAndRestatement",
    "DriftCandidate",
    "PresentationAbsence",
    "RestatementCandidate",
    "UnmatchedComparativeValue",
    "ValueAgreement",
    "comparative_lines",
    "discriminate_comparative_observation",
    "values_equal_at_declared_precision",
]
