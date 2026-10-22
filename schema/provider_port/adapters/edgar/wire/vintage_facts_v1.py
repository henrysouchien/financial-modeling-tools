"""Strict consumer boundary for the EDGAR restatement-vintage facts wire v1.

Stage B step B-6 (plan §5 B-6 and §12 item 3: `docs/design/completed/build-model-stage-b-
restatement-vintages-consumer-implementation-plan-2026-08-19.md`); the wire it
decodes is fixed by `docs/design/completed/edgar-restatement-producer-contract-design-
2026-08-18.md` §1.1 (lineage facts), §1.2 (conflict facts), §4.1 (the envelope)
and §4.2 (versioning and the tolerance split).

**The producer does not exist yet, and this module is deliberately older than
it.** A consumer boundary that predates its producer is how the contract stays
fixed from this side: the envelope shape, the field meanings and the refusals
are decided here, and the producer is written into them (§12 item 3). Every
fixture this module is tested against is synthetic and marked as such; the
real-envelope cross-repo test belongs to the producer plan (producer contract
§5.3) and is explicitly *not* claimed by Stage B.

Three properties are this module's whole contract:

* **The pin is the authority, the HTTP rail is transport** (producer contract
  §4.1). This module decodes *pinned corpus bytes* — the manifest's
  ``restatement_lineage_json`` artifact (`api/research/corpus/verify.py`'s
  ``RESTATEMENT_LINEAGE_ARTIFACT_KIND`` at B-4) — and has no transport of its
  own. It performs no I/O.
* **The tolerance split runs envelope-tolerant, facts-strict** (producer
  contract §4.2, which records the split as *its own* choice rather than the
  metric-series module's — `schema/edgar_metric_series_v1.py` runs both
  tolerant and overrides only its nested source-identity DTO). An envelope may
  gain fields within v1; a fact row may not, because a fact row silently
  carrying an unread field is a restatement the consumer did not see.
* **Absence and emptiness are different facts** (R3). No pinned artifact means
  "the producer never ran for this corpus" — the Stage B interim state. A
  pinned artifact whose ``restatements`` and ``conflicts`` are both empty is
  the positive fact "no restatements as of ``graph_cutoff``", and it is what
  makes as-filed binding *evidenced* rather than assumed. ``is_empty``
  distinguishes them for the caller; this module never conflates them.

The wire vocabulary is **filing-native** — tags, dimensions, accessions,
periods — and carries no house ``concept_id``. That is the property that makes
it survive Stage D unchanged (producer contract §1.1): pre-D the consumer maps
tags onto its living ids through the Edgar adapter tables *at the landing
site*, post-D the wire is already the destination vocabulary and the adapter
hop disappears. **No house-plane knowledge belongs in this module**, which is
why the tag→concept resolution lives in the landing
(`api/research/build_model_source_bundle.py`) and not here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Final, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


#: Pinned wire literals (producer contract §4.1/§4.2). A change to any field
#: meaning, to the total-output rule's quantifier, or to the normalization rule
#: is a ``v2`` literal plus a consumer-boundary update — never a silent
#: re-reading of v1. ``producer_version``/``policy_version`` ride inside the
#: envelope so a producer or policy change is visible without a contract cut.
EDGAR_VINTAGE_FACTS_CONTRACT_VERSION: Final[str] = "edgar-vintage-facts.v1"
EDGAR_VINTAGE_FACTS_PRODUCER_ID: Final[str] = "edgar_updater"

#: The producer's declared scale vocabulary: each label is the power-of-ten
#: multiplier that recovers native units from the stated value. Identical to
#: the ``/api/financials`` scale contract already consumed at
#: `schema/build_model_product_contracts.py`'s ``_EDGAR_API_SCALE_EXPONENTS``
#: (source of truth ``edgar_parser/financials_projection.py``) — the same
#: producer, so the same vocabulary, restated here because this wire is its
#: own frozen contract and may not drift with another endpoint's constant.
#: An unrecognised label fails closed; it never defaults to one.
WIRE_SCALE_EXPONENTS: Final[Mapping[str, int]] = {
    "hundredths": -2,
    "units": 0,
    "thousands": 3,
    "millions": 6,
    "billions": 9,
}

#: Detection mechanisms (producer contract §2 arm A, §3 arm B). A fact states
#: a non-empty subset; the bases union when both mechanisms fire on one
#: ``(period, A, B)``.
EdgarVintageDetectionBasis = Literal["amendment_graph", "comparative_year"]
#: Whether the producer compared the two accessions' values (§2.3). An
#: ``uncompared`` fact is an amendment the producer has not parsed; it is
#: emitted at capped confidence rather than withheld.
EdgarVintageComparisonStatus = Literal["diffed", "uncompared"]
#: Lineage requires affirmative evidence; there is no "low" (§1.1 corollary).
EdgarVintageConfidence = Literal["high", "medium"]
#: Why two accessions disagree with no lineage established (§1.2). All three
#: land identically on this side — retained candidates and a typed refusal —
#: until Stage C's discrimination exists; the reason is carried, never
#: dispatched on.
EdgarVintageConflictReason = Literal[
    "lineage_undetermined",
    "correspondence_link_required",
    "identity_ambiguous",
]

_CANONICAL_DECIMAL_RE: Final = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_ACCESSION_RE: Final = re.compile(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
_CIK_RE: Final = re.compile(r"^[0-9]{1,10}$")
_TICKER_RE: Final = re.compile(r"^[A-Z][A-Z0-9.-]*$")


class EdgarVintageFactsContractError(ValueError):
    """The pinned envelope violates the frozen ``edgar-vintage-facts.v1`` wire.

    One stable code and one repair target. A malformed envelope is never
    repaired by reading it loosely: the producer re-emits, the corpus lane
    re-pins, and the release is regenerated. Fail-closed with the repair named
    is the whole point — a pinned envelope the consumer cannot read is exactly
    the case where continuing would bind as-filed values while a restatement
    sits unread in the pin.
    """

    code: Final[str] = "restatement_lineage_envelope_invalid"
    repair_target: Final[str] = (
        "producer contract lane (re-emit the envelope against "
        f"{EDGAR_VINTAGE_FACTS_CONTRACT_VERSION}, then re-pin the corpus)"
    )

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}: {reason} [repair: {self.repair_target}]")


class _WireEnvelopeModel(BaseModel):
    """Envelope layer — **tolerant** of unknown fields (producer contract §4.2).

    Additive optional fields may land within v1, and an envelope carrying one
    this consumer does not read is still a readable envelope.
    """

    model_config = ConfigDict(extra="allow", frozen=True, strict=True)


class _WireFactModel(BaseModel):
    """Fact layer — **strict** (``extra="forbid"``, producer contract §4.2).

    The asymmetry is deliberate and is this contract's own choice: an
    unrecognised field on a *fact* row is a restatement detail the consumer
    silently did not read, which is the invisible-miss class the whole
    producer contract exists to make impossible (§1.3).
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _canonical_decimal(value: str, *, field_name: str) -> Decimal:
    """Parse one canonical decimal string, refusing every lossy spelling.

    Values cross this wire as strings, never JSON numbers: a JSON number is a
    float on the way in, and a restated financial value that round-trips
    through binary floating point is no longer the filing's value. Exponents,
    leading ``+``, leading zeros and whitespace are refused so one value has
    exactly one spelling on the wire.
    """

    if _CANONICAL_DECIMAL_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a canonical decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - regex admits none
        raise ValueError(f"{field_name} must be a canonical decimal string") from exc
    if not parsed.is_finite():  # pragma: no cover - regex admits none
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _canonical_iso_date(value: str, *, field_name: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a canonical ISO date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must be a canonical ISO date")
    return parsed


def _canonical_utc_datetime(value: str, *, field_name: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"{field_name} must be a canonical UTC datetime")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a canonical UTC datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be a canonical UTC datetime")
    return parsed


class EdgarVintagePeriodV1(_WireFactModel):
    """The period a vintage fact is about — PeriodRef-compatible (§1.1).

    ``fiscal_year`` only in v1: the consuming build lane is annual. Quarterly
    is an additive extension the producer plan owns (producer contract §7 Q2),
    which is why ``kind`` is a Literal rather than an assumed shape (plan §13
    item 5).
    """

    kind: Literal["fiscal_year"]
    fiscal_year: int = Field(ge=1900, le=2200)
    period_end: str = Field(min_length=10, max_length=10)

    @model_validator(mode="after")
    def _validate_period(self) -> "EdgarVintagePeriodV1":
        _canonical_iso_date(self.period_end, field_name="period.period_end")
        return self

    @property
    def label(self) -> str:
        """The living fiscal-period label — the consumer's ``period_id`` plane."""

        return f"FY{self.fiscal_year}"

    @property
    def period_end_date(self) -> date:
        return _canonical_iso_date(self.period_end, field_name="period.period_end")


class EdgarVintageFactLocatorV1(_WireFactModel):
    """Structured source anchor retained exactly as the producer observed it."""

    accession: str = Field(min_length=1, max_length=32)
    context_ref: str | None = Field(default=None, max_length=256)
    char_anchor: str | None = Field(default=None, max_length=512)
    filing_url: str | None = Field(default=None, max_length=2048)
    presented_text: str | None = Field(default=None, max_length=512)

    @model_validator(mode="after")
    def _validate_locator(self) -> "EdgarVintageFactLocatorV1":
        _require_accession(self.accession, field_name="locator accession")
        if not any((self.context_ref, self.char_anchor, self.filing_url)):
            raise ValueError("a fact locator must carry a source anchor")
        return self


class EdgarVintageValueDeltaV1(_WireFactModel):
    """One measured identity whose value differs between the two accessions.

    The identity is filing-native (``tag`` + ``dimensions`` + role) and the
    values are stated at a declared scale, so a consumer recovers native units
    without a second policy. ``original_*`` is accession A's carriage,
    ``restated_*`` is accession B's.
    """

    tag: str = Field(min_length=1, max_length=256)
    axis_key: str | None = Field(default=None, max_length=256)
    dimensions: dict[str, str] = Field(default_factory=dict)
    presentation_role: str | None = Field(default=None, max_length=256)
    unit: str = Field(min_length=1, max_length=64)
    scale: str = Field(min_length=1, max_length=32)
    original_value: str = Field(min_length=1, max_length=64)
    original_locator: EdgarVintageFactLocatorV1
    restated_value: str = Field(min_length=1, max_length=64)
    restated_locator: EdgarVintageFactLocatorV1
    #: The filing-facts content addresses of the printed cells each side was
    #: read from (``edgar_parser.filing_facts.occurrence_id``); ``None`` when a
    #: side was not read from a presentation cell. Omitted from dumps when
    #: absent so a row without them keeps its v1 bytes.
    original_occurrence_id: str | None = Field(
        default=None, max_length=256, exclude_if=lambda value: value is None
    )
    restated_occurrence_id: str | None = Field(
        default=None, max_length=256, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def _validate_delta(self) -> "EdgarVintageValueDeltaV1":
        if self.scale not in WIRE_SCALE_EXPONENTS:
            raise ValueError(
                "value delta scale is unrecognised: "
                f"{self.scale!r} (declared: {sorted(WIRE_SCALE_EXPONENTS)})"
            )
        _canonical_decimal(self.original_value, field_name="original_value")
        _canonical_decimal(self.restated_value, field_name="restated_value")
        for axis, member in self.dimensions.items():
            if not axis.strip() or not member.strip():
                raise ValueError("value delta dimensions must be non-empty")
        return self

    def _native(self, value: str, *, field_name: str) -> Decimal:
        # ``scaleb`` shifts the exponent rather than multiplying, so applying a
        # declared scale is exact at every magnitude — a restated value never
        # acquires or loses a digit on the way in.
        return _canonical_decimal(value, field_name=field_name).scaleb(
            WIRE_SCALE_EXPONENTS[self.scale]
        )

    @property
    def native_original_value(self) -> Decimal:
        """Accession A's value in native units — the declared scale applied."""

        return self._native(self.original_value, field_name="original_value")

    @property
    def native_restated_value(self) -> Decimal:
        """Accession B's value in native units — the declared scale applied."""

        return self._native(self.restated_value, field_name="restated_value")

    @property
    def is_dimensional(self) -> bool:
        """Whether the identity names an axis member rather than a statement line."""

        return bool(self.dimensions) or self.axis_key is not None


class EdgarRestatementLineageV1(_WireFactModel):
    """The strong fact: accession B restates accession A for one period (§1.1).

    Constraints the type itself carries, so a violation never reaches the
    consumer's evidence graph: same issuer on both ends, one period,
    ``restated_filed <= restating_filed`` (mirroring the consumer validator's
    non-forward ancestry rule), and two distinct accessions.
    """

    cik: str = Field(min_length=1, max_length=10)
    ticker: str = Field(min_length=1, max_length=16)
    period: EdgarVintagePeriodV1
    restated_accession: str = Field(min_length=1, max_length=32)
    restated_form: str = Field(min_length=1, max_length=32)
    restated_filed: str = Field(min_length=10, max_length=10)
    restating_accession: str = Field(min_length=1, max_length=32)
    restating_form: str = Field(min_length=1, max_length=32)
    restating_filed: str = Field(min_length=10, max_length=10)
    detection_basis: tuple[EdgarVintageDetectionBasis, ...] = Field(min_length=1)
    comparison_status: EdgarVintageComparisonStatus
    value_deltas: tuple[EdgarVintageValueDeltaV1, ...] = ()
    confidence: EdgarVintageConfidence
    evidence: dict[str, Any] = Field(default_factory=dict)
    producer_version: str = Field(min_length=1, max_length=128)
    policy_version: str = Field(min_length=1, max_length=128)
    detected_at: str = Field(min_length=1, max_length=40)

    @model_validator(mode="after")
    def _validate_lineage(self) -> "EdgarRestatementLineageV1":
        _require_issuer_identity(self.cik, self.ticker, label="restatement fact")
        for name in ("restated_accession", "restating_accession"):
            _require_accession(getattr(self, name), field_name=name)
        if self.restated_accession == self.restating_accession:
            raise ValueError("a restatement fact must name two distinct accessions")
        restated_filed = _canonical_iso_date(
            self.restated_filed, field_name="restated_filed"
        )
        restating_filed = _canonical_iso_date(
            self.restating_filed, field_name="restating_filed"
        )
        if restating_filed < restated_filed:
            raise ValueError("restatement lineage cannot point forward in filing time")
        if len(set(self.detection_basis)) != len(self.detection_basis):
            raise ValueError("detection_basis must not repeat a mechanism")
        _canonical_utc_datetime(self.detected_at, field_name="detected_at")
        return self

    @property
    def restating_filed_date(self) -> date:
        return _canonical_iso_date(self.restating_filed, field_name="restating_filed")

    @property
    def restated_filed_date(self) -> date:
        return _canonical_iso_date(self.restated_filed, field_name="restated_filed")


class EdgarVintageObservationV1(_WireFactModel):
    """One disputed observation carried by one conflict candidate (§1.2)."""

    tag: str = Field(min_length=1, max_length=256)
    dimensions: dict[str, str] = Field(default_factory=dict)
    unit: str = Field(min_length=1, max_length=64)
    scale: str = Field(min_length=1, max_length=32)
    value: str = Field(min_length=1, max_length=64)
    locator: EdgarVintageFactLocatorV1

    @model_validator(mode="after")
    def _validate_observation(self) -> "EdgarVintageObservationV1":
        if self.scale not in WIRE_SCALE_EXPONENTS:
            raise ValueError(
                "observation scale is unrecognised: "
                f"{self.scale!r} (declared: {sorted(WIRE_SCALE_EXPONENTS)})"
            )
        _canonical_decimal(self.value, field_name="observation value")
        for axis, member in self.dimensions.items():
            if not axis.strip() or not member.strip():
                raise ValueError("observation dimensions must be non-empty")
        return self

    @property
    def native_value(self) -> Decimal:
        return _canonical_decimal(self.value, field_name="observation value").scaleb(
            WIRE_SCALE_EXPONENTS[self.scale]
        )

    @property
    def is_dimensional(self) -> bool:
        return bool(self.dimensions)

    @property
    def axis_key(self) -> str | None:
        return next(iter(self.dimensions), None)


class EdgarVintageConflictCandidateV1(_WireFactModel):
    """One accession's side of a per-period value conflict (§1.2)."""

    accession: str = Field(min_length=1, max_length=32)
    form: str = Field(min_length=1, max_length=32)
    filed: str = Field(min_length=10, max_length=10)
    observations: tuple[EdgarVintageObservationV1, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_candidate(self) -> "EdgarVintageConflictCandidateV1":
        _require_accession(self.accession, field_name="candidate accession")
        _canonical_iso_date(self.filed, field_name="candidate filed")
        return self

    @property
    def filed_date(self) -> date:
        return _canonical_iso_date(self.filed, field_name="candidate filed")


class EdgarPeriodValueConflictV1(_WireFactModel):
    """The weaker sibling: two accessions disagree, no lineage established (§1.2).

    Not a producer failure state — the honest output when evidence falls short
    of lineage, and the construction that makes silence impossible when values
    disagree (§1.3). At least two candidates by type, so "a conflict with one
    side" is unwritable.
    """

    cik: str = Field(min_length=1, max_length=10)
    ticker: str = Field(min_length=1, max_length=16)
    period: EdgarVintagePeriodV1
    candidates: tuple[EdgarVintageConflictCandidateV1, ...] = Field(min_length=2)
    reason: EdgarVintageConflictReason
    evidence: dict[str, Any] = Field(default_factory=dict)
    producer_version: str = Field(min_length=1, max_length=128)
    policy_version: str = Field(min_length=1, max_length=128)
    detected_at: str = Field(min_length=1, max_length=40)

    @model_validator(mode="after")
    def _validate_conflict(self) -> "EdgarPeriodValueConflictV1":
        _require_issuer_identity(self.cik, self.ticker, label="conflict fact")
        accessions = tuple(item.accession for item in self.candidates)
        if len(set(accessions)) != len(accessions):
            raise ValueError("conflict candidates must name distinct accessions")
        _canonical_utc_datetime(self.detected_at, field_name="detected_at")
        return self


class EdgarVintageFactsEnvelopeV1(_WireEnvelopeModel):
    """The pinned ``edgar-vintage-facts.v1`` envelope (producer contract §4.1).

    ``graph_cutoff`` is the producer's declared reach — the submissions history
    it considered — and is a *fact about the envelope*, not a second cutoff for
    the build. The build's own cutoff is the pin's ``cutoff_at`` (P2), and the
    landing is what reconciles the two.
    """

    contract_version: Literal["edgar-vintage-facts.v1"]
    producer_id: Literal["edgar_updater"]
    producer_version: str = Field(min_length=1, max_length=128)
    policy_version: str = Field(min_length=1, max_length=128)
    cik: str = Field(min_length=1, max_length=10)
    ticker: str = Field(min_length=1, max_length=16)
    graph_cutoff: date
    computed_at: str = Field(min_length=1, max_length=40)
    restatements: tuple[EdgarRestatementLineageV1, ...] = ()
    conflicts: tuple[EdgarPeriodValueConflictV1, ...] = ()

    @model_validator(mode="after")
    def _validate_envelope(self) -> "EdgarVintageFactsEnvelopeV1":
        _require_issuer_identity(self.cik, self.ticker, label="envelope")
        _canonical_utc_datetime(self.computed_at, field_name="computed_at")
        for fact in (*self.restatements, *self.conflicts):
            if (fact.cik, fact.ticker) != (self.cik, self.ticker):
                raise ValueError(
                    "every fact must name the envelope's issuer: "
                    f"{fact.ticker}/{fact.cik} in a {self.ticker}/{self.cik} envelope"
                )
        seen: set[tuple[str, str, str]] = set()
        for lineage in self.restatements:
            key = (
                lineage.period.label,
                lineage.restated_accession,
                lineage.restating_accession,
            )
            if key in seen:
                raise ValueError(
                    "one fact per (period, restated, restating) is the contract; "
                    f"{key} repeats"
                )
            seen.add(key)
        return self


    @property
    def is_empty(self) -> bool:
        """Whether the envelope states *no restatements as of ``graph_cutoff``*.

        The positive fact, and the reason as-filed binding is evidenced rather
        than assumed. It is not the same fact as an unpinned envelope, which
        says only that the producer never ran (R3).
        """

        return not self.restatements and not self.conflicts


def _require_accession(value: str, *, field_name: str) -> None:
    if _ACCESSION_RE.fullmatch(value) is None:
        raise ValueError(
            f"{field_name} must be a canonical SEC accession "
            f"(NNNNNNNNNN-NN-NNNNNN): {value!r}"
        )


def _require_issuer_identity(cik: str, ticker: str, *, label: str) -> None:
    if _CIK_RE.fullmatch(cik) is None:
        raise ValueError(f"{label} cik must be 1-10 digits: {cik!r}")
    if _TICKER_RE.fullmatch(ticker) is None:
        raise ValueError(f"{label} ticker must be a canonical symbol: {ticker!r}")


def decode_edgar_vintage_facts(
    payload: bytes | str,
    *,
    expected_ticker: str | None = None,
) -> EdgarVintageFactsEnvelopeV1:
    """Decode pinned envelope bytes, or refuse with the repair named.

    ``expected_ticker`` binds the envelope to the corpus that pins it: an
    envelope for another issuer inside this issuer's pin is a mis-pinned
    artifact, and reading it would attach one company's restatements to
    another's evidence graph. Checked here rather than at the landing so the
    boundary — not its caller — owns every way the wire can be wrong.
    """

    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    if not isinstance(payload, bytes):
        raise EdgarVintageFactsContractError(
            "pinned restatement lineage artifact must decode from bytes"
        )
    try:
        envelope = EdgarVintageFactsEnvelopeV1.model_validate_json(payload)
    except ValidationError as exc:
        raise EdgarVintageFactsContractError(str(exc)) from exc
    if expected_ticker is not None and envelope.ticker != expected_ticker.upper():
        raise EdgarVintageFactsContractError(
            f"envelope states issuer {envelope.ticker} but is pinned by the "
            f"{expected_ticker.upper()} corpus"
        )
    return envelope


__all__ = [
    "EDGAR_VINTAGE_FACTS_CONTRACT_VERSION",
    "EDGAR_VINTAGE_FACTS_PRODUCER_ID",
    "WIRE_SCALE_EXPONENTS",
    "EdgarPeriodValueConflictV1",
    "EdgarRestatementLineageV1",
    "EdgarVintageComparisonStatus",
    "EdgarVintageConfidence",
    "EdgarVintageConflictCandidateV1",
    "EdgarVintageConflictReason",
    "EdgarVintageDetectionBasis",
    "EdgarVintageFactsContractError",
    "EdgarVintageFactsEnvelopeV1",
    "EdgarVintageFactLocatorV1",
    "EdgarVintageObservationV1",
    "EdgarVintagePeriodV1",
    "EdgarVintageValueDeltaV1",
    "decode_edgar_vintage_facts",
]
