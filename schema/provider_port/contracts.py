"""Canonical, provider-neutral payload contracts.

Adapters own normalization and ordering.  The contracts validate ordering rather
than repairing it so malformed adapter output fails at the boundary and replay
identity is never changed implicitly.

D-4 filing-line identity is the five-part accession + ``PeriodRef`` + normalized
role + tag + frozen dimension set, as ruled in
``docs/qa/edgar-statement-binding-rulings-2026-09-08.md`` row 2.
"""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, ClassVar, Final, Literal, TypeAlias, get_args

from pydantic import Field, TypeAdapter, ValidationError, WrapSerializer, model_validator
from value_semantics_core import PeriodRef, Quantity, normalize_decimal_v1

from industry_slice_core.profile_normalization import ProviderCapability, ProviderError
from industry_slice_core.metric_registry import require_registered_metric_id


ReportedPeriodType: TypeAlias = Literal["FY", "Q1", "Q2", "Q3", "Q4"]
ObservationStatus: TypeAlias = Literal["ok", "missing", "failed"]
EstimatePeriod: TypeAlias = Literal["annual", "quarter"]
PriceSeriesBasis: TypeAlias = Literal[
    "unadjusted_close",
    "dividend_adjusted_close",
]
ProviderFetchSource: TypeAlias = Literal["live", "cache"]
ProviderParameterValue: TypeAlias = str | int | bool | None


def _require_decimal(value: Decimal | None, field_name: str) -> None:
    # Money: quote prices, market caps, share counts and estimate values feed
    # per-share prices; a binary float here would carry its rounding error into them.
    if value is not None and not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")


@dataclass(frozen=True)
class VendorProvenance:
    provider: str
    native_endpoint: str
    native_field: str
    # Absent means absent on the wire: the compiler input digest is a function of
    # the provenance actually present, never of this class's field list.  The
    # convention in `schema/build_model_product_contracts.py` is
    # `SourceBundle.statement_source_material`,
    # `HistoricalBasisFact.{required_role_correspondence,reconciliation}` and
    # `AnnualPeriodCoverage.disclosure_search_{source_id,accession}`; dropping it
    # re-serializes every unset field as null and collides with bundles already
    # sealed under the unchanged identity (measured on `sb_6e42fd77`, 2026-09-10).
    preferred_label_text: Annotated[
        str | None, Field(exclude_if=lambda value: value is None)
    ] = None
    preferred_label_role_uri: Annotated[
        str | None, Field(exclude_if=lambda value: value is None)
    ] = None
    preferred_label_negates_value: Annotated[
        bool | None, Field(exclude_if=lambda value: value is None)
    ] = None
    # The filer's XBRL ``decimals`` for the fact (an integer lexical or ``INF``):
    # the precision it was printed to. Carried here, not in ``native_field``, because
    # ``native_field`` is embedded verbatim in evidence locators that accepted
    # foundations compare against.
    decimals: Annotated[
        str | None, Field(exclude_if=lambda value: value is None)
    ] = None
    provider_registry: InitVar[object] = None

    @model_validator(mode="before")
    @classmethod
    def _inject_registry_context(cls, value: object, info: object) -> object:
        from .registry import inject_provider_registry_validation_context

        return inject_provider_registry_validation_context(cls, value, info)

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema: object, handler: object) -> object:
        # ``provider_registry`` is construction-only context, not wire shape.
        schema = dict(handler(core_schema))  # type: ignore[operator]
        properties = dict(schema.get("properties") or {})
        properties.pop("provider_registry", None)
        schema["properties"] = properties
        return schema

    def __post_init__(self, provider_registry: object) -> None:
        object.__setattr__(
            self,
            "provider",
            self._validated_provider(self.provider, provider_registry),
        )

    @staticmethod
    def _validated_provider(provider: str, provider_registry: object) -> str:
        # Imported lazily because the registry owns implementations and imports
        # these dependency-light payload contracts.
        from .registry import ProviderRegistry, require_registered_provider_id

        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        return require_registered_provider_id(
            provider,
            provider_registry=registry,
            entry_point="vendor provenance provider",
        )


@dataclass(frozen=True)
class QualityFlag:
    code: str
    severity: Literal["info", "warning"]
    line: FilingLineRef | None
    metric_id: str | None
    fiscal_year: int | None
    detail: str

    def __post_init__(self) -> None:
        if self.line is not None and not isinstance(self.line, FilingLineRef):
            raise FilingLineIdentityInvalid("quality-flag line must be a FilingLineRef")
        if self.metric_id is not None:
            object.__setattr__(
                self,
                "metric_id",
                require_registered_metric_id(
                    self.metric_id,
                    entry_point="quality-flag metric",
                ),
            )



@dataclass(frozen=True)
class ReportedPeriod:
    fiscal_year: int
    period_type: ReportedPeriodType
    period_end: date
    period_start: date | None = field(default=None, kw_only=True)
    source_provider: str | None = field(default=None, kw_only=True)
    native_source_id: str | None = field(default=None, kw_only=True)
    source_available_on: date | None = field(default=None, kw_only=True)
    source_locator: str | None = field(default=None, kw_only=True)
    source_accepted_at: datetime | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        source_shape = (
            self.period_start,
            self.source_provider,
            self.native_source_id,
            self.source_available_on,
            self.source_locator,
        )
        if any(item is not None for item in source_shape) and not all(
            item is not None for item in source_shape
        ):
            raise ValueError("reported-period source fields must be all present or absent")
        if self.source_accepted_at is not None and self.period_start is None:
            raise ValueError(
                "reported-period acceptance time requires complete source fields"
            )
        if self.period_start is None:
            return
        if self.period_start >= self.period_end:
            raise ValueError("reported period start must precede end")
        if (
            self.source_available_on is not None
            and self.source_available_on < self.period_end
        ):
            raise ValueError("reported period cannot be available before its end")
        if self.source_accepted_at is not None:
            if (
                self.source_accepted_at.tzinfo is None
                or self.source_accepted_at.utcoffset() != timedelta(0)
            ):
                raise ValueError("reported-period source_accepted_at must be aware UTC")
            if self.source_accepted_at.date() < self.period_end:
                raise ValueError(
                    "reported period cannot be accepted before its end"
                )
        for field_name, value, max_length in (
            ("source_provider", self.source_provider, 128),
            ("native_source_id", self.native_source_id, 256),
            ("source_locator", self.source_locator, 2048),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be nonblank")
            if len(value) > max_length:
                raise ValueError(f"{field_name} exceeds {max_length} characters")

    @property
    def has_sourced_duration(self) -> bool:
        return self.period_start is not None

    @property
    def has_precise_source_acceptance(self) -> bool:
        return self.has_sourced_duration and self.source_accepted_at is not None


@dataclass(frozen=True)
class FiscalWindow:
    """Deterministic replay anchor, never derived from wall-clock time."""

    end_fiscal_year: int
    n_years: int

    def __post_init__(self) -> None:
        if self.n_years < 1:
            raise ValueError("n_years must be at least 1")


# ---------------------------------------------------------------------------
# Filing-line identity and provenance grade
#
# A sourced fact's identity is the filing column it *is*: accession +
# ``PeriodRef`` + tag-in-role, value as ``Quantity``, label as display
# metadata. There is one fact plane; the retired concept-keyed DTOs carried
# neither accession nor role and do not survive beside it.
# ---------------------------------------------------------------------------
#
# Not the same thing as ``schema.correspondence.PresentationLineRef``, and
# deliberately weaker: that ref answers "is this line the continuation of
# that line" and therefore carries the tree parent and calculation
# membership only a pinned presentation tree can supply. A fact's identity
# is the three parts the design names, which an adapter-asserted column can
# also carry.
# ---------------------------------------------------------------------------


#: Provenance grade of a pinned column, recorded at pin time (design §6.4,
#: three-valued per §15 item 7, accepted 2026-08-20):
#:
#: * ``filing_native`` — the value came from the filing: Edgar-filled columns
#: * ``adapter_asserted`` — a provider served the number and the adapter
#:   names the filing line it corresponds to; statement adapters supply only
#:   cross-check and visible gap-fill candidates behind Edgar authority;
#: * ``analyst_asserted`` — a value authored through the successor path that
#:   no producer served and no adapter asserted.
#:
#: Which producer earns which grade is pin-construction policy, decided
#: where the column is filled — never inferred from a value here.
FilingProvenanceGrade: TypeAlias = Literal[
    "filing_native",
    "adapter_asserted",
    "analyst_asserted",
]

#: The closed grade vocabulary, derived from the type so the two cannot
#: drift. Consumers read this rather than re-literalizing the values.
FILING_PROVENANCE_GRADES: Final[tuple[FilingProvenanceGrade, ...]] = get_args(
    FilingProvenanceGrade
)

#: The grades spec 2 item 1's identity-fidelity clause evaluates against
#: (design §6.4). Single owner of that scoping: an assertion that a pinned
#: column's identity, label, ``PeriodRef`` and ``Quantity`` reproduce the
#: filing is meaningful only where the filing is what served it.
IDENTITY_FIDELITY_GRADES: Final[frozenset[str]] = frozenset({"filing_native"})

#: The canonical SEC accession form (NNNNNNNNNN-NN-NNNNNN), as the
#: vintage-facts contract spells it.
_ACCESSION_PART_LENGTHS: Final = (10, 2, 6)

#: The one owner of "same statement role" in this repo is
#: ``schema.statement_topology.normalize_topology_role``. This module cannot
#: import it — the topology module reads these dependency-light contracts —
#: and would not want to: repairing a role here would change replay identity
#: implicitly, which is the thing this module's contracts exist to refuse.
#: So a role must arrive already normalized, and the shape that function
#: produces (lowercase alphanumeric) is what is admitted; anything else is a
#: typed refusal naming the normalizer.
_ROLE_NORMALIZER: Final = "schema.statement_topology.normalize_topology_role"

_PERIOD_REF_ADAPTER: Final = TypeAdapter(PeriodRef)

_FILING_LINE_PIN_LANE: Final = (
    "pin-construction lane (name the filing column this value is)"
)
_FILING_LINE_GRADE_LANE: Final = (
    "pin-construction lane (record the grade from the producer that filled "
    "the column)"
)
_FILING_LINE_ADAPTER_LANE: Final = (
    "provider-port adapter lane (the adapter that emitted this row)"
)


class FilingLineContractError(ValueError):
    """Typed refusal on the filing-line plane, with the lane that repairs it.

    Derived from ``ValueError`` deliberately: these run inside dataclass
    construction, where the boundary's callers already fail closed on one.
    """

    code: ClassVar[str] = "filing_line_contract_error"

    def __init__(self, reason: str, *, repair: str) -> None:
        self.reason = reason
        self.repair = repair
        super().__init__(f"{self.code}: {reason} (repair: {repair})")


class FilingLineIdentityInvalid(FilingLineContractError):
    """A filing-line reference does not name a filing column."""

    code: ClassVar[str] = "filing_line_identity_invalid"

    def __init__(self, reason: str, *, repair: str = _FILING_LINE_PIN_LANE) -> None:
        super().__init__(reason, repair=repair)


class FilingProvenanceGradeInvalid(FilingLineContractError):
    """A grade outside the closed vocabulary, or one its rules forbid."""

    code: ClassVar[str] = "filing_provenance_grade_invalid"

    def __init__(self, reason: str, *, repair: str = _FILING_LINE_GRADE_LANE) -> None:
        super().__init__(reason, repair=repair)


class FilingLineFactInvalid(FilingLineContractError):
    """A filing-line fact or observation contradicts its own identity."""

    code: ClassVar[str] = "filing_line_fact_invalid"

    def __init__(
        self, reason: str, *, repair: str = _FILING_LINE_ADAPTER_LANE
    ) -> None:
        super().__init__(reason, repair=repair)


def _require_filing_line_text(value: object, field_name: str) -> str:
    """Non-blank and exactly as given: an identity part is never repaired."""

    if not isinstance(value, str) or not value.strip():
        raise FilingLineIdentityInvalid(f"{field_name} must be a non-empty string")
    if value != value.strip():
        raise FilingLineIdentityInvalid(
            f"{field_name} must not carry surrounding whitespace: {value!r}"
        )
    return value


def _is_canonical_accession(value: str) -> bool:
    parts = value.split("-")
    if len(parts) != len(_ACCESSION_PART_LENGTHS):
        return False
    return all(
        len(part) == length and part.isascii() and part.isdigit()
        for part, length in zip(parts, _ACCESSION_PART_LENGTHS)
    )


def _is_normalized_role(value: str) -> bool:
    return value.isascii() and value.isalnum() and value == value.lower()


def _require_xbrl_qname(value: object, field_name: str) -> str:
    # Preserve lexical spelling, including filed NCName punctuation: a member
    # local name may legally contain a dot (`iss:AcquiredCo.Member`), so the
    # check admits it without adding a regex dependency to this port.
    qname = _require_filing_line_text(value, field_name)
    parts = qname.split(":")
    if len(parts) != 2 or not all(
        part
        and part.isascii()
        and (part[0].isalpha() or part[0] == "_")
        and all(character.isalnum() or character in "_.-" for character in part)
        for part in parts
    ):
        raise FilingLineIdentityInvalid(
            f"{field_name} must be a prefixed XBRL QName (prefix:Local): {value!r}"
        )
    return qname


@dataclass(frozen=True)
class FilingDimensionRef:
    """One explicit axis=member coordinate in a filing-line identity."""

    axis_qname: str
    member_qname: str

    def __post_init__(self) -> None:
        _require_xbrl_qname(self.axis_qname, "axis_qname")
        _require_xbrl_qname(self.member_qname, "member_qname")


@dataclass(frozen=True)
class FilingLineRef:
    """Identity of one filing presentation column (design §6.1).

    Accession + ``PeriodRef`` + normalized role + tag + frozen dimension set
    identify the tag as it sits in that filing's statement role and explicit
    member coordinate. Every part is stored exactly as given: the role arrives
    already normalized by the one owner of "same role" in this repo, so a role
    reached through the correspondence plane and a role reached through a pin
    compare equal, and an unnormalized part is refused here rather than
    repaired into a different identity.

    No label field: labels are display metadata carried beside a value, never
    identity (spec 2 §3). The presentation arc a binding row cites for the
    line rides beside it too, as the fetch's ``cited_arcs``. No vintage field
    either — an ``adapter_asserted`` column takes no independent restatement
    behavior, and ``select_vintage`` stays the only restatement authority for
    every pin (design §6.4).
    """

    accession: str
    period: PeriodRef
    statement_role: str
    tag: str
    # Empty is absent on the digested wire, preserving epoch-10 consolidated refs.
    dimensions: Annotated[
        tuple[FilingDimensionRef, ...], Field(exclude_if=lambda value: not value)
    ] = ()

    def __post_init__(self) -> None:
        accession = _require_filing_line_text(self.accession, "accession")
        if not _is_canonical_accession(accession):
            raise FilingLineIdentityInvalid(
                "accession must be a canonical SEC accession "
                f"(NNNNNNNNNN-NN-NNNNNN): {self.accession!r}"
            )
        _require_filing_line_text(self.tag, "tag")
        role = _require_filing_line_text(self.statement_role, "statement_role")
        if not _is_normalized_role(role):
            raise FilingLineIdentityInvalid(
                f"statement_role must arrive normalized through {_ROLE_NORMALIZER}: "
                f"{self.statement_role!r}"
            )
        try:
            period = _PERIOD_REF_ADAPTER.validate_python(self.period)
        except ValidationError as exc:
            raise FilingLineIdentityInvalid(
                f"period must be a PeriodRef: {exc}"
            ) from exc
        if not isinstance(self.dimensions, (list, tuple)):
            raise FilingLineIdentityInvalid(
                "dimensions must be a list or tuple of FilingDimensionRef objects"
            )
        dimensions = tuple(self.dimensions)
        if any(
            not isinstance(dimension, FilingDimensionRef)
            for dimension in dimensions
        ):
            raise FilingLineIdentityInvalid(
                "dimensions must contain only FilingDimensionRef objects"
            )
        axes = [dimension.axis_qname for dimension in dimensions]
        if len(set(axes)) != len(axes):
            raise FilingLineIdentityInvalid(
                "dimensions may contain each axis_qname only once"
            )
        object.__setattr__(
            self,
            "dimensions",
            tuple(
                sorted(
                    dimensions,
                    key=lambda dimension: (
                        dimension.axis_qname,
                        dimension.member_qname,
                    ),
                )
            ),
        )
        object.__setattr__(self, "period", period)


SourceFactRail: TypeAlias = Literal[
    "statement",
    "schedule_disclosure",
    "allocated_sbc",
    "lease_flow",
    "non_gaap_bridge",
    "kpi",
]
SOURCE_FACT_RAILS: Final[frozenset[str]] = frozenset(get_args(SourceFactRail))


def _escape_fact_id_component(value: str) -> str:
    return value.replace("%", "%25").replace(".", "%2E")


def _filing_line_observation_id(line: FilingLineRef) -> str:
    return (
        "filing-line:"
        f"{line.accession}:{line.period.canonical_label()}:{line.statement_role}:{line.tag}:"
        + ";".join(
            f"{dimension.axis_qname}={dimension.member_qname}"
            for dimension in line.dimensions
        )
    )


@dataclass(frozen=True)
class SourceFactRef:
    """Canonical identity of one sourced filing or KPI fact (ruling SOURCE-IDENTITY-ONCE).

    Minted once by ``mint_source_fact_ref(line, rail)``. Never hashes numeric value,
    label, requirement_id, driver_id, or locator text.
    """

    line: FilingLineRef
    rail: SourceFactRail
    fact_id: str
    evidence_id: str
    observation_id: str


def mint_source_fact_ref(
    line: FilingLineRef,
    rail: SourceFactRail = "statement",
    *,
    suffix: str | None = None,
) -> SourceFactRef:
    if not isinstance(line, FilingLineRef):
        raise FilingLineIdentityInvalid("line must be a FilingLineRef")
    if rail not in SOURCE_FACT_RAILS:
        raise ValueError(f"unsupported SourceFactRail: {rail!r}")
    period_id = line.period.canonical_label()
    prefix = (
        f"fact.{line.accession}.{period_id}.{line.statement_role}."
        f"{_escape_fact_id_component(line.tag)}."
    )
    if line.dimensions:
        dims = ";".join(
            f"{_escape_fact_id_component(dimension.axis_qname)}="
            f"{_escape_fact_id_component(dimension.member_qname)}"
            for dimension in line.dimensions
        )
        prefix += f"{dims}."
    token = suffix if suffix is not None else ("edgar" if rail in {"statement", "kpi"} else rail)
    fact_id = f"{prefix}{token}"
    evidence_id = fact_id.replace("fact.", "evidence.", 1)
    return SourceFactRef(
        line=line,
        rail=rail,
        fact_id=fact_id,
        evidence_id=evidence_id,
        observation_id=_filing_line_observation_id(line),
    )


def _serialize_filing_line_quantity(
    value: Quantity,
    handler,
    info,
) -> dict[str, object]:
    """Emit the strict Quantity wire in both Pydantic dump modes."""

    raw_value = value.value
    if isinstance(raw_value, str):
        try:
            canonical_value = normalize_decimal_v1(Decimal(raw_value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(
                "internally constructed Quantity string is not a finite decimal"
            ) from exc
        if canonical_value != raw_value:
            raise ValueError(
                "internally constructed Quantity string is not canonical"
            )
        payload: dict[str, object] = {
            "display": value.display.model_dump(mode=info.mode),
        }
    else:
        canonical_value = normalize_decimal_v1(raw_value)
        serialized = handler(value)
        if not isinstance(serialized, dict):
            raise TypeError("Quantity serializer must emit an object")
        payload = serialized
    return {
        **payload,
        "value": canonical_value,
    }


FilingLineQuantity: TypeAlias = Annotated[
    Quantity,
    WrapSerializer(
        _serialize_filing_line_quantity,
        return_type=dict[str, object],
        when_used="always",
    ),
]


@dataclass(frozen=True)
class FilingLineFact:
    """One pinned filing column's value, with the grade that names its source.

    ``label`` is the filing's preferred/presentation label — what the table
    shows a human. It is excluded from equality and hashing on purpose:
    display never joins (spec 2 §3, design R5).

    ``provenance`` records the producer whose payload served the value, and
    is exactly what the grade is derived from. ``analyst_asserted`` carries
    none: it names a value no producer served and no adapter asserted, so a
    vendor stamp there would be the synthetic mint that grade replaces
    (design §15 item 7).
    """

    line: FilingLineRef
    value: FilingLineQuantity
    grade: FilingProvenanceGrade
    provenance: VendorProvenance | None
    label: str | None = field(default=None, compare=False)
    source_ref: Annotated[
        SourceFactRef | None, Field(default=None, exclude=True)
    ] = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.line, FilingLineRef):
            raise FilingLineFactInvalid("line must be a FilingLineRef")
        if not isinstance(self.value, Quantity):
            raise FilingLineFactInvalid("value must be a Quantity")
        if self.grade not in FILING_PROVENANCE_GRADES:
            raise FilingProvenanceGradeInvalid(
                f"{self.grade!r} is not one of {list(FILING_PROVENANCE_GRADES)}"
            )
        if self.grade == "analyst_asserted":
            if self.provenance is not None:
                raise FilingProvenanceGradeInvalid(
                    "an analyst_asserted value carries no vendor provenance: "
                    "no producer served it and no adapter asserted it"
                )
        elif not isinstance(self.provenance, VendorProvenance):
            raise FilingProvenanceGradeInvalid(
                f"a value graded {self.grade} must record the producer that "
                "served it as VendorProvenance"
            )
        if self.label is not None and (
            not isinstance(self.label, str) or not self.label.strip()
        ):
            raise FilingLineFactInvalid(
                "label must be a non-empty string when present"
            )
        if self.source_ref is None:
            default_rail: SourceFactRail = (
                "kpi"
                if self.provenance is not None and self.provenance.native_endpoint == "operational_kpis"
                else "statement"
            )
            default_suffix = (
                self.provenance.provider
                if self.provenance is not None and default_rail == "statement"
                else None
            )
            object.__setattr__(
                self,
                "source_ref",
                mint_source_fact_ref(self.line, default_rail, suffix=default_suffix),
            )
        elif not isinstance(self.source_ref, SourceFactRef):
            raise FilingLineFactInvalid("source_ref must be a SourceFactRef")
        elif self.source_ref.line != self.line:
            # dataclasses.replace carries the prior reference when a caller
            # changes the line; retain its rail and producer suffix, not its ID.
            previous_prefix = mint_source_fact_ref(
                self.source_ref.line, self.source_ref.rail, suffix=""
            ).fact_id
            suffix = self.source_ref.fact_id[len(previous_prefix):]
            object.__setattr__(
                self,
                "source_ref",
                mint_source_fact_ref(self.line, self.source_ref.rail, suffix=suffix),
            )

    @property
    def provider(self) -> str | None:
        return (
            self.provenance.provider
            if self.provenance is not None
            else None
        )


    @property
    def carries_identity_fidelity(self) -> bool:
        """Whether spec 2 item 1's identity-fidelity clause evaluates here."""

        return self.grade in IDENTITY_FIDELITY_GRADES


@dataclass(frozen=True)
class FilingLineObservation:
    """One filing column asked for, and what came back.

    The concept-keyed observation above folds three parallel fields to check
    that a fact is the fact that was asked for; here identity is one object,
    so the check is one comparison.
    """

    line: FilingLineRef
    status: ObservationStatus
    fact: FilingLineFact | None
    detail: str | None
    provider: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.line, FilingLineRef):
            raise FilingLineFactInvalid("line must be a FilingLineRef")
        if self.status not in get_args(ObservationStatus):
            # The fill test below reads anything that is not ``ok`` as a
            # non-fill, so a drifted spelling would record an unfilled
            # column instead of refusing.
            raise FilingLineFactInvalid(
                f"{self.status!r} is not one of "
                f"{list(get_args(ObservationStatus))}"
            )
        if (self.status == "ok") != (self.fact is not None):
            raise FilingLineFactInvalid(
                "fact must be present if and only if status is 'ok'"
            )
        if self.fact is not None and self.fact.line != self.line:
            raise FilingLineFactInvalid(
                "fact identity must match observation identity"
            )
        fact_provider = self.fact.provider if self.fact is not None else None
        if self.provider is not None and (
            not isinstance(self.provider, str) or not self.provider.strip()
        ):
            raise FilingLineFactInvalid(
                "observation provider must be a non-empty string when present"
            )
        if (
            self.provider is not None
            and fact_provider is not None
            and self.provider != fact_provider
        ):
            raise FilingLineFactInvalid(
                "observation provider must match its fact provenance"
            )
        if self.provider is None and fact_provider is not None:
            object.__setattr__(self, "provider", fact_provider)


@dataclass(frozen=True)
class Quote:
    ticker: str
    price: Decimal
    currency: str | None
    as_of: datetime
    market_cap: Decimal | None
    shares_outstanding: Decimal | None

    def __post_init__(self) -> None:
        _require_decimal(self.price, "price")
        _require_decimal(self.market_cap, "market_cap")
        _require_decimal(self.shares_outstanding, "shares_outstanding")
        if self.as_of.utcoffset() != timedelta(0):
            raise ValueError("as_of must be a UTC datetime")


@dataclass(frozen=True)
class ProviderResponseLineage:
    """Provider-issued lineage for one normalized response batch."""

    provider: str
    native_endpoint: str
    response_observed_at: datetime
    normalized_response_sha256: str
    validated_params: tuple[tuple[str, ProviderParameterValue], ...]
    fetch_source: ProviderFetchSource
    scalar_result_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider must be nonblank")
        if not self.native_endpoint.strip():
            raise ValueError("native_endpoint must be nonblank")
        if self.response_observed_at.utcoffset() != timedelta(0):
            raise ValueError("response_observed_at must be a UTC datetime")
        digest = self.normalized_response_sha256
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise ValueError("normalized_response_sha256 must be lowercase SHA-256")
        names = tuple(name for name, _value in self.validated_params)
        if names != tuple(sorted(names)) or len(names) != len(set(names)):
            raise ValueError("validated_params must be uniquely sorted by name")
        for name, _value in self.validated_params:
            if not name.strip():
                raise ValueError("validated parameter names must be nonblank")
        if len(self.scalar_result_keys) != len(set(self.scalar_result_keys)):
            raise ValueError("scalar_result_keys must be unique")
        if any(not result_key.strip() for result_key in self.scalar_result_keys):
            raise ValueError("scalar_result_keys must be nonblank")


@dataclass(frozen=True)
class EstimateFact:
    ticker: str
    metric_id: str
    period: EstimatePeriod
    target_period_end: date
    statistic: Literal["mean", "median", "high", "low"]
    value: Decimal
    analyst_count: int | None
    provider_vintage: date | None
    target_period_end_result_key: str
    value_result_key: str
    analyst_count_result_key: str | None
    fiscal_year: int | None = None
    period_type: ReportedPeriodType | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metric_id",
            require_registered_metric_id(
                self.metric_id,
                entry_point="estimate-fact metric",
            ),
        )
        _require_decimal(self.value, "value")
        if (
            self.analyst_count is not None
            and (
                isinstance(self.analyst_count, bool)
                or not isinstance(self.analyst_count, int)
                or self.analyst_count < 0
            )
        ):
            raise ValueError("analyst_count must be a nonnegative integer or None")
        for field_name, result_key in (
            ("target_period_end_result_key", self.target_period_end_result_key),
            ("value_result_key", self.value_result_key),
        ):
            if not result_key.strip():
                raise ValueError(f"{field_name} must be nonblank")
        if (self.analyst_count is None) != (self.analyst_count_result_key is None):
            raise ValueError(
                "analyst_count_result_key must be present exactly when analyst_count is"
            )
        if (
            self.analyst_count_result_key is not None
            and not self.analyst_count_result_key.strip()
        ):
            raise ValueError("analyst_count_result_key must be nonblank")


@dataclass(frozen=True)
class EstimateFactsResult:
    facts: tuple[EstimateFact, ...]
    source_lineage: ProviderResponseLineage | None


@dataclass(frozen=True)
class MetricPoint:
    value: Decimal
    fiscal_year: int | None
    as_of: date

    def __post_init__(self) -> None:
        _require_decimal(self.value, "value")


@dataclass(frozen=True)
class MetricSeries:
    ticker: str
    metric_id: str
    period: Literal["annual", "ttm"]
    points: tuple[MetricPoint, ...]
    provenance: VendorProvenance

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metric_id",
            require_registered_metric_id(
                self.metric_id,
                entry_point="metric-series metric",
            ),
        )
        if any(
            self.points[index].as_of < self.points[index + 1].as_of
            for index in range(len(self.points) - 1)
        ):
            raise ValueError("points must be ordered by as_of descending")
        if self.period == "annual" and any(point.fiscal_year is None for point in self.points):
            raise ValueError("annual points must carry fiscal_year")
        if self.period == "ttm" and any(point.fiscal_year is not None for point in self.points):
            raise ValueError("TTM points must not carry fiscal_year")


@dataclass(frozen=True)
class PeerUniverse:
    ticker: str
    peers: tuple[str, ...]
    as_of: date | None
    method: str

    def __post_init__(self) -> None:
        if len(set(self.peers)) != len(self.peers):
            raise ValueError("peers must be deduplicated")
        if self.ticker in self.peers:
            raise ValueError("peers must exclude the target ticker")


@dataclass(frozen=True)
class PricePoint:
    trade_date: date
    adjusted_close: Decimal | None
    close: Decimal | None = None
    trade_date_result_key: str | None = None
    value_result_key: str | None = None

    def __post_init__(self) -> None:
        _require_decimal(self.adjusted_close, "adjusted_close")
        _require_decimal(self.close, "close")
        if self.adjusted_close is None and self.close is None:
            raise ValueError("price point requires adjusted_close or close")
        locator_shape = (self.trade_date_result_key, self.value_result_key)
        if any(item is not None for item in locator_shape) and not all(
            item is not None for item in locator_shape
        ):
            raise ValueError("price result keys must be all present or absent")
        if any(item is not None and not item.strip() for item in locator_shape):
            raise ValueError("price result keys must be nonblank")


@dataclass(frozen=True)
class PriceSeries:
    ticker: str
    basis: PriceSeriesBasis
    currency: str | None
    points: tuple[PricePoint, ...]
    source_lineage: ProviderResponseLineage | None = None

    def __post_init__(self) -> None:
        if any(
            self.points[index].trade_date > self.points[index + 1].trade_date
            for index in range(len(self.points) - 1)
        ):
            raise ValueError("points must be ordered by trade_date ascending")
        if self.basis == "unadjusted_close" and any(
            point.close is None or point.adjusted_close is not None
            for point in self.points
        ):
            raise ValueError("unadjusted price series must contain only close values")
        if self.basis == "dividend_adjusted_close" and any(
            point.adjusted_close is None or point.close is not None
            for point in self.points
        ):
            raise ValueError(
                "dividend-adjusted price series must contain only adjusted_close values"
            )




@dataclass(frozen=True)
class CanonicalBridgeComponent:
    role: str
    issuer_label: str
    sign: Literal[1, -1]
    fact: FilingLineFact | None
    evidenced_blank: Literal["dash"] | None = None


@dataclass(frozen=True)
class CanonicalNonGaapBridge:
    bridge_id: str
    canonical_output_metric: str
    output_metric_slug: str
    output_metric_label: str
    gaap_metric_id: str
    gaap_metric_label: str
    period: ReportedPeriod
    granularity: Literal["itemized", "condensed_summary"]
    closure_state: Literal["exact", "rounding_consistent", "unexplained"]
    half_ulp_budget: Decimal
    subtotal_half_ulp_budgets: tuple[Decimal, ...]
    gaap_base: FilingLineFact | None
    components: tuple[CanonicalBridgeComponent, ...]
    subtotals: tuple[FilingLineFact | None, ...]
    disclosed_output: FilingLineFact | None
    recomputed_output: Decimal
    output_residual: Decimal
    subtotal_residuals: tuple[Decimal, ...]
    gaap_base_blank: Literal["dash"] | None = None
    subtotal_blanks: tuple[Literal["dash"] | None, ...] = ()
    disclosed_output_blank: Literal["dash"] | None = None
    lineage: ProviderResponseLineage | None = None

class CapabilityNotAdvertisedError(ProviderError):
    """A registered partial provider does not implement the requested capability."""

    def __init__(self, *, provider: str, capability: ProviderCapability) -> None:
        super().__init__(
            provider=provider,
            capability=capability,
            ticker="",
            retryable=False,
            detail=(
                f"provider {provider!r} does not advertise capability "
                f"{capability.value}"
            ),
        )


__all__ = [
    "CanonicalBridgeComponent",
    "CanonicalNonGaapBridge",
    "CapabilityNotAdvertisedError",
    "EstimateFact",
    "EstimateFactsResult",
    "EstimatePeriod",
    "FILING_PROVENANCE_GRADES",
    "FilingDimensionRef",
    "FilingLineContractError",
    "FilingLineFact",
    "FilingLineFactInvalid",
    "FilingLineIdentityInvalid",
    "FilingLineObservation",
    "FilingLineQuantity",
    "FilingLineRef",
    "FilingProvenanceGrade",
    "FilingProvenanceGradeInvalid",
    "FiscalWindow",
    "IDENTITY_FIDELITY_GRADES",
    "MetricPoint",
    "MetricSeries",
    "ObservationStatus",
    "PeerUniverse",
    "PricePoint",
    "PriceSeries",
    "PriceSeriesBasis",
    "ProviderFetchSource",
    "ProviderParameterValue",
    "ProviderResponseLineage",
    "QualityFlag",
    "Quote",
    "ReportedPeriod",
    "ReportedPeriodType",
    "SOURCE_FACT_RAILS",
    "SourceFactRail",
    "SourceFactRef",
    "VendorProvenance",
    "mint_source_fact_ref",
]
