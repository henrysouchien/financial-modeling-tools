"""Strict consumer boundary for Edgar ``filing_facts.v1`` occurrences.

The models in this module preserve producer wire semantics.  They do not infer a
label, sign, scale, period, or binding.  In particular, preferred-label
negation remains a directive beside the native number; the Edgar provider
adapter is the sole consumer projection that applies it to produce the printed
presentation sign.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
import json
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
)
from value_semantics_core import DatePeriod, FiscalYearPeriod, PeriodRef


EDGAR_FILING_FACTS_CONTRACT_VERSION = "filing_facts.v1"
EDGAR_FILING_FACTS_PRODUCER_ID = "edgar_updater"


class EdgarFilingFactsContractError(ValueError):
    """The Edgar producer payload violates ``filing_facts.v1``."""


class _StrictWireModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _canonical_date(value: str, *, field_name: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a canonical ISO date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must be a canonical ISO date")
    return value


def _canonical_utc_timestamp(value: str, *, field_name: str) -> str:
    if not value.endswith("Z"):
        raise ValueError(f"{field_name} must use the canonical UTC Z suffix")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be UTC")
    return value


class ExpandedQNameV1(_StrictWireModel):
    lexical_qname: str = Field(min_length=1)
    namespace_uri: str = Field(min_length=1)
    local_name: str = Field(min_length=1)


class EntityV1(_StrictWireModel):
    scheme: str = Field(min_length=1)
    identifier: str = Field(min_length=1)
    is_filing_registrant: bool


class DecimalNumberV1(_StrictWireModel):
    kind: Literal["decimal"]
    value: str = Field(min_length=1)


class RationalNumberV1(_StrictWireModel):
    kind: Literal["rational"]
    numerator: str = Field(min_length=1)
    denominator: str = Field(min_length=1)


NativeNumberV1 = Annotated[
    DecimalNumberV1 | RationalNumberV1,
    Field(discriminator="kind"),
]


class StructuredUnitV1(_StrictWireModel):
    numerator_measures: tuple[ExpandedQNameV1, ...]
    denominator_measures: tuple[ExpandedQNameV1, ...]


class FractionOperandSourceV1(_StrictWireModel):
    lexical_value: str
    scale_exponent: int | None
    transform: str | None
    transform_namespace_uri: str | None = None


class NumericSourceV1(_StrictWireModel):
    lexical_value: str | None
    scale_exponent: int | None
    decimals: str | None
    transform: str | None
    inline_sign: Literal["+", "-"] | None
    transform_namespace_uri: str | None = None
    fraction_numerator: FractionOperandSourceV1 | None = None
    fraction_denominator: FractionOperandSourceV1 | None = None


class NumericQuantityV1(_StrictWireModel):
    state: Literal["numeric"]
    native_number: NativeNumberV1
    unit: StructuredUnitV1
    source: NumericSourceV1


class TextQuantityV1(_StrictWireModel):
    state: Literal["text"]
    text: str
    language: str | None = None


class NilQuantityV1(_StrictWireModel):
    state: Literal["nil"]
    item_kind: Literal["numeric", "text"]
    unit: StructuredUnitV1 | None = None
    numeric_source: NumericSourceV1 | None = None


NativeQuantityV1 = Annotated[
    NumericQuantityV1 | TextQuantityV1 | NilQuantityV1,
    Field(discriminator="state"),
]


class InstantPeriodV1(_StrictWireModel):
    kind: Literal["instant"]
    instant: str

    @field_validator("instant")
    @classmethod
    def _validate_instant(cls, value: str) -> str:
        return _canonical_date(value, field_name="instant")


class DurationPeriodV1(_StrictWireModel):
    kind: Literal["duration"]
    start: str
    end: str

    @field_validator("start", "end")
    @classmethod
    def _validate_date(cls, value: str, info: Any) -> str:
        return _canonical_date(value, field_name=info.field_name)


ExactPeriodV1 = Annotated[
    InstantPeriodV1 | DurationPeriodV1,
    Field(discriminator="kind"),
]


class ExplicitDimensionV1(_StrictWireModel):
    kind: Literal["explicit"]
    axis: ExpandedQNameV1
    member: ExpandedQNameV1
    member_label: str | None = None
    origin: Literal["segment", "scenario"]


class TypedDimensionV1(_StrictWireModel):
    kind: Literal["typed"]
    axis: ExpandedQNameV1
    canonical_member_xml: str
    origin: Literal["segment", "scenario"]


DimensionV1 = Annotated[
    ExplicitDimensionV1 | TypedDimensionV1,
    Field(discriminator="kind"),
]


class ContextQualifierV1(_StrictWireModel):
    origin: Literal["segment", "scenario"]
    canonical_xml: str


class FactLocatorV1(_StrictWireModel):
    anchor: str | None
    snippet: str | None
    source_xpath: str | None
    context_ref: str
    source_document_url: str
    source_filename: str
    source_document_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )


class FactProvenanceV1(_StrictWireModel):
    context_ref: str
    source_document_url: str
    source_filename: str
    source_document_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    locators: tuple[FactLocatorV1, ...] = Field(min_length=1)


class PreferredLabelV1(_StrictWireModel):
    text: str | None
    role_uri: str
    negates_value: bool


class PresentationV1(_StrictWireModel):
    statement_roles: tuple[
        Literal[
            "balance_sheet",
            "income_statement",
            "comprehensive_income",
            "equity",
            "cash_flow",
            "disclosure",
        ],
        ...,
    ]
    presentation_role_uri: str
    presentation_role_name: str
    table_title: str
    # edgar_updater dc9b53cc stamps the EFM 6.7.12 role kind; optional so
    # pre-dc9b53cc payloads still decode.
    presentation_kind: (
        Literal["statement", "parenthetical", "disclosure", "document"] | None
    ) = None


class PresentationTreeV1(_StrictWireModel):
    parent_qname: ExpandedQNameV1 | None
    order: float | None
    depth: int = Field(ge=0)
    sibling_index: int = Field(ge=0)


class PrintedFiscalYearPeriodV1(_StrictWireModel):
    kind: Literal["fiscal_year"]
    fiscal_year: int
    period_end: str

    @field_validator("period_end")
    @classmethod
    def _validate_period_end(cls, value: str) -> str:
        return _canonical_date(value, field_name="period_end")

    def to_period_ref(self) -> PeriodRef:
        return FiscalYearPeriod(kind="fiscal_year", fiscal_year=self.fiscal_year)


class PrintedInstantPeriodV1(_StrictWireModel):
    kind: Literal["instant"]
    date: str
    fiscal_year: int

    @field_validator("date")
    @classmethod
    def _validate_date(cls, value: str) -> str:
        return _canonical_date(value, field_name="date")

    def to_period_ref(self) -> PeriodRef:
        return DatePeriod(kind="date", date=self.date)


PrintedPeriodRefV1 = Annotated[
    PrintedFiscalYearPeriodV1 | PrintedInstantPeriodV1,
    Field(discriminator="kind"),
]


class PrintedColumnV1(_StrictWireModel):
    position: int = Field(ge=0)
    heading: str
    period_ref: PrintedPeriodRefV1
    period_start: str | None
    period_end: str

    @field_validator("period_start", "period_end")
    @classmethod
    def _validate_dates(cls, value: str | None, info: Any) -> str | None:
        if value is None:
            return None
        return _canonical_date(value, field_name=info.field_name)


class PresentationDisplayKnownV1(_StrictWireModel):
    kind: Literal["known"]
    numeric_kind: Literal["currency", "per_share", "count", "percentage"]
    scale: Literal["unit", "thousands", "millions", "billions"]
    currency: str | None
    unit_label: str | None
    precision: int | None = Field(ge=0)


class PresentationDisplayUnknownV1(_StrictWireModel):
    kind: Literal["unknown"]
    reason: str = Field(min_length=1)


PresentationDisplayV1 = Annotated[
    PresentationDisplayKnownV1 | PresentationDisplayUnknownV1,
    Field(discriminator="kind"),
]


class PresentationOccurrenceSourceV1(_StrictWireModel):
    primary_document_url: str
    source_filename: str
    table_locator: str
    row_locator: str
    cell_locator: str
    xbrl_locators: tuple[FactLocatorV1, ...]


class PresentationBoundFactV1(_StrictWireModel):
    qname: ExpandedQNameV1
    entity: EntityV1
    native_quantity: NativeQuantityV1
    period: ExactPeriodV1
    dimensions: tuple[DimensionV1, ...]
    context_qualifiers: tuple[ContextQualifierV1, ...]
    context_qualification: Literal[
        "unqualified", "dimensioned", "other_qualified", "mixed"
    ]
    provenance: FactProvenanceV1
    consistent_duplicates: tuple[FactLocatorV1, ...]


class BoundBindingV1(_StrictWireModel):
    kind: Literal["bound"]
    fact: PresentationBoundFactV1


class GapBindingV1(_StrictWireModel):
    kind: Literal["gap"]
    reason: Literal[
        "no_fact_for_presented_cell",
        "no_context_for_printed_column",
    ]


class AmbiguousBindingV1(_StrictWireModel):
    kind: Literal["ambiguous"]
    candidates: tuple[PresentationBoundFactV1, ...] = Field(min_length=2)


PresentationBindingV1 = Annotated[
    BoundBindingV1 | GapBindingV1 | AmbiguousBindingV1,
    Field(discriminator="kind"),
]


class PresentationCalculationV1(_StrictWireModel):
    parent_qname: str | None = None
    weight: float | None = None
    order: float | None = None
    role_uri: str | None = None
    arcrole: str | None = None


class FilingPresentationCellV1(_StrictWireModel):
    occurrence_kind: Literal["filing_presentation_cell"]
    accession: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    # Seven-input content address (Henry 2026-10-09, rulings index EDGAR-OCC-ID),
    # carried by producers from edgar_updater 8b368d1c; earlier stored captures omit it.
    occurrence_id: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    line_qname: ExpandedQNameV1
    line_kind: Literal["numeric", "text", "abstract"]
    presentation: PresentationV1
    tree: PresentationTreeV1
    preferred_label: PreferredLabelV1
    printed_column: PrintedColumnV1
    presentation_state: Literal[
        "reported_numeric",
        "reported_fixed_zero_dash",
        "reported_nil",
        "unbound_dash",
        "unbound_blank",
    ] | None
    printed_lexical: str | None
    presentation_display: PresentationDisplayV1
    binding: PresentationBindingV1 | None
    source: PresentationOccurrenceSourceV1
    calculation: PresentationCalculationV1 | None = None


class PresentationOccurrencesResultV1(_StrictWireModel):
    selector_id: str = Field(min_length=1)
    status: Literal["selected_set"]
    occurrences: tuple[FilingPresentationCellV1, ...] = Field(min_length=1)


class RoleNotPresentedResultV1(_StrictWireModel):
    selector_id: str = Field(min_length=1)
    status: Literal["role_not_presented"]
    statement_roles: tuple[
        Literal[
            "balance_sheet",
            "income_statement",
            "comprehensive_income",
            "equity",
            "cash_flow",
            "disclosure",
        ],
        ...,
    ] = Field(min_length=1)


PresentationSelectorResultV1 = Annotated[
    PresentationOccurrencesResultV1 | RoleNotPresentedResultV1,
    Field(discriminator="status"),
]


class ProducerV1(_StrictWireModel):
    id: Literal["edgar_updater"]
    version: str = Field(min_length=1)


class FilingIdentityV1(_StrictWireModel):
    ticker: str | None
    cik: str = Field(min_length=1)
    accession: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    form_type: str = Field(min_length=1)
    filing_date: str
    report_date: str
    accepted_at: str | None
    filing_index_url: str
    primary_document_url: str
    # The producer's filing FY label and how it was resolved; carried from
    # edgar_updater 8b368d1c, omitted by earlier stored captures.
    fiscal_year: int | None = None
    fiscal_year_source: Literal["companyconcept", "heuristic", "reconciled"] | None = None

    @field_validator("filing_date", "report_date")
    @classmethod
    def _validate_dates(cls, value: str, info: Any) -> str:
        return _canonical_date(value, field_name=info.field_name)

    @field_validator("accepted_at")
    @classmethod
    def _validate_accepted_at(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _canonical_utc_timestamp(value, field_name="accepted_at")


class RetrievalV1(_StrictWireModel):
    requested_fetch_policy: Literal["cache_only", "allow_fetch"]
    effective_fetch_policy: Literal["cache_only", "allow_fetch"]
    outcome: Literal["cache_hit", "fetched"]

class ConceptAttributesV1(_StrictWireModel):
    standard_label: str | None = None
    abstract: bool | None = None
    period_type: Literal["instant", "duration"] | None = None
    balance: Literal["debit", "credit"] | None = None
    is_extension: bool


class RoleIndexEntryV1(_StrictWireModel):
    role_uri: str = Field(min_length=1)
    role_name: str = Field(min_length=1)
    definition: str | None = None
    role_category: Literal["statement", "disclosure", "document"] | None = None
    statement_roles: tuple[
        Literal[
            "balance_sheet",
            "income_statement",
            "comprehensive_income",
            "equity",
            "cash_flow",
            "disclosure",
        ],
        ...,
    ]
    line_count: int = Field(ge=0)
    cell_count: int = Field(ge=0)



class EdgarFilingFactsEnvelopeV1(_StrictWireModel):

    contract_version: Literal["filing_facts.v1"]
    producer: ProducerV1
    filing: FilingIdentityV1
    retrieval: RetrievalV1
    results: tuple[PresentationSelectorResultV1, ...] = Field(min_length=1, max_length=25)
    concepts: dict[str, ConceptAttributesV1] | None = None
    roles: tuple[RoleIndexEntryV1, ...] | None = None

    @property
    def occurrences(self) -> tuple[FilingPresentationCellV1, ...]:
        return tuple(
            occurrence
            for result in self.results
            if isinstance(result, PresentationOccurrencesResultV1)
            for occurrence in result.occurrences
        )


_OCCURRENCE_ADAPTER = TypeAdapter(FilingPresentationCellV1)


def validate_presentation_occurrence_payload(
    payload: Mapping[str, Any],
) -> FilingPresentationCellV1:
    """Decode one strict occurrence row without applying its semantics."""

    try:
        raw = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        return _OCCURRENCE_ADAPTER.validate_json(raw, strict=True)
    except (TypeError, ValueError, ValidationError) as exc:
        raise EdgarFilingFactsContractError(
            f"invalid filing_facts.v1 presentation occurrence: {exc}"
        ) from exc


def validate_edgar_filing_facts_payload(
    payload: Mapping[str, Any],
    *,
    expected_accession: str | None = None,
) -> EdgarFilingFactsEnvelopeV1:
    """Decode one presentation-occurrence response envelope exactly once."""

    try:
        raw = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        envelope = EdgarFilingFactsEnvelopeV1.model_validate_json(raw, strict=True)
    except (TypeError, ValueError, ValidationError) as exc:
        raise EdgarFilingFactsContractError(
            f"invalid filing_facts.v1 response: {exc}"
        ) from exc
    if expected_accession is not None and envelope.filing.accession != expected_accession:
        raise EdgarFilingFactsContractError(
            "filing_facts.v1 response accession does not match its artifact key"
        )
    mismatched = tuple(
        row.accession for row in envelope.occurrences if row.accession != envelope.filing.accession
    )
    if mismatched:
        raise EdgarFilingFactsContractError(
            "filing_facts.v1 occurrence accession does not match envelope filing"
        )
    return envelope


__all__ = [
    "EDGAR_FILING_FACTS_CONTRACT_VERSION",
    "EDGAR_FILING_FACTS_PRODUCER_ID",
    "EdgarFilingFactsContractError",
    "EdgarFilingFactsEnvelopeV1",
    "FilingPresentationCellV1",
    "PresentationOccurrencesResultV1",
    "validate_edgar_filing_facts_payload",
    "validate_presentation_occurrence_payload",
]
