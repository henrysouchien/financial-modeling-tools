"""Thin product boundary for the deletion-first annual build-model path."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_EVEN
import json
import re
from types import MappingProxyType
from typing import (
    Annotated,
    Any,
    Final,
    Literal,
    Mapping,
    Sequence,
    TypedDict,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError
from value_semantics_core import (
    PeriodRef,
)

from . import coverage_profile as _coverage_profile
from .finding_vocabulary import (
    BuildVerdict,
    Declination,
    Finding,
    FindingScalar,
    derive_build_verdict,
    error_findings,
    uncleared_warnings,
)
from .business_model_revision import (
    BusinessModelRevision,
    BusinessModelRevisionRef,
)
from .historical_driver_seeds import (
    HistoricalDriverSeed,
    HistoricalDriverSeedRequirement,
    historical_driver_seed_denominator_sha256,
)
from schema.provider_port.adapters.edgar.wire.non_gaap_bridge_v2 import (
    NON_GAAP_METRIC_ID_PATTERN,
    IssuerNonGaapBridgeV2,
    bridge_from_v1,
    v3_gaap_metric_id,
    v3_output_metric_id,
    validate_edgar_non_gaap_bridge_source_record,
)
from schema.provider_port.adapters.edgar.wire.non_gaap_bridge_v3 import (
    EvidencedBlank,
    IssuerNonGaapBridgeV3,
    v3_operand_printed_decimal,
)
from schema.provider_port.adapters.edgar.wire.non_gaap_reconciliation_v1 import (
    VALUE_SCALE_FACTORS,
    IssuerNonGaapNetIncomeReconciliationV1,
    IssuerNonGaapReconciliationV1,
)
from .model_number_value import MissingReasonCode, UNRESOLVED_INPUT_STATES
from .model_kind_capabilities import (
    CapabilityDeclarationInvalid,
)
from .model_ref import ModelStatus, ModelVersionRef
from .model_state_identity import validate_sha256_hex
from .period_value_series import PeriodValueSeries
from .collected_claims import Claim, Reference
from .analysis import ExitEarningsMeasure
from .models import (
    FormulaSpec,
    ModelScale,
    ScenarioInputs,
)
from industry_slice_core.industry_types import Unit
from .presentation_captions import WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS
from .provider_port.contracts import (
    FilingLineObservation,
    FilingLineRef,
    FilingProvenanceGrade,
    ReportedPeriod,
    SourceFactRail,
    SourceFactRef,
    mint_source_fact_ref,
)
from .correspondence import (
    BasisDeclaration,
    CorrespondenceLink,
    DeclaredBasisBreak,
)
from .multi_basis_window import MultiBasisWindow
from .statement_topology import (
    financials_payloads_fingerprint,
    freeze_payloads_by_year,
)
from .stable_control_identity import FiscalAxis
from .thesis_shared_slice import (
    Assumption,
    FieldDisposition,
    ScalarValue,
    Valuation,
)
from industry_slice_core.thesis_shared_sources import SourceId, SourceRecord
from .ticker_contract import (
    normalize_contract_ticker,
    require_canonical_contract_ticker,
)
from .vintage_selection import (
    RestatementLineageInvalid,
    evidence_series_identity,
)


def _validate_named_sha256(field_name: str) -> Callable[[str], str]:
    """Bind a digest validator to the exact field named by its decorator."""

    def validate(value: str) -> str:
        return validate_sha256_hex(value, field_name=field_name)

    return validate


#: Every member maps to a ``FINDING_SEVERITY_MAP`` row through
#: ``schema/build_findings.py:_DIAGNOSTIC_CODE_FINDING``.
BuildDiagnosticCode = Literal[
    "missing_source",
    "source_absence_evidenced",
    "adapter_assertion",
    "incomplete_model_spec",
    "compile_failure",
    "non_gaap_bridge_rounding_gap",
    "non_gaap_bridge_mismatch",
]
AdapterAssertionKind = Literal[
    "resolves_and_agrees",
    "resolves_and_disagrees",
    "not_reported_by_issuer",
    "declared_tag_absent_alternate_present",
    "not_expressible_as_single_tag_aggregate",
    "not_expressible_as_single_tag_vendor_computed",
]
_STATEMENT_REQUIRED_ROLE_PATTERN: Final = (
    r"^statement\.(is|bs|cf)\.[a-z0-9][a-z0-9_]*$"
)
SourceCoverageDisposition = Literal[
    "observed",
    "derived",
    "aggregate_only_hidden",
    "genuinely_unreported",
    "not_applicable_evidenced",
    "not_separately_presented",
    "not_disclosed_in_vintage",
    "unknown_applicability",
    "unsupported",
]


@dataclass(frozen=True, slots=True)
class SourceMeasure:
    """What one requirement/fact unit name means on both sides of the pin.

    ``numeric_kind``/``currency`` are the filing ``Quantity`` display the
    unit reads (population checks a typed observation against them);
    ``model_unit`` is the compiled item ``Unit`` it lands on, and
    ``model_scale`` the compiled scale when the unit name fixes it (``None``:
    the requirement's or fact's own declared scale).
    """

    numeric_kind: str
    currency: str | None
    model_unit: Unit
    model_scale: ModelScale | None = None


_USD_AMOUNT: Final = SourceMeasure("currency", "USD", Unit.dollars)
#: Single owner of the requirement/fact source-measure plane: every unit name
#: the requirement denominator, the HFF fact rail and the KPI pin mint, with
#: the filing quantity it reads and the compiled item type it binds to.
#: Measure-translating consumers (requirement population, the accepted-HFF
#: fact application) read it through :func:`source_measure` — do not re-copy
#: it at a consumption site.
#:
#: ``ratio`` is a fraction: the KPI pin stores an issuer percentage as
#: ``ratio`` (``api/research/runtime_corpus.py:_KPI_MEASURES``) and a
#: ratio node's calculation computes one; it reads a
#: ``ratio`` quantity and lands on a percentage row, which holds fractions.
SOURCE_MEASURES: Final[Mapping[str, SourceMeasure]] = MappingProxyType(
    {
        "count": SourceMeasure("count", None, Unit.item_count),
        "counts": SourceMeasure("count", None, Unit.item_count),
        "units": SourceMeasure("count", None, Unit.item_count),
        "clients": SourceMeasure("count", None, Unit.item_count),
        "employees_per_client": SourceMeasure("count", None, Unit.item_count),
        "share_count": SourceMeasure("count", None, Unit.share_count),
        # The unit name folds the reported scale in: the value stays in
        # millions while the compiled share-count rows carry unit scale.
        "shares_millions": SourceMeasure(
            "count", None, Unit.share_count, "units"
        ),
        "ratio": SourceMeasure("ratio", None, Unit.percentage),
        "percentage": SourceMeasure("percentage", None, Unit.percentage),
        "pct": SourceMeasure("percentage", None, Unit.percentage),
        "multiple": SourceMeasure("multiple", None, Unit.multiple),
        "days": SourceMeasure("duration", None, Unit.days),
        "date": SourceMeasure("date", None, Unit.date),
        "dollars": _USD_AMOUNT,
        "usd": _USD_AMOUNT,
        "dollars_millions": _USD_AMOUNT,
        "usd_millions": _USD_AMOUNT,
        "usd_per_client": SourceMeasure("currency", "USD", Unit.per_count, "units"),
        "usd_per_share": SourceMeasure("per_share", "USD", Unit.per_share),
        "per_count": SourceMeasure("currency", "USD", Unit.per_count, "units"),
        "per_day": SourceMeasure("currency", "USD", Unit.per_day, "units"),
        "per_share": SourceMeasure("per_share", "USD", Unit.per_share),
    }
)


def source_measure(unit: str) -> SourceMeasure | None:
    """The measure ``unit`` names, or ``None`` when no producer mints it.

    A ``usd_<scale>`` name not listed is a USD amount whose scale rides the
    requirement's declared ``scale`` (population converts by it).
    """

    measure = SOURCE_MEASURES.get(unit)
    if measure is None and unit.startswith("usd_"):
        return _USD_AMOUNT
    return measure


#: The one ModelSpec wire version this binary writes and can decode. Stage D
#: cuts stored 2.2 specs off new work; replay of sealed pre-cut ModelVersions
#: is owned by ``model_build.stored_reads`` and does not widen this
#: canonical type.
MODEL_SPEC_SCHEMA_VERSION: Final = "3.0"
#: Generation 12 retired the registered-formula coverage and pin states: the
#: wire fields that carried them are inert, never produced.
SOURCE_BUNDLE_SCHEMA_EPOCH: Final = 12
#: One served generation: retired epochs rebuild rather than decode through a
#: second wire contract.
SUPPORTED_SOURCE_BUNDLE_SCHEMA_EPOCHS: Final = (12,)
_EDGAR_METRIC_SERIES_ARTIFACT_KIND: Final = "canonical_json_of_parsed_response"
_EDGAR_METRIC_SERIES_ARTIFACT_CODEC: Final = (
    "canonical_json_of_parsed_response.trailing_lf"
)
EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND: Final = (
    "edgar_api_financials_response_v1"
)
EDGAR_API_COVER_FACTS_RESPONSE_ARTIFACT_KIND: Final = (
    "edgar_api_cover_facts_response_v1"
)
#: The two captured edgar API responses that carry enterprise-value facts. The
#: valuation snapshot closure walks these kinds to pull in the paired capture.
EDGAR_API_EV_RESPONSE_ARTIFACT_KINDS: Final = (
    EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND,
    EDGAR_API_COVER_FACTS_RESPONSE_ARTIFACT_KIND,
)
#: The two source-id fields every EV response capture carries, naming both
#: captures of its filing. A cover-facts-sourced shares row reaches filing
#: acceptance only through the financials capture, so both ids ride on both.
EDGAR_API_EV_RESPONSE_PAIR_FIELDS: Final = (
    "financials_source_id",
    "cover_facts_source_id",
)
_EDGAR_API_EV_RESPONSE_IDENTITY: Final = {
    EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND: (
        "api_financials",
        "edgar_api:GET /api/financials",
        "financials_source_id",
    ),
    EDGAR_API_COVER_FACTS_RESPONSE_ARTIFACT_KIND: (
        "api_filing_cover_facts",
        "edgar_api:GET /api/filing/cover-facts",
        "cover_facts_source_id",
    ),
}
#: ``/api/financials`` states a display value plus a scale label; the label is
#: the power-of-ten multiplier that recovers native units. Source of truth:
#: ``edgar_parser/financials_projection.py`` ``SCALE_LABELS``.
_EDGAR_API_SCALE_EXPONENTS: Final = {
    "hundredths": -2,
    "units": 0,
    "thousands": 3,
    "millions": 6,
    "billions": 9,
}
_EDGAR_API_SCALE_FALLBACK: Final = re.compile(r"^10\^(-?\d+)$")
_EDGAR_API_SHARES_OUTSTANDING_FACT_NAME: Final = (
    "dei:EntityCommonStockSharesOutstanding"
)
#: Wording that marks a concept as holding money the issuer does not own. Cash
#: bound through such a concept would subtract client float as company cash, so
#: the bridge rejects it rather than reporting a plausible wrong number.
_PASS_THROUGH_CASH_TERMS: Final = (
    ("client", "fund"),
    ("customer", "fund"),
    ("fundsheld",),
    ("custodial",),
    ("escrow",),
    ("moneytransmission",),
)


#: The internal sentinel edgar_updater's parser fills ``axis_key`` with on an
#: undimensioned row. Since edgar_updater ``f12fe0f`` (CHANGES.md 2026-09-08,
#: "``axis_key=null``") the ``/api/financials`` projection
#: (``financials_projection._normalize_axis_key``) serves that row as ``null``;
#: caches not rebuilt since, and the 4Q derivation path (edgar_updater
#: ``docs/TODO.md`` "4Q-mode ``__NONE__`` leak check"), still serve the sentinel.
_EDGAR_API_UNDIMENSIONED_AXIS_SENTINEL: Final = "__NONE__"


def edgar_api_financials_fact_is_undimensioned(fact: Mapping[str, object]) -> bool:
    """Whether one ``/api/financials`` fact is the undimensioned line item.

    The producer encodes "no dimension" as a missing or ``null`` ``axis_key``
    and, on caches that predate the 2026-09-08 projection change, as the
    ``"__NONE__"`` sentinel; both encodings name the same fact. Every consumer
    that selects the balance-sheet line over its dimensioned duplicates asks
    here, so a further producer encoding change is one edit.
    """

    axis_key = fact.get("axis_key")
    return axis_key is None or axis_key == _EDGAR_API_UNDIMENSIONED_AXIS_SENTINEL


class SourceBundleRebuildRequired(ValueError):
    """A stored SourceBundle cannot be read by this binary's contract.

    Every stored payload this binary's contract classes reject reaches it, and
    for one reason: the contract moved after the bytes were sealed.  A retired
    ``schema_epoch``, a capability section minted under an earlier declaration
    shape (the issuer-generic plan's §7 IG-2 rollback contract — "roll forward
    or delete-and-rebuild"), and a section tightened within an epoch all have
    the same repair, which is why they carry the same refusal: rebuild the
    model and republish it.  The repair is never "fix the declaration" or
    "repair the bytes" — the bytes are intact, and the declaration this binary
    carries is the one the rebuild will write.

    It is *not* how the decode boundary reports damage.  Bytes that disagree
    with their own sealed wire digest are a different fact with a different
    repair, and ``decode_persisted_source_bundle_payload`` refuses those with a
    bare ``ValueError`` before any contract class sees them.
    """

class AdapterUnresolvedForRequiredLine(ValueError):
    """An adapter table cannot resolve one required pinned filing line."""

    code: Final = "adapter_unresolved_for_required_line"
    default_repair: Final = (
        "adapter table (map the producer native name to the required filing line)"
    )

    def __init__(
        self,
        *,
        producer: str,
        native_name: str,
        line: FilingLineRef,
        repair: str = default_repair,
    ) -> None:
        if not isinstance(producer, str) or not producer.strip():
            raise ValueError("adapter producer must be non-empty")
        if not isinstance(native_name, str) or not native_name.strip():
            raise ValueError("adapter native name must be non-empty")
        if not isinstance(line, FilingLineRef):
            raise TypeError("adapter unresolved required line must be a FilingLineRef")
        if not isinstance(repair, str) or not repair.strip():
            raise ValueError("adapter unresolved repair target must be non-empty")
        self.producer = producer
        self.native_name = native_name
        self.line = line
        self.repair = repair
        super().__init__(
            f"{self.code}: producer={producer!r} native_name={native_name!r} "
            f"line={line!r} [repair: {repair}]"
        )



class SourceBundleBindingError(ValueError):
    """A ModelSpec's digest or coverage profile does not bind to its bundle."""

    def __init__(
        self,
        *,
        expected_sha: str | None,
        actual_sha: str,
        expected_profile_id: str | None = None,
        actual_profile_id: str | None = None,
    ) -> None:
        self.expected_sha = expected_sha
        self.actual_sha = actual_sha
        self.expected_profile_id = expected_profile_id
        self.actual_profile_id = actual_profile_id
        super().__init__(
            "source bundle binding mismatch: "
            f"expected={expected_sha or '<missing>'}; actual={actual_sha}; "
            "profile="
            f"{expected_profile_id or '<missing>'}/"
            f"{actual_profile_id or '<missing>'}"
        )


_COMPILED_LITERAL_CONTROL_IDS = frozenset(
    {
        "tpl.a.header.scenario_value",
        "tpl.v.dcf.discount_period",
        "tpl.v.dcf.exit_multiple_bull",
        "tpl.v.dcf.exit_multiple_base",
        "tpl.v.dcf.exit_multiple_bear",
        "tpl.v.dcf.terminal_growth_bull",
        "tpl.v.dcf.terminal_growth_base",
        "tpl.v.dcf.terminal_growth_bear",
        "tpl.s.expected_value.bull_probability",
        "tpl.s.expected_value.base_probability",
        "tpl.s.expected_value.bear_probability",
    }
)
_WORKBOOK_METADATA_ITEM_IDS = frozenset(
    {
        *WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS,
        "tpl.s.valuation_scenarios.title",
    }
)


class _ProductContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

class AdapterAssertionOutcome(_ProductContract):
    """Nonblocking rung-1 observation of an adapter's filing-line assertion."""

    outcome: AdapterAssertionKind
    required_role: str = Field(
        min_length=1, pattern=_STATEMENT_REQUIRED_ROLE_PATTERN
    )
    period: PeriodRef
    asserted_tag: str | None = Field(default=None, min_length=1)
    filing_line: FilingLineRef | None = None
    vendor_value: Decimal | None = None
    filing_value: Decimal | None = None
    repair_target: str = Field(min_length=1)

    @field_validator("vendor_value", "filing_value", mode="before")
    @classmethod
    def _values_are_decimal(cls, value: object, info: ValidationInfo) -> object:
        if value is not None and not isinstance(value, Decimal):
            raise ValueError(f"{info.field_name} must be Decimal when present")
        return value

    @model_validator(mode="after")
    def _closed_outcome_shape(self) -> "AdapterAssertionOutcome":
        resolves_declared = self.outcome in {
            "resolves_and_agrees",
            "resolves_and_disagrees",
        }
        resolves_alternate = (
            self.outcome == "declared_tag_absent_alternate_present"
        )
        no_single_tag = self.outcome in {
            "not_expressible_as_single_tag_aggregate",
            "not_expressible_as_single_tag_vendor_computed",
        }
        filing_line = self.filing_line
        if self.vendor_value is None:
            raise ValueError("adapter assertion outcome requires the vendor value")
        if resolves_declared or resolves_alternate:
            if filing_line is None or self.filing_value is None:
                raise ValueError(
                    "resolved adapter assertion requires filing line and filing value"
                )
            if filing_line.period != self.period:
                raise ValueError(
                    "adapter assertion filing line must match the asserted period"
                )
        elif self.filing_line is not None or self.filing_value is not None:
            raise ValueError(
                "unresolved adapter assertion cannot carry a filing line or filing value"
            )
        if resolves_declared:
            if filing_line is None or self.asserted_tag != filing_line.tag:
                raise ValueError(
                    "resolved assertion tag must equal the filing line tag"
                )
        elif resolves_alternate:
            if (
                filing_line is None
                or self.asserted_tag is None
                or self.asserted_tag == filing_line.tag
            ):
                raise ValueError(
                    "alternate-tag outcome requires distinct declared and filing tags"
                )
        elif self.outcome == "not_reported_by_issuer":
            if self.asserted_tag is None:
                raise ValueError(
                    "not-reported outcome requires the declared asserted tag"
                )
        elif no_single_tag and self.asserted_tag is not None:
            raise ValueError("no-single-tag outcome cannot carry a tag claim")
        return self



class BuildDiagnostic(_ProductContract):
    code: BuildDiagnosticCode
    message: str = Field(min_length=1, max_length=1000)
    semantic_item_id: str | None = Field(default=None, max_length=200)
    period_id: str | None = Field(default=None, max_length=40)
    adapter_assertion: AdapterAssertionOutcome | None = None
    observed: Mapping[str, FindingScalar] = Field(
        default_factory=dict, exclude_if=lambda value: not value
    )

    @model_validator(mode="after")
    def _typed_payload_matches_code(self) -> "BuildDiagnostic":
        if (self.code == "adapter_assertion") != (
            self.adapter_assertion is not None
        ):
            raise ValueError(
                "adapter_assertion code and typed outcome must be present together"
            )
        if (
            self.adapter_assertion is not None
            and self.period_id is not None
            and self.period_id != self.adapter_assertion.period.canonical_label()
        ):
            raise ValueError(
                "diagnostic period_id must match adapter assertion period"
            )
        return self














#: The derivation expression of a zero an occurrence closure corroborates; the one expression
#: ``SourceEvidence`` admits without input ids.
CLOSURE_CORROBORATED_ZERO = "closure_corroborated_zero"
SourceEvidenceOrigin = Literal[
    "pinned",
    "unpinned_occurrence",
    "closure_operand",
    "basis",
]


class SourceEvidence(_ProductContract):
    """One filing-line evidence candidate with compact derivation lineage."""

    evidence_id: str = Field(min_length=1)
    issuer: str = Field(min_length=1)
    line: FilingLineRef
    grade: FilingProvenanceGrade
    period_id: str = Field(min_length=1)
    value: Decimal
    unit: str = Field(min_length=1)
    scale: str = Field(min_length=1)
    sign: str = Field(min_length=1)
    source_id: SourceId
    provider: str = Field(min_length=1)
    retrieved_at: datetime
    available_at: datetime
    as_of_date: date
    locator: str = Field(min_length=1)
    fiscal_basis: str = Field(min_length=1)
    period_length_months: int = Field(ge=1)
    accounting_definition: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    source_quality: Literal["primary", "fallback", "degraded"]
    label: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    preferred_label_role_uri: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    preferred_label_negates_value: bool | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    restates_evidence_id: str | None = None
    derivation_expression: str | None = None
    # A closure corroborated by structure alone (an abstract parent, a blank total) is a
    # derivation with no filed operand: the expression is its lineage, the structural
    # citations in ``locator`` its evidence (CLOSURE-ZERO-IS-DERIVED-WITHOUT-OPERANDS).
    derivation_input_ids: tuple[str, ...] = ()
    origin: SourceEvidenceOrigin = Field(
        default="pinned",
        exclude_if=lambda value: value == "pinned",
    )

    @property
    def source_ref(self) -> SourceFactRef:
        suffix = self.evidence_id[
            len(historical_evidence_id(self.line, self.period_id, "")):
        ]
        return mint_source_fact_ref(
            self.line, "statement", suffix=suffix or self.provider
        )

    @model_validator(mode="after")
    def _validate_derivation_shape(self) -> "SourceEvidence":
        if self.period_id != self.line.period.canonical_label():
            raise ValueError("evidence period must match filing-line period")
        if (
            bool(self.derivation_expression) != bool(self.derivation_input_ids)
            and self.derivation_expression != CLOSURE_CORROBORATED_ZERO
        ):
            raise ValueError("derived evidence requires both expression and input ids")
        if self.evidence_id in self.derivation_input_ids:
            raise ValueError("evidence cannot derive from itself")
        return self


class HistoricalReconciliationDefinitionRef(_ProductContract):
    """One exact issuer-definition location in the retained derived source."""

    source_id: SourceId
    raw_ref: str = Field(min_length=1)


class HistoricalReconciliationBinding(_ProductContract):
    """Closed issuer bridge, any metric, owned by its disclosed output fact.

    The output is the starting fact plus every signed component; each
    subtotal checks the starting fact plus the first
    ``subtotal_component_counts[i]`` components (the rows printed above it).
    The referenced facts carry the exact values in the issuer's sign; this
    binding retains only identities and the exact recomputation needed to
    reject drift.
    """

    operation: Literal["signed_sum"] = "signed_sum"
    output_metric: str = Field(min_length=1, pattern=NON_GAAP_METRIC_ID_PATTERN)
    gaap_metric: str = Field(min_length=1, pattern=NON_GAAP_METRIC_ID_PATTERN)
    raw_parent_source_ids: tuple[SourceId, ...] = Field(min_length=2, max_length=2)
    definition_refs: tuple[HistoricalReconciliationDefinitionRef, ...] = ()
    starting_fact_id: str = Field(min_length=1)
    component_fact_ids: tuple[str, ...] = Field(min_length=1)
    component_signs: tuple[Literal[1, -1], ...] = Field(min_length=1)
    subtotal_fact_ids: tuple[str, ...] = ()
    subtotal_component_counts: tuple[int, ...] = ()
    recomputed_output: Decimal
    output_residual: Decimal
    subtotal_residuals: tuple[Decimal, ...] = ()
    closure_state: Literal["exact", "rounding_consistent"] = "exact"
    half_ulp_budget: Decimal = Decimal(0)
    subtotal_half_ulp_budgets: tuple[Decimal, ...] = ()
    @model_validator(mode="before")
    @classmethod
    def _decode_two_stage_wire(cls, data: Any) -> Any:
        """Persisted SourceBundles carry the retired two-stage Adjusted EBITDA
        wire; it is this shape with one subtotal and additive components."""

        if not isinstance(data, Mapping) or data.get("operation") != "two_stage_signed_sum":
            return data
        first = tuple(data["first_stage_component_fact_ids"])
        components = (*first, *data["second_stage_component_fact_ids"])
        return {
            "operation": "signed_sum",
            "output_metric": data.get("output_metric", "adjusted_ebitda"),
            "gaap_metric": "net_income",
            "raw_parent_source_ids": data["raw_parent_source_ids"],
            "definition_refs": data["definition_refs"],
            "starting_fact_id": data["starting_fact_id"],
            "component_fact_ids": components,
            "component_signs": (1,) * len(components),
            "subtotal_fact_ids": (data["reported_subtotal_fact_id"],),
            "subtotal_component_counts": (len(first),),
            "recomputed_output": data["recomputed_output"],
            "output_residual": data["output_residual"],
            "subtotal_residuals": (data["subtotal_residual"],),
        }

    @model_validator(mode="after")
    def _closed_shape(self) -> "HistoricalReconciliationBinding":
        parent_ids = tuple(str(value) for value in self.raw_parent_source_ids)
        if len(parent_ids) != len(set(parent_ids)):
            raise ValueError("reconciliation raw parent source ids must be unique")
        definition_keys = tuple(
            (str(value.source_id), value.raw_ref) for value in self.definition_refs
        )
        if len(definition_keys) != len(set(definition_keys)):
            raise ValueError("reconciliation definition refs must be unique")
        if len(self.component_signs) != len(self.component_fact_ids):
            raise ValueError("reconciliation component signs must parallel components")
        if not (
            len(self.subtotal_fact_ids)
            == len(self.subtotal_component_counts)
            == len(self.subtotal_residuals)
        ):
            raise ValueError("reconciliation subtotal fields must be parallel")
        if any(
            count < 0 or count > len(self.component_fact_ids)
            for count in self.subtotal_component_counts
        ) or list(self.subtotal_component_counts) != sorted(
            self.subtotal_component_counts
        ):
            raise ValueError("reconciliation subtotals must follow table order")
        referenced_fact_ids = (
            self.starting_fact_id,
            *self.component_fact_ids,
            *self.subtotal_fact_ids,
        )
        if len(referenced_fact_ids) != len(set(referenced_fact_ids)):
            raise ValueError("reconciliation fact references must be unique")
        for field_name, value in (
            ("recomputed_output", self.recomputed_output),
            ("output_residual", self.output_residual),
            ("half_ulp_budget", self.half_ulp_budget),
            *(("subtotal_residuals", value) for value in self.subtotal_residuals),
            *(
                ("subtotal_half_ulp_budgets", value)
                for value in self.subtotal_half_ulp_budgets
            ),
        ):
            if not value.is_finite():
                raise ValueError(f"{field_name} must be finite")
        if self.half_ulp_budget < 0 or any(
            b < 0 for b in self.subtotal_half_ulp_budgets
        ):
            raise ValueError("reconciliation half_ulp_budget must be non-negative")
        if self.closure_state == "exact":
            if self.output_residual != 0 or any(self.subtotal_residuals):
                raise ValueError("mapped historical reconciliation must close exactly")
        else:
            if len(self.subtotal_half_ulp_budgets) != len(self.subtotal_residuals):
                raise ValueError(
                    "reconciliation subtotal half-ulp budgets must parallel subtotal residuals"
                )
            if self.output_residual == 0 and not any(self.subtotal_residuals):
                raise ValueError(
                    "rounding_consistent reconciliation requires a non-zero residual"
                )
            if abs(self.output_residual) > self.half_ulp_budget or any(
                abs(sr) > sb
                for sr, sb in zip(
                    self.subtotal_residuals,
                    self.subtotal_half_ulp_budgets,
                    strict=True,
                )
            ):
                raise ValueError(
                    "mapped historical reconciliation must close within half-ulp rounding budget"
                )
        return self

    @property
    def dependency_fact_ids(self) -> tuple[str, ...]:
        """The starting fact, then components and subtotals in table order."""

        rows: list[str] = [self.starting_fact_id]
        taken = 0
        for subtotal_id, count in zip(
            self.subtotal_fact_ids, self.subtotal_component_counts, strict=True
        ):
            rows.extend(self.component_fact_ids[taken:count])
            rows.append(subtotal_id)
            taken = count
        rows.extend(self.component_fact_ids[taken:])
        return tuple(rows)

    # Readers that list a binding's dependencies as
    # ``(starting, *first_stage, subtotal, *second_stage)`` read these three
    # views; their concatenation after ``starting_fact_id`` is exactly
    # ``dependency_fact_ids[1:]`` (with no subtotal, the last component stands
    # in the subtotal slot).
    @property
    def first_stage_component_fact_ids(self) -> tuple[str, ...]:
        return self.dependency_fact_ids[1 : _first_stage_split(self)]

    @property
    def reported_subtotal_fact_id(self) -> str:
        return self.dependency_fact_ids[_first_stage_split(self)]

    @property
    def second_stage_component_fact_ids(self) -> tuple[str, ...]:
        return self.dependency_fact_ids[_first_stage_split(self) + 1 :]


def _first_stage_split(binding: HistoricalReconciliationBinding) -> int:
    if binding.subtotal_component_counts:
        return 1 + binding.subtotal_component_counts[0]
    return len(binding.component_fact_ids)


def _historical_id_component(value: str) -> str:
    """Keep filed dots distinct from ID separators and escape literal markers."""
    return value.replace("%", "%25").replace(".", "%2E")


def historical_fact_id(line: FilingLineRef, period_id: str, suffix: str) -> str:
    """Mint an exact line id, preserving undotted parent-era component bytes."""
    if period_id == line.period.canonical_label():
        return mint_source_fact_ref(line, "statement", suffix=suffix).fact_id
    prefix = (
        f"fact.{line.accession}.{period_id}.{line.statement_role}."
        f"{_historical_id_component(line.tag)}."
    )
    if line.dimensions:
        dims = ";".join(
            f"{_historical_id_component(dimension.axis_qname)}="
            f"{_historical_id_component(dimension.member_qname)}"
            for dimension in line.dimensions
        )
        prefix += f"{dims}."
    return f"{prefix}{suffix}"


def historical_evidence_id(line: FilingLineRef, period_id: str, suffix: str) -> str:
    """Mint the evidence id paired with the exact filing-line fact id."""
    if period_id == line.period.canonical_label():
        return mint_source_fact_ref(line, "statement", suffix=suffix).evidence_id
    return historical_fact_id(line, period_id, suffix).replace("fact.", "evidence.", 1)


class HistoricalBasisFact(_ProductContract):
    """Digest-bound filing-line fact used as an assumption basis, not coverage."""

    fact_id: str = Field(min_length=1)
    line: FilingLineRef
    grade: FilingProvenanceGrade
    period_id: str = Field(min_length=1)
    value: Decimal
    unit: str = Field(default="usd_millions", min_length=1)
    scale: str = Field(default="millions", min_length=1)
    sign: str = Field(default="natural", min_length=1)
    source_id: SourceId
    provider: str = Field(default="edgar", min_length=1)
    retrieved_at: datetime
    available_at: datetime
    as_of_date: date
    locator: str = Field(min_length=1)
    fiscal_basis: str = Field(default="issuer_fiscal_year", min_length=1)
    period_length_months: int = Field(default=12, ge=1)
    accounting_definition: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    source_quality: Literal["primary", "fallback", "degraded"] = "primary"
    required_role_correspondence: str | None = Field(
        default=None,
        min_length=1,
        pattern=_coverage_profile.ANNUAL_REQUIRED_ROLE_PATTERN,
        exclude_if=lambda value: value is None,
    )
    reconciliation: HistoricalReconciliationBinding | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @property
    def source_ref(self) -> SourceFactRef:
        rail: SourceFactRail = (
            "non_gaap_bridge"
            if self.line.statement_role == "nongaapreconciliation"
            else "allocated_sbc"
            if self.accounting_definition == "allocated_share_based_compensation"
            else "lease_flow"
            if self.accounting_definition in {"lease_cash_paid", "rou_additions"}
            else "schedule_disclosure"
            if self.accounting_definition
            in {
                "acquired_intangible_amortization",
                "goodwill_acquired",
                "acquisition_cash_paid",
            }
            else "statement"
        )
        suffix = self.fact_id[len(historical_fact_id(self.line, self.period_id, "")):]
        return mint_source_fact_ref(self.line, rail, suffix=suffix or None)

    @model_validator(mode="after")
    def _closed_identity(self) -> "HistoricalBasisFact":
        if self.period_id != self.line.period.canonical_label():
            raise ValueError("historical-basis period must match filing-line period")
        # Escaped line components make this boundary exact even when the opaque
        # producer suffix contains dots; parsing that suffix cannot recover identity.
        expected_prefix = historical_fact_id(self.line, self.period_id, "")
        if (
            not self.fact_id.startswith(expected_prefix)
            or "=" in self.fact_id[len(expected_prefix):]
        ):
            raise ValueError(
                "historical-basis fact identity must match its filing line"
            )
        if not self.value.is_finite():
            raise ValueError("historical-basis fact value must be finite")
        return self


# The measures an issuer bridge's facts carry on the historical rail: a
# currency amount in natural USD millions (the fact's field defaults), a
# per-share amount in natural USD per share.
_RAIL_CURRENCY_MEASURE: Final[tuple[str, str]] = (
    HistoricalBasisFact.model_fields["unit"].default,
    HistoricalBasisFact.model_fields["scale"].default,
)
_RAIL_PER_SHARE_MEASURE: Final[tuple[str, str]] = ("usd_per_share", "units")
HISTORICAL_RECONCILIATION_MEASURES: Final[frozenset[tuple[str, str]]] = frozenset(
    {_RAIL_CURRENCY_MEASURE, _RAIL_PER_SHARE_MEASURE}
)


def historical_bridge_rail_measure(
    bridge: IssuerNonGaapBridgeV2
    | IssuerNonGaapBridgeV3
    | IssuerNonGaapReconciliationV1
    | IssuerNonGaapNetIncomeReconciliationV1,
) -> tuple[str, str, Decimal]:
    """``(unit, scale, factor)`` an issuer bridge's rows land on the rail with.

    A per-share bridge prints dollars per share whatever scale its table
    declares for amounts ("in thousands, except per share data"); every other
    family is a currency amount converted from its value scale to millions.
    """

    if isinstance(
        bridge, (IssuerNonGaapReconciliationV1, IssuerNonGaapNetIncomeReconciliationV1)
    ):
        bridge = bridge_from_v1(bridge)

    if bridge.metric_family == "per_share":
        return (*_RAIL_PER_SHARE_MEASURE, Decimal(1))
    unit, scale = _RAIL_CURRENCY_MEASURE
    return (
        unit,
        scale,
        VALUE_SCALE_FACTORS[bridge.value_scale] / VALUE_SCALE_FACTORS[scale],
    )


def validate_historical_reconciliation_bindings(
    facts: Sequence[HistoricalBasisFact],
    *,
    source_ids: Sequence[SourceId] | None = None,
    allow_reconciliations: bool = True,
) -> None:
    """Validate closed fact dependencies without introducing another fact rail."""

    rows = tuple(facts)
    facts_by_id = {row.fact_id: row for row in rows}
    if len(facts_by_id) != len(rows):
        raise ValueError("historical-basis fact ids must be unique")
    bound_outputs = tuple(row for row in rows if row.reconciliation is not None)
    if bound_outputs and not allow_reconciliations:
        raise ValueError(
            "historical reconciliation binding requires a versioned container upgrade"
        )
    retained_source_ids = (
        None if source_ids is None else {str(value) for value in source_ids}
    )

    output_keys = set()
    for output in bound_outputs:
        binding = output.reconciliation
        assert binding is not None
        if len(binding.raw_parent_source_ids) != 2:
            raise ValueError(
                "historical reconciliation requires filing and accepted-at parents"
            )
        if (
            output.unit,
            output.scale,
        ) not in HISTORICAL_RECONCILIATION_MEASURES or output.sign != "natural":
            raise ValueError(
                "historical reconciliation requires natural USD millions or USD per share"
            )
        output_key = evidence_series_identity(output)
        if output_key in output_keys:
            raise ValueError(
                "historical reconciliation output must be unique by filing line and period"
            )
        output_keys.add(output_key)
        reference_ids = binding.dependency_fact_ids
        if output.fact_id in reference_ids:
            raise ValueError("historical reconciliation output cannot reference itself")
        try:
            referenced = tuple(facts_by_id[value] for value in reference_ids)
        except KeyError as exc:
            raise ValueError(
                "historical reconciliation fact reference must resolve"
            ) from exc
        if any(row.reconciliation is not None for row in referenced):
            raise ValueError("historical reconciliations cannot nest")
        for row in referenced:
            if (
                row.period_id,
                row.unit,
                row.scale,
                row.sign,
                row.provider,
                row.as_of_date,
                row.fiscal_basis,
                row.period_length_months,
            ) != (
                output.period_id,
                output.unit,
                output.scale,
                output.sign,
                output.provider,
                output.as_of_date,
                output.fiscal_basis,
                output.period_length_months,
            ):
                raise ValueError(
                    "historical reconciliation facts must share period and measure"
                )
            if row.available_at > output.available_at:
                raise ValueError(
                    "historical reconciliation input cannot postdate output availability"
                )
            if row.retrieved_at > output.retrieved_at:
                raise ValueError(
                    "historical reconciliation input cannot postdate output retrieval"
                )
        exact_direct_source_ids = {
            *(str(row.source_id) for row in (*referenced, output)),
            *(str(value.source_id) for value in binding.definition_refs),
        }
        if len(exact_direct_source_ids) != 1:
            raise ValueError(
                "historical reconciliation facts and definitions require one derived source"
            )
        required_source_ids = {
            *exact_direct_source_ids,
            *(str(value) for value in binding.raw_parent_source_ids),
        }
        if (
            retained_source_ids is not None
            and not required_source_ids <= retained_source_ids
        ):
            raise ValueError(
                "historical reconciliation direct and raw parent sources must resolve"
            )

        base = facts_by_id[binding.starting_fact_id].value
        signed = tuple(
            sign * facts_by_id[fact_id].value
            for fact_id, sign in zip(
                binding.component_fact_ids, binding.component_signs, strict=True
            )
        )
        recomputed = base + sum(signed, Decimal(0))
        subtotal_residuals = tuple(
            facts_by_id[fact_id].value - (base + sum(signed[:count], Decimal(0)))
            for fact_id, count in zip(
                binding.subtotal_fact_ids, binding.subtotal_component_counts, strict=True
            )
        )
        output_residual = output.value - recomputed
        if binding.closure_state == "exact":
            if output_residual != 0 or any(subtotal_residuals):
                raise ValueError("historical reconciliation must close exactly")
        else:
            if abs(output_residual) > binding.half_ulp_budget or any(
                abs(sr) > sb
                for sr, sb in zip(
                    subtotal_residuals,
                    binding.subtotal_half_ulp_budgets,
                    strict=True,
                )
            ):
                raise ValueError(
                    "historical reconciliation must close within half-ulp rounding budget"
                )
        if (
            binding.recomputed_output,
            binding.output_residual,
            binding.subtotal_residuals,
        ) != (recomputed, output_residual, subtotal_residuals):
            raise ValueError(
                "historical reconciliation retained computation differs from exact facts"
            )

def _epoch_permits_reconciliation_bindings(epoch: int) -> bool:
    """The filing-native generation carries the surviving reconciliation rail."""

    return epoch in SUPPORTED_SOURCE_BUNDLE_SCHEMA_EPOCHS


def validate_historical_reconciliation_source_bindings(
    facts: Sequence[HistoricalBasisFact],
    sources: Sequence[SourceRecord],
    *,
    allow_reconciliations: bool,
) -> None:
    """Validate the derived Edgar DTO source and its two retained raw parents."""

    source_by_id = {str(row.id): row for row in sources}
    if len(source_by_id) != len(sources):
        raise ValueError("historical reconciliation source IDs must be unique")
    validate_historical_reconciliation_bindings(
        facts,
        source_ids=tuple(source_by_id),
        allow_reconciliations=allow_reconciliations,
    )
    facts_by_id = {row.fact_id: row for row in facts}

    def retrieved_at(source: SourceRecord) -> datetime:
        try:
            parsed = datetime.fromisoformat(
                str(source.retrieved_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                "historical reconciliation source time is invalid"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError("historical reconciliation source time must be aware UTC")
        return parsed

    for output in facts:
        binding = output.reconciliation
        if binding is None:
            continue
        direct_ids = {
            str(output.source_id),
            *(
                str(facts_by_id[value].source_id)
                for value in binding.dependency_fact_ids
            ),
            *(str(value.source_id) for value in binding.definition_refs),
        }
        if len(direct_ids) != 1:
            raise ValueError("historical reconciliation derived source is ambiguous")
        derived_id = next(iter(direct_ids))
        try:
            derived = source_by_id[derived_id]
        except KeyError as exc:
            raise ValueError(
                "historical reconciliation derived source must resolve"
            ) from exc
        bridge, locator, raw_parent_ids = validate_edgar_non_gaap_bridge_source_record(
            derived
        )
        if tuple(map(str, binding.raw_parent_source_ids)) != raw_parent_ids:
            raise ValueError("historical reconciliation raw parent order is invalid")
        try:
            filing_parent = source_by_id[raw_parent_ids[0]]
            accepted_parent = source_by_id[raw_parent_ids[1]]
        except KeyError as exc:
            raise ValueError(
                "historical reconciliation raw parent must resolve"
            ) from exc
        source = bridge.source
        accepted_evidence = source.accepted_at_evidence
        filing_fields = filing_parent.key_fields or {}
        accepted_fields = accepted_parent.key_fields or {}
        if (
            filing_parent.provider != "edgar"
            or filing_parent.endpoint_or_filing_id != source.accession
            or filing_fields.get("artifact_kind") != source.artifact_kind
            or filing_fields.get("artifact_sha256") != source.document_sha256
            or filing_fields.get("document_byte_count") != source.document_byte_count
            or filing_fields.get("decompressed_document_sha256")
            != source.decompressed_document_sha256
            or filing_fields.get("decompressed_document_byte_count")
            != source.decompressed_document_byte_count
            or filing_fields.get("accession") != source.accession
            or filing_fields.get("provider_artifact_locator") != source.artifact_locator
            or filing_fields.get("source_url") != source.document_url
        ):
            raise ValueError("historical reconciliation filing parent is invalid")
        if (
            accepted_parent.provider != "edgar"
            or accepted_parent.endpoint_or_filing_id != source.accession
            or accepted_fields.get("artifact_kind") != accepted_evidence.artifact_kind
            or accepted_fields.get("artifact_sha256")
            != accepted_evidence.document_sha256
            or accepted_fields.get("document_byte_count")
            != accepted_evidence.document_byte_count
            or accepted_fields.get("decompressed_document_sha256")
            != accepted_evidence.decompressed_document_sha256
            or accepted_fields.get("decompressed_document_byte_count")
            != accepted_evidence.decompressed_document_byte_count
            or accepted_fields.get("accession") != source.accession
            or accepted_fields.get("provider_artifact_locator")
            != accepted_evidence.artifact_locator
        ):
            raise ValueError("historical reconciliation accepted-at parent is invalid")
        if any(
            retrieved_at(parent) < source.accepted_at
            for parent in (filing_parent, accepted_parent, derived)
        ):
            raise ValueError("historical reconciliation custody predates acceptance")
        if any(
            retrieved_at(parent) > retrieved_at(derived)
            for parent in (filing_parent, accepted_parent)
        ):
            raise ValueError(
                "historical reconciliation parent custody postdates derivation"
            )
        expected_period_id = f"FY{bridge.period.fiscal_year}"
        if isinstance(bridge, IssuerNonGaapBridgeV3):
            if bridge.period.period_type != "annual":
                raise ValueError("historical reconciliation requires an annual bridge")
            output_metric_id = v3_output_metric_id(bridge)
            gaap_metric_id = v3_gaap_metric_id(bridge)
            closure_state = bridge.closure.state
            half_ulp_budget = bridge.closure.half_ulp_budget
            numeric_subtotals = tuple(
                subtotal
                for subtotal in bridge.subtotals
                if not isinstance(subtotal.value, EvidencedBlank)
            )
            subtotal_half_ulp_budgets = tuple(
                budget
                for subtotal, budget in zip(
                    bridge.subtotals,
                    bridge.closure.subtotal_half_ulp_budgets,
                    strict=True,
                )
                if not isinstance(subtotal.value, EvidencedBlank)
            )
            numeric_components: tuple[Any, ...] = tuple(
                component
                for component in bridge.components
                if not isinstance(component.value, EvidencedBlank)
            )
        else:
            if bridge.period.period_type != "FY":
                raise ValueError("historical reconciliation requires an annual bridge")
            output_metric_id = bridge.output_metric_id
            gaap_metric_id = bridge.gaap_metric_id
            closure_state = bridge.closure_state
            half_ulp_budget = bridge.half_ulp_budget
            subtotal_half_ulp_budgets = bridge.subtotal_half_ulp_budgets
            numeric_components = tuple(bridge.components)
            numeric_subtotals = tuple(bridge.subtotals)
        if (
            binding.output_metric,
            binding.gaap_metric,
            binding.component_signs,
            binding.closure_state,
        ) != (
            output_metric_id,
            gaap_metric_id,
            tuple(component.sign for component in numeric_components),
            closure_state,
        ):
            raise ValueError(
                "historical reconciliation binding differs from its provider bridge"
            )
        provider_lines = (
            bridge.gaap_base,
            *numeric_components,
            *numeric_subtotals,
            bridge.disclosed_output,
        )

        def _line_result_key(line: Any) -> str:
            if isinstance(bridge, IssuerNonGaapBridgeV3):
                return line.cell.result_key
            return line.locator.result_key

        def _line_printed_value(line: Any) -> Decimal:
            if isinstance(bridge, IssuerNonGaapBridgeV3):
                return v3_operand_printed_decimal(bridge, line)
            return Decimal(line.value)

        lines_by_locator = {
            f"{locator}#{_line_result_key(line)}": line for line in provider_lines
        }
        referenced_facts = (
            *(facts_by_id[value] for value in binding.dependency_fact_ids),
            output,
        )
        if {row.locator for row in referenced_facts} != set(lines_by_locator):
            raise ValueError(
                "historical reconciliation filing lines differ from provider rows"
            )
        unit, scale, to_rail = historical_bridge_rail_measure(bridge)
        if binding.closure_state == "rounding_consistent" and (
            binding.half_ulp_budget,
            binding.subtotal_half_ulp_budgets,
        ) != (
            Decimal(half_ulp_budget) * to_rail,
            tuple(Decimal(v) * to_rail for v in subtotal_half_ulp_budgets),
        ):
            raise ValueError(
                "historical reconciliation binding differs from its provider bridge"
            )
        for fact in referenced_facts:
            line = lines_by_locator[fact.locator]
            if (
                fact.period_id != expected_period_id
                or (fact.unit, fact.scale) != (unit, scale)
                or fact.value != _line_printed_value(line) * to_rail
                or fact.available_at != source.accepted_at
                or fact.retrieved_at != retrieved_at(derived)
                or fact.as_of_date != bridge.period.period_end
                or fact.locator != f"{locator}#{_line_result_key(line)}"
                or fact.provider != "edgar"
                or fact.fiscal_basis != "issuer_fiscal_year"
                or fact.period_length_months != 12
                or fact.source_quality != "primary"
                or fact.confidence != "high"
                or fact.accounting_definition
                != (
                    f"Issuer-disclosed {bridge.output_metric_label} bridge row: "
                    f"{line.issuer_label}"
                )
            ):
                raise ValueError(
                    "historical reconciliation fact differs from exact provider row"
                )
        expected_definition_refs = tuple(
            f"{locator}#{row.result_key}"
            for row in (
                *(() if bridge.definition is None else (bridge.definition,)),
                *bridge.footnotes,
            )
        )
        if (
            tuple(row.raw_ref for row in binding.definition_refs)
            != expected_definition_refs
        ):
            raise ValueError("historical reconciliation definition refs are incomplete")


class AnnualPeriodCoverage(_ProductContract):
    period_id: str = Field(min_length=1)
    disposition: SourceCoverageDisposition
    evidence_ids: tuple[str, ...] = ()
    # Inert wire fields until the next HFF/snapshot epoch (ruling R6).
    registered_formula_id: str | None = Field(default=None, min_length=1)
    formula_input_roles: tuple[str, ...] = ()
    rationale: str = Field(min_length=1)
    repair_target: str | None = None
    disclosure_search_basis: str | None = None
    disclosure_search_source_id: SourceId | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    disclosure_search_accession: str | None = Field(
        default=None,
        min_length=1,
        exclude_if=lambda value: value is None,
    )
    included_in: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    absence_citation: _coverage_profile.AbsenceCitation | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    applicability_evidence_ids: tuple[str, ...] = ()
    conflict_evidence_ids: tuple[str, ...] = ()
    aggregate_parent_requirement_id: str | None = None
    degraded: bool = False

    @classmethod
    def declared_not_disclosed_in_vintage(
        cls, *, period_id: str, required_role: str
    ) -> "AnnualPeriodCoverage":
        """The period of a ``statement_bindings.v2`` ``unbound`` cell whose reason is
        ``not_disclosed_in_vintage``: the agent read the multi-filing matrix and declared no
        captured filing prints the role for the period. A historical absence the issuer owes
        nothing for (``schema.role_disposition`` reads the same cell as evidenced absence),
        never ``unknown_applicability``; every producer of coverage from a cell builds it here."""

        return cls(
            period_id=period_id,
            disposition="not_disclosed_in_vintage",
            rationale=(
                "The statement_bindings.v2 row declares no captured filing prints "
                f"{required_role} for {period_id} (not_disclosed_in_vintage)."
            ),
        )

    @model_validator(mode="after")
    def _validate_disposition_shape(self) -> "AnnualPeriodCoverage":
        if self.disposition in {"observed", "derived"} and not self.evidence_ids:
            raise ValueError("supported disposition requires evidence")
        if self.disposition == "derived" and len(self.evidence_ids) != 1:
            raise ValueError("derived disposition requires one derived evidence")
        if self.disposition == "aggregate_only_hidden" and (
            not self.evidence_ids or not self.aggregate_parent_requirement_id
        ):
            raise ValueError(
                "aggregate_only_hidden requires aggregate evidence and parent"
            )
        has_search_source = self.disclosure_search_source_id is not None
        has_search_accession = self.disclosure_search_accession is not None
        if has_search_source != has_search_accession:
            raise ValueError(
                "disclosure search source and accession must be declared together"
            )
        if has_search_source and self.disposition not in {
            "genuinely_unreported",
            "not_applicable_evidenced",
            "not_separately_presented",
            "unknown_applicability",
        }:
            raise ValueError(
                "only non-numeric absence coverage carries disclosure search source"
            )
        source_backed_search = has_search_source and has_search_accession
        if self.disposition == "genuinely_unreported" and (
            not self.disclosure_search_basis
            or (not self.evidence_ids and not source_backed_search)
        ):
            raise ValueError(
                "genuinely_unreported requires a numeric or source-backed search basis"
            )
        if self.disposition == "not_applicable_evidenced" and (
            not self.applicability_evidence_ids and not source_backed_search
        ):
            raise ValueError("non-applicability requires evidence")
        if self.disposition == "not_separately_presented" and (
            not self.included_in
            or self.absence_citation is None
            or not source_backed_search
            or self.evidence_ids
            or self.absence_citation.accession != self.disclosure_search_accession
        ):
            raise ValueError("included coverage requires its exact cited source and no numeric evidence")
        if self.disposition in {"unknown_applicability", "unsupported"} and (
            not self.repair_target
        ):
            raise ValueError("blocking disposition requires a repair target")
        if self.conflict_evidence_ids and len(self.conflict_evidence_ids) < 2:
            raise ValueError("a conflict requires at least two visible candidates")
        return self


class AnnualRequirementCoverage(_ProductContract):
    requirement_id: str = Field(min_length=1)
    periods: tuple[AnnualPeriodCoverage, ...] = Field(min_length=1)


ValuationIssuer = str
ValuationShortfallReason = Literal[
    "missing",
    "stale_close",
    "wrong_close_date",
    "definition_nonconforming",
    "mapping_nonconforming",
    "cutoff_violation",
    "currency_nonconforming",
    "basis_unreconciled",
    "not_separable",
]
ValuationEligibilityReason = Literal[
    "missing",
    "stale_close",
    "wrong_close_date",
    "definition_nonconforming",
    "mapping_nonconforming",
    "cutoff_violation",
    "currency_nonconforming",
    "basis_unreconciled",
    "not_separable",
    "horizon_nonconforming",
    "unit_nonconforming",
    "value_nonconforming",
    "source_nonconforming",
    "duplicate_conforming_observation",
    "peer_roster_empty",
]
_DenominatorVerification = Literal[
    "issuer_reconciled_components",
    "consensus_non_gaap_not_issuer_reconciled",
]
VALUATION_MULTIPLE_QUANTUM: Final = Decimal("0.000001")
_VALUATION_DEFINITION_SCOPE: Final[dict[tuple[str, str], tuple[str, str]]] = {
    ("fmp", "fwd_pe"): ("analyst_estimates", "epsAvg"),
    ("fmp", "fwd_ev_adj_ebitda"): ("analyst_estimates", "ebitdaAvg"),
}
_SHORTFALL_REASON_ORDER: dict[ValuationShortfallReason, int] = {
    value: index
    for index, value in enumerate(
        (
            "missing",
            "stale_close",
            "wrong_close_date",
            "definition_nonconforming",
            "mapping_nonconforming",
            "cutoff_violation",
            "currency_nonconforming",
            "basis_unreconciled",
            "not_separable",
        )
    )
}


class ValuationDefinitionEvidence(_ProductContract):
    provider_doc_source_id: SourceId
    provider_doc_artifact_sha256: str
    provider_endpoint: str = Field(min_length=1, max_length=128)
    provider_field: str = Field(min_length=1, max_length=128)
    verbatim_definition_text: str = Field(min_length=1, max_length=1000)
    stated_basis: Literal["non_gaap", "adjusted", "gaap", "unstated"]
    provider_basis_scope: (
        Literal["issuer_defined_adjusted_ebitda", "consensus_non_gaap_ebitda"] | None
    ) = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    provider_currency_semantics: Literal["issuer_reporting_currency"] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    provider_numeric_scale: Literal["currency_units"] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    _sha = field_validator("provider_doc_artifact_sha256")(
        lambda value: validate_sha256_hex(
            value, field_name="provider_doc_artifact_sha256"
        )
    )


class EstimatePeriodEvidence(_ProductContract):
    raw_provider_period: str = Field(min_length=1, max_length=100)
    mapping_rule: Literal[
        "provider_annual_period_end_exact_match",
        "provider_period_within_issuer_fy",
        "provider_stated_fy_match",
    ]
    provider_statement_semantics: (
        Literal["issuer_fiscal_year_labels", "calendar_year_labels", "unstated"] | None
    ) = None
    verbatim_statement_text: str | None = Field(default=None, max_length=1000)
    statement_artifact_sha256: str | None = None

    @model_validator(mode="after")
    def _statement_shape(self) -> "EstimatePeriodEvidence":
        values = (
            self.provider_statement_semantics,
            self.verbatim_statement_text,
            self.statement_artifact_sha256,
        )
        if self.mapping_rule == "provider_stated_fy_match":
            if any(value is None for value in values):
                raise ValueError("stated-FY mapping requires all statement facts")
            validate_sha256_hex(
                self.statement_artifact_sha256 or "",
                field_name="statement_artifact_sha256",
            )
        elif any(value is not None for value in values):
            raise ValueError("interval mapping forbids provider-statement facts")
        return self


class FiscalPeriodFacts(_ProductContract):
    issuer: str
    basis: str = Field(min_length=1)
    period_start: date
    period_end: date
    source_artifact_sha256: str
    source_id: SourceId | None = None
    period_type: Literal["annual"] | None = None
    derivation_rule: (
        Literal["adjacent_provider_annual_period_ends", "reported_annual_duration"]
        | None
    ) = None
    source_locators: tuple[str, ...] = Field(default=(), max_length=8)
    available_at: datetime | None = None

    @field_validator("source_artifact_sha256")
    @classmethod
    def _fiscal_sha(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="source_artifact_sha256")

    @field_validator("issuer", mode="before")
    @classmethod
    def _issuer(cls, value: object) -> str:
        return normalize_contract_ticker(value)

    @model_validator(mode="after")
    def _ordered_interval(self) -> "FiscalPeriodFacts":
        if self.period_start > self.period_end:
            raise ValueError("fiscal period start must not follow end")
        sourced_values = (
            self.source_id,
            self.period_type,
            self.derivation_rule,
            self.available_at,
        )
        legacy_shape = all(value is None for value in sourced_values) and not (
            self.source_locators
        )
        sourced_shape = all(value is not None for value in sourced_values) and bool(
            self.source_locators
        )
        if not legacy_shape and not sourced_shape:
            raise ValueError(
                "fiscal sourced evidence requires source_id, annual period_type, "
                "derivation_rule, source_locators, and available_at together"
            )
        if any(not locator or len(locator) > 2048 for locator in self.source_locators):
            raise ValueError("fiscal source locators must be nonblank and bounded")
        if self.available_at is not None and (
            self.available_at.tzinfo is None
            or self.available_at.utcoffset() != timedelta(0)
        ):
            raise ValueError("fiscal available_at must be aware UTC")
        return self

    @property
    def has_exact_annual_source_evidence(self) -> bool:
        return (
            self.source_id is not None
            and self.period_type == "annual"
            and self.derivation_rule is not None
            and bool(self.source_locators)
            and self.available_at is not None
        )


class NtmReportedPeriodAnchor(_ProductContract):
    """The latest filing-reported period available before the pricing cutoff."""

    period_start: date
    period_end: date
    period_type: Literal["FY", "Q1", "Q2", "Q3", "Q4"]
    source_id: SourceId
    selection_source_ids: tuple[SourceId, ...] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    source_provider: str = Field(min_length=1, max_length=128)
    native_source_id: str = Field(min_length=1, max_length=256)
    source_available_on: date
    source_locator: str = Field(min_length=1, max_length=2048)
    source_accepted_at: datetime

    @model_validator(mode="after")
    def _reported_period_shape(self) -> "NtmReportedPeriodAnchor":
        if self.selection_source_ids is not None:
            if self.selection_source_ids != tuple(
                sorted(set(self.selection_source_ids))
            ):
                raise ValueError(
                    "reported period selection source ids must be canonical"
                )
            if self.source_id not in self.selection_source_ids:
                raise ValueError(
                    "reported period source_id must belong to selection sources"
                )
        if self.period_start >= self.period_end:
            raise ValueError(
                "reported period anchor requires period_start < period_end"
            )
        if self.source_available_on < self.period_end:
            raise ValueError("reported period cannot be available before its end")
        if (
            self.source_accepted_at.tzinfo is None
            or self.source_accepted_at.utcoffset() != timedelta(0)
        ):
            raise ValueError("reported period anchor accepted_at must be aware UTC")
        if self.source_accepted_at.date() < self.period_end:
            raise ValueError("reported period cannot be accepted before its end")
        # No ordering between availability and acceptance dates is asserted:
        # EDGAR dates a filing accepted after 17:30 ET with the next business
        # day, so `filing_date` legitimately follows the acceptance date.
        return self


class NtmQuarterlyEpsOperand(_ProductContract):
    """One provider-native quarterly EPS operand in an exact NTM horizon."""

    period_start: date
    period_end: date
    eps: Decimal
    analyst_count: int | None = Field(default=None, ge=0)
    estimate_period_end_ref: str = Field(min_length=1, max_length=2048)
    eps_ref: str = Field(min_length=1, max_length=2048)
    analyst_count_ref: str | None = Field(default=None, min_length=1, max_length=2048)
    fiscal_source_id: SourceId
    fiscal_boundary_rule: Literal[
        "reported_quarter_duration",
        "adjacent_provider_quarter_period_ends",
    ]
    fiscal_period_start_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=2048,
    )
    fiscal_predecessor_period_end: date | None = None
    fiscal_predecessor_period_end_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=2048,
    )
    fiscal_period_end_ref: str = Field(min_length=1, max_length=2048)

    @model_validator(mode="after")
    def _quarter_shape(self) -> "NtmQuarterlyEpsOperand":
        if self.period_start >= self.period_end:
            raise ValueError("quarterly EPS operand requires period_start < period_end")
        if not self.eps.is_finite():
            raise ValueError("quarterly EPS operand must be finite")
        if (self.analyst_count is None) != (self.analyst_count_ref is None):
            raise ValueError(
                "quarterly EPS analyst_count and analyst_count_ref must be present together"
            )
        if self.fiscal_boundary_rule == "reported_quarter_duration":
            if self.fiscal_period_start_ref is None or any(
                value is not None
                for value in (
                    self.fiscal_predecessor_period_end,
                    self.fiscal_predecessor_period_end_ref,
                )
            ):
                raise ValueError(
                    "reported quarter duration requires an exact start ref only"
                )
        else:
            if self.fiscal_period_start_ref is not None or any(
                value is None
                for value in (
                    self.fiscal_predecessor_period_end,
                    self.fiscal_predecessor_period_end_ref,
                )
            ):
                raise ValueError(
                    "adjacent quarter ends require predecessor end value and ref"
                )
            assert self.fiscal_predecessor_period_end is not None
            if self.period_start != self.fiscal_predecessor_period_end + timedelta(
                days=1
            ):
                raise ValueError(
                    "adjacent quarter start must immediately follow predecessor end"
                )
        return self


class NtmForwardPeOperands(_ProductContract):
    """Source-addressable operands for one exact next-four-quarter P/E."""

    reported_anchor: NtmReportedPeriodAnchor
    official_close_date_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=2048,
        exclude_if=lambda value: value is None,
    )
    quarterly_eps: tuple[NtmQuarterlyEpsOperand, ...] = Field(
        min_length=4,
        max_length=4,
    )

    @model_validator(mode="after")
    def _canonical_quarters(self) -> "NtmForwardPeOperands":
        if (
            self.reported_anchor.selection_source_ids is None
            or len(self.reported_anchor.selection_source_ids) != 2
        ):
            raise ValueError(
                "NTM reported anchor requires annual and quarterly selection sources"
            )
        canonical = tuple(
            sorted(
                self.quarterly_eps,
                key=lambda item: (item.period_start, item.period_end, item.eps_ref),
            )
        )
        if self.quarterly_eps != canonical:
            raise ValueError("NTM quarterly EPS operands must be in canonical order")
        if len({item.eps_ref for item in self.quarterly_eps}) != 4:
            raise ValueError("NTM quarterly EPS refs must be unique")
        if (
            len({item.period_start for item in self.quarterly_eps}) != 4
            or len({item.period_end for item in self.quarterly_eps}) != 4
        ):
            raise ValueError("NTM quarterly EPS period boundaries must be unique")
        return self


class ForwardEvAdjustedEbitdaOperands(_ProductContract):
    """Exact source-addressable operands for forward EV/adjusted EBITDA."""

    reported_anchor: NtmReportedPeriodAnchor
    estimate_period_end_ref: str = Field(min_length=1, max_length=2048)
    adjusted_ebitda: Decimal
    #: Exactly the four universal arithmetic terms — price, shares, debt, cash —
    #: plus one id per declared adjustment row the issuer's artifact presents.
    #: The shared bridge owns the closure: it fails closed unless the four core
    #: component types resolve exactly once and every remaining row is a
    #: declared adjustment whose stated effect it can honour.
    enterprise_value_component_ids: tuple[str, ...] = Field(min_length=4)

    @model_validator(mode="after")
    def _closed_operands(self) -> "ForwardEvAdjustedEbitdaOperands":
        if (
            self.reported_anchor.selection_source_ids is None
            or len(self.reported_anchor.selection_source_ids) != 2
        ):
            raise ValueError(
                "forward adjusted EBITDA anchor requires annual and quarterly sources"
            )
        if not self.adjusted_ebitda.is_finite() or self.adjusted_ebitda <= 0:
            raise ValueError("forward adjusted EBITDA denominator must be positive")
        if self.enterprise_value_component_ids != tuple(
            sorted(set(self.enterprise_value_component_ids))
        ):
            raise ValueError(
                "forward adjusted EBITDA EV component ids must be canonical"
            )
        return self


class NtmQuarterlyAdjustedEbitdaOperand(_ProductContract):
    """One provider-native quarterly consensus non-GAAP EBITDA operand."""

    period_start: date
    period_end: date
    adjusted_ebitda: Decimal
    analyst_count: int | None = Field(default=None, ge=0)
    estimate_period_end_ref: str = Field(min_length=1, max_length=2048)
    adjusted_ebitda_ref: str = Field(min_length=1, max_length=2048)
    analyst_count_ref: str | None = Field(default=None, min_length=1, max_length=2048)
    fiscal_source_id: SourceId
    fiscal_boundary_rule: Literal["adjacent_provider_quarter_period_ends"]
    fiscal_predecessor_period_end: date
    fiscal_predecessor_period_end_ref: str = Field(min_length=1, max_length=2048)
    fiscal_period_end_ref: str = Field(min_length=1, max_length=2048)

    @model_validator(mode="after")
    def _quarter_shape(self) -> "NtmQuarterlyAdjustedEbitdaOperand":
        if self.period_start >= self.period_end:
            raise ValueError(
                "quarterly adjusted EBITDA operand requires period_start < period_end"
            )
        if not self.adjusted_ebitda.is_finite() or self.adjusted_ebitda <= 0:
            raise ValueError("quarterly adjusted EBITDA operand must be positive")
        if (self.analyst_count is None) != (self.analyst_count_ref is None):
            raise ValueError(
                "quarterly adjusted EBITDA analyst_count and analyst_count_ref "
                "must be present together"
            )
        if self.period_start != self.fiscal_predecessor_period_end + timedelta(days=1):
            raise ValueError(
                "adjacent quarter start must immediately follow predecessor end"
            )
        return self


class NtmForwardEvAdjustedEbitdaOperands(_ProductContract):
    """Source-addressable operands for one exact next-four-quarter EV/EBITDA."""

    reported_anchor: NtmReportedPeriodAnchor
    quarterly_adjusted_ebitda: tuple[NtmQuarterlyAdjustedEbitdaOperand, ...] = Field(
        min_length=4,
        max_length=4,
    )
    #: The four universal arithmetic terms plus one id per declared adjustment
    #: row; see ForwardEvAdjustedEbitdaOperands for where closure is owned.
    enterprise_value_component_ids: tuple[str, ...] = Field(min_length=4)

    @model_validator(mode="after")
    def _canonical_quarters(self) -> "NtmForwardEvAdjustedEbitdaOperands":
        if (
            self.reported_anchor.selection_source_ids is None
            or len(self.reported_anchor.selection_source_ids) != 2
        ):
            raise ValueError(
                "NTM reported anchor requires annual and quarterly selection sources"
            )
        canonical = tuple(
            sorted(
                self.quarterly_adjusted_ebitda,
                key=lambda item: (
                    item.period_start,
                    item.period_end,
                    item.adjusted_ebitda_ref,
                ),
            )
        )
        if self.quarterly_adjusted_ebitda != canonical:
            raise ValueError(
                "NTM quarterly adjusted EBITDA operands must be in canonical order"
            )
        if (
            len({item.adjusted_ebitda_ref for item in self.quarterly_adjusted_ebitda})
            != 4
        ):
            raise ValueError("NTM quarterly adjusted EBITDA refs must be unique")
        if (
            len({item.period_start for item in self.quarterly_adjusted_ebitda}) != 4
            or len({item.period_end for item in self.quarterly_adjusted_ebitda}) != 4
        ):
            raise ValueError(
                "NTM quarterly adjusted EBITDA period boundaries must be unique"
            )
        if self.enterprise_value_component_ids != tuple(
            sorted(set(self.enterprise_value_component_ids))
        ):
            raise ValueError(
                "NTM forward adjusted EBITDA EV component ids must be canonical"
            )
        return self


class MarketCalendarFacts(_ProductContract):
    calendar_source_artifact_sha256: str
    completed_session_dates: tuple[date, ...] = Field(min_length=1)
    bond_session_dates: tuple[date, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )

    @field_validator("calendar_source_artifact_sha256")
    @classmethod
    def _calendar_sha(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="calendar_source_artifact_sha256")

    @model_validator(mode="after")
    def _canonical_sessions(self) -> "MarketCalendarFacts":
        if self.completed_session_dates != tuple(
            sorted(set(self.completed_session_dates))
        ):
            raise ValueError("completed sessions must be sorted and unique")
        if self.bond_session_dates != tuple(sorted(set(self.bond_session_dates))):
            raise ValueError("bond sessions must be sorted and unique")
        return self




DeclaredEnterpriseValueAdjustmentEffect = Literal[
    "disclosure_only",
    "adds_to_enterprise_value",
    "subtracts_from_enterprise_value",
]


class DeclaredEnterpriseValueAdjustmentLeg(_ProductContract):
    """One source-bound gross leg of a declared enterprise-value adjustment.

    ``sign`` is declared data. It is never inferred from leg order: a wrong sign
    yields a wrong enterprise value that still looks plausible.
    """

    role_label: str = Field(min_length=1)
    value: Decimal
    ref: str = Field(min_length=1)
    sign: Literal["add", "subtract"]


class DeclaredEnterpriseValueAdjustment(_ProductContract):
    """One company-specific balance declared beside the universal EV terms.

    The role label and every leg label are DATA carried up from the issuer
    artifact, so a new adjustment — custodial float, escrow, money-transmission
    balances, insurance float, a pension deficit, minority interest — is
    expressible without touching valuation infrastructure. ``effect`` is the
    whole arithmetic contract: a row either names the direction in which it
    enters the enterprise-value bridge or declares itself disclosure-only, and
    the bridge honours exactly what the row states.
    """

    role_label: str = Field(min_length=1)
    effect: DeclaredEnterpriseValueAdjustmentEffect
    legs: tuple[DeclaredEnterpriseValueAdjustmentLeg, ...] = Field(min_length=1)
    net_value: Decimal

    @model_validator(mode="after")
    def _exact_net(self) -> "DeclaredEnterpriseValueAdjustment":
        labels = tuple(leg.role_label for leg in self.legs)
        if len(set(labels)) != len(labels):
            raise ValueError("declared adjustment legs must carry distinct roles")
        refs = tuple(leg.ref for leg in self.legs)
        if len(set(refs)) != len(refs):
            raise ValueError("declared adjustment legs must bind distinct refs")
        net = sum(
            (leg.value if leg.sign == "add" else -leg.value for leg in self.legs),
            Decimal(0),
        )
        if self.net_value != net:
            raise ValueError(
                "declared adjustment net must equal the exact signed sum of its legs"
            )
        return self



class EnterpriseValueSignedRef(_ProductContract):
    """One fact a composite cash or debt component sums, with the sign it enters.

    A single added line does not carry this: it binds ``raw_ref`` alone. A
    subtracted line, or more than one stated fact, must name every ref and the
    direction it enters the sum the validator checks.
    """

    ref: str = Field(min_length=1)
    sign: Literal[1, -1]

    @field_validator("sign", mode="before")
    @classmethod
    def _sign_is_unit(cls, value: object) -> object:
        if type(value) is not int or value not in (1, -1):
            raise ValueError("signed EV ref sign must be the integer 1 or -1")
        return value


@dataclass(frozen=True)
class _OfficialCloseBinding:
    issuer: str
    market_date: date
    close_ref: str


class ValuationObservation(_ProductContract):
    observation_id: str = Field(min_length=1)
    issuer: ValuationIssuer | None
    metric: _coverage_profile.ValuationMetric
    fiscal_basis: _coverage_profile.ValuationFiscalBasis
    value: Decimal
    unit: str = Field(min_length=1)
    as_of_close: date
    retrieved_at: datetime
    provider: str = Field(min_length=1)
    source_id: SourceId
    raw_ref: str = Field(min_length=1)
    raw_market_date: date | None = None
    raw_close: Decimal | None = None
    raw_close_source_id: SourceId | None = None
    raw_close_ref: str | None = Field(default=None, min_length=1)
    # The valuation capture's identity (the digest over its artifact rows), never
    # the corpus manifest digest.
    snapshot_manifest_sha256: str
    definition_evidence: ValuationDefinitionEvidence | None = None
    estimate_period_evidence: EstimatePeriodEvidence | None = None
    ntm_operands: NtmForwardPeOperands | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    forward_adjusted_ebitda_operands: ForwardEvAdjustedEbitdaOperands | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    ntm_forward_adjusted_ebitda_operands: (
        NtmForwardEvAdjustedEbitdaOperands | None
    ) = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    denominator_component_verification: _DenominatorVerification | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    component_type: (
        Literal["price", "shares", "debt", "cash", "declared_adjustment"] | None
    ) = None
    component_as_of: date | None = None
    currency: str | None = None
    basis: str | None = None
    shares_reported_at: date | None = None
    split_events_between: tuple[str, ...] = ()
    split_history_source_id: SourceId | None = None
    split_history_ref: str | None = Field(default=None, min_length=1)
    currency_source_id: SourceId | None = None
    currency_ref: str | None = Field(default=None, min_length=1)
    declared_adjustment: DeclaredEnterpriseValueAdjustment | None = None
    separability: Literal["separable", "not_separable", "not_applicable"] | None = None
    derivation_expression: str | None = None
    derivation_input_ids: tuple[str, ...] = ()
    #: Concepts an issuer's declared selection lists that the filing states as
    #: zero. The owning API omits zero-valued facts from its projection, so the
    #: component contributes 0 for them; naming them here keeps that assumption
    #: in the artifact instead of losing it.
    assumed_zero_qnames: tuple[str, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    #: Contributing facts of a composite cash or debt role, each with the sign
    #: it enters the sum. Absent on a single added line, which stays the
    #: one-ref component it was.
    signed_refs: tuple[EnterpriseValueSignedRef, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )

    @field_validator("snapshot_manifest_sha256")
    @classmethod
    def _manifest_sha(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="snapshot_manifest_sha256")

    @field_validator("issuer", mode="before")
    @classmethod
    def _issuer(cls, value: object) -> str | None:
        if value is None:
            return None
        return normalize_contract_ticker(value)

    @model_validator(mode="after")
    def _observation_shape(self) -> "ValuationObservation":
        macro = self.metric in {"risk_free_10y", "erp"}
        if macro != (self.issuer is None):
            raise ValueError("macro observations require issuer=None exclusively")
        expected_component = (
            self.metric.removeprefix("ev_component:")
            if self.metric.startswith("ev_component:")
            else None
        )
        if self.component_type != expected_component:
            raise ValueError("EV component type must match its metric")
        if (self.split_history_source_id is None) != (self.split_history_ref is None):
            raise ValueError("split-history source and ref must be present together")
        if (self.currency_source_id is None) != (self.currency_ref is None):
            raise ValueError("currency source and ref must be present together")
        if self.component_type == "shares":
            if any(
                value is None
                for value in (
                    self.component_as_of,
                    self.shares_reported_at,
                    self.split_history_source_id,
                    self.split_history_ref,
                )
            ):
                raise ValueError("EV shares require dated source-bound split evidence")
            if self.component_as_of != self.shares_reported_at:
                raise ValueError(
                    "EV shares component date must equal reported share date"
                )
        elif self.metric == "fwd_pe" and self.fiscal_basis == "next_four_quarters":
            if self.shares_reported_at is not None:
                raise ValueError("only EV shares may carry a reported share date")
        elif (
            any(
                value is not None
                for value in (
                    self.shares_reported_at,
                    self.split_history_source_id,
                    self.split_history_ref,
                )
            )
            or self.split_events_between
        ):
            raise ValueError("only EV shares may carry split evidence")
        if self.component_type == "declared_adjustment":
            adjustment = self.declared_adjustment
            if adjustment is None or self.value != adjustment.net_value:
                raise ValueError(
                    "declared EV adjustment requires its exact declared net"
                )
            if self.separability != "separable":
                raise ValueError(
                    "declared EV adjustment must be explicitly separable from the "
                    "universal enterprise-value terms"
                )
        elif self.declared_adjustment is not None:
            raise ValueError(
                "only a declared EV adjustment component may carry declared "
                "adjustment evidence"
            )
        if self.component_type is not None:
            if self.component_as_of is None:
                raise ValueError("EV components require an exact component date")
            if self.component_type != "shares" and (
                self.currency_source_id is None or self.currency is None
            ):
                raise ValueError("monetary EV components require source-bound currency")
            if tuple(sorted(set(self.assumed_zero_qnames))) != (
                self.assumed_zero_qnames
            ):
                raise ValueError(
                    "EV component assumed-zero qnames must be sorted and distinct"
                )
        elif self.assumed_zero_qnames:
            raise ValueError(
                "only an EV component may declare assumed-zero qnames"
            )
        if self.signed_refs and self.component_type not in {"debt", "cash"}:
            raise ValueError(
                "only a monetary EV component may carry signed refs"
            )
        if self.signed_refs:
            refs = tuple(item.ref for item in self.signed_refs)
            if len(set(refs)) != len(refs):
                raise ValueError("signed EV refs must be distinct")
            if self.raw_ref != refs[0]:
                raise ValueError(
                    "monetary EV raw_ref must be its first signed ref"
                )
            if len(self.signed_refs) == 1 and self.signed_refs[0].sign == 1:
                raise ValueError(
                    "a single added EV line binds raw_ref alone"
                )
        if bool(self.derivation_expression) != bool(self.derivation_input_ids):
            raise ValueError("valuation derivation requires expression and inputs")
        is_forward = self.metric in {"fwd_pe", "fwd_ev_adj_ebitda"}
        if is_forward and any(
            value is None
            for value in (self.raw_close, self.raw_close_source_id, self.raw_close_ref)
        ):
            raise ValueError(
                "forward valuation requires raw close value, source_id, and ref"
            )
        if not is_forward and (
            (self.raw_close_source_id is None) != (self.raw_close_ref is None)
        ):
            raise ValueError(
                "valuation raw close source_id and ref must be present together"
            )
        if self.definition_evidence is not None:
            expected_definition_scope = _VALUATION_DEFINITION_SCOPE.get(
                (self.provider, self.metric)
            )
            observed_definition_scope = (
                self.definition_evidence.provider_endpoint,
                self.definition_evidence.provider_field,
            )
            if observed_definition_scope != expected_definition_scope:
                raise ValueError(
                    "valuation definition evidence scope must match its forward metric"
                )
        if self.ntm_operands is not None and (
            self.metric != "fwd_pe" or self.fiscal_basis != "next_four_quarters"
        ):
            raise ValueError(
                "NTM operands require forward P/E with next_four_quarters basis"
            )
        if self.ntm_operands is not None and self.raw_market_date is None:
            raise ValueError("valuation NTM requires an exact raw market date")
        if self.ntm_operands is not None and self.estimate_period_evidence is not None:
            raise ValueError(
                "NTM operands cannot carry singular annual estimate-period evidence"
            )
        if (
            self.metric == "fwd_pe"
            and self.fiscal_basis == "next_four_quarters"
            and self.ntm_operands is None
        ):
            raise ValueError(
                "next_four_quarters forward P/E requires exact NTM operands"
            )
        ev_operands = self.forward_adjusted_ebitda_operands
        ntm_ev_operands = self.ntm_forward_adjusted_ebitda_operands
        if (
            ev_operands is not None or ntm_ev_operands is not None
        ) and self.metric != "fwd_ev_adj_ebitda":
            raise ValueError(
                "forward adjusted EBITDA operands require fwd_ev_adj_ebitda"
            )
        if ev_operands is not None and ntm_ev_operands is not None:
            raise ValueError(
                "forward EV/adjusted EBITDA carries exactly one operand shape"
            )
        if ntm_ev_operands is not None and self.fiscal_basis != "next_four_quarters":
            raise ValueError(
                "NTM forward adjusted EBITDA operands require next_four_quarters basis"
            )
        if self.metric == "fwd_ev_adj_ebitda":
            definition = self.definition_evidence
            if self.fiscal_basis == "next_four_quarters":
                if ntm_ev_operands is None:
                    raise ValueError(
                        "next_four_quarters forward EV/adjusted EBITDA requires "
                        "exact NTM operands"
                    )
                if self.estimate_period_evidence is not None:
                    raise ValueError(
                        "NTM forward EV/adjusted EBITDA cannot carry singular "
                        "annual estimate-period evidence"
                    )
            else:
                if ev_operands is None:
                    raise ValueError(
                        "forward EV/adjusted EBITDA requires exact typed operands"
                    )
                if self.estimate_period_evidence is None:
                    raise ValueError(
                        "forward EV/adjusted EBITDA requires estimate-period evidence"
                    )
            if definition is None or (
                definition.stated_basis,
                definition.provider_basis_scope,
                definition.provider_currency_semantics,
                definition.provider_numeric_scale,
            ) != (
                "non_gaap",
                "consensus_non_gaap_ebitda",
                None,
                None,
            ):
                raise ValueError(
                    "forward EV/adjusted EBITDA requires the consensus non-GAAP "
                    "provider basis and no purported currency or scale semantics"
                )
            if (
                self.currency != "USD"
                or self.currency_source_id is None
                or self.currency_ref is None
                or self.basis != "consensus_non_gaap_ebitda"
            ):
                raise ValueError(
                    "forward EV/adjusted EBITDA requires source-bound USD "
                    "consensus non-GAAP EBITDA basis"
                )
            if (
                self.denominator_component_verification
                != "consensus_non_gaap_not_issuer_reconciled"
            ):
                raise ValueError(
                    "forward EV/adjusted EBITDA must declare its denominator "
                    "components are consensus non-GAAP and not issuer-reconciled"
                )
        elif self.denominator_component_verification is not None:
            raise ValueError(
                "only forward EV/adjusted EBITDA carries a denominator "
                "component verification class"
            )
        return self


class ValuationObservationVerdict(_ProductContract):
    observation_id: str = Field(min_length=1)
    coverage_reasons: tuple[ValuationShortfallReason, ...] = ()
    eligibility_reasons: tuple[ValuationEligibilityReason, ...] = ()

    @property
    def coverage_conforming(self) -> bool:
        return not self.coverage_reasons

    @property
    def selection_eligible(self) -> bool:
        return not self.coverage_reasons and not self.eligibility_reasons


class RelativeSelectionRejected(_ProductContract):
    observation_id: str = Field(min_length=1)
    reasons: tuple[ValuationEligibilityReason, ...] = Field(min_length=1)


class RelativeSelectionEligibilityShortfall(_ProductContract):
    scope_key: str = Field(min_length=1)
    reason: ValuationEligibilityReason



_METHOD_DENOMINATOR_VERIFICATION: Final[
    dict[str, _DenominatorVerification | None]
] = {
    "forward_pe": None,
    "ev_adjusted_ebitda": "consensus_non_gaap_not_issuer_reconciled",
}


class RelativeSelectionResolution(_ProductContract):
    method: Literal["forward_pe", "ev_adjusted_ebitda"]
    fiscal_basis: str = Field(min_length=1)
    status: Literal["resolved", "blocked"]
    selected_value: Decimal | None = None
    as_of_close: date | None = None
    focal_observation_ids: tuple[str, ...] = ()
    contributing_ids: tuple[str, ...] = ()
    rejected: tuple[RelativeSelectionRejected, ...] = ()
    shortfall: tuple[ValuationShortfall, ...] = ()
    eligibility_shortfall: tuple[RelativeSelectionEligibilityShortfall, ...] = ()
    denominator_verification: _DenominatorVerification | None = None

    @model_validator(mode="after")
    def _resolution_shape(self) -> "RelativeSelectionResolution":
        if self.status == "resolved":
            if self.selected_value is None or not self.contributing_ids:
                raise ValueError("resolved selection requires value and contributors")
            if self.shortfall or self.eligibility_shortfall:
                raise ValueError("resolved selection cannot carry shortfall")
            if self.denominator_verification != _METHOD_DENOMINATOR_VERIFICATION[
                self.method
            ]:
                raise ValueError(
                    "resolved selection must carry its method's exact denominator "
                    "component verification class"
                )
        else:
            if self.selected_value is not None or self.contributing_ids:
                raise ValueError("blocked selection cannot carry a selected value")
            if self.denominator_verification is not None:
                raise ValueError(
                    "blocked selection cannot claim a denominator verification class"
                )
        return self


class CoveredMarketInput(_ProductContract):
    requirement_id: str = Field(min_length=1)
    field_name: Literal[
        "stock_price", "raw_beta", "risk_free_rate", "equity_risk_premium"
    ]
    status: Literal["resolved", "blocked"]
    value: Decimal | None = None
    observation_id: str | None = None
    source_id: SourceId | None = None
    reasons: tuple[ValuationEligibilityReason, ...] = ()

    @model_validator(mode="after")
    def _market_input_shape(self) -> "CoveredMarketInput":
        populated = self.value is not None and self.observation_id is not None
        if (self.status == "resolved") != populated:
            raise ValueError("resolved market input requires one value and observation")
        return self


class CoveredMarketInputs(_ProductContract):
    stock_price: CoveredMarketInput
    raw_beta: CoveredMarketInput
    risk_free_rate: CoveredMarketInput
    equity_risk_premium: CoveredMarketInput


class ValuationMethodEvidenceStatus(_ProductContract):
    method: Literal["forward_pe", "ev_adjusted_ebitda"]
    status: Literal["resolved", "blocked"]
    fiscal_basis: str = Field(min_length=1)
    selected_value: Decimal | None = None
    as_of_close: date | None = None
    contributing_ids: tuple[str, ...] = ()
    rejected: tuple[RelativeSelectionRejected, ...] = ()
    shortfall: tuple[ValuationShortfall, ...] = ()
    eligibility_shortfall: tuple[RelativeSelectionEligibilityShortfall, ...] = ()
    denominator_verification: _DenominatorVerification | None = None

    @model_validator(mode="after")
    def _verification_class(self) -> "ValuationMethodEvidenceStatus":
        expected = (
            _METHOD_DENOMINATOR_VERIFICATION[self.method]
            if self.status == "resolved"
            else None
        )
        if self.denominator_verification != expected:
            raise ValueError(
                "valuation method status must carry its exact denominator "
                "component verification class"
            )
        return self


class ValuationEvidenceStatus(_ProductContract):
    kind: Literal["valuation_evidence_status"] = "valuation_evidence_status"
    bundle_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    methods: tuple[ValuationMethodEvidenceStatus, ...] = Field(
        min_length=2, max_length=2
    )

    @model_validator(mode="after")
    def _method_set(self) -> "ValuationEvidenceStatus":
        if {item.method for item in self.methods} != {
            "forward_pe",
            "ev_adjusted_ebitda",
        }:
            raise ValueError("valuation evidence status requires both methods")
        return self


class ValuationEvidenceStatusNotRecorded(_ProductContract):
    kind: Literal["pre_slice6_not_recorded"] = "pre_slice6_not_recorded"


ValuationEvidenceStatusArtifact = (
    ValuationEvidenceStatus | ValuationEvidenceStatusNotRecorded
)


class ScheduleInputOrigin(_ProductContract):
    kind: Literal["control", "evidence", "observation", "valuation"]
    reference: str = Field(min_length=1)


class ScheduleInputPeriodAudit(_ProductContract):
    period: int
    state: Literal["known", "missing", "invalid", "unsupported", "evidenced_absent"]
    reason: str | None = None


ScheduleInputFamily = Literal[
    "capitalized_software",
    "deferred_contract_cost",
    "lease",
    "debt",
    "share_count_capital_allocation",
    "working_capital",
]


class ScheduleInputAuditRow(_ProductContract):
    family: ScheduleInputFamily
    row_id: str = Field(min_length=1)
    public_label: str = Field(min_length=1)
    state: Literal["resolvable", "missing"]
    periods: tuple[ScheduleInputPeriodAudit, ...] = Field(min_length=1)
    missing_periods: tuple[int, ...] = ()
    origins: tuple[ScheduleInputOrigin, ...] = ()

    @model_validator(mode="after")
    def _truthful_state(self) -> "ScheduleInputAuditRow":
        missing = tuple(
            item.period for item in self.periods
            if item.state in UNRESOLVED_INPUT_STATES
        )
        if self.missing_periods != missing:
            raise ValueError("missing_periods must match unresolved period outcomes")
        if (self.state == "resolvable") != (not missing):
            raise ValueError("row state must reflect all required period outcomes")
        return self


class ScheduleInputAudit(_ProductContract):
    kind: Literal["schedule_input_audit"] = "schedule_input_audit"
    schema_version: Literal["1.0"] = "1.0"
    rows: tuple[ScheduleInputAuditRow, ...] = ()

    @model_validator(mode="after")
    def _unique_rows(self) -> "ScheduleInputAudit":
        identities = tuple((row.family, row.row_id) for row in self.rows)
        if len(identities) != len(set(identities)):
            raise ValueError("schedule audit rows must be unique by family and row")
        return self


class ScheduleInputAuditNotRecorded(_ProductContract):
    kind: Literal["pre_slice7_not_recorded"] = "pre_slice7_not_recorded"


ScheduleInputAuditArtifact = ScheduleInputAudit | ScheduleInputAuditNotRecorded


class ValuationShortfall(_ProductContract):
    scope_key: str = Field(min_length=1)
    reason: ValuationShortfallReason


class ValuationCoverage(_ProductContract):
    requirement_id: str = Field(min_length=1)
    status: Literal["covered", "uncovered"]
    observation_ids: tuple[str, ...] = ()
    shortfall: tuple[ValuationShortfall, ...] = ()

    @model_validator(mode="after")
    def _status_shape(self) -> "ValuationCoverage":
        if (self.status == "covered") != (not self.shortfall):
            raise ValueError("valuation shortfall must be empty iff covered")
        return self


class EvidenceAuditRecord(_ProductContract):
    requirement_id: str = Field(min_length=1)
    required_role: str = Field(
        min_length=1, pattern=_coverage_profile.ANNUAL_REQUIRED_ROLE_PATTERN
    )
    line: FilingLineRef | None
    period_id: str = Field(min_length=1)
    disposition: SourceCoverageDisposition
    evidence_ids: tuple[str, ...] = ()
    search_sources: tuple[SourceId, ...] = ()
    downstream_capability: str = Field(min_length=1)
    #: What the bundle writer composed, uncapped: an unbound row's note joins
    #: every authored unbound reason its windows carry, and the prepare pager,
    #: not this record, bounds the wire.
    note: str = Field(min_length=1)

    @model_validator(mode="after")
    def _line_matches_period(self) -> "EvidenceAuditRecord":
        if self.line is not None and self.line.period.canonical_label() != self.period_id:
            raise ValueError("audit filing line must match its fiscal period")
        return self


DEC08_VALUATION_REQUIREMENTS: Final[tuple[_coverage_profile.ValuationRequirement, ...]] = (
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.fwd_pe.fy2026e",
        metric="fwd_pe",
        fiscal_basis="fy2026e",
        roster_rule="focal_and_peers",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.fwd_pe.fy2027e",
        metric="fwd_pe",
        fiscal_basis="fy2027e",
        roster_rule="focal_and_peers",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.fwd_ev_adj_ebitda.fy2026e",
        metric="fwd_ev_adj_ebitda",
        fiscal_basis="fy2026e",
        roster_rule="focal_and_peers",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.fwd_ev_adj_ebitda.fy2027e",
        metric="fwd_ev_adj_ebitda",
        fiscal_basis="fy2027e",
        roster_rule="focal_and_peers",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.quote",
        metric="quote",
        fiscal_basis="point_in_time",
        roster_rule="single_observation",
        scope="focal",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.beta",
        metric="beta",
        fiscal_basis="point_in_time",
        roster_rule="single_observation",
        scope="focal",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.risk_free_10y",
        metric="risk_free_10y",
        fiscal_basis="point_in_time",
        roster_rule="single_observation",
        scope="macro",
    ),
    _coverage_profile.ValuationRequirement(
        requirement_id="dec08.erp",
        metric="erp",
        fiscal_basis="point_in_time",
        roster_rule="single_observation",
        scope="macro",
    ),
)


class ArtifactLocation(_ProductContract):
    location: str = Field(min_length=1)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="sha256")


class EvidenceLineageReference(_ProductContract):
    """Evidence-level lineage retained alongside the canonical SourceId."""

    evidence_id: str = Field(min_length=1)
    source_id: SourceId
    issuer: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    locator: str = Field(min_length=1)
    retrieved_at: datetime
    as_of_date: date
    confidence: Literal["high", "medium", "low"]
    derivation_input_ids: tuple[str, ...] = ()
    accession: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    # The filing presentation role the line sits in: a GAAP primary statement,
    # ``nongaapreconciliation``, or an operating-KPI role — the reported basis.
    statement_role: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    tag: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    preferred_label_text: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    preferred_label_role_uri: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    preferred_label_negates_value: bool | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class AssumptionLineageReference(_ProductContract):
    """Human-inspectable authored judgment used by one rendered cell."""

    assumption_id: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    confidence: str | None = None
    evidence_ids: tuple[str, ...] = ()
    source_refs: tuple[SourceId, ...] = ()


class HistoricalDriverSeedLineageReference(_ProductContract):
    """Server-owned accepted-HFF origin for one compiled historical level."""

    historical_foundation_artifact_id: str = Field(pattern=r"^hff_[0-9a-f]{32}$")
    historical_foundation_content_sha256: str
    historical_driver_seed_denominator_sha256: str
    driver_id: str = Field(pattern=r"^drv_[0-9a-f]{32}$")
    driver_key: str = Field(min_length=1)
    period_id: str = Field(min_length=1)
    disposition: Literal[
        "observed",
        "derived",
        "proxy",
        "analyst_normalized",
    ]
    derivation_kind: Literal["direct", "arithmetic_mean", "allocation"]
    derivation_rationale: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    limitations: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    supporting_evidence_ids: tuple[str, ...] = ()
    evidence_value_ids: tuple[str, ...] = ()
    fact_ids: tuple[str, ...] = ()

    _historical_foundation_content_digest = field_validator(
        "historical_foundation_content_sha256"
    )(_validate_named_sha256("historical_foundation_content_sha256"))
    _historical_driver_seed_denominator_digest = field_validator(
        "historical_driver_seed_denominator_sha256"
    )(_validate_named_sha256("historical_driver_seed_denominator_sha256"))

    @field_validator(
        "limitations",
        "evidence_ids",
        "supporting_evidence_ids",
        "evidence_value_ids",
        "fact_ids",
    )
    @classmethod
    def _seed_refs_canonical(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))) or any(not item for item in value):
            raise ValueError("historical driver seed lineage refs must be canonical")
        return value


class ValuationLineageReference(_ProductContract):
    observation_id: str = Field(min_length=1)
    source_id: SourceId
    raw_ref: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    as_of_close: date
    raw_market_date: date | None = None
    # The valuation capture's identity, as on `ValuationObservation`.
    snapshot_manifest_sha256: str

    @field_validator("snapshot_manifest_sha256")
    @classmethod
    def _snapshot_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="snapshot_manifest_sha256")


_CellLineageStatus = Literal[
    "complete",
    "missing_source",
    "presentation",
    "compiler_derived",
    "source_absence_evidenced",
    MissingReasonCode,
]


class _CellLineageCore(_ProductContract):
    sheet: str = Field(min_length=1)
    cell: str = Field(pattern=r"^[A-Z]+[1-9][0-9]*$")
    semantic_item_id: str = Field(min_length=1)
    formula: FormulaSpec | None = None
    observation_ids: tuple[str, ...] = ()
    assumption_ids: tuple[str, ...] = ()
    source_refs: tuple[SourceId, ...] = ()
    evidence_refs: tuple[EvidenceLineageReference, ...] = ()
    assumption_refs: tuple[AssumptionLineageReference, ...] = ()
    valuation_refs: tuple[ValuationLineageReference, ...] = ()


def _require_cell_lineage_origin(
    value: _CellLineageCore,
    *,
    lineage_status: _CellLineageStatus,
    historical_driver_seed_refs: tuple[
        HistoricalDriverSeedLineageReference, ...
    ] = (),
) -> None:
    if (
        value.formula is None
        and not value.observation_ids
        and not value.assumption_ids
        and not value.source_refs
        and not value.evidence_refs
        and not value.assumption_refs
        and not value.valuation_refs
        and not historical_driver_seed_refs
        and lineage_status == "complete"
    ):
        raise ValueError(
            "cell lineage requires a formula, observation, or assumption"
        )


class CellLineageV1_0(_CellLineageCore):
    lineage_status: Literal["complete", "missing_source", "presentation"] = "complete"

    @model_validator(mode="after")
    def _require_lineage_origin(self) -> "CellLineageV1_0":
        _require_cell_lineage_origin(self, lineage_status=self.lineage_status)
        return self


class CompilerOrigin(_ProductContract):
    """Closed compiler-owned origin taxonomy for deterministic cells."""

    kind: Literal[
        "discount_convention",
        "scenario_control",
        "mechanical_constant",
        "presentation_scaffold_value",
    ]


class CellLineage(_CellLineageCore):
    lineage_status: _CellLineageStatus = "complete"
    compiler_origin: CompilerOrigin | None = None
    historical_driver_seed_refs: tuple[HistoricalDriverSeedLineageReference, ...] = ()
    #: Every ModelSpec declination naming this cell's semantic item (or the
    #: required role it binds) and period.
    declination_ids: tuple[str, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

    @model_validator(mode="after")
    def _require_v1_1_origin_shape(self) -> "CellLineage":
        _require_cell_lineage_origin(
            self,
            lineage_status=self.lineage_status,
            historical_driver_seed_refs=self.historical_driver_seed_refs,
        )
        financial_origins = bool(
            self.observation_ids
            or self.assumption_ids
            or self.source_refs
            or self.evidence_refs
            or self.assumption_refs
            or self.valuation_refs
            or self.historical_driver_seed_refs
        )
        if self.compiler_origin is not None:
            if financial_origins:
                raise ValueError(
                    "compiler-origin lineage cannot carry financial origins"
                )
            if self.lineage_status != "compiler_derived":
                raise ValueError(
                    "compiler_origin requires compiler_derived lineage status"
                )
        elif self.lineage_status == "compiler_derived":
            raise ValueError("compiler_derived lineage requires compiler_origin")
        if self.lineage_status == "complete" and not financial_origins:
            raise ValueError("complete lineage requires a financial origin")
        return self


#: The two lineage edge kinds the SourceBundle acyclicity walk traverses
#: (Stage B step B-3; plan §1 item 3, §5 B-3). ``derivation_input_ids``
#: edges are the original edge set; ``restates_evidence_id`` edges joined it
#: at B-3 because guarding restatement ancestry by the non-forward
#: ``available_at`` check alone admits an equal-timestamp two-cycle (design
#: §7.3; producer contract §5.4 assigns the tightening here). The kind is
#: carried on the walk stack so a cycle can name the vocabulary of the edges
#: it actually runs through: a cycle touching at least one restatement edge
#: refuses as ``restatement_lineage_invalid`` (one code, one repair target,
#: defined once in ``schema/vintage_selection.py``), while a derivation-only
#: cycle keeps its original message unchanged.
_DERIVES_EDGE: Final[str] = "derives"
_RESTATES_EDGE: Final[str] = "restates"


def _plain_json(value: Any) -> Any:
    """Frozen payload containers back to plain JSON ones.

    ``freeze_payloads_by_year`` hands out ``MappingProxyType``/tuple trees so a
    folded topology cannot be mutated behind its fingerprint. Those are the
    natural thing for a caller to pass here, and pydantic accepts them — but
    ``model_dump(mode="json")`` cannot serialize a ``mappingproxy``, and that is
    the dump ``compiler_input_content_hash`` takes to bind a ``ModelSpec`` to
    its bundle. A section that hashes only by luck of how its caller built it
    is not a pin, so the contract normalizes on the way in and the frozen view
    stays a read accessor (``frozen_payloads``).
    """

    if isinstance(value, Mapping):
        return {str(key): _plain_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(child) for child in value]
    return value


class StatementSourceMaterial(_ProductContract):
    """What the pin carries so a window-active compile is pure over its inputs.

    Step C-4 restores the mapping-active build lane inside the sealed
    ``(source_bundle, model_spec)`` contract. The severed lane needed a live
    EDGAR transport and a caller-supplied topology; both are gone. What
    replaces them is declared here, on the pin:

    * ``payloads_by_year`` — the frozen ``/api/financials`` payloads, the same
      bytes the isolated preview consumes, with their content fingerprint. The
      compile folds its ``StatementTopology`` from these and refuses a fold
      whose fingerprint disagrees;
    * ``windows`` — the ticker's current ``MultiBasisWindow`` artifacts;
    * ``correspondence_links`` / ``basis_declarations`` / ``declared_breaks`` —
      the lineage heads of the human-approved artifacts current at prepare
      time. The compile reads correspondence **here** and never from the
      repository: a build must replay from what it pinned, not from whatever
      the admission door has admitted since.

    Absent for a ticker with no window artifacts (the ordinary case), and a
    bundle without it compiles exactly as it did before this section existed.
    """

    windows: tuple[MultiBasisWindow, ...] = ()
    payloads_by_year: Mapping[str, Any] = Field(default_factory=dict)
    payloads_fingerprint: str = ""
    correspondence_links: tuple[CorrespondenceLink, ...] = ()
    basis_declarations: tuple[BasisDeclaration, ...] = ()
    declared_breaks: tuple[DeclaredBasisBreak, ...] = ()

    @field_validator("payloads_by_year", mode="before")
    @classmethod
    def _payloads_are_plain_json(cls, value: Any) -> Any:
        return _plain_json(value) if isinstance(value, Mapping) else value

    @property
    def windows_by_id(self) -> dict[str, MultiBasisWindow]:
        return {window.window_id: window for window in self.windows}

    @property
    def frozen_payloads(self) -> Mapping[int, Mapping[str, Any]]:
        return freeze_payloads_by_year(
            {int(year): payload for year, payload in self.payloads_by_year.items()}
        )

    @model_validator(mode="after")
    def _validate_material(self) -> "StatementSourceMaterial":
        if not self.windows:
            raise ValueError(
                "statement_source_material exists to carry windows; a ticker "
                "with none carries no section"
            )
        window_ids = [window.window_id for window in self.windows]
        if len(window_ids) != len(set(window_ids)):
            raise ValueError("statement window ids must be unique")
        tickers = {window.ticker.upper() for window in self.windows}
        if len(tickers) > 1:
            raise ValueError("statement_source_material windows span two tickers")
        for year in self.payloads_by_year:
            int(year)
        expected = financials_payloads_fingerprint(self.frozen_payloads)
        if self.payloads_fingerprint != expected:
            raise ValueError(
                "statement_source_material payloads_fingerprint does not match "
                "the payloads it stamps"
            )
        return self


class PinUnboundPeriod(_ProductContract):
    """A historical period whose as-filed filing no approved window covers.

    The statement-fact projection's ``unbound_periods`` row
    (``api/research/build_model_source_bundle.py`` ``project_statement_facts``):
    the pin binds no statement line for ``period_id``, and ``accession`` is the
    filing a successor binding would add.
    """

    period_id: str = Field(min_length=1)
    accession: str = Field(min_length=1)


class SourceBundle(_ProductContract):
    """One provider-neutral build input; facts remain owned by existing contracts."""

    schema_version: Literal["2.0"] = "2.0"
    schema_epoch: Literal[12]
    ticker: str
    issuer: str = Field(min_length=1)
    retrieved_at: datetime
    cutoff_at: datetime
    coverage_profile: _coverage_profile.AnnualCoverageProfile
    authored_profile_sha256: str
    requirements: tuple[_coverage_profile.AnnualSourceRequirement, ...] = Field(min_length=1)
    evidence: tuple[SourceEvidence, ...] = ()
    historical_basis_facts: tuple[HistoricalBasisFact, ...] = ()
    #: What management or the street said about a number, and the numbers a
    #: model value is held against (``schema/collected_claims.py``). Empty is
    #: absent from the wire, so a bundle minted before they existed decodes
    #: and re-serializes unchanged.
    claims: tuple[Claim, ...] = Field(default=(), exclude_if=lambda value: not value)
    references: tuple[Reference, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    coverage: tuple[AnnualRequirementCoverage, ...] = Field(min_length=1)
    evidence_audit: tuple[EvidenceAuditRecord, ...]
    valuation_requirements: tuple[_coverage_profile.ValuationRequirement, ...]
    valuation_observations: tuple[ValuationObservation, ...]
    valuation_coverage: tuple[ValuationCoverage, ...]
    market_calendar: MarketCalendarFacts
    fiscal_calendar: tuple[FiscalPeriodFacts, ...]
    statement_facts: tuple[FilingLineObservation, ...]
    reported_periods: tuple[ReportedPeriod, ...]
    statement_source_material: StatementSourceMaterial | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    kpi_observations: tuple[FilingLineObservation, ...] = ()
    sources: tuple[SourceRecord, ...] = ()
    fiscal_calendar_notes: tuple[str, ...] = ()
    normalization_notes: tuple[str, ...] = ()
    diagnostics: tuple[BuildDiagnostic, ...] = ()
    #: Historical periods the pin leaves unbound (a partial statement binding).
    #: Empty is absent from the wire, so a fully bound bundle and one minted
    #: before the field existed decode and re-serialize unchanged.
    unbound_periods: tuple[PinUnboundPeriod, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

    @field_validator("schema_epoch", mode="before")
    @classmethod
    def _epoch_is_an_exact_wire_integer(cls, value: object) -> object:
        """The epoch is a wire fact, so it is read and never coerced.

        ``5.0`` and ``"5"`` are not bundles written at epoch 5, and pydantic's
        default leniency would let a float through wearing a stamp it never
        carried.  Both stored-payload read boundaries used to restate this as
        ``type(observed) is not int`` before handing the payload over; that is
        the field's own rule, so it is stated here once instead of twice at the
        doors — which is what let the doors be deleted.
        """

        if type(value) is not int:
            raise ValueError("schema_epoch must be an exact wire integer")
        return value

    @field_validator("authored_profile_sha256")
    @classmethod
    def _authored_profile_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="authored_profile_sha256")

    @field_validator("ticker", mode="before")
    @classmethod
    def _normalize_ticker(cls, value: object) -> str:
        return normalize_contract_ticker(value)

    @model_validator(mode="after")
    def _validate_annual_coverage(self) -> "SourceBundle":
        if self.coverage_profile.focal_ticker != self.ticker:
            raise ValueError("coverage profile focal ticker must match SourceBundle")
        if (
            self.coverage_profile.annual_requirements
            and self.requirements != self.coverage_profile.annual_requirements
        ):
            raise ValueError(
                "annual requirements must equal the resolved profile denominator"
            )
        requirement_by_id = {item.requirement_id: item for item in self.requirements}
        if len(requirement_by_id) != len(self.requirements):
            raise ValueError("source requirements must be unique")
        evidence_by_id = {item.evidence_id: item for item in self.evidence}
        if len(evidence_by_id) != len(self.evidence):
            raise ValueError("source evidence ids must be unique")
        coverage_by_id = {item.requirement_id: item for item in self.coverage}
        if len(coverage_by_id) != len(self.coverage):
            raise ValueError("coverage requirement ids must be unique")
        if set(coverage_by_id) != set(requirement_by_id):
            missing = sorted(set(requirement_by_id) - set(coverage_by_id))
            extra = sorted(set(coverage_by_id) - set(requirement_by_id))
            raise ValueError(
                f"coverage must equal fixed denominator; missing={missing}, extra={extra}"
            )

        # The valuation-evidence source bindings and the valuation-coverage
        # recomputation are Engine checks
        # (``validate_source_bundle_valuation_evidence`` in
        # ``schema/valuation_evidence_validation.py``), run by the bundle
        # producer and the compile path (ruling R10).
        source_by_id = {item.id: item for item in self.sources}
        source_ids = set(source_by_id)

        validate_historical_reconciliation_source_bindings(
            self.historical_basis_facts,
            self.sources,
            allow_reconciliations=_epoch_permits_reconciliation_bindings(
                self.schema_epoch
            ),
        )
        for fact in self.historical_basis_facts:
            if fact.source_id not in source_ids:
                raise ValueError(
                    "historical-basis fact source_id must resolve in SourceBundle"
                )
            if fact.retrieved_at > self.retrieved_at:
                raise ValueError(
                    "historical-basis fact retrieval cannot postdate bundle retrieval"
                )
            if fact.available_at > self.cutoff_at:
                raise ValueError(
                    "post-cutoff historical-basis fact cannot enter SourceBundle"
                )
        for evidence in self.evidence:
            if evidence.issuer != self.issuer:
                raise ValueError("evidence issuer must match SourceBundle issuer")
            if evidence.source_id not in source_ids:
                raise ValueError("evidence source_id must resolve in SourceBundle")
            if evidence.retrieved_at > self.retrieved_at:
                raise ValueError("evidence retrieval cannot postdate bundle retrieval")
            if evidence.available_at > self.cutoff_at:
                raise ValueError("post-cutoff evidence cannot enter SourceBundle")
            if evidence.restates_evidence_id is not None:
                ancestor = evidence_by_id.get(evidence.restates_evidence_id)
                if ancestor is None:
                    raise ValueError("restatement ancestor must resolve")
                if ancestor.available_at > evidence.available_at:
                    raise ValueError("restatement ancestry cannot point forward")
                # One authority answers "same series": filing role + tag and
                # period, with accession excluded so a restatement is a later
                # vintage of the same series.
                if evidence_series_identity(ancestor) != evidence_series_identity(
                    evidence
                ):
                    raise ValueError(
                        "restatement ancestry requires identical series and period"
                    )

        # Stage B B-3: the lineage walk is strict — it traverses
        # ``restates_evidence_id`` edges alongside ``derivation_input_ids``.
        # Before B-3 restatement edges were guarded only by the non-forward
        # ``available_at`` check above, which admits an equal-timestamp
        # two-cycle (A restates B, B restates A, both available at the same
        # instant): neither edge points forward, so both pass, and the bundle
        # validated. That is the recorded hole design §7.3 names and producer
        # contract §5.4 assigns here. The walk carries the kind of the edge it
        # entered each node by, so the refusal names the vocabulary of the
        # cycle it actually found.
        visiting: list[str] = []
        entered_by: list[str | None] = []
        on_path: set[str] = set()
        visited: set[str] = set()

        def visit(evidence_id: str, edge: str | None) -> None:
            if evidence_id in on_path:
                start = visiting.index(evidence_id)
                cycle = tuple(visiting[start:])
                cycle_edges = (*entered_by[start + 1 :], edge)
                if _RESTATES_EDGE in cycle_edges:
                    raise ValueError(
                        str(
                            RestatementLineageInvalid(
                                violation="cycle",
                                evidence_ids=cycle,
                                detail=(
                                    "restatement lineage must be acyclic; the "
                                    "non-forward available_at guard alone "
                                    "admits an equal-timestamp two-cycle"
                                ),
                            )
                        )
                    )
                raise ValueError("derived evidence lineage must be acyclic")
            if evidence_id in visited:
                return
            item = evidence_by_id.get(evidence_id)
            if item is None:
                raise ValueError("derived evidence input must resolve transitively")
            visiting.append(evidence_id)
            entered_by.append(edge)
            on_path.add(evidence_id)
            for input_id in item.derivation_input_ids:
                visit(input_id, _DERIVES_EDGE)
            if item.restates_evidence_id is not None:
                visit(item.restates_evidence_id, _RESTATES_EDGE)
            visiting.pop()
            entered_by.pop()
            on_path.discard(evidence_id)
            visited.add(evidence_id)

        for evidence_id in evidence_by_id:
            visit(evidence_id, None)

        # Ancestor evidence id -> the rows restating it inside this bundle.
        # Read by the coverage loop below (Stage B B-7 family 1).
        restated_evidence_ids: dict[str, tuple[str, ...]] = {}
        for item in self.evidence:
            if item.restates_evidence_id is None:
                continue
            restated_evidence_ids[item.restates_evidence_id] = (
                *restated_evidence_ids.get(item.restates_evidence_id, ()),
                item.evidence_id,
            )

        # A cited row's own value can rest on a superseded one: a derived row
        # computes from its ``derivation_input_ids``, so citing it binds every
        # ancestor in that closure just as surely as citing the ancestor
        # itself. The walk above already proved the graph acyclic over the
        # same edges, so this closure terminates. ``restates`` edges are
        # deliberately NOT traversed: a restating row's own ancestor is the
        # vintage it supersedes, which is exactly what R7 says to leave behind.
        derivation_closure: dict[str, frozenset[str]] = {}

        def _derivation_ancestry(evidence_id: str) -> frozenset[str]:
            cached = derivation_closure.get(evidence_id)
            if cached is not None:
                return cached
            derivation_closure[evidence_id] = frozenset()
            item = evidence_by_id.get(evidence_id)
            if item is None:
                return frozenset()
            reachable: set[str] = set()
            for input_id in item.derivation_input_ids:
                reachable.add(input_id)
                reachable |= _derivation_ancestry(input_id)
            resolved = frozenset(reachable)
            derivation_closure[evidence_id] = resolved
            return resolved

        for requirement_id, result in coverage_by_id.items():
            requirement = requirement_by_id[requirement_id]
            period_by_id = {item.period_id: item for item in result.periods}
            if len(period_by_id) != len(result.periods):
                raise ValueError("coverage periods must be unique")
            if tuple(period_by_id) != requirement.period_ids:
                raise ValueError(
                    "coverage periods must exactly match requirement periods"
                )
            for period_id, outcome in period_by_id.items():
                all_evidence_ids = (
                    outcome.evidence_ids
                    + outcome.applicability_evidence_ids
                    + outcome.conflict_evidence_ids
                )
                if any(item not in evidence_by_id for item in all_evidence_ids):
                    raise ValueError("coverage evidence id must resolve")
                if outcome.disclosure_search_source_id is not None:
                    search_source = source_by_id.get(
                        outcome.disclosure_search_source_id
                    )
                    if search_source is None:
                        raise ValueError(
                            "coverage disclosure search source id must resolve"
                        )
                    source_accession = str(
                        (search_source.key_fields or {}).get("accession")
                        or search_source.endpoint_or_filing_id
                        or ""
                    )
                    if source_accession != outcome.disclosure_search_accession:
                        raise ValueError(
                            "coverage disclosure search accession must match source"
                        )
                    cell = requirement.cell_for_period(period_id)
                    if cell.binding_state in {"not_applicable", "not_separately_presented"}:
                        citation = cell.absence_citation
                        if (
                            citation is None
                            or citation.accession != outcome.disclosure_search_accession
                        ):
                            raise ValueError(
                                "coverage disclosure search accession must match "
                                "the absence citation"
                            )
                    else:
                        pinned_accession = (
                            None
                            if cell.pinned_ref is None
                            else cell.pinned_ref.accession
                        )
                        if pinned_accession != outcome.disclosure_search_accession:
                            raise ValueError(
                                "coverage disclosure search accession must match pin"
                            )
                if outcome.disposition == "not_separately_presented":
                    cell = requirement.cell_for_period(period_id)
                    if (
                        cell.binding_state != "not_separately_presented"
                        or outcome.included_in != cell.included_in
                        or outcome.absence_citation != cell.absence_citation
                    ):
                        raise ValueError(
                            "included coverage must retain its exact period judgment"
                        )
                if outcome.disposition in {"observed", "derived"}:
                    for evidence_id in outcome.evidence_ids:
                        # Stage B B-7 (plan §5 B-7 family 1; §4.4's
                        # illegal-state audit): R7 says a build binds the
                        # *latest restated* vintage as of its cutoff. Bundle
                        # production derives that through ``select_vintage``,
                        # but nothing stopped a hand-assembled or
                        # successor-transformed bundle from citing a
                        # superseded ancestor and compiling green on the
                        # original's number. Post-cutoff evidence cannot enter
                        # a SourceBundle at all (checked above), so a
                        # restating row *present in this bundle* is by
                        # construction available at or before the cutoff:
                        # citing its ancestor is binding a vintage the
                        # selection rule cannot reach. The ancestor stays in
                        # the evidence — a restated arrival is an addition
                        # with ancestry, never a replacement (SAFE-03 as
                        # design §7.3 scopes it) — it just may not be what
                        # coverage binds.
                        superseded = sorted(
                            ancestor
                            for ancestor in (
                                {evidence_id} | _derivation_ancestry(evidence_id)
                            )
                            if ancestor in restated_evidence_ids
                        )
                        if superseded:
                            detail = "; ".join(
                                f"{ancestor} is restated by "
                                f"[{', '.join(restated_evidence_ids[ancestor])}]"
                                for ancestor in superseded
                            )
                            through = (
                                ""
                                if superseded == [evidence_id]
                                else f" through the derivation inputs of {evidence_id}"
                            )
                            raise ValueError(
                                "coverage must bind the latest restated "
                                f"vintage as of the bundle cutoff{through}; "
                                f"{detail} — rebuild the bundle so coverage "
                                "cites the restating vintage"
                            )
                        evidence = evidence_by_id[evidence_id]
                        if evidence.period_id != period_id:
                            raise ValueError(
                                "coverage evidence period must match requirement"
                            )
                        if (
                            evidence.unit,
                            evidence.scale,
                            evidence.sign,
                        ) != (
                            requirement.unit,
                            requirement.scale,
                            requirement.sign,
                        ):
                            raise ValueError(
                                "coverage evidence unit, scale, and sign must match"
                            )
                        if not evidence.locator:
                            raise ValueError(
                                "material supported evidence requires locator"
                            )
                        if (
                            outcome.disposition == "derived"
                            and not evidence.derivation_expression
                        ):
                            raise ValueError(
                                "derived coverage requires transitive lineage"
                            )
                if outcome.disposition == "aggregate_only_hidden":
                    if requirement.disclosed_child:
                        raise ValueError(
                            "aggregate cannot hide a separately disclosed child"
                        )
                    if requirement.independently_material:
                        raise ValueError(
                            "aggregate cannot hide an independently material child"
                        )
                    if (
                        requirement.aggregate_parent_requirement_id
                        != outcome.aggregate_parent_requirement_id
                    ):
                        raise ValueError(
                            "aggregate parent must match fixed requirement"
                        )
                    parent = coverage_by_id.get(
                        outcome.aggregate_parent_requirement_id or ""
                    )
                    parent_period = (
                        next(
                            (
                                item
                                for item in parent.periods
                                if item.period_id == period_id
                            ),
                            None,
                        )
                        if parent is not None
                        else None
                    )
                    if parent_period is None or parent_period.disposition not in {
                        "observed",
                        "derived",
                    }:
                        raise ValueError(
                            "aggregate-only child requires supported parent"
                        )

        audit_keys = tuple(
            (item.requirement_id, item.period_id) for item in self.evidence_audit
        )
        expected_audit_keys = tuple(
            (requirement.requirement_id, period_id)
            for requirement in self.requirements
            for period_id in requirement.period_ids
        )
        if len(audit_keys) != len(set(audit_keys)) or set(audit_keys) != set(
            expected_audit_keys
        ):
            raise ValueError("evidence audit must exactly equal annual denominator")
        for audit in self.evidence_audit:
            requirement = requirement_by_id[audit.requirement_id]
            if audit.required_role != requirement.required_role:
                raise ValueError("audit required role must match its requirement")
            expected_line = requirement.cell_for_period(audit.period_id).pinned_ref
            if audit.line != expected_line:
                raise ValueError("audit filing line must match its requirement binding")
            if any(item not in evidence_by_id for item in audit.evidence_ids):
                raise ValueError("audit evidence id must resolve")
            if any(item not in source_ids for item in audit.search_sources):
                raise ValueError("audit search source must resolve")

        expected_requirements = self.coverage_profile.valuation_requirements
        if self.valuation_requirements != expected_requirements:
            raise ValueError(
                "valuation requirements must equal the resolved profile denominator"
            )
        valuation_ids = {item.observation_id for item in self.valuation_observations}
        if len(valuation_ids) != len(self.valuation_observations):
            raise ValueError("valuation observation ids must be unique")
        for observation in self.valuation_observations:
            if observation.issuer is not None and observation.issuer not in {
                self.coverage_profile.focal_ticker,
                *self.coverage_profile.peer_tickers,
            }:
                raise ValueError(
                    "valuation observation issuer is outside the profile roster"
                )
        admitted_issuers = {
            self.coverage_profile.focal_ticker,
            *self.coverage_profile.peer_tickers,
        }
        covered_fiscal_issuers = {item.issuer for item in self.fiscal_calendar}
        if not covered_fiscal_issuers <= admitted_issuers:
            raise ValueError(
                "fiscal calendar must not contain issuers outside the profile roster"
            )
        fiscal_keys = {(item.issuer, item.basis) for item in self.fiscal_calendar}
        expected_fiscal_keys = {
            (issuer, basis)
            for issuer in covered_fiscal_issuers
            for basis in self.coverage_profile.valuation_fiscal_basis_ids
        }
        if fiscal_keys != expected_fiscal_keys or len(self.fiscal_calendar) != len(
            expected_fiscal_keys
        ):
            raise ValueError(
                "fiscal calendar must exactly cover the profile roster and axis"
            )
        for issuer in sorted(covered_fiscal_issuers):
            periods = {
                item.basis: item
                for item in self.fiscal_calendar
                if item.issuer == issuer
            }
            ordered = tuple(
                periods[basis]
                for basis in self.coverage_profile.valuation_fiscal_basis_ids
            )
            for previous, current in zip(ordered, ordered[1:]):
                if previous.period_end >= current.period_start:
                    raise ValueError("fiscal intervals must not overlap")
                if current.period_start != previous.period_end + timedelta(days=1):
                    raise ValueError("fiscal intervals must be exactly adjacent")
        return self

    @model_validator(mode="after")
    def _collected_claims_resolve(self) -> "SourceBundle":
        source_ids = {item.id for item in self.sources}
        claim_by_id = {item.claim_id: item for item in self.claims}
        if len(claim_by_id) != len(self.claims):
            raise ValueError("claim ids must be unique")
        for claim in self.claims:
            if claim.source_id not in source_ids:
                raise ValueError("claim source_id must resolve in SourceBundle")
        observation_by_id = {
            item.observation_id: item for item in self.valuation_observations
        }
        if len({item.reference_id for item in self.references}) != len(
            self.references
        ):
            raise ValueError("reference ids must be unique")
        for reference in self.references:
            if reference.source_id not in source_ids:
                raise ValueError("reference source_id must resolve in SourceBundle")
            if reference.claim_id is not None:
                claim = claim_by_id.get(reference.claim_id)
                expected_author = (
                    "consensus" if reference.kind == "consensus" else "management"
                )
                if claim is None or claim.author != expected_author:
                    raise ValueError(
                        f"{reference.kind} reference must point at a "
                        f"{expected_author} claim in SourceBundle"
                    )
            if reference.observation_id is not None:
                observation = observation_by_id.get(reference.observation_id)
                if (
                    observation is None
                    or observation.value != reference.value
                    or observation.as_of_close != reference.as_of
                ):
                    raise ValueError(
                        "price reference must project its pinned observation"
                    )
        return self


def _decode_stored_source_bundle(payload: object) -> SourceBundle:
    """``SourceBundle.model_validate`` with the stored-vintage refusal.

    IG-4 of the issuer-generic plan (§14 item 16): a stored capability section
    is *bytes*, minted under whatever declaration shape the binary that wrote
    them carried.  ``CapabilityDeclarationInvalid`` names the model-kind data
    lane as its repair target, which is right at construction — where an
    author is writing a declaration — and wrong here, where nobody is: the
    declaration this binary carries is fine and the stored bundle is stale.
    Worse, that refusal is deliberately not a ``ValueError`` (so pydantic
    cannot fold it), so it also escapes every ``except SourceBundleRebuildRequired``
    / ``except ValueError`` arm the product read paths use to turn a stale
    stored bundle into a typed refusal, and reaches the caller as an unhandled
    fault.  Translating it here restores both the correct repair lane and the
    typed boundary; the original refusal rides as ``__cause__`` and its code,
    field and repair are quoted in the message, so nothing is lost.

    The second arm is the same translation for the general case.  A stored
    payload is bytes under a digest; whether this binary's classes can build a
    ``SourceBundle`` out of them is decided here and nowhere else, so this
    function is total over its outcomes: it returns a ``SourceBundle`` or it
    raises ``SourceBundleRebuildRequired``.  A raw ``ValidationError`` escaping
    would be a third outcome, and because it subclasses ``ValueError`` every
    read path that carries this refusal would silently reclassify a stale
    contract as damaged bytes.  Which *rule* moved — a retired epoch, a
    renamed capability key, a tightened observation — is detail for the
    message; the product fact and its repair are the same one.
    """

    try:
        return SourceBundle.model_validate(payload)
    except CapabilityDeclarationInvalid as exc:
        raise SourceBundleRebuildRequired(
            "stored SourceBundle carries a capability section this binary "
            f"cannot decode ({exc}); the stored bundle is rebuild-required — "
            "re-prepare it against the current model-kind declaration"
        ) from exc
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "<root>"
        raise SourceBundleRebuildRequired(
            "stored SourceBundle does not satisfy this binary's contract "
            f"({exc.error_count()} rejections, first at {location}: "
            f"{first['msg']}); the stored bundle is rebuild-required — "
            "rebuild the model and republish it"
        ) from exc


def decode_source_bundle_payload(payload: object) -> SourceBundle:
    """Decode through the sole stored-bundle readability chokepoint."""

    return _decode_stored_source_bundle(payload)


def decode_persisted_source_bundle_payload(
    payload: Mapping[str, Any],
    *,
    expected_wire_digest: str,
) -> SourceBundle:
    """Verify a sealed payload against its wire digest, then decode it.

    The two questions are different and are answered in that order.  Bytes that
    disagree with their own digest are *damaged*, and that is the only thing
    this function decides on its own — a bare ``ValueError``, raised before any
    contract class sees them.  Whether intact bytes still satisfy this binary's
    contract is the chokepoint's decision, carried out untouched.

    Nothing is normalized on the way in.  A sealed payload is decoded exactly
    as it was sealed: a payload this binary rejects is rebuild-required, never
    migrated on read.
    """

    from .render_state import compiler_input_content_hash

    normalized_digest = str(expected_wire_digest).removeprefix("sha256:")
    validate_sha256_hex(
        normalized_digest,
        field_name="expected_source_bundle_wire_digest",
    )
    actual_digest = compiler_input_content_hash(payload)
    if actual_digest != f"sha256:{normalized_digest}":
        raise ValueError("persisted SourceBundle differs from its wire digest")
    return _decode_stored_source_bundle(payload)


EvidenceBasis = Literal["source_backed", "analyst_judgment"]


class ScenarioOverride(_ProductContract):
    """Review metadata for one authored scenario control override."""

    assumption_id: Annotated[
        str,
        FieldDisposition.CROSS_SECTION_NATURAL_KEY,
    ] = Field(min_length=1)
    target_control_id: Annotated[
        str,
        FieldDisposition.SERVER_MINTED,
    ] = Field(min_length=1)
    period_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ] = Field(min_length=1)
    unit: Annotated[str, FieldDisposition.CLOSED_VOCABULARY] = Field(min_length=1)
    value: Annotated[float | PeriodValueSeries, FieldDisposition.JUDGMENT]
    direction: Annotated[
        Literal[
            "increase",
            "decrease",
            "context_specific",
            "base",
        ],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    rationale: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    evidence_basis: Annotated[EvidenceBasis, FieldDisposition.CLOSED_VOCABULARY]
    evidence_ids: Annotated[tuple[str, ...], FieldDisposition.SELECTOR] = ()
    limitations: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)

    @model_validator(mode="after")
    def _complete_override(self) -> "ScenarioOverride":
        if len(self.period_ids) != len(set(self.period_ids)):
            raise ValueError(
                "scenario override period_ids must be non-empty and unique"
            )
        if self.evidence_basis == "source_backed" and not self.evidence_ids:
            raise ValueError("source-backed scenario override requires evidence")
        if isinstance(self.value, dict):
            expected_years = {
                int(period_id.removeprefix("FY").removesuffix("E"))
                for period_id in self.period_ids
            }
            if set(self.value) != expected_years:
                raise ValueError(
                    "scenario override series must cover its exact period_ids"
                )
        return self


def _review_value_by_period(
    value: ScalarValue | dict[int, float],
    period_ids: tuple[str, ...],
) -> dict[int, float]:
    years = tuple(
        int(period_id.removeprefix("FY").removesuffix("E")) for period_id in period_ids
    )
    if isinstance(value, dict):
        return {int(period): float(item) for period, item in value.items()}
    return {year: float(value) for year in years}


class ScenarioReview(_ProductContract):
    """Authored case review; ModelSpec admits base completeness, others may gap."""

    scenario_name: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    description: Annotated[str, FieldDisposition.JUDGMENT]
    overrides: Annotated[tuple[ScenarioOverride, ...], FieldDisposition.JUDGMENT] = ()


class HistoricalNormalizationDecision(_ProductContract):
    decision_id: Annotated[str, FieldDisposition.SERVER_MINTED] = Field(min_length=1)
    description: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    evidence_basis: Annotated[EvidenceBasis, FieldDisposition.CLOSED_VOCABULARY]
    evidence_ids: Annotated[tuple[str, ...], FieldDisposition.SELECTOR] = ()
    limitations: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)

    @model_validator(mode="after")
    def _source_backed_shape(self) -> "HistoricalNormalizationDecision":
        if self.evidence_basis == "source_backed" and not self.evidence_ids:
            raise ValueError(
                "source-backed normalization decisions require evidence_ids"
            )
        return self


class SensitivityAxis(_ProductContract):
    control_id: Annotated[str, FieldDisposition.SELECTOR] = Field(min_length=1)
    unit: Annotated[str, FieldDisposition.CLOSED_VOCABULARY] = Field(min_length=1)
    ordered_values: Annotated[
        tuple[float, ...],
        FieldDisposition.JUDGMENT,
    ] = Field(min_length=3)

    @model_validator(mode="after")
    def _ordered_unique_values(self) -> "SensitivityAxis":
        if self.ordered_values != tuple(sorted(self.ordered_values)) or len(
            self.ordered_values
        ) != len(set(self.ordered_values)):
            raise ValueError("sensitivity axis values must be sorted and unique")
        return self


class ModelSensitivity(_ProductContract):
    sensitivity_id: Annotated[str, FieldDisposition.SERVER_MINTED] = Field(min_length=1)
    row_axis: Annotated[SensitivityAxis, FieldDisposition.JUDGMENT]
    column_axis: Annotated[SensitivityAxis, FieldDisposition.JUDGMENT]
    output_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ] = Field(min_length=1)
    pinned_input_policy: Annotated[
        Literal["all_non_varied_model_spec_inputs"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    nonzero_movement_required: Annotated[
        Literal[True],
        FieldDisposition.CLOSED_VOCABULARY,
    ]

    @model_validator(mode="after")
    def _two_distinct_controls(self) -> "ModelSensitivity":
        if self.row_axis.control_id == self.column_axis.control_id:
            raise ValueError("sensitivity axes must use distinct controls")
        return self


ValuationEstimateBasis = Literal["issuer_fiscal_year", "exact_next_four_quarters"]


class ValuationPeerPolicy(_ProductContract):
    """The peer roster; empty when no relative method derives from peers."""

    peer_tickers: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ]
    minimum_covered_peer_count: Annotated[int, FieldDisposition.JUDGMENT] = Field(ge=0)
    metric_ids: Annotated[
        tuple[Literal["forward_pe", "ev_adjusted_ebitda"], ...],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    fiscal_basis_period_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ] = Field(min_length=1)
    estimate_basis: Annotated[
        ValuationEstimateBasis,
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    as_of_policy: Annotated[
        Literal[
            "common_market_close_at_or_before_cutoff_max_five_business_days"
        ],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    aggregation: Annotated[Literal["median"], FieldDisposition.CLOSED_VOCABULARY]
    replacement_policy: Annotated[
        Literal["no_automatic_replacements"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]

    @model_validator(mode="after")
    def _peer_policy(self) -> "ValuationPeerPolicy":
        if self.peer_tickers != tuple(sorted(set(self.peer_tickers))):
            raise ValueError("peer_tickers must be sorted and unique")
        if self.minimum_covered_peer_count > len(self.peer_tickers):
            raise ValueError("minimum peer count cannot exceed named peers")
        if self.metric_ids != tuple(sorted(set(self.metric_ids))):
            raise ValueError("metric_ids must be sorted and unique")
        return self


class DcfMethodControl(_ProductContract):
    method: Annotated[
        Literal["terminal_growth", "exit_multiple"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    display_role: Annotated[
        Literal["selected_canonical", "labeled_cross_check"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    wacc_control_field: Annotated[Literal["wacc"], FieldDisposition.CLOSED_VOCABULARY]
    terminal_control_field: Annotated[
        Literal[
            "terminal_growth_rate",
            "terminal_multiple",
        ],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    cash_flow_basis: Annotated[
        Literal["unlevered_free_cash_flow"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    forecast_period_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ] = Field(min_length=1)
    enterprise_to_equity_bridge: Annotated[
        Literal[
            "corporate_cash_less_corporate_debt_excluding_client_funds"
        ],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    share_basis: Annotated[
        Literal["diluted_shares"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    output_label: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    rationale: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    limitations: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)

    @model_validator(mode="after")
    def _control_references(self) -> "DcfMethodControl":
        expected = (
            "terminal_growth_rate"
            if self.method == "terminal_growth"
            else "terminal_multiple"
        )
        if self.terminal_control_field != expected:
            raise ValueError("DCF method and terminal control field must match")
        return self


class RelativeValuationControl(_ProductContract):
    method: Annotated[
        Literal["forward_pe", "ev_adjusted_ebitda"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    fiscal_basis_period_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ] = Field(min_length=1)
    selection_basis: Annotated[
        Literal[
            "none", "current_implied", "peer_derived", "thesis_judgment"
        ],
        FieldDisposition.CLOSED_VOCABULARY,
    ] = "none"
    rationale: Annotated[str | None, FieldDisposition.JUDGMENT] = None
    bridge: Annotated[
        Literal[
            "equity_value_per_diluted_share",
            "enterprise_value_to_equity_value_per_diluted_share",
        ],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    output_label: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    evidence_ids: Annotated[tuple[str, ...], FieldDisposition.SELECTOR] = ()
    limitations: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)

    @model_validator(mode="after")
    def _method_bridge(self) -> "RelativeValuationControl":
        expected = (
            "equity_value_per_diluted_share"
            if self.method == "forward_pe"
            else "enterprise_value_to_equity_value_per_diluted_share"
        )
        if self.bridge != expected:
            raise ValueError("relative method and bridge must match")
        if self.selection_basis == "none":
            if self.rationale is not None or self.evidence_ids:
                raise ValueError("unselected relative method forbids authorization")
        elif not self.rationale:
            raise ValueError("selected relative method requires rationale")
        return self


class ValuationFamilyAuthorization(_ProductContract):
    """One relative method's authorized bull/base/bear multiple family.

    ``case_order`` is the family's orientation; a base outside bull and bear
    is the build's declinable ``scenario_case_order_inverted`` warning, and an
    evidence-derived basis with no ``evidence_ids`` is a citation gap the
    pricing door records on the Thesis (V52, V54). Digest and coverage profile
    bind at ``validate_source_bundle_binding``; unresolved peer-derived
    selections and unbundled ``evidence_ids`` are recorded at compile/publish
    by ``valuation_selection_findings`` as ``valuation_selection_unresolved``."""

    method: Annotated[
        Literal["forward_pe", "ev_adjusted_ebitda"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    selection_basis: Annotated[
        Literal["current_implied", "peer_derived", "thesis_judgment"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    rationale: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    evidence_ids: Annotated[tuple[str, ...], FieldDisposition.SELECTOR] = ()
    case_order: Annotated[
        Literal["bull_high", "bull_low"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    bull: Annotated[Decimal, FieldDisposition.JUDGMENT] = Field(gt=0)
    base: Annotated[Decimal, FieldDisposition.JUDGMENT] = Field(gt=0)
    bear: Annotated[Decimal, FieldDisposition.JUDGMENT] = Field(gt=0)

    @field_validator("bull", "base", "bear")
    @classmethod
    def _canonical_multiple(cls, value: Decimal) -> Decimal:
        quantized = Decimal(value).quantize(
            VALUATION_MULTIPLE_QUANTUM,
            rounding=ROUND_HALF_EVEN,
        )
        if not quantized.is_finite() or quantized <= 0:
            raise ValueError("valuation family values must be positive and finite")
        return quantized


class ValuationSummaryPolicy(_ProductContract):
    displayed_method: Annotated[
        Literal["dcf_terminal_growth"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    displayed_output_label: Annotated[
        Literal["Model valuation — equity value per diluted share"],
        FieldDisposition.CLOSED_VOCABULARY,
    ]
    target_fiscal_period_id: Annotated[
        str,
        FieldDisposition.SELECTOR,
    ] = Field(min_length=1)
    requires_valid_base_model: Annotated[
        Literal[True],
        FieldDisposition.CLOSED_VOCABULARY,
    ]


class ValuationTerminalCaseValues(_ProductContract):
    """Exact precompile terminal cases retained for deterministic lowering.

    Cases that do not run bull > base > bear are the build's declinable
    ``scenario_case_order_inverted`` warning, not a refusal (V54)."""

    bull: Annotated[Decimal, FieldDisposition.JUDGMENT]
    base: Annotated[Decimal, FieldDisposition.JUDGMENT]
    bear: Annotated[Decimal, FieldDisposition.JUDGMENT]

    @field_validator("bull", "base", "bear")
    @classmethod
    def _finite_values(cls, value: Decimal) -> Decimal:
        result = Decimal(value)
        if not result.is_finite():
            raise ValueError("valuation terminal cases must be finite")
        return result


class ValuationNetShareDilutionYearRate(_ProductContract):
    """One forecast fiscal year's accepted annual net share dilution rate."""

    fiscal_year: Annotated[int, FieldDisposition.JUDGMENT] = Field(gt=0)
    rate: Annotated[Decimal, FieldDisposition.JUDGMENT]

    @field_validator("rate")
    @classmethod
    def _finite_rate(cls, value: Decimal) -> Decimal:
        result = Decimal(value)
        if not result.is_finite():
            raise ValueError("net share dilution rates must be finite")
        return result


class ValuationNetShareDilutionRates(_ProductContract):
    """Exact precompile net share dilution rates retained for deterministic lowering.

    The build writes each rate on the net-dilution rate row as kind
    ``judgment``; a projection year without a rate takes the house value.
    """

    rates: Annotated[
        tuple[ValuationNetShareDilutionYearRate, ...],
        FieldDisposition.JUDGMENT,
    ] = Field(min_length=1)
    rationale: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    evidence_ids: Annotated[tuple[str, ...], FieldDisposition.JUDGMENT] = ()

    @model_validator(mode="after")
    def _canonical_years(self) -> "ValuationNetShareDilutionRates":
        years = tuple(row.fiscal_year for row in self.rates)
        if years != tuple(sorted(set(years))):
            raise ValueError(
                "net share dilution rates must be sorted and unique by fiscal year"
            )
        return self


class ModelValuationPolicy(_ProductContract):
    dcf_methods: Annotated[
        tuple[DcfMethodControl, ...],
        FieldDisposition.JUDGMENT,
    ] = Field(min_length=2, max_length=2)
    relative_methods: Annotated[
        tuple[RelativeValuationControl, ...],
        FieldDisposition.JUDGMENT,
    ] = Field(
        min_length=2,
        max_length=2,
    )
    peer_policy: Annotated[ValuationPeerPolicy, FieldDisposition.JUDGMENT]
    sensitivities: Annotated[
        tuple[ModelSensitivity, ...],
        FieldDisposition.JUDGMENT,
    ] = Field(min_length=1)
    summary: Annotated[ValuationSummaryPolicy, FieldDisposition.JUDGMENT]
    terminal_growth_cases: Annotated[
        ValuationTerminalCaseValues | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    exit_multiple_cases: Annotated[
        ValuationTerminalCaseValues | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    net_share_dilution: Annotated[
        ValuationNetShareDilutionRates | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    exit_earnings_measure: Annotated[
        ExitEarningsMeasure,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default="terminal_fcf",
        exclude_if=lambda value: value == "terminal_fcf",
    )
    terminal_evidence_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=(), exclude_if=lambda value: not value)
    beta_floor_evidence_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=(), exclude_if=lambda value: not value)

    @model_validator(mode="after")
    def _complete_valuation_policy(self) -> "ModelValuationPolicy":
        dcf = {item.method: item for item in self.dcf_methods}
        if set(dcf) != {"terminal_growth", "exit_multiple"}:
            raise ValueError("both DCF methods must be explicitly controlled")
        if dcf["terminal_growth"].display_role != "selected_canonical":
            raise ValueError("terminal-growth DCF must own the displayed result")
        if dcf["exit_multiple"].display_role != "labeled_cross_check":
            raise ValueError("exit-multiple DCF must remain a labeled cross-check")
        if {item.method for item in self.relative_methods} != {
            "forward_pe",
            "ev_adjusted_ebitda",
        }:
            raise ValueError("P/E and EV/EBITDA relative controls are required")
        if (self.terminal_growth_cases is None) != (self.exit_multiple_cases is None):
            raise ValueError("terminal-growth and exit-multiple cases must pair")
        if self.terminal_growth_cases is not None and not all(
            Decimal("-0.05") <= value <= Decimal("0.10")
            for value in (
                self.terminal_growth_cases.bull,
                self.terminal_growth_cases.base,
                self.terminal_growth_cases.bear,
            )
        ):
            raise ValueError("terminal-growth cases must be decimal rates")
        if self.exit_multiple_cases is not None and not all(
            Decimal("0") < value <= Decimal("100")
            for value in (
                self.exit_multiple_cases.bull,
                self.exit_multiple_cases.base,
                self.exit_multiple_cases.bear,
            )
        ):
            raise ValueError("exit-multiple cases must be positive and bounded")
        return self


class DegradedModelChoice(_ProductContract):
    """Decode of stored ModelSpec payloads only; nothing writes or reads it.

    ``declinations`` replaced this section; the field stays so stored 3.0
    payloads (which all carry ``degraded_choices``) decode unchanged.
    """

    choice_id: Annotated[str, FieldDisposition.SERVER_MINTED] = Field(min_length=1)
    description: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    effect: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)
    repair_target: Annotated[str, FieldDisposition.JUDGMENT] = Field(min_length=1)


class ModelSpecHistoricalDriverSeedProjection(_ProductContract):
    """Exact sealed-HFF seed projection consumed only by the compiler.

    The projection deliberately carries natural BusinessModel node keys rather
    than template/workbook targets.  ``HistoricalFoundationRef`` cannot be
    imported here without creating the product-contract/HFF authoring cycle, so
    its exact immutable identity is retained as the two primitive ref fields and
    reconstructed/compared at the server assembly boundary.
    """

    schema_version: Literal["model-spec-historical-driver-seeds.v1"] = (
        "model-spec-historical-driver-seeds.v1"
    )
    historical_foundation_artifact_id: str = Field(pattern=r"^hff_[0-9a-f]{32}$")
    historical_foundation_content_sha256: str
    requirements: tuple[HistoricalDriverSeedRequirement, ...] = Field(min_length=1)
    denominator_sha256: str
    seeds: tuple[HistoricalDriverSeed, ...] = ()

    _historical_foundation_content_digest = field_validator(
        "historical_foundation_content_sha256"
    )(_validate_named_sha256("historical_foundation_content_sha256"))
    _denominator_digest = field_validator("denominator_sha256")(
        _validate_named_sha256("denominator_sha256")
    )

    @model_validator(mode="after")
    def _sealed_seed_subset(self) -> "ModelSpecHistoricalDriverSeedProjection":
        requirement_keys = tuple(row.driver_key for row in self.requirements)
        requirement_ids = tuple(row.driver_id for row in self.requirements)
        seed_keys = tuple(row.driver_key for row in self.seeds)
        seed_ids = tuple(row.driver_id for row in self.seeds)
        if requirement_keys != tuple(sorted(set(requirement_keys))):
            raise ValueError(
                "historical driver seed requirements must be sorted and unique"
            )
        if len(requirement_ids) != len(set(requirement_ids)):
            raise ValueError("historical driver seed requirements repeat driver_id")
        if seed_keys != tuple(sorted(set(seed_keys))):
            raise ValueError("historical driver seeds must be sorted and unique")
        if len(seed_ids) != len(set(seed_ids)):
            raise ValueError("historical driver seeds repeat driver_id")
        if not set(seed_keys) <= set(requirement_keys):
            raise ValueError(
                "historical driver seed projection seeds must be a subset of requirements"
            )
        requirements_by_key = {row.driver_key: row for row in self.requirements}
        for seed in self.seeds:
            requirement = requirements_by_key[seed.driver_key]
            if (
                seed.driver_id,
                seed.period_id,
                seed.unit,
                seed.model_scale,
            ) != (
                requirement.driver_id,
                requirement.period_id,
                requirement.unit,
                requirement.model_scale,
            ):
                raise ValueError(
                    "historical driver seed differs from its exact requirement"
                )
        if self.denominator_sha256 != historical_driver_seed_denominator_sha256(
            self.requirements
        ):
            raise ValueError("historical driver seed denominator digest mismatch")
        return self


class ModelSpecHistoricalFoundationObservation(_ProductContract):
    """One exact accepted-HFF fact/evidence pair admitted to compilation."""

    requirement_id: str = Field(min_length=1)
    semantic_id: str = Field(min_length=1)
    period_id: str = Field(min_length=1)
    fact: HistoricalBasisFact
    evidence: SourceEvidence

    @model_validator(mode="after")
    def _closed_observation(self) -> "ModelSpecHistoricalFoundationObservation":
        fact = self.fact
        evidence = self.evidence
        validate_historical_reconciliation_bindings(
            (fact,),
            allow_reconciliations=False,
        )
        if self.period_id != fact.period_id or self.period_id != evidence.period_id:
            raise ValueError("historical-foundation observation period mismatch")
        expected_evidence_id = fact.fact_id.replace("fact.", "evidence.", 1)
        if evidence.evidence_id != expected_evidence_id:
            raise ValueError(
                "historical-foundation fact/evidence identities do not pair"
            )
        if (
            fact.line,
            fact.grade,
            fact.value,
            fact.unit,
            fact.scale,
            fact.sign,
            fact.source_id,
            fact.provider,
            fact.retrieved_at,
            fact.available_at,
            fact.as_of_date,
            fact.locator,
            fact.fiscal_basis,
            fact.period_length_months,
            fact.accounting_definition,
            fact.confidence,
            fact.source_quality,
        ) != (
            evidence.line,
            evidence.grade,
            evidence.value,
            evidence.unit,
            evidence.scale,
            evidence.sign,
            evidence.source_id,
            evidence.provider,
            evidence.retrieved_at,
            evidence.available_at,
            evidence.as_of_date,
            evidence.locator,
            evidence.fiscal_basis,
            evidence.period_length_months,
            evidence.accounting_definition,
            evidence.confidence,
            evidence.source_quality,
        ):
            raise ValueError(
                "historical-foundation fact differs from exact source evidence"
            )
        return self


class ModelSpecHistoricalFoundationFactProjection(_ProductContract):
    """The total accepted-HFF fact projection as a server-owned compiler input.

    Total over the bound snapshot (Stage A step A-4): every supported
    (requirement, period) on the model kind's HFF fact-role plane appears as
    an exact fact/evidence observation; an empty tuple is the honest state
    of a snapshot whose HFF-plane periods all carry typed gap dispositions —
    the projection itself is never absent for an accepted foundation.
    """

    schema_version: Literal["model-spec-historical-foundation-facts.v1"] = (
        "model-spec-historical-foundation-facts.v1"
    )
    historical_foundation_artifact_id: str = Field(pattern=r"^hff_[0-9a-f]{32}$")
    historical_foundation_content_sha256: str
    observations: tuple[ModelSpecHistoricalFoundationObservation, ...] = ()

    @field_validator("historical_foundation_content_sha256")
    @classmethod
    def _foundation_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="historical_foundation_content_sha256",
        )

    @model_validator(mode="after")
    def _canonical_observations(self) -> "ModelSpecHistoricalFoundationFactProjection":
        keys = tuple(
            (row.requirement_id, row.semantic_id, row.period_id)
            for row in self.observations
        )
        fact_ids = tuple(
            (row.requirement_id, row.fact.fact_id) for row in self.observations
        )
        evidence_ids = tuple(
            (row.requirement_id, row.evidence.evidence_id)
            for row in self.observations
        )
        if keys != tuple(sorted(set(keys))):
            raise ValueError(
                "historical-foundation observations must be sorted and unique"
            )
        if len(fact_ids) != len(set(fact_ids)):
            raise ValueError("historical-foundation projection repeats fact IDs")
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("historical-foundation projection repeats evidence IDs")
        return self


class ModelSpecForecastAssumptionSetBinding(_ProductContract):
    """Human-resolvable exact forecast-stage lineage retained in ModelSpec."""

    schema_version: Literal["model-spec-forecast-assumption-set-binding.v1"] = (
        "model-spec-forecast-assumption-set-binding.v1"
    )
    forecast_assumption_set_id: str = Field(pattern=r"^fas_[0-9a-f]{32}$")
    forecast_assumption_set_content_sha256: str
    historical_foundation_artifact_id: str = Field(pattern=r"^hff_[0-9a-f]{32}$")
    historical_foundation_content_sha256: str

    _forecast_assumption_set_content_digest = field_validator(
        "forecast_assumption_set_content_sha256"
    )(_validate_named_sha256("forecast_assumption_set_content_sha256"))
    _historical_foundation_content_digest = field_validator(
        "historical_foundation_content_sha256"
    )(_validate_named_sha256("historical_foundation_content_sha256"))


class ModelSpecScenarioCaseProbabilities(_ProductContract):
    """The accepted ScenarioSet's authored bull/base/bear weights, carried exactly."""

    bull: float = Field(ge=0, le=1, allow_inf_nan=False)
    base: float = Field(ge=0, le=1, allow_inf_nan=False)
    bear: float = Field(ge=0, le=1, allow_inf_nan=False)
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()


class ModelSpecScenarioCriticalFactor(_ProductContract):
    """One accepted ScenarioSet critical factor, carried for the info findings."""

    factor_key: str = Field(min_length=1)
    driver_key: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()


class ModelSpecScenarioDriverRole(_ProductContract):
    """One accepted ScenarioSet curve's role, keyed by its FAS driver.

    The curves close exactly over the FAS drivers, before the compiler fans a
    driver out to several controls; carried for the info findings.
    """

    driver_key: str = Field(min_length=1)
    role: Literal["selected_factor", "amplifier_channel", "held_at_base"]


class ModelSpecScenarioSetBinding(_ProductContract):
    """Human-resolvable exact scenario-stage lineage retained in ModelSpec."""

    schema_version: Literal["model-spec-scenario-set-binding.v1"] = (
        "model-spec-scenario-set-binding.v1"
    )
    scenario_set_id: str = Field(pattern=r"^scn_[0-9a-f]{32}$")
    scenario_set_content_sha256: str
    scenario_stage_receipt_digest: str
    forecast_assumption_set_id: str = Field(pattern=r"^fas_[0-9a-f]{32}$")
    forecast_assumption_set_content_sha256: str
    critical_factors_artifact_id: str = Field(min_length=1)
    critical_factors_content_sha256: str
    case_probabilities: ModelSpecScenarioCaseProbabilities | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    critical_factors: tuple[ModelSpecScenarioCriticalFactor, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    driver_roles: tuple[ModelSpecScenarioDriverRole, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )

    _scenario_set_content_digest = field_validator("scenario_set_content_sha256")(
        _validate_named_sha256("scenario_set_content_sha256")
    )
    _scenario_stage_receipt_digest = field_validator(
        "scenario_stage_receipt_digest"
    )(_validate_named_sha256("scenario_stage_receipt_digest"))
    _forecast_assumption_set_content_digest = field_validator(
        "forecast_assumption_set_content_sha256"
    )(_validate_named_sha256("forecast_assumption_set_content_sha256"))
    _critical_factors_content_digest = field_validator(
        "critical_factors_content_sha256"
    )(_validate_named_sha256("critical_factors_content_sha256"))


class ModelSpecValuationPolicyBinding(_ProductContract):
    """Human-resolvable exact valuation-stage lineage retained in ModelSpec."""

    schema_version: Literal["model-spec-valuation-policy-binding.v1"] = (
        "model-spec-valuation-policy-binding.v1"
    )
    valuation_policy_id: str = Field(pattern=r"^vpl_[0-9a-f]{32}$")
    valuation_policy_content_sha256: str
    valuation_policy_stage_receipt_digest: str
    forecast_assumption_set_id: str = Field(pattern=r"^fas_[0-9a-f]{32}$")
    forecast_assumption_set_content_sha256: str
    scenario_set_id: str = Field(pattern=r"^scn_[0-9a-f]{32}$")
    scenario_set_content_sha256: str

    _valuation_policy_content_digest = field_validator(
        "valuation_policy_content_sha256"
    )(_validate_named_sha256("valuation_policy_content_sha256"))
    _valuation_policy_stage_receipt_digest = field_validator(
        "valuation_policy_stage_receipt_digest"
    )(_validate_named_sha256("valuation_policy_stage_receipt_digest"))
    _forecast_assumption_set_content_digest = field_validator(
        "forecast_assumption_set_content_sha256"
    )(_validate_named_sha256("forecast_assumption_set_content_sha256"))
    _scenario_set_content_digest = field_validator("scenario_set_content_sha256")(
        _validate_named_sha256("scenario_set_content_sha256")
    )


def _model_spec_error(
    reason: str,
    *,
    section: str | None,
    loc: tuple[str | int, ...],
) -> PydanticCustomError:
    """A ModelSpec validator's rejection, as data: the ``value_error`` text a ``ValueError`` would
    give, the authored section that repairs it (``section``, ``None`` when no authored section
    does) and the offending path in the spec (``loc``). Model validators get no field ``loc`` from
    pydantic, so this is the one place the failing section is known."""
    return PydanticCustomError(
        "value_error",
        "Value error, {reason}",
        {"reason": reason, "section": section, "loc": list(loc)},
    )


class _ModelSpecV3_0Core(_ProductContract):
    """Server-assembled financial judgment consumed by the compiler."""

    schema_version: Annotated[Literal["3.0"], FieldDisposition.SERVER_DERIVED]
    ticker: Annotated[str, FieldDisposition.SERVER_DERIVED]
    predecessor: Annotated[
        ModelVersionRef | None,
        FieldDisposition.SERVER_DERIVED,
    ] = None
    idempotency_key: Annotated[
        str | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=None, min_length=1)
    business_model_revision_ref: Annotated[
        BusinessModelRevisionRef,
        FieldDisposition.SERVER_DERIVED,
    ]
    driver_assumption_plan_sha256: Annotated[str, FieldDisposition.SERVER_DERIVED]
    source_bundle_sha256: Annotated[str, FieldDisposition.SERVER_DERIVED]
    profile_id: Annotated[str, FieldDisposition.SERVER_DERIVED] = Field(min_length=1)
    fiscal_axis: Annotated[FiscalAxis, FieldDisposition.SERVER_DERIVED]
    historical_driver_seed_projection: Annotated[
        ModelSpecHistoricalDriverSeedProjection | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=None, exclude_if=lambda value: value is None)
    historical_foundation_fact_projection: Annotated[
        ModelSpecHistoricalFoundationFactProjection | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=None, exclude_if=lambda value: value is None)
    forecast_assumption_set_binding: Annotated[
        ModelSpecForecastAssumptionSetBinding | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=None, exclude_if=lambda value: value is None)
    scenario_set_binding: Annotated[
        ModelSpecScenarioSetBinding | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    valuation_policy_binding: Annotated[
        ModelSpecValuationPolicyBinding | None,
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    historical_normalization_decisions: Annotated[
        tuple[HistoricalNormalizationDecision, ...],
        FieldDisposition.JUDGMENT,
    ] = ()
    forecast_assumptions: Annotated[
        tuple[Assumption, ...],
        FieldDisposition.JUDGMENT,
    ] = ()
    scenarios: Annotated[
        tuple[ScenarioInputs, ...],
        FieldDisposition.SERVER_DERIVED,
    ] = ()
    scenario_reviews: Annotated[
        tuple[ScenarioReview, ...],
        FieldDisposition.JUDGMENT,
    ] = ()
    valuation_controls: Annotated[tuple[Valuation, ...], FieldDisposition.JUDGMENT] = ()
    valuation_policy: Annotated[
        ModelValuationPolicy | None,
        FieldDisposition.JUDGMENT,
    ] = None
    #: Decode of stored payloads only (no writer, no reader); see
    #: :class:`DegradedModelChoice`.
    degraded_choices: Annotated[
        tuple[DegradedModelChoice, ...],
        FieldDisposition.SERVER_DERIVED,
    ] = Field(default=(), exclude_if=lambda value: not value)
    #: Answers to declinable warnings (``apply_declinations``): one projected row
    #: per BusinessModel ``declined_series`` entry. The analyst's ``decline``
    #: operations live in the working document (``document.declinations``).
    declinations: Annotated[
        tuple[Declination, ...],
        FieldDisposition.JUDGMENT,
    ] = Field(default=(), exclude_if=lambda value: not value)
    analyst_limitations: Annotated[tuple[str, ...], FieldDisposition.JUDGMENT] = ()
    declared_gaps: Annotated[
        tuple[BuildDiagnostic, ...],
        FieldDisposition.SERVER_DERIVED,
    ] = Field(
        default=(),
        max_length=32,
    )

    @model_validator(mode="after")
    def _canonicalize_forecast_assumption_semantics(self) -> "_ModelSpecV3_0Core":
        from .assumption_value_semantics import (
            canonicalize_model_spec_assumption_semantics,
        )

        canonical = canonicalize_model_spec_assumption_semantics(
            {"forecast_assumptions": self.forecast_assumptions}
        )["forecast_assumptions"]
        assumptions = tuple(Assumption.model_validate(row) for row in canonical)
        if assumptions != self.forecast_assumptions:
            object.__setattr__(self, "forecast_assumptions", assumptions)
        return self

    @field_validator("ticker", mode="before")
    @classmethod
    def _normalize_ticker(cls, value: object) -> str:
        return normalize_contract_ticker(value)

    @field_validator("driver_assumption_plan_sha256")
    @classmethod
    def _plan_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="driver_assumption_plan_sha256",
        )

    @field_validator("source_bundle_sha256")
    @classmethod
    def _source_bundle_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="source_bundle_sha256")

    @field_validator("analyst_limitations")
    @classmethod
    def _analyst_limitations_non_blank(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if any(not item.strip() for item in value):
            raise ValueError("analyst limitations must be non-empty strings")
        return value

    @model_validator(mode="after")
    def _match_business_model_ticker(self) -> "_ModelSpecV3_0Core":
        if (
            self.business_model_revision_ref.business_model_id
            != f"{self.ticker}_business_model"
        ):
            raise _model_spec_error(
                "business_model_revision_ref must equal ModelSpec ticker",
                section=None,
                loc=("business_model_revision_ref",),
            )
        forecast_period_ids = tuple(
            period.period_id for period in self.fiscal_axis.forecast_periods
        )
        seed_projection = self.historical_driver_seed_projection
        fact_projection = self.historical_foundation_fact_projection
        forecast_binding = self.forecast_assumption_set_binding
        scenario_binding = self.scenario_set_binding
        valuation_binding = self.valuation_policy_binding
        if seed_projection is not None and (
            forecast_binding is None
            or (
                seed_projection.historical_foundation_artifact_id,
                seed_projection.historical_foundation_content_sha256,
            )
            != (
                forecast_binding.historical_foundation_artifact_id,
                forecast_binding.historical_foundation_content_sha256,
            )
        ):
            raise _model_spec_error(
                "historical seed projection must match exact forecast-set HFF binding",
                section="forecast_assumptions",
                loc=("historical_driver_seed_projection",),
            )
        if seed_projection is not None:
            terminal_historical_period = self.fiscal_axis.historical_periods[-1]
            if terminal_historical_period.period_type != "annual":
                raise _model_spec_error(
                    "historical driver seeds require an annual terminal period",
                    section=None,
                    loc=("fiscal_axis", "historical_periods"),
                )
            if any(
                row.period_id != terminal_historical_period.period_id
                for row in seed_projection.requirements
            ):
                raise _model_spec_error(
                    "historical driver seeds must target the exact terminal "
                    "historical period",
                    section=None,
                    loc=("historical_driver_seed_projection", "requirements"),
                )
        if fact_projection is not None and (
            forecast_binding is None
            or (
                fact_projection.historical_foundation_artifact_id,
                fact_projection.historical_foundation_content_sha256,
            )
            != (
                forecast_binding.historical_foundation_artifact_id,
                forecast_binding.historical_foundation_content_sha256,
            )
        ):
            raise _model_spec_error(
                "historical fact projection must match exact forecast-set HFF binding",
                section="forecast_assumptions",
                loc=("historical_foundation_fact_projection",),
            )
        if scenario_binding is not None and (
            forecast_binding is None
            or (
                scenario_binding.forecast_assumption_set_id,
                scenario_binding.forecast_assumption_set_content_sha256,
            )
            != (
                forecast_binding.forecast_assumption_set_id,
                forecast_binding.forecast_assumption_set_content_sha256,
            )
        ):
            raise _model_spec_error(
                "scenario-set binding must match exact forecast-set binding",
                section="scenario_reviews",
                loc=("scenario_set_binding",),
            )
        if valuation_binding is not None and (
            forecast_binding is None
            or scenario_binding is None
            or (
                valuation_binding.forecast_assumption_set_id,
                valuation_binding.forecast_assumption_set_content_sha256,
            )
            != (
                forecast_binding.forecast_assumption_set_id,
                forecast_binding.forecast_assumption_set_content_sha256,
            )
            or (
                valuation_binding.scenario_set_id,
                valuation_binding.scenario_set_content_sha256,
            )
            != (
                scenario_binding.scenario_set_id,
                scenario_binding.scenario_set_content_sha256,
            )
        ):
            raise _model_spec_error(
                "valuation-policy binding must match exact forecast/scenario bindings",
                section="valuation_policy",
                loc=("valuation_policy_binding",),
            )
        if valuation_binding is not None and (
            self.valuation_policy is None
            or self.valuation_policy.terminal_growth_cases is None
            or self.valuation_policy.exit_multiple_cases is None
            or len(self.valuation_controls) != 1
            or self.valuation_controls[0].method != "dcf"
            or self.valuation_controls[0].terminal_growth_rate is None
            or self.valuation_controls[0].terminal_multiple is None
        ):
            raise _model_spec_error(
                "valuation-policy binding requires its complete deterministic projection",
                section="valuation_policy",
                loc=("valuation_policy_binding",),
            )
        if valuation_binding is not None:
            assert self.valuation_policy is not None
            assert self.valuation_policy.terminal_growth_cases is not None
            assert self.valuation_policy.exit_multiple_cases is not None
            control = self.valuation_controls[0]
            if (
                Decimal(str(control.terminal_growth_rate))
                != self.valuation_policy.terminal_growth_cases.base
                or Decimal(str(control.terminal_multiple))
                != self.valuation_policy.exit_multiple_cases.base
            ):
                raise _model_spec_error(
                    "valuation control bases must match exact policy terminal cases",
                    section="valuation_policy",
                    loc=("valuation_controls", 0),
                )
        seen_assumption_ids: set[str | None] = set()
        for index, item in enumerate(self.forecast_assumptions):
            if item.assumption_id in seen_assumption_ids:
                raise _model_spec_error(
                    "forecast assumption IDs must be unique",
                    section="forecast_assumptions",
                    loc=("forecast_assumptions", index, "assumption_id"),
                )
            seen_assumption_ids.add(item.assumption_id)
        for index, item in enumerate(self.forecast_assumptions):
            if getattr(item, "period_ids", ()) != forecast_period_ids:
                raise _model_spec_error(
                    "every forecast assumption must cover the complete forecast axis",
                    section="forecast_assumptions",
                    loc=("forecast_assumptions", index, "period_ids"),
                )
        for index, item in enumerate(self.forecast_assumptions):
            missing = next(
                (
                    name
                    for name, absent in (
                        ("period_values", item.value is None and item.period_values is None),
                        ("unit", item.unit is None),
                        ("rationale", item.rationale is None),
                        ("confidence", item.confidence is None),
                        ("evidence_basis", getattr(item, "evidence_basis", None) is None),
                    )
                    if absent
                ),
                None,
            )
            if missing is not None:
                raise _model_spec_error(
                    "reviewed profile forecast assumptions require values, units, "
                    "evidence basis, rationale, and confidence",
                    section="forecast_assumptions",
                    loc=("forecast_assumptions", index, missing),
                )
        forecast_years = {
            period.fiscal_year for period in self.fiscal_axis.forecast_periods
        }
        for index, item in enumerate(self.forecast_assumptions):
            if item.period_values is not None and set(item.period_values) != forecast_years:
                raise _model_spec_error(
                    "reviewed profile assumption series must cover the complete "
                    "forecast axis",
                    section="forecast_assumptions",
                    loc=("forecast_assumptions", index, "period_values"),
                )
        scenarios_by_name = {item.name: item for item in self.scenarios}
        reviews_by_name = {item.scenario_name: item for item in self.scenario_reviews}
        if len(scenarios_by_name) != len(self.scenarios):
            raise _model_spec_error(
                "scenario names must be unique",
                section="scenario_reviews",
                loc=("scenarios",),
            )
        if len(reviews_by_name) != len(self.scenario_reviews):
            raise _model_spec_error(
                "scenario review names must be unique",
                section="scenario_reviews",
                loc=("scenario_reviews",),
            )
        if (set(scenarios_by_name) | set(reviews_by_name)) - {"base", "bull", "bear"}:
            raise _model_spec_error(
                "authored scenarios must name base, bull, or bear",
                section="scenario_reviews",
                loc=("scenario_reviews",),
            )
        base_scenario = scenarios_by_name.get("base")
        base_review = reviews_by_name.get("base")
        base_defect_field = None
        if scenario_binding is not None or self.scenarios or self.scenario_reviews:
            if base_scenario is None or base_review is None:
                base_defect_field = "scenario_reviews.base"
            elif not base_scenario.description:
                base_defect_field = "scenarios.base.description"
            elif not base_review.description:
                base_defect_field = "scenario_reviews.base.description"
            elif not base_review.overrides:
                base_defect_field = "scenario_reviews.base.overrides"
            elif any(
                override.period_ids != forecast_period_ids
                for override in base_review.overrides
            ):
                base_defect_field = "scenario_reviews.base.overrides"
        if base_defect_field is not None:
            # Base is the forecast itself, not an optional comparison case.
            # The repair names real assumptions and preserves their values. Bull
            # and bear must override exactly the base's assumption set, so the
            # base overrides the union of the authored bull/bear assumptions;
            # with no bull/bear review, every forecast assumption is a base
            # override.
            base_description = "Base case uses the forecast assumptions unchanged."
            case_assumption_ids = {
                override.assumption_id
                for name in ("bull", "bear")
                if name in reviews_by_name
                for override in reviews_by_name[name].overrides
            }
            base_overrides = [
                {
                    "assumption_id": assumption.assumption_id,
                    "target_control_id": assumption.driver,
                    "period_ids": assumption.period_ids,
                    "unit": assumption.unit,
                    "value": (
                        assumption.value
                        if assumption.value is not None
                        else dict(assumption.period_values or {})
                    ),
                    "direction": "base",
                    "rationale": (
                        f"Uses forecast assumption {assumption.assumption_id} unchanged."
                    ),
                    "evidence_basis": "analyst_judgment",
                    "evidence_ids": (),
                    "limitations": "Inherits the forecast assumption's limitations.",
                }
                for assumption in self.forecast_assumptions
                if not case_assumption_ids
                or assumption.assumption_id in case_assumption_ids
            ]
            raise PydanticCustomError(
                "invalid_judgment",
                "reviewed scenarios require a complete base scenario and matching base review",
                {
                    "field": base_defect_field,
                    "invariant": "judgment_missing",
                    "skeleton": {
                        "scenarios": [
                            {
                                "name": "base",
                                "description": base_description,
                                "assumptions": {
                                    override["target_control_id"]: override["value"]
                                    for override in base_overrides
                                },
                            }
                        ],
                        "scenario_reviews": [
                            {
                                "scenario_name": "base",
                                "description": base_description,
                                "overrides": base_overrides,
                            }
                        ],
                    },
                },
            )
        for review_index, review in enumerate(self.scenario_reviews):
            scenario = scenarios_by_name.get(review.scenario_name)
            if (
                scenario is not None
                and scenario.description
                and review.description
                and review.description != scenario.description
            ):
                raise PydanticCustomError(
                    "invalid_judgment",
                    "reviewed scenario description must match its scenario description",
                    {
                        "field": f"scenario_reviews.{review.scenario_name}.description",
                        "invariant": "data_integrity",
                        "skeleton": {"description": scenario.description},
                    },
                )
            for override in review.overrides:
                if review.scenario_name == "base" and override.direction != "base":
                    raise PydanticCustomError(
                        "invalid_judgment",
                        "reviewed base scenario overrides require base direction",
                        {
                            "field": f"scenario_reviews.base.overrides.{override.assumption_id}.direction",
                            "invariant": "data_integrity",
                            "skeleton": {"direction": "base"},
                        },
                    )
            expected = {item.target_control_id: item.value for item in review.overrides}
            if len(expected) != len(review.overrides):
                raise _model_spec_error(
                    "scenario target controls must be unique",
                    section="scenario_reviews",
                    loc=("scenario_reviews", review_index, "overrides"),
                )
            if scenario is not None and review.overrides and scenario.assumptions != expected:
                raise _model_spec_error(
                    "scenario assumptions must exactly lower the declared overrides",
                    section="scenario_reviews",
                    loc=("scenario_reviews", review_index, "overrides"),
                )
        assumptions_by_id = {
            item.assumption_id: item for item in self.forecast_assumptions
        }
        base_overrides_by_assumption_id = (
            {item.assumption_id: item for item in base_review.overrides}
            if base_review is not None
            else {}
        )
        base_review_loc: tuple[str | int, ...] = next(
            (
                ("scenario_reviews", index, "overrides")
                for index, item in enumerate(self.scenario_reviews)
                if item.scenario_name == "base"
            ),
            ("scenario_reviews",),
        )
        if base_review is not None and len(base_overrides_by_assumption_id) != len(
            base_review.overrides
        ):
            raise _model_spec_error(
                "reviewed base scenario override assumption_ids must be unique",
                section="scenario_reviews",
                loc=base_review_loc,
            )
        for assumption_id, override in base_overrides_by_assumption_id.items():
            assumption = assumptions_by_id.get(assumption_id)
            if assumption is None:
                raise _model_spec_error(
                    "reviewed base scenario override must name an existing "
                    f"forecast assumption_id: {assumption_id}",
                    section="scenario_reviews",
                    loc=base_review_loc,
                )
            assumption_value = (
                assumption.value
                if assumption.value is not None
                else dict(assumption.period_values or {})
            )
            if _review_value_by_period(
                override.value, override.period_ids
            ) != _review_value_by_period(assumption_value, assumption.period_ids):
                raise _model_spec_error(
                    "reviewed base scenario override value must equal its "
                    f"forecast assumption value: {assumption_id}",
                    section="scenario_reviews",
                    loc=base_review_loc,
                )
        for review_index, review in enumerate(self.scenario_reviews):
            if review.scenario_name == "base":
                continue
            for override in review.overrides:
                assumption = assumptions_by_id.get(override.assumption_id)
                if assumption is None:
                    raise _model_spec_error(
                        "reviewed non-base scenario override must name an existing "
                        f"forecast assumption_id: {override.assumption_id}",
                        section="scenario_reviews",
                        loc=("scenario_reviews", review_index, "overrides"),
                    )
                current_values = _review_value_by_period(
                    override.value, override.period_ids
                )
                assumption_value = (
                    assumption.value
                    if assumption.value is not None
                    else dict(assumption.period_values or {})
                )
                base_values = _review_value_by_period(
                    assumption_value, assumption.period_ids
                )
                if override.direction == "increase" and not all(
                    current_values[period] > base_values[period]
                    for period in current_values.keys() & base_values.keys()
                ):
                    raise PydanticCustomError(
                        "invalid_judgment",
                        "reviewed scenario increase override must be strictly "
                        "greater than its base value: {case}/{assumption_id}",
                        {
                            "case": review.scenario_name,
                            "assumption_id": override.assumption_id,
                            "field": f"scenario_reviews.{review.scenario_name}.overrides.{override.assumption_id}.value",
                            "invariant": "data_integrity",
                            "skeleton": {
                                "direction": override.direction,
                                "base_values": base_values,
                            },
                        },
                    )
                if override.direction == "decrease" and not all(
                    current_values[period] < base_values[period]
                    for period in current_values.keys() & base_values.keys()
                ):
                    raise PydanticCustomError(
                        "invalid_judgment",
                        "reviewed scenario decrease override must be strictly "
                        "less than its base value: {case}/{assumption_id}",
                        {
                            "case": review.scenario_name,
                            "assumption_id": override.assumption_id,
                            "field": f"scenario_reviews.{review.scenario_name}.overrides.{override.assumption_id}.value",
                            "invariant": "data_integrity",
                            "skeleton": {
                                "direction": override.direction,
                                "base_values": base_values,
                            },
                        },
                    )
        dcf_controls = [
            item for item in self.valuation_controls if item.method == "dcf"
        ]
        if self.valuation_policy is not None:
            complete = (
                len(dcf_controls) == 1
                and dcf_controls[0].terminal_growth_rate is not None
                and dcf_controls[0].terminal_multiple is not None
            )
            if valuation_binding is None:
                complete = complete and dcf_controls[0].wacc is not None
            else:
                control = dcf_controls[0]
                complete = complete and all(
                    getattr(control, field) is not None
                    for field in ("beta_floor",)
                )
                complete = complete and control.wacc is None
            if not complete:
                raise _model_spec_error(
                    "reviewed profile requires one complete DCF valuation control",
                    section="valuation_policy",
                    loc=("valuation_controls",),
                )
        seen_decision_ids: set[str] = set()
        for index, item in enumerate(self.historical_normalization_decisions):
            if item.decision_id in seen_decision_ids:
                raise _model_spec_error(
                    "normalization decision IDs must be unique",
                    section="historical_normalization_decisions",
                    loc=("historical_normalization_decisions", index, "decision_id"),
                )
            seen_decision_ids.add(item.decision_id)
        return self

    def bind_successor(
        self,
        *,
        predecessor: ModelVersionRef,
        idempotency_key: str,
    ) -> "ModelSpec":
        """Bind one authored specification to its exact immutable predecessor."""

        validated_predecessor = ModelVersionRef.model_validate(predecessor)
        normalized_key = str(idempotency_key or "").strip()
        if not normalized_key:
            raise ValueError("ModelSpec successor idempotency_key is required")
        return ModelSpec.model_validate(
            self.model_copy(
                update={
                    "predecessor": validated_predecessor,
                    "idempotency_key": normalized_key,
                }
            )
        )


class ModelSpec(_ModelSpecV3_0Core):
    """The only canonical ModelSpec accepted by compiler boundaries."""

    valuation_family_authorizations: Annotated[
        tuple[ValuationFamilyAuthorization, ...],
        FieldDisposition.JUDGMENT,
    ] = ()

    @model_validator(mode="after")
    def _valuation_authorization_closure(self) -> "ModelSpec":
        authorizations = {
            item.method: item for item in self.valuation_family_authorizations
        }
        if len(authorizations) != len(self.valuation_family_authorizations):
            raise _model_spec_error(
                "valuation family authorizations must be unique by method",
                section="valuation_family_authorizations",
                loc=("valuation_family_authorizations",),
            )
        if self.valuation_policy is None:
            return self
        controls = {
            item.method: item for item in self.valuation_policy.relative_methods
        }
        for method, control in controls.items():
            authorization = authorizations.get(method)
            if control.selection_basis == "none":
                if authorization is not None:
                    raise _model_spec_error(
                        "unselected relative method forbids a family authorization",
                        section="valuation_family_authorizations",
                        loc=("valuation_family_authorizations",),
                    )
                continue
            if authorization is None:
                raise _model_spec_error(
                    "selected relative method requires a family authorization",
                    section="valuation_family_authorizations",
                    loc=("valuation_family_authorizations",),
                )
            if (
                authorization.selection_basis != control.selection_basis
                or authorization.rationale != control.rationale
                or authorization.evidence_ids != control.evidence_ids
            ):
                raise _model_spec_error(
                    "relative control and family authorization must agree",
                    section="valuation_family_authorizations",
                    loc=("valuation_family_authorizations",),
                )
        if set(authorizations) - set(controls):
            raise _model_spec_error(
                "reviewed valuation authorizations must match relative methods",
                section="valuation_family_authorizations",
                loc=("valuation_family_authorizations",),
            )
        return self


def validate_source_bundle_binding(
    source_bundle: SourceBundle,
    model_spec: ModelSpec,
    *,
    persisted_source_bundle_payload: Mapping[str, Any] | None = None,
    persisted_source_bundle_wire_digest: str | None = None,
) -> None:
    """Check only the specification's digest and coverage-profile binding.

    ``valuation_selection_findings`` records unresolved peer-derived selections
    and unbundled ``evidence_ids`` as ``valuation_selection_unresolved`` findings
    at compile/publish; neither is a digest or profile binding mismatch.
    """

    from .render_state import compiler_input_content_hash

    if (persisted_source_bundle_payload is None) != (
        persisted_source_bundle_wire_digest is None
    ):
        raise ValueError(
            "persisted SourceBundle binding requires both payload and wire digest"
        )
    if persisted_source_bundle_payload is not None:
        assert persisted_source_bundle_wire_digest is not None
        verified_source_bundle = decode_persisted_source_bundle_payload(
            persisted_source_bundle_payload,
            expected_wire_digest=persisted_source_bundle_wire_digest,
        )
        if verified_source_bundle != source_bundle:
            raise ValueError(
                "persisted SourceBundle wire payload differs from decoded binding input"
            )
    # A sealed row binds by its stored bytes' digest; only a bundle that was
    # never stored has no digest but its own canonical dump.
    actual = (
        compiler_input_content_hash(source_bundle)
        if persisted_source_bundle_wire_digest is None
        else f"sha256:{str(persisted_source_bundle_wire_digest).removeprefix('sha256:')}"
    )
    expected = f"sha256:{model_spec.source_bundle_sha256}"
    actual_profile_id = source_bundle.coverage_profile.profile_id
    if expected != actual or model_spec.profile_id != actual_profile_id:
        raise SourceBundleBindingError(
            expected_sha=expected,
            actual_sha=actual,
            expected_profile_id=model_spec.profile_id,
            actual_profile_id=actual_profile_id,
        )


def resolve_model_spec_business_model_revision(
    model_spec: ModelSpec,
    revision: BusinessModelRevision,
) -> BusinessModelRevision:
    """Validate the exact server-resolved revision pinned by a ModelSpec."""

    spec = ModelSpec.model_validate(model_spec)
    value = BusinessModelRevision.model_validate(revision)
    if (
        value.ticker != spec.ticker
        or value.ref != spec.business_model_revision_ref
        or value.driver_assumption_plan_sha256 != spec.driver_assumption_plan_sha256
    ):
        raise ValueError(
            "ModelSpec BusinessModel revision pins do not match the resolved revision"
        )
    return value




def _require_unique_provenance_cells(
    cells: tuple[CellLineageV1_0 | CellLineage, ...],
) -> None:
    identities = tuple((entry.sheet, entry.cell) for entry in cells)
    if len(identities) != len(set(identities)):
        raise ValueError("provenance cells must be unique by sheet and cell")


class ProvenanceMapV1_0(_ProductContract):
    schema_version: Literal["1.0"] = "1.0"
    source_bundle_digest: str
    model_spec_digest: str
    cells: tuple[CellLineageV1_0, ...]

    _source_bundle_digest = field_validator("source_bundle_digest")(
        _validate_named_sha256("source_bundle_digest")
    )
    _model_spec_digest = field_validator("model_spec_digest")(
        _validate_named_sha256("model_spec_digest")
    )

    @model_validator(mode="after")
    def _require_unique_cells(self) -> "ProvenanceMapV1_0":
        _require_unique_provenance_cells(self.cells)
        return self


class ProvenanceMap(_ProductContract):
    schema_version: Literal["1.1"] = "1.1"
    source_bundle_digest: str
    model_spec_digest: str
    cells: tuple[CellLineage, ...]

    _source_bundle_digest = field_validator("source_bundle_digest")(
        _validate_named_sha256("source_bundle_digest")
    )
    _model_spec_digest = field_validator("model_spec_digest")(
        _validate_named_sha256("model_spec_digest")
    )

    @model_validator(mode="after")
    def _require_unique_cells(self) -> "ProvenanceMap":
        _require_unique_provenance_cells(self.cells)
        return self


class ProvenanceMapRebuildRequired(ValueError):
    """Stored ProvenanceMap bytes cannot be read by this binary's contract."""

    code = "provenance_map_rebuild_required"
    fix = (
        "rebuild and republish this model on the current contract generation; "
        "stored ModelVersions are never migrated"
    )


def decode_provenance_map(payload: Any) -> ProvenanceMap:
    """Decode V1.1 or losslessly upgrade a V1.0 provenance payload."""

    if isinstance(payload, ProvenanceMap):
        return payload
    if isinstance(payload, (str, bytes, bytearray)):
        payload = json.loads(payload)
    if isinstance(payload, BaseModel):
        payload = payload.model_dump(mode="json")
    if not isinstance(payload, Mapping):
        raise TypeError("provenance payload must be a mapping or JSON")
    body = dict(payload)
    version = body.get("schema_version", "1.0")
    if type(version) is not str or version not in ("1.0", "1.1"):
        raise ProvenanceMapRebuildRequired(
            f"unsupported ProvenanceMap schema_version: {version!r}; "
            f"the stored ProvenanceMap is rebuild-required — {ProvenanceMapRebuildRequired.fix}"
        )
    try:
        if version == "1.0":
            legacy = ProvenanceMapV1_0.model_validate(body)
            upgraded = legacy.model_dump(mode="python")
            upgraded["schema_version"] = "1.1"
            for cell in upgraded["cells"]:
                if cell["lineage_status"] != "complete":
                    continue
                if any(
                    cell[field]
                    for field in (
                        "observation_ids",
                        "assumption_ids",
                        "source_refs",
                        "evidence_refs",
                        "assumption_refs",
                        "valuation_refs",
                    )
                ):
                    continue
                # V1.0 admitted formula-only "complete" cells. V1.1's closed
                # origin contract cannot infer a reviewed compiler kind during a
                # read-time upgrade, so preserve the bytes and fail closed in the
                # canonical in-memory view.
                cell["lineage_status"] = "missing_source"
            return ProvenanceMap.model_validate(upgraded)
        return ProvenanceMap.model_validate(body)
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "<root>"
        raise ProvenanceMapRebuildRequired(
            "stored ProvenanceMap does not satisfy this binary's contract "
            f"({exc.error_count()} rejections, first at {location}: "
            f"{first['msg']}); the stored ProvenanceMap is rebuild-required — "
            f"{ProvenanceMapRebuildRequired.fix}"
        ) from exc

class ValidationReport(_ProductContract):
    """One build's findings and the one verdict derived from them.

    ``status`` is ``derive_build_verdict(findings)``; ``diagnostic_report`` is
    the raw historical payload whose non-ok kinds are already findings.

    The five recalculation-evidence fields (engine, its version, and the
    formula, unexpected-error and divergence counts) record an evaluation of
    the rendered workbook's formulas by an external engine (publication runs
    LibreOffice). They are all ``None`` when no such evaluation ran
    (``check_model`` recalculates with the DependencyGraph only; a failed
    engine is a finding) and all set when one did: ``None`` is "not
    evaluated", never zero. ``intentional_na_count`` is not one of them: it
    counts the authored ``NA()`` formulas in the render contract.
    """

    schema_version: Literal["2.2"] = "2.2"
    status: BuildVerdict
    findings: tuple[Finding, ...] = ()
    diagnostic_report: dict[str, Any] | None = None
    validated_at: datetime
    recalculation_engine: str | None = Field(min_length=1)
    recalculation_engine_version: str | None = Field(min_length=1)
    recalculated_formula_cell_count: int | None = Field(ge=0)
    intentional_na_count: int = Field(ge=0)
    unexpected_excel_error_count: int | None = Field(ge=0)
    recalculation_divergence_count: int | None = Field(ge=0)
    readback_invariants_evaluated: bool
    readback_invariant_finding_count: int = Field(ge=0)

    @model_validator(mode="after")
    def _status_is_the_findings_verdict(self) -> "ValidationReport":
        derived = derive_build_verdict(self.findings)
        if self.status != derived:
            raise ValueError(
                f"validation status {self.status!r} must equal the findings "
                f"verdict {derived!r}"
            )
        return self

    @model_validator(mode="after")
    def _recalculation_evidence_is_all_or_nothing(self) -> "ValidationReport":
        evidence = (
            self.recalculation_engine,
            self.recalculation_engine_version,
            self.recalculated_formula_cell_count,
            self.unexpected_excel_error_count,
            self.recalculation_divergence_count,
        )
        if any(value is None for value in evidence) and any(
            value is not None for value in evidence
        ):
            raise ValueError(
                "recalculation evidence fields are either all evaluated or all None"
            )
        return self

    @property
    def error_count(self) -> int:
        return len(error_findings(self.findings))

    @property
    def uncleared_warning_count(self) -> int:
        return len(uncleared_warnings(self.findings))


class ValidationReportRebuildRequired(ValueError):
    """Stored validation bytes predate the only supported wire version.

    Kept for data integrity: a 2.1 report stored only blocking
    ``BuildDiagnostic`` rows under a ``pass``/``blocked`` status and never
    recorded warning or declination findings, so no read of it can derive a
    2.2 ``findings`` verdict — decoding it as 2.2 would present an unrecorded
    warning set as ``BUILD_CLEAN``. The repair is a fresh build of the same
    inputs (``build_model``), which writes a 2.2 report.
    """

    code = "validation_report_rebuild_required"


def decode_validation_report_payload(
    payload: Mapping[str, Any],
) -> ValidationReport:
    """Decode only exact ValidationReport 2.2 JSON values without coercion."""

    observed = payload.get("schema_version") if isinstance(payload, Mapping) else None
    if type(observed) is not str or observed != "2.2":
        raise ValidationReportRebuildRequired(
            "ValidationReport schema version is rebuild-required; "
            f"observed={observed!r}, expected='2.2'; "
            "next_call: build_model to rebuild the stored version"
        )
    strict_json = json.dumps(
        dict(payload),
        allow_nan=False,
        separators=(",", ":"),
    )
    return ValidationReport.model_validate_json(strict_json, strict=True)


class HandoffBuildModelVersionOrigin(_ProductContract):
    """Authenticated handoff locator for a normal build publication."""

    kind: Literal["handoff_build"] = "handoff_build"
    handoff_id: int = Field(gt=0)
    handoff_version: int = Field(gt=0)


class SuccessorModelVersionOrigin(_ProductContract):
    """Durable successor identity and the semantic base it was authorized from."""

    kind: Literal["successor"] = "successor"
    door: str = Field(min_length=1)
    proposal_handle: str = Field(min_length=1)
    semantic_base: ModelVersionRef


class LegacyModelVersionOrigin(_ProductContract):
    """Non-replayable migration stamp for pre-origin ModelVersions."""

    kind: Literal["legacy"] = "legacy"




class ModelVersionV1_0(_ProductContract):
    """Exact pre-origin ModelVersion contract retained for schema migration."""

    schema_version: Literal["1.0"] = "1.0"
    model_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str
    predecessor: ModelVersionRef | None = None
    model_spec_predecessor: ModelVersionRef | None = None
    model_spec_idempotency_key: str | None = Field(default=None, min_length=1)
    request_digest: str
    source_bundle_digest: str
    model_spec_digest: str
    compiler_version: str = Field(min_length=1)
    template_version: str = Field(min_length=1)
    workbook: ArtifactLocation
    semantic_sidecar: ArtifactLocation
    provenance_map: ArtifactLocation
    validation_report: ArtifactLocation
    status: ModelStatus
    created_by: str = Field(min_length=1)
    created_at: datetime
    committed_at: datetime

    @field_validator("ticker")
    @classmethod
    def _require_canonical_ticker(cls, value: str) -> str:
        return require_canonical_contract_ticker(value)

    _request_digest = field_validator("request_digest")(
        _validate_named_sha256("request_digest")
    )
    _source_bundle_digest = field_validator("source_bundle_digest")(
        _validate_named_sha256("source_bundle_digest")
    )
    _model_spec_digest = field_validator("model_spec_digest")(
        _validate_named_sha256("model_spec_digest")
    )

    @model_validator(mode="after")
    def _validate_version(self) -> "ModelVersionV1_0":
        if self.predecessor is not None:
            if self.predecessor.model_id != self.model_id:
                raise ValueError("predecessor must belong to the same model")
            if self.predecessor.version >= self.version:
                raise ValueError("predecessor version must precede this version")
        if self.model_spec_predecessor != self.predecessor:
            if (
                self.model_spec_predecessor is not None
                or self.model_spec_idempotency_key is not None
            ):
                raise ValueError(
                    "ModelSpec predecessor must equal the ModelVersion predecessor"
                )
        if (
            self.model_spec_predecessor is None
            and self.model_spec_idempotency_key is not None
        ):
            raise ValueError(
                "ModelSpec idempotency key requires a ModelSpec predecessor"
            )
        if (
            self.model_spec_predecessor is not None
            and self.model_spec_idempotency_key is None
        ):
            raise ValueError("ModelSpec successor requires an idempotency key")
        if self.committed_at < self.created_at:
            raise ValueError("committed_at must not precede created_at")
        return self


class ModelVersion(_ProductContract):
    """The sole immutable product commit boundary."""

    schema_version: Literal["2.0"] = "2.0"
    model_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str
    predecessor: ModelVersionRef | None = None
    model_spec_predecessor: ModelVersionRef | None = None
    model_spec_idempotency_key: str | None = Field(default=None, min_length=1)
    request_digest: str
    source_bundle_digest: str
    model_spec_digest: str
    compiler_version: str = Field(min_length=1)
    template_version: str = Field(min_length=1)
    workbook: ArtifactLocation
    semantic_sidecar: ArtifactLocation
    provenance_map: ArtifactLocation
    validation_report: ArtifactLocation
    status: ModelStatus
    created_by: str = Field(min_length=1)
    created_at: datetime
    committed_at: datetime
    origin: (
        HandoffBuildModelVersionOrigin
        | SuccessorModelVersionOrigin
        | LegacyModelVersionOrigin
    ) = Field(discriminator="kind")
    successor_candidate_digest: str | None = None

    @field_validator("ticker")
    @classmethod
    def _require_canonical_ticker(cls, value: str) -> str:
        return require_canonical_contract_ticker(value)

    _request_digest = field_validator("request_digest")(
        _validate_named_sha256("request_digest")
    )
    _source_bundle_digest = field_validator("source_bundle_digest")(
        _validate_named_sha256("source_bundle_digest")
    )
    _model_spec_digest = field_validator("model_spec_digest")(
        _validate_named_sha256("model_spec_digest")
    )

    @field_validator("successor_candidate_digest")
    @classmethod
    def _validate_successor_candidate_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(value, field_name="successor_candidate_digest")

    @model_validator(mode="after")
    def _validate_version(self) -> "ModelVersion":
        """Keep committed sequence separate from successor semantic lineage."""

        if self.predecessor is not None:
            if self.predecessor.model_id != self.model_id:
                raise ValueError("predecessor must belong to the same model")
            if self.predecessor.version >= self.version:
                raise ValueError("predecessor version must precede this version")
        if (
            self.model_spec_predecessor is None
            and self.model_spec_idempotency_key is not None
        ):
            raise ValueError(
                "ModelSpec idempotency key requires a ModelSpec predecessor"
            )
        if (
            self.model_spec_predecessor is not None
            and self.model_spec_idempotency_key is None
        ):
            raise ValueError("ModelSpec successor requires an idempotency key")
        if self.model_spec_predecessor is not None:
            if self.model_spec_predecessor.model_id != self.model_id:
                raise ValueError("ModelSpec predecessor must belong to the same model")
            if self.model_spec_predecessor.version >= self.version:
                raise ValueError(
                    "ModelSpec predecessor version must precede this version"
                )

        if (
            isinstance(self.origin, SuccessorModelVersionOrigin)
            and self.successor_candidate_digest is not None
        ):
            if self.origin.semantic_base.model_id != self.model_id:
                raise ValueError(
                    "successor semantic base must belong to the same model"
                )
            if self.origin.semantic_base.version >= self.version:
                raise ValueError("successor semantic base must precede this version")
            if self.model_spec_predecessor != self.origin.semantic_base:
                raise ValueError(
                    "successor ModelSpec predecessor must equal origin semantic base"
                )
        elif isinstance(self.origin, LegacyModelVersionOrigin):
            if self.model_spec_predecessor != self.predecessor and (
                self.model_spec_predecessor is not None
                or self.model_spec_idempotency_key is not None
            ):
                raise ValueError(
                    "legacy ModelSpec predecessor must equal the ModelVersion predecessor"
                )
        if self.committed_at < self.created_at:
            raise ValueError("committed_at must not precede created_at")
        return self

    @model_validator(mode="after")
    def _validate_origin(self) -> "ModelVersion":
        if isinstance(self.origin, SuccessorModelVersionOrigin):
            if self.successor_candidate_digest is None:
                raise ValueError("successor origin requires successor_candidate_digest")
        elif self.successor_candidate_digest is not None:
            raise ValueError(
                "successor_candidate_digest is valid only for successor origin"
            )
        return self


class AcceptanceDecision(_ProductContract):
    schema_version: Literal["1.0"] = "1.0"
    model_version: ModelVersionRef
    decision: Literal["accepted", "rejected"]
    principal_id: str = Field(min_length=1)
    decided_at: datetime
    expected_pointer_epoch: int = Field(ge=0)
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class OfflineValidation:
    """One offline validation: the recordable report plus what it computed.

    ``report`` is the only part that is ever persisted. ``recalculated_values``
    is the DependencyGraph's numeric semantic cells by item id and period
    (empty when the graph compute failed; the report carries that failure),
    ``recalculation_wall_seconds`` is that compute's wall time, and
    ``recalculation_input_bytes`` / ``recalculation_output_bytes`` are always 0
    (no worker runs). The external LibreOffice evaluation, when publication
    supplies one, is recorded in ``report``, not here.
    None of them enters content-addressed artifact bytes.
    """

    report: ValidationReport
    recalculated_values: dict[str, dict[int, float]]
    recalculation_wall_seconds: float
    recalculation_input_bytes: int
    recalculation_output_bytes: int


__all__ = [
    "SourceFactRail",
    "SourceFactRef",
    "mint_source_fact_ref",
    "AcceptanceDecision",
    "AdapterAssertionKind",
    "AdapterAssertionOutcome",
    "AdapterUnresolvedForRequiredLine",
    "AnnualPeriodCoverage",
    "AnnualRequirementCoverage",
    "ArtifactLocation",
    "BuildDiagnostic",
    "BuildDiagnosticCode",
    "CellLineage",
    "CellLineageV1_0",
    "CompilerOrigin",
    "DegradedModelChoice",
    "DcfMethodControl",
    "HistoricalNormalizationDecision",
    "HistoricalBasisFact",
    "HistoricalReconciliationBinding",
    "HistoricalReconciliationDefinitionRef",
    "HISTORICAL_RECONCILIATION_MEASURES",
    "historical_bridge_rail_measure",
    "EDGAR_API_COVER_FACTS_RESPONSE_ARTIFACT_KIND",
    "EDGAR_API_EV_RESPONSE_ARTIFACT_KINDS",
    "EDGAR_API_EV_RESPONSE_PAIR_FIELDS",
    "EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND",
    "edgar_api_financials_fact_is_undimensioned",
    "HandoffBuildModelVersionOrigin",
    "LegacyModelVersionOrigin",
    "ModelSensitivity",
    "ModelSpec",
    "ModelSpecForecastAssumptionSetBinding",
    "ModelSpecScenarioSetBinding",
    "ModelSpecValuationPolicyBinding",
    "ModelSpecHistoricalFoundationFactProjection",
    "ModelSpecHistoricalFoundationObservation",
    "ModelSpecHistoricalDriverSeedProjection",
    "HistoricalDriverSeedLineageReference",
    "ModelValuationPolicy",
    "ValuationTerminalCaseValues",
    "ValuationNetShareDilutionRates",
    "ValuationNetShareDilutionYearRate",
    "ModelVersion",
    "ModelVersionV1_0",
    "OfflineValidation",
    "PinUnboundPeriod",
    "ProvenanceMap",
    "ProvenanceMapV1_0",
    "SourceBundle",
    "SourceEvidence",
    "StatementSourceMaterial",
    "SOURCE_MEASURES",
    "SourceMeasure",
    "source_measure",
    "SuccessorModelVersionOrigin",
    "SensitivityAxis",
    "RelativeValuationControl",
    "ScenarioOverride",
    "ScenarioReview",
    "ScheduleInputAudit",
    "ScheduleInputAuditArtifact",
    "ScheduleInputAuditNotRecorded",
    "ScheduleInputAuditRow",
    "ScheduleInputFamily",
    "ScheduleInputOrigin",
    "ScheduleInputPeriodAudit",
    "ValuationPeerPolicy",
    "ValuationSummaryPolicy",
    "ValidationReport",
    "ProvenanceMapRebuildRequired",
    "ValidationReportRebuildRequired",
    "WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS",
    "decode_provenance_map",
    "decode_validation_report_payload",
    "resolve_model_spec_business_model_revision",
    "validate_historical_reconciliation_bindings",
    "validate_historical_reconciliation_source_bindings",
]
