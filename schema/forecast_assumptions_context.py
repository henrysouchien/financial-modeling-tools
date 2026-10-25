"""Bounded pre-compile context for forecast-assumption judgment.

The surface carries economic semantics and exact immutable lineage only.  It
cannot express workbook coordinates, current-model identities, compiler row
IDs, or stable control IDs.

Consumed by ``api/research/forecast_assumptions_context_service.py``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .build_model_product_contracts import (
    SourceCoverageDisposition,
    HistoricalBasisFact,
    SourceEvidence,
    validate_historical_reconciliation_bindings,
)
from .business_model_context import (
    BusinessModelContextRelationship,
)
from .business_model import (
    Factor,
    NodeBehavior,
    ObservationBasis,
    RevenueScope,
)
from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_stage import BusinessModelSelectedEvidence
from .finding_vocabulary import Finding
from .forecast_assumption_set import (
    ForecastAssumptionGap,
    ForecastAssumptionSetRef,
    ForecastDriverAssumption,
    ForecastTerminalSummary,
)
from .forecast_control_profile import ForecastControlProfileRef
from .historical_basis_snapshot import HistoricalBasisArtifactRef
from .historical_foundation import (
    HistoricalFactSnapshotRef,
    HistoricalFoundationGap,
    HistoricalFoundationRef,
)
from .coverage_profile import AbsenceCitation
from industry_slice_core.thesis_shared_sources import SourceId
from .provider_port.contracts import FilingDimensionRef
from .historical_driver_seeds import (
    HistoricalDriverSeedDerivationKind,
    HistoricalDriverSeedDisposition,
)
from .model_spec_authoring import AuthoringPeriodValueSeries, Confidence, ScalarValue
from .model_state_identity import validate_sha256_hex
from .models import ModelScale
from industry_slice_core.industry_types import Unit
from .stable_control_identity import FiscalAxis, FiscalPeriod
from .versioned_artifact import canonical_json_bytes


MAX_DRIVERS = 512
MAX_PERIODS = 64
MAX_EVIDENCE = 4096
MAX_GAPS = 512
MAX_BASELINES_PER_PERIOD = 1024
MAX_COVERAGE_PER_PERIOD = 1024
MAX_INHERITED_RATIONALE = 16
MAX_KEY_DECISIONS = 24

SemanticKey = Annotated[str, Field(min_length=1, max_length=256)]
Prose = Annotated[str, Field(min_length=1, max_length=4096)]
ShortText = Annotated[str, Field(min_length=1, max_length=128)]


class _ContextContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class ForecastAssumptionsContextBinding(_ContextContract):
    """Exact inputs named by a copyable locator, never an authority grant.

    The checksum makes accidental corruption visible.  Consumers must still
    re-resolve authenticated current state before trusting the referenced
    accepted HistoricalFoundation.
    """

    schema_version: Literal[
        "forecast-assumptions-context-binding.v3",
        "forecast-assumptions-context-binding.v4",
    ] = (
        "forecast-assumptions-context-binding.v3"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1, max_length=32)
    selected_business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    # ``None`` on a context issued while no HistoricalFoundation is persisted
    # for the bound fact snapshot: the response carries that gap, and no
    # ForecastAssumptionSet is written against it.
    historical_foundation_ref: HistoricalFoundationRef | None
    historical_foundation_stage_receipt_digest: str | None
    historical_fact_snapshot_ref: HistoricalFactSnapshotRef
    forecast_control_profile_ref: ForecastControlProfileRef
    forecast_control_denominator_sha256: str
    context_content_sha256: str

    @field_validator(
        "business_model_stage_receipt_digest",
        "historical_foundation_stage_receipt_digest",
        "forecast_control_denominator_sha256",
        "context_content_sha256",
    )
    @classmethod
    def _digests(cls, value: str | None, info) -> str | None:
        if value is None:
            return value
        return validate_sha256_hex(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _content_addressed(self) -> "ForecastAssumptionsContextBinding":
        expected_version = (
            "forecast-assumptions-context-binding.v4"
            if self.historical_foundation_ref is None
            or self.historical_foundation_ref.schema_version
            == "historical-foundation.v2"
            else "forecast-assumptions-context-binding.v3"
        )
        if self.schema_version != expected_version:
            raise ValueError("forecast context version must match foundation")
        expected = forecast_assumptions_context_content_digest(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
            selected_business_model_revision_ref=(
                self.selected_business_model_revision_ref
            ),
            business_model_selection_epoch=self.business_model_selection_epoch,
            business_model_stage_receipt_digest=(
                self.business_model_stage_receipt_digest
            ),
            historical_foundation_ref=self.historical_foundation_ref,
            historical_foundation_stage_receipt_digest=(
                self.historical_foundation_stage_receipt_digest
            ),
            historical_fact_snapshot_ref=self.historical_fact_snapshot_ref,
            forecast_control_profile_ref=self.forecast_control_profile_ref,
            forecast_control_denominator_sha256=(
                self.forecast_control_denominator_sha256
            ),
        )
        if self.context_content_sha256 != expected:
            raise ValueError("forecast-assumptions context binding digest mismatch")
        return self


def forecast_assumptions_context_content_digest(
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    selected_business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    historical_foundation_ref: HistoricalFoundationRef | None,
    historical_foundation_stage_receipt_digest: str | None,
    historical_fact_snapshot_ref: HistoricalFactSnapshotRef,
    forecast_control_profile_ref: ForecastControlProfileRef,
    forecast_control_denominator_sha256: str,
) -> str:
    schema_version = (
        "forecast-assumptions-context.v4"
        if historical_foundation_ref is None
        or historical_foundation_ref.schema_version == "historical-foundation.v2"
        else "forecast-assumptions-context.v3"
    )
    payload = {
        "schema_version": schema_version,
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "selected_business_model_revision_ref": (
            selected_business_model_revision_ref
        ),
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": (
            business_model_stage_receipt_digest
        ),
        "historical_foundation_ref": historical_foundation_ref,
        "historical_foundation_stage_receipt_digest": (
            historical_foundation_stage_receipt_digest
        ),
        "historical_fact_snapshot_ref": historical_fact_snapshot_ref,
        "forecast_control_profile_ref": forecast_control_profile_ref,
        "forecast_control_denominator_sha256": (
            forecast_control_denominator_sha256
        ),
    }
    return hashlib.sha256(
        schema_version.encode("utf-8") + b"\0" + canonical_json_bytes(payload)
    ).hexdigest()


def encode_forecast_assumptions_context_ref(
    binding: ForecastAssumptionsContextBinding,
) -> str:
    payload = canonical_json_bytes(binding.model_dump(mode="json"))
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    version = 4 if binding.schema_version.endswith(".v4") else 3
    checksum = hashlib.sha256(
        f"forecast-assumptions-context-ref.v{version}".encode("utf-8")
        + b"\0"
        + payload
    ).hexdigest()
    return f"facx{version}.{encoded}.{checksum}"


def decode_forecast_assumptions_context_ref(
    value: str,
) -> ForecastAssumptionsContextBinding:
    try:
        prefix, encoded, checksum = value.split(".", 2)
        if prefix not in {"facx3", "facx4"} or len(checksum) != 64:
            raise ValueError
        version = prefix.removeprefix("facx")
        payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        expected = hashlib.sha256(
            f"forecast-assumptions-context-ref.v{version}".encode("utf-8")
            + b"\0"
            + payload
        ).hexdigest()
        if not hmac.compare_digest(checksum, expected):
            raise ValueError
        return ForecastAssumptionsContextBinding.model_validate_json(payload)
    except (ValueError, TypeError) as exc:
        raise ValueError("forecast-assumptions context ref is invalid") from exc


class ForecastAssumptionsContextHeader(_ContextContract):
    schema_version: Literal[
        "forecast-assumptions-context.v3",
        "forecast-assumptions-context.v4",
    ] = (
        "forecast-assumptions-context.v3"
    )
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1, max_length=32)
    context_ref: str = Field(min_length=1)
    # Measured, never bounded: the issued token's length, derived below.
    context_ref_length: int = 0
    fiscal_axis: FiscalAxis
    freshness: "ForecastAssumptionsFreshness"

    @model_validator(mode="after")
    def _measure_context_ref(self) -> "ForecastAssumptionsContextHeader":
        object.__setattr__(self, "context_ref_length", len(self.context_ref))
        return self


class ForecastAssumptionsFreshness(_ContextContract):
    status: Literal["current", "blocking_coverage_lag", "blocking_unknown"]
    axis_historical_through_period_id: SemanticKey
    covered_historical_through_period_id: SemanticKey | None = None
    required_through_period_id: SemanticKey | None = None
    determination_basis: Literal[
        "business_model_review_basis_filing",
        "terminal_summary",
        "unavailable",
        "conflicting",
    ]
    review_accession: str | None = Field(default=None, min_length=1, max_length=128)
    message: Prose


class ForecastAssumptionsWriterVocabulary(_ContextContract):
    """Closed judgment vocabulary accepted by the deterministic writer."""

    value_shapes: tuple[Literal["period_values"], ...] = ("period_values",)
    unit_authority: Literal["derived_forecast_control"] = "derived_forecast_control"
    percentage_period_value_space: Literal["fraction"] = "fraction"
    held_at_base_policy: Literal["scenario_classification_only_full_curve_required"] = (
        "scenario_classification_only_full_curve_required"
    )
    evidence_bases: tuple[
        Literal["source_backed", "analyst_judgment"], ...
    ] = ("source_backed", "analyst_judgment")
    confidences: tuple[Literal["low", "medium", "high"], ...] = (
        "low",
        "medium",
        "high",
    )
    driver_categories: tuple[
        Literal[
            "revenue",
            "unit_economics",
            "cost_structure",
            "reinvestment",
            "capital_sources",
            "valuation",
            "other",
        ],
        ...,
    ] = (
        "revenue",
        "unit_economics",
        "cost_structure",
        "reinvestment",
        "capital_sources",
        "valuation",
        "other",
    )
    value_scale: Literal["model"] = "model"
    period_value_key: Literal["fiscal_year"] = "fiscal_year"
    forecast_period_ids: tuple[SemanticKey, ...] = Field(max_length=MAX_PERIODS)
    forecast_fiscal_years: tuple[int, ...] = Field(max_length=MAX_PERIODS)

    @model_validator(mode="after")
    def _aligned_period_vocabulary(self) -> "ForecastAssumptionsWriterVocabulary":
        if len(self.forecast_period_ids) != len(self.forecast_fiscal_years):
            raise ValueError("forecast period ids and fiscal years must align")
        return self


class ForecastAssumptionsUpstreamSummary(_ContextContract):
    economic_summary: Prose
    confidence: Literal["low", "medium", "high"]
    # The default summary omits the BusinessModel decisions (they stay on
    # ``get_business_model_context`` and on each driver read's inherited
    # rationale); a caller that wants them supplies them.
    key_decisions: tuple[Prose, ...] = Field(
        default=(),
        max_length=MAX_KEY_DECISIONS,
        exclude_if=lambda value: not value,
    )
    limitation_keys: tuple[SemanticKey, ...] = ()


class ForecastAssumptionsContextCounts(_ContextContract):
    drivers: int = Field(ge=0, le=MAX_DRIVERS)
    periods: int = Field(ge=0, le=MAX_PERIODS)
    evidence_available: int = Field(ge=0, le=MAX_EVIDENCE)
    gaps: int = Field(ge=0, le=MAX_GAPS)


class ForecastAssumptionsContextSemanticKeys(_ContextContract):
    drivers: tuple[SemanticKey, ...] = Field(max_length=MAX_DRIVERS)
    periods: tuple[SemanticKey, ...] = Field(max_length=MAX_PERIODS)
    gaps: tuple[SemanticKey, ...] = Field(max_length=MAX_GAPS)

    @field_validator("drivers", "periods", "gaps")
    @classmethod
    def _canonical(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("context semantic keys must be sorted and unique")
        return value


class ForecastAssumptionsHouseDefault(_ContextContract):
    """The house value an unanswered optional policy control compiles.

    ``value`` is in the control's own unit and scale (a fraction for a
    percentage); ``basis_period_id`` is the reported year it is read from and
    ``convention`` names the house rule, as ``judgment_absent_default_used``
    names it when the compile writes the value ``default``.
    """

    value: str = Field(min_length=1)
    basis_period_id: SemanticKey
    convention: SemanticKey


class ForecastAssumptionsDriverSummary(_ContextContract):
    """One compact index row: the exact key, its unit/value space and its latest
    historical value with the period it is read from, or why there is none.
    The long-form basis, factors and inherited rationale are served by the
    keyed ``item_kind="driver"`` read."""

    driver_key: SemanticKey
    citable_fact_refs: tuple[str, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    label: str = Field(min_length=1, max_length=512)
    value_basis: Prose | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    unit: Unit
    model_scale: ModelScale
    value_semantics: Literal["direct_control_value", "growth_rate"]
    category: Literal[
        "revenue",
        "unit_economics",
        "cost_structure",
        "reinvestment",
        "capital_sources",
        "other",
    ]
    applicability_basis: Literal[
        "business_model",
        "historical_foundation",
        "business_model_and_historical_foundation",
        "unresolved",
    ] = Field(default="business_model", exclude_if=lambda value: value == "business_model")
    factors: tuple[Factor, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    behavior: NodeBehavior | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    management_target: Prose | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    relationship: BusinessModelContextRelationship | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    observation_basis: ObservationBasis | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    revenue_scope: RevenueScope | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    judgment_modes: tuple[Literal["forecast", "held_at_base"], ...] = Field(
        default=("forecast", "held_at_base"),
        exclude_if=lambda value: value == ("forecast", "held_at_base"),
    )
    authoring: Literal["required", "optional_policy"] = Field(
        default="required", exclude_if=lambda value: value == "required"
    )
    house_default: ForecastAssumptionsHouseDefault | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    # Why an optional control has no value to carry here (a per-line SBC
    # control on an issuer that discloses no split); absent otherwise.
    applicability_note: Prose | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    # The latest historical value in the control's own unit and scale, and the
    # period it is read from; ``missing_reason`` says why there is none.
    latest_value: str | None = Field(
        default=None, min_length=1, exclude_if=lambda value: value is None
    )
    latest_period_id: SemanticKey | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    missing_reason: ShortText | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    historical_values_by_period: dict[str, str] = Field(
        default_factory=dict, exclude_if=lambda value: not value
    )

class ForecastAssumptionsHistoricalDriverSeed(_ContextContract):
    """Accepted historical opening value, stripped of mechanical driver identity."""

    base_driver_key: SemanticKey
    period_id: SemanticKey
    observation_basis: Literal["period_flow", "period_end", "period_average"]
    predecessor_period_id: SemanticKey | None = None
    allowed_derivation_kinds: tuple[HistoricalDriverSeedDerivationKind, ...] = Field(
        min_length=1,
        max_length=3,
    )
    value: str = Field(min_length=1)
    unit: Unit
    model_scale: ModelScale
    disposition: HistoricalDriverSeedDisposition
    derivation_kind: HistoricalDriverSeedDerivationKind
    derivation_rationale: Prose
    confidence: Literal["high", "medium", "low"]
    limitations: tuple[Prose, ...] = Field(max_length=MAX_INHERITED_RATIONALE)
    derivation_operands: tuple["ForecastAssumptionsHistoricalSeedOperand", ...] = Field(
        min_length=1,
        max_length=MAX_INHERITED_RATIONALE,
    )
    fact_keys: tuple[SemanticKey, ...] = Field(max_length=MAX_EVIDENCE)
    business_model_evidence_keys: tuple[SemanticKey, ...] = Field(
        max_length=MAX_EVIDENCE
    )
    supporting_evidence_keys: tuple[SemanticKey, ...] = Field(
        max_length=MAX_EVIDENCE
    )

    @field_validator(
        "fact_keys",
        "business_model_evidence_keys",
        "supporting_evidence_keys",
    )
    @classmethod
    def _canonical_seed_evidence(
        cls,
        value: tuple[str, ...],
        info,
    ) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError(f"{info.field_name} must be sorted and unique")
        return value


class ForecastAssumptionsHistoricalSeedOperand(_ContextContract):
    kind: Literal[
        "historical_fact",
        "business_model_evidence_value",
        "analyst_literal",
        "captured_table_cell",
    ]
    period_id: SemanticKey
    observation_basis: Literal["period_flow", "period_end", "period_average"]
    value: str = Field(min_length=1)
    unit: Unit
    model_scale: ModelScale
    coefficient: str | None = Field(default=None, min_length=1)
    evidence_key: SemanticKey | None = None
    evidence_value_id: str | None = Field(default=None, min_length=1, max_length=128)


class ForecastAssumptionsDriverDescriptor(_ContextContract):
    """Sanitized required writer target keyed only by its natural BM path."""

    driver_key: SemanticKey
    label: str = Field(min_length=1, max_length=512)
    value_basis: Prose
    unit: Unit
    model_scale: ModelScale
    value_semantics: Literal["direct_control_value", "growth_rate"]
    category: Literal[
        "revenue",
        "unit_economics",
        "cost_structure",
        "reinvestment",
        "capital_sources",
        "other",
    ]
    applicability_basis: Literal[
        "business_model",
        "historical_foundation",
        "business_model_and_historical_foundation",
        "unresolved",
    ]
    factors: tuple[Factor, ...]
    behavior: NodeBehavior | None = None
    management_target: Prose | None = None
    relationship: BusinessModelContextRelationship | None = None
    observation_basis: ObservationBasis | None = None
    revenue_scope: RevenueScope | None = None
    inherited_rationale: tuple[Prose, ...] = Field(max_length=MAX_INHERITED_RATIONALE)
    inherited_evidence_keys: tuple[SemanticKey, ...]
    authoring: Literal["required", "optional_policy"] = "required"
    house_default: ForecastAssumptionsHouseDefault | None = None
    applicability_note: Prose | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    historical_seeds: tuple[ForecastAssumptionsHistoricalDriverSeed, ...] = Field(
        max_length=1
    )


class ForecastAssumptionsPredecessorDriver(_ContextContract):
    """One driver's accepted judgment in the predecessor set, values only.

    The driver lookup returns the whole accepted row (rationale, extension
    basis, per-period confidence, methodology fields) as ``predecessor``.
    """

    driver_key: SemanticKey
    classification: Literal["critical", "supporting", "held_at_base"]
    held_at_base: bool
    confidence: Confidence | None
    value: ScalarValue | None
    period_values: AuthoringPeriodValueSeries | None


class ForecastAssumptionsPredecessor(_ContextContract):
    """The accepted ForecastAssumptionSet a new write on this file succeeds.

    Read to carry its judgment forward on a re-accept: a value or rationale
    the analyst did not change is resubmitted as it stands here.
    """

    forecast_assumption_set_ref: ForecastAssumptionSetRef
    historical_basis_ref: HistoricalBasisArtifactRef
    forecast_control_profile_version: str | None
    drivers: tuple[ForecastAssumptionsPredecessorDriver, ...] = Field(
        max_length=MAX_DRIVERS
    )
    gaps: tuple[ForecastAssumptionGap, ...] = Field(max_length=MAX_GAPS)
    terminal_summary: ForecastTerminalSummary


class ForecastAssumptionsDriverProjection(_ContextContract):
    kind: Literal["driver"] = "driver"
    driver: ForecastAssumptionsDriverDescriptor
    #: The predecessor set's accepted row for this driver, when it has one.
    predecessor: ForecastDriverAssumption | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    drivers: tuple[ForecastAssumptionsDriverDescriptor, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    items: tuple[dict[str, Any], ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

class ForecastAssumptionsHistoricalBaseline(_ContextContract):
    """One reported fact of the period. ``evidence_key`` carries the filing's
    accession and the period, so the row names only the presentation role, tag
    and member coordinate."""

    statement_role: str = Field(min_length=1, max_length=256)
    tag: str = Field(min_length=1, max_length=256)
    dimensions: tuple[FilingDimensionRef, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    value: str = Field(min_length=1)
    unit: str = Field(min_length=1, max_length=128)
    scale: str = Field(min_length=1, max_length=128)
    confidence: Literal["low", "medium", "high"]
    source_quality: Literal["primary", "fallback", "degraded"]
    evidence_key: SemanticKey
    # The observed requirements this fact is the one reported value of; the
    # period's coverage lists only the rows that say more than that.
    requirement_ids: tuple[SemanticKey, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )


class ForecastAssumptionsCoverageProjection(_ContextContract):
    """One requirement's coverage of the period. Fields the coverage leaves
    empty are absent; ``evidence_keys`` unions its evidence, applicability and
    conflict evidence."""

    requirement_id: SemanticKey
    disposition: SourceCoverageDisposition
    rationale: str = Field(min_length=1)
    evidence_keys: tuple[SemanticKey, ...] = Field(
        default=(), max_length=MAX_EVIDENCE, exclude_if=lambda value: not value
    )
    repair_target: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    disclosure_search_basis: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    disclosure_search_source_id: SourceId | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    disclosure_search_accession: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    included_in: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    absence_citation: AbsenceCitation | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    aggregate_parent_requirement_id: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    degraded: bool = Field(default=False, exclude_if=lambda value: not value)

    @field_validator("evidence_keys")
    @classmethod
    def _canonical_evidence_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("coverage evidence keys must be sorted and unique")
        return value


class ForecastAssumptionsPeriodProjection(_ContextContract):
    kind: Literal["period"] = "period"
    period: FiscalPeriod
    axis_role: Literal["historical", "forecast"]
    historical_baselines: tuple[ForecastAssumptionsHistoricalBaseline, ...] = Field(
        max_length=MAX_BASELINES_PER_PERIOD
    )
    coverage: tuple[ForecastAssumptionsCoverageProjection, ...] = Field(
        max_length=MAX_COVERAGE_PER_PERIOD
    )
    # Present when the read folded observed requirements onto the baselines or
    # left out facts no requirement or seed names.
    coverage_note: Prose | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    items: tuple[dict[str, Any], ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    @model_validator(mode="after")
    def _bounded_anchor_handles(self) -> "ForecastAssumptionsPeriodProjection":
        handle_count = len(self.historical_baselines) + sum(
            len(row.evidence_keys) for row in self.coverage
        )
        if handle_count > MAX_EVIDENCE:
            raise ValueError("period projection exceeds its exact evidence-handle bound")
        return self


class ForecastAssumptionsContextSummaryProjection(_ContextContract):
    kind: Literal["summary"] = "summary"
    freshness: ForecastAssumptionsFreshness
    upstream: ForecastAssumptionsUpstreamSummary
    writer_vocabulary: ForecastAssumptionsWriterVocabulary
    required_drivers: tuple[ForecastAssumptionsDriverSummary, ...] = Field(
        max_length=MAX_DRIVERS
    )
    latest_actual_anchors: ForecastAssumptionsPeriodProjection
    counts: ForecastAssumptionsContextCounts
    semantic_keys: ForecastAssumptionsContextSemanticKeys
    predecessor: ForecastAssumptionsPredecessor | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def _counts_match(self) -> "ForecastAssumptionsContextSummaryProjection":
        actual = (
            len(self.semantic_keys.drivers),
            len(self.semantic_keys.periods),
            len(self.semantic_keys.gaps),
        )
        expected = (
            self.counts.drivers,
            self.counts.periods,
            self.counts.gaps,
        )
        if actual != expected:
            raise ValueError("context counts must match semantic indexes")
        driver_keys = tuple(row.driver_key for row in self.required_drivers)
        if driver_keys != self.semantic_keys.drivers:
            raise ValueError("required driver descriptors must match the denominator")
        if (
            self.latest_actual_anchors.axis_role != "historical"
            or self.latest_actual_anchors.period.is_estimate
        ):
            raise ValueError("latest actual anchors must describe a historical period")
        if self.latest_actual_anchors.period.period_id not in self.semantic_keys.periods:
            raise ValueError("latest actual anchor period must be in the period index")
        return self


class ForecastAssumptionsHistoricalFactEvidence(_ContextContract):
    evidence_kind: Literal["historical_basis_fact"] = "historical_basis_fact"
    evidence_key: SemanticKey
    fact: HistoricalBasisFact
    reconciliation_dependencies: tuple[HistoricalBasisFact, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )

    @model_validator(mode="after")
    def _current_wire_excludes_reconciliation(
        self,
    ) -> "ForecastAssumptionsHistoricalFactEvidence":
        binding = self.fact.reconciliation
        expected_ids = (
            ()
            if binding is None
            else (
                binding.starting_fact_id,
                *binding.first_stage_component_fact_ids,
                binding.reported_subtotal_fact_id,
                *binding.second_stage_component_fact_ids,
            )
        )
        if tuple(row.fact_id for row in self.reconciliation_dependencies) != expected_ids:
            raise ValueError("forecast context reconciliation dependencies are incomplete")
        validate_historical_reconciliation_bindings(
            (*self.reconciliation_dependencies, self.fact),
            allow_reconciliations=True,
        )
        return self


class ForecastAssumptionsSourceEvidence(_ContextContract):
    evidence_kind: Literal["source_evidence"] = "source_evidence"
    evidence_key: SemanticKey
    evidence: SourceEvidence


class ForecastAssumptionsBusinessModelEvidence(_ContextContract):
    evidence_kind: Literal["business_model_evidence"] = "business_model_evidence"
    evidence_key: SemanticKey
    evidence: BusinessModelSelectedEvidence


class ForecastAssumptionsHistoricalJudgmentEvidence(_ContextContract):
    evidence_kind: Literal["historical_judgment_evidence"] = (
        "historical_judgment_evidence"
    )
    evidence_key: SemanticKey
    evidence: BusinessModelSelectedEvidence


ForecastAssumptionsEvidence = Annotated[
    ForecastAssumptionsHistoricalFactEvidence
    | ForecastAssumptionsSourceEvidence
    | ForecastAssumptionsBusinessModelEvidence
    | ForecastAssumptionsHistoricalJudgmentEvidence,
    Field(discriminator="evidence_kind"),
]


class ForecastAssumptionsEvidenceProjection(_ContextContract):
    kind: Literal["evidence"] = "evidence"
    item: ForecastAssumptionsEvidence
    items: tuple[dict[str, Any], ...] = Field(
        default=(), exclude_if=lambda value: not value
    )


class ForecastAssumptionsGapProjection(_ContextContract):
    kind: Literal["gap"] = "gap"
    gap_key: SemanticKey
    gap: HistoricalFoundationGap
    items: tuple[dict[str, Any], ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

ForecastAssumptionsContextProjection = Annotated[
    ForecastAssumptionsContextSummaryProjection
    | ForecastAssumptionsDriverProjection
    | ForecastAssumptionsPeriodProjection
    | ForecastAssumptionsEvidenceProjection
    | ForecastAssumptionsGapProjection,
    Field(discriminator="kind"),
]


class ForecastAssumptionsContextResponse(_ContextContract):
    status: Literal["success"] = "success"
    context: ForecastAssumptionsContextHeader
    context_ref: str | None = Field(default=None, exclude_if=lambda value: value is None)
    judgment_skeleton: dict[str, Any] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    projection: ForecastAssumptionsContextProjection
    # Warnings the context reports instead of refusing (the bound foundation's
    # forecast driver set drifted from current inputs); never blocking.
    findings: tuple[Finding, ...] = ()

    @model_validator(mode="after")
    def _versioned_reconciliation_projection(
        self,
    ) -> "ForecastAssumptionsContextResponse":
        if self.context_ref is None:
            object.__setattr__(self, "context_ref", self.context.context_ref)
        projection = self.projection
        has_reconciliation = (
            isinstance(projection, ForecastAssumptionsEvidenceProjection)
            and isinstance(projection.item, ForecastAssumptionsHistoricalFactEvidence)
            and projection.item.fact.reconciliation is not None
        )
        if has_reconciliation and self.context.schema_version != (
            "forecast-assumptions-context.v4"
        ):
            raise ValueError("bound historical facts require forecast context v4")
        return self


__all__ = [
    "ForecastAssumptionsContextBinding",
    "ForecastAssumptionsContextCounts",
    "ForecastAssumptionsContextHeader",
    "ForecastAssumptionsContextResponse",
    "ForecastAssumptionsContextSemanticKeys",
    "ForecastAssumptionsContextSummaryProjection",
    "ForecastAssumptionsBusinessModelEvidence",
    "ForecastAssumptionsCoverageProjection",
    "ForecastAssumptionsDriverDescriptor",
    "ForecastAssumptionsDriverProjection",
    "ForecastAssumptionsDriverSummary",
    "ForecastAssumptionsEvidenceProjection",
    "ForecastAssumptionsGapProjection",
    "ForecastAssumptionsFreshness",
    "ForecastAssumptionsHistoricalBaseline",
    "ForecastAssumptionsHistoricalDriverSeed",
    "ForecastAssumptionsHistoricalSeedOperand",
    "ForecastAssumptionsHouseDefault",
    "ForecastAssumptionsPeriodProjection",
    "ForecastAssumptionsPredecessor",
    "ForecastAssumptionsPredecessorDriver",
    "ForecastAssumptionsUpstreamSummary",
    "ForecastAssumptionsWriterVocabulary",
    "decode_forecast_assumptions_context_ref",
    "encode_forecast_assumptions_context_ref",
    "forecast_assumptions_context_content_digest",
]
