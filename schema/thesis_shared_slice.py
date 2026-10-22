from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from industry_slice_core._contract import _ContractModel

from industry_slice_core.enum_canonicalizers import (
    canonicalize_optional_direction,
    canonicalize_optional_strategy,
    canonicalize_optional_timeframe,
)
from industry_slice_core.industry_types import DriverCategory, Unit
from .assumption_value_semantics import PercentageValueSpace
from .process_template import StrategyCategoryId
from industry_slice_core.thesis_shared_slice_helpers import (
    _EXCHANGE_SUFFIXES as _EXCHANGE_SUFFIXES,
    _LEVEL_VALUES as _LEVEL_VALUES,
    _SKILL_NAME_RE as _SKILL_NAME_RE,
    _SHARE_CLASS_SUFFIXES as _SHARE_CLASS_SUFFIXES,
    _TICKER_RE as _TICKER_RE,
    _VALUATION_METHODS as _VALUATION_METHODS,
    _VALUATION_METHOD_ALIASES as _VALUATION_METHOD_ALIASES,
    _VALUATION_NUMERIC_FIELDS as _VALUATION_NUMERIC_FIELDS,
    _VALUATION_RATE_FIELDS as _VALUATION_RATE_FIELDS,
    _normalize_level as _normalize_level,
    _normalize_optional_identifier as _normalize_optional_identifier,
    _normalize_ticker as _normalize_ticker,
    _normalize_valuation_method as _normalize_valuation_method,
    _normalize_workspace_relative_path as _normalize_workspace_relative_path,
)
from industry_slice_core.thesis_shared_sources import (
    Excerpt as Excerpt,
    ExcerptLocator as ExcerptLocator,
    ExcerptLocatorKind as ExcerptLocatorKind,
    ReaderTableContext as ReaderTableContext,
    ReaderTableFilingIdentity as ReaderTableFilingIdentity,
    ReaderTableValueSource as ReaderTableValueSource,
    ScalarValue as ScalarValue,
    SourceId as SourceId,
    SourceRecord as SourceRecord,
    SourceType as SourceType,
)
from .numeric_display import NumericDisplaySpec
from .period_value_series import PeriodValueSeries
from industry_slice_core.thesis_shared_industry import (
    CompMetricCell as CompMetricCell,
    CompsNarrative as CompsNarrative,
    EditorialPeer as EditorialPeer,
    IndustryAnalysis as IndustryAnalysis,
    IndustryLandscape as IndustryLandscape,
    IndustryPeerComparison as IndustryPeerComparison,
    IndustryPeerComparisonPeer as IndustryPeerComparisonPeer,
    MacroOverlay as MacroOverlay,
    MacroOverlayDriver as MacroOverlayDriver,
    OperatingComparison as OperatingComparison,
    SnapshotMetric as SnapshotMetric,
    SnapshotSection as SnapshotSection,
    StructuralTrend as StructuralTrend,
    TimeseriesGroup as TimeseriesGroup,
    TimeseriesMetric as TimeseriesMetric,
)


DirectionValue = Literal["long", "short", "hedge", "pair"]
TimeframeValue = Literal["near_term", "medium", "long_term"]
ConfidenceLevel = Literal["low", "medium", "high"]
DerivedFromKind = Literal["catalyst", "trigger"]
DataGapStatus = Literal["open", "resolved", "superseded", "retained"]
ThresholdValue: TypeAlias = str | int | float


class FieldDisposition(StrEnum):
    """Where a ModelSpec 2.2 fragment field's value comes from.

    Each fragment field declares one member as ``Annotated`` metadata, which
    ``model_json_schema()`` ignores; ``schema.model_spec_authoring`` reads the
    partition back off the models.
    """

    JUDGMENT = "judgment"
    SELECTOR = "selector_namespace"
    CROSS_SECTION_NATURAL_KEY = "cross_section_natural_key"
    CLOSED_VOCABULARY = "closed_vocabulary"
    SERVER_DERIVED = "server_derived"
    SERVER_MINTED = "server_minted"




class CompanyField(_ContractModel):
    ticker: str
    name: str | None = None
    sector: str | None = None
    industry: str | None = None
    fiscal_year_end: str | None = None
    most_recent_fy: int | None = None
    exchange: str | None = None
    cik: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class ThesisField(_ContractModel):
    statement: str | None = None
    direction: DirectionValue | None = None
    strategy: StrategyCategoryId | None = None
    conviction: int | None = None
    timeframe: TimeframeValue | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("direction", mode="before")
    @classmethod
    def _canonicalize_direction(cls, value: object | None) -> str | None:
        return canonicalize_optional_direction(value)

    @field_validator("strategy", mode="before")
    @classmethod
    def _canonicalize_strategy(cls, value: object | None) -> str | None:
        return canonicalize_optional_strategy(value)

    @field_validator("timeframe", mode="before")
    @classmethod
    def _canonicalize_timeframe(cls, value: object | None) -> str | None:
        return canonicalize_optional_timeframe(value)

    @field_validator("conviction", mode="before")
    @classmethod
    def _validate_conviction(cls, value: object | None) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("conviction must be an int between 1 and 5")
        if value < 1 or value > 5:
            raise ValueError("conviction must be between 1 and 5")
        return value


class ConsensusView(_ContractModel):
    claim_id: str | None = Field(default=None, min_length=1)
    narrative: str
    basis: Literal["gaap", "non_gaap"] | None = None
    citations: list[SourceId] = Field(default_factory=list)


class MaterialityThreshold(_ContractModel):
    """Threshold used to filter critical-factor candidates and judge thesis violations."""

    basis: str
    threshold_pct: float = Field(gt=0, le=100)
    metric: str | None = None
    horizon: str | None = None
    rationale: str
    source_refs: list[SourceId] = Field(default_factory=list)


class HistoricalCoincidence(_ContractModel):
    """Episode where a critical factor's movement coincided with a stock outcome."""

    coincidence_id: str = Field(min_length=1)
    period: str
    factor: str
    assumption_id: str | None = None
    market_reaction: str
    factor_direction: str | None = None
    stock_outcome: (
        Literal["outperformance", "underperformance", "neutral", "mixed"] | None
    ) = None
    driver: str
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("coincidence_id", "assumption_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class DataGap(_ContractModel):
    """A known data unavailability that affected the analysis."""

    gap_id: str | None = None
    target_handle: str | None = None
    description: str
    workaround: str | None = None
    severity: Literal["blocking", "approximate", "minor"] | None = None
    status: DataGapStatus = "open"
    remediation: dict[str, Any] | None = None
    resolution_note: str | None = None
    resolution_source_refs: list[SourceId] = Field(default_factory=list)
    resolved_at: str | None = None
    superseded_by_gap_id: str | None = None

    @field_validator("gap_id", "target_handle", "superseded_by_gap_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @model_validator(mode="before")
    @classmethod
    def _coerce_string(cls, value: object) -> object:
        if isinstance(value, str):
            return {"description": value}
        return value

    @model_validator(mode="after")
    def _validate_lifecycle(self) -> "DataGap":
        if self.status == "open":
            if self.superseded_by_gap_id is not None:
                raise ValueError("open data gaps cannot set superseded_by_gap_id")
            return self
        if not self.resolution_note and not self.resolution_source_refs:
            raise ValueError(
                "non-open data gaps require resolution_note or resolution_source_refs"
            )
        if self.status == "superseded" and not self.superseded_by_gap_id:
            raise ValueError("superseded data gaps require superseded_by_gap_id")
        if self.status != "superseded" and self.superseded_by_gap_id is not None:
            raise ValueError(
                "superseded_by_gap_id is only valid when status is superseded"
            )
        return self


class DifferentiatedViewClaim(_ContractModel):
    claim_id: str | None = None
    claim: str
    rationale: str
    evidence: list[SourceId] = Field(default_factory=list)
    upside_if_right: str | None = None
    downside_if_wrong: str | None = None

    @field_validator("claim_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class InvalidationTrigger(_ContractModel):
    """A thesis-breaking condition an outcome scorer monitors.

    ``metric`` is the key a scorer matches; ``metric_label`` is the reader's name for that
    quantity and ``threshold_display`` the unit ``threshold`` is written in, both declared by
    the trigger's producer, in the shape ``ThesisQuantitativeScenario.return_pct`` declares
    (a label beside a ``NumericDisplaySpec``; percentages as decimal fractions).
    """

    trigger_id: str | None = None
    description: str
    metric: str | None = None
    metric_label: str | None = None
    threshold: ThresholdValue | None = None
    threshold_display: NumericDisplaySpec | None = None
    threshold_direction: Literal["above", "below"] | None = None
    direction: DirectionValue | None = None
    source_refs: list[SourceId] = Field(min_length=1)

    @field_validator("trigger_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("direction", mode="before")
    @classmethod
    def _canonicalize_direction(cls, value: object | None) -> str | None:
        return canonicalize_optional_direction(value)


class BusinessSegment(_ContractModel):
    name: str
    rev_pct: float | None = None


class BusinessOverview(_ContractModel):
    description: str | None = None
    segments: list[BusinessSegment] = Field(default_factory=list)
    source_refs: list[SourceId] = Field(default_factory=list)


def business_overview_is_non_empty(
    value: BusinessOverview | dict[str, Any] | None,
) -> bool:
    """Return whether an overview contains user-reviewable business content."""

    if value is None:
        return False
    overview = BusinessOverview.model_validate(value)
    return bool(
        str(overview.description or "").strip()
        or overview.segments
        or overview.source_refs
    )


def business_overview_fingerprint(
    value: BusinessOverview | dict[str, Any] | None,
) -> str:
    """Return the canonical stored-representation fingerprint for an overview."""

    if not business_overview_is_non_empty(value):
        return "EMPTY"
    payload = BusinessOverview.model_validate(value).model_dump(mode="json")
    canonical = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def business_overview_replacement_required(
    current: BusinessOverview | dict[str, Any] | None,
    incoming: BusinessOverview | dict[str, Any] | None,
) -> bool:
    """Return whether an incoming overview replaces populated canonical content."""

    return business_overview_is_non_empty(current) and business_overview_fingerprint(
        incoming
    ) != business_overview_fingerprint(current)


class Catalyst(_ContractModel):
    catalyst_id: str | None = None
    description: str
    expected_date: str | None = None
    severity: str
    source_ref: SourceId | None = None

    @field_validator("catalyst_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Risk(_ContractModel):
    risk_id: str | None = None
    description: str
    severity: str
    type: str | None = None
    source_ref: SourceId | None = None

    @field_validator("risk_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Valuation(_ContractModel):
    method: Annotated[str | None, FieldDisposition.CLOSED_VOCABULARY] = None
    low: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    mid: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    high: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    current_multiple: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    wacc: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    risk_free_rate: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    sofr_rate: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    credit_spread: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    equity_risk_premium: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    cost_of_equity: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    raw_beta: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    adjusted_beta: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    beta_floor: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    terminal_growth_rate: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    terminal_multiple: Annotated[float | None, FieldDisposition.JUDGMENT] = None
    rationale: Annotated[str | None, FieldDisposition.JUDGMENT] = None
    source_refs: Annotated[
        list[SourceId],
        FieldDisposition.SELECTOR,
    ] = Field(default_factory=list)

    @field_validator("method", mode="before")
    @classmethod
    def _normalize_method(cls, value: object | None) -> str | None:
        return _normalize_valuation_method(value)

    @field_validator(*_VALUATION_NUMERIC_FIELDS, mode="before")
    @classmethod
    def _reject_bool_numeric_inputs(cls, value: object | None) -> object | None:
        if isinstance(value, bool):
            raise ValueError("valuation numeric fields cannot be boolean")
        return value

    @model_validator(mode="after")
    def _rate_fields_use_decimal_scale(self) -> "Valuation":
        for field_name in _VALUATION_RATE_FIELDS:
            value = getattr(self, field_name)
            if value is None:
                continue
            if not -1.0 <= float(value) <= 1.0:
                raise ValueError(
                    f"valuation.{field_name} must be a decimal rate between -1 and 1"
                )
        return self


class Peer(_ContractModel):
    ticker: str
    name: str
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class Assumption(_ContractModel):
    """One modelled driver the thesis rests on.

    ``driver`` is the key; ``driver_label`` is the reader's name for it and
    ``value_display`` the unit ``value`` and ``period_values`` are written in, both
    declared by the producer in the shape ``InvalidationTrigger`` declares
    (``metric_label`` beside ``threshold_display``).
    """

    assumption_id: Annotated[str | None, FieldDisposition.SERVER_MINTED] = None
    driver: Annotated[str, FieldDisposition.SELECTOR]
    driver_label: Annotated[str | None, FieldDisposition.JUDGMENT] = None
    value: Annotated[ScalarValue | None, FieldDisposition.JUDGMENT] = None
    period_values: Annotated[PeriodValueSeries | None, FieldDisposition.JUDGMENT] = None
    unit: Annotated[Unit | None, FieldDisposition.CLOSED_VOCABULARY] = None
    percentage_value_space: Annotated[
        PercentageValueSpace | None,
        FieldDisposition.CLOSED_VOCABULARY,
    ] = None
    value_display: Annotated[
        NumericDisplaySpec | None,
        FieldDisposition.JUDGMENT,
    ] = None
    rationale: Annotated[str | None, FieldDisposition.JUDGMENT] = None
    source_refs: Annotated[
        list[SourceId],
        FieldDisposition.SELECTOR,
    ] = Field(default_factory=list)
    source_refs_by_period: Annotated[
        dict[int, list[SourceId]],
        FieldDisposition.SELECTOR,
    ] = Field(default_factory=dict)
    extension_basis_by_period: Annotated[
        dict[int, str],
        FieldDisposition.JUDGMENT,
    ] = Field(default_factory=dict)
    confidence_by_period: Annotated[
        dict[int, ConfidenceLevel],
        FieldDisposition.CLOSED_VOCABULARY,
    ] = Field(default_factory=dict)
    source_notes_by_period: Annotated[
        dict[int, str],
        FieldDisposition.JUDGMENT,
    ] = Field(default_factory=dict)
    period_ids: Annotated[tuple[str, ...], FieldDisposition.SELECTOR] = ()
    evidence_basis: Annotated[
        Literal["source_backed", "analyst_judgment"] | None,
        FieldDisposition.CLOSED_VOCABULARY,
    ] = None
    evidence_ids: Annotated[tuple[str, ...], FieldDisposition.SELECTOR] = ()
    inherited_baseline_ids: Annotated[
        tuple[str, ...],
        FieldDisposition.SELECTOR,
    ] = Field(default=(), exclude_if=lambda value: not value)
    limitations: Annotated[str | None, FieldDisposition.JUDGMENT] = None
    value_scale: Annotated[
        Literal["model"],
        FieldDisposition.CLOSED_VOCABULARY,
    ] = "model"
    driver_category: Annotated[
        DriverCategory | None,
        FieldDisposition.CLOSED_VOCABULARY,
    ] = None
    confidence: Annotated[
        ConfidenceLevel | None,
        FieldDisposition.CLOSED_VOCABULARY,
    ] = None
    held_at_base: Annotated[bool, FieldDisposition.JUDGMENT] = False
    # True means scenario engine intentionally did NOT flex this assumption:
    # held at recent average / management guidance / task input. Distinct from
    # "the base-case value of a flexed assumption" (that's just `value`).

    @field_validator("assumption_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("confidence", mode="before")
    @classmethod
    def _canonicalize_confidence(cls, value: object | None) -> str | None:
        return _normalize_level(value)

    @model_validator(mode="after")
    def _percentage_space_matches_unit(self) -> "Assumption":
        if self.unit is not Unit.percentage and self.percentage_value_space is not None:
            raise ValueError(
                "percentage_value_space is only valid for percentage assumptions"
            )
        return self

    @model_validator(mode="after")
    def _require_one_value_shape(self) -> "Assumption":
        if self.value is not None and self.period_values is not None:
            raise ValueError(
                "assumption cannot contain both scalar and per-period values"
            )
        value_periods = set(self.period_values or {})
        metadata_periods = {
            *self.source_refs_by_period,
            *self.extension_basis_by_period,
            *self.confidence_by_period,
            *self.source_notes_by_period,
        }
        if not metadata_periods <= value_periods:
            raise ValueError(
                "assumption period metadata must name a valued period"
            )
        if len(self.period_ids) != len(set(self.period_ids)):
            raise ValueError("assumption period_ids must be unique")
        if (
            self.evidence_basis == "source_backed"
            and not self.evidence_ids
            and not self.source_refs
        ):
            raise ValueError("source-backed assumption requires evidence")
        return self


class QualitativeFactor(_ContractModel):
    id: int | None = None
    category: str
    label: str
    assessment: str = Field(min_length=1)
    rating: ConfidenceLevel | None = None
    data: dict[str, Any] | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("rating", mode="before")
    @classmethod
    def _canonicalize_rating(cls, value: object | None) -> str | None:
        return _normalize_level(value)


class Ownership(_ContractModel):
    institutional_pct: float | None = None
    insider_pct: float | None = None
    recent_activity: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("institutional_pct", "insider_pct")
    @classmethod
    def _normalize_percentage_scale(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if value > 1.0:
            return value / 100.0
        return value

    @model_validator(mode="after")
    def _validate_evidence(self) -> "Ownership":
        has_content = (
            self.institutional_pct is not None
            or self.insider_pct is not None
            or (self.recent_activity is not None and self.recent_activity.strip())
        )
        if has_content and not self.source_refs:
            raise ValueError(
                "Ownership with populated fields requires non-empty source_refs (R1)"
            )
        return self


class WatchItem(BaseModel):
    watch_item_id: str | None = None
    description: str = Field(min_length=1)
    metric: str | None = None
    threshold: ThresholdValue | None = None
    threshold_direction: Literal["above", "below"] | None = None
    last_checked: str | None = None
    source_refs: list[SourceId] = Field(min_length=1)
    derived_from_handle: str | None = None
    derived_from_kind: DerivedFromKind | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    @field_validator("watch_item_id", "derived_from_handle", mode="before")
    @classmethod
    def _normalize_watch_item_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Monitoring(_ContractModel):
    watch_list: list[WatchItem] = Field(default_factory=list)
    source_refs: list[SourceId] = Field(default_factory=list)


__all__ = [
    "Assumption",
    "BusinessOverview",
    "business_overview_fingerprint",
    "business_overview_is_non_empty",
    "business_overview_replacement_required",
    "BusinessSegment",
    "Catalyst",
    "CompanyField",
    "ConfidenceLevel",
    "CompMetricCell",
    "CompsNarrative",
    "ConsensusView",
    "DataGap",
    "DataGapStatus",
    "DerivedFromKind",
    "DifferentiatedViewClaim",
    "DirectionValue",
    "EditorialPeer",
    "Excerpt",
    "ExcerptLocator",
    "ExcerptLocatorKind",
    "HistoricalCoincidence",
    "IndustryAnalysis",
    "IndustryLandscape",
    "IndustryPeerComparison",
    "IndustryPeerComparisonPeer",
    "InvalidationTrigger",
    "MacroOverlay",
    "MacroOverlayDriver",
    "MaterialityThreshold",
    "Monitoring",
    "OperatingComparison",
    "Ownership",
    "Peer",
    "QualitativeFactor",
    "ReaderTableContext",
    "ReaderTableFilingIdentity",
    "ReaderTableValueSource",
    "Risk",
    "ScalarValue",
    "SnapshotMetric",
    "SnapshotSection",
    "SourceId",
    "SourceRecord",
    "SourceType",
    "StructuralTrend",
    "ThesisField",
    "ThresholdValue",
    "TimeseriesGroup",
    "TimeseriesMetric",
    "TimeframeValue",
    "Valuation",
    "WatchItem",
]
