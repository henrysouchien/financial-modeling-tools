from __future__ import annotations

from typing import Any, Literal, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .enum_canonicalizers import (
    canonicalize_optional_direction,
    canonicalize_optional_strategy,
    canonicalize_optional_timeframe,
)
from .models import DriverCategory
from .process_template import StrategyCategoryId
from .thesis_shared_slice_helpers import (
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
from .thesis_shared_sources import (
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
from .thesis_shared_industry import (
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

class _ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )


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
    stock_outcome: Literal[
        "outperformance", "underperformance", "neutral", "mixed"
    ] | None = None
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
    trigger_id: str | None = None
    description: str
    metric: str | None = None
    threshold: ThresholdValue | None = None
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
    method: str | None = None
    low: float | None = None
    mid: float | None = None
    high: float | None = None
    current_multiple: float | None = None
    wacc: float | None = None
    risk_free_rate: float | None = None
    equity_risk_premium: float | None = None
    cost_of_equity: float | None = None
    raw_beta: float | None = None
    adjusted_beta: float | None = None
    beta_floor: float | None = None
    terminal_growth_rate: float | None = None
    terminal_multiple: float | None = None
    rationale: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

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
    assumption_id: str | None = None
    driver: str
    value: ScalarValue | None = None
    unit: str | None = None
    rationale: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)
    driver_category: DriverCategory | None = None
    confidence: ConfidenceLevel | None = None
    held_at_base: bool = False
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
