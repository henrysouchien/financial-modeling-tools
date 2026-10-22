from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field, ValidationInfo, field_validator, model_validator

from .models import PeriodMode
from industry_slice_core.industry_types import DriverCategory, Unit
from .period_value_series import PeriodValueSeries
from .provider_port.registry import (
    ProviderId,
    ProviderRegistry,
    require_registered_provider_id,
)
from .thesis_shared_slice import (
    ConfidenceLevel,
    ScalarValue,
    _ContractModel,
    _normalize_level,
    _normalize_optional_identifier,
    _normalize_ticker,
)


ProjectionValueScale = Literal["display", "model"]
MbcValuationMethod = Literal["dcf", "multiples", "sum_of_parts", "hybrid", "relative"]
DiscountRateMethod = Literal["capm", "wacc", "manual"]
ExitMultipleType = Literal["p_e", "ev_ebitda", "p_b", "p_s", "ev_sales"]
SegmentSnapshotSource = Literal["edgar_auto", "caller_override", "fallback_single"]
SegmentRevenueObservationSource = Literal["edgar_fact", "caller_override", "derived_other", "reconciliation"]
SegmentRevenueComparability = Literal["comparable", "not_comparable", "unknown", "not_applicable"]


def _provider_registry_from_validation(info: ValidationInfo) -> ProviderRegistry | None:
    if isinstance(info.context, Mapping):
        registry = info.context.get("provider_registry")
        if isinstance(registry, ProviderRegistry):
            return registry
    return None


def _segment_observation_structural_basis_key(
    *,
    source: str | None,
    axis: str | None,
    member: str | None,
    tag: str | None,
) -> str | None:
    if source != "edgar_fact":
        return None
    parts = [_segment_basis_component(value) for value in (axis, member, tag)]
    if any(part is None for part in parts):
        return None
    return "edgar_fact:" + "|".join(part for part in parts if part is not None)


def _segment_basis_component(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_mapping_keys(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a dictionary keyed by identifier")
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key).strip()
        if not key:
            raise ValueError(f"{field_name} keys must be non-empty strings")
        normalized[key] = raw_value
    return normalized


class MbcCompany(_ContractModel):
    ticker: str
    name: str = Field(min_length=1)
    fiscal_year_end: str = Field(min_length=1)
    most_recent_fy: int = Field(ge=1)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class MbcYearAxis(_ContractModel):
    period_mode: PeriodMode
    historical_years: list[int] = Field(min_length=1)
    projection_years: list[int] = Field(min_length=1)


class SegmentMappingEntry(_ContractModel):
    edgar_member: str = Field(min_length=1)
    name: str | None = None
    volume_label: str | None = None
    price_label: str | None = None


class SegmentRevenueObservation(_ContractModel):
    fiscal_year: int = Field(ge=1)
    value: float
    source: SegmentRevenueObservationSource
    tag: str | None = Field(default=None, min_length=1)
    scale: str | None = Field(default=None, min_length=1)
    axis: str | None = Field(default=None, min_length=1)
    member: str | None = Field(default=None, min_length=1)
    member_label: str | None = Field(default=None, min_length=1)
    source_filing_accession: str | None = Field(default=None, min_length=1)
    source_form: str | None = Field(default=None, min_length=1)
    filed_at: str | None = Field(default=None, min_length=1)
    period_end: str | None = Field(default=None, min_length=1)
    basis_key: str | None = Field(default=None, min_length=1)
    comparable_with_prior: SegmentRevenueComparability = "unknown"
    comparability_note: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _normalize_basis_and_comparability(self) -> "SegmentRevenueObservation":
        if not self.basis_key:
            basis_key = _segment_observation_structural_basis_key(
                source=self.source,
                axis=self.axis,
                member=self.member,
                tag=self.tag,
            )
            if basis_key:
                self.basis_key = basis_key
        if self.comparable_with_prior == "not_comparable" and not self.comparability_note:
            raise ValueError("not_comparable segment observations require comparability_note")
        return self


class SegmentSnapshotEntry(_ContractModel):
    segment_index: int = Field(ge=1)
    name: str = Field(min_length=1)
    edgar_member: str | None = None
    volume_label: str | None = None
    price_label: str | None = None
    revenue_observations: list[SegmentRevenueObservation] | None = None

    @model_validator(mode="after")
    def _normalize_revenue_observation_comparability(self) -> "SegmentSnapshotEntry":
        if not self.revenue_observations:
            return self

        observations_by_year: dict[int, SegmentRevenueObservation] = {}
        for observation in self.revenue_observations:
            year = int(observation.fiscal_year)
            if year in observations_by_year:
                raise ValueError("revenue_observations must not contain duplicate fiscal_year entries")
            observations_by_year[year] = observation

        normalized: list[SegmentRevenueObservation] = []
        prior: SegmentRevenueObservation | None = None
        for _year, observation in sorted(observations_by_year.items()):
            updates: dict[str, str | None] = {}
            if prior is None:
                updates["comparable_with_prior"] = "not_applicable"
            elif prior.basis_key and observation.basis_key:
                if prior.basis_key != observation.basis_key:
                    updates["comparable_with_prior"] = "not_comparable"
                    updates["comparability_note"] = (
                        observation.comparability_note
                        or f"segment basis changed from {prior.basis_key!r} to {observation.basis_key!r}"
                    )
                elif observation.comparable_with_prior == "not_comparable":
                    updates["comparable_with_prior"] = "not_comparable"
                    updates["comparability_note"] = observation.comparability_note
                else:
                    updates["comparable_with_prior"] = "comparable"
                    updates["comparability_note"] = None
            elif observation.comparable_with_prior == "not_comparable":
                updates["comparable_with_prior"] = "not_comparable"
                updates["comparability_note"] = observation.comparability_note
            else:
                updates["comparable_with_prior"] = "unknown"
                updates["comparability_note"] = (
                    observation.comparability_note
                    or "segment basis provenance is missing for adjacent-year comparability"
                )
            updated = observation.model_copy(update=updates)
            normalized.append(updated)
            prior = updated

        self.revenue_observations = normalized
        return self


class SegmentProfileSnapshot(_ContractModel):
    axis_used: str | None = None
    source: SegmentSnapshotSource
    segments: list[SegmentSnapshotEntry] = Field(min_length=1)
    total_revenue_check: dict[int, float] | None = None


class SegmentConfig(_ContractModel):
    axis: str | None = None
    segment_mapping: list[SegmentMappingEntry] | None = None
    segment_profile_snapshot: SegmentProfileSnapshot


class MbcDriverSpec(_ContractModel):
    assumption_id: str | None = None
    value: ScalarValue | None = None
    period_values: PeriodValueSeries | None = None
    unit: Unit
    value_scale: ProjectionValueScale = "model"
    periods: list[int] | None = None
    sia_category: DriverCategory | None = None
    rationale: str | None = None
    confidence: ConfidenceLevel | None = None

    @model_validator(mode="after")
    def _one_value_shape(self) -> "MbcDriverSpec":
        if (self.value is None) == (self.period_values is None):
            raise ValueError("driver requires exactly one scalar or period series")
        return self

    @field_validator("assumption_id", mode="before")
    @classmethod
    def _normalize_assumption_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("confidence", mode="before")
    @classmethod
    def _canonicalize_confidence(cls, value: object | None) -> str | None:
        return _normalize_level(value)


class MbcScenarioOverride(_ContractModel):
    value: ScalarValue | PeriodValueSeries
    periods: list[int] | None = None


class MbcScenarioSpec(_ContractModel):
    description: str | None = None
    overrides: dict[str, MbcScenarioOverride] = Field(default_factory=dict)

    @field_validator("overrides", mode="before")
    @classmethod
    def _normalize_overrides(cls, value: object) -> dict[str, Any]:
        return _normalize_mapping_keys(value, field_name="overrides")


class DiscountRateSpec(_ContractModel):
    value: float
    method: DiscountRateMethod
    inputs: dict[str, ScalarValue] | None = None


class TerminalGrowthSpec(_ContractModel):
    value: float
    rationale: str | None = None


class ExitMultipleSpec(_ContractModel):
    value: float
    multiple_type: ExitMultipleType


class ValuationInputs(_ContractModel):
    discount_rate: DiscountRateSpec | None = None
    terminal_growth: TerminalGrowthSpec | None = None
    exit_multiple: ExitMultipleSpec | None = None
    equity_risk_premium: float | None = Field(
        default=None,
        description=(
            "Explicit equity risk premium as decimal (e.g., 0.045). When absent, "
            "build leaves ERP blank instead of inferring a default. Persists across handoffs."
        ),
    )
    equity_risk_premium_source: str | None = Field(
        default=None,
        description=(
            "Provenance source for ERP (e.g. house policy or analyst); required "
            "alongside a non-null ERP value."
        ),
    )
    equity_risk_premium_rationale: str | None = Field(
        default=None,
        description="Why this ERP was selected.",
    )
    equity_risk_premium_as_of: str | None = Field(
        default=None,
        description="As-of observation month for the ERP source, YYYY-MM.",
    )


class ValuationRanges(_ContractModel):
    low: float | None = None
    mid: float | None = None
    high: float | None = None


class MbcValuationSpec(_ContractModel):
    method: MbcValuationMethod
    inputs: ValuationInputs = Field(default_factory=ValuationInputs)
    ranges: ValuationRanges = Field(default_factory=ValuationRanges)
    rationale: str | None = None


class HistoricalSourceOverride(_ContractModel):
    required_role: str = Field(min_length=1)
    preferred: ProviderId
    fallback_order: list[ProviderId] = Field(min_length=1)

    @field_validator("preferred", mode="before")
    @classmethod
    def _normalize_preferred_provider(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> ProviderId:
        return require_registered_provider_id(
            value,
            provider_registry=_provider_registry_from_validation(info),
            entry_point="historical override preferred provider",
        )

    @field_validator("fallback_order", mode="before")
    @classmethod
    def _normalize_fallback_providers(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        registry = _provider_registry_from_validation(info)
        return [
            require_registered_provider_id(
                provider_id,
                provider_registry=registry,
                entry_point="historical override fallback provider",
            )
            for provider_id in value
        ]

    @model_validator(mode="after")
    def _enforce_fallback_order_head(self) -> "HistoricalSourceOverride":
        if self.fallback_order[0] != self.preferred:
            raise ValueError(
                "fallback_order[0] must equal preferred "
                f"(got {self.fallback_order[0]!r} vs {self.preferred!r})"
            )
        return self


class HistoricalSources(_ContractModel):
    default_source: ProviderId = "fmp"
    default_fallback_enabled: bool = False
    overrides: list[HistoricalSourceOverride] = Field(default_factory=list)

    @field_validator("default_source", mode="before")
    @classmethod
    def _normalize_default_provider(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> ProviderId:
        return require_registered_provider_id(
            value,
            provider_registry=_provider_registry_from_validation(info),
            entry_point="historical default provider",
        )


class BuildFlags(_ContractModel):
    include_historicals: bool = True
    annotate_with_research: bool = True
    preview_mode: bool = False


class ModelBuildContext(_ContractModel):
    source: ProviderId = "fmp"
    n_historical: int = Field(default=5, ge=1)
    n_projection: int = Field(default=12, ge=1)
    sector: str | None = None
    business_model_path: str | None = None
    business_model_revision: str | None = None

    company: MbcCompany
    fiscal_axis: MbcYearAxis
    segment_config: SegmentConfig | None = None
    drivers: dict[str, MbcDriverSpec] = Field(default_factory=dict)
    scenarios: dict[str, MbcScenarioSpec] = Field(default_factory=dict)
    valuation: MbcValuationSpec
    historical_sources: HistoricalSources = Field(default_factory=HistoricalSources)
    build_flags: BuildFlags = Field(default_factory=BuildFlags)

    schema_version: Literal["1.0"] = "1.0"

    @field_validator("source", mode="before")
    @classmethod
    def _normalize_source_provider(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> ProviderId:
        return require_registered_provider_id(
            value,
            provider_registry=_provider_registry_from_validation(info),
            entry_point="model-build source provider",
        )

    @field_validator("drivers", mode="before")
    @classmethod
    def _normalize_drivers(cls, value: object) -> dict[str, Any]:
        return _normalize_mapping_keys(value, field_name="drivers")

    @field_validator("scenarios", mode="before")
    @classmethod
    def _normalize_scenarios(cls, value: object) -> dict[str, Any]:
        return _normalize_mapping_keys(value, field_name="scenarios")


__all__ = [
    "BuildFlags",
    "DiscountRateMethod",
    "DiscountRateSpec",
    "MbcCompany",
    "MbcDriverSpec",
    "ExitMultipleSpec",
    "ExitMultipleType",
    "MbcYearAxis",
    "HistoricalSourceOverride",
    "HistoricalSources",
    "ModelBuildContext",
    "PeriodMode",
    "MbcScenarioSpec",
    "MbcScenarioOverride",
    "SegmentConfig",
    "SegmentMappingEntry",
    "SegmentProfileSnapshot",
    "SegmentRevenueObservation",
    "SegmentSnapshotEntry",
    "SegmentSnapshotSource",
    "TerminalGrowthSpec",
    "MbcValuationSpec",
    "ValuationInputs",
    "MbcValuationMethod",
    "ValuationRanges",
]
