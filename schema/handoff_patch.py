from __future__ import annotations

from datetime import date
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from ._insights_shared import (
    Confidence,
    DriverCategory,
    DriverUnit,
    Severity,
    _FROZEN_CONTRACT,
)
from .thesis import (
    GaapNonGaapBridge,
    PositionSize,
    ThesisPriceTarget,
    RiskDecomposition,
    ScenarioMultiple,
    ScenarioProbability,
)
from .thesis_shared_slice import (
    BusinessOverview,
    CompsNarrative,
    DataGap,
    EditorialPeer,
    HistoricalCoincidence,
    IndustryLandscape,
    IndustryPeerComparison,
    MacroOverlay,
    MaterialityThreshold,
    Ownership,
    OperatingComparison,
    ScalarValue,
    SnapshotSection,
    SourceId,
    SourceRecord,
    StructuralTrend,
    ThresholdValue,
    WatchItem,
    _ContractModel,
)
from .handoff_patch_helpers import (
    _EMPTY_TARGET_NORMALIZED_OPS as _EMPTY_TARGET_NORMALIZED_OPS,
    _SOURCE_REF_LIST_KEYS as _SOURCE_REF_LIST_KEYS,
    _SOURCE_REF_SCALAR_KEYS as _SOURCE_REF_SCALAR_KEYS,
    _VALUATION_NUMERIC_FIELDS as _VALUATION_NUMERIC_FIELDS,
    _VALUATION_RATE_FIELDS as _VALUATION_RATE_FIELDS,
    _VALUATION_STRING_FIELDS as _VALUATION_STRING_FIELDS,
    _next_split_op_id as _next_split_op_id,
    _normalize_empty_target as _normalize_empty_target,
    _normalize_patch_batch_payload as _normalize_patch_batch_payload,
    _rewrite_source_ref_leaf as _rewrite_source_ref_leaf,
    _rewrite_source_refs_recursive as _rewrite_source_refs_recursive,
    _split_inline_assumption_held_at_base as _split_inline_assumption_held_at_base,
)


class _PatchOpBase(BaseModel):
    op_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class AssumptionTarget(BaseModel):
    assumption_id: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class AssumptionFieldTarget(BaseModel):
    assumption_id: str = Field(min_length=1)
    field: Literal["rationale", "confidence", "unit", "driver_category", "source_refs"]

    model_config = _FROZEN_CONTRACT


class ThesisFieldTargetStr(BaseModel):
    field: Literal["statement", "direction", "strategy", "timeframe"]

    model_config = _FROZEN_CONTRACT


class ThesisFieldTargetInt(BaseModel):
    field: Literal["conviction"]

    model_config = _FROZEN_CONTRACT


class ThesisQuantitativeTarget(BaseModel):
    section: Literal[
        "revenue",
        "margins",
        "eps_fcf",
        "scenarios.bull",
        "scenarios.base",
        "scenarios.bear",
    ]
    field: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class ValuationTarget(BaseModel):
    field: Literal[
        "low",
        "mid",
        "high",
        "method",
        "current_multiple",
        "wacc",
        "risk_free_rate",
        "equity_risk_premium",
        "cost_of_equity",
        "raw_beta",
        "adjusted_beta",
        "beta_floor",
        "terminal_growth_rate",
        "terminal_multiple",
        "rationale",
    ]

    model_config = _FROZEN_CONTRACT


class StableIdTarget(BaseModel):
    id: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class WatchItemTarget(BaseModel):
    watch_item_id: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class HistoricalCoincidenceTarget(BaseModel):
    coincidence_id: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class DataGapTarget(BaseModel):
    gap_id: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class AddAssumptionValue(BaseModel):
    assumption_id: str = Field(min_length=1)
    driver: str = Field(min_length=1)
    value: float
    unit: DriverUnit
    rationale: str = Field(min_length=1)
    source_refs: list[SourceId] = Field(default_factory=list)
    driver_category: DriverCategory | None = None
    confidence: Confidence | None = None

    model_config = _FROZEN_CONTRACT


class AddRiskValue(BaseModel):
    risk_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    severity: Severity
    type: str | None = None

    model_config = _FROZEN_CONTRACT


class AddCatalystValue(BaseModel):
    catalyst_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    expected_date: str | None = None
    severity: Severity

    model_config = _FROZEN_CONTRACT


class AddTriggerValue(BaseModel):
    trigger_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    metric: str | None = None
    threshold: float | None = None
    threshold_direction: Literal["above", "below"] | None = None
    source_refs: list[SourceId] = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class AddClaimValue(BaseModel):
    claim_id: str = Field(min_length=1)
    claim: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    evidence: list[SourceId] = Field(min_length=1)
    upside_if_right: str | None = None
    downside_if_wrong: str | None = None

    model_config = _FROZEN_CONTRACT


class UpdateRiskValue(BaseModel):
    description: str | None = Field(default=None, min_length=1)
    severity: Severity | None = None
    type: str | None = None

    model_config = _FROZEN_CONTRACT


class UpdateCatalystValue(BaseModel):
    description: str | None = Field(default=None, min_length=1)
    expected_date: str | None = None
    severity: Severity | None = None

    model_config = _FROZEN_CONTRACT


class UpdateTriggerValue(BaseModel):
    description: str | None = Field(default=None, min_length=1)
    metric: str | None = None
    threshold: float | None = None
    threshold_direction: Literal["above", "below"] | None = None
    source_refs: list[SourceId] | None = Field(default=None, min_length=1)

    model_config = _FROZEN_CONTRACT


class UpdateClaimValue(BaseModel):
    claim: str | None = Field(default=None, min_length=1)
    rationale: str | None = Field(default=None, min_length=1)
    evidence: list[SourceId] | None = None
    upside_if_right: str | None = None
    downside_if_wrong: str | None = None

    model_config = _FROZEN_CONTRACT


class UpdateConsensusViewValue(BaseModel):
    narrative: str | None = None
    basis: Literal["gaap", "non_gaap"] | None = None
    citations: list[SourceId] | None = None

    model_config = _FROZEN_CONTRACT


class UpdateEpsFcfValue(BaseModel):
    """Partial update for EPS/FCF framing.

    Runtime behavior shallow-merges populated fields into eps_fcf. The bridge is
    replaced as a whole GaapNonGaapBridge object; nested bridge fields are not
    deep-merged.
    """

    projection: ScalarValue | None = None
    delta_vs_consensus: ScalarValue | None = None
    terminal_year: int | None = Field(default=None, ge=1900, le=2200)
    basis: Literal["gaap", "non_gaap"] | None = None
    bridge: GaapNonGaapBridge | None = None

    model_config = _FROZEN_CONTRACT


class UpdateScenarioMultipleValue(BaseModel):
    bull: ScenarioMultiple | None = None
    base: ScenarioMultiple | None = None
    bear: ScenarioMultiple | None = None

    model_config = _FROZEN_CONTRACT


class UpdateScenarioProbabilityValue(BaseModel):
    bull: ScenarioProbability
    base: ScenarioProbability
    bear: ScenarioProbability

    @model_validator(mode="after")
    def _weights_sum_to_one(self) -> "UpdateScenarioProbabilityValue":
        claim_ids = [self.bull.claim_id, self.base.claim_id, self.bear.claim_id]
        if len(set(claim_ids)) != len(claim_ids):
            raise ValueError(
                "scenario probabilities require distinct bull/base/bear claim_id values"
            )
        total = self.bull.weight + self.base.weight + self.bear.weight
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"scenario probabilities must sum to 1.0; got {total}")
        return self

    model_config = _FROZEN_CONTRACT


class UpdatePortfolioFitValue(BaseModel):
    """Partial update for position metadata portfolio-fit fields.

    Runtime behavior shallow-merges populated fields into portfolio_fit. The
    market_only_decomposition is replaced as a whole RiskDecomposition object;
    nested decomposition fields are not deep-merged.
    """

    sector_exposure: str | None = None
    factor_exposure: str | None = None
    correlation_cluster: str | None = None
    dominant_factor_stability: bool | None = None
    idiosyncratic_volatility_annualized_pct: float | None = None
    market_only_decomposition: RiskDecomposition | None = None

    model_config = _FROZEN_CONTRACT


class MonitoringWriteValue(BaseModel):
    watch_list: list[WatchItem] = Field(default_factory=list)
    source_refs: list[SourceId] = Field(default_factory=list)

    model_config = _FROZEN_CONTRACT

    @model_validator(mode="after")
    def _no_duplicate_caller_ids(self) -> "MonitoringWriteValue":
        provided_ids = [item.watch_item_id for item in self.watch_list if item.watch_item_id]
        duplicate_ids = sorted(
            {
                item_id
                for item_id in provided_ids
                if provided_ids.count(item_id) > 1
            }
        )
        if duplicate_ids:
            raise ValueError(
                "MonitoringWriteValue.watch_list has duplicate caller-provided "
                f"watch_item_id(s): {duplicate_ids}"
            )
        return self


class WatchItemWriteValue(BaseModel):
    last_checked: str | None = None
    description: str | None = Field(default=None, min_length=1)
    metric: str | None = None
    threshold: ThresholdValue | None = None
    threshold_direction: Literal["above", "below"] | None = None
    source_refs: list[SourceId] | None = Field(default=None, min_length=1)

    model_config = _FROZEN_CONTRACT

    @model_validator(mode="after")
    def _validate_partial_watch_item_write(self) -> "WatchItemWriteValue":
        writable_fields = {
            "last_checked",
            "description",
            "metric",
            "threshold",
            "threshold_direction",
            "source_refs",
        }
        provided_fields = self.model_fields_set & writable_fields
        if not provided_fields:
            raise ValueError(
                "update_watch_item requires at least one writable field"
            )
        if "description" in provided_fields and self.description is None:
            raise ValueError("update_watch_item.description cannot be null")
        if "source_refs" in provided_fields and self.source_refs is None:
            raise ValueError("update_watch_item.source_refs cannot be null")

        claim_fields = {
            "description",
            "metric",
            "threshold",
            "threshold_direction",
        }
        if provided_fields & claim_fields and not self.source_refs:
            raise ValueError(
                "update_watch_item claim-content changes require non-empty source_refs"
            )
        return self


class UpdateHistoricalCoincidenceValue(BaseModel):
    period: str | None = Field(default=None, min_length=1)
    factor: str | None = Field(default=None, min_length=1)
    assumption_id: str | None = None
    market_reaction: str | None = Field(default=None, min_length=1)
    factor_direction: str | None = None
    stock_outcome: Literal[
        "outperformance", "underperformance", "neutral", "mixed"
    ] | None = None
    driver: str | None = Field(default=None, min_length=1)
    source_refs: list[SourceId] | None = None

    model_config = _FROZEN_CONTRACT


class UpdateDataGapValue(BaseModel):
    target_handle: str | None = None
    description: str | None = Field(default=None, min_length=1)
    workaround: str | None = None
    severity: Literal["blocking", "approximate", "minor"] | None = None
    status: Literal["open", "resolved", "superseded", "retained"] | None = None
    resolution_note: str | None = Field(default=None, min_length=1)
    resolution_source_refs: list[SourceId] | None = None
    resolved_at: str | None = Field(default=None, min_length=1)
    superseded_by_gap_id: str | None = Field(default=None, min_length=1)

    model_config = _FROZEN_CONTRACT


class ReplaceAssumptionValueOp(_PatchOpBase):
    op: Literal["replace_assumption_value"] = "replace_assumption_value"
    target: AssumptionTarget
    value: float


class UpdateAssumptionFieldOp(_PatchOpBase):
    op: Literal["update_assumption_field"] = "update_assumption_field"
    target: AssumptionFieldTarget
    value: str | float | Confidence | list[SourceId]

    @model_validator(mode="after")
    def _value_matches_field(self) -> "UpdateAssumptionFieldOp":
        if self.target.field == "source_refs":
            if not isinstance(self.value, list) or not self.value:
                raise ValueError("source_refs requires a non-empty list of source ids")
            return self
        if self.target.field == "confidence":
            if self.value not in {"low", "medium", "high"}:
                raise ValueError("confidence must be one of low, medium, high")
            return self
        if not isinstance(self.value, str):
            raise ValueError(f"{self.target.field} requires str value")
        return self


class AddAssumptionOp(_PatchOpBase):
    op: Literal["add_assumption"] = "add_assumption"
    target: None = None
    value: AddAssumptionValue

    @model_validator(mode="before")
    @classmethod
    def _normalize_common_empty_target(cls, value: object) -> object:
        return _normalize_empty_target(value)


class RemoveAssumptionOp(_PatchOpBase):
    op: Literal["remove_assumption"] = "remove_assumption"
    target: AssumptionTarget
    value: None = None


class SetAssumptionHeldAtBaseOp(_PatchOpBase):
    op: Literal["set_assumption_held_at_base"] = "set_assumption_held_at_base"
    target: AssumptionTarget
    value: bool


class ReplaceThesisFieldStrOp(_PatchOpBase):
    op: Literal["replace_thesis_field_str"] = "replace_thesis_field_str"
    target: ThesisFieldTargetStr
    value: str | None = Field(default=None, min_length=1)


class ReplaceThesisFieldIntOp(_PatchOpBase):
    op: Literal["replace_thesis_field_int"] = "replace_thesis_field_int"
    target: ThesisFieldTargetInt
    value: int | None = None


class UpdateThesisQuantitativeOp(_PatchOpBase):
    op: Literal["update_thesis_quantitative"] = "update_thesis_quantitative"
    target: ThesisQuantitativeTarget
    value: float | str


class UpdateValuationOp(_PatchOpBase):
    op: Literal["update_valuation"] = "update_valuation"
    target: ValuationTarget
    value: float | str

    @field_validator("value", mode="before")
    @classmethod
    def _reject_bool_value(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("update_valuation value cannot be boolean")
        return value

    @model_validator(mode="after")
    def _value_matches_target_field(self) -> "UpdateValuationOp":
        field = self.target.field
        if field in _VALUATION_NUMERIC_FIELDS:
            if isinstance(self.value, str):
                raise ValueError(f"valuation.{field} requires numeric value")
            if field in _VALUATION_RATE_FIELDS and not -1.0 <= float(self.value) <= 1.0:
                raise ValueError(
                    f"valuation.{field} must be a decimal rate between -1 and 1"
                )
            return self
        if field in _VALUATION_STRING_FIELDS and not isinstance(self.value, str):
            raise ValueError(f"valuation.{field} requires string value")
        return self


class SetPriceTargetOp(_PatchOpBase):
    op: Literal["set_price_target"] = "set_price_target"
    target: None = None
    value: ThesisPriceTarget


class UpdateConsensusViewOp(_PatchOpBase):
    op: Literal["update_consensus_view"] = "update_consensus_view"
    target: None = None
    value: UpdateConsensusViewValue


class UpdateBusinessOverviewOp(_PatchOpBase):
    op: Literal["update_business_overview"] = "update_business_overview"
    target: None = None
    value: BusinessOverview


class UpdateEpsFcfOp(_PatchOpBase):
    op: Literal["update_eps_fcf"] = "update_eps_fcf"
    target: None = None
    value: UpdateEpsFcfValue


class UpdateScenarioMultipleOp(_PatchOpBase):
    op: Literal["update_scenario_multiple"] = "update_scenario_multiple"
    target: None = None
    value: UpdateScenarioMultipleValue


class UpdateScenarioProbabilityOp(_PatchOpBase):
    op: Literal["update_scenario_probability"] = "update_scenario_probability"
    target: None = None
    value: UpdateScenarioProbabilityValue


class UpdatePortfolioFitOp(_PatchOpBase):
    op: Literal["update_portfolio_fit"] = "update_portfolio_fit"
    target: None = None
    value: UpdatePortfolioFitValue


class UpdateOwnershipOp(_PatchOpBase):
    op: Literal["update_ownership"] = "update_ownership"
    target: None = None
    value: Ownership


class UpdateMonitoringOp(_PatchOpBase):
    op: Literal["update_monitoring"] = "update_monitoring"
    target: None = None
    value: MonitoringWriteValue


class UpdateWatchItemOp(_PatchOpBase):
    op: Literal["update_watch_item"] = "update_watch_item"
    target: WatchItemTarget
    value: WatchItemWriteValue


class UpdatePositionSizeOp(_PatchOpBase):
    op: Literal["update_position_size"] = "update_position_size"
    target: None = None
    value: PositionSize

    @model_validator(mode="after")
    def _requires_at_least_one_field(self) -> "UpdatePositionSizeOp":
        if self.value.target_pct is None and self.value.current_pct is None:
            raise ValueError("update_position_size requires target_pct or current_pct")
        return self


class SetDateInitiatedOp(_PatchOpBase):
    op: Literal["set_date_initiated"] = "set_date_initiated"
    target: None = None
    value: str = Field(min_length=1)

    @field_validator("value")
    @classmethod
    def _validate_iso_date(cls, value: str) -> str:
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("set_date_initiated requires ISO date YYYY-MM-DD") from exc
        if value != parsed.isoformat():
            raise ValueError("set_date_initiated requires ISO date YYYY-MM-DD")
        return value


class SetMaterialityOp(_PatchOpBase):
    op: Literal["set_materiality"] = "set_materiality"
    target: None = None
    value: MaterialityThreshold


class AddHistoricalCoincidenceOp(_PatchOpBase):
    op: Literal["add_historical_coincidence"] = "add_historical_coincidence"
    target: None = None
    value: HistoricalCoincidence


class UpdateHistoricalCoincidenceOp(_PatchOpBase):
    op: Literal["update_historical_coincidence"] = "update_historical_coincidence"
    target: HistoricalCoincidenceTarget
    value: UpdateHistoricalCoincidenceValue


class RemoveHistoricalCoincidenceOp(_PatchOpBase):
    op: Literal["remove_historical_coincidence"] = "remove_historical_coincidence"
    target: HistoricalCoincidenceTarget
    value: None = None


class AddDataGapOp(_PatchOpBase):
    op: Literal["add_data_gap"] = "add_data_gap"
    target: None = None
    value: DataGap

    @model_validator(mode="after")
    def _requires_gap_id(self) -> "AddDataGapOp":
        if not self.value.gap_id:
            raise ValueError("add_data_gap requires non-empty gap_id")
        return self


class UpdateDataGapOp(_PatchOpBase):
    op: Literal["update_data_gap"] = "update_data_gap"
    target: DataGapTarget
    value: UpdateDataGapValue


class RemoveDataGapOp(_PatchOpBase):
    op: Literal["remove_data_gap"] = "remove_data_gap"
    target: DataGapTarget
    value: None = None


class AddDifferentiatedViewClaimOp(_PatchOpBase):
    op: Literal["add_differentiated_view_claim"] = "add_differentiated_view_claim"
    target: None = None
    value: AddClaimValue


class UpdateDifferentiatedViewClaimOp(_PatchOpBase):
    op: Literal["update_differentiated_view_claim"] = "update_differentiated_view_claim"
    target: StableIdTarget
    value: UpdateClaimValue


class RemoveDifferentiatedViewClaimOp(_PatchOpBase):
    op: Literal["remove_differentiated_view_claim"] = "remove_differentiated_view_claim"
    target: StableIdTarget
    value: None = None


class AddRiskOp(_PatchOpBase):
    op: Literal["add_risk"] = "add_risk"
    target: None = None
    value: AddRiskValue


class UpdateRiskOp(_PatchOpBase):
    op: Literal["update_risk"] = "update_risk"
    target: StableIdTarget
    value: UpdateRiskValue


class RemoveRiskOp(_PatchOpBase):
    op: Literal["remove_risk"] = "remove_risk"
    target: StableIdTarget
    value: None = None


class AddCatalystOp(_PatchOpBase):
    op: Literal["add_catalyst"] = "add_catalyst"
    target: None = None
    value: AddCatalystValue


class UpdateCatalystOp(_PatchOpBase):
    op: Literal["update_catalyst"] = "update_catalyst"
    target: StableIdTarget
    value: UpdateCatalystValue


class RemoveCatalystOp(_PatchOpBase):
    op: Literal["remove_catalyst"] = "remove_catalyst"
    target: StableIdTarget
    value: None = None


class AddInvalidationTriggerOp(_PatchOpBase):
    op: Literal["add_invalidation_trigger"] = "add_invalidation_trigger"
    target: None = None
    value: AddTriggerValue


class UpdateInvalidationTriggerOp(_PatchOpBase):
    op: Literal["update_invalidation_trigger"] = "update_invalidation_trigger"
    target: StableIdTarget
    value: UpdateTriggerValue


class RemoveInvalidationTriggerOp(_PatchOpBase):
    op: Literal["remove_invalidation_trigger"] = "remove_invalidation_trigger"
    target: StableIdTarget
    value: None = None


class UpdateIndustryLandscapeOp(_PatchOpBase):
    op: Literal["update_industry_landscape"] = "update_industry_landscape"
    target: None = None
    value: IndustryLandscape


class UpdateCompsNarrativeOp(_PatchOpBase):
    op: Literal["update_comps_narrative"] = "update_comps_narrative"
    target: None = None
    value: CompsNarrative


class ReplaceIndustryPeerComparisonOp(_PatchOpBase):
    op: Literal["replace_industry_peer_comparison"] = "replace_industry_peer_comparison"
    target: None = None
    value: IndustryPeerComparison


class AddEditorialPeerOp(_PatchOpBase):
    op: Literal["add_editorial_peer"] = "add_editorial_peer"
    target: None = None
    value: EditorialPeer


class RemoveEditorialPeerOp(_PatchOpBase):
    op: Literal["remove_editorial_peer"] = "remove_editorial_peer"
    target: StableIdTarget
    value: None = None


class SetEditorialPeerSetOp(_PatchOpBase):
    op: Literal["set_editorial_peer_set"] = "set_editorial_peer_set"
    target: None = None
    value: list[EditorialPeer]

    @field_validator("value")
    @classmethod
    def _reject_duplicate_tickers(
        cls, value: list[EditorialPeer]
    ) -> list[EditorialPeer]:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for peer in value:
            if peer.ticker in seen:
                duplicates.add(peer.ticker)
            seen.add(peer.ticker)
        if duplicates:
            raise ValueError(
                "editorial_peer_set contains duplicate tickers: "
                f"{sorted(duplicates)}"
            )
        return value


class PeerComparisonSectionsValue(_ContractModel):
    """Payload for patching v1.2 peer-comparison fields without touching peers."""

    sections: list[SnapshotSection] = Field(default_factory=list)
    industry_key: str | None = None
    template_manifest_id: str | None = None
    as_of: str | None = None


class SetPeerComparisonSectionsOp(_PatchOpBase):
    op: Literal["set_peer_comparison_sections"] = "set_peer_comparison_sections"
    target: None = None
    value: PeerComparisonSectionsValue


class SetOperatingComparisonOp(_PatchOpBase):
    op: Literal["set_operating_comparison"] = "set_operating_comparison"
    target: None = None
    value: OperatingComparison


class RegisterSourcesOp(_PatchOpBase):
    op: Literal["register_sources"] = "register_sources"
    target: None = None
    value: list[SourceRecord]

    @model_validator(mode="before")
    @classmethod
    def _normalize_common_source_wrappers(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        return _normalize_empty_target(value)


class UpdateMacroOverlayOp(_PatchOpBase):
    op: Literal["update_macro_overlay"] = "update_macro_overlay"
    target: None = None
    value: MacroOverlay


class ReplaceStructuralTrendsOp(_PatchOpBase):
    op: Literal["replace_structural_trends"] = "replace_structural_trends"
    target: None = None
    value: list[StructuralTrend]


HandoffPatchOp = Annotated[
    Union[
        ReplaceAssumptionValueOp,
        UpdateAssumptionFieldOp,
        AddAssumptionOp,
        RemoveAssumptionOp,
        SetAssumptionHeldAtBaseOp,
        ReplaceThesisFieldStrOp,
        ReplaceThesisFieldIntOp,
        UpdateThesisQuantitativeOp,
        UpdateValuationOp,
        SetPriceTargetOp,
        UpdateConsensusViewOp,
        UpdateBusinessOverviewOp,
        UpdateEpsFcfOp,
        UpdateScenarioMultipleOp,
        UpdateScenarioProbabilityOp,
        UpdatePortfolioFitOp,
        UpdateOwnershipOp,
        UpdateMonitoringOp,
        UpdateWatchItemOp,
        UpdatePositionSizeOp,
        SetDateInitiatedOp,
        SetMaterialityOp,
        AddHistoricalCoincidenceOp,
        UpdateHistoricalCoincidenceOp,
        RemoveHistoricalCoincidenceOp,
        AddDataGapOp,
        UpdateDataGapOp,
        RemoveDataGapOp,
        AddDifferentiatedViewClaimOp,
        UpdateDifferentiatedViewClaimOp,
        RemoveDifferentiatedViewClaimOp,
        AddRiskOp,
        UpdateRiskOp,
        RemoveRiskOp,
        AddCatalystOp,
        UpdateCatalystOp,
        RemoveCatalystOp,
        AddInvalidationTriggerOp,
        UpdateInvalidationTriggerOp,
        RemoveInvalidationTriggerOp,
        UpdateIndustryLandscapeOp,
        UpdateCompsNarrativeOp,
        ReplaceIndustryPeerComparisonOp,
        AddEditorialPeerOp,
        RemoveEditorialPeerOp,
        SetEditorialPeerSetOp,
        SetPeerComparisonSectionsOp,
        SetOperatingComparisonOp,
        RegisterSourcesOp,
        UpdateMacroOverlayOp,
        ReplaceStructuralTrendsOp,
    ],
    Field(discriminator="op"),
]


class HandoffPatchBatch(BaseModel):
    ops: list[HandoffPatchOp] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_live_agent_patch_payload(cls, value: object) -> object:
        return _normalize_patch_batch_payload(value)

    @model_validator(mode="after")
    def _unique_op_ids(self) -> "HandoffPatchBatch":
        seen: set[str] = set()
        duplicates: set[str] = set()
        for op in self.ops:
            if op.op_id in seen:
                duplicates.add(op.op_id)
            seen.add(op.op_id)
        if duplicates:
            raise ValueError(
                f"op_ids must be unique within batch; duplicates: {sorted(duplicates)}"
            )
        return self

    model_config = _FROZEN_CONTRACT


__all__ = [
    "AddEditorialPeerOp",
    "AddAssumptionOp",
    "AddAssumptionValue",
    "AddCatalystOp",
    "AddCatalystValue",
    "AddClaimValue",
    "AddDataGapOp",
    "AddDifferentiatedViewClaimOp",
    "AddHistoricalCoincidenceOp",
    "AddInvalidationTriggerOp",
    "AddRiskOp",
    "AddRiskValue",
    "AddTriggerValue",
    "AssumptionFieldTarget",
    "AssumptionTarget",
    "DataGapTarget",
    "HandoffPatchBatch",
    "HandoffPatchOp",
    "HistoricalCoincidenceTarget",
    "MonitoringWriteValue",
    "PeerComparisonSectionsValue",
    "RemoveAssumptionOp",
    "RemoveCatalystOp",
    "RemoveDataGapOp",
    "RemoveDifferentiatedViewClaimOp",
    "RemoveEditorialPeerOp",
    "RemoveHistoricalCoincidenceOp",
    "RemoveInvalidationTriggerOp",
    "RemoveRiskOp",
    "RegisterSourcesOp",
    "ReplaceAssumptionValueOp",
    "ReplaceIndustryPeerComparisonOp",
    "ReplaceThesisFieldIntOp",
    "ReplaceThesisFieldStrOp",
    "ReplaceStructuralTrendsOp",
    "SetAssumptionHeldAtBaseOp",
    "SetDateInitiatedOp",
    "SetEditorialPeerSetOp",
    "SetMaterialityOp",
    "SetOperatingComparisonOp",
    "SetPeerComparisonSectionsOp",
    "SetPriceTargetOp",
    "StableIdTarget",
    "ThesisFieldTargetInt",
    "ThesisFieldTargetStr",
    "ThesisQuantitativeTarget",
    "UpdateAssumptionFieldOp",
    "UpdateBusinessOverviewOp",
    "UpdateCatalystOp",
    "UpdateCatalystValue",
    "UpdateClaimValue",
    "UpdateCompsNarrativeOp",
    "UpdateConsensusViewOp",
    "UpdateConsensusViewValue",
    "UpdateDataGapOp",
    "UpdateDataGapValue",
    "UpdateDifferentiatedViewClaimOp",
    "UpdateEpsFcfOp",
    "UpdateEpsFcfValue",
    "UpdateScenarioMultipleOp",
    "UpdateScenarioMultipleValue",
    "UpdateScenarioProbabilityOp",
    "UpdateScenarioProbabilityValue",
    "UpdateHistoricalCoincidenceOp",
    "UpdateHistoricalCoincidenceValue",
    "UpdateIndustryLandscapeOp",
    "UpdateInvalidationTriggerOp",
    "UpdateMacroOverlayOp",
    "UpdateMonitoringOp",
    "UpdateOwnershipOp",
    "UpdatePortfolioFitOp",
    "UpdatePortfolioFitValue",
    "UpdatePositionSizeOp",
    "UpdateRiskOp",
    "UpdateRiskValue",
    "UpdateThesisQuantitativeOp",
    "UpdateTriggerValue",
    "UpdateValuationOp",
    "UpdateWatchItemOp",
    "ValuationTarget",
    "WatchItemTarget",
    "WatchItemWriteValue",
]
