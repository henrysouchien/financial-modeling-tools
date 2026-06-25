from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from ._insights_shared import (
    Confidence,
    DriverCategory,
    DriverUnit,
    Severity,
    _FROZEN_CONTRACT,
)
from .thesis import (
    GaapNonGaapBridge,
    RiskDecomposition,
    ScenarioMultiple,
    ScenarioProbability,
)
from .thesis_shared_slice import (
    SnapshotSection,
    SourceId,
    ThresholdValue,
    WatchItem,
    _ContractModel,
    ScalarValue,
)


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
    remediation: dict[str, object] | None = None
    resolution_note: str | None = Field(default=None, min_length=1)
    resolution_source_refs: list[SourceId] | None = None
    resolved_at: str | None = Field(default=None, min_length=1)
    superseded_by_gap_id: str | None = Field(default=None, min_length=1)

    model_config = _FROZEN_CONTRACT


class PeerComparisonSectionsValue(_ContractModel):
    """Payload for patching v1.2 peer-comparison fields without touching peers."""

    sections: list[SnapshotSection] = Field(default_factory=list)
    industry_key: str | None = None
    template_manifest_id: str | None = None
    as_of: str | None = None


__all__ = [
    "AddAssumptionValue",
    "AddCatalystValue",
    "AddClaimValue",
    "AddRiskValue",
    "AddTriggerValue",
    "MonitoringWriteValue",
    "PeerComparisonSectionsValue",
    "UpdateCatalystValue",
    "UpdateClaimValue",
    "UpdateConsensusViewValue",
    "UpdateDataGapValue",
    "UpdateEpsFcfValue",
    "UpdateHistoricalCoincidenceValue",
    "UpdatePortfolioFitValue",
    "UpdateRiskValue",
    "UpdateScenarioMultipleValue",
    "UpdateScenarioProbabilityValue",
    "UpdateTriggerValue",
    "WatchItemWriteValue",
]
