from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, field_validator

from .business_model import CompileTargetType, DriverAssumptionPlan, Factor
from .models import Unit
from .model_readiness import (
    ModelQualityReadiness,
    ModelProjectionReadiness,
    ModelScenarioBridgeReadiness,
    ModelScenarioOutputReadiness,
    ValuationInputReadiness,
)
from .thesis import BusinessModelRef, QuantitativeFraming, SummaryStatus
from .thesis_shared_slice import (
    Assumption,
    BusinessOverview,
    Catalyst,
    CompanyField,
    ConsensusView,
    DataGap,
    DifferentiatedViewClaim,
    HistoricalCoincidence,
    IndustryAnalysis,
    InvalidationTrigger,
    MaterialityThreshold,
    Monitoring,
    Ownership,
    Peer,
    QualitativeFactor,
    Risk,
    SourceRecord,
    ThesisField,
    Valuation,
    _ContractModel,
    _normalize_optional_identifier,
)


AnnotationIdentifier = str
DiligenceCompletionState = Literal["empty", "draft", "confirmed"]
FinancialsSource = Literal["fmp", "edgar"]
ModelFeedbackStatus = Literal["success", "missing", "skipped", "disabled", "error", "pending", "unknown"]
ModelArtifactStatus = Literal["present", "missing", "unknown"]


def _require_identifier(value: object | None, *, field_name: str) -> str:
    normalized = _normalize_optional_identifier(value)
    if normalized is None:
        raise ValueError(f"{field_name} is required")
    return normalized


class IdeaProvenance(_ContractModel):
    idea_id: str
    source_summary: str

    @field_validator("idea_id", mode="before")
    @classmethod
    def _normalize_idea_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="idea_id")


class AssumptionLineageEntry(_ContractModel):
    assumption_id: str
    supporting_annotation_ids: list[AnnotationIdentifier] = Field(min_length=1)
    refutes_annotation_ids: list[AnnotationIdentifier] | None = None

    @field_validator("assumption_id", mode="before")
    @classmethod
    def _normalize_assumption_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="assumption_id")


class ScorecardRef(_ContractModel):
    scorecard_id: str
    version: int = Field(ge=1)
    scored_at: str
    summary_status: SummaryStatus

    @field_validator("scorecard_id", mode="before")
    @classmethod
    def _normalize_scorecard_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="scorecard_id")


class ThesisRef(_ContractModel):
    thesis_id: str
    version: int = Field(ge=1)
    markdown_path: str | None = None

    @field_validator("thesis_id", mode="before")
    @classmethod
    def _normalize_thesis_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="thesis_id")


class HandoffModelRef(_ContractModel):
    model_id: str
    version: int = Field(ge=1)
    model_build_context_id: str
    model_build_context_version: int = Field(ge=1)
    file_path: str | None = None
    diagnostic_status: ModelArtifactStatus = "unknown"
    seed_projection_status: ModelArtifactStatus = "unknown"
    projection_readiness: ModelProjectionReadiness | None = None
    scenario_output_readiness: ModelScenarioOutputReadiness | None = None
    scenario_bridge_readiness: ModelScenarioBridgeReadiness | None = None
    model_quality_readiness: ModelQualityReadiness | None = None
    valuation_input_readiness: ValuationInputReadiness | None = None
    last_price_target: float | None = None

    @field_validator("model_id", mode="before")
    @classmethod
    def _normalize_model_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="model_id")

    @field_validator("model_build_context_id", mode="before")
    @classmethod
    def _normalize_model_build_context_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="model_build_context_id")


class ForecastAssumptionWriteTarget(_ContractModel):
    source: Literal["DriverAssumptionPlan"] = "DriverAssumptionPlan"
    driver_key: str
    item_id: str | None = None
    label: str
    unit: Unit
    driver_unit: Unit | None = None
    value_unit: Unit | None = None
    factors: list[Factor] = Field(default_factory=list)
    compile_target_type: CompileTargetType
    existing_driver_key: str | None = None
    business_model_id: str
    revision: str
    segment_id: str
    driver_node_id: str
    aliases: list[str] = Field(default_factory=list)

    @field_validator(
        "driver_key",
        "label",
        "business_model_id",
        "revision",
        "segment_id",
        "driver_node_id",
        mode="before",
    )
    @classmethod
    def _normalize_required_write_target_identifier(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="forecast_assumption_write_target")


class CurrentModelRef(_ContractModel):
    ticker: str
    research_file_id: int = Field(ge=1)
    handoff_id: int = Field(ge=1)
    handoff_version: int = Field(ge=1)
    handoff_status: str
    model_id: str
    version: int = Field(ge=1)
    model_build_context_id: str
    model_build_context_version: int = Field(ge=1)
    workbook_path: str
    file_path: str
    workbook_exists: bool = False
    diagnostic_status: ModelArtifactStatus = "unknown"
    seed_projection_status: ModelArtifactStatus = "unknown"
    projection_readiness: ModelProjectionReadiness | None = None
    scenario_output_readiness: ModelScenarioOutputReadiness | None = None
    scenario_bridge_readiness: ModelScenarioBridgeReadiness | None = None
    model_quality_readiness: ModelQualityReadiness | None = None
    valuation_input_readiness: ValuationInputReadiness | None = None
    business_model_ref: BusinessModelRef | None = None
    driver_assumption_plan: DriverAssumptionPlan | None = None
    driver_assumption_plan_status: ModelFeedbackStatus = "missing"
    driver_assumption_plan_error: dict[str, Any] | None = None
    forecast_assumption_write_targets: list[ForecastAssumptionWriteTarget] = Field(default_factory=list)
    model_semantics_status: ModelFeedbackStatus = "missing"
    model_semantics_hash: str | None = None
    model_insights_status: ModelFeedbackStatus = "missing"
    model_insights_id: str | None = None
    price_target_status: ModelFeedbackStatus = "missing"
    price_target_id: str | None = None
    last_price_target: float | None = None
    price_target_as_of: str | None = None
    price_target_current_price: float | None = None
    price_target_current_price_source: str | None = None
    price_target_current_price_provider: str | None = None
    price_target_current_price_as_of: str | None = None
    price_target_current_price_retrieved_at: str | None = None
    price_target_current_price_currency: str | None = None
    price_target_horizon_basis: str | None = None
    price_target_horizon_fiscal_period: str | None = None
    price_target_horizon_fiscal_year_end: str | None = None
    price_target_horizon_months_to_period_end: int | None = None
    price_target_horizon_caveat: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def _normalize_current_model_ticker(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="ticker").upper()

    @field_validator("handoff_status", "model_id", "model_build_context_id", "workbook_path", "file_path", mode="before")
    @classmethod
    def _normalize_required_current_model_identifier(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="current_model")


class Financials(_ContractModel):
    source: FinancialsSource = "fmp"
    data: dict[str, Any] = Field(default_factory=dict)


class HandoffMetadata(_ContractModel):
    diligence_completion: dict[str, DiligenceCompletionState] = Field(default_factory=dict)
    diligence_sections: dict[str, Any] = Field(default_factory=dict)
    next_factor_id: int = Field(default=1, ge=1)
    analyst_session_id: str | None = None
    modeling_workflow_state_cache: dict[str, Any] | None = None


class HandoffArtifactV1_1(_ContractModel):
    handoff_id: int = Field(ge=1)
    created_at: float = Field(ge=0)
    research_file_id: int = Field(ge=1)

    company: CompanyField
    thesis: ThesisField = Field(default_factory=ThesisField)
    consensus_view: ConsensusView | None = None
    differentiated_view: list[DifferentiatedViewClaim] = Field(default_factory=list)
    invalidation_triggers: list[InvalidationTrigger] = Field(default_factory=list)
    business_overview: BusinessOverview | None = None
    catalysts: list[Catalyst] = Field(default_factory=list)
    risks: list[Risk] = Field(default_factory=list)
    valuation: Valuation | None = None
    peers: list[Peer] = Field(default_factory=list)
    assumptions: list[Assumption] = Field(default_factory=list)
    qualitative_factors: list[QualitativeFactor] = Field(default_factory=list)
    ownership: Ownership | None = None
    monitoring: Monitoring | None = None
    sources: list[SourceRecord] = Field(default_factory=list)
    industry_analysis: IndustryAnalysis | None = None
    materiality: MaterialityThreshold | None = None
    historical_coincidences: list[HistoricalCoincidence] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)
    quantitative_framing: QuantitativeFraming | None = None

    idea_provenance: IdeaProvenance | None = None
    assumption_lineage: list[AssumptionLineageEntry] | None = None
    process_template_id: str | None = None
    scorecard_ref: ScorecardRef | None = None
    thesis_ref: ThesisRef | None = None
    model_ref: HandoffModelRef | None = None
    business_model_ref: BusinessModelRef | None = None
    financials: Financials | None = None
    metadata: HandoffMetadata = Field(default_factory=HandoffMetadata)

    schema_version: Literal["1.1", "1.2"] = "1.2"

    @field_validator("process_template_id", mode="before")
    @classmethod
    def _normalize_process_template_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


__all__ = [
    "AnnotationIdentifier",
    "AssumptionLineageEntry",
    "CurrentModelRef",
    "DiligenceCompletionState",
    "Financials",
    "FinancialsSource",
    "ForecastAssumptionWriteTarget",
    "HandoffArtifactV1_1",
    "HandoffMetadata",
    "HandoffModelRef",
    "IdeaProvenance",
    "ModelArtifactStatus",
    "ModelFeedbackStatus",
    "ModelProjectionReadiness",
    "ModelScenarioBridgeReadiness",
    "ModelScenarioOutputReadiness",
    "ScorecardRef",
    "ThesisRef",
]
