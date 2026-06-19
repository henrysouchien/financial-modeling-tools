from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any, Literal, TypeAlias

from pydantic import Field, field_validator, model_validator

from ._insights_shared import Confidence, DriverUnit, Severity
from ._outcome_shared import OutcomeComponent
from .models import DriverCategory
from .price_target import CurrentPriceSnapshot, ValuationHorizon
from .research_labels import canonicalize_optional_research_label
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
    ScalarValue,
    SourceId,
    SourceRecord,
    ThesisField,
    Valuation,
    _ContractModel,
    _normalize_optional_identifier,
    _normalize_ticker,
)


ThesisLinkCategory = Literal["revenue", "margin", "growth", "valuation", "catalyst", "risk"]
ThesisDirectionComparison = Literal["above_consensus", "below_consensus", "specific_value", "directional"]
ResolutionAnchor = Literal[
    "driver_key",
    "business_model_node",
    "data_concept_id",
    "structural_fingerprint",
    "template_version_cache",
    "model_item_id",
    "none",
]
ScorecardEntryStatus = Literal["confirmed", "tracking", "challenged", "disconfirmed", "unresolvable"]
ActualVsThesisStatus = Literal["above", "at", "below", "not_yet_reported"]
ConsensusVsThesisStatus = Literal["converging", "diverging", "aligned"]
ScorecardEvidenceStatus = Literal[
    "bound",
    "out_of_period",
    "unmatched",
    "unusable",
    "unbound",
    "ambiguous",
    "not_required",
]
SummaryStatus = Literal["on_track", "mixed", "at_risk", "invalidated"]
PatchOpPreview: TypeAlias = dict[str, Any]
DecisionValue: TypeAlias = ScalarValue | dict[str, Any] | list[Any]
_SKILL_NAME_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def _rank_is_dense_from_one(ranks: list[int], *, field_name: str) -> None:
    if ranks != list(range(1, len(ranks) + 1)):
        raise ValueError(f"{field_name}.rank must be dense [1..{len(ranks)}]; got {ranks}")


class UnknownSection(_ContractModel):
    anchor_after: str | None = None
    content: str


class ScenarioMultiple(_ContractModel):
    type: str
    multiple: float
    basis: str
    historical_low: float | None = None
    historical_median: float | None = None
    historical_high: float | None = None
    current_percentile: float | None = None
    peg_check: str | None = None


class ScenarioProbability(_ContractModel):
    claim_id: str = Field(min_length=1)
    weight: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)
    source_refs: list[SourceId] = Field(min_length=1)


class ThesisQuantitativeScenario(_ContractModel):
    target_price: float | None = None
    return_pct: float | None = None
    scenario_multiple: ScenarioMultiple | None = None
    probability: ScenarioProbability | None = None
    revenue_m: ScalarValue | None = None
    op_margin_pct: ScalarValue | None = None
    ebitda_margin_pct: ScalarValue | None = None
    eps: ScalarValue | None = None
    eps_gaap: ScalarValue | None = None
    eps_non_gaap: ScalarValue | None = None
    adj_eps: ScalarValue | None = None
    fcf_per_share: ScalarValue | None = None
    what_has_to_happen: str | None = None
    methodology: str | None = None
    what_goes_wrong: str | None = None


class ScenarioFraming(_ContractModel):
    bull: ThesisQuantitativeScenario | None = None
    base: ThesisQuantitativeScenario | None = None
    bear: ThesisQuantitativeScenario | None = None

    @model_validator(mode="after")
    def _probabilities_are_complete_and_sum_to_one(self) -> "ScenarioFraming":
        probabilities = [
            scenario.probability
            for scenario in (self.bull, self.base, self.bear)
            if scenario is not None and scenario.probability is not None
        ]
        if not probabilities:
            return self
        scenarios = (("bull", self.bull), ("base", self.base), ("bear", self.bear))
        if self.bull is None or self.base is None or self.bear is None:
            raise ValueError("scenario probabilities require bull/base/bear scenarios")
        if not (self.bull.probability and self.base.probability and self.bear.probability):
            raise ValueError("scenario probabilities require bull/base/bear weights")
        missing_targets = [
            name
            for name, scenario in scenarios
            if scenario is not None and scenario.target_price is None
        ]
        if missing_targets:
            joined = ", ".join(missing_targets)
            raise ValueError(
                f"scenario probabilities require target prices for bull/base/bear; missing {joined}"
            )
        claim_ids = [
            self.bull.probability.claim_id,
            self.base.probability.claim_id,
            self.bear.probability.claim_id,
        ]
        if len(set(claim_ids)) != len(claim_ids):
            raise ValueError(
                "scenario probabilities require distinct bull/base/bear claim_id values"
            )
        total = (
            self.bull.probability.weight
            + self.base.probability.weight
            + self.bear.probability.weight
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"scenario probabilities must sum to 1.0; got {total}")
        return self


class RevenueQuantitativeFraming(_ContractModel):
    base: ScalarValue | None = None
    bull: ScalarValue | None = None
    bear: ScalarValue | None = None
    rationale: str | None = None


class MarginsQuantitativeFraming(_ContractModel):
    trajectory: str | None = None
    key_drivers: list[str] = Field(default_factory=list)


class GaapNonGaapBridge(_ContractModel):
    """Reconciliation between GAAP and non-GAAP terminal values."""

    metric: str
    period: str | None = None
    gaap_value: ScalarValue
    non_gaap_value: ScalarValue
    bridge_value: ScalarValue | None = None
    bridge_components: list[str] = Field(default_factory=list)
    rationale: str | None = None
    approximation_quality: Literal["reported", "estimated", "rough"] | None = None
    source_refs: list[SourceId] = Field(default_factory=list)


class EpsFcfQuantitativeFraming(_ContractModel):
    projection: ScalarValue | None = None
    delta_vs_consensus: ScalarValue | None = None
    terminal_year: int | None = Field(default=None, ge=1900, le=2200)
    basis: Literal["gaap", "non_gaap"] | None = None
    bridge: GaapNonGaapBridge | None = None


class QuantitativeFraming(_ContractModel):
    revenue: RevenueQuantitativeFraming | None = None
    margins: MarginsQuantitativeFraming | None = None
    eps_fcf: EpsFcfQuantitativeFraming | None = None
    scenarios: ScenarioFraming | None = None


class PositionSize(_ContractModel):
    target_pct: float | None = None
    current_pct: float | None = None


class RiskDecomposition(_ContractModel):
    systematic_proxy: str | None = None
    factor_variance_share_pct: float | None = None
    idiosyncratic_variance_share_pct: float | None = None
    idiosyncratic_volatility_monthly_pct: float | None = None
    idiosyncratic_volatility_annualized_pct: float | None = None
    note: str | None = None


class PortfolioFit(_ContractModel):
    sector_exposure: str | None = None
    factor_exposure: str | None = None
    correlation_cluster: str | None = None
    dominant_factor_stability: bool | None = None
    idiosyncratic_volatility_annualized_pct: float | None = None
    market_only_decomposition: RiskDecomposition | None = None


class PositionMetadata(_ContractModel):
    position_size: PositionSize | None = None
    date_initiated: str | None = None
    portfolio_fit: PortfolioFit | None = None


class ThesisModelRef(_ContractModel):
    model_id: str
    version: int
    file_path: str | None = None
    last_updated: str | None = None
    drivers_locked: list[str] = Field(default_factory=list)


class BusinessModelRef(_ContractModel):
    """Reference to a concrete BusinessModel artifact revision."""

    business_model_id: str = Field(min_length=1)
    schema_version: str = "1.0"
    revision: str = Field(min_length=1)
    last_updated: str = Field(min_length=1)


class ThesisModelVersionRef(_ContractModel):
    model_id: str = Field(min_length=1)
    version: int = Field(ge=1)


class ThesisDriverSensitivity(_ContractModel):
    driver_key: str = Field(min_length=1)
    target_metric: str = Field(min_length=1)
    impact_per_unit: float | None = None
    rank: int | None = Field(default=None, ge=1)
    periods: list[int] | None = None
    computation_status: Literal[
        "computed",
        "pinned_by_policy",
        "unresolved",
        "not_upstream",
        "unavailable",
        "legacy_diagnostic",
    ] = "computed"
    sensitivity_mode: Literal[
        "semantic_driver",
        "workbook_explicit",
        "workbook_global",
        "legacy_global",
    ] | None = None
    recompute_policy: Literal["projection_safe", "legacy_global"] | None = None
    shocked_item_id: str | None = None
    target_period: int | None = None
    bump_pct: float | None = None
    impact_basis: str | None = None
    candidate_scope: Literal[
        "semantic_driver_keys",
        "explicit_workbook_ids",
        "global_ranked",
        "mbc_drivers",
        "legacy_global",
    ] | None = None
    readout_item_ids: list[str] = Field(default_factory=list)
    display_label: str | None = None
    impact_direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] | None = None
    expected_direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] | None = None
    expected_moved_item_ids: list[str] = Field(default_factory=list)
    expected_pinned_item_ids: list[str] = Field(default_factory=list)
    temporal_pattern: Literal[
        "immediate",
        "lagged",
        "ramp",
        "fade",
        "capped",
        "cyclical",
        "terminal",
        "unknown",
    ] | None = None
    reconciliation_status: Literal[
        "matched_prior",
        "contradicts_prior",
        "requires_review",
        "not_evaluated",
        "legacy_unreviewed",
    ] | None = None
    judgment_notes: str | None = None
    warnings: list[dict[str, object]] = Field(default_factory=list)


class ThesisImpliedAssumption(_ContractModel):
    driver_key: str = Field(min_length=1)
    sia_category: DriverCategory | None = None
    value: float
    unit: DriverUnit
    rationale: str = Field(min_length=1)
    suggests_patch: bool = False

    @field_validator("sia_category", mode="before")
    @classmethod
    def _canonicalize_sia_category(cls, value: object | None) -> object | None:
        if value is None:
            return None
        return re.sub(r"[\s\-_]+", "_", str(value).strip()).lower()


class ThesisRiskSurfaced(_ContractModel):
    description: str = Field(min_length=1)
    severity: Severity
    type: str | None = None
    evidence: list[SourceId] = Field(default_factory=list)


class ThesisModelInsight(_ContractModel):
    model_insights_id: str = Field(min_length=1)
    model_ref: ThesisModelVersionRef
    derived_from_model_build_context_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    driver_sensitivities: list[ThesisDriverSensitivity] = Field(default_factory=list)
    implied_assumptions: list[ThesisImpliedAssumption] = Field(default_factory=list)
    risks_surfaced: list[ThesisRiskSurfaced] = Field(default_factory=list)
    schema_version: Literal["1.0", "1.1"] = "1.1"

    @field_validator("driver_sensitivities")
    @classmethod
    def _rank_is_dense_from_one(
        cls,
        value: list[ThesisDriverSensitivity],
    ) -> list[ThesisDriverSensitivity]:
        if not value:
            return value
        for item in value:
            if item.computation_status != "computed" and item.rank is not None:
                raise ValueError("non-computed driver_sensitivities rows must not be ranked")
            if item.computation_status == "computed" and (
                item.impact_per_unit is None or item.rank is None
            ):
                raise ValueError("computed driver_sensitivities rows require impact_per_unit and rank")
        ranks = sorted(
            item.rank
            for item in value
            if item.computation_status == "computed" and item.rank is not None
        )
        if not ranks:
            return value
        _rank_is_dense_from_one(ranks, field_name="driver_sensitivities")
        return value


class ThesisPriceTargetRanges(_ContractModel):
    low: float
    mid: float
    high: float

    @model_validator(mode="after")
    def _ordered(self) -> "ThesisPriceTargetRanges":
        if not (self.low <= self.mid <= self.high):
            raise ValueError(
                f"ranges must satisfy low<=mid<=high; got ({self.low}, {self.mid}, {self.high})"
            )
        return self


class ThesisPriceTargetDriverSensitivity(_ContractModel):
    driver_key: str = Field(min_length=1)
    delta_per_pct: float
    rank: int = Field(ge=1)


class ThesisPriceTarget(_ContractModel):
    price_target_id: str = Field(min_length=1)
    model_ref: ThesisModelVersionRef
    derived_from_model_build_context_id: str = Field(min_length=1)
    as_of: str = Field(min_length=1)
    ranges: ThesisPriceTargetRanges
    confidence: Confidence
    method: str | None = Field(default=None, min_length=1)
    driver_sensitivities: list[ThesisPriceTargetDriverSensitivity] = Field(default_factory=list)
    time_horizon_months: int = Field(ge=1, le=120)
    current_price: float = Field(gt=0)
    current_price_snapshot: CurrentPriceSnapshot | None = None
    valuation_horizon: ValuationHorizon | None = None
    implied_return_pct: float
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("driver_sensitivities")
    @classmethod
    def _rank_dense(
        cls,
        value: list[ThesisPriceTargetDriverSensitivity],
    ) -> list[ThesisPriceTargetDriverSensitivity]:
        if not value:
            return value
        _rank_is_dense_from_one(
            sorted(item.rank for item in value),
            field_name="driver_sensitivities",
        )
        return value

    @model_validator(mode="after")
    def _current_price_snapshot_matches_current_price(self) -> "ThesisPriceTarget":
        if (
            self.current_price_snapshot is not None
            and abs(float(self.current_price_snapshot.price) - float(self.current_price)) > 1e-9
        ):
            raise ValueError("current_price_snapshot.price must equal current_price")
        return self


class ThesisFromIdea(_ContractModel):
    idea_id: str
    thesis_hypothesis: str
    seeded_at: str
    schema_version: Literal["1.0"] = "1.0"


class DecisionLogArtifactRef(_ContractModel):
    artifact_path: str
    artifact_id: str | None = None
    skill_name: str | None = None
    run_id: str | None = None
    source_path: str | None = None

    @field_validator("artifact_id", "run_id", mode="before")
    @classmethod
    def _normalize_optional_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("skill_name", mode="before")
    @classmethod
    def _normalize_skill_name(cls, value: object | None) -> str | None:
        normalized = _normalize_optional_identifier(value)
        if normalized is not None and not _SKILL_NAME_RE.fullmatch(normalized):
            raise ValueError("skill_name must use lowercase kebab-case")
        return normalized

    @field_validator("artifact_path", "source_path", mode="before")
    @classmethod
    def _normalize_workspace_path(cls, value: object | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if "\\" in text:
            raise ValueError("workspace-relative paths cannot contain backslashes")
        path = PurePosixPath(text)
        if path.is_absolute():
            raise ValueError("workspace-relative paths cannot be absolute")
        if ".." in path.parts:
            raise ValueError("workspace-relative paths cannot contain '..'")
        return path.as_posix()

    @model_validator(mode="after")
    def _validate_paths(self) -> "DecisionLogArtifactRef":
        if not self.artifact_path:
            raise ValueError("artifact_path is required")
        if self.artifact_path.startswith("artifacts/"):
            if PurePosixPath(self.artifact_path).suffix != ".json":
                raise ValueError("artifacts/ artifact refs must point to .json typed sidecars")
        elif self.artifact_path.startswith("skills/"):
            if PurePosixPath(self.artifact_path).suffix != ".md":
                raise ValueError("skills/ artifact refs must point to .md deliverables")
        else:
            raise ValueError("artifact_path must live under artifacts/ or skills/")
        if self.source_path is not None:
            if not self.source_path.startswith(("notes/skills/", "skills/")):
                raise ValueError("source_path must live under notes/skills/ or skills/")
            if PurePosixPath(self.source_path).suffix != ".md":
                raise ValueError("source_path must point to a .md deliverable")
        return self


_DECISION_LOG_ARTIFACT_REF_KINDS = {
    "artifact",
    "artifact_ref",
    "artifact_path",
    "deliverable",
    "markdown",
    "skill",
    "skill_artifact",
    "typed_artifact",
}


def _normalize_decision_log_artifact_ref(value: Any) -> Any:
    if isinstance(value, DecisionLogArtifactRef):
        return value
    if isinstance(value, str):
        return {"artifact_path": value}
    if not isinstance(value, dict):
        return value
    item = dict(value)
    if "artifact_path" not in item and "ref" in item:
        kind = _normalize_optional_identifier(item.get("kind"))
        if kind is None or kind in _DECISION_LOG_ARTIFACT_REF_KINDS:
            item["artifact_path"] = item.pop("ref")
            item.pop("kind", None)
    return item


class DecisionsLogEntry(_ContractModel):
    entry_id: str | None = None
    date: str
    skill: str
    verdict: str | None = None
    decision: str
    rationale: str
    previous_value: DecisionValue | None = None
    new_value: DecisionValue | None = None
    patch_ops_applied: list[PatchOpPreview] = Field(default_factory=list)
    run_id: str | None = None
    artifact_refs: list[DecisionLogArtifactRef] = Field(default_factory=list)

    @field_validator("entry_id", "run_id", mode="before")
    @classmethod
    def _normalize_entry_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("artifact_refs", mode="before")
    @classmethod
    def _normalize_artifact_refs(cls, value: object | None) -> object:
        if value is None:
            return []
        if isinstance(value, str):
            return [_normalize_decision_log_artifact_ref(value)]
        if isinstance(value, list):
            return [_normalize_decision_log_artifact_ref(item) for item in value]
        return value

    @field_validator("verdict", mode="before")
    @classmethod
    def _normalize_verdict(cls, value: object | None) -> str | None:
        normalized = _normalize_optional_identifier(value)
        return normalized.upper() if normalized is not None else None


class StructuralFingerprint(_ContractModel):
    sheet: str
    section_id: str
    driver_category: DriverCategory | None = None
    repeat_group_id: str | None = None
    repeat_group_role: str | None = None
    segment_index: int | None = Field(default=None, ge=1)
    segment_edgar_member: str | None = None
    label_pattern: str | None = None
    position_index: int | None = Field(default=None, ge=1)


class ThesisLink(_ContractModel):
    thesis_link_id: str | None = None
    thesis_point_id: str
    thesis_text: str
    category: ThesisLinkCategory
    driver_key: str | None = None
    data_concept_id: str | None = None
    structural_fingerprint: StructuralFingerprint | None = None
    model_item_id: str | None = None
    business_model_node_id: str | None = None
    template_version: str | None = None
    model_id: str | None = None
    thesis_value: float | None = None
    thesis_direction: ThesisDirectionComparison
    periods: list[int] = Field(default_factory=list)
    consensus_value: float | None = None
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("thesis_link_id", mode="before")
    @classmethod
    def _normalize_link_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class ThesisScorecardEvidence(_ContractModel):
    status: ScorecardEvidenceStatus
    candidate_keys: list[str] = Field(default_factory=list)
    matched_keys: list[str] = Field(default_factory=list)
    series_by_key: dict[str, dict[int, float]] = Field(default_factory=dict)
    observation_periods: list[int] = Field(default_factory=list)
    latest_observation_period: int | None = None
    latest_observation_value: float | None = None


class ThesisScorecardEntry(_ContractModel):
    thesis_point_id: str
    thesis_text: str
    status: ScorecardEntryStatus
    resolution_anchor: ResolutionAnchor
    resolution_warnings: list[str] = Field(default_factory=list)
    model_reflects_thesis: bool
    latest_actual: float | None = None
    actual_vs_thesis: ActualVsThesisStatus | None = None
    consensus_vs_thesis: ConsensusVsThesisStatus | None = None
    actual_evidence: ThesisScorecardEvidence | None = None
    consensus_evidence: ThesisScorecardEvidence | None = None
    notes: str | None = None


class ComponentScorecardEntry(_ContractModel):
    prediction_target_id: str
    component: OutcomeComponent
    predicted: float | None = Field(default=None, allow_inf_nan=False)
    realized: float | None = Field(default=None, allow_inf_nan=False)
    status: ScorecardEntryStatus
    score: float | None = Field(default=None, allow_inf_nan=False)
    realized_absolute: float | None = Field(default=None, allow_inf_nan=False)
    realized_benchmark_relative: float | None = Field(default=None, allow_inf_nan=False)
    realized_factor_adjusted: float | None = Field(default=None, allow_inf_nan=False)
    methodology_version: str | None = None
    scored_at: str | None = None


class ThesisScorecard(_ContractModel):
    scorecard_id: str | None = None
    thesis_id: str | None = None
    scored_at: str | None = None
    entries: list[ThesisScorecardEntry] = Field(default_factory=list)
    component_entries: list[ComponentScorecardEntry] = Field(default_factory=list)
    summary_status: SummaryStatus
    outcome_summary: SummaryStatus | None = None
    confirmed_count: int = Field(ge=0)
    challenged_count: int = Field(ge=0)
    disconfirmed_count: int = Field(ge=0)
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("scorecard_id", mode="before")
    @classmethod
    def _normalize_scorecard_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("thesis_id", mode="before")
    @classmethod
    def _normalize_thesis_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Thesis(_ContractModel):
    thesis_id: str | None = None
    user_id: str
    ticker: str
    label: str | None = None
    version: int = Field(default=1, ge=1)
    created_at: float | None = None
    updated_at: float | None = None
    markdown_path: str | None = None

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

    decisions_log: list[DecisionsLogEntry] = Field(default_factory=list)
    model_links: list[ThesisLink] = Field(default_factory=list)
    scorecard: ThesisScorecard | None = None
    raw_markdown_extras: list[UnknownSection] = Field(default_factory=list)
    quantitative_framing: QuantitativeFraming | None = None
    position_metadata: PositionMetadata | None = None
    model_ref: ThesisModelRef | None = None
    business_model_ref: BusinessModelRef | None = None
    model_insights: list[ThesisModelInsight] = Field(default_factory=list)
    price_target: ThesisPriceTarget | None = None
    from_idea: ThesisFromIdea | None = None

    schema_version: Literal["1.0"] = "1.0"

    @field_validator("thesis_id", mode="before")
    @classmethod
    def _normalize_thesis_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)

    @field_validator("label", mode="before")
    @classmethod
    def _normalize_label(cls, value: object | None) -> str | None:
        return canonicalize_optional_research_label(value)

    @field_validator("user_id", mode="before")
    @classmethod
    def _normalize_user_id(cls, value: object) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("user_id is required")
        return normalized

    @model_validator(mode="after")
    def _validate_company_identity(self) -> "Thesis":
        if self.company.ticker != self.ticker:
            raise ValueError("company.ticker must match thesis ticker")
        return self


__all__ = [
    "ActualVsThesisStatus",
    "BusinessModelRef",
    "ComponentScorecardEntry",
    "ConsensusVsThesisStatus",
    "DecisionLogArtifactRef",
    "DecisionsLogEntry",
    "DecisionValue",
    "EpsFcfQuantitativeFraming",
    "GaapNonGaapBridge",
    "MarginsQuantitativeFraming",
    "PortfolioFit",
    "PositionMetadata",
    "PositionSize",
    "QuantitativeFraming",
    "ResolutionAnchor",
    "RevenueQuantitativeFraming",
    "RiskDecomposition",
    "ScenarioMultiple",
    "ScenarioFraming",
    "ScenarioProbability",
    "ScorecardEntryStatus",
    "ScorecardEvidenceStatus",
    "StructuralFingerprint",
    "SummaryStatus",
    "Thesis",
    "ThesisFromIdea",
    "ThesisDirectionComparison",
    "ThesisDriverSensitivity",
    "ThesisImpliedAssumption",
    "ThesisLink",
    "ThesisLinkCategory",
    "ThesisModelInsight",
    "ThesisModelRef",
    "ThesisModelVersionRef",
    "ThesisPriceTarget",
    "ThesisPriceTargetDriverSensitivity",
    "ThesisPriceTargetRanges",
    "ThesisRiskSurfaced",
    "ThesisQuantitativeScenario",
    "ThesisScorecard",
    "ThesisScorecardEvidence",
    "ThesisScorecardEntry",
    "UnknownSection",
]
