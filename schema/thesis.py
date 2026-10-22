from __future__ import annotations

import hashlib
import re
from datetime import datetime
from pathlib import PurePosixPath
from collections.abc import Mapping
from typing import Any, Literal, TypeAlias

from pydantic import Field, field_validator, model_serializer, model_validator

from ._insights_shared import DriverUnit, Severity
from ._outcome_shared import OutcomeComponent
from industry_slice_core.industry_types import DriverCategory
from .model_ref import ModelVersionRef
from .model_state_identity import CurrentModelIdentity, validate_sha256_hex
from .numeric_display import NumericDisplaySpec
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
from .versioned_artifact import canonical_json_bytes


ThesisLinkCategory = Literal["revenue", "margin", "growth", "valuation", "catalyst", "risk"]
ThesisDirectionComparison = Literal["above_consensus", "below_consensus", "specific_value", "directional"]
ResolutionAnchor = Literal[
    "driver_key",
    "business_model_node",
    "required_role",
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

#: A scenario's return on the current price, declared once for every writer and reader of
#: ``ThesisQuantitativeScenario.return_pct``: a decimal fraction, ``0.12`` for 12%
#: (docs/reference/value-semantics-contract.md *Percentages And Basis Points*).
SCENARIO_RETURN_LABEL = "Return"
SCENARIO_RETURN_DISPLAY = NumericDisplaySpec(numeric_kind="percentage", scale="unit")
SCENARIO_TARGET_PRICE_DISPLAY = NumericDisplaySpec(
    numeric_kind="per_share", currency="USD", precision=2,
)
SCENARIO_MULTIPLE_TYPE_LABELS = {
    "dcf": "DCF",
    "forward_pe": "Forward P/E",
    "forward_ev_ebitda": "Forward EV / EBITDA",
    "ev_adjusted_ebitda": "EV/Adj-EBITDA",
    "price_to_book": "Price / Book",
    "nav": "Net Asset Value",
    "rnpv": "Risk-adjusted NPV",
    "sum_of_parts": "Sum of the Parts",
}


def _rank_is_dense_from_one(ranks: list[int], *, field_name: str) -> None:
    if ranks != list(range(1, len(ranks) + 1)):
        raise ValueError(f"{field_name}.rank must be dense [1..{len(ranks)}]; got {ranks}")


class UnknownSection(_ContractModel):
    anchor_after: str | None = None
    content: str


class ScenarioMultiple(_ContractModel):
    claim_id: str | None = Field(default=None, min_length=1)
    source_refs: list[SourceId] = Field(default_factory=list)
    type: str = Field(json_schema_extra={"labels": dict(SCENARIO_MULTIPLE_TYPE_LABELS)})
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
    target_price: float | None = Field(
        default=None,
        json_schema_extra={"display": SCENARIO_TARGET_PRICE_DISPLAY.model_dump(mode="json", exclude_none=True)},
    )
    return_pct: float | None = Field(
        default=None,
        title=SCENARIO_RETURN_LABEL,
        description="Return on the current price as a decimal fraction: 0.12 is 12%.",
        json_schema_extra={"display": SCENARIO_RETURN_DISPLAY.model_dump(mode="json", exclude_none=True)},
    )
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
    claim_id: str | None = Field(default=None, min_length=1)
    source_refs: list[SourceId] = Field(default_factory=list)
    base: ScalarValue | None = None
    bull: ScalarValue | None = None
    bear: ScalarValue | None = None
    rationale: str | None = None


class MarginsQuantitativeFraming(_ContractModel):
    claim_id: str | None = Field(default=None, min_length=1)
    source_refs: list[SourceId] = Field(default_factory=list)
    trajectory: str | None = None
    key_drivers: list[str] = Field(default_factory=list)


class GaapNonGaapBridge(_ContractModel):
    """Reconciliation between GAAP and non-GAAP terminal values."""

    bridge_id: str | None = Field(default=None, min_length=1)
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
    claim_id: str | None = Field(default=None, min_length=1)
    source_refs: list[SourceId] = Field(default_factory=list)
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
    model_ref: ModelVersionRef | None = None
    as_of: str | None = None


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


class PortfolioFitProvenance(_ContractModel):
    tool_version: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    computation_id: str = Field(pattern=r"^fms:quantifying_risk:[A-Z0-9.\-]+:\d{4}-\d{2}:[0-9a-f]{16}$")
    computation_params: dict[str, Any]
    helper_output_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    covered_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    as_of: str = Field(min_length=1)


class PortfolioFit(_ContractModel):
    sector_exposure: str | None = None
    factor_exposure: str | None = None
    correlation_cluster: str | None = None
    dominant_factor_stability: bool | None = None
    idiosyncratic_volatility_annualized_pct: float | None = None
    market_only_decomposition: RiskDecomposition | None = None
    derived_from: PortfolioFitProvenance | None = None


class PositionMetadata(_ContractModel):
    position_size: PositionSize | None = None
    date_initiated: str | None = None
    portfolio_fit: PortfolioFit | None = None


class BusinessModelRef(_ContractModel):
    """Reference to a concrete BusinessModel artifact revision."""

    business_model_id: str = Field(min_length=1)
    schema_version: str = "1.0"
    revision: str = Field(min_length=1)
    last_updated: str = Field(min_length=1)


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
    source_refs: list[SourceId] = Field(default_factory=list)
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
    model_id: str = Field(min_length=1)
    model_version: int = Field(ge=1)
    generated_at: str = Field(min_length=1)
    driver_sensitivities: list[ThesisDriverSensitivity] = Field(default_factory=list)
    implied_assumptions: list[ThesisImpliedAssumption] = Field(default_factory=list)
    risks_surfaced: list[ThesisRiskSurfaced] = Field(default_factory=list)
    schema_version: Literal["2.0"] = "2.0"

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


class DecisionLogActor(_ContractModel):
    kind: str
    id: str | None = None
    role: str | None = None
    channel: str | None = None

    @field_validator("kind", mode="before")
    @classmethod
    def _normalize_kind(cls, value: object | None) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("actor.kind is required")
        return text

    @field_validator("id", "role", "channel", mode="before")
    @classmethod
    def _normalize_optional_fields(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


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
    actor: DecisionLogActor | None = None

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

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        if isinstance(data, dict):
            actor = data.get("actor")
            if actor is None or (isinstance(actor, dict) and actor.get("kind") == "unattributed"):
                data.pop("actor", None)
        return data


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
    required_role: str | None = Field(
        default=None,
        pattern=r"^(statement\.(is|bs|cf)|operating|adjustment)\.[a-z0-9][a-z0-9_]*$",
    )
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


class ThesisScorecardBasis(_ContractModel):
    """Exact thesis/current-model snapshot used by one scorecard run."""

    research_file_id: int = Field(gt=0, strict=True)
    thesis_version: int = Field(gt=0, strict=True)
    thesis_links_sha256: str
    current_model: CurrentModelIdentity

    @field_validator("thesis_links_sha256")
    @classmethod
    def _validate_links_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="thesis_links_sha256")


def thesis_scorecard_links_sha256(links: list[Any]) -> str:
    payloads = [
        link.model_dump(mode="json")
        if hasattr(link, "model_dump")
        else dict(link)
        for link in links
    ]
    payloads.sort(key=lambda payload: str(payload.get("thesis_link_id") or ""))
    return hashlib.sha256(canonical_json_bytes(payloads)).hexdigest()


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
    basis: ThesisScorecardBasis | None = None
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("scorecard_id", mode="before")
    @classmethod
    def _normalize_scorecard_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("thesis_id", mode="before")
    @classmethod
    def _normalize_thesis_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class DifferentiatedViewMerge(_ContractModel):
    """A differentiated-view claim retired into a surviving claim.

    Handles that named ``retired_claim_id`` (thesis links, scorecard entries,
    excerpt ``claim_ids``) resolve to ``into_claim_id``.
    """

    retired_claim_id: str = Field(min_length=1)
    into_claim_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


QuestionContext = Literal["equity", "transaction", "corporate"]
QuestionPeriodicity = Literal["annual", "quarterly"]


class ThesisQuestionProposal(_ContractModel):
    """What the proposer states: the debate the model exists to answer (spec §3 Question).

    The subject is the Thesis's Business (its ticker), and the author, date, id and
    ratification are stamped by the doors, so none of them is proposed.
    """

    claim: str = Field(min_length=1, description="The claim in dispute.")
    context: QuestionContext
    horizon: str = Field(min_length=1, description="The horizon the claim resolves over, e.g. FY2028.")
    periodicity: QuestionPeriodicity


class ThesisQuestion(ThesisQuestionProposal):
    """One recorded Question beside the Thesis (R1, 2026-10-03: agent-proposed, human-ratified).

    ``Thesis.questions`` is append-only: a restated Question is a new record that names
    the one it ``supersedes``; the prior record survives. ``ratified`` is false until the
    human ratify door (``POST /theses/{research_file_id}/questions/{question_id}/ratify``)
    stamps ``ratified_by`` (the trusted principal) and ``ratified_at``.
    """

    question_id: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    author: str = Field(min_length=1)
    proposed_at: datetime
    supersedes: str | None = Field(default=None, min_length=1)
    ratified: bool = False
    ratified_by: str | None = Field(default=None, min_length=1)
    ratified_at: datetime | None = None

    @model_validator(mode="after")
    def _ratification_is_stamped(self) -> "ThesisQuestion":
        stamped = (self.ratified_by is not None, self.ratified_at is not None)
        if stamped != (self.ratified, self.ratified):
            raise ValueError(
                "a ratified Question carries ratified_by and ratified_at; "
                "an unratified Question carries neither"
            )
        return self


class QuestionRef(_ContractModel):
    """Names one recorded Question: the research file holding its Thesis, and its id (R2)."""

    research_file_id: int = Field(gt=0, strict=True)
    question_id: str = Field(min_length=1)


def _validate_question_history(questions: list[ThesisQuestion], *, ticker: str) -> None:
    seen: set[str] = set()
    for question in questions:
        if question.question_id in seen:
            raise ValueError(f"duplicate question_id {question.question_id!r}")
        if question.supersedes is not None and question.supersedes not in seen:
            raise ValueError(
                f"question {question.question_id!r} supersedes {question.supersedes!r}, "
                "which is not an earlier Question"
            )
        if question.subject != ticker:
            raise ValueError("question subject must be the thesis ticker")
        seen.add(question.question_id)


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
    differentiated_view_merges: list[DifferentiatedViewMerge] = Field(default_factory=list)
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
    model_ref: ModelVersionRef | None = None
    business_model_ref: BusinessModelRef | None = None
    model_insights: list[ThesisModelInsight] = Field(default_factory=list)
    from_idea: ThesisFromIdea | None = None
    #: The Question this research file exists to answer, as an append-only history; the
    #: last record is current (``current_question``). Excluded from dumps while empty so a
    #: Thesis written before it keeps its bytes.
    questions: list[ThesisQuestion] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )

    schema_version: Literal["1.0"] = "1.0"

    @model_validator(mode="before")
    @classmethod
    def _stored_price_target(cls, data: Any) -> Any:
        # A Thesis stores no price target; it reads the PriceTarget bound to
        # the ModelVersion (``schema/thesis_model_view.py``). Thesis rows and
        # proposal payloads written before that carry a ``price_target`` key,
        # which decodes to nothing.
        if isinstance(data, Mapping) and "price_target" in data:
            return {key: value for key, value in data.items() if key != "price_target"}
        return data

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
        _validate_question_history(self.questions, ticker=self.ticker)
        return self

    def current_question(self) -> ThesisQuestion | None:
        return self.questions[-1] if self.questions else None

    def question(self, question_id: str) -> ThesisQuestion | None:
        return next(
            (item for item in self.questions if item.question_id == question_id),
            None,
        )

    def current_question_ref(self, research_file_id: int) -> QuestionRef | None:
        """The ref a new decomposition or deliverable of this research file carries (R2)."""

        current = self.current_question()
        if current is None:
            return None
        return QuestionRef(
            research_file_id=int(research_file_id),
            question_id=current.question_id,
        )


__all__ = [
    "ActualVsThesisStatus",
    "BusinessModelRef",
    "ComponentScorecardEntry",
    "ConsensusVsThesisStatus",
    "DecisionLogActor",
    "DecisionLogArtifactRef",
    "DecisionsLogEntry",
    "DecisionValue",
    "DifferentiatedViewMerge",
    "EpsFcfQuantitativeFraming",
    "GaapNonGaapBridge",
    "MarginsQuantitativeFraming",
    "PortfolioFit",
    "PositionMetadata",
    "PositionSize",
    "QuantitativeFraming",
    "QuestionContext",
    "QuestionPeriodicity",
    "QuestionRef",
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
    "ThesisRiskSurfaced",
    "ThesisQuantitativeScenario",
    "ThesisQuestion",
    "ThesisQuestionProposal",
    "ThesisScorecard",
    "ThesisScorecardEvidence",
    "ThesisScorecardEntry",
    "UnknownSection",
]
