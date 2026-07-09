from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, model_validator

from .handoff_patch import (
    AddInvalidationTriggerOp,
    AddRiskOp,
    UpdateInvalidationTriggerOp,
    UpdatePortfolioFitOp,
    UpdateRiskOp,
)


MetricValue: TypeAlias = str | float

RiskReviewPatchOp: TypeAlias = Annotated[
    Union[
        AddRiskOp,
        UpdateRiskOp,
        AddInvalidationTriggerOp,
        UpdateInvalidationTriggerOp,
        UpdatePortfolioFitOp,
    ],
    Field(discriminator="op"),
]

_RECOMMENDATION_TYPES_BY_WORKFLOW: dict[str, frozenset[str]] = {
    "risk_review": frozenset(
        {
            "reduce_concentration",
            "factor_hedge",
            "tax_loss_harvest",
            "leverage_reduction",
            "thesis_patch_proposal",
            "monitor",
            "run_quantifying_risk",
        }
    ),
    "allocation_review": frozenset(
        {
            "rebalance",
            "fill_empty_sleeve",
            "reduce_overweight",
            "increase_underweight",
            "skip",
            "target_setup_required",
        }
    ),
    "scenario_analysis": frozenset(
        {
            "rebalance",
            "stress_defense",
            "scenario_selected",
            "scenario_declined",
        }
    ),
    "hedging": frozenset(
        {
            "hedge",
            "no_hedge_needed",
            "hedge_review_required",
        }
    ),
    "macro_review": frozenset(
        {
            "monitor",
            "risk_review",
            "sector_review",
            "rebalance_review",
            "no_action",
        }
    ),
    "strategy_executor": frozenset(
        {
            "trade_recommendation",
            "no_action",
            "account_setup_required",
            "signal_refresh",
            "reconciliation_blocked",
            "monitor",
        }
    ),
}


class _RecommendationsModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class PreviewImpact(_RecommendationsModel):
    vol_delta: MetricValue | None = None
    conc_delta: MetricValue | None = None
    factor_var_delta: MetricValue | None = None
    violations_resolved: int | None = None
    tax_impact: str | None = None
    beta_delta: MetricValue | None = None
    cash_impact: MetricValue | None = None


class OptionDetails(_RecommendationsModel):
    strike: float
    expiration: str = Field(alias="expiry")
    option_type: Literal["call", "put"] = Field(alias="type")
    premium: float | None = None
    multiplier: float | None = None


class Recommendation(_RecommendationsModel):
    action: str
    recommendation_text: str | None = None
    description: str | None = None
    priority: Literal["high", "medium", "low"]
    source_tool: str
    workflow_name: str
    recommendation_type: str
    ticker: str | None = None
    side: Literal["BUY", "SELL", "HOLD", "SHORT", "COVER"] | None = None
    quantity: int | None = None
    account_id: str | None = None
    target_weight: float | None = None
    delta_change: float | None = None
    instrument_type: Literal["equity", "etf", "option", "future"] | None = None
    option_details: OptionDetails | None = None
    group: str | None = None
    group_order: int | None = None
    source_flag: str | None = None
    flag_severity: Literal["error", "warning", "info"] | None = None
    preview_impact: PreviewImpact | None = None

    @model_validator(mode="before")
    @classmethod
    def _backfill_recommendation_text_from_description(cls, data: Any) -> Any:
        if isinstance(data, dict) and not data.get("recommendation_text") and data.get(
            "description"
        ):
            data = dict(data)
            data["recommendation_text"] = data["description"]
        return data

    @model_validator(mode="after")
    def _require_at_least_one_text_field(self) -> "Recommendation":
        if not self.recommendation_text and not self.description:
            raise ValueError(
                "Recommendation requires at least one of "
                "{recommendation_text, description}"
            )
        return self

    @model_validator(mode="after")
    def _validate_recommendation_type_for_workflow(self) -> "Recommendation":
        workflow_key = self.workflow_name.replace("-", "_")
        allowed = _RECOMMENDATION_TYPES_BY_WORKFLOW.get(workflow_key)
        if allowed is not None and self.recommendation_type not in allowed:
            raise ValueError(
                f"recommendation_type {self.recommendation_type!r} is not valid "
                f"for workflow_name {self.workflow_name!r}"
            )
        return self


class CombinedImpact(_RecommendationsModel):
    vol_before: MetricValue | None = None
    vol_after: MetricValue | None = None
    compliance_before: str | None = None
    compliance_after: str | None = None
    factor_exposures_delta: dict[str, MetricValue] | None = None
    violations_before: int | None = None
    violations_after: int | None = None
    violations_resolved: int | None = None
    total_cash_impact: MetricValue | None = None
    beta_delta: MetricValue | None = None
    hhi_delta: MetricValue | None = None


class RecommendationsBundle(_RecommendationsModel):
    recommendations: list[Recommendation] = Field(default_factory=list)
    combined_impact: CombinedImpact | None = None
    summary: str
    # Accept bare strings (analyst.py:508 contract) OR richer {action, description, ...} objects
    # — LLM emits the richer form for hedging deferreds with executor metadata (validated live 2026-05-29)
    deferred_actions: list[str | dict[str, Any]] = Field(default_factory=list)


class ViolationDetail(_RecommendationsModel):
    id: str
    severity: Literal["error", "warning", "info"]
    category: str
    description: str
    current_value: float
    # nullable: no configured limit — e.g. info-severity observations or
    # limits_not_configured portfolios (bg_606: required-float forced agents to fabricate limits)
    limit: float | None = None
    driver_positions: list[str] = Field(default_factory=list)


class MethodologyLensRow(_RecommendationsModel):
    issue: str
    identifying_risk_pillar: Literal[
        "systematic",
        "idiosyncratic",
        "sub_industry",
        "invalidation_trigger",
    ]
    quantifying_risk_evidence: str
    methodology_verdict: str
    action_implication: str


class PriorReviewContext(_RecommendationsModel):
    last_review_date: str | None = None
    unresolved_issue_ids: list[str] = Field(default_factory=list)
    prior_recommendations_status: list[dict[str, Any]] = Field(default_factory=list)


class AssetClassDrift(_RecommendationsModel):
    category: str
    current_pct: float
    target_pct: float
    drift_pct: float
    status: Literal["on_target", "overweight", "underweight"]
    severity: Literal["none", "minor", "material"]


class DriftDriver(_RecommendationsModel):
    asset_class: str
    contributor_ticker: str
    contribution_pct: float
    driver_type: Literal["price_move", "new_position", "withdrawal"]


class EmptySleeveDecision(_RecommendationsModel):
    asset_class: str
    current_pct: float
    target_pct: float
    decision: Literal["fill", "reduce_target", "skip", "deferred"]
    rationale: str | None = None


class AccountContext(_RecommendationsModel):
    account_id: str
    account_type: str | None = None
    market_value: float | None = None


class AllocationPreviewMetrics(_RecommendationsModel):
    hhi_before: float | None = None
    hhi_after: float | None = None
    beta_before: float | None = None
    beta_after: float | None = None
    compliance_before: str | None = None
    compliance_after: str | None = None


class TradeLeg(_RecommendationsModel):
    ticker: str
    # SHORT (open short position) + COVER (close short) needed for hedging legs;
    # BUY/SELL cover long-position rebalance
    side: Literal["BUY", "SELL", "SHORT", "COVER"]
    # Nullable in recommend mode when account routing / live preview has not
    # converted target dollars or weight deltas into exact share quantities yet.
    shares: float | None = None
    estimated_value: float
    weight_change: float
    preview_id: str | None = None
    account_id: str | None = None


class TradeTotals(_RecommendationsModel):
    total_sell_value: float
    total_buy_value: float
    net_cash_impact: float


class ScenarioMetricsAbsolute(_RecommendationsModel):
    vol: float | None = None
    total_violations: int | None = None
    hhi: float | None = None
    beta: float | None = None


class ScenarioMetricsDelta(_RecommendationsModel):
    vol_delta: float | None = None
    conc_delta: float | None = None
    total_violations: int | None = None
    factor_var_delta: float | None = None


class Scenario(_RecommendationsModel):
    name: str
    target_weights: dict[str, float] | None = None
    delta_changes: dict[str, str] | None = None
    metrics: ScenarioMetricsDelta
    tradeoffs: str | None = None
    assumptions: list[str] = Field(default_factory=list)


class InitialResult(_RecommendationsModel):
    # All fields nullable: LLM may emit an InitialResult shell with null fields when
    # scenario data is unavailable (e.g., SCENARIO_INSUFFICIENT_DATA verdict)
    scenario_name: str | None = None
    baseline_metrics: ScenarioMetricsAbsolute | None = None
    scenario_metrics: ScenarioMetricsDelta | None = None
    target_weights: dict[str, float] | None = None
    delta_changes: dict[str, str] | None = None
    assumptions: list[str] = Field(default_factory=list)


class RefinedScenarioPayload(_RecommendationsModel):
    # All fields nullable: LLM may emit a shell when scenario data is unavailable
    scenario_name: str | None = None
    target_weights: dict[str, float] | None = None
    delta_changes: dict[str, str] | None = None
    rationale: str | None = None
    expected_metrics_after: ScenarioMetricsAbsolute | None = None


class HedgeObjective(_RecommendationsModel):
    # description nullable: may be unknown when verdict is INSUFFICIENT_PORTFOLIO_CONTEXT
    description: str | None = None
    target_metrics: dict[str, MetricValue] = Field(default_factory=dict)


class ExposureItem(_RecommendationsModel):
    issue: str
    severity: Literal["error", "warning", "info"]
    driver_positions: list[str] = Field(default_factory=list)
    factor_or_concentration_evidence: str


class ETFHedgeLeg(TradeLeg):
    pass


class OptionLeg(_RecommendationsModel):
    symbol: str
    position: Literal["long", "short"]
    option_type: Literal["call", "put", "stock"]
    strike: float | None = None
    expiration: str | None = None
    premium: float | None = None
    size: int = 1
    multiplier: float | None = None
    label: str | None = None
    con_id: str | None = None
    account_id: str | None = None
    preview_id: str | None = None

    @model_validator(mode="after")
    def _validate_strike_expiry_for_options(self) -> "OptionLeg":
        if self.option_type in {"call", "put"} and (
            self.strike is None or self.expiration is None
        ):
            raise ValueError(
                f"OptionLeg with option_type={self.option_type!r} requires "
                "strike + expiration"
            )
        return self


class FutureLeg(_RecommendationsModel):
    symbol: str
    position: Literal["long", "short"]
    contract_month: str
    size: int
    account_id: str | None = None
    preview_id: str | None = None


class HedgeCandidate(_RecommendationsModel):
    instrument_type: Literal["etf", "option", "future"]
    structure: str
    expected_effect: dict[str, MetricValue] = Field(default_factory=dict)
    estimated_cost: MetricValue | None = None
    etf_legs: list[ETFHedgeLeg] | None = None
    option_legs: list[OptionLeg] | None = None
    future_legs: list[FutureLeg] | None = None


class DecisionLogEntry(_RecommendationsModel):
    decision_type: str
    timestamp: str
    rationale: str
    inputs_used: list[str] = Field(default_factory=list)
    outcome: str | None = None


class RiskReviewContext(RecommendationsBundle):
    risk_score: int | None = None
    risk_category: str | None = None
    compliance_status: str | None = None  # nullable: may be unknown when verdict is RISK_REVIEW_INSUFFICIENT_DATA
    violations: list[ViolationDetail] = Field(default_factory=list)
    methodology_lens: list[MethodologyLensRow] = Field(default_factory=list)
    methodology_applied: list[str] = Field(default_factory=list)
    # Proposal dictionaries intentionally accept low-friction LLM shapes such as
    # {op, ticker, risk|trigger|portfolio_fit, source_refs}. Parent/apply layers
    # normalize or reject them at the workflow boundary; RiskReviewPatchOp stays
    # exported for strict downstream validation where formal patch ops are needed.
    patch_ops_proposed: list[dict[str, Any]] = Field(default_factory=list)
    prior_review_context: PriorReviewContext | None = None


class AllocationReviewContext(RecommendationsBundle):
    allocation_snapshot: list[AssetClassDrift] = Field(default_factory=list)
    drift_drivers: list[DriftDriver] = Field(default_factory=list)
    empty_sleeve_decisions: list[EmptySleeveDecision] = Field(default_factory=list)
    targets_set: bool | None = None  # nullable: may be unknown when verdict is ALLOCATION_INSUFFICIENT_DATA
    stale_pending_actions: list[str] = Field(default_factory=list)
    compliance_flags: list[ViolationDetail] = Field(default_factory=list)
    concentration_flags: list[ViolationDetail] = Field(default_factory=list)
    account_context: AccountContext | None = None
    expected_legs: list[TradeLeg] = Field(default_factory=list)
    trade_totals: TradeTotals | None = None
    preview_metrics: AllocationPreviewMetrics | None = None
    post_trade_checklist: list[str] = Field(default_factory=list)
    decision_log_entries: list[DecisionLogEntry] = Field(default_factory=list)


class ScenarioAnalysisContext(RecommendationsBundle):
    # Nullable: may be unknown when verdict is SCENARIO_INSUFFICIENT_DATA
    mode: Literal["custom", "template", "stress"] | None = None
    baseline_metrics: ScenarioMetricsAbsolute | None = None
    initial_result: InitialResult | None = None
    scenarios: list[Scenario] = Field(default_factory=list)
    ranked_alternatives: list[Scenario] = Field(default_factory=list)
    rank_by: Literal["vol_delta", "conc_delta", "total_violations", "factor_var_delta"] | None = None
    rank_order: Literal["asc", "desc"] | None = None
    selected_scenario_name: str | None = None
    refined_payload: RefinedScenarioPayload | None = None
    executable_legs: list[TradeLeg] = Field(default_factory=list)
    post_trade_checks: list[str] = Field(default_factory=list)
    decision_log_entries: list[DecisionLogEntry] = Field(default_factory=list)


class HedgingContext(RecommendationsBundle):
    # Nullable: may be unknown when verdict is INSUFFICIENT_PORTFOLIO_CONTEXT
    hedge_objective: HedgeObjective | None = None
    exposure_diagnosis: list[ExposureItem] = Field(default_factory=list)
    hedge_candidates: list[HedgeCandidate] = Field(default_factory=list)
    scenarios: list[Scenario] = Field(default_factory=list)
    selected_hedge: HedgeCandidate | None = None
    post_trade_checks: list[str] = Field(default_factory=list)
    decision_log_entries: list[DecisionLogEntry] = Field(default_factory=list)


class MacroIndexSnapshot(_RecommendationsModel):
    name: str
    value: MetricValue | None = None
    change_pct: float | None = None
    as_of: str | None = None
    note: str | None = None


class MacroRateSnapshot(_RecommendationsModel):
    curve_shape: Literal["normal", "flat", "inverted", "unknown"]
    fed_funds: MetricValue | None = None
    two_year: MetricValue | None = None
    ten_year: MetricValue | None = None
    thirty_year: MetricValue | None = None
    credit_spread_baa_10y: MetricValue | None = None
    moves_30d: dict[str, MetricValue] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class MacroVolatilitySnapshot(_RecommendationsModel):
    vix_level: float | None = None
    regime: Literal["low", "moderate", "elevated", "fear", "unknown"] = "unknown"
    term_structure: str | None = None
    spy_iv_percentile: MetricValue | None = None
    skew: MetricValue | None = None
    resolved_trade_date: str | None = None
    warnings: list[str] = Field(default_factory=list)


class MacroEconomicIndicator(_RecommendationsModel):
    name: str
    value: MetricValue | None = None
    observation_date: str | None = None
    trend: str | None = None
    note: str | None = None


class MacroSectorSnapshot(_RecommendationsModel):
    leaders: list[str] = Field(default_factory=list)
    laggards: list[str] = Field(default_factory=list)
    leadership_note: str | None = None
    rotation_signal: str | None = None


class MacroPortfolioImplication(_RecommendationsModel):
    category: str
    description: str
    affected_positions: list[str] = Field(default_factory=list)
    severity: Literal["high", "medium", "low", "info"] = "info"


class MacroReviewContext(RecommendationsBundle):
    as_of: str | None = None
    regime: Literal["risk-on", "risk-off", "transitional", "stable", "unknown"]
    vix_level: float | None = None
    curve_shape: Literal["normal", "flat", "inverted", "unknown"]
    market_snapshot: list[MacroIndexSnapshot] = Field(default_factory=list)
    rate_environment: MacroRateSnapshot | None = None
    volatility_positioning: MacroVolatilitySnapshot | None = None
    economic_indicators: list[MacroEconomicIndicator] = Field(default_factory=list)
    sector_leadership: MacroSectorSnapshot | None = None
    portfolio_implications: list[MacroPortfolioImplication] = Field(default_factory=list)
    watchlist: list[str] = Field(default_factory=list)
    data_gaps: list[str] = Field(default_factory=list)
    source_tools: list[str] = Field(default_factory=list)
    decision_log_entries: list[DecisionLogEntry] = Field(default_factory=list)


class StrategySignalSummary(_RecommendationsModel):
    strategy: Literal["SCE", "RMESH", "unknown"]
    profile: str | None = None
    action: str | None = None
    regime: str | None = None
    regime_changed: bool | None = None
    vix: MetricValue | None = None
    vix_z: MetricValue | None = None
    target_equity_alloc: MetricValue | None = None
    short_target: int | None = None
    long_target: int | None = None
    put_10d_target: int | None = None
    put_20d_target: int | None = None
    roll_due: bool | None = None
    roll_tomorrow: bool | None = None
    dte_targets: list[dict[str, Any]] = Field(default_factory=list)
    source_path: str | None = None
    source_mtime: str | None = None
    stale: bool | None = None
    warnings: list[str] = Field(default_factory=list)


class StrategyAccountStatus(_RecommendationsModel):
    strategy: Literal["SCE", "RMESH", "unknown"]
    account_id: str | None = None
    confirmed: bool | None = None
    account_required: bool = False
    informational_only: bool = False
    nav: MetricValue | None = None
    note: str | None = None


class StrategyPositionReconciliation(_RecommendationsModel):
    instrument: str
    current: MetricValue | None = None
    target: MetricValue | None = None
    delta: MetricValue | None = None
    status: Literal[
        "aligned",
        "missing_leg",
        "excess_leg",
        "roll_required",
        "account_required",
        "blocked",
        "info",
    ]
    detail: str | None = None


class StrategyTradeInstruction(_RecommendationsModel):
    strategy: Literal["SCE", "RMESH", "unknown"]
    action: str
    instrument_type: Literal["equity", "option"]
    underlying_symbol: str = "SPY"
    side: Literal["BUY", "SELL", "HOLD", "SHORT", "COVER"] | None = None
    quantity: int | None = None
    account_id: str | None = None
    account_required: bool = False
    order_type: str | None = None
    limit_price: float | None = None
    option_legs: list[OptionLeg] = Field(default_factory=list)
    preview_tool: Literal["preview_trade", "preview_option_trade"] | None = None
    preview_payload: dict[str, Any] = Field(default_factory=dict)
    requires_user_approval: bool = True
    note: str | None = None


def _strategy_trade_instruction_has_preview_payload(
    instruction: StrategyTradeInstruction,
) -> bool:
    return bool(instruction.preview_payload)


def _strategy_trade_instruction_is_executable(
    instruction: StrategyTradeInstruction,
) -> bool:
    if instruction.account_required:
        return False
    if not instruction.account_id:
        return False
    if instruction.requires_user_approval is not True:
        return False
    if instruction.quantity is None or instruction.quantity <= 0:
        return False
    if instruction.preview_tool is None or not instruction.preview_payload:
        return False
    if instruction.instrument_type == "equity":
        return bool(
            instruction.side in {"BUY", "SELL", "SHORT", "COVER"}
            and instruction.preview_payload.get("account_id")
            and instruction.preview_payload.get("quantity")
            and instruction.preview_payload.get("side")
        )
    if instruction.instrument_type == "option":
        return bool(
            instruction.option_legs
            and instruction.preview_payload.get("account_id")
            and (
                instruction.preview_payload.get("legs")
                or instruction.preview_payload.get("option_legs")
            )
        )
    return False


class StrategyExecutorContext(RecommendationsBundle):
    as_of: str | None = None
    strategy: Literal["SCE", "RMESH", "unknown"]
    profile: str | None = None
    regime: str | None = None
    signal_status: Literal["current", "stale", "missing", "inconsistent", "unknown"]
    signal_summary: StrategySignalSummary | None = None
    account_status: StrategyAccountStatus | None = None
    position_reconciliation: list[StrategyPositionReconciliation] = Field(default_factory=list)
    trade_instructions: list[StrategyTradeInstruction] = Field(default_factory=list)
    preview_parameters_deferred: bool = False
    warnings: list[str] = Field(default_factory=list)
    data_gaps: list[str] = Field(default_factory=list)
    source_tools: list[str] = Field(default_factory=list)
    decision_log_entries: list[DecisionLogEntry] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_verdict_trade_invariants(
        self,
        info: ValidationInfo,
    ) -> "StrategyExecutorContext":
        context = info.context if isinstance(info.context, dict) else {}
        verdict = str(context.get("verdict") or "").strip().upper()
        if not verdict:
            return self

        has_executable_payload = any(
            _strategy_trade_instruction_has_preview_payload(item)
            for item in self.trade_instructions
        )
        if verdict != "STRATEGY_TRADE_RECOMMENDED":
            if has_executable_payload:
                raise ValueError(
                    f"{verdict} must not include executable preview_payload values"
                )
            return self

        account_required = bool(
            self.preview_parameters_deferred
            or (
                self.account_status is not None
                and self.account_status.account_required
            )
        )
        if account_required:
            raise ValueError(
                "STRATEGY_TRADE_RECOMMENDED requires confirmed account context "
                "and non-deferred preview parameters"
            )
        if self.signal_status != "current":
            raise ValueError(
                "STRATEGY_TRADE_RECOMMENDED requires signal_status='current'"
            )
        if not self.trade_instructions:
            raise ValueError(
                "STRATEGY_TRADE_RECOMMENDED requires at least one trade instruction"
            )

        bad_indexes = [
            str(index)
            for index, instruction in enumerate(self.trade_instructions)
            if not _strategy_trade_instruction_is_executable(instruction)
        ]
        if bad_indexes:
            raise ValueError(
                "STRATEGY_TRADE_RECOMMENDED requires every trade instruction to "
                "include preview_tool, executable preview_payload, account_id, "
                "positive quantity, and requires_user_approval=true; invalid "
                f"trade_instructions indexes: {', '.join(bad_indexes)}"
            )
        return self


__all__ = [
    "AccountContext",
    "AllocationPreviewMetrics",
    "AllocationReviewContext",
    "AssetClassDrift",
    "CombinedImpact",
    "DecisionLogEntry",
    "DriftDriver",
    "ETFHedgeLeg",
    "EmptySleeveDecision",
    "ExposureItem",
    "FutureLeg",
    "HedgeCandidate",
    "HedgeObjective",
    "HedgingContext",
    "InitialResult",
    "MacroEconomicIndicator",
    "MacroIndexSnapshot",
    "MacroPortfolioImplication",
    "MacroRateSnapshot",
    "MacroReviewContext",
    "MacroSectorSnapshot",
    "MacroVolatilitySnapshot",
    "MethodologyLensRow",
    "OptionDetails",
    "OptionLeg",
    "PreviewImpact",
    "PriorReviewContext",
    "Recommendation",
    "RecommendationsBundle",
    "RefinedScenarioPayload",
    "RiskReviewContext",
    "RiskReviewPatchOp",
    "Scenario",
    "ScenarioAnalysisContext",
    "ScenarioMetricsAbsolute",
    "ScenarioMetricsDelta",
    "StrategyAccountStatus",
    "StrategyExecutorContext",
    "StrategyPositionReconciliation",
    "StrategySignalSummary",
    "StrategyTradeInstruction",
    "TradeLeg",
    "TradeTotals",
    "ViolationDetail",
]
