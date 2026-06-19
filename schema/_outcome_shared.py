from __future__ import annotations

from typing import Literal


OutcomeComponent = Literal[
    "return",
    "fundamentals",
    "valuation",
    "sentiment",
    "market_risk",
    "thesis_risk",
]

ReturnStatedKind = Literal["absolute", "benchmark_relative", "factor_adjusted"]
ValuationMultipleKind = Literal["pe", "ev_ebitda", "p_b", "p_fcf", "p_s"]
ValuationBasisKind = Literal["forward", "trailing", "ntm"]
EarningsBasis = Literal["gaap", "non_gaap"]
RestatementPolicy = Literal["as_first_reported", "as_restated"]
OneTimeHandling = Literal["include", "exclude", "both"]
SentimentMeasure = Literal["level", "change", "direction"]
MarketRiskMetric = Literal["volatility", "downside_deviation", "max_drawdown", "beta"]
MarketRiskFrequency = Literal["daily", "weekly", "monthly"]


__all__ = [
    "EarningsBasis",
    "MarketRiskFrequency",
    "MarketRiskMetric",
    "OneTimeHandling",
    "OutcomeComponent",
    "RestatementPolicy",
    "ReturnStatedKind",
    "SentimentMeasure",
    "ValuationBasisKind",
    "ValuationMultipleKind",
]
