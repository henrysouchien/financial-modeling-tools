from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from ._insights_shared import _FROZEN_CONTRACT


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


__all__ = [
    "AssumptionFieldTarget",
    "AssumptionTarget",
    "DataGapTarget",
    "HistoricalCoincidenceTarget",
    "StableIdTarget",
    "ThesisFieldTargetInt",
    "ThesisFieldTargetStr",
    "ThesisQuantitativeTarget",
    "ValuationTarget",
    "WatchItemTarget",
]
