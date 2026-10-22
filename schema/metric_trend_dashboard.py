"""The stored metric-trend dashboard: one metric's source-cited series across tickers.

``fms_report_metric_trend_dashboard`` constructs :class:`MetricTrendDashboard`
from the admitted judgment and the sources it registered; this model validates
what the door built.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .thesis_shared_slice import _ContractModel


MetricTrendVerdict = Literal["TREND_ANALYZED", "TREND_PARTIAL", "INSUFFICIENT_DATA"]
MetricTrendPeriodicity = Literal["annual", "quarterly"]
MetricTrendConfidence = Literal["HIGH", "MEDIUM", "LOW"]


class MetricTrendSeriesRow(_ContractModel):
    ticker: str = Field(min_length=1)
    period: str = Field(min_length=1)
    value: float = Field(allow_inf_nan=False)
    unit: str | None = None
    citations: list[str] = Field(min_length=1)


class MetricTrendDashboard(_ContractModel):
    metric_label: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    periodicity: MetricTrendPeriodicity
    tickers: list[str]
    series: list[MetricTrendSeriesRow]
    period_notes: list[Any]
    trend_read: str = Field(min_length=1)
    data_gaps: list[Any]
    verdict: MetricTrendVerdict
    confidence: MetricTrendConfidence | None
    sources: list[dict[str, Any]]


__all__ = [
    "MetricTrendConfidence",
    "MetricTrendDashboard",
    "MetricTrendPeriodicity",
    "MetricTrendSeriesRow",
    "MetricTrendVerdict",
]
