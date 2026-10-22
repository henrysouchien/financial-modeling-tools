"""Narrow, fail-closed period metric reads from a reconstructed financial model."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Literal

from .models import FinancialModel, FiscalPeriodMetadata, LineItem
from .periods import build_period_metadata, parse_fiscal_year_end_month


ReaderPeriodGranularity = Literal["quarter", "annual"]
MetricName = Literal["eps", "revenue"]
PeriodMetricDiagnosticCode = Literal[
    "invalid_fiscal_year_end",
    "period_unmappable",
    "period_ambiguous",
    "metric_missing",
    "metric_ambiguous",
    "metric_value_missing",
    "metric_value_invalid",
]


@dataclass(frozen=True)
class PeriodMetricDiagnostic:
    code: PeriodMetricDiagnosticCode
    metric: MetricName | None = None
    detail: str | None = None


@dataclass(frozen=True)
class PeriodMetricsResult:
    period_key: int | None = None
    period_metadata: FiscalPeriodMetadata | None = None
    eps: float | None = None
    revenue: float | None = None
    diagnostics: tuple[PeriodMetricDiagnostic, ...] = ()

    @property
    def period_matched(self) -> bool:
        return self.period_key is not None


_CANONICAL_IDS: dict[MetricName, str] = {
    "eps": "tpl.fm.adjusted_earnings.adjusted_eps",
    "revenue": "tpl.fm.income_statement.total_revenue",
}
_EPS_PATTERN = re.compile(r"(?:\beps\b|earnings\s+per\s+share)", re.IGNORECASE)
_REVENUE_PATTERN = re.compile(r"\b(?:total\s+)?(?:revenue|revenues|sales)\b", re.IGNORECASE)
_SHARE_COUNT_PATTERN = re.compile(
    r"(?:weighted\s+average.*shares?|shares?\s+outstanding|share\s+count)",
    re.IGNORECASE,
)


def _items(model: FinancialModel) -> list[LineItem]:
    return [
        item
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    ]


def _value_at(item: LineItem, period_key: int) -> float | None:
    if item.values is None:
        return None
    cell = item.values.values.get(period_key)
    if cell is None or cell.value is None or isinstance(cell.value, bool):
        return None
    try:
        value = float(cell.value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _fallback_candidates(
    model: FinancialModel,
    metric: MetricName,
    period_key: int,
) -> list[LineItem]:
    pattern = _EPS_PATTERN if metric == "eps" else _REVENUE_PATTERN
    candidates: list[LineItem] = []
    for item in _items(model):
        item_id = str(item.id or "")
        text = f"{item_id.replace('_', ' ')} {item.label or ''}"
        if item_id.lower().startswith("tpl.s."):
            continue
        if _SHARE_COUNT_PATTERN.search(text):
            continue
        if not pattern.search(text):
            continue
        if _value_at(item, period_key) is not None:
            candidates.append(item)
    return candidates


def _read_metric(
    model: FinancialModel,
    metric: MetricName,
    period_key: int,
) -> tuple[float | None, PeriodMetricDiagnostic | None]:
    try:
        canonical = model.get_item(_CANONICAL_IDS[metric])
    except (KeyError, ValueError):
        canonical = None
    if canonical is not None:
        value = _value_at(canonical, period_key)
        if value is not None:
            return value, None

    candidates = _fallback_candidates(model, metric, period_key)
    if not candidates:
        return None, PeriodMetricDiagnostic(
            "metric_value_missing" if canonical is not None else "metric_missing",
            metric,
            canonical.id if canonical is not None else None,
        )
    if len(candidates) > 1:
        return None, PeriodMetricDiagnostic(
            "metric_ambiguous",
            metric,
            ",".join(sorted(item.id for item in candidates)),
        )
    value = _value_at(candidates[0], period_key)
    if value is None:
        return None, PeriodMetricDiagnostic("metric_value_invalid", metric, candidates[0].id)
    return value, None


def read_period_metrics(
    model: FinancialModel,
    *,
    calendar_end: date,
    period_type: ReaderPeriodGranularity,
    expected_currency: str | None,
    fiscal_year_end: str,
) -> PeriodMetricsResult:
    """Read EPS and revenue for a calendar-aligned fiscal model period.

    ``expected_currency`` is deliberately accepted as part of the narrow read
    contract but is not inferred from ``FinancialModel``: reconstructed models
    carry no authoritative currency.  The coverage caller owns that gate.
    """

    del expected_currency
    normalized_fye = str(fiscal_year_end or "").strip()
    try:
        parse_fiscal_year_end_month(normalized_fye)
    except ValueError as exc:
        return PeriodMetricsResult(
            diagnostics=(PeriodMetricDiagnostic("invalid_fiscal_year_end", detail=str(exc)),)
        )

    enriched_time = model.time_structure.model_copy(
        deep=True,
        update={"fiscal_year_end": normalized_fye, "period_metadata": {}},
    )
    enriched_time.period_metadata = build_period_metadata(
        enriched_time,
        reported_period_ends=None,
    )
    typed = [
        metadata
        for metadata in enriched_time.period_metadata.values()
        if metadata.period_type == period_type and metadata.calendar_end is not None
    ]
    exact = [metadata for metadata in typed if metadata.calendar_end == calendar_end]
    if exact:
        matches = exact
    else:
        matches = [
            metadata
            for metadata in typed
            if abs((metadata.calendar_end - calendar_end).days) <= 6  # type: ignore[operator]
        ]
    if not matches:
        return PeriodMetricsResult(
            diagnostics=(
                PeriodMetricDiagnostic(
                    "period_unmappable",
                    detail=f"{period_type}:{calendar_end.isoformat()}",
                ),
            )
        )
    if len(matches) > 1:
        return PeriodMetricsResult(
            diagnostics=(
                PeriodMetricDiagnostic(
                    "period_ambiguous",
                    detail=",".join(str(item.period_key) for item in matches),
                ),
            )
        )

    metadata = matches[0]
    eps, eps_diagnostic = _read_metric(model, "eps", metadata.period_key)
    revenue, revenue_diagnostic = _read_metric(model, "revenue", metadata.period_key)
    diagnostics = tuple(
        diagnostic
        for diagnostic in (eps_diagnostic, revenue_diagnostic)
        if diagnostic is not None
    )
    return PeriodMetricsResult(
        period_key=metadata.period_key,
        period_metadata=metadata,
        eps=eps,
        revenue=revenue,
        diagnostics=diagnostics,
    )


__all__ = [
    "PeriodMetricDiagnostic",
    "PeriodMetricDiagnosticCode",
    "PeriodMetricsResult",
    "ReaderPeriodGranularity",
    "read_period_metrics",
]
