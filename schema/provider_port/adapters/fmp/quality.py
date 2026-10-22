"""FMP-specific quality flags keyed by filing-line identity."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json

from schema.provider_port.contracts import FilingLineRef, QualityFlag
from schema.vintage_selection import evidence_series_identity

@dataclass(frozen=True)
class _QualitySeriesProbe:
    evidence_id: str
    line: FilingLineRef
    period_id: str
    source_id: str
    available_at: datetime
    restates_evidence_id: str | None = None


def _series_key(line: FilingLineRef) -> tuple[str, ...]:
    probe = _QualitySeriesProbe(
        evidence_id="quality-series-probe",
        line=line,
        period_id="quality-series",
        source_id=line.accession,
        available_at=datetime.min.replace(tzinfo=timezone.utc),
    )
    return evidence_series_identity(probe).series


QUALITY_BUCKET_FIELDS = frozenset(
    {
        ("balance_sheet", "otherCurrentAssets"),
        ("balance_sheet", "otherNonCurrentAssets"),
        ("balance_sheet", "otherCurrentLiabilities"),
        ("balance_sheet", "otherNonCurrentLiabilities"),
        ("cash_flow", "otherWorkingCapital"),
        ("cash_flow", "otherNonCashItems"),
        ("cash_flow", "otherInvestingActivities"),
        ("cash_flow", "otherFinancingActivities"),
    }
)
_INFORMATIONAL_BUCKET_TAGS = frozenset(
    {
        "us-gaap:IncreaseDecreaseInOtherOperatingCapitalNet",
        "us-gaap:OtherNoncashIncomeExpense",
    }
)
_YOY_RATIO_THRESHOLD = 3.0
_ABS_DELTA_M = 100.0


def quality_warnings(
    observations: dict[
        tuple[FilingLineRef, str, str],
        dict[int, float],
    ],
) -> tuple[QualityFlag, ...]:
    grouped: dict[
        tuple[tuple[str, ...], str, str],
        dict[int, tuple[FilingLineRef, float]],
    ] = {}
    for (line, endpoint, native_field), values_by_year in observations.items():
        series_key = _series_key(line)
        bucket = grouped.setdefault((series_key, endpoint, native_field), {})
        for year, value in values_by_year.items():
            bucket[int(year)] = (line, value)

    warnings: list[QualityFlag] = []
    for (_grouped_series_key, endpoint, native_field), values_by_year in sorted(
        grouped.items()
    ):
        years = sorted(values_by_year)
        for prior_year, year in zip(years, years[1:]):
            if year != prior_year + 1:
                continue
            prior_line, prior_value = values_by_year[prior_year]
            line, value = values_by_year[year]
            delta = value - prior_value
            if abs(delta) < _ABS_DELTA_M:
                continue
            prior_abs = abs(prior_value)
            ratio = None if prior_abs < 1e-9 else abs(value) / prior_abs
            extreme_change = (
                abs(value) >= _ABS_DELTA_M
                if ratio is None
                else ratio >= _YOY_RATIO_THRESHOLD
                or ratio <= 1.0 / _YOY_RATIO_THRESHOLD
            )
            if not extreme_change:
                continue
            informational = (
                endpoint == "cash_flow"
                and line.tag in _INFORMATIONAL_BUCKET_TAGS
            )
            detail = {
                "kind": "fmp_bucket_yoy_jump",
                "endpoint": endpoint,
                "field": native_field,
                "tag": line.tag,
                "statement_role": line.statement_role,
                "prior_accession": prior_line.accession,
                "accession": line.accession,
                "prior_year": prior_year,
                "year": year,
                "prior_value": prior_value,
                "value": value,
                "delta": delta,
                "ratio": ratio,
                "ratio_threshold": _YOY_RATIO_THRESHOLD,
                "abs_delta_threshold": _ABS_DELTA_M,
            }
            if informational:
                detail["classification"] = "broad_cash_flow_bucket_reclassification"
            warnings.append(
                QualityFlag(
                    code="fmp_bucket_yoy_jump",
                    severity="info" if informational else "warning",
                    line=line,
                    metric_id=None,
                    fiscal_year=year,
                    detail=json.dumps(detail, sort_keys=True, separators=(",", ":")),
                )
            )
    return tuple(warnings)


__all__ = ["QUALITY_BUCKET_FIELDS", "quality_warnings"]
