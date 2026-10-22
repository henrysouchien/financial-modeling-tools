"""FMP compatibility projections for scenario-multiple history.

The canonical consumer uses ``MetricSeries``.  These helpers keep the existing
endpoint-named CSV cache and provenance-handle framing readable while confining
FMP field aliases to the adapter package.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from schema.provider_port.contracts import MetricPoint, MetricSeries, VendorProvenance

from .metrics import build_annual_metric_series, build_ttm_metric_series
from .native import (
    EARNINGS_YIELD_FIELDS,
    EARNINGS_YIELD_TTM_FIELDS,
    PE_FIELDS,
    PE_TTM_FIELDS,
    record_datetime,
)


PERIOD_IDENTITY_FIELDS = ("date", "period", "fiscalYear")


def frame_legacy_scenario_multiple_rows(
    records: list[dict[str, Any]],
    *,
    endpoint: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Preserve the v1 endpoint-cache framing at the adapter boundary."""

    if not records:
        raise ValueError("scenario multiple cache has zero parseable data rows")
    if endpoint == "key_metrics":
        fields = PERIOD_IDENTITY_FIELDS + PE_FIELDS + EARNINGS_YIELD_FIELDS
        row_limit = limit
    elif endpoint == "key_metrics_ttm":
        fields = PERIOD_IDENTITY_FIELDS + PE_TTM_FIELDS + EARNINGS_YIELD_TTM_FIELDS
        row_limit = 1
    else:
        raise ValueError(f"unsupported scenario multiple endpoint: {endpoint}")
    bounded = records if row_limit is None else records[: max(0, int(row_limit))]
    if not bounded:
        raise ValueError("scenario multiple cache has zero parseable data rows")
    return [
        {field: record[field] for field in fields if field in record}
        for record in bounded
    ]


def _record_year(record: dict[str, Any]) -> int | None:
    raw = record.get("fiscalYear")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    parsed = record_datetime(record)
    return parsed.year if parsed is not None else None


def _empty_series(ticker: str, endpoint: str, period: str) -> MetricSeries:
    return MetricSeries(
        ticker=ticker,
        metric_id="pe_ratio",
        period=period,  # type: ignore[arg-type]
        points=(),
        provenance=VendorProvenance(
            provider="fmp",
            native_endpoint=endpoint,
            native_field="peRatio",
        ),
    )


def legacy_rows_to_pe_series(
    ticker: str,
    records: list[dict[str, Any]],
    *,
    endpoint: str,
    limit: int | None = None,
) -> MetricSeries:
    """Translate a selected legacy cache snapshot into canonical P/E series."""

    framed = frame_legacy_scenario_multiple_rows(
        records,
        endpoint=endpoint,
        limit=limit,
    )
    if endpoint == "key_metrics":
        years = [year for record in framed if (year := _record_year(record)) is not None]
        if not years:
            raise ValueError("scenario multiple cache has no valid annual observation date")
        series = build_annual_metric_series(
            ticker,
            framed,
            "pe_ratio",
            window_end_year=max(years),
            window_n_years=max(years) - min(years) + 1,
            native_endpoint=endpoint,
        )
        return series or _empty_series(ticker, endpoint, "annual")
    if endpoint == "key_metrics_ttm":
        dates = [
            parsed.date()
            for record in framed
            if (parsed := record_datetime(record)) is not None
        ]
        if not dates:
            raise ValueError("scenario multiple cache has no valid TTM observation date")
        series = build_ttm_metric_series(
            ticker,
            framed,
            "pe_ratio",
            as_of_cutoff=max(dates),
            limit=1,
            native_endpoint=endpoint,
        )
        return series or _empty_series(ticker, endpoint, "ttm")
    raise ValueError(f"unsupported scenario multiple endpoint: {endpoint}")


def _series_by_metric(series: Sequence[MetricSeries]) -> dict[str, MetricSeries]:
    return {item.metric_id: item for item in series}


def _point_lookup(series: MetricSeries | None) -> dict[tuple[int | None, date], MetricPoint]:
    if series is None:
        return {}
    return {(point.fiscal_year, point.as_of): point for point in series.points}


def project_metric_series_to_legacy_rows(
    series: Sequence[MetricSeries],
    *,
    endpoint: str,
) -> list[dict[str, Any]]:
    """Project canonical P/E/yield series into the existing CSV/handle shape."""

    by_metric = _series_by_metric(series)
    pe_series = by_metric.get("pe_ratio")
    if pe_series is None:
        return []
    yield_series = by_metric.get("earnings_yield")
    yields = _point_lookup(yield_series)
    rows: list[dict[str, Any]] = []
    for point in pe_series.points:
        row: dict[str, Any] = {"date": point.as_of.isoformat()}
        if endpoint == "key_metrics":
            row["period"] = "FY"
            row["fiscalYear"] = point.fiscal_year
            direct_fields = PE_FIELDS
            yield_fallback = "earningsYield"
        elif endpoint == "key_metrics_ttm":
            direct_fields = PE_TTM_FIELDS
            yield_fallback = "earningsYieldTTM"
        else:
            raise ValueError(f"unsupported scenario multiple endpoint: {endpoint}")

        yield_point = yields.get((point.fiscal_year, point.as_of))
        native_field = pe_series.provenance.native_field
        if native_field in direct_fields:
            row[native_field] = str(point.value)
        elif yield_point is None:
            row["peRatio" if endpoint == "key_metrics" else "peRatioTTM"] = str(point.value)
        if yield_point is not None:
            yield_field = (
                yield_series.provenance.native_field
                if yield_series is not None
                else yield_fallback
            )
            row[yield_field] = str(yield_point.value)
        rows.append(row)
    return rows


__all__ = [
    "PE_TTM_FIELDS",
    "frame_legacy_scenario_multiple_rows",
    "legacy_rows_to_pe_series",
    "project_metric_series_to_legacy_rows",
]
