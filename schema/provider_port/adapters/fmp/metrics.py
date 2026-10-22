"""Metric-series extraction for the FMP adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal, cast

from schema.provider_port.contracts import MetricPoint, MetricSeries, VendorProvenance
from industry_slice_core.metric_registry import (
    registered_metric_ids,
    require_registered_metric_id,
)

from .native import (
    EARNINGS_YIELD_FIELDS,
    EARNINGS_YIELD_TTM_FIELDS,
    PE_FIELDS,
    PE_TTM_FIELDS,
    first_numeric,
    record_datetime,
)


@dataclass(frozen=True)
class MetricFieldMapping:
    metric_id: str
    capability: Literal["metric_series", "estimates"]
    annual_endpoint: str | None
    ttm_endpoint: str | None
    native_fields: tuple[str, ...]
    calculation: Literal["identity", "ratio", "growth"] = "identity"
    denominator_endpoint: str | None = None
    denominator_fields: tuple[str, ...] = ()
    scale_divisor: Decimal = Decimal("1")


_CONFIG_PATH = Path(__file__).with_name("metric_mapping.json")


@lru_cache(maxsize=1)
def load_metric_field_mappings() -> tuple[MetricFieldMapping, ...]:
    payload = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    rows: list[MetricFieldMapping] = []
    for row in payload:
        metric_id = require_registered_metric_id(
            row["metric_id"],
            entry_point="FMP metric-adapter mapping",
        )
        capability = str(row.get("capability", "metric_series"))
        if capability not in {"metric_series", "estimates"}:
            raise ValueError(f"unsupported FMP metric capability: {capability!r}")
        calculation = str(row.get("calculation", "identity"))
        if calculation not in {"identity", "ratio", "growth"}:
            raise ValueError(f"unsupported FMP metric calculation: {calculation!r}")
        native_fields = tuple(str(field) for field in row["native_fields"])
        if not native_fields:
            raise ValueError(f"FMP metric mapping {metric_id!r} has no native fields")
        scale_divisor = Decimal(str(row.get("scale_divisor", 1)))
        if scale_divisor <= 0:
            raise ValueError(f"FMP metric mapping {metric_id!r} has invalid scale divisor")
        rows.append(
            MetricFieldMapping(
                metric_id=metric_id,
                capability=cast(
                    Literal["metric_series", "estimates"],
                    capability,
                ),
                annual_endpoint=(
                    str(row["annual_endpoint"])
                    if row.get("annual_endpoint") is not None
                    else None
                ),
                ttm_endpoint=(
                    str(row["ttm_endpoint"])
                    if row.get("ttm_endpoint") is not None
                    else None
                ),
                native_fields=native_fields,
                calculation=cast(
                    Literal["identity", "ratio", "growth"],
                    calculation,
                ),
                denominator_endpoint=(
                    str(row["denominator_endpoint"])
                    if row.get("denominator_endpoint") is not None
                    else None
                ),
                denominator_fields=tuple(
                    str(field) for field in row.get("denominator_fields", ())
                ),
                scale_divisor=scale_divisor,
            )
        )
    ids = [row.metric_id for row in rows]
    duplicates = sorted({metric_id for metric_id in ids if ids.count(metric_id) > 1})
    if duplicates:
        raise ValueError(f"duplicate FMP metric mappings: {duplicates}")
    missing = sorted(registered_metric_ids() - set(ids))
    if missing:
        raise ValueError(f"canonical metrics missing FMP adapter mappings: {missing}")
    return tuple(rows)


def mapping_for_metric(metric_id: str) -> MetricFieldMapping | None:
    canonical_id = require_registered_metric_id(
        metric_id,
        entry_point="FMP adapter metric",
    )
    for row in load_metric_field_mappings():
        if row.metric_id == canonical_id:
            return row
    return None


def _record_year(record: dict) -> int | None:
    raw = record.get("fiscalYear") or record.get("calendarYear")
    if raw is None:
        parsed = record_datetime(record)
        return parsed.year if parsed else None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _record_as_of(record: dict) -> date:
    parsed = record_datetime(record)
    if parsed is not None:
        return parsed.date()
    year = _record_year(record)
    if year is not None:
        return date(year, 12, 31)
    raise ValueError("metric record has no valid date or fiscal year")


def _first_numeric_with_field(
    record: dict,
    fields: tuple[str, ...],
) -> tuple[float | None, str | None]:
    for field in fields:
        value = first_numeric(record, (field,))
        if value is not None:
            return value, field
    return None, None


def _pe_from_record(
    record: dict,
    *,
    ttm: bool,
) -> tuple[float | None, str | None]:
    if ttm:
        pe_fields = PE_TTM_FIELDS
        yield_fields = EARNINGS_YIELD_TTM_FIELDS
    else:
        pe_fields = PE_FIELDS
        yield_fields = EARNINGS_YIELD_FIELDS
    value, field_used = _first_numeric_with_field(record, pe_fields)
    if value is not None and value > 0:
        return value, field_used
    ey, yield_field_used = _first_numeric_with_field(record, yield_fields)
    if ey is not None and ey > 0:
        return 1.0 / ey, yield_field_used
    return None, None


def _metric_value(
    record: dict,
    mapping: MetricFieldMapping,
    *,
    ttm: bool,
    denominator_record: dict | None = None,
    previous_record: dict | None = None,
) -> tuple[float | None, str | None]:
    if mapping.metric_id == "pe_ratio":
        return _pe_from_record(record, ttm=ttm)
    value, field_used = _first_numeric_with_field(record, mapping.native_fields)
    if value is None:
        return None, None
    if mapping.calculation == "identity":
        return value, field_used
    if mapping.calculation == "ratio":
        if denominator_record is None:
            return None, None
        denominator, denominator_field = _first_numeric_with_field(
            denominator_record,
            mapping.denominator_fields,
        )
        if denominator in (None, 0):
            return None, None
        return value / denominator, f"{field_used}/{denominator_field}"
    if mapping.calculation == "growth":
        if previous_record is None:
            return None, None
        previous, previous_field = _first_numeric_with_field(
            previous_record,
            mapping.native_fields,
        )
        if previous in (None, 0):
            return None, None
        return value / previous - 1.0, f"{field_used}/{previous_field}"
    raise ValueError(f"unsupported FMP metric calculation: {mapping.calculation!r}")


def build_annual_metric_series(
    ticker: str,
    records: list[dict],
    metric_id: str,
    *,
    window_end_year: int,
    window_n_years: int,
    native_endpoint: str,
    denominator_records: list[dict] | None = None,
) -> MetricSeries | None:
    mapping = mapping_for_metric(metric_id)
    if mapping is None:
        return None
    start_year = window_end_year - window_n_years + 1
    points: list[MetricPoint] = []
    native_field_used: str | None = None
    records_by_year = {
        year: record
        for record in records
        if (year := _record_year(record)) is not None
    }
    denominator_by_year = {
        year: record
        for record in denominator_records or []
        if (year := _record_year(record)) is not None
    }
    for record in records:
        fiscal_year = _record_year(record)
        if fiscal_year is None or fiscal_year < start_year or fiscal_year > window_end_year:
            continue
        value, field_used = _metric_value(
            record,
            mapping,
            ttm=False,
            denominator_record=denominator_by_year.get(fiscal_year),
            previous_record=records_by_year.get(fiscal_year - 1),
        )
        if value is None:
            continue
        native_field_used = native_field_used or field_used
        points.append(
            MetricPoint(
                value=Decimal(str(value)) / mapping.scale_divisor,
                fiscal_year=fiscal_year,
                as_of=_record_as_of(record),
            )
        )
    points.sort(key=lambda point: (point.as_of, point.fiscal_year or 0), reverse=True)
    if not points:
        return None
    return MetricSeries(
        ticker=ticker,
        metric_id=metric_id,
        period="annual",
        points=tuple(points),
        provenance=VendorProvenance(
            provider="fmp",
            native_endpoint=native_endpoint,
            native_field=native_field_used or mapping.native_fields[0],
        ),
    )


def build_ttm_metric_series(
    ticker: str,
    records: list[dict],
    metric_id: str,
    *,
    as_of_cutoff: date,
    limit: int,
    native_endpoint: str,
) -> MetricSeries | None:
    mapping = mapping_for_metric(metric_id)
    if mapping is None or not records or limit <= 0:
        return None
    points: list[MetricPoint] = []
    native_field_used: str | None = None
    for record in records:
        as_of = _record_as_of(record)
        if as_of > as_of_cutoff:
            continue
        value, field_used = _metric_value(record, mapping, ttm=True)
        if value is None:
            continue
        native_field_used = native_field_used or field_used
        points.append(
            MetricPoint(
                value=Decimal(str(value)) / mapping.scale_divisor,
                fiscal_year=None,
                as_of=as_of,
            )
        )
    points.sort(key=lambda point: point.as_of, reverse=True)
    points = points[:limit]
    if not points:
        return None
    return MetricSeries(
        ticker=ticker,
        metric_id=metric_id,
        period="ttm",
        points=tuple(points),
        provenance=VendorProvenance(
            provider="fmp",
            native_endpoint=native_endpoint,
            native_field=native_field_used or mapping.native_fields[0],
        ),
    )
