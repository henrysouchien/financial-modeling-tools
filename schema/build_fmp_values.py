"""FMP record and value normalization helpers for schema build orchestration."""

from __future__ import annotations

import math
import sys

from .models import DataSourceMapping, FmpProvenance
from .scaling import _PER_SHARE_CONCEPTS


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _build_fmp_lookup(fmp_data: dict) -> dict[str, dict[int, dict]]:
    lookup: dict[str, dict[int, dict]] = {}
    record_year = _parent_attr("_record_year", _record_year)
    prefer_record = _parent_attr("_prefer_record", _prefer_record)

    for endpoint, records in (fmp_data or {}).items():
        endpoint_lookup: dict[int, dict] = {}
        for record in records or []:
            if not isinstance(record, dict):
                continue
            year = record_year(record)
            if year is None:
                continue
            existing = endpoint_lookup.get(year)
            if prefer_record(record, existing):
                endpoint_lookup[year] = record
        lookup[str(endpoint)] = endpoint_lookup

    return lookup


def _record_year(record: dict) -> int | None:
    raw_year = record.get("calendarYear")
    if raw_year is None:
        date_value = record.get("date")
        if isinstance(date_value, str) and len(date_value) >= 4:
            raw_year = date_value[:4]
    try:
        return int(raw_year) if raw_year is not None else None
    except (TypeError, ValueError):
        return None


def _prefer_record(candidate: dict, existing: dict | None) -> bool:
    if existing is None:
        return True
    candidate_period = str(candidate.get("period") or "").upper()
    existing_period = str(existing.get("period") or "").upper()
    return candidate_period == "FY" and existing_period != "FY"


def _make_fmp_provenance(
    concept: DataSourceMapping,
    field_used: str | None,
) -> FmpProvenance | None:
    endpoint = concept.fmp_endpoint
    if not endpoint or not field_used:
        return None
    return FmpProvenance(
        endpoint=endpoint,
        field=field_used,
        fallback_field_used=(
            field_used
            if concept.fallback_fmp_field and field_used != concept.fmp_field
            else None
        ),
    )


def _scale_fmp_value(
    concept_id: str,
    raw_value,
    *,
    concept: DataSourceMapping | None = None,
) -> float:
    value = float(raw_value)
    per_share_concepts = _parent_attr("_PER_SHARE_CONCEPTS", _PER_SHARE_CONCEPTS)
    if concept_id in per_share_concepts:
        scaled = value
    else:
        scaled = value / 1_000_000.0
    if concept is not None and concept.fmp_negate:
        scaled = -abs(scaled)
    return scaled


def _is_fmp_zero_value(raw_value) -> bool:
    if isinstance(raw_value, bool) or raw_value is None:
        return False
    try:
        return float(raw_value) == 0.0
    except (TypeError, ValueError):
        return False


def _fmp_float_value(raw_value: object | None) -> float | None:
    if isinstance(raw_value, bool) or raw_value is None:
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _raw_fmp_value_for_concept(
    concept: DataSourceMapping,
    record: dict | None,
) -> tuple[object | None, str | None, bool]:
    is_fmp_zero_value = _parent_attr("_is_fmp_zero_value", _is_fmp_zero_value)
    fmp_fallback_value_for_concept = _parent_attr(
        "_fmp_fallback_value_for_concept",
        _fmp_fallback_value_for_concept,
    )

    field = concept.fmp_field
    raw_value = record.get(field) if record and field else None
    field_used = field

    if concept.treat_zero_as_missing and is_fmp_zero_value(raw_value):
        raw_value = None

    fallback_field = concept.fallback_fmp_field
    fallback_value = record.get(fallback_field) if record and fallback_field else None
    fallback_value, fallback_field_used = fmp_fallback_value_for_concept(
        concept,
        record,
        fallback_value,
        fallback_field,
    )
    zero_split_with_combined_sga_fallback = (
        fallback_field == "sellingGeneralAndAdministrativeExpenses"
        and is_fmp_zero_value(raw_value)
        and fallback_value is not None
        and not is_fmp_zero_value(fallback_value)
    )
    if (raw_value is None or zero_split_with_combined_sga_fallback) and fallback_field:
        if fallback_value is not None:
            return fallback_value, fallback_field_used, True
    return raw_value, field_used, False


def _fmp_fallback_value_for_concept(
    concept: DataSourceMapping,
    record: dict | None,
    fallback_value: object | None,
    fallback_field: str | None,
) -> tuple[object | None, str | None]:
    is_fmp_zero_value = _parent_attr("_is_fmp_zero_value", _is_fmp_zero_value)
    combined_sga_residual_fallback = _parent_attr(
        "_combined_sga_residual_fallback",
        _combined_sga_residual_fallback,
    )
    operating_expenses_residual_fallback = _parent_attr(
        "_operating_expenses_residual_fallback",
        _operating_expenses_residual_fallback,
    )

    if (
        concept.fmp_field != "sellingAndMarketingExpenses"
        or concept.fallback_fmp_field != "sellingGeneralAndAdministrativeExpenses"
        or not record
    ):
        return fallback_value, fallback_field

    combined_sga_reported = fallback_value is not None and not is_fmp_zero_value(fallback_value)
    combined_sga_fallback = combined_sga_residual_fallback(record, fallback_value)
    if combined_sga_reported:
        return combined_sga_fallback, fallback_field

    operating_expenses_fallback = operating_expenses_residual_fallback(record)
    if operating_expenses_fallback is not None:
        return operating_expenses_fallback

    if combined_sga_fallback is not None:
        return combined_sga_fallback, fallback_field
    return None, fallback_field


def _combined_sga_residual_fallback(
    record: dict,
    fallback_value: object | None,
) -> object | None:
    is_fmp_zero_value = _parent_attr("_is_fmp_zero_value", _is_fmp_zero_value)
    fmp_float_value = _parent_attr("_fmp_float_value", _fmp_float_value)

    if fallback_value is None:
        return None

    gna_value = record.get("generalAndAdministrativeExpenses")
    if gna_value is None or is_fmp_zero_value(gna_value):
        return fallback_value

    fallback_numeric = fmp_float_value(fallback_value)
    gna_numeric = fmp_float_value(gna_value)
    if fallback_numeric is None or gna_numeric is None:
        return fallback_value
    derived_value = fallback_numeric - gna_numeric
    if derived_value < 0:
        return None
    return derived_value


def _operating_expenses_residual_fallback(record: dict) -> tuple[object, str] | None:
    fmp_float_value = _parent_attr("_fmp_float_value", _fmp_float_value)

    total_opex = fmp_float_value(record.get("operatingExpenses"))
    source_field = "operatingExpenses"
    if total_opex is None or total_opex == 0.0:
        gross_profit = fmp_float_value(record.get("grossProfit"))
        operating_income = fmp_float_value(record.get("operatingIncome"))
        if gross_profit is None or operating_income is None:
            return None
        total_opex = gross_profit - operating_income
        source_field = "grossProfit-operatingIncome"

    if total_opex <= 0:
        return None

    residual = total_opex
    for component_field in (
        "researchAndDevelopmentExpenses",
        "generalAndAdministrativeExpenses",
    ):
        component = fmp_float_value(record.get(component_field))
        if component is None or component <= 0:
            continue
        residual -= component

    if residual < 0:
        return None
    return residual, source_field


__all__ = [
    "_build_fmp_lookup",
    "_combined_sga_residual_fallback",
    "_fmp_fallback_value_for_concept",
    "_fmp_float_value",
    "_is_fmp_zero_value",
    "_make_fmp_provenance",
    "_operating_expenses_residual_fallback",
    "_prefer_record",
    "_raw_fmp_value_for_concept",
    "_record_year",
    "_scale_fmp_value",
]
