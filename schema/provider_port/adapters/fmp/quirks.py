"""FMP record normalization and provider-specific value quirks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import math

from schema.provider_port.adapters.fmp.mapping import ConceptMapping
from schema.provider_port.contracts import VendorProvenance


def build_endpoint_lookup(
    fmp_data: Mapping[str, Sequence[dict]],
) -> dict[str, dict[int, dict]]:
    lookup: dict[str, dict[int, dict]] = {}
    for endpoint, records in (fmp_data or {}).items():
        endpoint_lookup: dict[int, dict] = {}
        for record in records or ():
            if not isinstance(record, dict):
                continue
            year = _record_year(record)
            if year is None:
                continue
            existing = endpoint_lookup.get(year)
            if _prefer_record(record, existing):
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


def _record_is_zero(record: dict, field: str) -> bool:
    value = record.get(field)
    if isinstance(value, bool) or value is None:
        return False
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _float_from_record(record: dict, field: str) -> float | None:
    value = record.get(field)
    return None if value is None else float(value)


def _finite_float_from_record(record: dict, field: str) -> float | None:
    value = record.get(field)
    if isinstance(value, bool) or value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _combined_sga_residual(
    record: dict,
    combined_field: str,
    *,
    combined_is_zero: bool,
) -> tuple[float | None, dict | None, bool]:
    combined = record.get(combined_field)
    if combined is None:
        return None, None, False
    gna_field = "generalAndAdministrativeExpenses"
    gna = record.get(gna_field)
    if gna is None or _record_is_zero(record, gna_field):
        return None, record, combined_is_zero
    combined_numeric = _finite_float_from_record(record, combined_field)
    gna_numeric = _finite_float_from_record(record, gna_field)
    if combined_numeric is None or gna_numeric is None:
        return None, record, combined_is_zero
    residual = combined_numeric - gna_numeric
    if residual < 0:
        return None, None, False
    return residual, None, residual == 0.0


def _operating_expenses_residual(record: dict) -> tuple[float, str] | None:
    total_opex = _finite_float_from_record(record, "operatingExpenses")
    source_field = "operatingExpenses"
    if total_opex is None or total_opex == 0.0:
        gross_profit = _finite_float_from_record(record, "grossProfit")
        operating_income = _finite_float_from_record(record, "operatingIncome")
        if gross_profit is None or operating_income is None:
            return None
        total_opex = gross_profit - operating_income
        source_field = "grossProfit-operatingIncome"
    if total_opex <= 0:
        return None
    residual = total_opex
    for field in (
        "researchAndDevelopmentExpenses",
        "generalAndAdministrativeExpenses",
    ):
        component = _finite_float_from_record(record, field)
        if component is not None and component > 0:
            residual -= component
    return (residual, source_field) if residual >= 0 else None


def _fallback_value(
    mapping: ConceptMapping,
    record: dict | None,
) -> tuple[float | None, str | None, dict | None, bool]:
    fallback_field = mapping.fallback_native_field
    if not record or fallback_field is None:
        return None, fallback_field, None, False
    fallback_present = record.get(fallback_field) is not None
    if (
        mapping.native_field != "sellingAndMarketingExpenses"
        or fallback_field != "sellingGeneralAndAdministrativeExpenses"
    ):
        fallback_source = record if fallback_present else None
        return None, fallback_field, fallback_source, False

    combined_is_zero = (
        _record_is_zero(record, fallback_field) if fallback_present else False
    )
    combined_reported = fallback_present and not combined_is_zero
    combined_value, combined_source, combined_zero = _combined_sga_residual(
        record,
        fallback_field,
        combined_is_zero=combined_is_zero,
    )
    if combined_reported:
        return combined_value, fallback_field, combined_source, combined_zero
    opex_residual = _operating_expenses_residual(record)
    if opex_residual is not None:
        value, field_used = opex_residual
        return value, field_used, None, value == 0.0
    if combined_source is not None or combined_value is not None:
        return combined_value, fallback_field, combined_source, combined_zero
    return None, fallback_field, None, False


def _extracted_value(
    mapping: ConceptMapping,
    record: dict | None,
) -> tuple[float | None, dict | None, str | None, bool]:
    raw_value = record.get(mapping.native_field) if record else None
    if (
        mapping.treat_zero_as_missing
        and record is not None
        and _record_is_zero(record, mapping.native_field)
    ):
        raw_value = None

    fallback_field = mapping.fallback_native_field
    (
        fallback_value,
        fallback_field_used,
        fallback_source,
        fallback_is_zero,
    ) = _fallback_value(mapping, record)
    fallback_present = fallback_source is not None or fallback_value is not None
    zero_split_with_combined_sga = (
        fallback_field == "sellingGeneralAndAdministrativeExpenses"
        and record is not None
        and _record_is_zero(record, mapping.native_field)
        and fallback_present
        and not (
            _record_is_zero(fallback_source, fallback_field)
            if fallback_source is not None
            else fallback_is_zero
        )
    )
    if (raw_value is None or zero_split_with_combined_sga) and fallback_field:
        if fallback_present:
            return fallback_value, fallback_source, fallback_field_used, True
    if raw_value is None or record is None:
        return None, None, mapping.native_field, False
    return None, record, mapping.native_field, False


def extract_raw_value(
    mapping: ConceptMapping,
    record: dict | None,
) -> tuple[object | None, str | None, bool]:
    value, source, field_used, fallback_used = _extracted_value(mapping, record)
    if source is not None and field_used is not None:
        return source.get(field_used), field_used, fallback_used
    return value, field_used, fallback_used


def _extract_numeric_value(
    mapping: ConceptMapping,
    record: dict | None,
) -> tuple[float | None, str | None, bool]:
    value, source, field_used, fallback_used = _extracted_value(mapping, record)
    if source is not None and field_used is not None:
        return _float_from_record(source, field_used), field_used, fallback_used
    return value, field_used, fallback_used


def scale_value(mapping: ConceptMapping, raw_value: float) -> float:
    value = float(raw_value)
    is_per_share = bool(mapping.tag and "EarningsPerShare" in mapping.tag)
    scaled = value if is_per_share else value / 1_000_000.0
    return -abs(scaled) if mapping.negate else scaled


def vendor_provenance(
    mapping: ConceptMapping,
    field_used: str | None,
    *,
    fallback_used: bool,
) -> VendorProvenance:
    del fallback_used
    return VendorProvenance(
        provider="fmp",
        native_endpoint=mapping.native_endpoint,
        native_field=field_used or mapping.native_field,
    )


__all__ = [
    "_prefer_record",
    "_record_year",
    "build_endpoint_lookup",
    "extract_raw_value",
    "scale_value",
    "vendor_provenance",
]
