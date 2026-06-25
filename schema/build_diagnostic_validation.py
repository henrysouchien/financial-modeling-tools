"""Cross-source validation checks for build diagnostics."""

from __future__ import annotations

from typing import Any

from .build_diagnostic_types import CrossSourceValidationCheck, DiagnosticTolerances
from .build_diagnostic_values import SEVERITY_ORDER
from .models import DataSourceMapping
from .validation_input import ValidationInput


def _check_cross_source_validation(
    validation_input: ValidationInput | None,
    *,
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
) -> CrossSourceValidationCheck:
    if validation_input is None:
        return CrossSourceValidationCheck(enabled=False)

    result = CrossSourceValidationCheck(enabled=True)
    opted_in = sorted(
        {str(concept_id) for concept_id in validation_input.opted_in_concepts}
    )
    historical_years = sorted(int(year) for year in validation_input.historical_years)

    for concept_id in opted_in:
        mapping = taxonomy.get(concept_id)
        if mapping is None or mapping.validation_tolerance_pct is None:
            continue

        tolerance_pct = float(mapping.validation_tolerance_pct)
        concept_payload: dict[str, Any] = {
            "tolerance_pct": tolerance_pct,
            "preferred_source": mapping.preferred_source,
            "headline_severity": "ok",
            "by_year": {},
        }

        for year in historical_years:
            fmp_value = _validation_buffer_value(
                validation_input.fmp_buffer.get(concept_id),
                year,
                ("values", "values_dict"),
            )
            edgar_value = _validation_buffer_value(
                validation_input.edgar_buffer.get(concept_id),
                year,
                ("values_dict", "values"),
            )

            if fmp_value is None and edgar_value is None:
                continue

            served_source = _validation_served_source(
                validation_input, concept_id, year
            )
            if fmp_value is None or edgar_value is None:
                concept_payload["by_year"][str(year)] = {
                    "comparison_status": "incomparable",
                    "fmp_value": fmp_value,
                    "edgar_value": edgar_value,
                    "delta": None,
                    "delta_pct": None,
                    "abs_delta_pct": None,
                    "served_source": served_source,
                    "severity": "ok",
                }
                result.summary["cells_incomparable"] += 1
                continue

            delta = float(edgar_value) - float(fmp_value)
            denominator = max(abs(float(edgar_value)), abs(float(fmp_value)))
            if denominator > 0:
                delta_pct = delta / denominator
                abs_delta_pct = abs(delta) / denominator
            else:
                delta_pct = 0.0
                abs_delta_pct = 0.0

            severity = "ok"
            if abs_delta_pct >= tolerances.cross_source_material_pct:
                severity = "material_gap"
            elif abs_delta_pct >= tolerance_pct:
                severity = "gap"

            concept_payload["by_year"][str(year)] = {
                "comparison_status": "compared",
                "fmp_value": float(fmp_value),
                "edgar_value": float(edgar_value),
                "delta": delta,
                "delta_pct": delta_pct,
                "abs_delta_pct": abs_delta_pct,
                "served_source": served_source,
                "severity": severity,
            }
            result.summary["cells_compared"] += 1
            if (
                SEVERITY_ORDER[severity]
                > SEVERITY_ORDER[concept_payload["headline_severity"]]
            ):
                concept_payload["headline_severity"] = severity

        result.by_concept[concept_id] = concept_payload

    result.summary["concepts_checked"] = len(result.by_concept)
    for payload in result.by_concept.values():
        headline = payload.get("headline_severity")
        if headline == "gap":
            result.summary["concepts_with_gap"] += 1
        elif headline == "material_gap":
            result.summary["concepts_with_material_gap"] += 1

    return result


def _validation_buffer_value(
    fetch_result: Any,
    year: int,
    value_attrs: tuple[str, ...],
) -> float | None:
    if fetch_result is None:
        return None

    for attr in value_attrs:
        values = getattr(fetch_result, attr, None)
        if isinstance(values, dict):
            value = _year_lookup(values, year)
            if value is not None:
                return float(value)

    if isinstance(fetch_result, dict):
        for key in value_attrs:
            values = fetch_result.get(key)
            if isinstance(values, dict):
                value = _year_lookup(values, year)
                if value is not None:
                    return float(value)
        value = _year_lookup(fetch_result, year)
        if value is not None:
            return float(value)

    return None


def _year_lookup(values: dict[Any, Any], year: int) -> Any | None:
    if year in values:
        return values[year]
    return values.get(str(year))


def _validation_served_source(
    validation_input: ValidationInput,
    concept_id: str,
    year: int,
) -> str | None:
    source_by_year = validation_input.served_source_by_concept_year.get(concept_id, {})
    if year in source_by_year:
        return source_by_year[year]
    return source_by_year.get(str(year))  # type: ignore[arg-type]


__all__ = [
    "_check_cross_source_validation",
    "_validation_buffer_value",
    "_year_lookup",
    "_validation_served_source",
]
