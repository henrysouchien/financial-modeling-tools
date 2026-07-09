"""Source-arbitration shadow diagnostics."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .build_diagnostic_types import DiagnosticTolerances, SourceArbitrationCheck
from .build_diagnostic_validation import _validation_buffer_value
from .build_diagnostic_values import _concept_item_map, _constant_override_value
from .build_source_arbitration import (
    SourceArbitrationDecision,
    SourceArbitrationInput,
    SourceName,
    SourceObservationStatus,
    decide_source_arbitration,
)
from .models import (
    DataSourceMapping,
    EdgarProvenance,
    FinancialModel,
    FmpProvenance,
    ValueProvenance,
)
from .source_arbitration_input import SourceArbitrationDiagnosticInput


def _check_source_arbitration(
    source_arbitration_input: SourceArbitrationDiagnosticInput | None,
    *,
    model: FinancialModel,
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
) -> SourceArbitrationCheck:
    if source_arbitration_input is None:
        return SourceArbitrationCheck(enabled=False)

    result = SourceArbitrationCheck(enabled=True, mode=source_arbitration_input.mode)
    concept_items = _concept_item_map(model)
    historical_years = sorted(
        int(year) for year in source_arbitration_input.historical_years
    )

    for concept_id in sorted(source_arbitration_input.opted_in_concepts):
        mapping = taxonomy.get(concept_id)
        if mapping is None:
            continue

        concept_payload: dict[str, Any] = {
            "policy": mapping.source_arbitration_policy or "diagnostic_only",
            "preferred_source": mapping.preferred_source,
            "tolerance_pct": mapping.validation_tolerance_pct,
            "by_year": {},
        }

        for year in historical_years:
            fmp_result = source_arbitration_input.fmp_buffer.get(concept_id)
            edgar_result = source_arbitration_input.edgar_buffer.get(concept_id)
            fmp_value = _validation_buffer_value(
                fmp_result,
                year,
                ("values", "values_dict"),
            )
            edgar_value = _validation_buffer_value(
                edgar_result,
                year,
                ("values_dict", "values"),
            )
            original_served_source = _served_source(
                source_arbitration_input,
                concept_id,
                year,
            )
            if (
                fmp_value is None
                and edgar_value is None
                and original_served_source is None
            ):
                continue

            current_value, current_provenance = _current_cell_state(
                concept_items.get(concept_id, []),
                year,
            )
            decision_input = SourceArbitrationInput(
                concept_id=concept_id,
                year=year,
                original_served_source=original_served_source,
                original_cell_value=_served_source_value(
                    original_served_source,
                    fmp_value=fmp_value,
                    edgar_value=edgar_value,
                ),
                original_cell_provenance=_served_value_provenance(
                    original_served_source
                ),
                fmp_value=fmp_value,
                fmp_status=_fmp_status(fmp_result, fmp_value),
                fmp_provenance=_fmp_provenance(mapping, fmp_result, year),
                edgar_value=edgar_value,
                edgar_status=_edgar_status(edgar_result, year, edgar_value),
                edgar_provenance=_edgar_provenance(edgar_result, year),
                current_cell_value=current_value,
                current_cell_provenance=current_provenance,
            )
            decision = decide_source_arbitration(
                decision_input,
                mapping=mapping,
                mode=source_arbitration_input.mode,
                materiality_pct=tolerances.cross_source_material_pct,
            )
            row = _decision_payload(decision)
            concept_payload["by_year"][str(year)] = row
            _record_decision_summary(result, decision, row)

        if concept_payload["by_year"]:
            result.by_concept[concept_id] = concept_payload

    result.summary["concepts_checked"] = len(result.by_concept)
    return result


def _decision_payload(decision: SourceArbitrationDecision) -> dict[str, Any]:
    payload = asdict(decision)
    final_source = _actual_final_source(decision)
    would_final_source = _would_final_source(decision)
    payload["applied"] = False
    payload["final_source"] = final_source
    payload["would_final_source"] = would_final_source
    return payload


def _record_decision_summary(
    result: SourceArbitrationCheck,
    decision: SourceArbitrationDecision,
    row: dict[str, Any],
) -> None:
    result.summary["cells_decided"] += 1
    result.summary["actions"][decision.action] = (
        result.summary["actions"].get(decision.action, 0) + 1
    )
    if decision.would_apply:
        result.summary["cells_would_apply"] += 1
    if decision.action == "fail_closed_missing_provenance":
        result.summary["cells_fail_closed"] += 1
    if decision.action == "skip_superseded_cell":
        result.summary["cells_skipped_superseded"] += 1
    final_source = row.get("final_source")
    if final_source:
        result.final_source_by_concept_year.setdefault(decision.concept_id, {})[
            str(decision.year)
        ] = str(final_source)


def _actual_final_source(decision: SourceArbitrationDecision) -> SourceName | None:
    if decision.action == "keep_authoritative_value":
        return decision.chosen_source
    return decision.original_served_source


def _would_final_source(decision: SourceArbitrationDecision) -> SourceName | None:
    if decision.chosen_source is None:
        return _actual_final_source(decision)
    if decision.action in {
        "apply_authoritative_value",
        "keep_authoritative_value",
    }:
        return decision.chosen_source
    return _actual_final_source(decision)


def _served_source(
    source_arbitration_input: SourceArbitrationDiagnosticInput,
    concept_id: str,
    year: int,
) -> SourceName | None:
    served_by_year = source_arbitration_input.served_source_by_concept_year.get(
        concept_id,
        {},
    )
    value = served_by_year.get(int(year), served_by_year.get(str(year)))
    if value in {"fmp", "edgar"}:
        return value
    return None


def _served_source_value(
    served_source: SourceName | None,
    *,
    fmp_value: float | None,
    edgar_value: float | None,
) -> float | None:
    if served_source == "fmp":
        return fmp_value
    if served_source == "edgar":
        return edgar_value
    return None


def _served_value_provenance(
    served_source: SourceName | None,
) -> ValueProvenance | None:
    if served_source == "fmp":
        return ValueProvenance.imported_fmp
    if served_source == "edgar":
        return ValueProvenance.imported_edgar
    return None


def _current_cell_state(
    items: list[Any],
    year: int,
) -> tuple[float | None, ValueProvenance | None]:
    for item in items:
        if item.values is None:
            cell = None
        else:
            cell = item.values.values.get(int(year))
        if cell is not None and cell.value is not None:
            return float(cell.value), cell.provenance
        if item.overrides is None:
            continue
        override = item.overrides.get(int(year))
        override_value = _constant_override_value(override)
        if override_value is not None:
            return float(override_value), _override_provenance(override)
    return None, None


def _override_provenance(override: Any) -> ValueProvenance | None:
    if getattr(override, "edgar_provenance", None) is not None:
        return ValueProvenance.imported_edgar
    if getattr(override, "fmp_provenance", None) is not None:
        return ValueProvenance.imported_fmp
    return ValueProvenance.computed


def _fmp_status(fetch_result: Any, value: float | None) -> SourceObservationStatus:
    if value is not None:
        return "ok"
    if getattr(fetch_result, "missing", True):
        return "missing"
    return "failed"


def _edgar_status(
    fetch_result: Any,
    year: int,
    value: float | None,
) -> SourceObservationStatus:
    if value is not None:
        return "ok"
    if fetch_result is None:
        return "missing"
    failed_years = set(getattr(fetch_result, "failed_years", set()) or set())
    if int(year) in failed_years or getattr(fetch_result, "status", None) == "failed":
        return "failed"
    return "missing"


def _fmp_provenance(
    mapping: DataSourceMapping,
    fetch_result: Any,
    year: int,
) -> FmpProvenance | None:
    field_used_by_year = _year_map(fetch_result, "field_used_by_year")
    field_used = field_used_by_year.get(int(year), field_used_by_year.get(str(year)))
    if not field_used or not mapping.fmp_endpoint:
        return None
    return FmpProvenance(
        endpoint=mapping.fmp_endpoint,
        field=str(field_used),
        fallback_field_used=(
            str(field_used)
            if mapping.fallback_fmp_field and field_used != mapping.fmp_field
            else None
        ),
    )


def _edgar_provenance(fetch_result: Any, year: int) -> EdgarProvenance | None:
    provenance_by_year = _year_map(fetch_result, "provenance_by_year")
    provenance = provenance_by_year.get(int(year), provenance_by_year.get(str(year)))
    if isinstance(provenance, EdgarProvenance):
        return provenance
    return None


def _year_map(fetch_result: Any, attr_name: str) -> dict[Any, Any]:
    if fetch_result is None:
        return {}
    if isinstance(fetch_result, dict):
        value = fetch_result.get(attr_name)
    else:
        value = getattr(fetch_result, attr_name, None)
    if isinstance(value, dict):
        return value
    return {}


__all__ = ["_check_source_arbitration"]
