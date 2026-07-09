"""Policy-gated source-arbitration application pass."""

from __future__ import annotations

from typing import Any

from .build_diagnostic_source_arbitration import (
    _check_source_arbitration,
    _current_cell_state,
    _edgar_provenance,
    _fmp_provenance,
    _served_value_provenance,
)
from .build_diagnostic_types import DiagnosticTolerances, SourceArbitrationCheck
from .build_diagnostic_values import _concept_item_map
from .build_source_arbitration import SourceName
from .build_value_writers import _set_constant_override, _set_imported_value
from .models import DataSourceMapping, FinancialModel, LineItem, ValueProvenance
from .source_arbitration_input import SourceArbitrationDiagnosticInput

_EQUALITY_EPSILON = 1e-9


def apply_source_arbitration(
    model: FinancialModel,
    source_arbitration_input: SourceArbitrationDiagnosticInput,
    *,
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances | None = None,
) -> SourceArbitrationCheck:
    """Apply authoritative source values and return the pre-mutation decision surface."""

    tolerances = tolerances or DiagnosticTolerances()
    check = _check_source_arbitration(
        source_arbitration_input,
        model=model,
        taxonomy=taxonomy,
        tolerances=tolerances,
    )
    if source_arbitration_input.mode != "apply":
        return check

    concept_items = _concept_item_map(model)
    for concept_id, concept_payload in check.by_concept.items():
        mapping = taxonomy.get(concept_id)
        if mapping is None:
            continue
        for year_key, row in concept_payload.get("by_year", {}).items():
            if row.get("action") != "apply_authoritative_value":
                continue
            chosen_source = row.get("chosen_source")
            if chosen_source not in {"fmp", "edgar"}:
                continue
            year = int(year_key)
            value = row.get(f"{chosen_source}_value")
            if value is None:
                continue
            provenance = _authority_provenance(
                source_arbitration_input,
                mapping,
                concept_id=concept_id,
                year=year,
                source=chosen_source,
            )
            if provenance is None:
                continue
            applied = _apply_to_matching_items(
                concept_items.get(concept_id, []),
                year=year,
                value=float(value),
                source=chosen_source,
                provenance=provenance,
                original_served_source=row.get("original_served_source"),
                original_value=row.get(f"{row.get('original_served_source')}_value"),
            )
            if not applied:
                continue
            row["applied"] = True
            row["final_source"] = chosen_source
            check.final_source_by_concept_year.setdefault(concept_id, {})[
                str(year)
            ] = chosen_source
            check.summary["cells_applied"] = int(check.summary.get("cells_applied", 0)) + 1

    return check


def _authority_provenance(
    source_arbitration_input: SourceArbitrationDiagnosticInput,
    mapping: DataSourceMapping,
    *,
    concept_id: str,
    year: int,
    source: SourceName,
) -> Any | None:
    if source == "edgar":
        return _edgar_provenance(
            source_arbitration_input.edgar_buffer.get(concept_id),
            year,
        )
    return _fmp_provenance(
        mapping,
        source_arbitration_input.fmp_buffer.get(concept_id),
        year,
    )


def _apply_to_matching_items(
    items: list[LineItem],
    *,
    year: int,
    value: float,
    source: SourceName,
    provenance: Any,
    original_served_source: object,
    original_value: object,
) -> int:
    applied = 0
    expected_provenance = (
        _served_value_provenance(original_served_source)
        if isinstance(original_served_source, str)
        else None
    )
    for item in items:
        current_value, current_provenance = _current_cell_state([item], year)
        if not _matches_original(
            current_value,
            current_provenance,
            original_value=original_value,
            expected_provenance=expected_provenance,
        ):
            continue
        _write_authoritative_value(
            item,
            year=year,
            value=value,
            source=source,
            provenance=provenance,
        )
        applied += 1
    return applied


def _matches_original(
    current_value: float | None,
    current_provenance: ValueProvenance | None,
    *,
    original_value: object,
    expected_provenance: ValueProvenance | None,
) -> bool:
    if original_value is None:
        return current_value is None and current_provenance is None
    if current_value is None:
        return False
    try:
        original_float = float(original_value)
    except (TypeError, ValueError):
        return False
    if abs(float(current_value) - original_float) > _EQUALITY_EPSILON:
        return False
    return current_provenance == expected_provenance


def _write_authoritative_value(
    item: LineItem,
    *,
    year: int,
    value: float,
    source: SourceName,
    provenance: Any,
) -> None:
    value_provenance = (
        ValueProvenance.imported_edgar
        if source == "edgar"
        else ValueProvenance.imported_fmp
    )
    kwargs = (
        {"edgar_provenance": provenance}
        if source == "edgar"
        else {"fmp_provenance": provenance}
    )
    has_override_cell = item.overrides is not None and int(year) in item.overrides
    if item.historical is None and not has_override_cell:
        _set_imported_value(
            item,
            year,
            value,
            provenance=value_provenance,
            **kwargs,
        )
        return
    _set_constant_override(item, year, value, **kwargs)


__all__ = ["apply_source_arbitration"]
