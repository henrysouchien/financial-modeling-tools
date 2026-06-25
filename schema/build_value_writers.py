"""Value and override writer helpers for schema build population."""

from __future__ import annotations

from .models import (
    EdgarProvenance,
    FmpProvenance,
    FormulaSpec,
    FormulaType,
    LineItem,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)


def _set_imported_value(
    item: LineItem,
    year: int,
    value: float,
    provenance: ValueProvenance = ValueProvenance.imported_fmp,
    *,
    edgar_provenance: EdgarProvenance | None = None,
    fmp_provenance: FmpProvenance | None = None,
) -> None:
    if item.values is None:
        item.values = ValueSeries()
    item.values.values[int(year)] = ValueCell(
        period=int(year),
        value=float(value),
        provenance=provenance,
        edgar_provenance=edgar_provenance,
        fmp_provenance=fmp_provenance,
    )


def _set_constant_override(
    item: LineItem,
    year: int,
    value: float,
    *,
    synthetic: bool = False,
    edgar_provenance: EdgarProvenance | None = None,
    fmp_provenance: FmpProvenance | None = None,
) -> None:
    if item.overrides is None:
        item.overrides = {}
    item.overrides[int(year)] = FormulaSpec(
        type=FormulaType.constant,
        params={"value": float(value) if isinstance(value, int) else value},
        note="synthetic" if synthetic else None,
        edgar_provenance=edgar_provenance,
        fmp_provenance=fmp_provenance,
    )


__all__ = [
    "_set_constant_override",
    "_set_imported_value",
]
