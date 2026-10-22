"""Shared display contracts and the model-driver unit display boundary."""

from typing import cast

from industry_slice_core.industry_types import Unit
from value_semantics_core import CONVENTIONS_IR_V1
from value_semantics_core.numeric_display import (
    DenominatorBasis,
    NumericDisplaySpec,
    NumericKind,
    NumericScale,
)

_UNIT_NUMERIC_KIND = {
    binding.unit: binding.numeric_kind
    for binding in CONVENTIONS_IR_V1.unit_to_numeric_kind
}


def driver_unit_numeric_kind(unit: Unit) -> NumericKind:
    """Map internal driver dimensions onto the published display vocabulary."""
    if unit in (Unit.per_count, Unit.per_day):
        return "currency"
    return cast(NumericKind, _UNIT_NUMERIC_KIND[unit.value])


__all__ = [
    "DenominatorBasis",
    "NumericDisplaySpec",
    "NumericKind",
    "NumericScale",
    "driver_unit_numeric_kind",
]
