"""The one scale-conversion owner for consumer numeric values.

Conversion changes magnitude only; callers own unit compatibility.
"""

from decimal import Decimal
from typing import Literal

Scale = Literal["units", "thousands", "millions", "billions"]

SCALE_EXPONENT: dict[Scale, int] = {
    "units": 0,
    "thousands": 3,
    "millions": 6,
    "billions": 9,
}


def canonical_scale(spelling: str | None) -> Scale:
    normalized = (spelling or "").strip().casefold()
    if normalized in {"", "unit", "units"}:
        return "units"
    if normalized == "thousands":
        return "thousands"
    if normalized == "millions":
        return "millions"
    if normalized == "billions":
        return "billions"
    raise ValueError(f"unsupported scale: {spelling!r}")


def convert_scale(
    value: Decimal,
    *,
    source_scale: str | None,
    target_scale: str | None,
) -> Decimal:
    return value.scaleb(
        SCALE_EXPONENT[canonical_scale(source_scale)]
        - SCALE_EXPONENT[canonical_scale(target_scale)]
    )
