"""Shared scalar-or-period-series value contracts."""

from __future__ import annotations

import math
from typing import Annotated, Any, TypeAlias

from pydantic import BeforeValidator, TypeAdapter

from industry_slice_core.thesis_shared_sources import ScalarValue


_FISCAL_YEAR_KEY = TypeAdapter(int)


def _normalize_period_value_series(value: Any) -> dict[int, float]:
    if not isinstance(value, dict) or not value:
        raise ValueError("period value series must be a non-empty mapping")
    normalized: dict[int, float] = {}
    for raw_period, raw_value in value.items():
        if isinstance(raw_period, bool):
            raise ValueError("period value series keys must be fiscal years")
        period = _FISCAL_YEAR_KEY.validate_python(raw_period)
        if (
            isinstance(raw_value, bool)
            or not isinstance(raw_value, (int, float))
            or not math.isfinite(float(raw_value))
        ):
            raise ValueError("period values must be finite numbers")
        if period in normalized:
            raise ValueError("period value series keys must be unique")
        normalized[period] = float(raw_value)
    return dict(sorted(normalized.items()))


PeriodValueSeries: TypeAlias = Annotated[
    dict[int, float],
    BeforeValidator(_normalize_period_value_series),
]
AssumptionValue: TypeAlias = ScalarValue | PeriodValueSeries


__all__ = ["AssumptionValue", "PeriodValueSeries"]
