from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict


_FROZEN_CONTRACT = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
    populate_by_name=True,
    frozen=True,
)

Severity = Literal["low", "medium", "high"]
Confidence = Literal["low", "medium", "high"]
DriverCategory = Literal[
    "revenue",
    "unit_economics",
    "cost_structure",
    "reinvestment",
    "capital_sources",
    "valuation",
    "other",
]
DriverUnit = Literal["dollars", "percentage", "ratio", "count", "per_share", "days", "multiple"]


__all__ = [
    "_FROZEN_CONTRACT",
    "Confidence",
    "DriverCategory",
    "DriverUnit",
    "Severity",
]
