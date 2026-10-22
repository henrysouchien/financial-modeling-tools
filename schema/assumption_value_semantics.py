"""Canonical percentage semantics for ModelSpec forecast assumptions.

``Assumption`` is also used by non-computational Thesis surfaces, so the
lowering contract is enforced at the ModelSpec boundary.  Percentage values
must declare their source numeric space.  Point-valued input is converted once
to the model's canonical fraction space; no magnitude inference is permitted.
A value that is not a finite number (``finite_number``) is left as authored:
the build's ModelSpec receiver reports it as ``forecast_assumption_incomplete``
and binds nothing from it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
import math
from typing import Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, model_validator

from industry_slice_core.industry_types import Unit


PercentageValueSpace = Literal["fraction", "percentage_points"]


def _identity(row: Mapping[str, Any]) -> str:
    assumption_id = str(row.get("assumption_id") or "").strip()
    if assumption_id:
        return f"assumption_id={assumption_id!r}"
    driver = str(row.get("driver") or "").strip()
    if driver:
        return f"driver={driver!r}"
    return "unidentified assumption"


def _unit_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


def finite_number(value: Any) -> float | None:
    """``value`` as a float when it is a finite real number, else ``None``.

    ``bool`` is not a number here, and neither is numeric text.
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


def _fraction(value: Any, divisor: float) -> Any:
    number = finite_number(value)
    return value if number is None else number / divisor


def _canonicalize_percentage_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(dict(row))
    identity = _identity(normalized)
    unit = _unit_value(normalized.get("unit"))
    raw_space = normalized.get("percentage_value_space")

    if unit != "percentage":
        if raw_space is not None:
            raise ValueError(
                "incomplete_model_spec: non-percentage assumption "
                f"{identity} cannot declare percentage_value_space"
            )
        return normalized

    if raw_space not in get_args(PercentageValueSpace):
        raise ValueError(
            "incomplete_model_spec: percentage assumption "
            f"{identity} requires percentage_value_space='fraction' or "
            "'percentage_points'"
        )

    divisor = 100.0 if raw_space == "percentage_points" else 1.0
    if normalized.get("value") is not None:
        normalized["value"] = _fraction(normalized["value"], divisor)
    period_values = normalized.get("period_values")
    if isinstance(period_values, Mapping):
        normalized["period_values"] = {
            period: _fraction(period_value, divisor)
            for period, period_value in period_values.items()
        }

    # The returned payload is canonical.  Re-validating or persisting it cannot
    # apply the point-to-fraction conversion a second time: a value left as
    # authored is never a number, so no later pass converts it either.
    normalized["percentage_value_space"] = "fraction"
    return normalized


def canonicalize_model_spec_assumption_semantics(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a ModelSpec payload whose percentage assumptions are fractions.

    This is the generic ingress contract.  It deliberately has no legacy
    defaults: an undeclared percentage source space fails closed.
    """

    normalized = deepcopy(dict(payload))
    if "forecast_assumptions" not in normalized:
        return normalized
    raw_assumptions = normalized.get("forecast_assumptions", ())
    if raw_assumptions is None:
        raw_assumptions = ()
    if not isinstance(raw_assumptions, (list, tuple)):
        raise ValueError(
            "incomplete_model_spec: forecast_assumptions must be a sequence"
        )

    assumptions: list[dict[str, Any]] = []
    for raw_row in raw_assumptions:
        if isinstance(raw_row, BaseModel):
            row = raw_row.model_dump(mode="python")
        elif isinstance(raw_row, Mapping):
            row = dict(raw_row)
        else:
            raise ValueError(
                "incomplete_model_spec: each forecast assumption must be an object"
            )
        assumptions.append(_canonicalize_percentage_row(row))
    normalized["forecast_assumptions"] = assumptions
    return normalized


class LegacyAssumptionSemanticsRule(BaseModel):
    """One explicit unit/source-space classification for a legacy assumption."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    producer: str = Field(min_length=1)
    source_unit: str = Field(min_length=1)
    canonical_unit: Unit | None
    percentage_value_space: PercentageValueSpace | None = None
    assumption_id: str | None = Field(default=None, min_length=1)
    driver: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _classification_is_coherent(self) -> "LegacyAssumptionSemanticsRule":
        if self.assumption_id is not None and self.driver is not None:
            raise ValueError(
                "legacy assumption rule may select assumption_id or driver, not both"
            )
        if self.canonical_unit is Unit.percentage:
            if self.percentage_value_space is None:
                raise ValueError(
                    "percentage classification requires percentage_value_space"
                )
        elif self.percentage_value_space is not None:
            raise ValueError(
                "non-percentage classification cannot declare percentage_value_space"
            )
        return self

    @property
    def is_identity_specific(self) -> bool:
        return self.assumption_id is not None or self.driver is not None

    def matches(self, *, producer: str, row: Mapping[str, Any]) -> bool:
        if producer != self.producer or _unit_value(row.get("unit")) != self.source_unit:
            return False
        if self.assumption_id is not None:
            return str(row.get("assumption_id") or "").strip() == self.assumption_id
        if self.driver is not None:
            return str(row.get("driver") or "").strip() == self.driver
        return True


def migrate_legacy_model_spec_assumption_semantics(
    payload: Mapping[str, Any],
    *,
    producer: str,
    rules: Sequence[LegacyAssumptionSemanticsRule | Mapping[str, Any]],
) -> dict[str, Any]:
    """Classify and canonicalize a legacy ModelSpec without numeric guessing.

    Identity-specific rules take precedence over a producer-wide rule.  Every
    legacy free-text unit or undeclared percentage source space must match
    exactly one applicable rule.
    """

    normalized_producer = str(producer or "").strip()
    if not normalized_producer:
        raise ValueError("legacy percentage migration requires producer provenance")
    typed_rules = tuple(
        rule
        if isinstance(rule, LegacyAssumptionSemanticsRule)
        else LegacyAssumptionSemanticsRule.model_validate(rule)
        for rule in rules
    )

    migrated = deepcopy(dict(payload))
    raw_assumptions = migrated.get("forecast_assumptions", ())
    if not isinstance(raw_assumptions, (list, tuple)):
        raise ValueError(
            "incomplete_model_spec: forecast_assumptions must be a sequence"
        )

    classified: list[dict[str, Any]] = []
    for raw_row in raw_assumptions:
        if not isinstance(raw_row, Mapping):
            raise ValueError("legacy assumption migration requires object assumptions")
        row = deepcopy(dict(raw_row))
        raw_unit = _unit_value(row.get("unit"))
        try:
            canonical_unit = Unit(raw_unit) if raw_unit else None
        except ValueError:
            canonical_unit = None
        requires_classification = bool(raw_unit) and (
            canonical_unit is None
            or (
                canonical_unit is Unit.percentage
                and row.get("percentage_value_space") is None
            )
        )
        if requires_classification:
            matching = [
                rule
                for rule in typed_rules
                if rule.matches(producer=normalized_producer, row=row)
            ]
            identity_specific = [rule for rule in matching if rule.is_identity_specific]
            selected = identity_specific or matching
            if len(selected) != 1:
                disposition = "unclassified" if not selected else "multiply classified"
                raise ValueError(
                    "legacy assumption migration "
                    f"{disposition} {_identity(row)} for producer "
                    f"{normalized_producer!r}"
                )
            classification = selected[0]
            row["unit"] = (
                None
                if classification.canonical_unit is None
                else classification.canonical_unit.value
            )
            if classification.percentage_value_space is None:
                row.pop("percentage_value_space", None)
            else:
                row["percentage_value_space"] = (
                    classification.percentage_value_space
                )
        classified.append(row)

    migrated["forecast_assumptions"] = classified
    return canonicalize_model_spec_assumption_semantics(migrated)


__all__ = [
    "LegacyAssumptionSemanticsRule",
    "PercentageValueSpace",
    "canonicalize_model_spec_assumption_semantics",
    "finite_number",
    "migrate_legacy_model_spec_assumption_semantics",
]
