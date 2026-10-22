"""Canonical typed semantics for driver-formula scaling."""

from __future__ import annotations

import math
import re
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, field_validator


_SCALE_FN_RE = re.compile(
    r"^\s*(?P<operator>[/*])\s*(?P<factor>[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$"
)


class DriverScaleOperation(BaseModel):
    """One ordered scaling operation applied after ``base * rate``."""

    model_config = ConfigDict(frozen=True)

    operator: Literal["divide", "multiply"]
    factor: float

    @field_validator("factor")
    @classmethod
    def _positive_finite_factor(cls, value: float) -> float:
        factor = float(value)
        if not math.isfinite(factor) or factor <= 0:
            raise ValueError("driver scale factor must be a finite number greater than zero")
        return factor


class DriverFormulaParams(BaseModel):
    """Validated compatibility contract for ``FormulaType.driver`` params."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    base: Any
    rate: Any
    scale: float | None = None
    scale_fn: str | None = None
    # Retained operand lineage of a baked ``rate`` literal, in the shape a
    # constant carries: ``{"value": <literal>, "expr": <input expression>}``.
    # Provenance walks it; it never computes and is never a graph edge.
    lineage: Any = None

    @field_validator("base", "rate")
    @classmethod
    def _required_expression(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("driver base and rate expressions are required")
        return value

    @field_validator("scale", mode="before")
    @classmethod
    def _valid_scale(cls, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("driver scale must be a JSON number")
        return DriverScaleOperation(operator="divide", factor=value).factor

    @field_validator("scale_fn")
    @classmethod
    def _canonical_scale_fn(cls, value: str | None) -> str | None:
        if value is None:
            return None
        match = _SCALE_FN_RE.fullmatch(str(value))
        if match is None:
            raise ValueError("driver scale_fn must be '/N' or '*N' with a positive finite N")
        operation = DriverScaleOperation(
            operator="divide" if match.group("operator") == "/" else "multiply",
            factor=float(match.group("factor")),
        )
        symbol = "/" if operation.operator == "divide" else "*"
        return f"{symbol}{operation.factor:g}"

    def scale_operations(self) -> tuple[DriverScaleOperation, ...]:
        operations: list[DriverScaleOperation] = []
        if self.scale is not None:
            operations.append(DriverScaleOperation(operator="divide", factor=self.scale))
        if self.scale_fn is not None:
            operations.append(
                DriverScaleOperation(
                    operator="divide" if self.scale_fn.startswith("/") else "multiply",
                    factor=float(self.scale_fn[1:]),
                )
            )
        return tuple(operations)


def driver_formula_params(params: Mapping[str, Any] | None) -> DriverFormulaParams:
    """Parse driver params into the one typed scaling contract."""

    return DriverFormulaParams.model_validate(dict(params or {}))


def normalize_driver_formula_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate legacy keys while preserving the established serialized shape."""

    parsed = driver_formula_params(params)
    normalized: dict[str, Any] = {"base": parsed.base, "rate": parsed.rate}
    if parsed.scale is not None:
        normalized["scale"] = parsed.scale
    if parsed.scale_fn is not None:
        normalized["scale_fn"] = parsed.scale_fn
    if parsed.lineage is not None:
        normalized["lineage"] = parsed.lineage
    return normalized


def apply_driver_scale_operations(
    value: float,
    operations: tuple[DriverScaleOperation, ...],
) -> float:
    """Apply canonical scaling operations in declared order."""

    result = float(value)
    for operation in operations:
        if operation.operator == "divide":
            result /= operation.factor
        else:
            result *= operation.factor
    return result


def apply_driver_scale_fn(value: float, scale_fn: str) -> float:
    """Apply a legacy ``scale_fn`` through the typed compatibility contract."""

    parsed = DriverFormulaParams(base=1.0, rate=1.0, scale_fn=scale_fn)
    return apply_driver_scale_operations(value, parsed.scale_operations())


def evaluate_driver_value(
    base: float | None,
    rate: float | None,
    params: Mapping[str, Any] | None,
) -> float | None:
    """Evaluate a driver with the canonical runtime semantics."""

    if base is None or rate is None:
        return None
    parsed = driver_formula_params(params)
    return apply_driver_scale_operations(float(base) * float(rate), parsed.scale_operations())


__all__ = [
    "DriverFormulaParams",
    "DriverScaleOperation",
    "apply_driver_scale_fn",
    "apply_driver_scale_operations",
    "driver_formula_params",
    "evaluate_driver_value",
    "normalize_driver_formula_params",
]
