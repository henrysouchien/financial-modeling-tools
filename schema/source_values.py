"""Source-value normalization and arbitration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import re
from typing import Any, Mapping

from value_semantics_core import Quantity

from schema.provider_port.contracts import ReportedPeriodType


PER_SHARE_CONCEPTS = frozenset({"eps_basic", "eps_diluted"})
NORMALIZED_UNIT_MILLIONS = "millions"
NORMALIZED_UNIT_PER_SHARE = "per_share"

ScalePrecedence = tuple[int, int]

_NAMED_SCALE_EXPONENT = {
    "hundredths": -2,
    "units": 0,
    "thousands": 3,
    "millions": 6,
    "billions": 9,
    "trillions": 12,
}
_NUMERIC_SCALE_RE = re.compile(r"^10\^(-?\d+)$")
_PRECEDENCE_KNOWN = 0
_PRECEDENCE_UNKNOWN = 1


@dataclass(frozen=True)
class SourceValue:
    """A raw source fact plus its normalized model value."""

    normalized_value: float | None
    normalized_unit: str
    raw_value: Any
    raw_scale: str | None
    scale_precedence: ScalePrecedence
    source: str
    tag: str | None = None
    year: int | None = None
    source_ref: Mapping[str, Any] | None = None
    fact_unit: str | None = None
    reported_period_start: date | None = None
    reported_duration_period_type: ReportedPeriodType | None = None
    source_accepted_at: datetime | None = None

    def __post_init__(self) -> None:
        if (
            self.reported_duration_period_type is not None
            and self.reported_period_start is None
        ):
            raise ValueError(
                "reported duration source requires reported_period_start"
            )
        if self.source_accepted_at is not None:
            if self.reported_duration_period_type is None:
                raise ValueError(
                    "source acceptance time requires reported duration provenance"
                )
            if (
                self.source_accepted_at.tzinfo is None
                or self.source_accepted_at.utcoffset() != timedelta(0)
            ):
                raise ValueError("source_accepted_at must be aware UTC")


def normalized_unit_for_concept(series_key: str) -> str:
    return NORMALIZED_UNIT_PER_SHARE if series_key in PER_SHARE_CONCEPTS else NORMALIZED_UNIT_MILLIONS


def scale_precedence(scale: str | None) -> ScalePrecedence:
    """Return a scale precision tuple where lower means finer."""

    if scale is None:
        return (_PRECEDENCE_KNOWN, 0)
    scale_norm = str(scale).strip().lower()
    if scale_norm == "":
        return (_PRECEDENCE_KNOWN, 0)
    if scale_norm in _NAMED_SCALE_EXPONENT:
        return (_PRECEDENCE_KNOWN, _NAMED_SCALE_EXPONENT[scale_norm])
    match = _NUMERIC_SCALE_RE.match(scale_norm)
    if match:
        return (_PRECEDENCE_KNOWN, int(match.group(1)))
    return (_PRECEDENCE_UNKNOWN, 0)


def normalize_edgar_value(value: Any, scale: str | None, series_key: str) -> float:
    numeric_value = float(value)
    scale_label = None if scale is None else str(scale).strip().lower()
    is_per_share = series_key in PER_SHARE_CONCEPTS

    if scale_label == "millions":
        return numeric_value
    if scale_label == "thousands":
        return numeric_value / 1_000.0
    if scale_label == "billions":
        return numeric_value * 1_000.0
    if scale_label == "hundredths":
        if is_per_share:
            return numeric_value / 100.0
        return numeric_value / 100_000_000.0
    if scale_label is None or scale_label in {"units", ""}:
        if is_per_share:
            return numeric_value
        return numeric_value / 1_000_000.0

    match = _NUMERIC_SCALE_RE.fullmatch(scale_label)
    if match:
        multiplier = 10 ** int(match.group(1))
        if is_per_share:
            return numeric_value * multiplier
        return numeric_value * multiplier / 1_000_000.0

    logging.warning("Unknown EDGAR scale '%s' for series '%s'; treating as raw units", scale, series_key)
    if is_per_share:
        return numeric_value
    return numeric_value / 1_000_000.0


def normalize_edgar_source_value(
    value: Any,
    scale: str | None,
    series_key: str,
    *,
    source: str = "edgar",
    tag: str | None = None,
    year: int | None = None,
    source_ref: Mapping[str, Any] | None = None,
) -> SourceValue:
    return SourceValue(
        normalized_value=(
            None
            if value is None
            else normalize_edgar_value(value, scale, series_key)
        ),
        normalized_unit=normalized_unit_for_concept(series_key),
        raw_value=value,
        raw_scale=None if scale is None else str(scale),
        scale_precedence=scale_precedence(scale),
        source=source,
        tag=tag,
        year=year,
        source_ref=source_ref,
    )


_DECLARED_SCALE_TO_MILLIONS = {
    "unit": 1e-6,
    "thousands": 1e-3,
    "millions": 1.0,
    "billions": 1e3,
}


def source_value_from_edgar_quantity(
    quantity: Quantity,
    *,
    tag: str,
    year: int,
    source_ref: Mapping[str, Any] | None,
    reported_period_start: date | None = None,
    reported_duration_period_type: ReportedPeriodType | None = None,
    source_accepted_at: datetime | None = None,
) -> SourceValue | None:
    """Map producer-owned quantity semantics into model historical units.

    Currency and share-count statement values use the model's established
    millions basis. Per-share facts remain unit scale. Generic counts are not
    assumed to be shares.
    """

    display = quantity.display
    value = float(quantity.value)
    if display.numeric_kind == "currency":
        assert display.currency is not None
        normalized_value = value * _DECLARED_SCALE_TO_MILLIONS[display.scale]
        normalized_unit = NORMALIZED_UNIT_MILLIONS
        fact_unit = f"{display.currency.lower()}_millions"
    elif display.numeric_kind == "per_share":
        assert display.currency is not None
        normalized_value = value
        normalized_unit = NORMALIZED_UNIT_PER_SHARE
        fact_unit = f"{display.currency.lower()}_per_share"
    elif display.numeric_kind == "count" and display.unit_label == "shares":
        normalized_value = value * _DECLARED_SCALE_TO_MILLIONS[display.scale]
        normalized_unit = NORMALIZED_UNIT_MILLIONS
        fact_unit = "shares_millions"
    else:
        return None

    return SourceValue(
        normalized_value=normalized_value,
        normalized_unit=normalized_unit,
        raw_value=quantity.value,
        raw_scale=display.scale,
        scale_precedence=scale_precedence(display.scale),
        source="edgar",
        tag=tag,
        year=year,
        source_ref=source_ref,
        fact_unit=fact_unit,
        reported_period_start=reported_period_start,
        reported_duration_period_type=reported_duration_period_type,
        source_accepted_at=source_accepted_at,
    )


def choose_preferred_source_value(
    existing: SourceValue | None,
    incoming: SourceValue | None,
) -> SourceValue | None:
    """Choose the preferred duplicate source value.

    Non-null normalized values beat null placeholders. Among two real values,
    finer source scale wins. Equal precision intentionally keeps current
    last-write-wins behavior by choosing the incoming value.
    """

    if existing is None:
        return incoming
    if incoming is None:
        return existing
    if incoming.normalized_value is None and existing.normalized_value is not None:
        return existing
    if incoming.normalized_value is not None and existing.normalized_value is None:
        return incoming
    if incoming.scale_precedence < existing.scale_precedence:
        return incoming
    if incoming.scale_precedence == existing.scale_precedence:
        return incoming
    return existing
