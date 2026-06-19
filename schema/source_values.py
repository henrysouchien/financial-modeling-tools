"""Source-value normalization and arbitration helpers."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any, Mapping


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


def normalized_unit_for_concept(concept_id: str) -> str:
    return NORMALIZED_UNIT_PER_SHARE if concept_id in PER_SHARE_CONCEPTS else NORMALIZED_UNIT_MILLIONS


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


def normalize_edgar_value(value: Any, scale: str | None, concept_id: str) -> float:
    numeric_value = float(value)
    scale_label = None if scale is None else str(scale).strip().lower()
    is_per_share = concept_id in PER_SHARE_CONCEPTS

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
    if scale_label in {"units", None, ""}:
        if is_per_share:
            return numeric_value
        return numeric_value / 1_000_000.0

    match = _NUMERIC_SCALE_RE.fullmatch(scale_label)
    if match:
        multiplier = 10 ** int(match.group(1))
        if is_per_share:
            return numeric_value * multiplier
        return numeric_value * multiplier / 1_000_000.0

    logging.warning("Unknown EDGAR scale '%s' for concept '%s'; treating as raw units", scale, concept_id)
    if is_per_share:
        return numeric_value
    return numeric_value / 1_000_000.0


def normalize_edgar_source_value(
    value: Any,
    scale: str | None,
    concept_id: str,
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
            else normalize_edgar_value(value, scale, concept_id)
        ),
        normalized_unit=normalized_unit_for_concept(concept_id),
        raw_value=value,
        raw_scale=None if scale is None else str(scale),
        scale_precedence=scale_precedence(scale),
        source=source,
        tag=tag,
        year=year,
        source_ref=source_ref,
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
