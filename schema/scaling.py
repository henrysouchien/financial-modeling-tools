"""Shared scaling helpers for model-build data sources."""

from __future__ import annotations

from .source_values import PER_SHARE_CONCEPTS, normalize_edgar_value

_PER_SHARE_CONCEPTS = set(PER_SHARE_CONCEPTS)


def _edgar_scale_to_millions(value, scale, concept_id: str) -> float:
    return normalize_edgar_value(value, scale, concept_id)
