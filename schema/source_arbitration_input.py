"""Diagnostic-only input contract for source-arbitration shadow checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .build_source_arbitration import SourceArbitrationMode


@dataclass
class SourceArbitrationDiagnosticInput:
    mode: SourceArbitrationMode
    fmp_buffer: dict[str, Any] = field(default_factory=dict)
    edgar_buffer: dict[str, Any] = field(default_factory=dict)
    opted_in_concepts: list[str] = field(default_factory=list)
    historical_years: list[int] = field(default_factory=list)
    served_source_by_concept_year: dict[str, dict[int, str]] = field(default_factory=dict)


__all__ = ["SourceArbitrationDiagnosticInput"]
