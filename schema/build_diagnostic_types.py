"""Result types for build diagnostics."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from .build_diagnostic_values import SEVERITY_ORDER, _collect_severities


@dataclass(frozen=True)
class DiagnosticTolerances:
    bs_balance_abs_m: float = 10.0
    bs_balance_pct: float = 0.001
    bs_subline_gap_pct: float = 0.01
    bs_subline_material_pct: float = 0.10
    is_subtotal_abs_m: float = 1.0
    is_subtotal_pct: float = 0.005
    cf_reconciliation_abs_m: float = 5.0
    cf_reconciliation_pct: float = 0.02
    eps_abs: float = 0.01
    cross_source_material_pct: float = 0.10


@dataclass
class BSBalanceCheck:
    by_year: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class BSSublineCheck:
    by_section: dict[str, dict[str, Any]] = field(default_factory=dict)
    section_role_resolution: dict[str, Any] = field(default_factory=dict)


@dataclass
class ISSubtotalCheck:
    results: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class CFReconciliationCheck:
    net_change_reconciliation: dict[str, Any] = field(
        default_factory=lambda: {"by_year": {}}
    )
    duplicate_concept_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class CoverageSummary:
    total_edgar_sourced: int = 0
    populated: int = 0
    populated_breakdown: dict[str, int] = field(
        default_factory=lambda: {
            "edgar_primary": 0,
            "edgar_fallback": 0,
            "fmp_primary": 0,
            "fmp_fallback": 0,
        }
    )
    missing: list[dict[str, Any]] = field(default_factory=list)
    intentionally_blank: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    synthetic_zero: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class FallbackSummary:
    fallback_engaged_cells: int = 0
    concepts_with_fallback: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class SyntheticZeroCheck:
    items_with_synthetic_zero: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class HistoricalPathCoverageCheck:
    by_section: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


@dataclass
class CrossSourceValidationCheck:
    enabled: bool = False
    by_concept: dict[str, dict[str, Any]] = field(default_factory=dict)
    summary: dict[str, int] = field(
        default_factory=lambda: {
            "concepts_checked": 0,
            "concepts_with_gap": 0,
            "concepts_with_material_gap": 0,
            "cells_compared": 0,
            "cells_incomparable": 0,
        }
    )


@dataclass
class SourceArbitrationCheck:
    enabled: bool = False
    mode: str = "off"
    by_concept: dict[str, dict[str, Any]] = field(default_factory=dict)
    final_source_by_concept_year: dict[str, dict[str, str]] = field(
        default_factory=dict
    )
    summary: dict[str, Any] = field(
        default_factory=lambda: {
            "concepts_checked": 0,
            "cells_decided": 0,
            "cells_would_apply": 0,
            "cells_applied": 0,
            "cells_fail_closed": 0,
            "cells_skipped_superseded": 0,
            "actions": {},
        }
    )


@dataclass
class DiagnosticReport:
    ticker: str
    fiscal_year_end: str
    most_recent_fy: int
    diagnostic_version: int
    generated_at: str
    bs_balance: BSBalanceCheck
    bs_subline_reconciliation: BSSublineCheck
    is_subtotal_integrity: ISSubtotalCheck
    cf_reconciliation: CFReconciliationCheck
    coverage_summary: CoverageSummary
    fallback_summary: FallbackSummary
    synthetic_zero_propagation: SyntheticZeroCheck
    historical_path_coverage: HistoricalPathCoverageCheck
    cross_source_validation: CrossSourceValidationCheck = field(
        default_factory=CrossSourceValidationCheck
    )
    source_arbitration: SourceArbitrationCheck = field(
        default_factory=SourceArbitrationCheck
    )

    def headline_severity(
        self,
    ) -> Literal["ok", "gap", "material_gap", "inconsistency"]:
        payload = asdict(self)
        highest = "ok"
        for severity in _collect_severities(payload):
            if SEVERITY_ORDER[severity] > SEVERITY_ORDER[highest]:
                highest = severity
        return highest


__all__ = [
    "DiagnosticTolerances",
    "BSBalanceCheck",
    "BSSublineCheck",
    "ISSubtotalCheck",
    "CFReconciliationCheck",
    "CoverageSummary",
    "FallbackSummary",
    "SyntheticZeroCheck",
    "HistoricalPathCoverageCheck",
    "CrossSourceValidationCheck",
    "SourceArbitrationCheck",
    "DiagnosticReport",
]
