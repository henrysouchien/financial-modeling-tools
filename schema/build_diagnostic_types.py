"""Result types for build diagnostics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import InitVar, asdict, dataclass, field
from typing import Any, Literal

from .build_diagnostic_values import SEVERITY_ORDER, _collect_severities
from .provider_port.registry import (
    EDGAR_PROVIDER_ID,
    FMP_PROVIDER_ID,
    LEGACY_PROVIDER_IDS,
    ProviderId,
    ProviderRegistry,
    normalize_provider_id,
    require_registered_provider_id,
)


def require_coverage_provider_id(
    value: object,
    *,
    provider_registry: ProviderRegistry | None,
    entry_point: str,
) -> ProviderId:
    """Validate diagnostic keys while retaining canonical empty built-in buckets."""

    provider_id = normalize_provider_id(value)
    return require_registered_provider_id(
        provider_id,
        provider_registry=(
            None if provider_id in LEGACY_PROVIDER_IDS else provider_registry
        ),
        entry_point=entry_point,
    )


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
    # Top-line revenue consolidation reconciliation. 8% sits just above the 5%
    # residual the segment decomposition already tolerates (eliminations /
    # Corporate&Other), so it cannot fire on a legit multi-segment model, while a
    # double-count is +100% — ~12x headroom.
    revenue_consolidation_abs_m: float = 5.0
    revenue_consolidation_pct: float = 0.08


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
    duplicate_role_rows: list[dict[str, Any]] = field(default_factory=list)
    #: Face cash from operations against the modeled components, with other
    #: working capital at its derived leaf identity. Observed, never a block.
    operating_cash_reconciliation: dict[str, Any] = field(
        default_factory=lambda: {"by_year": {}}
    )


@dataclass
class RevenueConsolidationCheck:
    """Reconciles the model's built consolidated top-line revenue against the
    independently-reported EDGAR consolidated entity revenue
    (``segment_profile.total_revenue_check``), per historical year.

    Catches the class of segment mis-seed — e.g. an absorb-blend double-count
    where two revenue nodes each resolve the consolidated total — that otherwise
    builds silently (0 gaps / HIGH confidence) because no existing check
    reconciles the top line: ``is_subtotal_integrity`` starts at gross profit and
    ``cross_source_validation`` compares source-vs-source, never the built
    consolidation output.
    """

    by_year: dict[str, dict[str, Any]] = field(default_factory=dict)

    def severity(self) -> str:
        highest = "ok"
        for entry in self.by_year.values():
            sev = entry.get("severity")
            if sev in SEVERITY_ORDER and SEVERITY_ORDER[sev] > SEVERITY_ORDER[highest]:
                highest = sev
        return highest


@dataclass
class CoverageSummary:
    total_required_roles: int = 0
    populated: int = 0
    populated_breakdown: dict[ProviderId, int] = field(
        default_factory=lambda: {
            EDGAR_PROVIDER_ID: 0,
            FMP_PROVIDER_ID: 0,
        }
    )
    missing: list[dict[str, Any]] = field(default_factory=list)
    intentionally_blank: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    synthetic_zero: list[dict[str, Any]] = field(default_factory=list)
    provider_registry: InitVar[object] = None

    def __post_init__(self, provider_registry: object) -> None:
        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        # Preserve request-scoped extension context without adding it to the
        # dataclass payload emitted by ``asdict``.
        self._provider_registry = registry
        self.populated_breakdown = {
            require_coverage_provider_id(
                provider_id,
                provider_registry=registry,
                entry_point="coverage-summary provider",
            ): int(count)
            for provider_id, count in self.populated_breakdown.items()
        }


def validate_coverage_summary_provider_keys(
    summary: CoverageSummary | Mapping[str, Any],
    *,
    provider_registry: ProviderRegistry | None = None,
) -> None:
    """Revalidate provider-keyed coverage immediately before consumption."""

    breakdown = (
        summary.get("populated_breakdown")
        if isinstance(summary, Mapping)
        else summary.populated_breakdown
    )
    if not isinstance(breakdown, Mapping):
        return
    active_registry = provider_registry
    if active_registry is None:
        active_registry = getattr(summary, "_provider_registry", None)
    for provider_id in breakdown:
        require_coverage_provider_id(
            provider_id,
            provider_registry=active_registry,
            entry_point="coverage-summary provider",
        )


def validate_diagnostic_provider_keys(
    report: object,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> None:
    """Validate every provider-keyed diagnostic map before it is consumed."""

    coverage = (
        report.get("coverage_summary")
        if isinstance(report, Mapping)
        else getattr(report, "coverage_summary", None)
    )
    if coverage is None or not isinstance(coverage, (CoverageSummary, Mapping)):
        return
    validate_coverage_summary_provider_keys(
        coverage,
        provider_registry=provider_registry,
    )



@dataclass
class SyntheticZeroCheck:
    items_with_synthetic_zero: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class HistoricalPathCoverageCheck:
    by_section: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


@dataclass
class CrossSourceValidationCheck:
    enabled: bool = False
    by_role: dict[str, dict[str, Any]] = field(default_factory=dict)
    summary: dict[str, int] = field(
        default_factory=lambda: {
            "roles_checked": 0,
            "roles_with_gap": 0,
            "roles_with_material_gap": 0,
            "cells_compared": 0,
            "cells_incomparable": 0,
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
    synthetic_zero_propagation: SyntheticZeroCheck
    historical_path_coverage: HistoricalPathCoverageCheck
    cross_source_validation: CrossSourceValidationCheck = field(
        default_factory=CrossSourceValidationCheck
    )
    revenue_consolidation: RevenueConsolidationCheck = field(
        default_factory=RevenueConsolidationCheck
    )

    def headline_severity(
        self,
    ) -> Literal["ok", "gap", "material_gap", "inconsistency"]:
        validate_diagnostic_provider_keys(self)
        payload = asdict(self)
        highest_order = SEVERITY_ORDER["ok"]
        for severity in _collect_severities(payload):
            highest_order = max(highest_order, SEVERITY_ORDER[severity])
        severities_by_order: tuple[
            Literal["ok"],
            Literal["gap"],
            Literal["material_gap"],
            Literal["inconsistency"],
        ] = ("ok", "gap", "material_gap", "inconsistency")
        return severities_by_order[highest_order]


__all__ = [
    "DiagnosticTolerances",
    "BSBalanceCheck",
    "BSSublineCheck",
    "ISSubtotalCheck",
    "CFReconciliationCheck",
    "RevenueConsolidationCheck",
    "CoverageSummary",
    "validate_coverage_summary_provider_keys",
    "validate_diagnostic_provider_keys",
    "SyntheticZeroCheck",
    "HistoricalPathCoverageCheck",
    "CrossSourceValidationCheck",
    "DiagnosticReport",
    "require_coverage_provider_id",
]
