"""Build diagnostics for populated financial models."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Literal, Optional

from .models import (
    DataSourceMapping,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    FinancialModel,
    shift_period,
)
from .presentation_tree import PresentationChild, PresentationTree
from .validation_input import ValidationInput

if TYPE_CHECKING:
    from .build import PopulateStats


logger = logging.getLogger(__name__)

DIAGNOSTIC_VERSION = 1
VALID_SEVERITIES = {"ok", "gap", "material_gap", "inconsistency"}
VALID_KINDS = {
    "missing_concept",
    "missing_mapping",
    "wrong_tag_suspected",
    "insufficient_inputs",
    "duplicate_rows",
    "synthetic_zero_propagation",
    "tree_missing_parent",
    "tree_missing_year",
    "unmapped_xbrl_concept",
    "excluded_out_of_section_concept",
    "covered_by_concept",
    "optional_unreported",
    "projection_only",
}
SEVERITY_ORDER = {
    "ok": 0,
    "gap": 1,
    "material_gap": 2,
    "inconsistency": 3,
}
_COVERAGE_FINDING_TOP_N = 5
_CV_CPV_REL_TOL = 1e-9
_CV_CPV_ABS_TOL = 1e-6
_KNOWN_CONTRA_BS_FACE_TAGS: frozenset[str] = frozenset(
    {
        "us-gaap:TreasuryStockCommonValue",
        "us-gaap:TreasuryStockValue",
        "us-gaap:TreasuryStockPreferredValue",
        "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
        "us-gaap:PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAccumulatedDepreciationAndAmortization",
        "us-gaap:AllowanceForDoubtfulAccountsReceivable",
        "us-gaap:AllowanceForDoubtfulAccountsReceivableCurrent",
        "us-gaap:AllowanceForDoubtfulAccountsReceivableNoncurrent",
    }
)
"""BS-face contra concepts used when calc-linkbase sign metadata is unavailable."""


@dataclass(frozen=True)
class SectionMember:
    template_item_id: str
    expected_concept_id: str | None = None


@dataclass(frozen=True)
class ParentCandidate:
    parent: str
    exclude_tags: tuple[str, ...]
    requires_companion: str | None = None
    requires_no_abstract: tuple[str, ...] = ()


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

    def headline_severity(self) -> Literal["ok", "gap", "material_gap", "inconsistency"]:
        payload = asdict(self)
        highest = "ok"
        for severity in _collect_severities(payload):
            if SEVERITY_ORDER[severity] > SEVERITY_ORDER[highest]:
                highest = severity
        return highest


BS_SECTIONS = {
    "current_assets": {
        "sub_lines": [
            SectionMember(
                "tpl.fm.balance_sheet.cash_and_marketable_securities",
                "cash_and_short_term_investments",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.accounts_receivable_net",
                "accounts_receivable",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.current_asset_1",
                "other_current_assets",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.current_asset_2",
                "inventory",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.current_asset_3",
                "prepaid_expenses",
            ),
            SectionMember("tpl.fm.balance_sheet.current_asset_4", None),
        ],
        "pre_subtotal_item_id": "tpl.fm.balance_sheet.total_current_assets_before_funds_held_for_clients",
        "total_item_id": "tpl.fm.balance_sheet.total_current_assets",
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:AssetsCurrentAbstract",
                exclude_tags=("us-gaap:AssetsCurrent",),
            ),
        ],
    },
    "non_current_assets": {
        "sub_lines": [
            SectionMember("tpl.fm.balance_sheet.software_investment_if_applicable", None),
            SectionMember(
                "tpl.fm.balance_sheet.property_and_equipment_net",
                "property_plant_equipment_net",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.operating_lease_right_of_use_assets",
                "operating_lease_right_of_use_assets",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.intangible_assets_net",
                "intangible_assets",
            ),
            SectionMember("tpl.fm.balance_sheet.goodwill", "goodwill"),
            SectionMember(
                "tpl.fm.balance_sheet.long_term_asset_1",
                "marketable_securities_noncurrent",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.long_term_asset_2",
                "other_non_current_assets",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.deferred_income_tax_assets",
                "deferred_tax_assets",
            ),
        ],
        "total_item_id": "tpl.fm.balance_sheet.total_assets",
        "also_includes_subtotal": "tpl.fm.balance_sheet.total_current_assets",
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:AssetsNoncurrentAbstract",
                exclude_tags=("us-gaap:AssetsNoncurrent",),
            ),
            ParentCandidate(
                parent="us-gaap:AssetsAbstract",
                exclude_tags=("us-gaap:Assets",),
                requires_companion="us-gaap:AssetsCurrentAbstract",
            ),
        ],
    },
    "assets_combined": {
        "sub_lines": [],
        "total_item_id": "tpl.fm.balance_sheet.total_assets",
        "presentation_only": True,
        "emit_missing_parent": False,
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:AssetsAbstract",
                exclude_tags=("us-gaap:Assets",),
                requires_no_abstract=("us-gaap:AssetsCurrentAbstract",),
            ),
        ],
    },
    "current_liabilities": {
        "sub_lines": [
            SectionMember("tpl.fm.balance_sheet.accounts_payable", "accounts_payable"),
            SectionMember("tpl.fm.balance_sheet.accrued_expenses", "accrued_expenses"),
            SectionMember(
                "tpl.fm.balance_sheet.current_liability_1",
                "deferred_revenue",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.current_liability_2",
                "other_current_liabilities",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.current_liability_3",
                "commercial_paper",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.current_portion_of_long_term_debt",
                "short_term_debt",
            ),
        ],
        "pre_subtotal_item_id": "tpl.fm.balance_sheet.total_current_liabilities_before_client_fund_obligations",
        "total_item_id": "tpl.fm.balance_sheet.total_current_liabilities",
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:LiabilitiesCurrentAbstract",
                exclude_tags=("us-gaap:LiabilitiesCurrent",),
            ),
        ],
    },
    "non_current_liabilities": {
        "sub_lines": [
            SectionMember(
                "tpl.fm.balance_sheet.long_term_operating_lease_liabilities",
                "lease_liability_noncurrent",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.other_long_term_liabilities",
                "other_non_current_liabilities",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.deferred_income_tax_liabilities",
                "deferred_tax_liabilities",
            ),
            SectionMember("tpl.fm.balance_sheet.long_term_debt", "long_term_debt"),
            SectionMember(
                "tpl.fm.balance_sheet.deferred_revenue_noncurrent",
                "deferred_revenue_noncurrent",
            ),
        ],
        "total_item_id": "tpl.fm.balance_sheet.total_liabilities",
        "also_includes_subtotal": "tpl.fm.balance_sheet.total_current_liabilities",
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:LiabilitiesNoncurrentAbstract",
                exclude_tags=("us-gaap:LiabilitiesNoncurrent",),
            ),
            ParentCandidate(
                parent="us-gaap:LiabilitiesAbstract",
                exclude_tags=("us-gaap:Liabilities",),
                requires_companion="us-gaap:LiabilitiesCurrentAbstract",
            ),
            ParentCandidate(
                parent="us-gaap:LiabilitiesAndStockholdersEquityAbstract",
                exclude_tags=(
                    "us-gaap:Liabilities",
                    "us-gaap:LiabilitiesAndStockholdersEquity",
                ),
                requires_companion="us-gaap:LiabilitiesCurrentAbstract",
            ),
        ],
    },
    "liabilities_combined": {
        "sub_lines": [],
        "total_item_id": "tpl.fm.balance_sheet.total_liabilities",
        "presentation_only": True,
        "emit_missing_parent": False,
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:LiabilitiesAbstract",
                exclude_tags=("us-gaap:Liabilities",),
                requires_no_abstract=("us-gaap:LiabilitiesCurrentAbstract",),
            ),
        ],
    },
    "stockholders_equity": {
        "sub_lines": [
            SectionMember("tpl.fm.balance_sheet.common_stock", None),
            SectionMember(
                "tpl.fm.balance_sheet.additional_paid_in_capital",
                "additional_paid_in_capital",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.retained_earnings",
                "retained_earnings",
            ),
            SectionMember(
                "tpl.fm.balance_sheet.accumulated_other_comprehensive_income_loss",
                "accumulated_other_comprehensive_income",
            ),
        ],
        "total_item_id": "tpl.fm.balance_sheet.total_equity",
        "xbrl_section_parents": [
            ParentCandidate(
                parent="us-gaap:StockholdersEquityAbstract",
                exclude_tags=(
                    "us-gaap:StockholdersEquity",
                    "us-gaap:MinorityInterest",
                    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                ),
            ),
            ParentCandidate(
                parent="us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterestAbstract",
                exclude_tags=(
                    "us-gaap:StockholdersEquity",
                    "us-gaap:MinorityInterest",
                    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                ),
            ),
            ParentCandidate(
                parent="us-gaap:MembersEquityAbstract",
                exclude_tags=(
                    "us-gaap:StockholdersEquity",
                    "us-gaap:MinorityInterest",
                    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                ),
            ),
        ],
    },
}

IS_SECTIONS = {
    "gross_profit": {
        "subtotal_item_id": "tpl.fm.income_statement.gross_profit",
    },
    "operating_income": {
        "subtotal_item_id": "tpl.fm.income_statement.operating_income",
    },
    "pretax_income": {
        "subtotal_item_id": "tpl.fm.income_statement.income_before_income_taxes",
    },
    "net_income": {
        "subtotal_item_id": "tpl.fm.income_statement.net_income",
    },
}

CF_SECTIONS = {
    "net_change_reconciliation": {
        "operating_item_id": "tpl.fm.cash_flow.operating_cash_flow",
        "investing_item_id": "tpl.fm.cash_flow.investing_cash_flow",
        "financing_item_id": "tpl.fm.cash_flow.financing_cash_flow",
        "cash_balance_item_id": "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period",
        "beginning_cash_item_id": "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period",
        "forex_item_id": "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash",
    },
}


def run_build_diagnostic(
    model: FinancialModel,
    *,
    ticker: str,
    fy: int,
    taxonomy: dict[str, DataSourceMapping],
    stats: PopulateStats,
    derivable_items: dict[str, set[int]] | None = None,
    tolerances: DiagnosticTolerances | None = None,
    presentation_tree: PresentationTree | None = None,
    validation_input: ValidationInput | None = None,
) -> DiagnosticReport:
    model.build_index()
    historical_years = _historical_years(model)
    tolerances = tolerances or DiagnosticTolerances()
    reverse_graph = _build_reverse_dependency_graph(model)
    synthetic_zero = _check_synthetic_zero_propagation(
        model,
        historical_years=historical_years,
        reverse_graph=reverse_graph,
    )

    return DiagnosticReport(
        ticker=str(ticker),
        fiscal_year_end=str(
            model.company.fiscal_year_end or model.time_structure.fiscal_year_end or ""
        ),
        most_recent_fy=int(fy),
        diagnostic_version=DIAGNOSTIC_VERSION,
        generated_at=_iso_utc_now(),
        bs_balance=_check_bs_balance(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        bs_subline_reconciliation=_check_bs_subline_reconciliation(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            tolerances=tolerances,
            presentation_tree=presentation_tree,
        ),
        is_subtotal_integrity=_check_is_subtotal_integrity(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        cf_reconciliation=_check_cf_reconciliation(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        coverage_summary=_check_coverage_summary(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            stats=stats,
            derivable_items=derivable_items or {},
            synthetic_zero_check=synthetic_zero,
        ),
        fallback_summary=_fallback_summary_from_stats(stats),
        synthetic_zero_propagation=synthetic_zero,
        historical_path_coverage=_check_historical_path_coverage(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            derivable_items=derivable_items or {},
        ),
        cross_source_validation=_check_cross_source_validation(
            validation_input,
            taxonomy=taxonomy,
            tolerances=tolerances,
        ),
    )


def _check_cross_source_validation(
    validation_input: ValidationInput | None,
    *,
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
) -> CrossSourceValidationCheck:
    if validation_input is None:
        return CrossSourceValidationCheck(enabled=False)

    result = CrossSourceValidationCheck(enabled=True)
    opted_in = sorted({str(concept_id) for concept_id in validation_input.opted_in_concepts})
    historical_years = sorted(int(year) for year in validation_input.historical_years)

    for concept_id in opted_in:
        mapping = taxonomy.get(concept_id)
        if mapping is None or mapping.validation_tolerance_pct is None:
            continue

        tolerance_pct = float(mapping.validation_tolerance_pct)
        concept_payload: dict[str, Any] = {
            "tolerance_pct": tolerance_pct,
            "preferred_source": mapping.preferred_source,
            "headline_severity": "ok",
            "by_year": {},
        }

        for year in historical_years:
            fmp_value = _validation_buffer_value(
                validation_input.fmp_buffer.get(concept_id),
                year,
                ("values", "values_dict"),
            )
            edgar_value = _validation_buffer_value(
                validation_input.edgar_buffer.get(concept_id),
                year,
                ("values_dict", "values"),
            )

            if fmp_value is None and edgar_value is None:
                continue

            served_source = _validation_served_source(validation_input, concept_id, year)
            if fmp_value is None or edgar_value is None:
                concept_payload["by_year"][str(year)] = {
                    "comparison_status": "incomparable",
                    "fmp_value": fmp_value,
                    "edgar_value": edgar_value,
                    "delta": None,
                    "delta_pct": None,
                    "abs_delta_pct": None,
                    "served_source": served_source,
                    "severity": "ok",
                }
                result.summary["cells_incomparable"] += 1
                continue

            delta = float(edgar_value) - float(fmp_value)
            denominator = max(abs(float(edgar_value)), abs(float(fmp_value)))
            if denominator > 0:
                delta_pct = delta / denominator
                abs_delta_pct = abs(delta) / denominator
            else:
                delta_pct = 0.0
                abs_delta_pct = 0.0

            severity = "ok"
            if abs_delta_pct >= tolerances.cross_source_material_pct:
                severity = "material_gap"
            elif abs_delta_pct >= tolerance_pct:
                severity = "gap"

            concept_payload["by_year"][str(year)] = {
                "comparison_status": "compared",
                "fmp_value": float(fmp_value),
                "edgar_value": float(edgar_value),
                "delta": delta,
                "delta_pct": delta_pct,
                "abs_delta_pct": abs_delta_pct,
                "served_source": served_source,
                "severity": severity,
            }
            result.summary["cells_compared"] += 1
            if SEVERITY_ORDER[severity] > SEVERITY_ORDER[concept_payload["headline_severity"]]:
                concept_payload["headline_severity"] = severity

        result.by_concept[concept_id] = concept_payload

    result.summary["concepts_checked"] = len(result.by_concept)
    for payload in result.by_concept.values():
        headline = payload.get("headline_severity")
        if headline == "gap":
            result.summary["concepts_with_gap"] += 1
        elif headline == "material_gap":
            result.summary["concepts_with_material_gap"] += 1

    return result


def _validation_buffer_value(
    fetch_result: Any,
    year: int,
    value_attrs: tuple[str, ...],
) -> float | None:
    if fetch_result is None:
        return None

    for attr in value_attrs:
        values = getattr(fetch_result, attr, None)
        if isinstance(values, dict):
            value = _year_lookup(values, year)
            if value is not None:
                return float(value)

    if isinstance(fetch_result, dict):
        for key in value_attrs:
            values = fetch_result.get(key)
            if isinstance(values, dict):
                value = _year_lookup(values, year)
                if value is not None:
                    return float(value)
        value = _year_lookup(fetch_result, year)
        if value is not None:
            return float(value)

    return None


def _year_lookup(values: dict[Any, Any], year: int) -> Any | None:
    if year in values:
        return values[year]
    return values.get(str(year))


def _validation_served_source(
    validation_input: ValidationInput,
    concept_id: str,
    year: int,
) -> str | None:
    source_by_year = validation_input.served_source_by_concept_year.get(concept_id, {})
    if year in source_by_year:
        return source_by_year[year]
    return source_by_year.get(str(year))  # type: ignore[arg-type]


def _check_bs_balance(
    model: FinancialModel,
    *,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
) -> BSBalanceCheck:
    assets_item = model.get_item("tpl.fm.balance_sheet.total_assets")
    liabilities_item = model.get_item("tpl.fm.balance_sheet.total_liabilities")
    equity_item = model.get_item("tpl.fm.balance_sheet.total_equity")
    result = BSBalanceCheck()
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for year in historical_years:
        assets = _observed_value(model, assets_item, year, value_memo)
        liabilities = _observed_value(model, liabilities_item, year, value_memo)
        equity = _observed_value(model, equity_item, year, value_memo)
        missing = [
            name
            for name, value in (
                ("assets", assets),
                ("liabilities", liabilities),
                ("equity", equity),
            )
            if value is None
        ]
        payload: dict[str, Any] = {
            "assets": assets,
            "liab_plus_equity": None if liabilities is None or equity is None else liabilities + equity,
            "delta": None,
            "delta_pct": None,
            "severity": "ok",
            "kind": None,
        }
        if missing:
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
            liab_plus_equity = float(liabilities) + float(equity)
            delta = float(assets) - liab_plus_equity
            base = max(abs(float(assets)), abs(liab_plus_equity))
            tol = _max_tolerance(base, tolerances.bs_balance_abs_m, tolerances.bs_balance_pct)
            payload["delta"] = delta
            payload["delta_pct"] = _percent(delta, base)
            payload["liab_plus_equity"] = liab_plus_equity
            if abs(delta) > tol:
                payload["severity"] = "inconsistency"
                payload["kind"] = "wrong_tag_suspected"
                payload["inputs"] = {
                    "total_assets": float(assets),
                    "total_liabilities": float(liabilities),
                    "total_equity": float(equity),
                }
        result.by_year[str(year)] = payload
    return result


def _check_bs_subline_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
    presentation_tree: PresentationTree | None = None,
) -> BSSublineCheck:
    if presentation_tree is None:
        return _check_bs_subline_reconciliation_legacy(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            tolerances=tolerances,
        )
    return _check_bs_subline_reconciliation_presentation(
        model,
        historical_years=historical_years,
        taxonomy=taxonomy,
        tolerances=tolerances,
        presentation_tree=presentation_tree,
    )


def _check_bs_subline_reconciliation_legacy(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
) -> BSSublineCheck:
    result = BSSublineCheck()
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for section_name, definition in BS_SECTIONS.items():
        if definition.get("presentation_only"):
            continue
        section_members = _effective_section_members(model, definition)
        section_payload: dict[str, Any] = {"by_year": {}, "coverage_findings": []}

        for year in historical_years:
            findings: list[dict[str, Any]] = []
            sub_lines_sum = 0.0

            for member in section_members:
                item = model.get_item(member.template_item_id)
                expected_concept_id = member.expected_concept_id or item.data_concept_id
                value = _observed_value(model, item, year, value_memo)
                if value is not None:
                    sub_lines_sum += float(value)
                    continue

                finding: dict[str, Any] = {
                    "template_item": member.template_item_id,
                    "expected_concept_id": expected_concept_id,
                    "severity": "gap",
                }
                if expected_concept_id is None:
                    finding["kind"] = "missing_mapping"
                else:
                    finding["kind"] = "missing_concept"
                    mapping = taxonomy.get(expected_concept_id)
                    detail: dict[str, Any] = {}
                    if mapping is not None and mapping.nonadmissible_reason_code is not None:
                        detail["nonadmissible_reason_code"] = str(mapping.nonadmissible_reason_code.value)
                    if detail:
                        finding["detail"] = detail
                findings.append(finding)

            section_total = _resolve_section_total(model, definition, year, value_memo)
            missing_inputs: list[str] = []
            if section_total is None:
                total_item = model.get_item(definition["total_item_id"])
                total_reported_raw = _observed_value(model, total_item, year, value_memo)
                if total_reported_raw is None:
                    missing_inputs.append(total_item.id)
                included_subtotal_id = definition.get("also_includes_subtotal")
                if included_subtotal_id:
                    included_subtotal = model.get_item(included_subtotal_id)
                    included_value = _observed_value(model, included_subtotal, year, value_memo)
                    if total_reported_raw is None or included_value is None:
                        missing_inputs.append(included_subtotal.id)

            payload: dict[str, Any] = {
                "sub_lines_sum": sub_lines_sum,
                "total_reported": section_total,
                "delta": None,
                "delta_pct": None,
                "severity": "ok",
                "kind": None,
                "findings": findings,
            }

            if section_total is None:
                payload["severity"] = "gap"
                payload["kind"] = "insufficient_inputs"
                payload["missing_inputs"] = sorted(set(missing_inputs))
            else:
                delta = float(section_total) - sub_lines_sum
                delta_pct_ratio = _ratio(abs(delta), float(section_total))
                payload["delta"] = delta
                payload["delta_pct"] = delta_pct_ratio * 100.0
                if delta_pct_ratio >= tolerances.bs_subline_material_pct:
                    payload["severity"] = "material_gap"
                elif delta_pct_ratio >= tolerances.bs_subline_gap_pct:
                    payload["severity"] = "gap"
            section_payload["by_year"][str(year)] = payload
        result.by_section[section_name] = section_payload

    return result


def _check_bs_subline_reconciliation_presentation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
    presentation_tree: PresentationTree,
) -> BSSublineCheck:
    result = BSSublineCheck()
    tag_to_concept = _build_taxonomy_tag_index(taxonomy)
    concept_sections = _concept_sections_by_id(model)
    coverage_per_section: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for section_name, definition in BS_SECTIONS.items():
        parent_candidates = tuple(definition.get("xbrl_section_parents", ()))
        if not parent_candidates:
            continue
        section_members = _effective_section_members(model, definition)

        section_payload: dict[str, Any] = {"by_year": {}}

        def parent_present(tag: str) -> bool:
            return bool(presentation_tree.immediate_children_of(tag))

        selected_candidate: ParentCandidate | None = None
        children: tuple[PresentationChild, ...] = ()
        for candidate in parent_candidates:
            if candidate.requires_companion and not parent_present(candidate.requires_companion):
                continue
            if candidate.requires_no_abstract and any(
                parent_present(tag) for tag in candidate.requires_no_abstract
            ):
                continue
            candidate_children = presentation_tree.immediate_children_of(candidate.parent)
            if candidate_children:
                selected_candidate = candidate
                children = candidate_children
                break

        if selected_candidate is None:
            if definition.get("emit_missing_parent", True) is False:
                continue
            for year in historical_years:
                section_payload["by_year"][str(year)] = {
                    "severity": "ok",
                    "kind": "tree_missing_parent",
                    "xbrl_section_parents_tried": [
                        candidate.parent for candidate in parent_candidates
                    ],
                    "sub_lines_sum": None,
                    "total_reported": None,
                    "delta": None,
                    "delta_pct": None,
                    "sign_metadata_notes": [],
                    "findings": [],
                }
            section_payload["coverage_findings"] = []
            result.by_section[section_name] = section_payload
            continue

        exclude_tags = selected_candidate.exclude_tags

        for year in historical_years:
            findings: list[dict[str, Any]] = []
            template_value_memo: dict[tuple[str, int], Optional[float]] = {}

            selected_children = _select_non_overlapping_presentation_children(
                children,
                year=year,
                section_total_tags=exclude_tags,
                definition=definition,
                section_members=section_members,
                tag_to_concept=tag_to_concept,
                model=model,
                template_value_memo=template_value_memo,
                parent_tag=selected_candidate.parent,
            )

            sub_lines_sum = 0.0
            children_with_year_value = 0
            sign_metadata_notes: list[dict] = []
            for child in selected_children:
                concept_id = tag_to_concept.get(child.tag)
                member = _section_member_for_concept(
                    model,
                    section_members,
                    concept_id,
                )
                owning_sections = concept_sections.get(concept_id or "", set())
                if (
                    concept_id is not None
                    and member is None
                    and owning_sections
                    and section_name not in owning_sections
                ):
                    findings.append(
                        {
                            "kind": "excluded_out_of_section_concept",
                            "severity": "ok",
                            "xbrl_tag": child.tag,
                            "expected_concept_id": concept_id,
                            "expected_sections": sorted(owning_sections),
                        }
                    )
                    continue

                contribution = _resolve_signed_contribution(
                    child,
                    year=year,
                    parent_tag=selected_candidate.parent,
                    cv_disagreement_log=sign_metadata_notes,
                )
                if contribution is not None:
                    sub_lines_sum += float(contribution)
                    children_with_year_value += 1

                if concept_id is not None and member is None and contribution is not None:
                    coverage_per_section[section_name].append(
                        {
                            "kind": "unmapped_xbrl_concept",
                            "xbrl_tag": child.tag,
                            "expected_concept_id": concept_id,
                            "year": year,
                            "value_observed": contribution,
                            "severity": "ok",
                        }
                    )
                elif member is not None:
                    item = model.get_item(member.template_item_id)
                    template_value = _observed_value(model, item, year, template_value_memo)
                    if template_value is None and contribution is not None:
                        findings.append(
                            {
                                "template_item": member.template_item_id,
                                "expected_concept_id": concept_id,
                                "kind": "missing_concept",
                                "severity": "gap",
                                "xbrl_tag": child.tag,
                                "value_xbrl": contribution,
                            }
                        )

            if children_with_year_value == 0:
                section_payload["by_year"][str(year)] = {
                    "severity": "ok",
                    "kind": "tree_missing_year",
                    "xbrl_section_parent": selected_candidate.parent,
                    "year": year,
                    "sub_lines_sum": None,
                    "total_reported": None,
                    "delta": None,
                    "delta_pct": None,
                    "sign_metadata_notes": sign_metadata_notes,
                    "findings": findings,
                }
                continue

            total_reported = _resolve_section_total(model, definition, year, template_value_memo)
            payload: dict[str, Any] = {
                "sub_lines_sum": sub_lines_sum,
                "total_reported": total_reported,
                "delta": None,
                "delta_pct": None,
                "severity": "ok",
                "kind": None,
                "sign_metadata_notes": sign_metadata_notes,
                "findings": findings,
            }

            if total_reported is None:
                payload["severity"] = "gap"
                payload["kind"] = "insufficient_inputs"
            else:
                delta = float(total_reported) - sub_lines_sum
                abs_delta_pct = _ratio(abs(delta), float(total_reported))
                payload["delta"] = delta
                payload["delta_pct"] = abs_delta_pct * 100.0
                if abs_delta_pct >= tolerances.bs_subline_material_pct:
                    payload["severity"] = "material_gap"
                elif abs_delta_pct >= tolerances.bs_subline_gap_pct:
                    payload["severity"] = "gap"

            section_payload["by_year"][str(year)] = payload

        section_payload["coverage_findings"] = sorted(
            coverage_per_section[section_name],
            key=lambda finding: abs(finding.get("value_observed") or 0.0),
            reverse=True,
        )[:_COVERAGE_FINDING_TOP_N]
        result.by_section[section_name] = section_payload

    return result


def _values_match_magnitude(a: float, b: float) -> bool:
    return math.isclose(
        abs(float(a)),
        abs(float(b)),
        rel_tol=_CV_CPV_REL_TOL,
        abs_tol=_CV_CPV_ABS_TOL,
    )


def _resolve_signed_contribution(
    child: PresentationChild,
    *,
    year: int,
    parent_tag: str,
    cv_disagreement_log: list[dict] | None = None,
) -> float | None:
    """Return the child's signed contribution to its parent rollup."""

    obs = child.observation_by_year.get(year)
    legacy = child.value_by_year.get(year)

    if obs is None or (
        obs.current_value is None and obs.current_period_value is None
    ):
        return float(legacy) if legacy is not None else None

    cv = obs.current_value
    cpv = obs.current_period_value

    if cv is not None and cpv is not None:
        if _values_match_magnitude(cv, cpv):
            cv_sign = math.copysign(1.0, cv) if cv != 0 else 0.0
            cpv_sign = math.copysign(1.0, cpv) if cpv != 0 else 0.0
            opposite_signs = (
                cv_sign != cpv_sign and cv_sign != 0.0 and cpv_sign != 0.0
            )
            if opposite_signs:
                weight = obs.calculation_weight
                if weight is not None:
                    signed = float(weight) * float(cpv)
                    if not math.isclose(
                        float(cv),
                        signed,
                        rel_tol=_CV_CPV_REL_TOL,
                        abs_tol=_CV_CPV_ABS_TOL,
                    ):
                        if cv_disagreement_log is not None:
                            cv_disagreement_log.append(
                                {
                                    "tag": child.tag,
                                    "year": year,
                                    "parent_tag": parent_tag,
                                    "cv": cv,
                                    "cpv": cpv,
                                    "weight": weight,
                                    "weight_x_cpv": signed,
                                    "note": "cv disagrees with weight*cpv; preferring weight",
                                }
                            )
                    return signed
                return float(cv)

            weight = _resolve_rollup_weight(
                child,
                year=year,
                parent_tag=parent_tag,
            )
            return weight * float(cpv)

        if cv_disagreement_log is not None:
            cv_disagreement_log.append(
                {
                    "tag": child.tag,
                    "year": year,
                    "parent_tag": parent_tag,
                    "cv": cv,
                    "cpv": cpv,
                    "note": "cv/cpv magnitude divergence; using cpv with weight resolution",
                }
            )
        weight = _resolve_rollup_weight(child, year=year, parent_tag=parent_tag)
        return weight * float(cpv)

    if cpv is not None and cv is None:
        weight = _resolve_rollup_weight(child, year=year, parent_tag=parent_tag)
        return weight * float(cpv)

    if cv is not None and cpv is None:
        if cv > 0:
            negated_label = (
                obs.preferred_label_role is not None
                and "negated" in obs.preferred_label_role.lower()
            )
            if child.tag in _KNOWN_CONTRA_BS_FACE_TAGS or negated_label:
                return -abs(float(cv))
        return float(cv)

    return None


def _resolve_rollup_weight(
    child: PresentationChild,
    *,
    year: int,
    parent_tag: str,
) -> float:
    """Determine the calculation-linkbase sign for a child under its parent."""

    _ = parent_tag
    obs = child.observation_by_year.get(year)
    if obs is not None:
        if obs.calculation_weight is not None:
            return float(obs.calculation_weight)
        preferred_label_role = (obs.preferred_label_role or "").lower()
        if "negated" in preferred_label_role:
            return -1.0

    if child.tag in _KNOWN_CONTRA_BS_FACE_TAGS:
        return -1.0
    return 1.0


def _select_non_overlapping_presentation_children(
    children: tuple[PresentationChild, ...],
    *,
    year: int,
    section_total_tags: tuple[str, ...],
    definition: dict[str, Any],
    section_members: list[SectionMember],
    tag_to_concept: dict[str, str],
    model: FinancialModel,
    template_value_memo: dict[tuple[str, int], Optional[float]],
    parent_tag: str,
) -> list[PresentationChild]:
    """Select an additive basis from ordered presentation children.

    Some filers present both leaf concepts and intermediate subtotals as siblings
    under the same balance-sheet section abstract. Reconciliation needs exactly
    one basis: either the template-mapped roll-up or its components, not both.
    """

    selected: list[PresentationChild] = []
    for child in children:
        if any(_same_xbrl_tag(child.tag, tag) for tag in section_total_tags):
            continue
        contribution = _resolve_signed_contribution(
            child,
            year=year,
            parent_tag=parent_tag,
        )
        if contribution is None:
            continue

        rollup_slice = _find_rollup_component_slice(
            selected,
            year,
            float(contribution),
            parent_tag=parent_tag,
        )
        if rollup_slice is None:
            selected.append(child)
            continue

        start, end = rollup_slice
        components = selected[start:end]
        if _prefer_rollup_child(
            child,
            components,
            year=year,
            definition=definition,
            section_members=section_members,
            tag_to_concept=tag_to_concept,
            model=model,
            template_value_memo=template_value_memo,
        ):
            selected = selected[:start] + [child] + selected[end:]

    return selected


def _find_rollup_component_slice(
    selected: list[PresentationChild],
    year: int,
    target_value: float,
    *,
    parent_tag: str,
) -> tuple[int, int] | None:
    if len(selected) < 2:
        return None

    running_sum = 0.0
    for start in range(len(selected) - 1, -1, -1):
        contribution = _resolve_signed_contribution(
            selected[start],
            year=year,
            parent_tag=parent_tag,
        )
        if contribution is None:
            return None
        running_sum += float(contribution)
        if len(selected) - start >= 2 and _presentation_values_equal(running_sum, target_value):
            return (start, len(selected))
    return None


def _prefer_rollup_child(
    child: PresentationChild,
    components: list[PresentationChild],
    *,
    year: int,
    definition: dict[str, Any],
    section_members: list[SectionMember],
    tag_to_concept: dict[str, str],
    model: FinancialModel,
    template_value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    rollup_member = _section_member_for_concept(
        model,
        section_members,
        tag_to_concept.get(child.tag),
    )
    if rollup_member is None:
        return False

    rollup_item = model.get_item(rollup_member.template_item_id)
    if _observed_value(model, rollup_item, year, template_value_memo) is not None:
        return True

    for component in components:
        component_member = _section_member_for_concept(
            model,
            section_members,
            tag_to_concept.get(component.tag),
        )
        if component_member is None:
            continue
        component_item = model.get_item(component_member.template_item_id)
        if _observed_value(model, component_item, year, template_value_memo) is not None:
            return False

    return True


def _presentation_values_equal(left: float, right: float) -> bool:
    tolerance = max(1.0, abs(right) * 0.001)
    return abs(left - right) <= tolerance


def _same_xbrl_tag(left: str | None, right: str | None) -> bool:
    if left is None or right is None:
        return False
    return bool(_taxonomy_tag_keys(left) & _taxonomy_tag_keys(right))


def _build_taxonomy_tag_index(taxonomy: dict[str, DataSourceMapping]) -> dict[str, str]:
    candidates_by_tag: dict[str, list[DataSourceMapping]] = defaultdict(list)
    for concept_id, mapping in taxonomy.items():
        tags = list(mapping.edgar_tags or [])
        if mapping.canonical_tag:
            tags.append(str(mapping.canonical_tag))
        for tag in tags:
            for key in _taxonomy_tag_keys(str(tag)):
                candidates_by_tag[key].append(mapping)

    index: dict[str, str] = {}
    for tag, candidates in candidates_by_tag.items():
        unique_by_concept = {candidate.concept_id: candidate for candidate in candidates}
        unique_candidates = sorted(unique_by_concept.values(), key=lambda candidate: candidate.concept_id)
        edgar_preferred = [
            candidate
            for candidate in unique_candidates
            if str(candidate.preferred_source or "").lower() == "edgar"
        ]
        best_candidates = edgar_preferred or unique_candidates
        if len(best_candidates) > 1:
            logger.warning(
                "Taxonomy EDGAR tag collision for %s; choosing %s from %s",
                tag,
                best_candidates[0].concept_id,
                [candidate.concept_id for candidate in best_candidates],
            )
        index[tag] = best_candidates[0].concept_id
    return index


def _taxonomy_tag_keys(tag: str) -> set[str]:
    cleaned = str(tag).strip()
    if not cleaned:
        return set()
    keys = {cleaned}
    if ":" in cleaned:
        keys.add(cleaned.split(":", 1)[-1])
    else:
        keys.add(f"us-gaap:{cleaned}")
    return keys


def _section_member_for_concept(
    model: FinancialModel,
    members: list[SectionMember],
    concept_id: str | None,
) -> SectionMember | None:
    if concept_id is None:
        return None
    for member in members:
        if member.expected_concept_id == concept_id:
            return member
        item = model.get_item(member.template_item_id)
        if item.data_concept_id == concept_id:
            return member
    return None


def _concept_sections_by_id(model: FinancialModel) -> dict[str, set[str]]:
    sections_by_concept: dict[str, set[str]] = defaultdict(set)
    for section_name, definition in BS_SECTIONS.items():
        if definition.get("presentation_only"):
            continue
        for member in _effective_section_members(model, definition):
            concept_id = member.expected_concept_id
            if concept_id is None:
                item = model.get_item(member.template_item_id)
                concept_id = item.data_concept_id
            if concept_id:
                sections_by_concept[str(concept_id)].add(section_name)
    return dict(sections_by_concept)


def _effective_section_members(
    model: FinancialModel,
    definition: dict[str, Any],
) -> list[SectionMember]:
    members = list(definition["sub_lines"])
    member_ids = {member.template_item_id for member in members}
    if not members:
        return members

    try:
        member_rows = [int(model.get_item(member.template_item_id).row) for member in members]
        total_row = int(model.get_item(definition["total_item_id"]).row)
    except KeyError:
        return members
    min_member_row = min(member_rows)

    for item in _iter_items(model):
        if item.id in member_ids or not item.data_concept_id:
            continue
        if min_member_row <= int(item.row) < total_row:
            members.append(SectionMember(item.id, item.data_concept_id))
            member_ids.add(item.id)
    return sorted(members, key=lambda member: int(model.get_item(member.template_item_id).row))


def _resolve_section_total(
    model: FinancialModel,
    definition: dict[str, Any],
    year: int,
    memo: dict[tuple[str, int], Optional[float]],
) -> float | None:
    total_item = model.get_item(definition["total_item_id"])
    total_reported_raw = _observed_value(model, total_item, year, memo)
    if total_reported_raw is None:
        return None

    included_subtotal_id = definition.get("also_includes_subtotal")
    if not included_subtotal_id:
        return float(total_reported_raw)

    included_subtotal = model.get_item(included_subtotal_id)
    included_value = _observed_value(model, included_subtotal, year, memo)
    if included_value is None:
        return None
    return float(total_reported_raw) - float(included_value)


def _check_is_subtotal_integrity(
    model: FinancialModel,
    *,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
) -> ISSubtotalCheck:
    results: list[dict[str, Any]] = []
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for subtotal_name, definition in IS_SECTIONS.items():
        item = model.get_item(definition["subtotal_item_id"])
        for year in historical_years:
            computed = _evaluate_formula_spec(
                model,
                item.historical,
                year,
                value_memo,
                current_item_id=item.id,
            )
            reported = _observed_value(model, item, year, value_memo)
            entry: dict[str, Any] = {
                "subtotal": subtotal_name,
                "template_item": item.id,
                "year": year,
                "computed": computed,
                "reported": reported,
                "delta": None,
                "delta_pct": None,
                "severity": "ok",
                "kind": None,
            }
            if computed is None or reported is None:
                entry["severity"] = "gap"
                entry["kind"] = "insufficient_inputs"
                entry["missing_inputs"] = _missing_ref_ids(model, item, year, value_memo)
            else:
                delta = float(reported) - float(computed)
                base = max(abs(float(reported)), abs(float(computed)))
                tol = _max_tolerance(base, tolerances.is_subtotal_abs_m, tolerances.is_subtotal_pct)
                entry["delta"] = delta
                entry["delta_pct"] = _percent(delta, base)
                if abs(delta) > tol:
                    entry["severity"] = "inconsistency"
                    entry["kind"] = "wrong_tag_suspected"
                    entry["inputs"] = _collect_subtotal_input_concepts(model, item, year)
            results.append(entry)

    return ISSubtotalCheck(results=results)


def _collect_subtotal_input_concepts(
    model: FinancialModel,
    subtotal_item: LineItem,
    year: int,
) -> dict[str, float]:
    inputs: dict[str, float] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}
    visited: set[tuple[str, int]] = set()

    def _walk(item: LineItem, period: int) -> None:
        key = (item.id, int(period))
        if key in visited:
            return
        visited.add(key)

        spec = _historical_spec(item, int(period))
        refs = _extract_ref_targets(
            spec.params if spec is not None else None,
            period=int(period),
            mode=model.time_structure.period_mode,
        )
        if refs:
            before_count = len(inputs)
            for ref_id, ref_period in refs:
                if ref_period is None:
                    continue
                try:
                    ref_item = model.get_item(ref_id)
                except KeyError:
                    continue
                _walk(ref_item, int(ref_period))
            if len(inputs) > before_count:
                return

        concept_id = getattr(item, "data_concept_id", None)
        if concept_id:
            value = _observed_value(model, item, int(period), value_memo)
            if value is not None:
                inputs[str(concept_id)] = float(value)

    _walk(subtotal_item, int(year))
    return inputs


def _check_cf_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
) -> CFReconciliationCheck:
    definition = CF_SECTIONS["net_change_reconciliation"]
    operating_item = model.get_item(definition["operating_item_id"])
    investing_item = model.get_item(definition["investing_item_id"])
    financing_item = model.get_item(definition["financing_item_id"])
    cash_balance_item = model.get_item(definition["cash_balance_item_id"])
    beginning_cash_item = model.get_item(definition["beginning_cash_item_id"])
    forex_item = None
    if definition.get("forex_item_id"):
        try:
            forex_item = model.get_item(definition["forex_item_id"])
        except KeyError:
            forex_item = None
    value_memo: dict[tuple[str, int], Optional[float]] = {}
    by_year: dict[str, dict[str, Any]] = {}

    for year in historical_years:
        operating = _observed_value(model, operating_item, year, value_memo)
        investing = _observed_value(model, investing_item, year, value_memo)
        financing = _observed_value(model, financing_item, year, value_memo)
        current_cash = _observed_value(model, cash_balance_item, year, value_memo)
        prior_cash = _observed_value(model, cash_balance_item, year - 1, value_memo)
        if prior_cash is None:
            prior_cash = _observed_value(model, beginning_cash_item, year, value_memo)
        forex = (
            _observed_value(model, forex_item, year, value_memo)
            if forex_item is not None
            else None
        )

        payload: dict[str, Any] = {
            "operating": operating,
            "investing": investing,
            "financing": financing,
            "sum": None,
            "reported_net_change": None,
            "delta": None,
            "delta_pct": None,
            "severity": "ok",
            "kind": None,
            "forex_ignored": forex_item is None or forex is None,
        }
        missing = [
            name
            for name, value in (
                ("operating_cash_flow", operating),
                ("investing_cash_flow", investing),
                ("financing_cash_flow", financing),
                ("cash_and_cash_equivalents", current_cash),
                ("prior_cash_and_cash_equivalents", prior_cash),
            )
            if value is None
        ]
        if missing:
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
            sum_value = float(operating) + float(investing) + float(financing)
            if forex is not None:
                sum_value += float(forex)
                payload["forex"] = forex
                payload["forex_ignored"] = False
            reported_net_change = float(current_cash) - float(prior_cash)
            delta = sum_value - reported_net_change
            base = max(abs(sum_value), abs(reported_net_change))
            tol = _max_tolerance(
                base,
                tolerances.cf_reconciliation_abs_m,
                tolerances.cf_reconciliation_pct,
            )
            payload["sum"] = sum_value
            payload["reported_net_change"] = reported_net_change
            payload["delta"] = delta
            payload["delta_pct"] = _percent(delta, base)
            if abs(delta) > tol:
                payload["severity"] = "inconsistency"
                payload["kind"] = "wrong_tag_suspected"
                payload["inputs"] = {
                    "operating_cash_flow": float(operating),
                    "investing_cash_flow": float(investing),
                    "financing_cash_flow": float(financing),
                }
                if forex is not None:
                    payload["inputs"]["effect_of_exchange_rate_on_cash"] = float(forex)
        by_year[str(year)] = payload

    duplicate_rows: list[dict[str, Any]] = []
    concept_groups: dict[str, list[LineItem]] = {}
    for item in _iter_items(model):
        if not item.data_concept_id or not item.id.startswith("tpl.fm.cash_flow."):
            continue
        concept_groups.setdefault(item.data_concept_id, []).append(item)

    for concept_id, items in sorted(concept_groups.items()):
        if len(items) < 2:
            continue
        differing_years: dict[str, dict[str, float]] = {}
        for year in historical_years:
            values: dict[str, float] = {}
            for item in items:
                observed = _observed_value(model, item, year, value_memo)
                if observed is not None:
                    values[item.id] = float(observed)
            if len(values) < 2:
                continue
            if len({round(value, 9) for value in values.values()}) > 1:
                differing_years[str(year)] = values
        if differing_years:
            duplicate_rows.append(
                {
                    "concept_id": concept_id,
                    "template_items": sorted(item.id for item in items),
                    "values_differ": True,
                    "severity": "inconsistency",
                    "kind": "duplicate_rows",
                    "values_by_year": differing_years,
                }
            )

    return CFReconciliationCheck(
        net_change_reconciliation={"by_year": by_year},
        duplicate_concept_rows=duplicate_rows,
    )


def _check_coverage_summary(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    stats: PopulateStats,
    derivable_items: dict[str, set[int]],
    synthetic_zero_check: SyntheticZeroCheck,
) -> CoverageSummary:
    concept_items = _concept_item_map(model)
    synthetic_by_concept = {
        entry["concept"]: entry
        for entry in synthetic_zero_check.items_with_synthetic_zero
        if entry.get("concept")
    }
    populated = 0
    missing: list[dict[str, Any]] = []
    intentionally_blank: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    synthetic_zero: list[dict[str, Any]] = []
    missing_set = {str(concept) for concept in stats.missing_concepts}
    error_set = {str(concept) for concept in (stats.edgar_errors or [])}
    partial_set = {str(concept) for concept in (stats.edgar_partial_failures or [])}
    path_memo: dict[tuple[str, int], bool] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    edgar_concepts = [
        concept_id
        for concept_id, mapping in sorted(taxonomy.items())
        if _is_edgar_sourced(mapping)
    ]

    for concept_id in edgar_concepts:
        mapping = taxonomy[concept_id]
        items = concept_items.get(concept_id, [])
        if concept_id in synthetic_by_concept:
            synthetic_entry = synthetic_by_concept[concept_id]
            synthetic_zero.append(
                {
                    "concept": concept_id,
                    "template_item": synthetic_entry["template_item"],
                    "downstream_impact": synthetic_entry["downstream_impact"],
                }
            )
            continue
        if concept_id in error_set or concept_id in partial_set:
            error_entry = {
                "concept": concept_id,
                "status": "edgar_errors" if concept_id in error_set else "edgar_partial_failures",
                "upstream_semantics": "no data for this (ticker, concept)",
            }
            served_detail = _served_error_detail_from_stats(stats, concept_id)
            if served_detail is not None:
                populated += 1
                error_entry.update(served_detail)
            errors.append(error_entry)
            continue
        if _concept_has_coverage(
            model,
            items,
            historical_years,
            derivable_items,
            path_memo,
            value_memo,
        ):
            populated += 1
            continue

        if concept_id in missing_set or items:
            blank_detail = _intentional_blank_detail(
                model,
                mapping=mapping,
                concept_items=concept_items,
                years=historical_years,
                derivable_items=derivable_items,
                path_memo=path_memo,
                value_memo=value_memo,
            )
            if blank_detail is not None:
                intentionally_blank.append(
                    {
                        "concept": concept_id,
                        **blank_detail,
                    }
                )
                continue

            missing_entry: dict[str, Any] = {
                "concept": concept_id,
                "status": "missing",
                "filer_reports_as": "unknown",
            }
            if mapping.nonadmissible_reason_code is not None:
                missing_entry["nonadmissible_reason_code"] = str(
                    mapping.nonadmissible_reason_code.value
                )
            missing.append(missing_entry)

    return CoverageSummary(
        total_edgar_sourced=len(edgar_concepts),
        populated=populated,
        populated_breakdown=_populated_breakdown_from_stats(stats),
        missing=missing,
        intentionally_blank=intentionally_blank,
        errors=errors,
        synthetic_zero=synthetic_zero,
    )


def _served_error_detail_from_stats(stats: PopulateStats, concept_id: str) -> dict[str, Any] | None:
    entry = getattr(stats, "served_by_breakdown", {}).get(concept_id)
    if entry is None:
        return None

    years_via_primary = list(getattr(entry, "years_via_primary", []) or [])
    years_via_fallback = list(getattr(entry, "years_via_fallback", []) or [])
    if not years_via_primary and not years_via_fallback:
        return None

    detail: dict[str, Any] = {}
    if years_via_primary:
        detail["years_via_primary"] = years_via_primary
    if years_via_fallback:
        detail["years_via_fallback"] = years_via_fallback
        detail["years_recovered"] = years_via_fallback
        primary = getattr(entry, "primary_source", None)
        if primary == "edgar":
            detail["recovered_via"] = "fmp_fallback"
        elif primary == "fmp":
            detail["recovered_via"] = "edgar_fallback"
        else:
            detail["recovered_via"] = "fallback"

    years_unserved = list(getattr(entry, "years_unserved", []) or [])
    if years_unserved:
        detail["years_unserved"] = years_unserved
    return detail


def _populated_breakdown_from_stats(stats: PopulateStats) -> dict[str, int]:
    breakdown = {
        "edgar_primary": 0,
        "edgar_fallback": 0,
        "fmp_primary": 0,
        "fmp_fallback": 0,
    }
    for entry in getattr(stats, "served_by_breakdown", {}).values():
        primary = getattr(entry, "primary_source", None)
        years_via_primary = list(getattr(entry, "years_via_primary", []) or [])
        years_via_fallback = list(getattr(entry, "years_via_fallback", []) or [])
        if primary == "edgar":
            breakdown["edgar_primary"] += len(years_via_primary)
            breakdown["fmp_fallback"] += len(years_via_fallback)
        elif primary == "fmp":
            breakdown["fmp_primary"] += len(years_via_primary)
            breakdown["edgar_fallback"] += len(years_via_fallback)
    return breakdown


def _fallback_summary_from_stats(stats: PopulateStats) -> FallbackSummary:
    concepts: list[dict[str, Any]] = []
    for concept_id, entry in sorted(getattr(stats, "served_by_breakdown", {}).items()):
        years_via_fallback = list(getattr(entry, "years_via_fallback", []) or [])
        if not years_via_fallback:
            continue
        concepts.append(
            {
                "concept_id": concept_id,
                "primary": getattr(entry, "primary_source", None),
                "years_via_primary": list(getattr(entry, "years_via_primary", []) or []),
                "years_via_fallback": years_via_fallback,
                "years_unserved": list(getattr(entry, "years_unserved", []) or []),
            }
        )
    return FallbackSummary(
        fallback_engaged_cells=int(getattr(stats, "fallback_engaged_cells", 0) or 0),
        concepts_with_fallback=concepts,
    )


def _check_synthetic_zero_propagation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    reverse_graph: dict[str, set[str]],
) -> SyntheticZeroCheck:
    findings: list[dict[str, Any]] = []

    for item in _iter_items(model):
        years_synthetic = sorted(
            year
            for year in historical_years
            if item.overrides is not None
            and year in item.overrides
            and _is_synthetic_override(item.overrides[year])
        )
        if not years_synthetic:
            continue
        findings.append(
            {
                "concept": item.data_concept_id,
                "template_item": item.id,
                "years_synthetic_zeroed": years_synthetic,
                "severity": "material_gap",
                "kind": "synthetic_zero_propagation",
                "downstream_impact": sorted(_transitive_downstream_ids(item.id, reverse_graph)),
            }
        )

    findings.sort(key=lambda item: item["template_item"])
    return SyntheticZeroCheck(items_with_synthetic_zero=findings)


def _check_historical_path_coverage(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    derivable_items: dict[str, set[int]],
) -> HistoricalPathCoverageCheck:
    by_section: dict[str, list[dict[str, Any]]] = {}
    section_lookup = _section_lookup(model)
    concept_items = _concept_item_map(model)
    path_memo: dict[tuple[str, int], bool] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for item in _iter_items(model):
        if not item.id.startswith("tpl.fm.") or item.item_type in {ItemType.header, ItemType.spacer}:
            continue
        has_real_value = any(_has_real_value(item, year) for year in historical_years)
        has_path = any(
            _item_has_path(
                model,
                item,
                year,
                derivable_items,
                path_memo,
                set(),
            )
            for year in historical_years
        )
        if has_real_value or has_path:
            continue

        synthetic_years = sorted(
            year
            for year in historical_years
            if item.overrides is not None
            and year in item.overrides
            and _is_synthetic_override(item.overrides[year])
        )
        entry: dict[str, Any] = {
            "template_item": item.id,
            "data_concept_id": item.data_concept_id,
            "years": list(historical_years),
        }
        if synthetic_years:
            entry["severity"] = "material_gap"
            entry["kind"] = "synthetic_zero_propagation"
            entry["years"] = synthetic_years
        elif (
            projection_only_detail := _projection_only_historical_blank_detail(
                item,
                historical_years,
            )
        ) is not None:
            entry.update(projection_only_detail)
        elif item.data_concept_id is None and item.historical is None:
            entry["severity"] = "material_gap"
            entry["kind"] = "missing_mapping"
        elif item.data_concept_id and item.historical is None:
            mapping = taxonomy.get(item.data_concept_id)
            blank_detail = (
                _intentional_blank_detail(
                    model,
                    mapping=mapping,
                    concept_items=concept_items,
                    years=historical_years,
                    derivable_items=derivable_items,
                    path_memo=path_memo,
                    value_memo=value_memo,
                )
                if mapping is not None
                else None
            )
            if blank_detail is not None:
                entry.update(blank_detail)
            else:
                entry["severity"] = "material_gap"
                entry["kind"] = "missing_concept"
        else:
            entry["severity"] = "gap"
            entry["kind"] = "insufficient_inputs"
            entry["blocking_refs"] = _blocking_ref_ids(model, item, historical_years, value_memo)

        section_id = section_lookup.get(item.id, "unassigned")
        by_section.setdefault(section_id, []).append(entry)

    for entries in by_section.values():
        entries.sort(key=lambda entry: entry["template_item"])
    return HistoricalPathCoverageCheck(by_section=by_section)


def _write_diagnostic_log(
    report: DiagnosticReport,
    *,
    log_dir: Path | None = None,
) -> Optional[Path]:
    try:
        base_dir = log_dir or (
            Path(__file__).resolve().parents[1] / "api" / "logs" / "build_diagnostics"
        )
        base_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        ticker = str(report.ticker)
        filename = base_dir / f"{ticker}_{int(report.most_recent_fy)}_{timestamp}.json"
        filename.write_text(json.dumps(asdict(report), indent=2, sort_keys=True) + "\n")

        latest = base_dir / f"{ticker}_latest.json"
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(filename.name)
        return filename
    except Exception:
        logger.exception(
            "Failed to write build diagnostic log for %s fy=%s",
            report.ticker,
            report.most_recent_fy,
        )
        return None


def _is_synthetic_override(spec: FormulaSpec) -> bool:
    return spec.type is FormulaType.constant and spec.note == "synthetic"


def _iter_items(model: FinancialModel) -> Iterable[LineItem]:
    for sheet in model.sheets.values():
        for section in sheet.sections:
            yield from section.line_items


def _section_lookup(model: FinancialModel) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                lookup[item.id] = section.id
    return lookup


def _historical_years(model: FinancialModel) -> list[int]:
    periods = model.time_structure.historical_periods or model.time_structure.historical_years
    return sorted(int(period) for period in periods)


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _collect_severities(obj: Any) -> Iterable[str]:
    if isinstance(obj, dict):
        severity = obj.get("severity")
        if severity in VALID_SEVERITIES:
            yield severity
        for value in obj.values():
            yield from _collect_severities(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _collect_severities(value)


def _max_tolerance(base: float, abs_floor: float, pct_floor: float) -> float:
    return max(float(abs_floor), float(pct_floor) * abs(float(base)))


def _percent(delta: float | None, base: float | None) -> float | None:
    if delta is None or base is None:
        return None
    return _ratio(delta, base) * 100.0


def _ratio(delta: float, base: float) -> float:
    denominator = abs(float(base))
    if denominator < 1e-9:
        return 0.0 if abs(float(delta)) < 1e-9 else 1.0
    return abs(float(delta)) / denominator


def _constant_override_value(spec: FormulaSpec | None) -> Optional[float]:
    if spec is None or spec.type is not FormulaType.constant:
        return None
    value = spec.params.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_real_value(item: LineItem, period: int) -> bool:
    if item.values is not None and int(period) in item.values.values:
        cell = item.values.values[int(period)]
        if cell.value is not None:
            return True
    if item.overrides is not None and int(period) in item.overrides:
        spec = item.overrides[int(period)]
        if not _is_synthetic_override(spec) and _constant_override_value(spec) is not None:
            return True
    return False


def _observed_value(
    model: FinancialModel,
    item: LineItem,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
    stack: set[tuple[str, int]] | None = None,
) -> Optional[float]:
    key = (item.id, int(period))
    if key in memo:
        return memo[key]

    if item.overrides is not None and int(period) in item.overrides:
        override_value = _constant_override_value(item.overrides[int(period)])
        if override_value is not None:
            memo[key] = override_value
            return override_value
    if item.values is not None and int(period) in item.values.values:
        value_cell = item.values.values[int(period)]
        if value_cell.value is not None:
            memo[key] = float(value_cell.value)
            return memo[key]

    spec = _historical_spec(item, int(period))
    if spec is None:
        memo[key] = None
        return None

    active_stack = stack if stack is not None else set()
    if key in active_stack:
        return None
    active_stack.add(key)
    value = _evaluate_formula_spec(
        model,
        spec,
        int(period),
        memo,
        stack=active_stack,
        current_item_id=item.id,
    )
    active_stack.remove(key)
    memo[key] = value
    return value


def _historical_spec(item: LineItem, period: int) -> FormulaSpec | None:
    if item.historical is None:
        return None
    if item.formula_periods is None:
        return item.historical
    active_periods = {int(active_period) for active_period in item.formula_periods}
    if int(period) in active_periods:
        return item.historical
    return None


def _evaluate_formula_spec(
    model: FinancialModel,
    spec: FormulaSpec | None,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
    *,
    stack: set[tuple[str, int]] | None = None,
    current_item_id: str | None = None,
) -> Optional[float]:
    if spec is None:
        return None
    params = spec.params or {}

    if spec.type is FormulaType.constant:
        return _constant_override_value(spec)
    if spec.type is FormulaType.ref:
        value = _evaluate_expr(model, params.get("source"), period, memo, stack=stack)
        if value is None:
            return None
        adjustment = params.get("adjustment")
        if adjustment is not None:
            value += float(adjustment)
        if params.get("negate"):
            value = -value
        return value
    if spec.type is FormulaType.arithmetic:
        if "expr" in params:
            return _evaluate_expr(model, params.get("expr"), period, memo, stack=stack)
        function = params.get("function")
        if function in {"SUM", "AVERAGE"}:
            values = [
                _evaluate_expr(model, expr, period, memo, stack=stack)
                for expr in list(params.get("items", []) or [])
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            return sum(values) / len(values) if function == "AVERAGE" else sum(values)
        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            args = list(operands)
            operator = "+"
            if isinstance(args[0], str) and args[0] in {"+", "-", "*", "/"}:
                operator = args.pop(0)
            values = [
                _evaluate_expr(model, expr, period, memo, stack=stack)
                for expr in args
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if operator == "+":
                return sum(values)
            if operator == "-":
                result = values[0]
                for value in values[1:]:
                    result -= value
                return result
            if operator == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
            if operator == "/":
                result = values[0]
                for value in values[1:]:
                    if abs(value) < 1e-12:
                        return None
                    result /= value
                return result
    if spec.type is FormulaType.ratio:
        numerator = _evaluate_expr(model, params.get("numerator"), period, memo, stack=stack)
        denominator = _evaluate_expr(model, params.get("denominator"), period, memo, stack=stack)
        if numerator is None or denominator is None or abs(denominator) < 1e-12:
            return None
        result = numerator / denominator
        if params.get("subtract_one"):
            result -= 1
        return result
    if spec.type is FormulaType.growth:
        base = _evaluate_expr(model, params.get("base"), period, memo, stack=stack)
        rate = _evaluate_expr(model, params.get("rate"), period, memo, stack=stack)
        if base is None or rate is None:
            return None
        return base * (1.0 + rate)
    if spec.type is FormulaType.driver:
        base = _evaluate_expr(model, params.get("base"), period, memo, stack=stack)
        rate = _evaluate_expr(model, params.get("rate"), period, memo, stack=stack)
        if base is None or rate is None:
            return None
        scale = float(params.get("scale", 1.0))
        return base * rate * scale
    return None


def _evaluate_expr(
    model: FinancialModel,
    expr: Any,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
    *,
    stack: set[tuple[str, int]] | None = None,
) -> Optional[float]:
    if expr is None:
        return None
    if isinstance(expr, bool):
        return float(int(expr))
    if isinstance(expr, (int, float)):
        return float(expr)
    if isinstance(expr, LineItemRef):
        shifted = shift_period(int(period), int(expr.t), model.time_structure.period_mode)
        if shifted is None:
            return None
        try:
            return _observed_value(model, model.get_item(expr.id), int(shifted), memo, stack)
        except KeyError:
            return None
    if isinstance(expr, dict):
        if "id" in expr and isinstance(expr["id"], str):
            try:
                ref_t = int(expr.get("t", 0))
            except (TypeError, ValueError):
                ref_t = 0
            shifted = shift_period(int(period), ref_t, model.time_structure.period_mode)
            if shifted is None:
                return None
            try:
                return _observed_value(model, model.get_item(expr["id"]), int(shifted), memo, stack)
            except KeyError:
                return None
        op = expr.get("op")
        args = list(expr.get("args", []) or [])
        if op in {"SUM", "AVERAGE"}:
            values = [
                _evaluate_expr(model, arg, period, memo, stack=stack)
                for arg in args
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            return sum(values) / len(values) if op == "AVERAGE" else sum(values)
        if op == "+":
            values = [
                _evaluate_expr(model, arg, period, memo, stack=stack)
                for arg in args
            ]
            if any(value is None for value in values):
                return None
            return sum(values)
        if op == "*":
            values = [
                _evaluate_expr(model, arg, period, memo, stack=stack)
                for arg in args
            ]
            if any(value is None for value in values):
                return None
            result = 1.0
            for value in values:
                result *= value
            return result
        if op == "-":
            left = _evaluate_expr(model, expr.get("left"), period, memo, stack=stack)
            right = _evaluate_expr(model, expr.get("right"), period, memo, stack=stack)
            if left is None or right is None:
                return None
            return left - right
        if op == "/":
            left = _evaluate_expr(model, expr.get("left"), period, memo, stack=stack)
            right = _evaluate_expr(model, expr.get("right"), period, memo, stack=stack)
            if left is None or right is None or abs(right) < 1e-12:
                return None
            return left / right
        if op == "NEG":
            value = _evaluate_expr(model, expr.get("arg"), period, memo, stack=stack)
            if value is None:
                return None
            return -value
    return None


def _extract_ref_ids(obj: Any) -> set[str]:
    ids: set[str] = set()
    if obj is None:
        return ids
    if isinstance(obj, LineItemRef):
        return {obj.id}
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], str):
            return {obj["id"]}
        for value in obj.values():
            ids |= _extract_ref_ids(value)
        return ids
    if isinstance(obj, (list, tuple)):
        for value in obj:
            ids |= _extract_ref_ids(value)
    return ids


def _extract_ref_targets(
    obj: Any,
    *,
    period: int,
    mode: str,
) -> list[tuple[str, int | None]]:
    targets: list[tuple[str, int | None]] = []
    if obj is None:
        return targets
    if isinstance(obj, LineItemRef):
        shifted = shift_period(int(period), int(obj.t), mode)
        targets.append((obj.id, None if shifted is None else int(shifted)))
        return targets
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], str):
            try:
                ref_t = int(obj.get("t", 0))
            except (TypeError, ValueError):
                ref_t = 0
            shifted = shift_period(int(period), ref_t, mode)
            targets.append((obj["id"], None if shifted is None else int(shifted)))
            return targets
        for value in obj.values():
            targets.extend(_extract_ref_targets(value, period=period, mode=mode))
        return targets
    if isinstance(obj, (list, tuple)):
        for value in obj:
            targets.extend(_extract_ref_targets(value, period=period, mode=mode))
    return targets


def _missing_ref_ids(
    model: FinancialModel,
    item: LineItem,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
) -> list[str]:
    missing: list[str] = []
    for ref_id in sorted(_extract_ref_ids(item.historical.params if item.historical else None)):
        try:
            ref_item = model.get_item(ref_id)
        except KeyError:
            missing.append(ref_id)
            continue
        if _observed_value(model, ref_item, period, memo) is None:
            missing.append(ref_id)
    return missing


def _build_reverse_dependency_graph(model: FinancialModel) -> dict[str, set[str]]:
    reverse_graph: dict[str, set[str]] = {}
    for item in _iter_items(model):
        refs = _extract_ref_ids(item.historical.params if item.historical else None)
        refs |= _extract_ref_ids(item.projected.params if item.projected else None)
        for ref_id in refs:
            reverse_graph.setdefault(ref_id, set()).add(item.id)
    return reverse_graph


def _transitive_downstream_ids(
    item_id: str,
    reverse_graph: dict[str, set[str]],
) -> set[str]:
    discovered: set[str] = set()
    frontier = sorted(reverse_graph.get(item_id, set()))
    while frontier:
        current = frontier.pop(0)
        if current in discovered:
            continue
        discovered.add(current)
        frontier.extend(sorted(reverse_graph.get(current, set()) - discovered))
    return discovered


def _concept_item_map(model: FinancialModel) -> dict[str, list[LineItem]]:
    concept_items: dict[str, list[LineItem]] = {}
    for item in _iter_items(model):
        if item.data_concept_id:
            concept_items.setdefault(item.data_concept_id, []).append(item)
    return concept_items


def _concept_has_coverage(
    model: FinancialModel,
    items: list[LineItem],
    historical_years: list[int],
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    for item in items:
        for year in historical_years:
            if _item_has_coverage_for_year(
                model,
                item,
                year,
                derivable_items,
                path_memo,
                value_memo,
            ):
                return True
    return False


def _concept_has_full_coverage(
    model: FinancialModel,
    items: list[LineItem],
    historical_years: list[int],
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    if not items or not historical_years:
        return False
    for year in historical_years:
        if not any(
            _item_has_coverage_for_year(
                model,
                item,
                year,
                derivable_items,
                path_memo,
                value_memo,
            )
            for item in items
        ):
            return False
    return True


def _item_has_coverage_for_year(
    model: FinancialModel,
    item: LineItem,
    year: int,
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    if _has_real_value(item, year):
        return True
    if year in derivable_items.get(item.id, set()):
        return True
    if _observed_value(model, item, year, value_memo) is not None and not (
        item.overrides is not None
        and year in item.overrides
        and _is_synthetic_override(item.overrides[year])
    ):
        return True
    return _item_has_path(model, item, year, derivable_items, path_memo, set())


def _intentional_blank_detail(
    model: FinancialModel,
    *,
    mapping: DataSourceMapping,
    concept_items: dict[str, list[LineItem]],
    years: list[int],
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> dict[str, Any] | None:
    covered_by = [
        cover_concept_id
        for cover_concept_id in list(mapping.missing_ok_when_covered_by or [])
        if _concept_has_full_coverage(
            model,
            concept_items.get(cover_concept_id, []),
            years,
            derivable_items,
            path_memo,
            value_memo,
        )
    ]
    if covered_by:
        return {
            "status": "covered_by_concept",
            "severity": "ok",
            "kind": "covered_by_concept",
            "covered_by": covered_by,
        }
    if mapping.optional_if_unreported:
        return {
            "status": "optional_unreported",
            "severity": "ok",
            "kind": "optional_unreported",
        }
    return None


def _projection_only_historical_blank_detail(
    item: LineItem,
    historical_years: list[int],
) -> dict[str, Any] | None:
    if (
        item.data_concept_id is not None
        or item.historical is not None
        or item.projected is None
        or item.formula_periods is None
    ):
        return None

    historical_periods = {int(year) for year in historical_years}
    formula_periods = {int(period) for period in item.formula_periods}
    if not formula_periods or formula_periods & historical_periods:
        return None

    return {
        "status": "projection_only",
        "severity": "ok",
        "kind": "projection_only",
        "formula_periods": sorted(formula_periods),
    }


def _item_has_path(
    model: FinancialModel,
    item: LineItem,
    year: int,
    derivable_items: dict[str, set[int]],
    memo: dict[tuple[str, int], bool],
    stack: set[tuple[str, int]],
) -> bool:
    key = (item.id, int(year))
    if key in memo:
        return memo[key]
    if key in stack:
        return False
    if int(year) in derivable_items.get(item.id, set()):
        memo[key] = True
        return True
    if _has_real_value(item, int(year)):
        memo[key] = True
        return True
    if item.overrides is not None and int(year) in item.overrides and _is_synthetic_override(item.overrides[int(year)]):
        memo[key] = False
        return False
    spec = _historical_spec(item, int(year))
    if spec is None:
        memo[key] = False
        return False
    if spec.type is FormulaType.constant:
        memo[key] = _constant_override_value(spec) is not None
        return memo[key]

    targets = _extract_ref_targets(spec.params, period=int(year), mode=model.time_structure.period_mode)
    if not targets:
        memo[key] = True
        return True

    stack.add(key)
    result = True
    for ref_id, ref_year in targets:
        if ref_year is None:
            result = False
            break
        try:
            ref_item = model.get_item(ref_id)
        except KeyError:
            result = False
            break
        if not _item_has_path(model, ref_item, int(ref_year), derivable_items, memo, stack):
            result = False
            break
    stack.remove(key)
    memo[key] = result
    return result


def _blocking_ref_ids(
    model: FinancialModel,
    item: LineItem,
    historical_years: list[int],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> list[str]:
    blocked: set[str] = set()
    for year in historical_years:
        for ref_id in _extract_ref_ids(item.historical.params if item.historical else None):
            try:
                ref_item = model.get_item(ref_id)
            except KeyError:
                blocked.add(ref_id)
                continue
            if _observed_value(model, ref_item, year, value_memo) is None:
                blocked.add(ref_id)
    return sorted(blocked)


def _is_edgar_sourced(mapping: DataSourceMapping) -> bool:
    return bool(mapping.edgar_tags or mapping.registry_group_id or mapping.canonical_tag)


__all__ = [
    "BS_SECTIONS",
    "CF_SECTIONS",
    "CoverageSummary",
    "BSBalanceCheck",
    "BSSublineCheck",
    "CFReconciliationCheck",
    "CrossSourceValidationCheck",
    "DiagnosticReport",
    "DiagnosticTolerances",
    "FallbackSummary",
    "HistoricalPathCoverageCheck",
    "IS_SECTIONS",
    "ISSubtotalCheck",
    "SectionMember",
    "SyntheticZeroCheck",
    "_is_synthetic_override",
    "_write_diagnostic_log",
    "run_build_diagnostic",
]
