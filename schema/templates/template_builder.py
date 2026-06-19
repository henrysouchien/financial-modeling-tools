"""Build the checked-in PCTY reference and generic SIA template artifacts."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set

from ..models import (
    BuildStatus,
    CellColor,
    CellStyle,
    CompanyInfo,
    CustomizationType,
    DriverCategory,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ModelMetadata,
    Section,
    SheetLayout,
    SheetType,
    Unit,
)
from ..reader import read_model


PCTY_PATH = Path(
    "/Users/henrychien/Library/CloudStorage/OneDrive-HenryChienLLC/Portfolio/Models/PCTY-model.xlsx"
)
MODEL_TEMPLATE_PATH = Path(
    "/Users/henrychien/Library/CloudStorage/OneDrive-HenryChienLLC/Stock Investor Accelerator/6 - Valuation/Model_template.xlsx"
)
PCTY_REFERENCE_TEMPLATE_PATH = Path(__file__).resolve().parent / "pcty_reference.json"
SIA_GENERIC_TEMPLATE_PATH = Path(__file__).resolve().parent / "sia_generic.json"

KEPT_SHEETS: Sequence[str] = ("Assumptions", "Financial_model")
EXPECTED_ITEM_COUNT = 392
EXPECTED_ITEM_COUNT_GENERIC = 393
EXPECTED_SECTION_COUNTS = {"Assumptions": 18, "Financial_model": 8}
EXPECTED_SHEET_ITEM_COUNTS = {"Assumptions": 227, "Financial_model": 165}
EXPECTED_SHEET_ITEM_COUNTS_GENERIC = {"Assumptions": 226, "Financial_model": 167}


class TemplateRole(str, Enum):
    """How a line item behaves in the template."""

    input = "input"
    derived = "derived"
    header = "header"
    reference = "reference"
    scenario_linked = "scenario_linked"


@dataclass(frozen=True)
class SectionSpec:
    id: str
    label: str
    row_start: int
    row_end: int
    driver_category: DriverCategory


@dataclass(frozen=True)
class TemplateMetadataSpec:
    source_model: str
    notes: str
    company_name: str
    company_ticker: str = "TPL"


PlaceholderInserter = Callable[[FinancialModel], None]


@dataclass(frozen=True)
class TemplateBuildConfig:
    name: str
    source_path: Path
    artifact_path: Path
    section_specs: Dict[str, Sequence[SectionSpec]]
    name_overrides: Dict[str, str]
    expected_item_count: int
    expected_sheet_item_counts: Dict[str, int]
    metadata: TemplateMetadataSpec
    expected_header_dependency_targets: Set[str]
    scenario_linked_ids: Set[str]
    scenario_table_ids: Set[str]
    input_ids: Set[str]
    key_driver_ids: Set[str]
    optional_ids: Set[str]
    template_tokens: Dict[str, str]
    build_notes: Dict[str, str]
    header_dependency_notes: Dict[str, str]
    repeat_group_roles: Dict[str, str]
    data_concept_map: Dict[str, str]
    label_overrides: Dict[str, str] = field(default_factory=dict)
    placeholder_inserters: Sequence[PlaceholderInserter] = field(default_factory=tuple)
    load_kwargs: Dict[str, Any] = field(default_factory=dict)
    clear_cash_flow_artifact_historicals: bool = False
    set_if_applicable_default_zero_historicals: bool = False
    strip_scenario_table_constant_overrides: bool = False
    normalize_cash_flow_projection_links: bool = False
    normalize_valuation_projection_links: bool = False


ASSUMPTIONS_SECTIONS: Sequence[SectionSpec] = (
    SectionSpec("header", "Header", 1, 4, DriverCategory.other),
    SectionSpec("revenue_drivers", "Key Sales Drivers", 5, 28, DriverCategory.revenue),
    SectionSpec("unit_economics", "Unit Economics", 30, 36, DriverCategory.unit_economics),
    SectionSpec("operating_leverage", "Operating Leverage", 38, 58, DriverCategory.cost_structure),
    SectionSpec("adj_ebitda", "Adjusted EBITDA", 61, 76, DriverCategory.cost_structure),
    SectionSpec(
        "depreciation_amortization",
        "D&A",
        78,
        98,
        DriverCategory.reinvestment,
    ),
    SectionSpec(
        "stock_based_compensation",
        "SBC",
        100,
        115,
        DriverCategory.cost_structure,
    ),
    SectionSpec(
        "adj_operating_income",
        "Adj Operating Income",
        117,
        124,
        DriverCategory.cost_structure,
    ),
    SectionSpec(
        "other_income_interest",
        "Other Income & Interest",
        126,
        132,
        DriverCategory.capital_sources,
    ),
    SectionSpec("tax_net_income", "Tax & Net Income", 134, 154, DriverCategory.other),
    SectionSpec(
        "balance_sheet_wc",
        "Working Capital",
        156,
        176,
        DriverCategory.reinvestment,
    ),
    SectionSpec(
        "capital_investments",
        "Capital Investments",
        178,
        197,
        DriverCategory.reinvestment,
    ),
    SectionSpec(
        "intangibles_goodwill",
        "Intangibles & Goodwill",
        199,
        204,
        DriverCategory.reinvestment,
    ),
    SectionSpec("other_assets", "Other Assets", 206, 221, DriverCategory.reinvestment),
    SectionSpec(
        "capital_sources",
        "Capital Sources",
        223,
        235,
        DriverCategory.capital_sources,
    ),
    SectionSpec(
        "dividends_shares",
        "Dividends & Shares",
        237,
        271,
        DriverCategory.capital_sources,
    ),
    SectionSpec("free_cash_flow", "Free Cash Flow", 273, 274, DriverCategory.other),
    SectionSpec("scenario_tables", "Operating Scenarios", 276, 286, DriverCategory.other),
)

FINANCIAL_MODEL_SECTIONS: Sequence[SectionSpec] = (
    SectionSpec("header", "Header", 1, 3, DriverCategory.other),
    SectionSpec("income_statement", "Income Statement", 4, 21, DriverCategory.revenue),
    SectionSpec(
        "adjusted_earnings",
        "Adjusted Earnings",
        22,
        38,
        DriverCategory.cost_structure,
    ),
    SectionSpec("margins", "Margins", 40, 49, DriverCategory.other),
    SectionSpec("growth_rates", "Growth Rates", 51, 61, DriverCategory.other),
    SectionSpec("balance_sheet", "Balance Sheet", 63, 130, DriverCategory.reinvestment),
    SectionSpec("cash_flow", "Cash Flow Statement", 132, 181, DriverCategory.other),
    SectionSpec("fcf_metrics", "FCF Metrics", 183, 187, DriverCategory.other),
)

SECTION_SPECS: Dict[str, Sequence[SectionSpec]] = {
    "Assumptions": ASSUMPTIONS_SECTIONS,
    "Financial_model": FINANCIAL_MODEL_SECTIONS,
}

SECTION_SPECS_PCTY: Dict[str, Sequence[SectionSpec]] = SECTION_SPECS

ASSUMPTIONS_SECTIONS_GENERIC: Sequence[SectionSpec] = (
    SectionSpec("header", "Header", 1, 4, DriverCategory.other),
    SectionSpec("revenue_drivers", "Key Sales Drivers", 5, 28, DriverCategory.revenue),
    SectionSpec("unit_economics", "Unit Economics", 30, 36, DriverCategory.unit_economics),
    SectionSpec("operating_leverage", "Operating Leverage", 38, 58, DriverCategory.cost_structure),
    SectionSpec("adj_ebitda", "Adjusted EBITDA", 61, 76, DriverCategory.cost_structure),
    SectionSpec("depreciation_amortization", "D&A", 78, 98, DriverCategory.reinvestment),
    SectionSpec("stock_based_compensation", "SBC", 100, 114, DriverCategory.cost_structure),
    SectionSpec("adj_operating_income", "Adj Operating Income", 116, 123, DriverCategory.cost_structure),
    SectionSpec("other_income_interest", "Other Income & Interest", 125, 131, DriverCategory.capital_sources),
    SectionSpec("tax_net_income", "Tax & Net Income", 133, 153, DriverCategory.other),
    SectionSpec("balance_sheet_wc", "Working Capital", 155, 175, DriverCategory.reinvestment),
    SectionSpec("capital_investments", "Capital Investments", 177, 196, DriverCategory.reinvestment),
    SectionSpec("intangibles_goodwill", "Intangibles & Goodwill", 198, 203, DriverCategory.reinvestment),
    SectionSpec("other_assets", "Other Assets", 205, 220, DriverCategory.reinvestment),
    SectionSpec("capital_sources", "Capital Sources", 222, 234, DriverCategory.capital_sources),
    SectionSpec("dividends_shares", "Dividends & Shares", 236, 270, DriverCategory.capital_sources),
    SectionSpec("free_cash_flow", "Free Cash Flow", 272, 273, DriverCategory.other),
    SectionSpec("scenario_tables", "Operating Scenarios", 275, 285, DriverCategory.other),
)

FINANCIAL_MODEL_SECTIONS_GENERIC: Sequence[SectionSpec] = (
    SectionSpec("header", "Header", 1, 3, DriverCategory.other),
    SectionSpec("income_statement", "Income Statement", 4, 21, DriverCategory.revenue),
    SectionSpec("adjusted_earnings", "Adjusted Earnings", 22, 38, DriverCategory.cost_structure),
    SectionSpec("margins", "Margins", 40, 49, DriverCategory.other),
    SectionSpec("growth_rates", "Growth Rates", 51, 61, DriverCategory.other),
    SectionSpec("balance_sheet", "Balance Sheet", 63, 130, DriverCategory.reinvestment),
    SectionSpec("cash_flow", "Cash Flow Statement", 132, 185, DriverCategory.other),
    SectionSpec("fcf_metrics", "FCF Metrics", 187, 188, DriverCategory.other),
)

SECTION_SPECS_GENERIC: Dict[str, Sequence[SectionSpec]] = {
    "Assumptions": ASSUMPTIONS_SECTIONS_GENERIC,
    "Financial_model": FINANCIAL_MODEL_SECTIONS_GENERIC,
}

SHEET_PREFIX = {"Assumptions": "tpl.a", "Financial_model": "tpl.fm"}


CANONICAL_NAME_OVERRIDES: Dict[str, str] = {
    "assumptions.m_or_unless_otherwise_stated": "units_header",
    "assumptions.year_ended": "year_header",
    "assumptions.scenario_base_2_bull_1_bear_3": "scenario_selector_label",
    "assumptions.2": "scenario_value",
    "assumptions.client_count": "volume_driver_1",
    "assumptions.y_y_chg": "volume_1_growth",
    "assumptions.employee_average_client_size": "volume_driver_2",
    "assumptions.y_y_chg_r10": "volume_2_growth",
    "assumptions.client_employees": "volume_driver_3",
    "assumptions.y_y_chg_r12": "volume_3_growth",
    "assumptions.recurring_revenue_per_client_employee": "price_driver_1",
    "assumptions.y_y_chg_r14": "price_1_growth",
    "assumptions.revenue_retention": "operating_metric",
    "assumptions.y_y_chg_r16": "operating_metric_growth",
    "assumptions.recurring_and_other_revenue": "business_segment_1_revenue",
    "assumptions.y_y_chg_r18": "business_segment_1_growth",
    "assumptions.funds_held_for_clients_m": "business_segment_2_volume_driver_1",
    "assumptions.y_y_chg_r21": "business_segment_2_volume_growth",
    "assumptions.interest_rate_on_funds_held_for_clients": "business_segment_2_price_driver_1",
    "assumptions.y_y_chg_r23": "business_segment_2_price_growth",
    "assumptions.interest_income_on_funds_held_for_clients": "business_segment_2_revenue",
    "assumptions.y_y_chg_r25": "business_segment_2_growth",
    "assumptions.revenues": "total_revenue",
    "assumptions.y_y_chg_r28": "total_revenue_growth",
    "assumptions.costs_of_goods_sold": "cost_of_goods_sold",
    "assumptions.y_y_chg_r35": "gross_profit_growth",
    "assumptions.sales_and_marketing": "sales_and_marketing_amount",
    "assumptions.research_and_development": "research_and_development_amount",
    "assumptions.general_and_administrative": "general_and_administrative_amount",
    "assumptions.total_operating_expenses": "total_operating_expenses_amount",
    "assumptions.operating_expenses_of_rev": "operating_expenses_pct_revenue_header",
    "assumptions.sales_and_marketing_r46": "sales_and_marketing_pct_revenue",
    "assumptions.research_and_development_r47": "research_and_development_pct_revenue",
    "assumptions.general_and_administrative_r48": "general_and_administrative_pct_revenue",
    "assumptions.total_operating_expenses_r49": "total_operating_expenses_pct_revenue",
    "assumptions.y_y_chg_r52": "operating_income_growth",
    "assumptions.incremental_operating_margin_annual": "incremental_operating_margin_header",
    "assumptions.ebtida": "ebitda",
    "assumptions.adjusted_ebitda_margins": "adjusted_ebitda_margin",
    "assumptions.incremental_adj_ebitda_margin_annual": "incremental_adjusted_ebitda_margin_header",
    "assumptions.beg_property_and_equipment": "beginning_property_and_equipment",
    "assumptions.end_property_and_equipment": "ending_property_and_equipment",
    "assumptions.avg_property_and_equipment": "average_property_and_equipment",
    "assumptions.depreciation_as_of_beginning_property_and_equipment": "depreciation_pct_beginning_property_and_equipment",
    "assumptions.as_of_revenues": "depreciation_pct_revenue",
    "assumptions.chg_y_y": "depreciation_growth",
    "assumptions.beg_capitalized_internal_use_software": "beginning_capitalized_internal_use_software",
    "assumptions.end_capitalized_internal_use_software": "ending_capitalized_internal_use_software",
    "assumptions.avg_capitalized_internal_use_software": "average_capitalized_internal_use_software",
    "assumptions.amortization_of_beginning_capitalized_internal_use_software": "amortization_pct_beginning_capitalized_internal_use_software",
    "assumptions.beg_intangibles": "beginning_intangibles",
    "assumptions.end_intangibles": "ending_intangibles",
    "assumptions.avg_intangibles": "average_intangibles",
    "assumptions.amortization": "amortization_pct_intangibles",
    "assumptions.depreciation_and_amortization_m": "depreciation_and_amortization",
    "assumptions.of_rev": "stock_based_compensation_pct_revenue",
    "assumptions.y_y_chg_r102": "stock_based_compensation_growth",
    "assumptions.cost_of_revenues": "cost_of_revenues_amount",
    "assumptions.sales_and_marketing_r106": "sales_and_marketing_amount",
    "assumptions.research_and_development_r107": "research_and_development_amount",
    "assumptions.general_and_administrative_r108": "general_and_administrative_amount",
    "assumptions.total": "total_amount",
    "assumptions.of_line_items": "pct_of_line_items",
    "assumptions.cost_of_revenues_r112": "cost_of_revenues_pct_line_item",
    "assumptions.sales_and_marketing_r113": "sales_and_marketing_pct_line_item",
    "assumptions.research_and_development_r114": "research_and_development_pct_line_item",
    "assumptions.general_and_administrative_r115": "general_and_administrative_pct_line_item",
    "assumptions.y_y_chg_r118": "adjusted_operating_income_growth",
    "assumptions.adj_operating_margin": "adjusted_operating_margin",
    "assumptions.incremental_adj_operating_margin_annual": "incremental_adjusted_operating_margin_header",
    "assumptions.interest_expense_r128": "interest_expense_assumption",
    "assumptions.debt_beg_of_period": "debt_beginning_of_period",
    "assumptions.debt_end_of_period": "debt_end_of_period",
    "assumptions.average": "average_debt",
    "assumptions.income_before_income_taxes": "pretax_income",
    "assumptions.income_tax_expense_benefit": "income_tax_expense",
    "assumptions.diluted_shares_outstanding": "diluted_shares",
    "assumptions.y_y_chg_r140": "diluted_shares_growth",
    "assumptions.net_income_r143": "net_income_base",
    "assumptions.stock_based_compensation_and_related_payroll_taxes_r144": "stock_based_compensation_and_related_payroll_taxes_adjustment",
    "assumptions.other_items_r146": "other_items_adjustment",
    "assumptions.change_y_y": "adjusted_eps_growth",
    "assumptions.accounts_receivable_net": "accounts_receivable",
    "assumptions.of_revenue": "accounts_receivable_pct_revenue",
    "assumptions.days_sales_outstanding_dso": "days_sales_outstanding",
    "assumptions.of_sales_and_marketing_expenses": "deferred_contract_costs_pct_sales_and_marketing",
    "assumptions.of_cogs_and_sg_a": "prepaid_expenses_and_other_pct_cogs_and_sga",
    "assumptions.of_cogs_and_sg_a_r169": "accounts_payable_pct_cogs_and_sga",
    "assumptions.days_payable_outstanding_dpo": "days_payable_outstanding",
    "assumptions.of_cogs_and_sg_a_r173": "accrued_expenses_pct_cogs_and_sga",
    "assumptions.deferred_revenues": "deferred_revenue",
    "assumptions.of_rev_r176": "deferred_revenue_pct_revenue",
    "assumptions.capitalized_internal_use_software_net": "capitalized_internal_use_software",
    "assumptions.add_capitalized_internal_use_software_costs": "capitalized_internal_use_software_additions",
    "assumptions.of_revenue_r181": "capitalized_internal_use_software_pct_revenue",
    "assumptions.change_y_y_r182": "capitalized_internal_use_software_growth",
    "assumptions.less_amortization": "capitalized_internal_use_software_amortization",
    "assumptions.property_and_equipment_net": "property_and_equipment",
    "assumptions.add_purchases_of_property_and_equipment": "property_and_equipment_additions",
    "assumptions.of_revenue_r187": "property_and_equipment_pct_revenue",
    "assumptions.change_y_y_r188": "property_and_equipment_growth",
    "assumptions.less_depreciation": "property_and_equipment_depreciation",
    "assumptions.research_and_development_r191": "research_and_development",
    "assumptions.capitalized_portion_of_research_and_development": "capitalized_research_and_development",
    "assumptions.expensed_portion_of_research_and_development": "expensed_research_and_development",
    "assumptions.r_d_as_of_revenue": "research_and_development_pct_revenue",
    "assumptions.capitalized_portion_of_r_d_as_of_internal_use_software_costs": "capitalized_research_and_development_pct_internal_use_software",
    "assumptions.intangible_assets_net": "intangible_assets",
    "assumptions.add_acquisitions_of_businesses": "acquisitions_of_businesses",
    "assumptions.less_amortization_for_acquired_intangible_assets": "acquired_intangible_amortization",
    "assumptions.add_less_acquisitions_write_offs": "goodwill_acquisitions_write_offs",
    "assumptions.of_sales_and_marketing_expenses_r208": "long_term_deferred_contract_costs_pct_sales_and_marketing",
    "assumptions.of_general_and_administrative": "long_term_prepaid_expenses_and_other_pct_general_and_administrative",
    "assumptions.of_operating_expense": "deferred_income_tax_assets_pct_operating_expense",
    "assumptions.of_general_and_administrative_r217": "operating_lease_right_of_use_assets_pct_general_and_administrative",
    "assumptions.funds_held_for_client_s_cash_and_cash_equivalents": "funds_held_for_clients_cash_and_cash_equivalents",
    "assumptions.cash_equivalents": "cash_equivalents_pct_funds_held_for_clients",
    "assumptions.change": "debt_change",
    "assumptions.of_general_and_administrative_r229": "long_term_operating_lease_liabilities_pct_general_and_administrative",
    "assumptions.of_general_and_administrative_r232": "other_long_term_liabilities_pct_general_and_administrative",
    "assumptions.of_operating_expense_r235": "deferred_income_tax_liabilities_pct_operating_expense",
    "assumptions.dividend_paid": "dividends_paid",
    "assumptions.no_of_shares": "dividend_share_count",
    "assumptions.y_y_change": "dividend_yoy_change",
    "assumptions.dividend_payout_ratio_of_eps": "dividend_payout_ratio",
    "assumptions.accumulated_other_comprehensive_income_loss": "accumulated_other_comprehensive_income",
    "assumptions.diluted_shares_outstanding_r250": "diluted_shares_outstanding",
    "assumptions.expected_average_share_price_r261": "share_repurchase_average_share_price",
    "assumptions.total_amount": "share_repurchase_total_amount",
    "assumptions.of_shares": "secondary_offering_share_count",
    "assumptions.price_per_share": "secondary_offering_price_per_share",
    "assumptions.gross_proceeds_mil": "secondary_offering_gross_proceeds",
    "assumptions.less_fees": "secondary_offering_fees",
    "assumptions.net_proceeds": "secondary_offering_net_proceeds",
    "assumptions.common_stock_r270": "secondary_offering_common_stock",
    "assumptions.paid_in_capital": "secondary_offering_paid_in_capital",
    "assumptions.as_of_adjusted_ebitda": "free_cash_flow_pct_adjusted_ebitda",
    "assumptions.client_count_growth": "scenario_volume_growth_label",
    "assumptions.bull_case": "volume_bull",
    "assumptions.base_case": "volume_base",
    "assumptions.bear_case": "volume_bear",
    "assumptions.gross_profit_margin": "scenario_margin_label",
    "assumptions.bull_case_r284": "margin_bull",
    "assumptions.base_case_r285": "margin_base",
    "assumptions.bear_case_r286": "margin_bear",
    "financial_model.scenario_base_2_bull_1_bear_3": "scenario_selector_label",
    "financial_model.paylocity_pcty": "company_name_ticker",
    "financial_model.millions_except_per_share_data": "units_header",
    "financial_model.recurring_and_other_revenue": "business_segment_1_revenue",
    "financial_model.interest_income_on_funds_held_for_clients": "business_segment_2_revenue",
    "financial_model.total_revenues": "total_revenue",
    "financial_model.cost_of_goods_sold": "cost_of_goods_sold",
    "financial_model.eps": "eps_diluted",
    "financial_model.diluted_shares_outstanding_m": "diluted_shares",
    "financial_model.net_income_r23": "net_income_base",
    "financial_model.income_tax_expense": "income_tax_expense",
    "financial_model.ebitda": "ebitda",
    "financial_model.stock_based_compensation_and_related_payroll_taxes": "stock_based_compensation_and_related_payroll_taxes_base",
    "financial_model.other_items": "other_items_base",
    "financial_model.stock_based_compensation_and_related_payroll_taxes_r33": "stock_based_compensation_and_related_payroll_taxes_adjustment",
    "financial_model.other_items_r35": "other_items_adjustment",
    "financial_model.of_revenues": "pct_of_revenue",
    "financial_model.recurring_and_other_revenue_r41": "business_segment_1_pct_revenue",
    "financial_model.interest_income_on_funds_held_for_clients_r42": "business_segment_2_pct_revenue",
    "financial_model.adj_ebtida_margin": "adjusted_ebitda_margin",
    "financial_model.revenues_r52": "revenue_growth",
    "financial_model.recurring_and_other_revenue_r53": "business_segment_1_growth",
    "financial_model.interest_income_on_funds_held_for_clients_r54": "business_segment_2_growth",
    "financial_model.total_operating_expenses_r55": "operating_expenses_growth",
    "financial_model.adj_ebitda": "adjusted_ebitda_growth",
    "financial_model.operating_income_r57": "operating_income_growth",
    "financial_model.pretax_income": "pretax_income_growth",
    "financial_model.net_income_r59": "net_income_growth",
    "financial_model.eps_r60": "eps_growth",
    "financial_model.adjusted_eps_r61": "adjusted_eps_growth",
    "financial_model.balance_sheet_m": "balance_sheet_units_header",
    "financial_model.accounts_receivable_net": "accounts_receivable",
    "financial_model.capitalized_internal_use_software_net": "capitalized_internal_use_software",
    "financial_model.property_and_equipment_net": "property_and_equipment",
    "financial_model.intangible_assets_net": "intangible_assets",
    "financial_model.shareholders_equity": "total_equity",
    "financial_model.total_liabilities_and_shareholders_equity": "total_liabilities_and_equity",
    "financial_model.return_on_equity_roe": "return_on_equity",
    "financial_model.return_on_invested_capital_roic": "return_on_invested_capital",
    "financial_model.incremental_operating_margins": "incremental_operating_margin",
    "financial_model.incremental_ebitda_margins": "incremental_ebitda_margin",
    "financial_model.debt_ebitda_ratio": "debt_to_ebitda",
    "financial_model.change": "book_value_per_share_growth",
    "financial_model.change_r122": "net_cash_per_share_growth",
    "financial_model.accumulated_other_comprehensive_income_loss": "accumulated_other_comprehensive_income",
    "financial_model.y_y_change": "free_cash_flow_per_share_growth",
    "financial_model.balance": "balance_check",
    "financial_model.net_income_r132": "net_income",
    "financial_model.depreciation_and_amortization_r134": "depreciation_and_amortization",
    "financial_model.operating_lease_right_of_use_assets_r136": "operating_lease_right_of_use_assets_change",
    "financial_model.long_term_operating_lease_liabilities_r137": "long_term_operating_lease_liabilities_change",
    "financial_model.long_term_deferred_contract_costs_r138": "long_term_deferred_contract_costs_change",
    "financial_model.long_term_prepaid_expenses_and_other_r139": "long_term_prepaid_expenses_and_other_change",
    "financial_model.deferred_income_tax_assets_r140": "deferred_income_tax_assets_change",
    "financial_model.deferred_income_tax_liabilities_r141": "deferred_income_tax_liabilities_change",
    "financial_model.other_long_term_liabilities_r142": "other_long_term_liabilities_change",
    "financial_model.changes_in_operating_assets_and_liabilities": "working_capital_changes",
    "financial_model.accounts_receivable_net_r144": "accounts_receivable_change",
    "financial_model.deferred_contract_costs_r145": "deferred_contract_costs_change",
    "financial_model.prepaid_expenses_and_other_r146": "prepaid_expenses_and_other_change",
    "financial_model.accounts_payable_r147": "accounts_payable_change",
    "financial_model.accrued_expenses_r148": "accrued_expenses_change",
    "financial_model.other_assets_liabilities": "other_assets_and_liabilities_change",
    "financial_model.net_cash_provided_by_operating_activities": "operating_cash_flow",
    "financial_model.acquisitions_of_businesses_net_of_case_acquired": "acquisitions_of_businesses",
    "financial_model.net_cash_provided_by_investing_activities": "investing_cash_flow",
    "financial_model.proceeds_or_payment_for_equity": "equity_proceeds_or_payments",
    "financial_model.net_cash_provided_by_financing_activities": "financing_cash_flow",
    "financial_model.net_change_in_cash_and_cash_equivalents": "net_change_in_cash",
    "financial_model.cash_and_cash_equivalents_beginning_of_period": "cash_beginning_of_period",
    "financial_model.cash_and_cash_equivalents_end_of_period": "cash_end_of_period",
    "financial_model.reconciliation_of_cash_equivalents_and_client_funds": "cash_reconciliation",
    "financial_model.funds_held_for_client_s_cash_and_cash_equivalents": "funds_held_for_clients_cash_and_cash_equivalents",
    "financial_model.cash_and_cash_equivalents": "cash_and_cash_equivalents",
    "financial_model.free_cash_flow_per_share_r181": "free_cash_flow_per_share",
    "financial_model.y_y_change_r182": "free_cash_flow_per_share_growth",
    "financial_model.free_cash_flow_conversion_fcf_ebitda": "free_cash_flow_conversion",
}

CANONICAL_NAME_OVERRIDES_PCTY: Dict[str, str] = CANONICAL_NAME_OVERRIDES

CANONICAL_NAME_OVERRIDES_GENERIC: Dict[str, str] = {
    "assumptions.m_or_unless_otherwise_stated": "units_header",
    "assumptions.year_ended": "year_header",
    "assumptions.scenario_base_2_bull_1_bear_3": "scenario_selector_label",
    "assumptions.2": "scenario_value",
    "assumptions.y_y_chg": "volume_1_growth",
    "assumptions.y_y_chg_r10": "volume_2_growth",
    "assumptions.y_y_chg_r12": "volume_3_growth",
    "assumptions.y_y_chg_r14": "price_1_growth",
    "assumptions.y_y_chg_r16": "operating_metric_growth",
    "assumptions.business_segment_1": "business_segment_1_revenue",
    "assumptions.y_y_chg_r18": "business_segment_1_growth",
    "assumptions.volume_driver_1_r20": "business_segment_2_volume_driver_1",
    "assumptions.y_y_chg_r21": "business_segment_2_volume_growth",
    "assumptions.price_driver_1_r22": "business_segment_2_price_driver_1",
    "assumptions.y_y_chg_r23": "business_segment_2_price_growth",
    "assumptions.business_segment_2": "business_segment_2_revenue",
    "assumptions.y_y_chg_r25": "business_segment_2_growth",
    "assumptions.revenues": "total_revenue",
    "assumptions.y_y_chg_r28": "total_revenue_growth",
    "assumptions.operating_growth": "scenario_volume_growth_label",
    "assumptions.bull_case": "volume_bull",
    "assumptions.base_case": "volume_base",
    "assumptions.bear_case": "volume_bear",
    "assumptions.margin": "scenario_margin_label",
    "assumptions.bull_case_r283": "margin_bull",
    "assumptions.base_case_r284": "margin_base",
    "assumptions.bear_case_r285": "margin_bear",
    "assumptions.sales_and_marketing_r46": "sales_and_marketing_pct_revenue",
    "assumptions.research_and_development_r47": "research_and_development_pct_revenue",
    "assumptions.general_and_administrative_r48": "general_and_administrative_pct_revenue",
    "assumptions.total_operating_expenses_r49": "total_operating_expenses_pct_revenue",
    "assumptions.cost_of_revenues_r111": "cost_of_revenues_pct_line_item",
    "assumptions.sales_and_marketing_r112": "sales_and_marketing_pct_line_item",
    "assumptions.research_and_development_r113": "research_and_development_pct_line_item",
    "assumptions.general_and_administrative_r114": "general_and_administrative_pct_line_item",
    "assumptions.of_cogs_and_sg_a_r168": "accounts_payable_pct_cogs_and_sga",
    "assumptions.of_cogs_and_sg_a_r172": "accrued_expenses_pct_cogs_and_sga",
    "assumptions.of_revenue_r186": "property_and_equipment_pct_revenue",
    "assumptions.change_y_y_r187": "property_and_equipment_growth",
    "assumptions.of_operating_line_adjust_link_as_neccesary_r210": "long_term_asset_2_pct_operating_line",
    "assumptions.of_general_and_administrative_r231": "other_long_term_liabilities_pct_general_and_administrative",
    "assumptions.expected_average_share_price_r260": "share_repurchase_average_share_price",
    "assumptions.common_stock_r269": "secondary_offering_common_stock",
    "financial_model.total_revenues": "total_revenue",
    "financial_model.business_segment_1": "business_segment_1_revenue",
    "financial_model.business_segment_2": "business_segment_2_revenue",
    "financial_model.net_income_r23": "net_income_base",
    "financial_model.stock_based_compensation_and_related_payroll_taxes_r33": "stock_based_compensation_and_related_payroll_taxes_adjustment",
    "financial_model.other_items_r35": "other_items_adjustment",
    "financial_model.business_segment_1_r41": "business_segment_1_pct_revenue",
    "financial_model.business_segment_2_r42": "business_segment_2_pct_revenue",
    "financial_model.business_segment_1_r53": "business_segment_1_growth",
    "financial_model.business_segment_2_r54": "business_segment_2_growth",
    "financial_model.shareholders_equity": "total_equity",
    "financial_model.total_liabilities_and_shareholders_equity": "total_liabilities_and_equity",
    "financial_model.change_r122": "net_cash_per_share_growth",
    "financial_model.net_cash_provided_by_operating_activities": "operating_cash_flow",
    "financial_model.net_cash_provided_by_investing_activities": "investing_cash_flow",
    "financial_model.net_cash_provided_by_financing_activities": "financing_cash_flow",
}

EXPECTED_HEADER_DEPENDENCY_TARGETS: Set[str] = {
    "assumptions.2",
    "assumptions.add_less_acquisitions_write_offs",
    "assumptions.change",
    "assumptions.client_count_growth",
    "assumptions.dividend_paid",
    "assumptions.gross_profit_margin",
    "assumptions.total_amount",
    "financial_model.current_portion_of_long_term_debt",
    "financial_model.long_term_debt",
    "financial_model.net_change_in_client_fund_obligations",
}

SCENARIO_LINKED_IDS: Set[str] = {
    "assumptions.y_y_chg",
    "assumptions.gross_margin",
}

SCENARIO_TABLE_IDS: Set[str] = {
    "assumptions.bull_case",
    "assumptions.base_case",
    "assumptions.bear_case",
    "assumptions.bull_case_r284",
    "assumptions.base_case_r285",
    "assumptions.bear_case_r286",
}

INPUT_IDS: Set[str] = {
    "assumptions.revenue_retention",
    "assumptions.y_y_chg_r10",
    "assumptions.y_y_chg_r14",
    "assumptions.y_y_chg_r21",
    "assumptions.y_y_chg_r23",
    "assumptions.sales_and_marketing_r46",
    "assumptions.research_and_development_r47",
    "assumptions.general_and_administrative_r48",
    "assumptions.depreciation_as_of_beginning_property_and_equipment",
    "assumptions.amortization_of_beginning_capitalized_internal_use_software",
    "assumptions.cost_of_revenues_r112",
    "assumptions.sales_and_marketing_r113",
    "assumptions.research_and_development_r114",
    "assumptions.general_and_administrative_r115",
    "assumptions.interest_expense_r128",
    "assumptions.interest_rate",
    "assumptions.tax_rate",
    "assumptions.y_y_chg_r140",
    "assumptions.effective_tax_rate",
    "assumptions.days_sales_outstanding_dso",
    "assumptions.of_sales_and_marketing_expenses",
    "assumptions.of_cogs_and_sg_a",
    "assumptions.days_payable_outstanding_dpo",
    "assumptions.of_cogs_and_sg_a_r173",
    "assumptions.of_revenue_r181",
    "assumptions.of_revenue_r187",
    "assumptions.of_sales_and_marketing_expenses_r208",
    "assumptions.of_general_and_administrative",
    "assumptions.of_operating_expense",
    "assumptions.of_general_and_administrative_r217",
    "assumptions.cash_equivalents",
    "assumptions.of_general_and_administrative_r229",
    "assumptions.of_general_and_administrative_r232",
    "assumptions.of_operating_expense_r235",
    "assumptions.common_stock",
    "assumptions.accumulated_other_comprehensive_income_loss",
    "assumptions.expected_average_share_price",
    "assumptions.consensus_eps_growth",
    "assumptions.dilutive_shares_weighted_average",
    "assumptions.bull_case",
    "assumptions.base_case",
    "assumptions.bear_case",
    "assumptions.bull_case_r284",
    "assumptions.base_case_r285",
    "assumptions.bear_case_r286",
    "financial_model.corporate_investments",
    "financial_model.acquisitions_of_businesses_net_of_case_acquired",
}

KEY_DRIVER_IDS: Set[str] = {
    "assumptions.y_y_chg",
    "assumptions.y_y_chg_r10",
    "assumptions.y_y_chg_r21",
    "assumptions.gross_margin",
    "assumptions.tax_rate",
    "assumptions.bull_case",
    "assumptions.base_case",
    "assumptions.bear_case",
    "assumptions.bull_case_r284",
    "assumptions.base_case_r285",
    "assumptions.bear_case_r286",
}

OPTIONAL_IDS: Set[str] = {
    "assumptions.revenue_retention",
    "assumptions.deferred_revenues",
    "assumptions.of_rev_r176",
    "assumptions.dividend_payable",
    "assumptions.no_of_shares",
    "assumptions.dividend_per_share",
    "assumptions.y_y_change",
    "assumptions.dividend_payout_ratio_of_eps",
    "assumptions.treasury_stock",
    "assumptions.share_repurchase",
    "assumptions.number_of_shares_repurchased",
    "assumptions.expected_average_share_price_r261",
    "assumptions.total_amount",
    "assumptions.secondary_offering",
    "assumptions.of_shares",
    "assumptions.price_per_share",
    "assumptions.gross_proceeds_mil",
    "assumptions.less_fees",
    "assumptions.net_proceeds",
    "assumptions.common_stock_r270",
    "assumptions.paid_in_capital",
    "financial_model.corporate_investments",
}

TEMPLATE_TOKENS: Dict[str, str] = {
    "assumptions.client_count": "[Volume driver 1]",
    "assumptions.employee_average_client_size": "[Volume driver 2]",
    "assumptions.client_employees": "[Volume driver 3]",
    "assumptions.recurring_revenue_per_client_employee": "[Price driver 1]",
    "assumptions.revenue_retention": "[Operating Metric]",
    "assumptions.recurring_and_other_revenue": "[Business Segment 1]",
    "assumptions.funds_held_for_clients_m": "[Volume driver 1]",
    "assumptions.interest_rate_on_funds_held_for_clients": "[Price driver 1]",
    "assumptions.interest_income_on_funds_held_for_clients": "[Business Segment 2]",
    "assumptions.deferred_contract_costs": "[Current asset 1]",
    "assumptions.prepaid_expenses_and_other": "[Current asset 2]",
    "assumptions.capitalized_internal_use_software_net": "[Software investment]",
    "assumptions.long_term_deferred_contract_costs": "[Long-term asset 1]",
    "assumptions.long_term_prepaid_expenses_and_other": "[Long-term asset 2]",
    "financial_model.paylocity_pcty": "[Company Name] ([TICKER])",
    "financial_model.recurring_and_other_revenue": "[Business Segment 1]",
    "financial_model.interest_income_on_funds_held_for_clients": "[Business Segment 2]",
    "financial_model.deferred_contract_costs": "[Current asset 1]",
    "financial_model.prepaid_expenses_and_other": "[Current asset 2]",
    "financial_model.capitalized_internal_use_software_net": "[Software investment]",
    "financial_model.long_term_deferred_contract_costs": "[Long-term asset 1]",
    "financial_model.long_term_prepaid_expenses_and_other": "[Long-term asset 2]",
}

BUILD_NOTES: Dict[str, str] = {
    "assumptions.client_count": "Map to the primary volume KPI from filings.",
    "assumptions.employee_average_client_size": "Map to the secondary volume KPI if the model needs a second driver.",
    "assumptions.client_employees": "Derived composite volume driver placeholder for the repeatable revenue segment pattern.",
    "assumptions.recurring_revenue_per_client_employee": "Map to the primary price or yield driver for the segment.",
    "assumptions.revenue_retention": "Optional operating metric placeholder. Leave blank if the company has no equivalent KPI.",
    "assumptions.recurring_and_other_revenue": "Rename to the primary business segment line in the modeled company.",
    "assumptions.funds_held_for_clients_m": "Secondary segment volume placeholder. Rename if this segment exists for the company.",
    "assumptions.interest_rate_on_funds_held_for_clients": "Secondary segment price or yield placeholder.",
    "assumptions.interest_income_on_funds_held_for_clients": "Rename to the secondary business segment line if used.",
    "assumptions.y_y_chg": "Scenario-linked via OFFSET formula. Rebuild formula to reference the actual scenario table rows.",
    "assumptions.gross_margin": "Scenario-linked via OFFSET formula. Rebuild formula to reference the actual scenario table rows.",
    "assumptions.interest_rate": "Interest rate assumption. Drop the PCTY historical raw formula and use input or carry-forward logic.",
    "assumptions.amortization_expense_for_acquired_intangible_assets": "Acquired intangible amortization schedule seed. Preserve structural projected overrides.",
    "assumptions.less_amortization_for_acquired_intangible_assets": "Acquired intangible amortization schedule seed. Preserve structural projected overrides.",
    "assumptions.deferred_revenues": "Optional deferred revenue row. Keep only if the company reports the liability.",
    "assumptions.deferred_contract_costs": "Rename to the current asset concept that matches the company if needed.",
    "assumptions.prepaid_expenses_and_other": "Rename to the second current asset concept if needed.",
    "assumptions.capitalized_internal_use_software_net": "Rename if the company uses a different software investment label or omit if not applicable.",
    "assumptions.long_term_deferred_contract_costs": "Rename to the first long-term asset placeholder if needed.",
    "assumptions.long_term_prepaid_expenses_and_other": "Rename to the second long-term asset placeholder if needed.",
    "financial_model.paylocity_pcty": "Replace with the target company name and ticker during template instantiation.",
    "financial_model.recurring_and_other_revenue": "Rename to the primary business segment line.",
    "financial_model.interest_income_on_funds_held_for_clients": "Rename to the secondary business segment line if used.",
    "financial_model.deferred_contract_costs": "Rename to the current asset concept that matches the company if needed.",
    "financial_model.prepaid_expenses_and_other": "Rename to the second current asset concept if needed.",
    "financial_model.capitalized_internal_use_software_net": "Rename if the company uses a different software investment label or omit if not applicable.",
    "financial_model.long_term_deferred_contract_costs": "Rename to the first long-term asset placeholder if needed.",
    "financial_model.long_term_prepaid_expenses_and_other": "Rename to the second long-term asset placeholder if needed.",
}

HEADER_DEPENDENCY_NOTES: Dict[str, str] = {
    "assumptions.2": "Scenario selector value. Default to Base (2) unless a scenario table selection changes it.",
    "assumptions.add_less_acquisitions_write_offs": "Optional goodwill adjustment row. Leave at zero unless acquisitions or write-offs apply.",
    "assumptions.change": "Optional debt change input. Use for borrowing or repayment assumptions.",
    "assumptions.client_count_growth": "Scenario table anchor for volume growth. Rename the label to the active volume driver if needed.",
    "assumptions.dividend_paid": "Optional dividend cash outflow row. Leave at zero if the company does not pay dividends.",
    "assumptions.gross_profit_margin": "Scenario table anchor for margin. Rename the label to the active margin driver if needed.",
    "assumptions.total_amount": "Optional share repurchase cash outflow row. Leave at zero if no buyback is modeled.",
    "financial_model.current_portion_of_long_term_debt": "Optional current debt input. Default to zero if no current maturities exist.",
    "financial_model.long_term_debt": "Optional long-term debt input. Default to zero if the company is debt-free.",
    "financial_model.net_change_in_client_fund_obligations": "Optional financing line. Default to zero if the company has no client fund obligations.",
}

REPEAT_GROUP_ROLES: Dict[str, str] = {
    "assumptions.client_count": "volume",
    "assumptions.y_y_chg": "growth",
    "assumptions.employee_average_client_size": "volume",
    "assumptions.y_y_chg_r10": "growth",
    "assumptions.client_employees": "output",
    "assumptions.y_y_chg_r12": "growth",
    "assumptions.recurring_revenue_per_client_employee": "price",
    "assumptions.y_y_chg_r14": "growth",
    "assumptions.revenue_retention": "metric",
    "assumptions.y_y_chg_r16": "growth",
    "assumptions.recurring_and_other_revenue": "output",
    "assumptions.y_y_chg_r18": "growth",
    "assumptions.funds_held_for_clients_m": "volume",
    "assumptions.y_y_chg_r21": "growth",
    "assumptions.interest_rate_on_funds_held_for_clients": "price",
    "assumptions.y_y_chg_r23": "growth",
    "assumptions.interest_income_on_funds_held_for_clients": "output",
    "assumptions.y_y_chg_r25": "growth",
    "financial_model.recurring_and_other_revenue": "output",
    "financial_model.interest_income_on_funds_held_for_clients": "output",
}

REPEAT_GROUP_IDS: Dict[str, str] = {
    item_id: "revenue_segment" for item_id in REPEAT_GROUP_ROLES
}

CASH_FLOW_ARTIFACT_HISTORICAL_IDS: Set[str] = {
    "financial_model.other_non_cash_adjustments",
    "financial_model.changes_in_operating_assets_and_liabilities",
    "financial_model.other_changes_in_investing_activities",
    "financial_model.proceeds_or_payment_for_debt",
    "financial_model.proceeds_from_employee_stock_purchase_plan",
    "financial_model.other_financing_activities",
}

IF_APPLICABLE_DEFAULT_ZERO_IDS: Set[str] = {
    "financial_model.amortization_of_acquired_intangibles",
    "financial_model.capitalized_software_costs_if_applicable",
    "financial_model.deferred_income_tax_liabilities_r141",
    "financial_model.effect_of_exchange_rate_on_cash",
    "financial_model.funds_held_for_client_s_cash_and_cash_equivalents",
    "financial_model.income_tax_effect_on_adjustments",
    "financial_model.other_cash_flows_from_financing",
    "financial_model.other_items",
    "financial_model.software_investment_if_applicable",
    "financial_model.stock_based_compensation_and_related_payroll_taxes",
}

CASH_FLOW_FULL_PROJECTION_IDS: Set[str] = {
    "tpl.fm.cash_flow.net_income",
    "tpl.fm.cash_flow.other_non_cash_adjustments",
    "tpl.fm.cash_flow.changes_in_operating_assets_and_liabilities",
}

CASH_FLOW_PER_SHARE_PROJECTION_IDS: Set[str] = {
    "tpl.fm.balance_sheet.free_cash_flow_per_share",
    "tpl.fm.cash_flow.free_cash_flow_per_share",
}

CASH_FLOW_NET_INCOME_SOURCE_ID = "tpl.fm.income_statement.net_income"

DATA_CONCEPT_MAP: Dict[str, str] = {
    "financial_model.total_revenues": "revenue",
    "financial_model.cost_of_goods_sold": "cost_of_revenue",
    "financial_model.sales_and_marketing": "sales_and_marketing",
    "financial_model.research_and_development": "research_and_development",
    "financial_model.general_and_administrative": "general_and_administrative",
    "financial_model.total_operating_expenses": "operating_expenses",
    "financial_model.operating_income": "operating_income",
    "financial_model.other_income_expense": "other_income_expense",
    "financial_model.interest_expense": "interest_expense",
    "financial_model.interest_income_on_funds_held_for_clients": "interest_income",
    "financial_model.income_before_income_taxes": "pretax_income",
    "financial_model.income_tax_expense_benefit": "income_tax",
    "financial_model.net_income": "net_income",
    "financial_model.eps": "eps_diluted",
    "financial_model.diluted_shares_outstanding_m": "diluted_shares",
    "financial_model.ebitda": "ebitda",
    "financial_model.depreciation_and_amortization_r134": "depreciation_amortization",
    "financial_model.share_based_compensation": "stock_based_compensation",
    "financial_model.cash_and_cash_equivalents": "cash_and_equivalents",
    "financial_model.cash_and_marketable_securities": "cash_and_short_term_investments",
    "financial_model.accounts_receivable_net": "accounts_receivable",
    "financial_model.inventory": "inventory",
    "financial_model.prepaid_expenses_and_other": "prepaid_expenses",
    "financial_model.deferred_contract_costs": "other_current_assets",
    "financial_model.total_current_assets": "total_current_assets",
    "financial_model.property_and_equipment_net": "property_plant_equipment_net",
    "financial_model.goodwill": "goodwill",
    "financial_model.intangible_assets_net": "intangible_assets",
    "financial_model.other_non_current_assets": "other_non_current_assets",
    "financial_model.total_assets": "total_assets",
    "financial_model.accounts_payable": "accounts_payable",
    "financial_model.accrued_expenses": "accrued_expenses",
    "financial_model.deferred_revenue_current": "deferred_revenue",
    "financial_model.total_current_liabilities": "total_current_liabilities",
    "financial_model.current_portion_of_long_term_debt": "short_term_debt",
    "financial_model.deferred_income_tax_assets": "deferred_tax_assets",
    "financial_model.other_long_term_liabilities": "other_non_current_liabilities",
    "financial_model.deferred_income_tax_liabilities": "deferred_tax_liabilities",
    "financial_model.long_term_debt": "long_term_debt",
    "financial_model.total_liabilities": "total_liabilities",
    "financial_model.shareholders_equity": "total_equity",
    "financial_model.additional_paid_in_capital": "additional_paid_in_capital",
    "financial_model.retained_earnings": "retained_earnings",
    "financial_model.accumulated_other_comprehensive_income_loss": "accumulated_other_comprehensive_income",
    "financial_model.net_cash_provided_by_operating_activities": "operating_cash_flow",
    "financial_model.net_cash_provided_by_investing_activities": "investing_cash_flow",
    "financial_model.net_cash_provided_by_financing_activities": "financing_cash_flow",
    "financial_model.purchases_of_property_and_equipment": "capital_expenditures",
    "financial_model.dividends": "dividends_paid",
    "financial_model.share_repurchases": "share_repurchases",
    "assumptions.end_of_period_shares_outstanding": "shares_outstanding",
    "financial_model.free_cash_flow": "free_cash_flow",
}

EXPECTED_HEADER_DEPENDENCY_TARGETS_PCTY: Set[str] = EXPECTED_HEADER_DEPENDENCY_TARGETS
SCENARIO_LINKED_IDS_PCTY: Set[str] = SCENARIO_LINKED_IDS
SCENARIO_TABLE_IDS_PCTY: Set[str] = SCENARIO_TABLE_IDS
INPUT_IDS_PCTY: Set[str] = INPUT_IDS
KEY_DRIVER_IDS_PCTY: Set[str] = KEY_DRIVER_IDS
OPTIONAL_IDS_PCTY: Set[str] = OPTIONAL_IDS
TEMPLATE_TOKENS_PCTY: Dict[str, str] = TEMPLATE_TOKENS
BUILD_NOTES_PCTY: Dict[str, str] = BUILD_NOTES
HEADER_DEPENDENCY_NOTES_PCTY: Dict[str, str] = HEADER_DEPENDENCY_NOTES
REPEAT_GROUP_ROLES_PCTY: Dict[str, str] = REPEAT_GROUP_ROLES
REPEAT_GROUP_IDS_PCTY: Dict[str, str] = REPEAT_GROUP_IDS
DATA_CONCEPT_MAP_PCTY: Dict[str, str] = DATA_CONCEPT_MAP

EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC: Set[str] = {
    "assumptions.2",
    "assumptions.add_less_acquisitions_write_offs",
    "assumptions.change",
    "assumptions.dividend_paid",
    "assumptions.dividend_payable",
    "assumptions.margin",
    "assumptions.operating_growth",
    "assumptions.total_amount",
    "financial_model.current_portion_of_long_term_debt",
    "financial_model.long_term_debt",
    "financial_model.other_cash_flows_from_financing",
}

SCENARIO_LINKED_IDS_GENERIC: Set[str] = {
    "assumptions.y_y_chg",
    "assumptions.gross_margin",
}

_SCENARIO_SELECTOR_ID = "tpl.a.header.scenario_value"
_COLUMN_OFFSET_MODE_PERIOD_RELATIVE = "period_relative"
_SCENARIO_LINKED_OFFSET_ANCHORS: Dict[str, str] = {
    "tpl.a.revenue_drivers.volume_1_growth": "tpl.a.scenario_tables.scenario_volume_growth_label",
    "tpl.a.unit_economics.gross_margin": "tpl.a.scenario_tables.scenario_margin_label",
}

SCENARIO_TABLE_IDS_GENERIC: Set[str] = {
    "assumptions.bull_case",
    "assumptions.base_case",
    "assumptions.bear_case",
    "assumptions.bull_case_r283",
    "assumptions.base_case_r284",
    "assumptions.bear_case_r285",
}

INPUT_IDS_GENERIC: Set[str] = {
    "assumptions.operating_metric",
    "assumptions.y_y_chg_r10",
    "assumptions.y_y_chg_r14",
    "assumptions.y_y_chg_r21",
    "assumptions.y_y_chg_r23",
    "assumptions.sales_and_marketing_r46",
    "assumptions.research_and_development_r47",
    "assumptions.general_and_administrative_r48",
    "assumptions.depreciation_as_of_beginning_property_and_equipment",
    "assumptions.amortization_of_beginning_capitalized_internal_use_software",
    "assumptions.cost_of_revenues_r111",
    "assumptions.sales_and_marketing_r112",
    "assumptions.research_and_development_r113",
    "assumptions.general_and_administrative_r114",
    "assumptions.interest_expense_r127",
    "assumptions.interest_rate",
    "assumptions.tax_rate",
    "assumptions.y_y_chg_r139",
    "assumptions.effective_tax_rate",
    "assumptions.days_sales_outstanding_dso",
    "assumptions.of_operating_line_adjust_link_as_neccesary",
    "assumptions.of_cogs_and_sg_a",
    "assumptions.days_payable_outstanding_dpo",
    "assumptions.of_cogs_and_sg_a_r172",
    "assumptions.of_revenue_r180",
    "assumptions.of_revenue_r186",
    "assumptions.of_operating_line_adjust_link_as_neccesary_r207",
    "assumptions.of_operating_line_adjust_link_as_neccesary_r210",
    "assumptions.of_operating_expense",
    "assumptions.of_general_and_administrative",
    "assumptions.of_general_and_administrative_r228",
    "assumptions.of_general_and_administrative_r231",
    "assumptions.of_operating_expense_r234",
    "assumptions.common_stock",
    "assumptions.accumulated_other_comprehensive_income_loss",
    "assumptions.expected_average_share_price",
    "assumptions.consensus_eps_growth",
    "assumptions.dilutive_shares_weighted_average",
    "assumptions.bull_case",
    "assumptions.base_case",
    "assumptions.bear_case",
    "assumptions.bull_case_r283",
    "assumptions.base_case_r284",
    "assumptions.bear_case_r285",
    "financial_model.current_asset_3",
    "financial_model.current_asset_4",
    "financial_model.current_liability_1",
    "financial_model.acquisitions_of_businesses",
}

KEY_DRIVER_IDS_GENERIC: Set[str] = {
    "assumptions.y_y_chg",
    "assumptions.y_y_chg_r10",
    "assumptions.y_y_chg_r21",
    "assumptions.gross_margin",
    "assumptions.tax_rate",
    "assumptions.bull_case",
    "assumptions.base_case",
    "assumptions.bear_case",
    "assumptions.bull_case_r283",
    "assumptions.base_case_r284",
    "assumptions.bear_case_r285",
}

OPTIONAL_IDS_GENERIC: Set[str] = {
    "assumptions.deferred_revenues",
    "assumptions.of_rev",
    "assumptions.dividend_payable",
    "assumptions.dividend_paid",
    "assumptions.no_of_shares",
    "assumptions.dividend_per_share",
    "assumptions.y_y_change",
    "assumptions.dividend_payout_ratio_of_eps",
    "assumptions.treasury_stock",
    "assumptions.share_repurchase",
    "assumptions.number_of_shares_repurchased",
    "assumptions.expected_average_share_price_r260",
    "assumptions.total_amount",
    "assumptions.secondary_offering",
    "assumptions.of_shares",
    "assumptions.price_per_share",
    "assumptions.gross_proceeds_mil",
    "assumptions.less_fees",
    "assumptions.net_proceeds",
    "assumptions.common_stock_r269",
    "assumptions.paid_in_capital",
    "financial_model.current_asset_3",
    "financial_model.current_asset_4",
    "financial_model.current_liability_1",
    "financial_model.other_cash_flows_from_financing",
}

TEMPLATE_TOKENS_GENERIC: Dict[str, str] = {
    "assumptions.volume_driver_1": "[Volume driver 1]",
    "assumptions.volume_driver_2": "[Volume driver 2]",
    "assumptions.volume_driver_3": "[Volume driver 3]",
    "assumptions.price_driver_1": "[Price driver 1]",
    "assumptions.operating_metric": "[Operating Metric]",
    "assumptions.business_segment_1": "[Business Segment 1]",
    "assumptions.volume_driver_1_r20": "[Volume driver 1]",
    "assumptions.price_driver_1_r22": "[Price driver 1]",
    "assumptions.business_segment_2": "[Business Segment 2]",
    "assumptions.current_asset_1": "[Current asset 1]",
    "assumptions.current_asset_2": "[Current asset 2]",
    "assumptions.software_investment_if_applicable": "[Software investment, if applicable]",
    "assumptions.long_term_asset_1": "[Long-term asset 1]",
    "assumptions.long_term_asset_2": "[Long-term asset 2]",
    "financial_model.company_name_ticker": "[Company Name] ([TICKER])",
    "financial_model.business_segment_1": "[Business Segment 1]",
    "financial_model.business_segment_2": "[Business Segment 2]",
    "financial_model.current_asset_1": "[Current asset 1]",
    "financial_model.current_asset_2": "[Current asset 2]",
    "financial_model.current_asset_3": "[Current asset 3]",
    "financial_model.current_asset_4": "[Current asset 4]",
    "financial_model.current_liability_1": "[Current Liability 1]",
    "financial_model.software_investment_if_applicable": "[Software investment, if applicable]",
    "financial_model.long_term_asset_1": "[Long-term asset 1]",
    "financial_model.long_term_asset_2": "[Long-term asset 2]",
    "financial_model.other_cash_flows_from_financing": "[Other cash flows from financing]",
}

BUILD_NOTES_GENERIC: Dict[str, str] = {
    "assumptions.volume_driver_1": "Map to the primary volume KPI from filings.",
    "assumptions.volume_driver_2": "Map to the secondary volume KPI if the model needs a second driver.",
    "assumptions.volume_driver_3": "Derived composite volume driver placeholder for the repeatable revenue segment pattern.",
    "assumptions.price_driver_1": "Map to the primary price or yield driver for the segment.",
    "assumptions.operating_metric": "Optional operating metric placeholder. Leave blank if the company has no equivalent KPI.",
    "assumptions.business_segment_1": "Rename to the primary business segment line in the modeled company.",
    "assumptions.volume_driver_1_r20": "Secondary segment volume placeholder. Rename if this segment exists for the company.",
    "assumptions.price_driver_1_r22": "Secondary segment price or yield placeholder.",
    "assumptions.business_segment_2": "Rename to the secondary business segment line if used.",
    "assumptions.y_y_chg": "Scenario-linked via OFFSET formula. Rebuild formula to reference the actual scenario table rows.",
    "assumptions.gross_margin": "Scenario-linked via OFFSET formula. Rebuild formula to reference the actual scenario table rows.",
    "assumptions.interest_rate": "Interest rate assumption. Use input or carry-forward logic.",
    "assumptions.current_asset_1": "Rename to the first current asset concept if needed.",
    "assumptions.current_asset_2": "Rename to the second current asset concept if needed.",
    "assumptions.software_investment_if_applicable": "Rename if the company uses a different software investment label or omit if not applicable.",
    "assumptions.long_term_asset_1": "Rename to the first long-term asset placeholder if needed.",
    "assumptions.long_term_asset_2": "Rename to the second long-term asset placeholder if needed.",
    "financial_model.company_name_ticker": "Replace with the target company name and ticker during template instantiation.",
    "financial_model.business_segment_1": "Rename to the primary business segment line.",
    "financial_model.business_segment_2": "Rename to the secondary business segment line if used.",
    "financial_model.current_asset_1": "Rename to the first current asset concept if needed.",
    "financial_model.current_asset_2": "Rename to the second current asset concept if needed.",
    "financial_model.current_asset_3": "Optional current asset input. Leave blank unless the company needs it.",
    "financial_model.current_asset_4": "Optional post-subtotal current asset input for company-specific current assets.",
    "financial_model.current_liability_1": "Optional current liability input. Use for deferred revenue or another current liability if needed.",
    "financial_model.software_investment_if_applicable": "Rename if the company uses a different software investment label or omit if not applicable.",
    "financial_model.long_term_asset_1": "Rename to the first long-term asset placeholder if needed.",
    "financial_model.long_term_asset_2": "Rename to the second long-term asset placeholder if needed.",
    "financial_model.other_cash_flows_from_financing": "Optional financing catch-all row. Leave at zero unless needed.",
}

LABEL_OVERRIDES_GENERIC: Dict[str, str] = {
    "assumptions.amortization_of_capitalized_internal_use_software": "Amortization of capitalized software",
    "assumptions.beg_capitalized_internal_use_software": "  Beg: Capitalized software",
    "assumptions.end_capitalized_internal_use_software": "  End: Capitalized software",
    "assumptions.avg_capitalized_internal_use_software": "  Avg. Capitalized software",
    "assumptions.amortization_of_beginning_capitalized_internal_use_software": "   % Amortization of beginning capitalized software",
    "assumptions.capitalized_portion_of_r_d_as_of_internal_use_software_costs": "Capitalized portion of R&D as % of capitalized software costs",
    "assumptions.funds_held_for_clients": "Optional current asset bridge",
    "assumptions.funds_held_for_client_s_cash_and_cash_equivalents": "Optional cash bridge adjustment",
    "financial_model.total_current_assets_before_funds_held_for_clients": "Subtotal current assets",
    "financial_model.total_current_liabilities_before_client_fund_obligations": "Subtotal current liabilities",
    "financial_model.current_asset_2_r146": " Change in inventory",
    "financial_model.long_term_asset_1": " Long-term marketable securities",
    "financial_model.long_term_operating_lease_liabilities": " Long-term lease liabilities",
    "financial_model.reconciliation_of_cash_equivalents_and_client_funds": "Reconciliation of cash and equivalents",
    "financial_model.funds_held_for_client_s_cash_and_cash_equivalents": " Optional cash bridge adjustment",
}

HEADER_DEPENDENCY_NOTES_GENERIC: Dict[str, str] = {
    "assumptions.2": "Scenario selector value. Default to Base (2) unless a scenario table selection changes it.",
    "assumptions.add_less_acquisitions_write_offs": "Optional goodwill adjustment row. Leave at zero unless acquisitions or write-offs apply.",
    "assumptions.change": "Optional debt change input. Use for borrowing or repayment assumptions.",
    "assumptions.dividend_payable": "Optional dividend payable row. Leave at zero if the company does not use it.",
    "assumptions.dividend_paid": "Optional dividend cash outflow row. Leave at zero if the company does not pay dividends.",
    "assumptions.margin": "Scenario table anchor for margin. Rename the label to the active margin driver if needed.",
    "assumptions.operating_growth": "Scenario table anchor for volume growth. Rename the label to the active volume driver if needed.",
    "assumptions.total_amount": "Optional share repurchase cash outflow row. Leave at zero if no buyback is modeled.",
    "financial_model.current_portion_of_long_term_debt": "Optional current debt input. Default to zero if no current maturities exist.",
    "financial_model.long_term_debt": "Optional long-term debt input. Default to zero if the company is debt-free.",
    "financial_model.other_cash_flows_from_financing": "Optional financing catch-all row. Default to zero unless the company needs it.",
}

REPEAT_GROUP_ROLES_GENERIC: Dict[str, str] = {
    "assumptions.volume_driver_1": "volume",
    "assumptions.y_y_chg": "growth",
    "assumptions.volume_driver_2": "volume",
    "assumptions.y_y_chg_r10": "growth",
    "assumptions.volume_driver_3": "output",
    "assumptions.y_y_chg_r12": "growth",
    "assumptions.price_driver_1": "price",
    "assumptions.y_y_chg_r14": "growth",
    "assumptions.operating_metric": "metric",
    "assumptions.y_y_chg_r16": "growth",
    "assumptions.business_segment_1": "output",
    "assumptions.y_y_chg_r18": "growth",
    "assumptions.volume_driver_1_r20": "volume",
    "assumptions.y_y_chg_r21": "growth",
    "assumptions.price_driver_1_r22": "price",
    "assumptions.y_y_chg_r23": "growth",
    "assumptions.business_segment_2": "output",
    "assumptions.y_y_chg_r25": "growth",
    "financial_model.business_segment_1": "output",
    "financial_model.business_segment_2": "output",
}

REPEAT_GROUP_IDS_GENERIC: Dict[str, str] = {
    item_id: "revenue_segment" for item_id in REPEAT_GROUP_ROLES_GENERIC
}

DATA_CONCEPT_MAP_GENERIC: Dict[str, str] = {
    "financial_model.total_revenues": "revenue",
    "financial_model.cost_of_goods_sold": "cost_of_revenue",
    "financial_model.sales_and_marketing": "sales_and_marketing",
    "financial_model.research_and_development": "research_and_development",
    "financial_model.general_and_administrative": "general_and_administrative",
    "financial_model.total_operating_expenses": "operating_expenses",
    "financial_model.operating_income": "operating_income",
    "financial_model.other_income_expense": "other_income_expense",
    "financial_model.interest_expense": "interest_expense",
    "financial_model.income_before_income_taxes": "pretax_income",
    "financial_model.income_tax_expense_benefit": "income_tax",
    "financial_model.net_income": "net_income",
    "financial_model.eps": "eps_diluted",
    "financial_model.diluted_shares_outstanding_m": "diluted_shares",
    "financial_model.ebitda": "ebitda",
    "financial_model.depreciation_and_amortization_r134": "depreciation_amortization",
    "financial_model.share_based_compensation": "stock_based_compensation",
    "financial_model.cash_and_marketable_securities": "cash_and_short_term_investments",
    "financial_model.cash_and_cash_equivalents": "cash_and_equivalents",
    "financial_model.accounts_receivable_net": "accounts_receivable",
    "financial_model.current_asset_1": "other_current_assets",
    "financial_model.current_asset_2": "inventory",
    "financial_model.current_asset_3": "prepaid_expenses",
    "financial_model.total_current_assets": "total_current_assets",
    "financial_model.property_and_equipment_net": "property_plant_equipment_net",
    "financial_model.goodwill": "goodwill",
    "financial_model.intangible_assets_net": "intangible_assets",
    "financial_model.operating_lease_right_of_use_assets": "operating_lease_right_of_use_assets",
    "financial_model.long_term_asset_1": "marketable_securities_noncurrent",
    "financial_model.long_term_asset_2": "other_non_current_assets",
    "financial_model.total_assets": "total_assets",
    "financial_model.accounts_payable": "accounts_payable",
    "financial_model.accrued_expenses": "accrued_expenses",
    "financial_model.current_liability_1": "deferred_revenue",
    "financial_model.current_liability_2": "other_current_liabilities",
    "financial_model.current_liability_3": "commercial_paper",
    "financial_model.total_current_liabilities": "total_current_liabilities",
    "financial_model.current_portion_of_long_term_debt": "short_term_debt",
    "financial_model.deferred_income_tax_assets": "deferred_tax_assets",
    "financial_model.other_long_term_liabilities": "other_non_current_liabilities",
    "financial_model.deferred_income_tax_liabilities": "deferred_tax_liabilities",
    "financial_model.long_term_operating_lease_liabilities": "lease_liability_noncurrent",
    "financial_model.long_term_debt": "long_term_debt",
    "financial_model.deferred_revenue_noncurrent": "deferred_revenue_noncurrent",
    "financial_model.total_liabilities": "total_liabilities",
    "financial_model.common_stock": "common_stock",
    "financial_model.additional_paid_in_capital": "additional_paid_in_capital",
    "financial_model.retained_earnings": "retained_earnings",
    "financial_model.accumulated_other_comprehensive_income_loss": "accumulated_other_comprehensive_income",
    "financial_model.shareholders_equity": "total_equity",
    "financial_model.net_cash_provided_by_operating_activities": "operating_cash_flow",
    "financial_model.other_non_cash_adjustments": "cf_other_non_cash_items",
    "financial_model.deferred_income_tax_assets_r140": "change_in_deferred_income_taxes",
    "financial_model.accounts_receivable_net_r144": "change_in_accounts_receivable",
    "financial_model.changes_in_operating_assets_and_liabilities": "change_in_other_working_capital",
    "financial_model.current_asset_2_r146": "change_in_inventory",
    "financial_model.accounts_payable_r147": "change_in_accounts_payable",
    "financial_model.accrued_expenses_r148": "change_in_accrued_expenses",
    "financial_model.acquisitions_of_businesses": "acquisitions_of_businesses",
    "financial_model.purchases_of_investments": "purchases_of_investments",
    "financial_model.sales_maturities_of_investments": "sales_maturities_of_investments",
    "financial_model.other_changes_in_investing_activities": "cf_other_investing_activities",
    "financial_model.net_cash_provided_by_investing_activities": "investing_cash_flow",
    "financial_model.proceeds_or_payment_for_debt": "net_long_term_debt_issuance",
    "financial_model.proceeds_from_employee_stock_purchase_plan": "proceeds_from_stock_plans",
    "financial_model.other_financing_activities": "cf_other_financing_activities",
    "financial_model.net_cash_provided_by_financing_activities": "financing_cash_flow",
    "financial_model.effect_of_exchange_rate_on_cash": "effect_of_exchange_rate_on_cash",
    "financial_model.cash_and_cash_equivalents_end_of_period": "cash_flow_cash_end_of_period",
    "financial_model.purchases_of_property_and_equipment": "capital_expenditures",
    "financial_model.dividends": "dividends_paid",
    "financial_model.share_repurchases": "share_repurchases",
    "assumptions.end_of_period_shares_outstanding": "shares_outstanding",
    "financial_model.free_cash_flow": "free_cash_flow",
}


def _iter_items(model: FinancialModel) -> Iterable[tuple[str, LineItem]]:
    for sheet_name in KEPT_SHEETS:
        sheet = model.sheets[sheet_name]
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, item


def _iter_formula_specs(item: LineItem) -> Iterable[FormulaSpec]:
    if item.historical:
        yield item.historical
    if item.projected:
        yield item.projected
    if item.overrides:
        yield from item.overrides.values()


def _extract_refs(obj) -> List[LineItemRef]:
    refs: List[LineItemRef] = []
    if obj is None:
        return refs
    if isinstance(obj, LineItemRef):
        return [obj]
    if isinstance(obj, dict):
        for value in obj.values():
            refs.extend(_extract_refs(value))
        return refs
    if isinstance(obj, (list, tuple, set)):
        for value in obj:
            refs.extend(_extract_refs(value))
    return refs


def _section_for_row(
    sheet_name: str,
    row: int,
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
) -> SectionSpec:
    for spec in section_specs[sheet_name]:
        if spec.row_start <= row <= spec.row_end:
            return spec
    raise ValueError(f"No section for {sheet_name} row {row}")


def _clean_source_name(item_id: str) -> str:
    base = item_id.split(".", 1)[1]
    return re.sub(r"_r\d+$", "", base)


def _build_tpl_mapping(
    model: FinancialModel,
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
    name_overrides: Dict[str, str],
    expected_item_count: int,
) -> Dict[str, str]:
    id_map: Dict[str, str] = {}
    for sheet_name, item in _iter_items(model):
        section = _section_for_row(sheet_name, item.row, section_specs=section_specs)
        canonical_name = name_overrides.get(item.id, _clean_source_name(item.id))
        id_map[item.id] = f"{SHEET_PREFIX[sheet_name]}.{section.id}.{canonical_name}"

    if len(id_map) != expected_item_count:
        raise AssertionError(f"Expected {expected_item_count} mapped items, got {len(id_map)}")
    if len(set(id_map.values())) != len(id_map):
        raise AssertionError("Template ID mapping produced duplicates")
    return id_map


def _build_pcty_to_tpl_mapping(model: FinancialModel) -> Dict[str, str]:
    return _build_tpl_mapping(
        model,
        section_specs=SECTION_SPECS_PCTY,
        name_overrides=CANONICAL_NAME_OVERRIDES_PCTY,
        expected_item_count=EXPECTED_ITEM_COUNT,
    )


def _build_generic_to_tpl_mapping(model: FinancialModel) -> Dict[str, str]:
    return _build_tpl_mapping(
        model,
        section_specs=SECTION_SPECS_GENERIC,
        name_overrides=CANONICAL_NAME_OVERRIDES_GENERIC,
        expected_item_count=EXPECTED_ITEM_COUNT_GENERIC,
    )


def _rewrite_refs(obj, id_map: Dict[str, str]):
    if obj is None:
        return None
    if isinstance(obj, LineItemRef):
        if obj.id in id_map:
            return LineItemRef(id=id_map[obj.id], t=obj.t, resolved=True)
        return LineItemRef(id=obj.id, t=obj.t, resolved=False)
    if isinstance(obj, dict):
        return {key: _rewrite_refs(value, id_map) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_rewrite_refs(value, id_map) for value in obj]
    if isinstance(obj, tuple):
        return tuple(_rewrite_refs(value, id_map) for value in obj)
    if isinstance(obj, set):
        return {_rewrite_refs(value, id_map) for value in obj}
    return obj


def _rewrite_formula_spec(spec: Optional[FormulaSpec], id_map: Dict[str, str]) -> Optional[FormulaSpec]:
    if spec is None:
        return None
    return spec.model_copy(update={"params": _rewrite_refs(spec.params, id_map)})


def _discover_header_dependency_targets(model: FinancialModel) -> Set[str]:
    item_by_id = {item.id: item for _, item in _iter_items(model)}
    result: Set[str] = set()
    for _, item in _iter_items(model):
        for spec in _iter_formula_specs(item):
            for ref in _extract_refs(spec.params):
                target = item_by_id.get(ref.id)
                if target and target.item_type == ItemType.header:
                    result.add(ref.id)
    return result


def _has_cross_sheet_ref(item: LineItem) -> bool:
    source_sheet = item.id.split(".", 1)[0]
    for spec in _iter_formula_specs(item):
        for ref in _extract_refs(spec.params):
            if ref.id.split(".", 1)[0] != source_sheet:
                return True
    return False


def _customization_for_item(
    item_id: str,
    *,
    optional_ids: Set[str],
    expected_header_dependency_targets: Set[str],
    template_tokens: Dict[str, str],
    repeat_group_ids: Dict[str, str],
) -> CustomizationType:
    customization = CustomizationType.fixed
    if item_id in optional_ids or item_id in expected_header_dependency_targets:
        customization = CustomizationType.optional
    if item_id in template_tokens and item_id not in repeat_group_ids:
        customization = CustomizationType.rename
    if item_id in repeat_group_ids:
        customization = CustomizationType.repeatable
    return customization


def _style_for_item(item_id: str, role: TemplateRole, *, key_driver_ids: Set[str]) -> CellStyle:
    if role == TemplateRole.header:
        return CellStyle(color=CellColor.header, bold=True)
    if item_id in key_driver_ids:
        return CellStyle(color=CellColor.key_driver)
    if role == TemplateRole.input:
        return CellStyle(color=CellColor.input_blue)
    return CellStyle(color=CellColor.formula_black)


def _merge_notes(*notes: Optional[str]) -> Optional[str]:
    parts = [note.strip() for note in notes if note and note.strip()]
    if not parts:
        return None
    return " ".join(parts)


def _assign_metadata(
    model: FinancialModel,
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
    expected_header_dependency_targets: Set[str],
    input_ids: Set[str],
    scenario_linked_ids: Set[str],
    optional_ids: Set[str],
    template_tokens: Dict[str, str],
    build_notes: Dict[str, str],
    header_dependency_notes: Dict[str, str],
    repeat_group_roles: Dict[str, str],
    data_concept_map: Dict[str, str],
    key_driver_ids: Set[str],
) -> Set[str]:
    promoted_headers = _discover_header_dependency_targets(model)
    if promoted_headers != expected_header_dependency_targets:
        raise AssertionError(
            f"Header dependency targets mismatch: expected {sorted(expected_header_dependency_targets)}, "
            f"got {sorted(promoted_headers)}"
        )

    repeat_group_ids = {item_id: "revenue_segment" for item_id in repeat_group_roles}
    for sheet_name, item in _iter_items(model):
        section = _section_for_row(sheet_name, item.row, section_specs=section_specs)
        if item.id in promoted_headers or item.id in input_ids:
            role = TemplateRole.input
        elif item.id in scenario_linked_ids:
            role = TemplateRole.scenario_linked
        elif item.item_type == ItemType.header:
            role = TemplateRole.header
        elif _has_cross_sheet_ref(item):
            role = TemplateRole.reference
        else:
            role = TemplateRole.derived

        if role == TemplateRole.input:
            item.item_type = ItemType.input
        elif role == TemplateRole.header:
            item.item_type = ItemType.header
        else:
            item.item_type = ItemType.derived

        item.driver_category = section.driver_category
        item.customization = _customization_for_item(
            item.id,
            optional_ids=optional_ids,
            expected_header_dependency_targets=expected_header_dependency_targets,
            template_tokens=template_tokens,
            repeat_group_ids=repeat_group_ids,
        )
        item.style = _style_for_item(item.id, role, key_driver_ids=key_driver_ids)
        item.data_concept_id = data_concept_map.get(item.id)
        item.template_token = template_tokens.get(item.id)
        item.repeat_group_id = repeat_group_ids.get(item.id)
        item.repeat_group_role = repeat_group_roles.get(item.id)
        item.build_notes = _merge_notes(
            build_notes.get(item.id),
            header_dependency_notes.get(item.id) if item.id in promoted_headers else None,
        )

    return promoted_headers


def _filter_overrides(
    item_id: str,
    overrides: Optional[Dict[int, FormulaSpec]],
    historical_periods: Set[int],
    *,
    scenario_table_ids: Set[str],
    strip_scenario_table_constant_overrides: bool = False,
) -> Optional[Dict[int, FormulaSpec]]:
    if not overrides:
        return None
    if item_id in scenario_table_ids:
        result = {
            period: spec
            for period, spec in overrides.items()
            if not (
                strip_scenario_table_constant_overrides
                and spec.type == FormulaType.constant
            )
        }
        return result or None
    result: Dict[int, FormulaSpec] = {}
    for period, spec in overrides.items():
        if period in historical_periods:
            continue
        if spec.type == FormulaType.constant:
            continue
        result[period] = spec
    return result or None


def _extend_formula_periods_for_stripped_constants(
    item: LineItem,
    *,
    scenario_table_ids: Set[str],
    strip_scenario_table_constant_overrides: bool,
) -> None:
    if (
        not strip_scenario_table_constant_overrides
        or item.id not in scenario_table_ids
        or not item.overrides
    ):
        return

    stripped_periods = {
        int(period)
        for period, spec in item.overrides.items()
        if spec.type == FormulaType.constant
    }
    if not stripped_periods:
        return

    item.formula_periods = sorted(
        {int(period) for period in (item.formula_periods or [])} | stripped_periods
    )


def _apply_scenario_linked_offset_params(model: FinancialModel) -> None:
    for _, item in _iter_items(model):
        anchor_id = _SCENARIO_LINKED_OFFSET_ANCHORS.get(item.id)
        if anchor_id is None:
            continue
        item.projected = FormulaSpec(
            type=FormulaType.valuation,
            subtype="offset_scenario",
            params={
                "anchor": LineItemRef(id=anchor_id),
                "selector": LineItemRef(id=_SCENARIO_SELECTOR_ID),
                "column_offset_mode": _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
            }
        )


def _find_sheet_item(model: FinancialModel, sheet_name: str, item_id: str) -> LineItem:
    for section in model.sheets[sheet_name].sections:
        for item in section.line_items:
            if item.id == item_id:
                return item
    raise AssertionError(f"Could not find {sheet_name} item {item_id}")


def _find_section_with_item(sheet, item_id: str) -> Section:
    for section in sheet.sections:
        if any(item.id == item_id for item in section.line_items):
            return section
    raise AssertionError(f"Could not find section containing {item_id}")


def _apply_label_overrides(model: FinancialModel, overrides: Dict[str, str]) -> None:
    for source_id, new_label in overrides.items():
        if source_id.startswith("assumptions."):
            sheet_name = "Assumptions"
        elif source_id.startswith("financial_model."):
            sheet_name = "Financial_model"
        else:
            raise ValueError(f"Unknown source ID prefix in label override: {source_id!r}")
        item = _find_sheet_item(model, sheet_name, source_id)
        item.label = new_label


def _insert_sum_ref_after(items: object, *, after_id: str, new_id: str, context: str) -> List[LineItemRef]:
    if not isinstance(items, list):
        raise AssertionError(f"{context} is missing SUM items")

    updated_items: List[LineItemRef] = []
    inserted = False
    for ref in items:
        if not isinstance(ref, LineItemRef):
            raise AssertionError(f"{context} contains non-reference SUM items")
        updated_items.append(ref)
        if ref.id == after_id:
            updated_items.append(LineItemRef(id=new_id))
            inserted = True

    if not inserted:
        raise AssertionError(f"Could not insert {new_id} into {context}")
    return updated_items


def _insert_other_non_current_assets_row(model: FinancialModel) -> None:
    item_id = "financial_model.other_non_current_assets"
    insert_row = 85
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic other_non_current_assets row already exists")

    target_section = _find_section_with_item(financial_sheet, "financial_model.deferred_income_tax_assets")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Other non-current assets",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    total_assets = _find_sheet_item(model, "Financial_model", "financial_model.total_assets")
    for spec in (total_assets.historical, total_assets.projected):
        if spec is None:
            continue
        expr = spec.params.get("expr")
        if not isinstance(expr, dict) or expr.get("op") != "+":
            raise AssertionError("Total assets formula is missing the expected additive expression")
        args = expr.get("args")
        if not isinstance(args, list) or not args:
            raise AssertionError("Total assets formula is missing expression args")
        long_term_assets = args[0]
        if not isinstance(long_term_assets, dict) or long_term_assets.get("op") != "SUM":
            raise AssertionError("Total assets formula is missing the long-term asset SUM")
        long_term_assets["args"] = _insert_sum_ref_after(
            long_term_assets.get("args"),
            after_id="financial_model.deferred_income_tax_assets",
            new_id=item_id,
            context="total assets",
        )


def _insert_deferred_revenue_row(model: FinancialModel) -> None:
    item_id = "financial_model.deferred_revenue_current"
    insert_row = 92
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic deferred_revenue row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, "financial_model.accrued_expenses")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Deferred revenue",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    current_liabilities = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.total_current_liabilities_before_client_fund_obligations",
    )
    for spec in (current_liabilities.historical, current_liabilities.projected):
        if spec is None or spec.params.get("function") != "SUM":
            continue
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id="financial_model.accrued_expenses",
            new_id=item_id,
            context="current liabilities subtotal",
        )


def _insert_other_current_liabilities_row(model: FinancialModel) -> None:
    item_id = "financial_model.current_liability_2"
    insert_row = 93
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic current_liability_2 row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, "financial_model.current_liability_1")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Other current liabilities",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    total_current_liabilities = _find_sheet_item(
        model, "Financial_model", "financial_model.total_current_liabilities"
    )
    for spec in (total_current_liabilities.historical, total_current_liabilities.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            raise AssertionError("total_current_liabilities formula has unexpected operands shape")
        spec.params["operands"] = [
            operands[0],
            *_insert_sum_ref_after(
                operands[1:],
                after_id="financial_model.current_liability_1",
                new_id=item_id,
                context="total current liabilities",
            ),
        ]


def _insert_commercial_paper_row(model: FinancialModel) -> None:
    item_id = "financial_model.current_liability_3"
    insert_row = 94
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic current_liability_3 row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, "financial_model.current_liability_2")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Commercial paper",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    total_current_liabilities = _find_sheet_item(
        model, "Financial_model", "financial_model.total_current_liabilities"
    )
    for spec in (total_current_liabilities.historical, total_current_liabilities.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            raise AssertionError("total_current_liabilities formula has unexpected operands shape")
        spec.params["operands"] = [
            operands[0],
            *_insert_sum_ref_after(
                operands[1:],
                after_id="financial_model.current_liability_2",
                new_id=item_id,
                context="total current liabilities",
            ),
        ]


def _insert_deferred_revenue_noncurrent_row(model: FinancialModel) -> None:
    item_id = "financial_model.deferred_revenue_noncurrent"
    insert_row = 103
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic deferred_revenue_noncurrent row already exists")
    if any(
        sheet_name == "Financial_model" and item.row == insert_row
        for sheet_name, item in _iter_items(model)
    ):
        raise AssertionError("Synthetic deferred_revenue_noncurrent row target is occupied")

    target_section = _find_section_with_item(financial_sheet, "financial_model.long_term_debt")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Deferred revenue, noncurrent",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    total_liabilities = _find_sheet_item(
        model, "Financial_model", "financial_model.total_liabilities"
    )
    for spec in (total_liabilities.historical, total_liabilities.projected):
        if spec is None:
            continue
        expr = spec.params.get("expr")
        if not isinstance(expr, dict) or expr.get("op") != "+":
            raise AssertionError("total_liabilities formula is missing the expected additive expression")
        args = expr.get("args")
        if not isinstance(args, list) or not args:
            raise AssertionError("total_liabilities formula is missing expression args")
        noncurrent_liabilities = args[0]
        if not isinstance(noncurrent_liabilities, dict) or noncurrent_liabilities.get("op") != "SUM":
            raise AssertionError("total_liabilities formula is missing the noncurrent liability SUM")
        sum_args = noncurrent_liabilities.get("args")
        after_id = "financial_model.long_term_debt"
        if isinstance(sum_args, list) and not any(
            isinstance(ref, LineItemRef) and ref.id == after_id for ref in sum_args
        ):
            after_id = "financial_model.deferred_income_tax_liabilities"
        noncurrent_liabilities["args"] = _insert_sum_ref_after(
            sum_args,
            after_id=after_id,
            new_id=item_id,
            context="total liabilities",
        )


def _insert_inventory_row(model: FinancialModel) -> None:
    inventory_id = "financial_model.inventory"
    insert_row = 68
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == inventory_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic inventory row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = next(
        (
            section
            for section in financial_sheet.sections
            if any(item.id == "financial_model.accounts_receivable_net" for item in section.line_items)
        ),
        None,
    )
    if target_section is None:
        raise AssertionError("Could not find Financial_model section for inventory insertion")

    target_section.line_items.append(
        LineItem(
            id=inventory_id,
            label=" Inventory",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    current_assets = next(
        (
            item
            for section in financial_sheet.sections
            for item in section.line_items
            if item.id == "financial_model.total_current_assets_before_funds_held_for_clients"
        ),
        None,
    )
    if current_assets is None:
        raise AssertionError("Could not find current assets subtotal for inventory insertion")

    for spec in (current_assets.historical, current_assets.projected):
        if spec is None or spec.params.get("function") != "SUM":
            continue
        items = spec.params.get("items")
        if not isinstance(items, list):
            raise AssertionError("Current assets subtotal is missing SUM items")

        updated_items = []
        inserted = False
        for ref in items:
            updated_items.append(ref)
            if isinstance(ref, LineItemRef) and ref.id == "financial_model.accounts_receivable_net":
                updated_items.append(LineItemRef(id=inventory_id))
                inserted = True

        if not inserted:
            raise AssertionError("Could not insert inventory into current assets subtotal")
        spec.params["items"] = updated_items


def _insert_investment_activity_rows(model: FinancialModel) -> None:
    purchase_id = "financial_model.purchases_of_investments"
    proceeds_id = "financial_model.sales_maturities_of_investments"
    purchase_row = 157
    proceeds_row = 158
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id in {purchase_id, proceeds_id} for _, item in _iter_items(model)):
        raise AssertionError("Synthetic investment activity rows already exist")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= purchase_row:
                item.row += 2

    target_section = _find_section_with_item(financial_sheet, "financial_model.acquisitions_of_businesses")
    target_section.line_items.extend(
        [
            LineItem(
                id=purchase_id,
                label="Purchases of investments",
                row=purchase_row,
                item_type=ItemType.derived,
                unit=Unit.dollars,
            ),
            LineItem(
                id=proceeds_id,
                label="Sales and maturities of investments",
                row=proceeds_row,
                item_type=ItemType.derived,
                unit=Unit.dollars,
            ),
        ]
    )

    investing_cash_flow = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.net_cash_provided_by_investing_activities",
    )
    for spec in (investing_cash_flow.historical, investing_cash_flow.projected):
        if spec is None or spec.params.get("function") != "SUM":
            continue
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id="financial_model.acquisitions_of_businesses",
            new_id=purchase_id,
            context="investing cash flow",
        )
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id=purchase_id,
            new_id=proceeds_id,
            context="investing cash flow",
        )


def _clear_cash_flow_artifact_historicals(model: FinancialModel) -> None:
    for item_id in CASH_FLOW_ARTIFACT_HISTORICAL_IDS:
        _find_sheet_item(model, "Financial_model", item_id).historical = None


def _set_if_applicable_default_zero_historicals(model: FinancialModel) -> None:
    historical_periods = list(
        model.time_structure.historical_periods or model.time_structure.historical_years or []
    )
    projection_periods = list(
        model.time_structure.projection_periods or model.time_structure.projection_years or []
    )
    for item_id in IF_APPLICABLE_DEFAULT_ZERO_IDS:
        item = _find_sheet_item(model, "Financial_model", item_id)
        item.historical = FormulaSpec(
            type=FormulaType.constant,
            subtype=None,
            params={"value": 0},
            note=None,
        )
        existing = [int(p) for p in (item.formula_periods or [])]
        merged = sorted(
            set(existing)
            | {int(p) for p in historical_periods}
            | {int(p) for p in projection_periods}
        )
        item.formula_periods = merged


def _normalize_cash_flow_projection_links(model: FinancialModel) -> None:
    projection_periods = {
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    }
    if not projection_periods:
        return

    net_income_item = _find_sheet_item(model, "Financial_model", "tpl.fm.cash_flow.net_income")
    net_income_item.projected = FormulaSpec(
        type=FormulaType.ref,
        params={"source": LineItemRef(id=CASH_FLOW_NET_INCOME_SOURCE_ID)},
    )

    for item_id in sorted(CASH_FLOW_FULL_PROJECTION_IDS):
        item = _find_sheet_item(model, "Financial_model", item_id)
        item.formula_periods = sorted(
            {int(period) for period in (item.formula_periods or [])}
            | projection_periods
        )

    for item_id in sorted(CASH_FLOW_PER_SHARE_PROJECTION_IDS):
        item = _find_sheet_item(model, "Financial_model", item_id)
        if item.historical is not None:
            item.projected = item.historical.model_copy(deep=True)
        item.formula_periods = sorted(
            {int(period) for period in (item.formula_periods or [])}
            | projection_periods
        )


def _normalize_valuation_projection_links(model: FinancialModel) -> None:
    projection_periods = [
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    ]
    if not projection_periods:
        return

    stock_based_compensation = _find_sheet_item(
        model,
        "Financial_model",
        "tpl.fm.adjusted_earnings.stock_based_compensation_and_related_payroll_taxes",
    )
    stock_based_compensation_ref = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id="tpl.a.stock_based_compensation.stock_based_compensation"),
        },
    )
    stock_based_compensation.historical = stock_based_compensation_ref
    stock_based_compensation.projected = stock_based_compensation_ref

    adjusted_operating_income = _find_sheet_item(
        model,
        "Financial_model",
        "tpl.fm.adjusted_earnings.adjusted_operating_income",
    )
    adjusted_operating_income.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "+",
                LineItemRef(id="tpl.fm.income_statement.operating_income"),
                LineItemRef(id="tpl.fm.adjusted_earnings.stock_based_compensation_and_related_payroll_taxes"),
                LineItemRef(id="tpl.fm.adjusted_earnings.other_items"),
                LineItemRef(id="tpl.fm.adjusted_earnings.amortization_of_acquired_intangibles"),
            ]
        },
    )
    adjusted_operating_income.formula_periods = sorted(
        {int(period) for period in (adjusted_operating_income.formula_periods or [])}
        | set(projection_periods[-2:])
    )

    assumptions_adjusted_operating_income = _find_sheet_item(
        model,
        "Assumptions",
        "tpl.a.adj_operating_income.adjusted_operating_income",
    )
    assumptions_adjusted_operating_income.projected = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id="tpl.fm.adjusted_earnings.adjusted_operating_income"),
        },
    )

    long_term_debt = _find_sheet_item(model, "Financial_model", "tpl.fm.balance_sheet.long_term_debt")
    long_term_debt.projected = FormulaSpec(
        type=FormulaType.ref,
        params={"source": LineItemRef(id="tpl.a.capital_sources.long_term_debt")},
    )
    long_term_debt.formula_periods = projection_periods

    net_cash = _find_sheet_item(model, "Financial_model", "tpl.fm.balance_sheet.net_cash")
    net_cash.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "-",
                LineItemRef(id="tpl.fm.balance_sheet.cash_and_marketable_securities"),
                LineItemRef(id="tpl.fm.balance_sheet.long_term_debt"),
            ]
        },
    )
    net_cash.formula_periods = sorted(
        {int(period) for period in (net_cash.formula_periods or [])}
        | set(projection_periods)
    )


def _normalize_generic_cash_reconciliation(model: FinancialModel) -> None:
    forex_item_id = "financial_model.effect_of_exchange_rate_on_cash"
    if any(item.id == forex_item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic effect_of_exchange_rate_on_cash row already exists")

    net_change_item = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.net_change_in_cash_and_cash_equivalents",
    )
    forex_row = net_change_item.row - 1
    if any(
        sheet_name == "Financial_model" and item.row == forex_row
        for sheet_name, item in _iter_items(model)
    ):
        raise AssertionError("Synthetic effect_of_exchange_rate_on_cash row target is occupied")

    target_section = _find_section_with_item(
        model.sheets["Financial_model"],
        "financial_model.net_change_in_cash_and_cash_equivalents",
    )
    target_section.line_items.append(
        LineItem(
            id=forex_item_id,
            label="Effect of exchange rate on cash",
            row=forex_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    for spec in (net_change_item.historical, net_change_item.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            raise AssertionError("net_change_in_cash_and_cash_equivalents formula has unexpected operands shape")
        if not any(isinstance(ref, LineItemRef) and ref.id == forex_item_id for ref in operands[1:]):
            operands.append(LineItemRef(id=forex_item_id))

    end_item = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.cash_and_cash_equivalents_end_of_period",
    )
    cash_item = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.cash_and_cash_equivalents",
    )

    end_item.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "+",
                LineItemRef(id="financial_model.cash_and_cash_equivalents"),
                LineItemRef(id="financial_model.funds_held_for_client_s_cash_and_cash_equivalents"),
            ]
        },
    )
    cash_item.historical = None
    if cash_item.projected is None:
        cash_item.formula_periods = None


def _split_sections(
    sheet_name: str,
    items: Sequence[LineItem],
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
    expected_sheet_item_counts: Dict[str, int],
    expected_section_counts: Dict[str, int],
) -> List[Section]:
    sections: List[Section] = []
    assigned: Set[str] = set()
    for spec in section_specs[sheet_name]:
        line_items = [
            item
            for item in items
            if spec.row_start <= item.row <= spec.row_end
        ]
        for item in line_items:
            assigned.add(item.id)
        sections.append(
            Section(
                id=spec.id,
                label=spec.label,
                line_items=sorted(line_items, key=lambda item: item.row),
                driver_category=spec.driver_category,
            )
        )

    if len(assigned) != len(items):
        missing = sorted(item.id for item in items if item.id not in assigned)
        raise AssertionError(f"Unassigned {sheet_name} items: {missing}")
    if len(sections) != expected_section_counts[sheet_name]:
        raise AssertionError(f"Unexpected section count for {sheet_name}: {len(sections)}")
    if sum(len(section.line_items) for section in sections) != expected_sheet_item_counts[sheet_name]:
        raise AssertionError(f"Unexpected item count in split sections for {sheet_name}")
    return sections


def _validate_template(
    model: FinancialModel,
    *,
    expected_section_counts: Dict[str, int],
    expected_sheet_item_counts: Dict[str, int],
) -> None:
    model.build_index()
    FinancialModel.model_validate_json(model.model_dump_json())
    for sheet_name in KEPT_SHEETS:
        sheet = model.sheets[sheet_name]
        if len(sheet.sections) != expected_section_counts[sheet_name]:
            raise AssertionError(f"Unexpected section count for {sheet_name}")
        if any(section.id == "main" for section in sheet.sections):
            raise AssertionError(f"{sheet_name} still contains a main section")
        total = sum(len(section.line_items) for section in sheet.sections)
        if total != expected_sheet_item_counts[sheet_name]:
            raise AssertionError(f"Unexpected item count for {sheet_name}: {total}")


PCTY_TEMPLATE_CONFIG = TemplateBuildConfig(
    name="pcty",
    source_path=PCTY_PATH,
    artifact_path=PCTY_REFERENCE_TEMPLATE_PATH,
    section_specs=SECTION_SPECS_PCTY,
    name_overrides=CANONICAL_NAME_OVERRIDES_PCTY,
    expected_item_count=EXPECTED_ITEM_COUNT,
    expected_sheet_item_counts=EXPECTED_SHEET_ITEM_COUNTS,
    metadata=TemplateMetadataSpec(
        source_model="pcty_reference",
        notes="PCTY reference 2-sheet template derived from the PCTY reference model",
        company_name="PCTY Reference Template",
    ),
    expected_header_dependency_targets=EXPECTED_HEADER_DEPENDENCY_TARGETS_PCTY,
    scenario_linked_ids=SCENARIO_LINKED_IDS_PCTY,
    scenario_table_ids=SCENARIO_TABLE_IDS_PCTY,
    input_ids=INPUT_IDS_PCTY,
    key_driver_ids=KEY_DRIVER_IDS_PCTY,
    optional_ids=OPTIONAL_IDS_PCTY,
    template_tokens=TEMPLATE_TOKENS_PCTY,
    build_notes=BUILD_NOTES_PCTY,
    header_dependency_notes=HEADER_DEPENDENCY_NOTES_PCTY,
    repeat_group_roles=REPEAT_GROUP_ROLES_PCTY,
    data_concept_map=DATA_CONCEPT_MAP_PCTY,
    placeholder_inserters=(
        _insert_inventory_row,
        _insert_other_non_current_assets_row,
        _insert_deferred_revenue_row,
    ),
    load_kwargs={
        "expand_shared": True,
        "historical_cutoff_year": 2023,
    },
    clear_cash_flow_artifact_historicals=True,
)

GENERIC_TEMPLATE_CONFIG = TemplateBuildConfig(
    name="generic",
    source_path=MODEL_TEMPLATE_PATH,
    artifact_path=SIA_GENERIC_TEMPLATE_PATH,
    section_specs=SECTION_SPECS_GENERIC,
    name_overrides=CANONICAL_NAME_OVERRIDES_GENERIC,
    expected_item_count=EXPECTED_ITEM_COUNT_GENERIC,
    expected_sheet_item_counts=EXPECTED_SHEET_ITEM_COUNTS_GENERIC,
    metadata=TemplateMetadataSpec(
        source_model="sia_generic",
        notes="Generic SIA 2-sheet template derived from Model_template.xlsx",
        company_name="SIA Generic Template",
    ),
    expected_header_dependency_targets=EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC,
    scenario_linked_ids=SCENARIO_LINKED_IDS_GENERIC,
    scenario_table_ids=SCENARIO_TABLE_IDS_GENERIC,
    input_ids=INPUT_IDS_GENERIC,
    key_driver_ids=KEY_DRIVER_IDS_GENERIC,
    optional_ids=OPTIONAL_IDS_GENERIC,
    template_tokens=TEMPLATE_TOKENS_GENERIC,
    build_notes=BUILD_NOTES_GENERIC,
    header_dependency_notes=HEADER_DEPENDENCY_NOTES_GENERIC,
    repeat_group_roles=REPEAT_GROUP_ROLES_GENERIC,
    data_concept_map=DATA_CONCEPT_MAP_GENERIC,
    label_overrides=LABEL_OVERRIDES_GENERIC,
    placeholder_inserters=(
        _insert_other_current_liabilities_row,
        _insert_commercial_paper_row,
        _insert_deferred_revenue_noncurrent_row,
        _insert_investment_activity_rows,
        _normalize_generic_cash_reconciliation,
    ),
    load_kwargs={
        "historical_cutoff_year": 2023,
    },
    clear_cash_flow_artifact_historicals=True,
    set_if_applicable_default_zero_historicals=True,
    strip_scenario_table_constant_overrides=True,
    normalize_cash_flow_projection_links=True,
    normalize_valuation_projection_links=True,
)

TEMPLATE_CONFIGS = {
    "pcty": PCTY_TEMPLATE_CONFIG,
    "generic": GENERIC_TEMPLATE_CONFIG,
}


def _load_source_model(config: TemplateBuildConfig) -> FinancialModel:
    model = read_model(
        str(config.source_path),
        mode="full",
        **config.load_kwargs,
    )
    if not isinstance(model, FinancialModel):
        raise TypeError("read_model() did not return a FinancialModel")
    return model


def build_template(
    source_path: str | Path,
    section_specs: Dict[str, Sequence[SectionSpec]],
    name_overrides: Dict[str, str],
    metadata: TemplateMetadataSpec,
    placeholder_inserters: Sequence[PlaceholderInserter] = (),
    *,
    artifact_path: Path,
    expected_item_count: int,
    expected_sheet_item_counts: Dict[str, int],
    expected_header_dependency_targets: Set[str],
    scenario_linked_ids: Set[str],
    scenario_table_ids: Set[str],
    input_ids: Set[str],
    key_driver_ids: Set[str],
    optional_ids: Set[str],
    template_tokens: Dict[str, str],
    build_notes: Dict[str, str],
    header_dependency_notes: Dict[str, str],
    repeat_group_roles: Dict[str, str],
    data_concept_map: Dict[str, str],
    load_kwargs: Optional[Dict[str, Any]] = None,
    clear_cash_flow_artifact_historicals: bool = False,
    name: str = "custom",
    model: Optional[FinancialModel] = None,
) -> FinancialModel:
    config = TemplateBuildConfig(
        name=name,
        source_path=Path(source_path),
        artifact_path=artifact_path,
        section_specs=section_specs,
        name_overrides=name_overrides,
        expected_item_count=expected_item_count,
        expected_sheet_item_counts=expected_sheet_item_counts,
        metadata=metadata,
        expected_header_dependency_targets=expected_header_dependency_targets,
        scenario_linked_ids=scenario_linked_ids,
        scenario_table_ids=scenario_table_ids,
        input_ids=input_ids,
        key_driver_ids=key_driver_ids,
        optional_ids=optional_ids,
        template_tokens=template_tokens,
        build_notes=build_notes,
        header_dependency_notes=header_dependency_notes,
        repeat_group_roles=repeat_group_roles,
        data_concept_map=data_concept_map,
        placeholder_inserters=tuple(placeholder_inserters),
        load_kwargs=dict(load_kwargs or {}),
        clear_cash_flow_artifact_historicals=clear_cash_flow_artifact_historicals,
    )
    return _build_template_from_config(config, model=model)


def _build_template_from_config(
    config: TemplateBuildConfig,
    *,
    model: Optional[FinancialModel] = None,
) -> FinancialModel:
    source_model = _load_source_model(config) if model is None else model

    template = source_model.model_copy(deep=True)
    template.sheets = {name: template.sheets[name] for name in KEPT_SHEETS}
    for inserter in config.placeholder_inserters:
        inserter(template)
    _apply_label_overrides(template, config.label_overrides)

    id_map = _build_tpl_mapping(
        template,
        section_specs=config.section_specs,
        name_overrides=config.name_overrides,
        expected_item_count=config.expected_item_count,
    )
    _assign_metadata(
        template,
        section_specs=config.section_specs,
        expected_header_dependency_targets=config.expected_header_dependency_targets,
        input_ids=config.input_ids,
        scenario_linked_ids=config.scenario_linked_ids,
        optional_ids=config.optional_ids,
        template_tokens=config.template_tokens,
        build_notes=config.build_notes,
        header_dependency_notes=config.header_dependency_notes,
        repeat_group_roles=config.repeat_group_roles,
        data_concept_map=config.data_concept_map,
        key_driver_ids=config.key_driver_ids,
    )
    if config.clear_cash_flow_artifact_historicals:
        _clear_cash_flow_artifact_historicals(template)
    if config.set_if_applicable_default_zero_historicals:
        _set_if_applicable_default_zero_historicals(template)

    historical_periods = set(
        template.time_structure.historical_periods or template.time_structure.historical_years
    )
    for _, item in _iter_items(template):
        _extend_formula_periods_for_stripped_constants(
            item,
            scenario_table_ids=config.scenario_table_ids,
            strip_scenario_table_constant_overrides=config.strip_scenario_table_constant_overrides,
        )
        item.overrides = _filter_overrides(
            item.id,
            item.overrides,
            historical_periods,
            scenario_table_ids=config.scenario_table_ids,
            strip_scenario_table_constant_overrides=config.strip_scenario_table_constant_overrides,
        )
        if item.id == "assumptions.interest_rate":
            item.historical = None
            if item.overrides:
                item.overrides = {
                    period: spec
                    for period, spec in item.overrides.items()
                    if spec.type != FormulaType.raw
                } or None

    for _, item in _iter_items(template):
        item.id = id_map[item.id]
        item.historical = _rewrite_formula_spec(item.historical, id_map)
        item.projected = _rewrite_formula_spec(item.projected, id_map)
        if item.overrides:
            item.overrides = {
                period: _rewrite_formula_spec(spec, id_map)
                for period, spec in item.overrides.items()
            }

    _apply_scenario_linked_offset_params(template)
    if config.normalize_cash_flow_projection_links:
        _normalize_cash_flow_projection_links(template)
    if config.normalize_valuation_projection_links:
        _normalize_valuation_projection_links(template)

    for sheet_name in KEPT_SHEETS:
        sheet = template.sheets[sheet_name]
        items = [item for section in sheet.sections for item in section.line_items]
        sheet.sections = _split_sections(
            sheet_name,
            items,
            section_specs=config.section_specs,
            expected_sheet_item_counts=config.expected_sheet_item_counts,
            expected_section_counts=EXPECTED_SECTION_COUNTS,
        )

    for _, item in _iter_items(template):
        item.values = None

    template.sheets["Assumptions"].sheet_type = SheetType.assumptions
    template.sheets["Assumptions"].description = (
        "Driver sheet containing all inputs and assumptions that feed Financial_model"
    )
    template.sheets["Assumptions"].layout = SheetLayout(
        label_column="A",
        first_data_column="D",
        column_width_label=40.0,
        column_width_data=14.0,
        header_rows=4,
        freeze_panes="D5",
    )

    template.sheets["Financial_model"].sheet_type = SheetType.financial_model
    template.sheets["Financial_model"].description = (
        "Three-statement model: income statement, balance sheet, cash flow, with margins, growth rates, and ratios"
    )
    template.sheets["Financial_model"].layout = SheetLayout(
        label_column="A",
        first_data_column="D",
        column_width_label=36.0,
        column_width_data=14.0,
        header_rows=3,
        freeze_panes="D4",
    )

    template.metadata = ModelMetadata(
        is_template=True,
        methodology="sia",
        source_model=config.metadata.source_model,
        build_status=BuildStatus.template,
        notes=config.metadata.notes,
    )
    template.company = CompanyInfo(
        ticker=config.metadata.company_ticker,
        name=config.metadata.company_name,
        fiscal_year_end=None,
    )

    _validate_template(
        template,
        expected_section_counts=EXPECTED_SECTION_COUNTS,
        expected_sheet_item_counts=config.expected_sheet_item_counts,
    )
    return template


def load_pcty_reference() -> FinancialModel:
    """Load the checked-in PCTY reference artifact."""

    return FinancialModel.model_validate_json(PCTY_REFERENCE_TEMPLATE_PATH.read_text(encoding="utf-8"))


def load_sia_generic_template() -> FinancialModel:
    """Load the checked-in generic SIA template artifact."""

    return FinancialModel.model_validate_json(SIA_GENERIC_TEMPLATE_PATH.read_text(encoding="utf-8"))


def build_pcty_reference_template(model: Optional[FinancialModel] = None) -> FinancialModel:
    """Transform the parsed PCTY workbook into the checked-in PCTY reference artifact."""

    return _build_template_from_config(PCTY_TEMPLATE_CONFIG, model=model)


def build_sia_generic_template(model: Optional[FinancialModel] = None) -> FinancialModel:
    """Transform the generic source workbook into the checked-in generic artifact."""

    return _build_template_from_config(GENERIC_TEMPLATE_CONFIG, model=model)


def _load_pcty_model() -> FinancialModel:
    return _load_source_model(PCTY_TEMPLATE_CONFIG)


def _load_generic_model() -> FinancialModel:
    return _load_source_model(GENERIC_TEMPLATE_CONFIG)


def _write_template_artifact(config: TemplateBuildConfig) -> Path:
    if config.name == "pcty":
        template = build_pcty_reference_template()
    elif config.name == "generic":
        template = build_sia_generic_template()
    else:  # pragma: no cover - guarded by CLI choices
        raise ValueError(f"Unknown template target: {config.name}")
    config.artifact_path.write_text(template.model_dump_json(indent=2), encoding="utf-8")
    return config.artifact_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate checked-in template artifacts.")
    parser.add_argument(
        "--target",
        choices=("pcty", "generic", "all"),
        default="all",
        help="Which artifact(s) to regenerate.",
    )
    args = parser.parse_args(argv)

    targets = ("pcty", "generic") if args.target == "all" else (args.target,)
    for target in targets:
        print(_write_template_artifact(TEMPLATE_CONFIGS[target]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
