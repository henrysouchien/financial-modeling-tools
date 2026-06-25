"""Static configuration for template artifact construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Sequence, Set

from ..models import DriverCategory, FinancialModel
from .template_builder_config_generic import (
    EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC as EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC,
    SCENARIO_LINKED_IDS_GENERIC as SCENARIO_LINKED_IDS_GENERIC,
    _SCENARIO_SELECTOR_ID as _SCENARIO_SELECTOR_ID,
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _SCENARIO_LINKED_OFFSET_ANCHORS as _SCENARIO_LINKED_OFFSET_ANCHORS,
    SCENARIO_TABLE_IDS_GENERIC as SCENARIO_TABLE_IDS_GENERIC,
    INPUT_IDS_GENERIC as INPUT_IDS_GENERIC,
    KEY_DRIVER_IDS_GENERIC as KEY_DRIVER_IDS_GENERIC,
    OPTIONAL_IDS_GENERIC as OPTIONAL_IDS_GENERIC,
    TEMPLATE_TOKENS_GENERIC as TEMPLATE_TOKENS_GENERIC,
    BUILD_NOTES_GENERIC as BUILD_NOTES_GENERIC,
    LABEL_OVERRIDES_GENERIC as LABEL_OVERRIDES_GENERIC,
    HEADER_DEPENDENCY_NOTES_GENERIC as HEADER_DEPENDENCY_NOTES_GENERIC,
    REPEAT_GROUP_ROLES_GENERIC as REPEAT_GROUP_ROLES_GENERIC,
    REPEAT_GROUP_IDS_GENERIC as REPEAT_GROUP_IDS_GENERIC,
    DATA_CONCEPT_MAP_GENERIC as DATA_CONCEPT_MAP_GENERIC,
)

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


__all__ = [
    "PCTY_PATH",
    "MODEL_TEMPLATE_PATH",
    "PCTY_REFERENCE_TEMPLATE_PATH",
    "SIA_GENERIC_TEMPLATE_PATH",
    "KEPT_SHEETS",
    "EXPECTED_ITEM_COUNT",
    "EXPECTED_ITEM_COUNT_GENERIC",
    "EXPECTED_SECTION_COUNTS",
    "EXPECTED_SHEET_ITEM_COUNTS",
    "EXPECTED_SHEET_ITEM_COUNTS_GENERIC",
    "TemplateRole",
    "SectionSpec",
    "TemplateMetadataSpec",
    "PlaceholderInserter",
    "TemplateBuildConfig",
    "ASSUMPTIONS_SECTIONS",
    "FINANCIAL_MODEL_SECTIONS",
    "SECTION_SPECS",
    "SECTION_SPECS_PCTY",
    "ASSUMPTIONS_SECTIONS_GENERIC",
    "FINANCIAL_MODEL_SECTIONS_GENERIC",
    "SECTION_SPECS_GENERIC",
    "SHEET_PREFIX",
    "CANONICAL_NAME_OVERRIDES",
    "CANONICAL_NAME_OVERRIDES_PCTY",
    "CANONICAL_NAME_OVERRIDES_GENERIC",
    "EXPECTED_HEADER_DEPENDENCY_TARGETS",
    "SCENARIO_LINKED_IDS",
    "SCENARIO_TABLE_IDS",
    "INPUT_IDS",
    "KEY_DRIVER_IDS",
    "OPTIONAL_IDS",
    "TEMPLATE_TOKENS",
    "BUILD_NOTES",
    "HEADER_DEPENDENCY_NOTES",
    "REPEAT_GROUP_ROLES",
    "REPEAT_GROUP_IDS",
    "CASH_FLOW_ARTIFACT_HISTORICAL_IDS",
    "IF_APPLICABLE_DEFAULT_ZERO_IDS",
    "CASH_FLOW_FULL_PROJECTION_IDS",
    "CASH_FLOW_PER_SHARE_PROJECTION_IDS",
    "CASH_FLOW_NET_INCOME_SOURCE_ID",
    "DATA_CONCEPT_MAP",
    "EXPECTED_HEADER_DEPENDENCY_TARGETS_PCTY",
    "SCENARIO_LINKED_IDS_PCTY",
    "SCENARIO_TABLE_IDS_PCTY",
    "INPUT_IDS_PCTY",
    "KEY_DRIVER_IDS_PCTY",
    "OPTIONAL_IDS_PCTY",
    "TEMPLATE_TOKENS_PCTY",
    "BUILD_NOTES_PCTY",
    "HEADER_DEPENDENCY_NOTES_PCTY",
    "REPEAT_GROUP_ROLES_PCTY",
    "REPEAT_GROUP_IDS_PCTY",
    "DATA_CONCEPT_MAP_PCTY",
    "EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC",
    "SCENARIO_LINKED_IDS_GENERIC",
    "_SCENARIO_SELECTOR_ID",
    "_COLUMN_OFFSET_MODE_PERIOD_RELATIVE",
    "_SCENARIO_LINKED_OFFSET_ANCHORS",
    "SCENARIO_TABLE_IDS_GENERIC",
    "INPUT_IDS_GENERIC",
    "KEY_DRIVER_IDS_GENERIC",
    "OPTIONAL_IDS_GENERIC",
    "TEMPLATE_TOKENS_GENERIC",
    "BUILD_NOTES_GENERIC",
    "LABEL_OVERRIDES_GENERIC",
    "HEADER_DEPENDENCY_NOTES_GENERIC",
    "REPEAT_GROUP_ROLES_GENERIC",
    "REPEAT_GROUP_IDS_GENERIC",
    "DATA_CONCEPT_MAP_GENERIC",
]
