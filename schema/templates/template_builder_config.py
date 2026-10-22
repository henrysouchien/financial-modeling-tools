"""Static configuration for template artifact construction.

The build reads its source model from a **checked-in input artifact**
(``TemplateBuildConfig.source_input``; see ``template_source_inputs``), never
from a workbook path on one operator's machine. The two absolute
cloud-storage paths that once fed construction are not runtime inputs. The
reference-only tables live beside their tests in
``tests/reference_template_oracle.py``. What remains here is the live generic
template's configuration and the shared shapes both builds use. See
``schema/templates/source_inputs/README.md`` and ``schema/README.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Sequence, Set

from ..models import FinancialModel
from industry_slice_core.industry_types import DriverCategory
from .template_builder_config_generic import (
    EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC as EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC,
    SCENARIO_LINKED_IDS_GENERIC as SCENARIO_LINKED_IDS_GENERIC,
    _SCENARIO_SELECTOR_ID as _SCENARIO_SELECTOR_ID,
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _SCENARIO_LINKED_OFFSET_ANCHORS as _SCENARIO_LINKED_OFFSET_ANCHORS,
    SCENARIO_TABLE_IDS_GENERIC as SCENARIO_TABLE_IDS_GENERIC,
    INPUT_IDS_GENERIC as INPUT_IDS_GENERIC,
    PROJECTED_ZERO_DEFAULT_TEMPLATE_IDS_GENERIC as PROJECTED_ZERO_DEFAULT_TEMPLATE_IDS_GENERIC,
    KEY_DRIVER_IDS_GENERIC as KEY_DRIVER_IDS_GENERIC,
    OPTIONAL_IDS_GENERIC as OPTIONAL_IDS_GENERIC,
    TEMPLATE_TOKENS_GENERIC as TEMPLATE_TOKENS_GENERIC,
    BUILD_NOTES_GENERIC as BUILD_NOTES_GENERIC,
    LABEL_OVERRIDES_GENERIC as LABEL_OVERRIDES_GENERIC,
    HEADER_DEPENDENCY_NOTES_GENERIC as HEADER_DEPENDENCY_NOTES_GENERIC,
    REPEAT_GROUP_ROLES_GENERIC as REPEAT_GROUP_ROLES_GENERIC,
    REPEAT_GROUP_IDS_GENERIC as REPEAT_GROUP_IDS_GENERIC,
)

SIA_GENERIC_TEMPLATE_PATH = Path(__file__).resolve().parent / "sia_generic.json"

KEPT_SHEETS: Sequence[str] = ("Assumptions", "Financial_model")
EXPECTED_ITEM_COUNT_GENERIC = 389
EXPECTED_SECTION_COUNTS = {"Assumptions": 18, "Financial_model": 8}
EXPECTED_SHEET_ITEM_COUNTS_GENERIC = {"Assumptions": 223, "Financial_model": 166}


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
    #: The checked-in capture of the parsed source workbook (plan §3 row T1)
    #: and the identity that capture must declare. Both are stated, not
    #: derived from the target's name or the file's stem: a capture loaded
    #: under the wrong identity is a typed refusal, not a silent swap.
    source_input: Path
    source_input_ref: str
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
    label_overrides: Dict[str, str] = field(default_factory=dict)
    placeholder_inserters: Sequence[PlaceholderInserter] = field(default_factory=tuple)
    projected_zero_default_ids: Sequence[str] = field(default_factory=tuple)
    clear_cash_flow_artifact_historicals: bool = False
    #: Declared, not inferred from ``name``: the section-role and model-scale
    #: pass the live generic template runs and the reference build does not.
    apply_statement_section_roles: bool = False
    set_if_applicable_default_zero_historicals: bool = False
    strip_scenario_table_constant_overrides: bool = False
    normalize_cash_flow_projection_links: bool = False
    normalize_valuation_projection_links: bool = False


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
    # The cash flow statement carries one more row than the captured source
    # workbook: the DCF's unlevered free cash flow, inserted after free cash
    # flow, which pushes the rest of the sheet down one row.
    SectionSpec("cash_flow", "Cash Flow Statement", 132, 186, DriverCategory.other),
    SectionSpec("fcf_metrics", "FCF Metrics", 188, 189, DriverCategory.other),
)

SECTION_SPECS_GENERIC: Dict[str, Sequence[SectionSpec]] = {
    "Assumptions": ASSUMPTIONS_SECTIONS_GENERIC,
    "Financial_model": FINANCIAL_MODEL_SECTIONS_GENERIC,
}

SHEET_PREFIX = {"Assumptions": "tpl.a", "Financial_model": "tpl.fm"}


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
    "assumptions.stock_based_compensation_and_related_payroll_taxes": "stock_based_compensation_add_back",
    "assumptions.stock_based_compensation_and_related_payroll_taxes_r143": "stock_based_compensation_add_back",
    "financial_model.stock_based_compensation_and_related_payroll_taxes": "stock_based_compensation_add_back",
    "financial_model.stock_based_compensation_and_related_payroll_taxes_r33": "stock_based_compensation_add_back_adjustment",
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
    "financial_model.funds_held_for_client_s_cash_and_cash_equivalents": "cash_reconciliation_adjustment",
    "financial_model.reconciliation_of_cash_equivalents_and_client_funds": "reconciliation_of_cash_and_equivalents",
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
    "financial_model.deferred_income_tax_liabilities_r141",
    "financial_model.effect_of_exchange_rate_on_cash",
    "financial_model.other_cash_flows_from_financing",
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


__all__ = [
    "SIA_GENERIC_TEMPLATE_PATH",
    "KEPT_SHEETS",
    "EXPECTED_ITEM_COUNT_GENERIC",
    "EXPECTED_SECTION_COUNTS",
    "EXPECTED_SHEET_ITEM_COUNTS_GENERIC",
    "TemplateRole",
    "SectionSpec",
    "TemplateMetadataSpec",
    "PlaceholderInserter",
    "TemplateBuildConfig",
    "ASSUMPTIONS_SECTIONS_GENERIC",
    "FINANCIAL_MODEL_SECTIONS_GENERIC",
    "SECTION_SPECS_GENERIC",
    "SHEET_PREFIX",
    "CANONICAL_NAME_OVERRIDES_GENERIC",
    "CASH_FLOW_ARTIFACT_HISTORICAL_IDS",
    "IF_APPLICABLE_DEFAULT_ZERO_IDS",
    "CASH_FLOW_FULL_PROJECTION_IDS",
    "CASH_FLOW_PER_SHARE_PROJECTION_IDS",
    "CASH_FLOW_NET_INCOME_SOURCE_ID",
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
]
