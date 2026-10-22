"""Generic SIA template static configuration."""

from __future__ import annotations

from typing import Dict, Set

from ..models import (
    COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
)

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
    # Cash-flow and balance-sheet rows the forecast-control profile admits as
    # authored policy controls (schema/forecast_control_profile.py); the driver
    # resolver lowers a control only onto an input row.
    "financial_model.deferred_revenue_noncurrent",
    "financial_model.purchases_of_investments",
    "financial_model.sales_maturities_of_investments",
    "financial_model.other_changes_in_investing_activities",
    "financial_model.effect_of_exchange_rate_on_cash",
}

PROJECTED_ZERO_DEFAULT_TEMPLATE_IDS_GENERIC = (
    "tpl.fm.cash_flow.other_changes_in_investing_activities",
)

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

# Generic balance-sheet slots have no required role; their section and total
# membership belongs to the template rather than the statement-role catalog.
OPTIONAL_BALANCE_SHEET_SLOT_PARENT_TOTALS_GENERIC: Dict[str, str] = {
    "tpl.fm.balance_sheet.current_asset_4": "tpl.fm.balance_sheet.total_current_assets",
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
    "assumptions.long_term_asset_3": "[Long-term asset 3]",
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
    "financial_model.long_term_asset_3": "[Long-term asset 3]",
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
    "assumptions.long_term_asset_3": "Rename to the third long-term asset placeholder if needed.",
    "financial_model.company_name_ticker": "Replace with the target company name and ticker during template instantiation.",
    "financial_model.business_segment_1": "Rename to the primary business segment line.",
    "financial_model.business_segment_2": "Rename to the secondary business segment line if used.",
    "financial_model.current_asset_1": "Rename to the first current asset concept if needed.",
    "financial_model.current_asset_2": "Rename to the second current asset concept if needed.",
    "financial_model.current_asset_3": "Optional current asset input. Leave blank unless the company needs it.",
    "financial_model.current_asset_4": "Optional current asset input for company-specific current assets.",
    "financial_model.current_liability_1": "Optional current liability input. Use for deferred revenue or another current liability if needed.",
    "financial_model.software_investment_if_applicable": "Rename if the company uses a different software investment label or omit if not applicable.",
    "financial_model.long_term_asset_1": "Rename to the first long-term asset placeholder if needed.",
    "financial_model.long_term_asset_2": "Rename to the second long-term asset placeholder if needed.",
    "financial_model.long_term_asset_3": "Rename to the third long-term asset placeholder if needed.",
    "financial_model.other_cash_flows_from_financing": "Optional financing catch-all row. Leave at zero unless needed.",
}

LABEL_OVERRIDES_GENERIC: Dict[str, str] = {
    "assumptions.amortization_of_capitalized_internal_use_software": "Amortization of capitalized software",
    "assumptions.beg_capitalized_internal_use_software": "  Beg: Capitalized software",
    "assumptions.end_capitalized_internal_use_software": "  End: Capitalized software",
    "assumptions.avg_capitalized_internal_use_software": "  Avg. Capitalized software",
    "assumptions.amortization_of_beginning_capitalized_internal_use_software": "   % Amortization of beginning capitalized software",
    "assumptions.capitalized_portion_of_r_d_as_of_internal_use_software_costs": "Capitalized portion of R&D as % of capitalized software costs",
    "financial_model.current_asset_2_r146": " Change in inventory",
    "financial_model.long_term_operating_lease_liabilities": " Long-term lease liabilities",
    "financial_model.reconciliation_of_cash_equivalents_and_client_funds": "Reconciliation of cash and equivalents",
    "financial_model.funds_held_for_client_s_cash_and_cash_equivalents": " Cash reconciliation adjustment",
    "assumptions.stock_based_compensation_and_related_payroll_taxes": "Stock-based compensation add-back",
    "assumptions.stock_based_compensation_and_related_payroll_taxes_r143": "Stock-based compensation add-back",
    "financial_model.stock_based_compensation_and_related_payroll_taxes": "Stock-based compensation add-back",
    "financial_model.stock_based_compensation_and_related_payroll_taxes_r33": "Stock-based compensation add-back",
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



__all__ = [
    "EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC",
    "SCENARIO_LINKED_IDS_GENERIC",
    "_SCENARIO_SELECTOR_ID",
    "_COLUMN_OFFSET_MODE_PERIOD_RELATIVE",
    "_SCENARIO_LINKED_OFFSET_ANCHORS",
    "SCENARIO_TABLE_IDS_GENERIC",
    "INPUT_IDS_GENERIC",
    "KEY_DRIVER_IDS_GENERIC",
    "OPTIONAL_IDS_GENERIC",
    "OPTIONAL_BALANCE_SHEET_SLOT_PARENT_TOTALS_GENERIC",
    "TEMPLATE_TOKENS_GENERIC",
    "BUILD_NOTES_GENERIC",
    "LABEL_OVERRIDES_GENERIC",
    "HEADER_DEPENDENCY_NOTES_GENERIC",
    "REPEAT_GROUP_ROLES_GENERIC",
    "REPEAT_GROUP_IDS_GENERIC",
]
