"""Static section definitions for build diagnostics."""

from __future__ import annotations

from dataclasses import dataclass


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
            SectionMember(
                "tpl.a.dividends_shares.treasury_stock",
                "treasury_stock",
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


__all__ = [
    "BS_SECTIONS",
    "CF_SECTIONS",
    "IS_SECTIONS",
    "ParentCandidate",
    "SectionMember",
]
