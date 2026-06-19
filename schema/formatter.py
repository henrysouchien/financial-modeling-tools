"""Formatting rules for rendered financial models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .models import CellColor, ItemType, LineItem


_PERCENTAGE_KEYWORDS = (
    "_growth",
    "_margin",
    "_pct_revenue",
    "_pct_",
    "_rate",
    "payout_ratio",
    "yield",
    "_conversion",
    "return_on_",
)
_SCENARIO_PERCENTAGE_SUFFIXES = (
    "volume_bull",
    "volume_base",
    "volume_bear",
    "margin_bull",
    "margin_base",
    "margin_bear",
)
_RATIO_KEYWORDS = (
    "debt_to_",
    "_to_ebitda",
    "_to_revenue",
    "_ratio",
    "_multiple",
    "_turns",
)
_SHARE_COUNT_KEYWORDS = (
    "shares_outstanding",
    "diluted_shares",
    "dilutive_shares",
    "shares_issued",
    "weighted_average_diluted",
)
_TOTAL_ROW_SUFFIXES = {
    "income_statement.total_revenue",
    "income_statement.gross_profit",
    "income_statement.total_operating_expenses",
    "income_statement.operating_income",
    "income_statement.net_income",
    "adjusted_earnings.ebitda",
    "adjusted_earnings.adjusted_ebitda",
    "adjusted_earnings.adjusted_operating_income",
    "adjusted_earnings.adjusted_net_income",
    "balance_sheet.total_current_assets_before_funds_held_for_clients",
    "balance_sheet.total_current_assets",
    "balance_sheet.total_assets",
    "balance_sheet.total_current_liabilities_before_client_fund_obligations",
    "balance_sheet.total_current_liabilities",
    "balance_sheet.total_liabilities",
    "balance_sheet.total_equity",
    "balance_sheet.total_liabilities_and_equity",
    "cash_flow.operating_cash_flow",
    "cash_flow.investing_cash_flow",
    "cash_flow.financing_cash_flow",
    "cash_flow.free_cash_flow",
}


def _label_implies_percentage(label: str) -> bool:
    normalized = f" {str(label or '').strip().lower()} "
    stripped = normalized.strip()
    return (
        "(%" in stripped
        or stripped.startswith("%")
        or "% of " in stripped
        or "% change" in stripped
        or "% chg" in stripped
        or " as % " in normalized
    )


@dataclass
class ModelFormatter:
    """Configurable formatting rules for rendered financial models."""

    style_guide_id: str = "pcty_house_financial_model_v1"
    style_guide_source_reference: str = "PCTY-model.xlsx benchmark inspected 2026-06-03"

    dollar_format: str = "#,##0"
    percentage_format: str = "0.0%"
    per_share_format: str = "#,##0.00"
    share_count_format: str = "#,##0.0"
    ratio_format: str = "0.0x"

    constant_color: str = "#0000FF"
    formula_color: str = "#000000"
    header_color: str = "#000000"
    key_driver_color: str = "#008000"
    input_color: str = "#0000FF"
    section_header_fill_color: str | None = "#FFFF00"
    section_header_fill_sheets: tuple[str, ...] | None = ("Assumptions",)
    section_header_border_color: str | None = "#000000"
    section_header_border_sheets: tuple[str, ...] | None = ("Assumptions", "Financial_model")
    input_fill_color: str | None = "#FFFF00"
    input_fill_sheets: tuple[str, ...] | None = ("Assumptions", "Valuation", "Scenarios")
    total_separator_border_color: str | None = "#000000"
    total_separator_border_sheets: tuple[str, ...] | None = ("Assumptions", "Financial_model")
    underline_before_totals: bool = False
    summary_sheet_enabled: bool = True
    summary_sheet_name: str = "Summary"
    summary_max_key_drivers: int = 8
    summary_missing_value: str = "Not available"

    def number_format_for(self, item: LineItem) -> Optional[str]:
        if item.item_type in {ItemType.header, ItemType.spacer}:
            return None

        item_id = item.id.lower()
        label = item.label.lower()

        if any(keyword in item_id for keyword in _PERCENTAGE_KEYWORDS):
            return self.percentage_format
        if _label_implies_percentage(label):
            return self.percentage_format
        if any(item_id.endswith(suffix) for suffix in _SCENARIO_PERCENTAGE_SUFFIXES):
            return self.percentage_format
        if any(keyword in item_id for keyword in _RATIO_KEYWORDS):
            return self.ratio_format
        if ("eps" in item_id or "per_share" in item_id) and "share_repurchase" not in item_id:
            return self.per_share_format
        if any(keyword in item_id for keyword in _SHARE_COUNT_KEYWORDS):
            return self.share_count_format
        return self.dollar_format

    def label_color_for(self, item: LineItem) -> Optional[str]:
        """Labels are always black — blue/black convention applies to data cells only."""
        return self.header_color

    def data_color_for(self, is_formula: bool, item: LineItem) -> str:
        if item.style is not None and item.style.color is CellColor.key_driver:
            return self.key_driver_color
        return self.formula_color if is_formula else self.constant_color

    def is_total_row(self, item_id: str) -> bool:
        lowered = item_id.lower()
        return any(lowered.endswith(suffix) for suffix in _TOTAL_ROW_SUFFIXES)


SIA_FORMATTER = ModelFormatter()


__all__ = ["ModelFormatter", "SIA_FORMATTER"]
