"""Section definitions for build diagnostics.

The balance-sheet sections share their membership with the template totals:
required-role ``parent_total`` rows own named lines, and the generic template
configuration owns optional slots with no role. Both lanes of each total and
its diagnostic section read ``balance_sheet_total_members``. The section names
and XBRL parents its presentation is recovered from live here.
"""

from __future__ import annotations

from dataclasses import dataclass

from .required_role_catalog import load_required_role_catalog
from .templates.template_builder_config_generic import (
    OPTIONAL_BALANCE_SHEET_SLOT_PARENT_TOTALS_GENERIC,
)


@dataclass(frozen=True)
class SectionMember:
    template_item_id: str


@dataclass(frozen=True)
class ParentCandidate:
    parent: str
    exclude_tags: tuple[str, ...]
    requires_companion: str | None = None
    requires_no_abstract: tuple[str, ...] = ()


def balance_sheet_total_members(total_item_id: str) -> tuple[str, ...]:
    """A template total's named face lines and generic optional slots."""

    slots = (
        item_id
        for item_id, parent_total in OPTIONAL_BALANCE_SHEET_SLOT_PARENT_TOTALS_GENERIC.items()
        if parent_total == total_item_id
    )
    return tuple(
        dict.fromkeys((*load_required_role_catalog().total_members(total_item_id), *slots))
    )


def _section_members(total_item_id: str) -> tuple[list[SectionMember], str | None]:
    """A total's leaf lines and the one subtotal that rolls into it, if any."""

    catalog = load_required_role_catalog()
    totals = frozenset(catalog.statement_totals())
    lines = balance_sheet_total_members(total_item_id)
    subtotals = [line for line in lines if line in totals]
    return (
        [SectionMember(line) for line in lines if line not in totals],
        subtotals[0] if subtotals else None,
    )


def _balance_sheet_section(
    total_item_id: str,
    *,
    xbrl_section_parents: list[ParentCandidate],
    presentation_only: bool = False,
) -> dict:
    definition: dict = {}
    if presentation_only:
        definition["sub_lines"] = []
        definition["total_item_id"] = total_item_id
        definition["presentation_only"] = True
        definition["emit_missing_parent"] = False
    else:
        sub_lines, subtotal = _section_members(total_item_id)
        definition["sub_lines"] = sub_lines
        definition["total_item_id"] = total_item_id
        if subtotal is not None:
            definition["also_includes_subtotal"] = subtotal
    definition["xbrl_section_parents"] = xbrl_section_parents
    return definition


BS_SECTIONS = {
    "current_assets": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_current_assets",
        xbrl_section_parents=[
            ParentCandidate(
                parent="us-gaap:AssetsCurrentAbstract",
                exclude_tags=("us-gaap:AssetsCurrent",),
            ),
        ],
    ),
    "non_current_assets": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_assets",
        xbrl_section_parents=[
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
    ),
    "assets_combined": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_assets",
        presentation_only=True,
        xbrl_section_parents=[
            ParentCandidate(
                parent="us-gaap:AssetsAbstract",
                exclude_tags=("us-gaap:Assets",),
                requires_no_abstract=("us-gaap:AssetsCurrentAbstract",),
            ),
        ],
    ),
    "current_liabilities": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_current_liabilities",
        xbrl_section_parents=[
            ParentCandidate(
                parent="us-gaap:LiabilitiesCurrentAbstract",
                exclude_tags=("us-gaap:LiabilitiesCurrent",),
            ),
        ],
    ),
    "non_current_liabilities": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_liabilities",
        xbrl_section_parents=[
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
    ),
    "liabilities_combined": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_liabilities",
        presentation_only=True,
        xbrl_section_parents=[
            ParentCandidate(
                parent="us-gaap:LiabilitiesAbstract",
                exclude_tags=("us-gaap:Liabilities",),
                requires_no_abstract=("us-gaap:LiabilitiesCurrentAbstract",),
            ),
        ],
    ),
    "stockholders_equity": _balance_sheet_section(
        "tpl.fm.balance_sheet.total_equity",
        xbrl_section_parents=[
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
    ),
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
    "balance_sheet_total_members",
    "CF_SECTIONS",
    "IS_SECTIONS",
    "ParentCandidate",
    "SectionMember",
]
