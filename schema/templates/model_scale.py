"""Template helpers for explicit financial model scale metadata."""

from __future__ import annotations

import hashlib
import json
from typing import Iterable

from ..formatter import SIA_FORMATTER
from ..models import FormulaSpec, FormulaType, ItemType, LineItem, ModelScale
from industry_slice_core.industry_types import Unit


PRICE_OR_UNIT_DOLLAR_ROW_IDS = {
    "tpl.a.revenue_drivers.price_driver_1",
    "tpl.a.revenue_drivers.business_segment_2_price_driver_1",
    "tpl.a.dividends_shares.expected_average_share_price",
}
FORCED_STATEMENT_DOLLAR_ROW_IDS = {
    "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash",
}

_NON_VALUE_ID_TOKENS = (
    ".header.",
    "_header",
    "_ticker",
    ".ticker",
    "scenario_value",
    "scenario_selector",
)
_UNIT_SCALE_ID_TOKENS = (
    "volume_driver",
    "operating_metric",
    "days_sales_outstanding",
    "days_payable_outstanding",
    "revenue_per_",
    "per_unit",
    "average_share_price",
    "share_price",
    ".expected_value.",
)
_UNIT_SCALE_LABEL_TOKENS = (
    "price",
    "per share",
    "per client",
    "per unit",
    "days sales outstanding",
    "days payable outstanding",
)


# Closed, reviewed semantic-unit migration for the two canonical SIA template
# artifacts. This registry is intentionally stable-ID based: generation and
# loading never infer a semantic unit from labels or identifier fragments.
_PERCENTAGE_UNIT_ITEM_IDS = frozenset(
    {
        "tpl.a.adj_ebitda.adjusted_ebitda_margin",
        "tpl.a.adj_ebitda.adjusted_ebitda_margins",
        "tpl.a.adj_ebitda.incremental_adj_ebitda_margin",
        "tpl.a.adj_operating_income.adj_operating_margin",
        "tpl.a.adj_operating_income.adjusted_operating_income_growth",
        "tpl.a.adj_operating_income.adjusted_operating_margin",
        "tpl.a.adj_operating_income.incremental_adj_operating_margin",
        "tpl.a.adj_operating_income.y_y_chg",
        "tpl.a.balance_sheet_wc.accounts_payable_pct_cogs_and_sga",
        "tpl.a.balance_sheet_wc.accounts_receivable_pct_revenue",
        "tpl.a.balance_sheet_wc.accrued_expenses_pct_cogs_and_sga",
        "tpl.a.balance_sheet_wc.deferred_contract_costs_pct_sales_and_marketing",
        "tpl.a.balance_sheet_wc.of_cogs_and_sg_a",
        "tpl.a.balance_sheet_wc.of_operating_line_adjust_link_as_neccesary",
        "tpl.a.balance_sheet_wc.of_revenue",
        "tpl.a.balance_sheet_wc.prepaid_expenses_and_other_pct_cogs_and_sga",
        "tpl.a.capital_investments.capitalized_internal_use_software_growth",
        "tpl.a.capital_investments.capitalized_internal_use_software_pct_revenue",
        "tpl.a.capital_investments.capitalized_portion_of_r_d_as_of_internal_use_software_costs",
        "tpl.a.capital_investments.capitalized_research_and_development_pct_internal_use_software",
        "tpl.a.capital_investments.change_y_y",
        "tpl.a.capital_investments.of_revenue",
        "tpl.a.capital_investments.property_and_equipment_growth",
        "tpl.a.capital_investments.property_and_equipment_pct_revenue",
        "tpl.a.capital_investments.r_d_as_of_revenue",
        "tpl.a.capital_investments.research_and_development_pct_revenue",
        "tpl.a.capital_sources.deferred_income_tax_liabilities_pct_operating_expense",
        "tpl.a.capital_sources.long_term_operating_lease_liabilities_pct_general_and_administrative",
        "tpl.a.capital_sources.of_general_and_administrative",
        "tpl.a.capital_sources.of_operating_expense",
        "tpl.a.capital_sources.other_long_term_liabilities_pct_general_and_administrative",
        "tpl.a.depreciation_amortization.amortization",
        "tpl.a.depreciation_amortization.amortization_of_beginning_capitalized_internal_use_software",
        "tpl.a.depreciation_amortization.amortization_pct_beginning_capitalized_internal_use_software",
        "tpl.a.depreciation_amortization.amortization_pct_intangibles",
        "tpl.a.depreciation_amortization.as_of_revenues",
        "tpl.a.depreciation_amortization.chg_y_y",
        "tpl.a.depreciation_amortization.depreciation_as_of_beginning_property_and_equipment",
        "tpl.a.depreciation_amortization.depreciation_growth",
        "tpl.a.depreciation_amortization.depreciation_pct_beginning_property_and_equipment",
        "tpl.a.depreciation_amortization.depreciation_pct_revenue",
        "tpl.a.dividends_shares.consensus_eps_growth",
        "tpl.a.free_cash_flow.as_of_adjusted_ebitda",
        "tpl.a.free_cash_flow.free_cash_flow_pct_adjusted_ebitda",
        "tpl.a.operating_leverage.general_and_administrative_pct_revenue",
        "tpl.a.operating_leverage.incremental_operating_margin",
        "tpl.a.operating_leverage.operating_income_growth",
        "tpl.a.operating_leverage.operating_margin",
        "tpl.a.operating_leverage.research_and_development_pct_revenue",
        "tpl.a.operating_leverage.sales_and_marketing_pct_revenue",
        "tpl.a.operating_leverage.total_operating_expenses_pct_revenue",
        "tpl.a.operating_leverage.y_y_chg",
        "tpl.a.other_assets.cash_equivalents_pct_funds_held_for_clients",
        "tpl.a.other_assets.deferred_income_tax_assets_pct_operating_expense",
        "tpl.a.other_assets.long_term_asset_2_pct_operating_line",
        "tpl.a.other_assets.long_term_deferred_contract_costs_pct_sales_and_marketing",
        "tpl.a.other_assets.long_term_prepaid_expenses_and_other_pct_general_and_administrative",
        "tpl.a.other_assets.of_general_and_administrative",
        "tpl.a.other_assets.of_operating_expense",
        "tpl.a.other_assets.of_operating_line_adjust_link_as_neccesary",
        "tpl.a.other_assets.operating_lease_right_of_use_assets_pct_general_and_administrative",
        "tpl.a.other_income_interest.interest_rate",
        "tpl.a.revenue_drivers.business_segment_1_growth",
        "tpl.a.revenue_drivers.business_segment_2_growth",
        "tpl.a.revenue_drivers.business_segment_2_price_driver_1",
        "tpl.a.revenue_drivers.business_segment_2_price_growth",
        "tpl.a.revenue_drivers.business_segment_2_volume_growth",
        "tpl.a.revenue_drivers.operating_metric_growth",
        "tpl.a.revenue_drivers.price_1_growth",
        "tpl.a.revenue_drivers.total_revenue_growth",
        "tpl.a.revenue_drivers.volume_1_growth",
        "tpl.a.revenue_drivers.volume_2_growth",
        "tpl.a.revenue_drivers.volume_3_growth",
        "tpl.a.scenario_tables.margin_base",
        "tpl.a.scenario_tables.margin_bear",
        "tpl.a.scenario_tables.margin_bull",
        "tpl.a.scenario_tables.volume_base",
        "tpl.a.scenario_tables.volume_bear",
        "tpl.a.scenario_tables.volume_bull",
        "tpl.a.stock_based_compensation.cost_of_revenues_pct_line_item",
        "tpl.a.stock_based_compensation.general_and_administrative_pct_line_item",
        "tpl.a.stock_based_compensation.research_and_development_pct_line_item",
        "tpl.a.stock_based_compensation.sales_and_marketing_pct_line_item",
        "tpl.a.stock_based_compensation.stock_based_compensation_growth",
        "tpl.a.stock_based_compensation.stock_based_compensation_pct_revenue",
        "tpl.a.stock_based_compensation.y_y_chg",
        "tpl.a.tax_net_income.adjusted_eps_growth",
        "tpl.a.tax_net_income.change_y_y",
        "tpl.a.tax_net_income.difference_from_gaap_rate",
        "tpl.a.tax_net_income.diluted_shares_growth",
        "tpl.a.tax_net_income.effective_tax_rate",
        "tpl.a.tax_net_income.tax_rate",
        "tpl.a.tax_net_income.y_y_chg",
        "tpl.a.unit_economics.gross_margin",
        "tpl.a.unit_economics.gross_profit_growth",
        "tpl.a.unit_economics.y_y_chg",
        "tpl.fm.balance_sheet.book_value_per_share_growth",
        "tpl.fm.balance_sheet.change",
        "tpl.fm.balance_sheet.free_cash_flow_per_share_growth",
        "tpl.fm.balance_sheet.incremental_ebitda_margin",
        "tpl.fm.balance_sheet.incremental_ebitda_margins",
        "tpl.fm.balance_sheet.incremental_operating_margin",
        "tpl.fm.balance_sheet.incremental_operating_margins",
        "tpl.fm.balance_sheet.net_cash_per_share_growth",
        "tpl.fm.balance_sheet.return_on_equity",
        "tpl.fm.balance_sheet.return_on_equity_roe",
        "tpl.fm.balance_sheet.return_on_invested_capital",
        "tpl.fm.balance_sheet.return_on_invested_capital_roic",
        "tpl.fm.fcf_metrics.free_cash_flow_conversion",
        "tpl.fm.fcf_metrics.free_cash_flow_conversion_fcf_ebitda",
        "tpl.fm.fcf_metrics.free_cash_flow_margin",
        "tpl.fm.fcf_metrics.free_cash_flow_per_share_growth",
        "tpl.fm.growth_rates.adj_ebitda",
        "tpl.fm.growth_rates.adjusted_ebitda_growth",
        "tpl.fm.growth_rates.adjusted_eps",
        "tpl.fm.growth_rates.adjusted_eps_growth",
        "tpl.fm.growth_rates.business_segment_1_growth",
        "tpl.fm.growth_rates.business_segment_2_growth",
        "tpl.fm.growth_rates.eps",
        "tpl.fm.growth_rates.eps_growth",
        "tpl.fm.growth_rates.net_income",
        "tpl.fm.growth_rates.net_income_growth",
        "tpl.fm.growth_rates.operating_expenses_growth",
        "tpl.fm.growth_rates.operating_income",
        "tpl.fm.growth_rates.operating_income_growth",
        "tpl.fm.growth_rates.pretax_income",
        "tpl.fm.growth_rates.pretax_income_growth",
        "tpl.fm.growth_rates.revenue_growth",
        "tpl.fm.growth_rates.revenues",
        "tpl.fm.growth_rates.total_operating_expenses",
        "tpl.fm.margins.adj_ebtida_margin",
        "tpl.fm.margins.adj_operating_margin",
        "tpl.fm.margins.adjusted_ebitda_margin",
        "tpl.fm.margins.business_segment_1_pct_revenue",
        "tpl.fm.margins.business_segment_2_pct_revenue",
        "tpl.fm.margins.gross_profit_margin",
        "tpl.fm.margins.net_margin",
        "tpl.fm.margins.operating_margin",
        "tpl.fm.margins.pretax_margin",
        "tpl.fm.margins.tax_rate",
    }
)
_SHARE_COUNT_UNIT_ITEM_IDS = frozenset(
    {
        "tpl.a.dividends_shares.diluted_shares_outstanding",
        "tpl.a.dividends_shares.dilutive_shares_weighted_average",
        "tpl.a.dividends_shares.end_of_period_shares_outstanding",
        "tpl.a.dividends_shares.shares_issued",
        "tpl.a.dividends_shares.weighted_average_diluted_shares_outstanding",
        "tpl.a.tax_net_income.diluted_shares",
        "tpl.a.tax_net_income.diluted_shares_outstanding",
        "tpl.fm.income_statement.diluted_shares",
        "tpl.fm.income_statement.diluted_shares_outstanding_m",
        "tpl.fm.income_statement.basic_shares_outstanding_m",
    }
)
_PER_SHARE_UNIT_ITEM_IDS = frozenset(
    {
        "tpl.a.tax_net_income.adjusted_eps",
        "tpl.fm.adjusted_earnings.adjusted_eps",
        "tpl.fm.balance_sheet.book_value_per_share",
        "tpl.fm.balance_sheet.free_cash_flow_per_share",
        "tpl.fm.balance_sheet.net_cash_per_share",
        "tpl.fm.cash_flow.free_cash_flow_per_share",
        "tpl.fm.fcf_metrics.free_cash_flow_per_share",
        "tpl.fm.income_statement.eps",
        "tpl.fm.income_statement.eps_diluted",
        "tpl.fm.income_statement.eps_basic",
    }
)
_RATIO_UNIT_ITEM_IDS = frozenset(
    {
        "tpl.fm.balance_sheet.debt_ebitda_ratio",
        "tpl.fm.balance_sheet.debt_to_ebitda",
    }
)
_DATE_UNIT_ITEM_IDS = frozenset({"tpl.a.header.year_header"})
_TEXT_WITHOUT_SEMANTIC_UNIT_ITEM_IDS = frozenset(
    {
        "tpl.a.header.units_header",
        "tpl.a.scenario_tables.scenario_margin_label",
        "tpl.a.scenario_tables.scenario_volume_growth_label",
        "tpl.fm.header.millions_except_per_share_data",
        "tpl.fm.header.units_header",
        "tpl.fm.balance_sheet.balance_sheet_m",
    }
)

KNOWN_SEMANTIC_UNIT_HEURISTIC_FALSE_POSITIVE_IDS = frozenset(
    {
        "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash",
        "tpl.v.dcf.terminal_value_growth",
        "tpl.v.dcf.terminal_value_multiple",
        "tpl.v.dcf.terminal_year_fcf_growth",
    }
)

TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID: dict[str, Unit | None] = {
    **dict.fromkeys(_PERCENTAGE_UNIT_ITEM_IDS, Unit.percentage),
    **dict.fromkeys(_SHARE_COUNT_UNIT_ITEM_IDS, Unit.share_count),
    **dict.fromkeys(_PER_SHARE_UNIT_ITEM_IDS, Unit.per_share),
    **dict.fromkeys(_RATIO_UNIT_ITEM_IDS, Unit.ratio),
    **dict.fromkeys(_DATE_UNIT_ITEM_IDS, Unit.date),
    **dict.fromkeys(_TEXT_WITHOUT_SEMANTIC_UNIT_ITEM_IDS, None),
}

_REGISTERED_UNIT_GROUPS = (
    _PERCENTAGE_UNIT_ITEM_IDS,
    _SHARE_COUNT_UNIT_ITEM_IDS,
    _PER_SHARE_UNIT_ITEM_IDS,
    _RATIO_UNIT_ITEM_IDS,
    _DATE_UNIT_ITEM_IDS,
    _TEXT_WITHOUT_SEMANTIC_UNIT_ITEM_IDS,
)
if sum(len(group) for group in _REGISTERED_UNIT_GROUPS) != len(
    TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID
):
    raise RuntimeError("template semantic-unit registry contains overlapping IDs")
if KNOWN_SEMANTIC_UNIT_HEURISTIC_FALSE_POSITIVE_IDS.intersection(
    TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID
):
    raise RuntimeError("known semantic-unit false positives must remain unregistered")


def _semantic_unit_registry_payload() -> dict[str, str | None]:
    return {
        item_id: unit.value if unit is not None else None
        for item_id, unit in sorted(TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID.items())
    }


TEMPLATE_SEMANTIC_UNIT_REGISTRY_SHA256 = hashlib.sha256(
    json.dumps(
        _semantic_unit_registry_payload(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


def template_semantic_unit_mismatches(
    model,
) -> tuple[tuple[str, Unit | None, Unit | None], ...]:
    """Return exact stable-ID unit drift without mutating the model."""

    mismatches: list[tuple[str, Unit | None, Unit | None]] = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                if item.id not in TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID:
                    continue
                expected = TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID[item.id]
                if item.unit is not expected:
                    mismatches.append((item.id, expected, item.unit))
    return tuple(sorted(mismatches, key=lambda row: row[0]))


def assert_template_semantic_units(model, *, origin: str) -> None:
    """Reject persisted/builder state that drifts from the closed registry."""

    mismatches = template_semantic_unit_mismatches(model)
    if not mismatches:
        return
    sample = ", ".join(
        f"{item_id}:expected={getattr(expected, 'value', None)!r}:"
        f"actual={getattr(actual, 'value', None)!r}"
        for item_id, expected, actual in mismatches[:10]
    )
    raise ValueError(
        "REBUILD_REQUIRED_MODEL_BUNDLE_SEMANTIC_UNITS "
        f"origin={origin} mismatch_count={len(mismatches)} sample={sample}"
    )


def apply_template_semantic_units(model) -> tuple[str, ...]:
    """Apply the closed semantic-unit registry to IDs present in ``model``."""

    applied: list[str] = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                if item.id not in TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID:
                    continue
                semantic_unit = TEMPLATE_SEMANTIC_UNIT_BY_ITEM_ID[item.id]
                item.unit = semantic_unit
                if semantic_unit is None:
                    item.model_scale = "units"
                applied.append(item.id)
    return tuple(sorted(applied))


def _iter_formula_specs(item: LineItem) -> Iterable[FormulaSpec]:
    if item.historical is not None:
        yield item.historical
    if item.projected is not None:
        yield item.projected
    if item.overrides:
        yield from item.overrides.values()


def _is_string_constant(spec: FormulaSpec) -> bool:
    return spec.type is FormulaType.constant and isinstance(spec.params.get("value"), str)


def _is_non_value_row(item: LineItem) -> bool:
    item_id = item.id.lower()
    if any(token in item_id for token in _NON_VALUE_ID_TOKENS):
        return True
    specs = list(_iter_formula_specs(item))
    return bool(specs) and all(
        _is_string_constant(spec) or spec.type is FormulaType.raw for spec in specs
    )


def requires_template_model_scale(item: LineItem) -> bool:
    """Return whether a template row must carry explicit scale metadata."""

    if item.item_type in {ItemType.header, ItemType.spacer}:
        return False
    if item.unit is not Unit.dollars:
        return False
    if (
        item.id not in FORCED_STATEMENT_DOLLAR_ROW_IDS
        and SIA_FORMATTER.number_format_for(item) != SIA_FORMATTER.dollar_format
    ):
        return False
    return not _is_non_value_row(item)


def infer_template_model_scale(item: LineItem) -> ModelScale | None:
    """Infer template-row scale for generated SIA artifacts."""

    if not requires_template_model_scale(item):
        return None

    item_id = item.id.lower()
    label = item.label.lower()
    if item.id in FORCED_STATEMENT_DOLLAR_ROW_IDS:
        return "millions"
    if item.id in PRICE_OR_UNIT_DOLLAR_ROW_IDS:
        return "units"
    if any(token in item_id for token in _UNIT_SCALE_ID_TOKENS):
        return "units"
    if any(token in label for token in _UNIT_SCALE_LABEL_TOKENS):
        return "units"
    return "millions"


def apply_template_model_scales(model) -> None:
    """Populate explicit scale metadata for generated template rows."""

    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                scale = infer_template_model_scale(item)
                if scale is not None:
                    item.model_scale = scale


def assert_template_model_scales_declared(model) -> None:
    """Fail if any value-bearing dollar template row lacks explicit scale metadata."""

    missing: list[str] = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                if requires_template_model_scale(item) and "model_scale" not in item.model_fields_set:
                    missing.append(item.id)
    if missing:
        sample = ", ".join(sorted(missing)[:20])
        raise AssertionError(f"value-bearing dollar template rows missing model_scale: {sample}")


def require_declared_model_scale(item: LineItem) -> None:
    """Validate a builder-created item declares scale when the row requires it."""

    if requires_template_model_scale(item) and "model_scale" not in item.model_fields_set:
        raise ValueError(f"{item.id} is a value-bearing dollar row and must declare model_scale")
