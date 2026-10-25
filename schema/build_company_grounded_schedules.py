"""Company-grounded forecast schedule policies for exact artifact builds.

These policies are deliberately narrower than forecast judgment.  They only
activate when the compiled model already contains the accepted company's own
historical anchors and the exact ModelSpec/BMR controls needed by the formula.
Every output remains a live formula and carries an explicit derivation owner.

Entered through ``schema/build.py`` and ``model_build/target_build.py`` with the compiled
model, ``BusinessModelRevision``, driver registry and role dispositions;
``apply_company_grounded_schedule_policies`` mutates the ``FinancialModel`` in
place and returns the wired items, source-fact coverage and diagnostics. Shared
schedule helpers come from ``build_financial_schedules``. See ``schema/README.md``.
"""

from __future__ import annotations

import copy
import math
import re
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, TypeGuard, TypedDict, cast

from schema.business_model import DriverNode, declared_balance_role
from schema.business_model_revision import BusinessModelRevision
from schema.build_model_product_contracts import (
    AnnualRequirementCoverage,
    BuildDiagnostic,
    HistoricalBasisFact,
    SourceEvidence,
    validate_historical_reconciliation_bindings,
)
from schema.coverage_profile import (
    AbsenceCitation,
    AnnualSourceRequirement,
    PeriodPinCell,
)
from schema.provider_port.adapters.edgar.wire.non_gaap_bridge_v2 import bridge_row_key

from industry_slice_core.industry_types import Unit

from .build_financial_schedules import (
    _bind_or_insert_item,
    _formula_ref_ids,
    _protected_cell,
    _protected_line,
    _remove_formula_ref,
    complete_capitalized_software_schedule,
    lower_interest_inclusion,
    _software_inclusion_fact,
    _SOFTWARE_INVESTING_CASH_ITEM_ID,
    _SOFTWARE_ADDITIONS_ROLE,
    _SOFTWARE_AMORTIZATION_ROLE,
    _SOFTWARE_BALANCE_ROLE,
    _software_inside_ppe,
    _software_inside_ppe_at_terminal,
)
from .build_formula_refs import _extract_ref_ids, _extract_single_ref, saved_formula_slots
from .build_semantic_rows_tax import (
    _DEFERRED_TAX_LIABILITY_ITEM_ID,
    _deferred_tax_cash_flow_terms,
    _deferred_tax_inclusion_fact,
    _asset_change,
    _liability_change,
    _refresh_deferred_tax_formulas,
)
from .role_disposition import (
    RoleDisposition,
    RoleDispositionState,
    RoleDispositionTable,
    RoleItems,
)
from .business_model_compiler import CompiledDriverRegistry
from .dependency_graph import DependencyGraph
from .renderer_columns import item_owns_cell
from .finding_vocabulary import Finding
from .handle import ComputedValues
from .build_population import _SCALE_FACTORS
from .required_role_catalog import (
    LeaseReconciliationShape,
    RequiredRole,
    balance_roles,
    face_lines,
    load_required_role_catalog,
    node_driven_balance_roles,
    pass_through_counterparts,
)
from .templates.template_builder_insertions import (
    ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID,
    BALANCE_SHEET_FACE_ROWS,
    NCI_SHARE_RATE_ITEM_ID,
    NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID,
    OPTIONAL_SLOT_CARRY_FORWARD_NOTE,
    SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
    SGA_DRIVER_BASE_ITEM_ID,
    TEMPLATE_PROJECTED_ZERO_DEFAULT_NOTE,
)
from .build_value_writers import (
    _authored_periods,
    _set_constant_override,
    _set_imported_value,
    _write_default_unless_authored,
)
from .build_diagnostic_sections import BS_SECTIONS, balance_sheet_total_members
from .models import (
    CustomizationType,
    FinancialStatement,
    StatementSectionRole,
    FinancialModel,
    FormulaDerivation,
    FormulaDerivationStrategy,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ValueCell,
    ValueProvenance,
    operation_kind,
)

if TYPE_CHECKING:
    from .build_bm_node_history import OpeningBalance
    from .refs import FormulaRef


_OWNER = "build_company_grounded_schedules"
_REVENUE = "tpl.fm.income_statement.total_revenue"

_OTHER_CURRENT_ASSETS = "tpl.fm.balance_sheet.current_asset_3"
# Policy parameter rows (W1): each per-issuer revenue ratio below is a typed
# input row the formula reads, never a literal inside it. An authored
# forecast-assumption row lands ``input`` there; the house ratio otherwise
# lands ``default`` and raises ``judgment_absent_default_used``.
_OTHER_CURRENT_ASSETS_RATE = "tpl.a.balance_sheet_wc.prepaid_pct_revenue"
_OTHER_CURRENT_ASSETS_RATE_ANCHOR = "tpl.a.balance_sheet_wc.of_rev"
_LEASE_CURRENT_RATE = (
    "tpl.a.capital_sources.current_operating_lease_liabilities_pct_revenue"
)
_PHYSICAL_DEPRECIATION_RATE = "tpl.a.depreciation_amortization.depreciation_pct_revenue"
_REPURCHASE_RATE = "tpl.a.dividends_shares.share_repurchases_pct_revenue"
_EMPLOYEE_PLAN_PROCEEDS_RATE = (
    "tpl.a.dividends_shares.employee_stock_plan_proceeds_pct_revenue"
)
_DIVIDENDS_RATE = "tpl.a.dividends_shares.dividends_paid_pct_revenue"

_FACE_BALANCE_SHEET_PREFIX = "tpl.fm.balance_sheet."
_FACE_INCOME_STATEMENT_PREFIX = "tpl.fm.income_statement."
_PRETAX_INCOME = "tpl.fm.income_statement.income_before_income_taxes"
_INCOME_TAX_PROVISION = "tpl.fm.income_statement.income_tax_expense_benefit"
#: Rows an optional income-statement face row drives; they leave the model with it.
_OPTIONAL_INCOME_STATEMENT_ROW_SCHEDULES: Mapping[str, tuple[str, ...]] = MappingProxyType({
    NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID: (NCI_SHARE_RATE_ITEM_ID,),
})
_OTHER_WORKING_CAPITAL_ROLE = "statement.cf.change_in_other_working_capital"
_OTHER_WORKING_CAPITAL_IDENTITY_NOTE = "catalog_other_working_capital_leaf_identity"
_OTHER_NON_CASH_ADJUSTMENTS = "tpl.fm.cash_flow.other_non_cash_adjustments"
_LEASE_ROU = "tpl.a.other_assets.operating_lease_right_of_use_assets"
_LEASE_ROU_ADDITIONS = "tpl.a.other_assets.operating_lease_rou_asset_additions"
_LEASE_ROU_AMORTIZATION = (
    "tpl.a.other_assets.operating_lease_rou_asset_amortization"
)
_LEASE_CURRENT = "tpl.a.capital_sources.current_operating_lease_liabilities"
_LEASE_LONG = "tpl.a.capital_sources.long_term_operating_lease_liabilities"
_LEASE_LONG_STATEMENT = "tpl.fm.balance_sheet.long_term_operating_lease_liabilities"
_LEASE_LONG_CASH = "tpl.fm.cash_flow.long_term_operating_lease_liabilities"
_LEASE_BEGINNING = "tpl.a.capital_sources.operating_lease_liability_beginning"
_LEASE_ADDITIONS = "tpl.a.capital_sources.operating_lease_liability_additions"
_LEASE_PAYMENTS = "tpl.a.capital_sources.operating_lease_liability_payments"
_LEASE_ENDING = "tpl.a.capital_sources.operating_lease_liability_ending"

_CURRENT_DEBT = "tpl.a.capital_sources.current_portion_of_long_term_debt"
_SHORT_TERM_DEBT_ROLE = "statement.bs.short_term_debt"
_LONG_TERM_DEBT_ROLE = "statement.bs.long_term_debt"

_BASIC_SHARES = "tpl.a.dividends_shares.basic_shares_outstanding"
_DILUTED_SHARES = "tpl.a.tax_net_income.diluted_shares_outstanding"
_DILUTIVE_INCREMENT = "tpl.a.dividends_shares.dilutive_shares_weighted_average"
_END_SHARES = "tpl.a.dividends_shares.end_of_period_shares_outstanding"
_SHARES_ISSUED = "tpl.a.dividends_shares.shares_issued"
_DILUTION_RATE = "tpl.a.tax_net_income.y_y_chg"
_REPURCHASE_AMOUNT = "tpl.a.dividends_shares.total_amount"
_DIVIDENDS = "tpl.a.dividends_shares.dividend_paid"
_CASH_FLOW_DIVIDENDS = "tpl.fm.cash_flow.dividends"
_CASH_FLOW_SHARE_REPURCHASES = "tpl.fm.cash_flow.share_repurchases"
_DIVIDEND_PAYABLE = "tpl.a.dividends_shares.dividend_payable"
_EMPLOYEE_PLAN_PROCEEDS = (
    "tpl.a.dividends_shares.proceeds_from_employee_stock_purchase_plan"
)
_FM_EMPLOYEE_PLAN_PROCEEDS = (
    "tpl.fm.cash_flow.proceeds_from_employee_stock_purchase_plan"
)

_ACQUISITION_ADDITIONS = (
    "tpl.a.intangibles_goodwill.add_acquisitions_of_businesses"
)
_ACQUIRED_INTANGIBLE_AMORTIZATION = (
    "tpl.a.intangibles_goodwill.less_amortization_for_acquired_intangible_assets"
)
_ACQUIRED_INTANGIBLE_EXPENSE = (
    "tpl.a.depreciation_amortization.amortization_expense_for_acquired_intangible_assets"
)
_FM_ACQUIRED_INTANGIBLE_AMORTIZATION = (
    "tpl.fm.adjusted_earnings.amortization_of_acquired_intangibles"
)
_ADJUSTED_EBITDA_OTHER_ITEMS = "tpl.a.adj_ebitda.other_items"
_FM_OTHER_ITEMS = "tpl.fm.adjusted_earnings.other_items"
_FM_SBC_ADD_BACK = "tpl.fm.adjusted_earnings.stock_based_compensation_add_back"
# The non-GAAP net income bridge's twins of the SBC and other-items rows.
_FM_SBC_ADJUSTMENT = (
    "tpl.fm.adjusted_earnings.stock_based_compensation_add_back_adjustment"
)
_FM_OTHER_ITEMS_ADJUSTMENT = "tpl.fm.adjusted_earnings.other_items_adjustment"

_COMMERCIAL_PAPER = "tpl.fm.balance_sheet.current_liability_3"
# Face rows the accounting baseline gives every model that carry no role: a
# row that presents a schedule balance, not a line of its own.
_ROLELESS_FACE_BALANCE_PLACEHOLDERS = (
    "tpl.fm.balance_sheet.deferred_revenue_current",
    "tpl.fm.balance_sheet.current_operating_lease_liabilities",
    "tpl.fm.balance_sheet.current_liability_1",
)
_GOODWILL_ADDITIONS = "tpl.a.intangibles_goodwill.add_less_acquisitions_write_offs"
_RETAINED_EARNINGS = "tpl.fm.balance_sheet.retained_earnings"
_RETAINED_EARNINGS_ROLE = "statement.bs.retained_earnings"
_PHYSICAL_DEPRECIATION = "tpl.a.depreciation_amortization.depreciation"
_TOTAL_DEPRECIATION_AND_AMORTIZATION = (
    "tpl.a.depreciation_amortization.depreciation_and_amortization_m"
)
_SOFTWARE_AMORTIZATION = (
    "tpl.a.depreciation_amortization.amortization_of_capitalized_internal_use_software"
)
_PPE_LESS_DEPRECIATION = "tpl.a.capital_investments.less_depreciation"
_PPE_SCHEDULE = "tpl.a.capital_investments.property_and_equipment_net"
_PPE_ADDITIONS = "tpl.a.capital_investments.add_purchases_of_property_and_equipment"
_SOFTWARE_BALANCE = "tpl.fm.balance_sheet.software_investment_if_applicable"
_SOFTWARE_SCHEDULE = (
    "tpl.a.capital_investments.software_investment_if_applicable"
)
_ROU_BALANCE = "tpl.fm.balance_sheet.operating_lease_right_of_use_assets"
_PPE_BALANCE = "tpl.fm.balance_sheet.property_and_equipment_net"
_INTANGIBLE_BALANCE = "tpl.fm.balance_sheet.intangible_assets_net"
_INTANGIBLE_SCHEDULE = "tpl.a.intangibles_goodwill.intangible_assets_net"
_INTANGIBLE_ADDITIONS = "tpl.a.intangibles_goodwill.add_acquisitions_of_businesses"
_INTANGIBLE_AMORTIZATION = (
    "tpl.a.intangibles_goodwill.less_amortization_for_acquired_intangible_assets"
)
_CONTRACT_COST_NONCURRENT = "tpl.fm.balance_sheet.long_term_asset_1"
_TOTAL_CURRENT_ASSETS = "tpl.fm.balance_sheet.total_current_assets"
_TOTAL_ASSETS = "tpl.fm.balance_sheet.total_assets"
_TOTAL_CURRENT_LIABILITIES = "tpl.fm.balance_sheet.total_current_liabilities"
_ACCOUNTS_PAYABLE = "tpl.fm.balance_sheet.accounts_payable"
_ACCRUED_EXPENSES = "tpl.fm.balance_sheet.accrued_expenses"
#: S5 (plan §3): the grounded-schedule semantic rows this pass wires are the
#: model kind's, not the mechanism's. Their *roles* are the catalog's: a
#: balance's grounded-row role is its role leaf, its period-change row's is
#: ``change_role``, and the liability a declared ``balance_of``
#: balance is held against is its catalog ``counterpart``; the item ids arrive
#: on ``ModelKindCapabilities.grounded_semantic_rows`` and are resolved per call.
_GOODWILL = "tpl.fm.balance_sheet.goodwill"
_DEFERRED_CONTRACT_COSTS = "tpl.fm.balance_sheet.long_term_asset_1"
_CURRENT_DEFERRED_CONTRACT_COSTS = "tpl.fm.balance_sheet.current_asset_1"
_CURRENT_DEFERRED_CONTRACT_COSTS_SCHEDULE = "tpl.a.balance_sheet_wc.current_asset_1"
_LONG_TERM_DEFERRED_CONTRACT_COSTS_SCHEDULE = "tpl.a.other_assets.long_term_asset_1"
_OTHER_NONCURRENT_ASSETS = "tpl.fm.balance_sheet.long_term_asset_2"
_DEFERRED_TAX_ASSETS = "tpl.fm.balance_sheet.deferred_income_tax_assets"
_TOTAL_LIABILITIES = "tpl.fm.balance_sheet.total_liabilities"
_TOTAL_LIABILITIES_AND_EQUITY = (
    "tpl.fm.balance_sheet.total_liabilities_and_equity"
)

_CASH_BEGINNING = "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period"
_CASH_ENDING = "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period"
_CASH_ACQUISITIONS = "tpl.fm.cash_flow.acquisitions_of_businesses"
_INVESTMENT_PURCHASES = "tpl.fm.cash_flow.purchases_of_investments"
_INVESTMENT_SALES = "tpl.fm.cash_flow.sales_maturities_of_investments"
_OTHER_INVESTING = "tpl.fm.cash_flow.other_changes_in_investing_activities"
_CASH_FX = "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash"
_CASH_NET_CHANGE = "tpl.fm.cash_flow.net_change_in_cash_and_cash_equivalents"
_CASH_BRIDGE_ITEM_ID = "tpl.fm.cash_flow.cash_reconciliation_adjustment"
_CASH_BRIDGE_FLOW_ITEM_IDS = (_INVESTMENT_PURCHASES, _INVESTMENT_SALES)
_FM_NON_GAAP_TAX_EFFECT = (
    "tpl.fm.adjusted_earnings.income_tax_effect_on_adjustments"
)
_ADJUSTMENT_TAX_EFFECT = "tpl.a.tax_net_income.income_tax_effect_on_adjustments"
_ADJUSTMENT_TAX_RATE = "tpl.a.tax_net_income.effective_tax_rate"
_ADJUSTMENT_TAX_BASE = (
    "tpl.a.tax_net_income.stock_based_compensation_add_back",
    "tpl.a.tax_net_income.amortization_of_acquired_intangibles",
    "tpl.a.tax_net_income.other_items",
)
_GAAP_TAX_RATE = "tpl.a.tax_net_income.tax_rate"
_FM_DEFERRED_TAX_CASH_FLOW = "tpl.fm.cash_flow.deferred_income_tax_liabilities"

_PPE_AVERAGE = "tpl.a.depreciation_amortization.avg_property_and_equipment"
_PPE_DEPRECIATION_RATE = (
    "tpl.a.depreciation_amortization."
    "depreciation_as_of_beginning_property_and_equipment"
)
_DEPRECIATION_GROWTH = "tpl.a.depreciation_amortization.chg_y_y"
_SOFTWARE_ENDING = (
    "tpl.a.depreciation_amortization.end_capitalized_internal_use_software"
)
_SOFTWARE_AVERAGE = (
    "tpl.a.depreciation_amortization.avg_capitalized_internal_use_software"
)
_SOFTWARE_BEGINNING = (
    "tpl.a.depreciation_amortization.beg_capitalized_internal_use_software"
)
_SOFTWARE_READOUT_CHAIN = frozenset({_SOFTWARE_BEGINNING, _SOFTWARE_ENDING, _SOFTWARE_AVERAGE})
_SOFTWARE_AMORTIZATION_RATE = (
    "tpl.a.depreciation_amortization."
    "amortization_of_beginning_capitalized_internal_use_software"
)
_INTANGIBLE_AVERAGE = "tpl.a.depreciation_amortization.avg_intangibles"
_INTANGIBLE_AMORTIZATION_RATE = "tpl.a.depreciation_amortization.amortization"
_CAPITALIZED_RESEARCH = (
    "tpl.a.capital_investments.capitalized_portion_of_research_and_development"
)
_SOFTWARE_ADDITIONS = (
    "tpl.a.capital_investments.add_capitalized_software_costs_if_applicable"
)
_CAPITALIZED_RESEARCH_RATIO = (
    "tpl.a.capital_investments.capitalized_portion_of_r_d_as_of_internal_use_software_costs"
)
_SBC_TOTAL = "tpl.a.stock_based_compensation.stock_based_compensation"
_SBC_ALLOCATED_TOTAL = "tpl.a.stock_based_compensation.total"
_SBC_RATE_IDS = (
    "tpl.a.stock_based_compensation.cost_of_revenues_pct_line_item",
    "tpl.a.stock_based_compensation.sales_and_marketing_pct_line_item",
    "tpl.a.stock_based_compensation.research_and_development_pct_line_item",
    "tpl.a.stock_based_compensation.general_and_administrative_pct_line_item",
)
_SBC_COMPONENT_IDS = (
    "tpl.a.stock_based_compensation.cost_of_revenues",
    "tpl.a.stock_based_compensation.sales_and_marketing",
    "tpl.a.stock_based_compensation.research_and_development",
    "tpl.a.stock_based_compensation.general_and_administrative",
)
_SBC_BASE_IDS = (
    "tpl.a.unit_economics.costs_of_goods_sold",
    "tpl.a.operating_leverage.sales_and_marketing",
    "tpl.a.operating_leverage.research_and_development",
    "tpl.a.operating_leverage.general_and_administrative",
)
_STATEMENT_COST_OF_REVENUE = "tpl.fm.income_statement.cost_of_goods_sold"
_ASSUMPTION_RETAINED_EARNINGS = "tpl.a.dividends_shares.retained_earnings"
_EQUITY_SBC_OFFSET = "tpl.fm.cash_flow.share_based_compensation_offset"
_CASH_FLOW_SBC = "tpl.fm.cash_flow.share_based_compensation"
_CASH_FLOW_SBC_ROLE = "statement.cf.stock_based_compensation"
_SBC_YOY_GROWTH = "tpl.a.stock_based_compensation.y_y_chg"
_DEBT_AVERAGE = "tpl.a.other_income_interest.average"
_DEBT_INTEREST_RATE = "tpl.a.other_income_interest.interest_rate"
_FIRST_PERIOD_DERIVED_ROWS = (
    "tpl.a.operating_leverage.incremental_operating_margin",
    "tpl.a.adj_ebitda.incremental_adj_ebitda_margin",
    "tpl.a.adj_operating_income.incremental_adj_operating_margin",
    _PPE_AVERAGE,
    _SOFTWARE_BEGINNING,
    _SOFTWARE_AVERAGE,
)
# Projected acquired-intangible amortization is the capped runoff of the
# balance roll-forward (build_non_gaap_addbacks), not a rate on the average
# balance.  That balance amortizes to zero, where its rate is 0/0, so the
# rate and the average it divides by read out reported years only.
_HISTORICAL_READOUT_ROWS = (
    _INTANGIBLE_AVERAGE,
    _INTANGIBLE_AMORTIZATION_RATE,
)


def _values_are_present(
    values: Mapping[str, float | None],
) -> TypeGuard[Mapping[str, float]]:
    return all(value is not None for value in values.values())


def _derivation(
    strategy: FormulaDerivationStrategy,
    dependencies: Iterable[str],
) -> FormulaDerivation:
    return FormulaDerivation(
        owner=_OWNER,
        strategy=strategy,
        dependencies=tuple(dict.fromkeys(dependencies)),
    )


def opening_balance_roles() -> frozenset[str]:
    """The roles whose prior-year instant opens the first historical year.

    Every catalog balance-sheet role, every catalog balance (the operating
    ones included), and the role the cash bridge's ending item populates. The pin reads each
    at the year before the axis
    (``build_model_source_bundle._opening_balance_observations``); the
    first-year period-average, average-denominator and opening-cash cells
    read it.
    """

    catalog = load_required_role_catalog()
    return frozenset(
        (
            *(row.required_role for row in catalog.roles_on_statement("balance_sheet")),
            *catalog.balances,
            catalog.role_for_template_item(_CASH_ENDING).required_role,
        )
    )


def _historical_periods(model: FinancialModel) -> tuple[int, ...]:
    return tuple(sorted(int(value) for value in model.time_structure.historical_periods))


def _projection_periods(model: FinancialModel) -> tuple[int, ...]:
    return tuple(sorted(int(value) for value in model.time_structure.projection_periods))


def _latest_ratio_basis(
    values: ComputedValues,
    *,
    numerator_id: str,
    denominator_id: str,
    periods: Iterable[int],
) -> tuple[int, float] | None:
    """The latest period's finite, non-negative ratio and that period."""

    for period in sorted({int(value) for value in periods}, reverse=True):
        numerator = values.get(numerator_id, {}).get(period)
        denominator = values.get(denominator_id, {}).get(period)
        if numerator is None or denominator is None or denominator <= 0:
            continue
        ratio = float(numerator) / float(denominator)
        if math.isfinite(ratio) and ratio >= 0:
            return period, ratio
    return None


def _latest_ratio(
    values: ComputedValues,
    *,
    numerator_id: str,
    denominator_id: str,
    periods: Iterable[int],
) -> float | None:
    basis = _latest_ratio_basis(
        values,
        numerator_id=numerator_id,
        denominator_id=denominator_id,
        periods=periods,
    )
    return None if basis is None else basis[1]


def house_nci_share(
    values: ComputedValues,
    historical: Iterable[int],
) -> HouseRevenueRatio | None:
    """The one owner of the held-share rule (NCI-FORECAST-HELD-SHARE).

    The latest reported year's net income attributable to noncontrolling interest
    over consolidated net income (pretax income less the tax provision). The
    policy writes it ``default`` on the share row for every projection year no
    authored cell holds. Absent when no year reports the line.
    """

    for period in sorted({int(value) for value in historical}, reverse=True):
        nci = values.get(NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID, {}).get(period)
        pretax = values.get(_PRETAX_INCOME, {}).get(period)
        tax = values.get(_INCOME_TAX_PROVISION, {}).get(period)
        if nci is None or pretax is None or tax is None:
            continue
        consolidated = float(pretax) - float(tax)
        if consolidated == 0:
            continue
        share = float(nci) / consolidated
        if math.isfinite(share):
            return HouseRevenueRatio(
                rate_item_id=NCI_SHARE_RATE_ITEM_ID,
                strategy="latest_historical_nci_share",
                value=share,
                basis_period=period,
            )
    return None


def _physical_depreciation_basis(
    values: ComputedValues,
    periods: Iterable[int],
    software_included_periods: Collection[int] = (),
) -> tuple[int, float] | None:
    """Latest filed PPE depreciation over revenue, preserving its definition.

    Subtract a separately presented software amount; when software is cited
    inside total D&A, that filed inclusive amount depreciates inclusive PPE.
    An inclusion is never a manufactured zero software amortization.
    """
    for period in sorted({int(value) for value in periods}, reverse=True):
        component_ids = [_ACQUIRED_INTANGIBLE_EXPENSE]
        if period not in software_included_periods:
            component_ids.append(_SOFTWARE_AMORTIZATION)
        total_da = values.get(_TOTAL_DEPRECIATION_AND_AMORTIZATION, {}).get(period)
        components = [values.get(item_id, {}).get(period) for item_id in component_ids]
        revenue = values.get(_REVENUE, {}).get(period)
        if total_da is None or revenue is None or any(value is None for value in components):
            continue
        if float(revenue) <= 0:
            continue
        physical_depreciation = float(total_da) - sum(
            float(cast(float, value)) for value in components
        )
        if physical_depreciation < 0 or not math.isfinite(physical_depreciation):
            continue
        return period, physical_depreciation / float(revenue)
    return None


@dataclass(frozen=True)
class HouseRevenueRatio:
    """One authorable house revenue ratio as the compile writes it ``default``.

    ``rate_item_id`` is the rate row the ratio lands on, ``strategy`` the
    house convention its ``default`` cells name, ``basis_period`` the
    reported year it is read from.
    """

    rate_item_id: str
    strategy: FormulaDerivationStrategy
    value: float
    basis_period: int


_HouseRatioBasis = Callable[
    [ComputedValues, tuple[int, ...], Collection[int]], tuple[int, float] | None
]

#: Each authorable house revenue ratio: its rate row, the house convention its
#: ``default`` cells name, and the rule reading its value and basis year.
_HOUSE_REVENUE_RATIO_RULES: dict[str, tuple[FormulaDerivationStrategy, _HouseRatioBasis]] = {
    _OTHER_CURRENT_ASSETS_RATE: (
        "latest_historical_revenue_ratio",
        lambda values, periods, _included: _latest_ratio_basis(
            values,
            numerator_id=_OTHER_CURRENT_ASSETS,
            denominator_id=_REVENUE,
            periods=periods,
        ),
    ),
    _LEASE_CURRENT_RATE: (
        "lease_current_revenue_ratio",
        lambda values, periods, _included: _latest_ratio_basis(
            values,
            numerator_id=_LEASE_CURRENT,
            denominator_id=_REVENUE,
            periods=periods,
        ),
    ),
    _PHYSICAL_DEPRECIATION_RATE: (
        "historical_physical_depreciation_residual_ratio",
        _physical_depreciation_basis,
    ),
}
HOUSE_REVENUE_RATIO_RATE_ITEM_IDS = frozenset(_HOUSE_REVENUE_RATIO_RULES)


def house_revenue_ratios(
    values: ComputedValues,
    historical: Iterable[int],
    *,
    software_included_periods: Collection[int] = (),
) -> dict[str, HouseRevenueRatio]:
    """The house value of each authorable revenue-ratio row, by rate row.

    The one owner of these house rules: the schedule policies below write
    exactly these values ``default`` where no authored forecast-assumption row
    holds the rate, and the forecast-assumptions context shows them beside
    their controls. A ratio whose reported operands are missing is absent.
    """

    periods = tuple(int(value) for value in historical)
    ratios: dict[str, HouseRevenueRatio] = {}
    for rate_item_id, (strategy, rule) in _HOUSE_REVENUE_RATIO_RULES.items():
        basis = rule(values, periods, software_included_periods)
        if basis is not None:
            ratios[rate_item_id] = HouseRevenueRatio(
                rate_item_id=rate_item_id,
                strategy=strategy,
                value=basis[1],
                basis_period=basis[0],
            )
    return ratios


def _software_inside_ppe_periods(
    model: FinancialModel, historical: Iterable[int],
) -> frozenset[int]:
    return frozenset(
        period for period in historical if _software_inside_ppe(model, period)
    )


def _reported_history_values(
    *,
    requirements: Iterable[AnnualSourceRequirement],
    coverage: Iterable[AnnualRequirementCoverage],
    evidence: Iterable[SourceEvidence],
    historical_basis_facts: Iterable[HistoricalBasisFact],
    historical: Iterable[int],
) -> ComputedValues:
    """The reported history of template rows, read from accepted facts.

    A requirement's row takes its one supported filing value per period and a
    retained filing-schedule fact lands on its schedule row, as the compile
    populates them; a row the template carries historically as a reference to
    another row, or as a constant, follows that template formula. Rows the
    template derives any other way are absent.
    """

    from .driver_resolver import _load_base_template_model

    template = _load_base_template_model()
    periods = frozenset(int(value) for value in historical)

    def year(period_id: str) -> int | None:
        digits = period_id.removeprefix("FY")
        if digits == period_id or not digits.isdigit():
            return None
        return int(digits) if int(digits) in periods else None

    reported: dict[str, dict[int, float]] = {}
    evidence_by_id = {row.evidence_id: row for row in evidence}
    coverage_by_id = {row.requirement_id: row for row in coverage}
    for requirement in requirements:
        item = template._index.get(requirement.model_item)
        row = coverage_by_id.get(requirement.requirement_id)
        if item is None or row is None:
            continue
        for period in row.periods:
            period_year = year(period.period_id)
            if (
                period_year is None
                or period.disposition not in {"observed", "derived"}
                or len(period.evidence_ids) != 1
            ):
                continue
            source = evidence_by_id.get(period.evidence_ids[0])
            if source is None:
                continue
            reported.setdefault(item.id, {})[period_year] = float(
                _scaled_to(
                    source.value,
                    source_scale=source.scale,
                    target_scale=item.model_scale,
                )
            )
    for (item_id, period_year), (value, _) in _filing_schedule_cells(
        template._index, periods, historical_basis_facts
    ).items():
        reported.setdefault(item_id, {})[period_year] = float(value)

    resolved: dict[str, dict[int, float]] = {}

    def resolve(item_id: str, seen: frozenset[str]) -> dict[int, float]:
        if item_id in resolved:
            return resolved[item_id]
        series = dict(reported.get(item_id, {}))
        item = template._index.get(item_id)
        spec = None if item is None else item.historical
        missing = periods - series.keys()
        if missing and spec is not None:
            source = spec.params.get("source")
            constant = spec.params.get("value")
            if (
                spec.type is FormulaType.ref
                and isinstance(source, LineItemRef)
                and source.t == 0
                and source.id not in seen
            ):
                upstream = resolve(source.id, seen | {item_id})
                series.update(
                    {period: upstream[period] for period in missing if period in upstream}
                )
            elif (
                spec.type is FormulaType.constant
                and isinstance(constant, (int, float))
                and not isinstance(constant, bool)
            ):
                series.update(dict.fromkeys(missing, float(constant)))
        resolved[item_id] = series
        return series

    return {
        item_id: series
        for item_id in template._index
        if (series := resolve(item_id, frozenset()))
    }


def reported_house_revenue_ratios(
    *,
    requirements: Iterable[AnnualSourceRequirement],
    coverage: Iterable[AnnualRequirementCoverage],
    evidence: Iterable[SourceEvidence],
    historical_basis_facts: Iterable[HistoricalBasisFact],
    historical: Iterable[int],
) -> dict[str, HouseRevenueRatio]:
    """``house_revenue_ratios`` over the reported history in accepted facts.

    What a pre-compile reader (the forecast-assumptions context) shows as each
    authorable ratio's house value: the same rules, read from the history the
    compile populates from the same facts.
    """

    periods = tuple(int(value) for value in historical)
    requirements = tuple(requirements)
    return house_revenue_ratios(
        _reported_history_values(
            requirements=requirements,
            coverage=coverage,
            evidence=evidence,
            historical_basis_facts=historical_basis_facts,
            historical=periods,
        ),
        periods,
        software_included_periods=(
            _nsp_periods(requirements, "statement.cf.capitalized_software_amortization")
            & _nsp_periods(requirements, "statement.bs.capitalized_software")
        ),
    )


def _nsp_periods(
    requirements: Iterable[AnnualSourceRequirement], role: str,
) -> frozenset[int]:
    return frozenset(
        int(cell.period_id.removeprefix("FY"))
        for requirement in requirements
        if requirement.required_role == role
        for cell in requirement.period_cells
        if cell.binding_state == "not_separately_presented"
    )



@dataclass(frozen=True)
class HouseNetDilution:
    """The net-dilution rate the compile writes ``default`` unauthored.

    The house convention ``strategy`` names: the latest reported diluted-share
    year-over-year change, held flat. ``basis_period`` is the reported year it
    is read from, ``prior_period`` the year it is measured against.
    """

    strategy: FormulaDerivationStrategy
    value: float
    basis_period: int
    prior_period: int


def house_net_dilution(
    values: ComputedValues,
    historical: Iterable[int],
) -> HouseNetDilution | None:
    """The one owner of the net-dilution house rule.

    The share policy writes this value ``default`` on every projection year
    no ValuationPolicy ``net_share_dilution`` rate holds, and the valuation-policy
    context shows it beside that parameter. Absent when no two consecutive
    reported diluted share counts exist.
    """

    shares = values.get(_DILUTED_SHARES, {})
    periods = sorted({int(value) for value in historical})
    for prior, period in reversed(tuple(zip(periods, periods[1:]))):
        current_count = shares.get(period)
        prior_count = shares.get(prior)
        if current_count is None or prior_count is None or prior_count <= 0:
            continue
        rate = float(current_count) / float(prior_count) - 1.0
        if math.isfinite(rate):
            return HouseNetDilution(
                strategy="latest_reported_net_dilution",
                value=rate,
                basis_period=period,
                prior_period=prior,
            )
    return None


def reported_house_net_dilution(
    *,
    requirements: Iterable[AnnualSourceRequirement],
    coverage: Iterable[AnnualRequirementCoverage],
    evidence: Iterable[SourceEvidence],
    historical_basis_facts: Iterable[HistoricalBasisFact],
    historical: Iterable[int],
) -> HouseNetDilution | None:
    """``house_net_dilution`` over the reported history in accepted facts."""

    periods = tuple(int(value) for value in historical)
    return house_net_dilution(
        _reported_history_values(
            requirements=requirements,
            coverage=coverage,
            evidence=evidence,
            historical_basis_facts=historical_basis_facts,
            historical=periods,
        ),
        periods,
    )


@dataclass(frozen=True)
class HouseSbcMix:
    """The issuer-disclosed SBC line mix the compile carries and scales.

    ``line_rates`` holds each SBC rate row's latest disclosed rate (the line's
    SBC over its own expense) as a ``HouseRevenueRatio`` the compile writes
    ``default`` unauthored; ``aggregate`` is r_agg, the latest year's reported
    SBC total over the sum of the four eligible expenses. An authored
    ``sbc_pct`` lands on line i as ``rate_i × sbc_pct ÷ r_agg``.
    """

    line_rates: Mapping[str, HouseRevenueRatio]
    aggregate: HouseRevenueRatio | None


def _sbc_reported_periods(
    values: ComputedValues,
    historical: Iterable[int],
) -> tuple[int, ...]:
    """The historical years the aggregate SBC is reported."""

    return tuple(
        period
        for period in sorted({int(value) for value in historical})
        if _has_known_historical_value(values, item_id=_SBC_TOTAL, periods=(period,))
    )


def house_sbc_mix(
    values: ComputedValues,
    sbc_periods: Iterable[int],
) -> HouseSbcMix:
    """The one owner of the disclosed SBC mix rule, over ``sbc_periods``."""

    periods = tuple(sorted({int(value) for value in sbc_periods}))
    line_rates: dict[str, HouseRevenueRatio] = {}
    for rate_id, component_id, base_id in zip(
        _SBC_RATE_IDS, _SBC_COMPONENT_IDS, _SBC_BASE_IDS, strict=True
    ):
        basis = _latest_ratio_basis(
            values,
            numerator_id=component_id,
            denominator_id=base_id,
            periods=periods,
        )
        if basis is not None:
            line_rates[rate_id] = HouseRevenueRatio(
                rate_item_id=rate_id,
                strategy="sbc_allocation",
                value=basis[1],
                basis_period=basis[0],
            )
    aggregate = None
    for period in reversed(periods):
        sbc_total = values.get(_SBC_TOTAL, {}).get(period)
        bases = [values.get(item_id, {}).get(period) for item_id in _SBC_BASE_IDS]
        if sbc_total is None or any(value is None for value in bases):
            continue
        base_total = sum(float(value) for value in bases if value is not None)
        if base_total > 0 and math.isfinite(float(sbc_total) / base_total):
            aggregate = HouseRevenueRatio(
                rate_item_id=_SBC_ALLOCATED_TOTAL,
                strategy="sbc_allocation",
                value=float(sbc_total) / base_total,
                basis_period=period,
            )
            break
    return HouseSbcMix(line_rates=line_rates, aggregate=aggregate)


def reported_house_sbc_mix(
    *,
    requirements: Iterable[AnnualSourceRequirement],
    coverage: Iterable[AnnualRequirementCoverage],
    evidence: Iterable[SourceEvidence],
    historical_basis_facts: Iterable[HistoricalBasisFact],
    historical: Iterable[int],
) -> HouseSbcMix | None:
    """``house_sbc_mix`` over the reported history, or None undisclosed.

    Disclosed means what the compile's split test reads: every SBC line holds
    a reported value in every year the aggregate SBC is reported.
    """

    periods = tuple(int(value) for value in historical)
    values = _reported_history_values(
        requirements=requirements,
        coverage=coverage,
        evidence=evidence,
        historical_basis_facts=historical_basis_facts,
        historical=periods,
    )
    sbc_periods = _sbc_reported_periods(values, periods)
    if not sbc_periods or not all(
        values.get(component_id, {}).get(period) is not None
        for component_id in _SBC_COMPONENT_IDS
        for period in sbc_periods
    ):
        return None
    # The template carries the eligible COGS as revenue x (1 - gross margin),
    # which is the statement's cost of revenue; the reported read has no
    # derived rows, so it takes that statement line.
    return house_sbc_mix(
        {
            **values,
            _SBC_BASE_IDS[0]: values.get(_SBC_BASE_IDS[0])
            or values.get(_STATEMENT_COST_OF_REVENUE, {}),
        },
        sbc_periods,
    )


def _disclosed_mix_rate(
    authored: float,
    *,
    disclosed_rate: float,
    aggregate_rate: float,
) -> float:
    """The one aggregate_scale rule: rate_i × sbc_pct ÷ r_agg."""

    return disclosed_rate * authored / aggregate_rate


def scale_aggregate_sbc_scenario_values(
    item: LineItem,
    values: Mapping[int, float],
) -> dict[int, float]:
    """Lower a scenario's ``sbc_pct`` values onto one SBC rate row.

    A period whose base row is ``aggregate_scale`` and was scaled by the
    disclosed mix takes the scenario value through the same rule with the
    rates its lineage names, so bull and bear keep the disclosed proportions
    the base case keeps. Any other period (an undisclosed split, a
    ``per_line`` row, a non-SBC row) takes the value as written.
    """

    overrides = item.overrides or {}
    scaled: dict[int, float] = {}
    for period, value in values.items():
        spec = overrides.get(int(period))
        lineage = None if spec is None else spec.params.get("lineage")
        if (
            isinstance(lineage, dict)
            and lineage.get("forecast_method") == "aggregate_scale"
            and "aggregate_rate" in lineage
        ):
            scaled[period] = _disclosed_mix_rate(
                value,
                disclosed_rate=float(lineage["disclosed_rate"]),
                aggregate_rate=float(lineage["aggregate_rate"]),
            )
        else:
            scaled[period] = value
    return scaled


def _scale_aggregate_sbc_rate(
    item: LineItem,
    periods: Iterable[int],
    *,
    line_rate: HouseRevenueRatio,
    aggregate: HouseRevenueRatio | None,
) -> None:
    """Scale an ``aggregate_scale`` row's authored ``sbc_pct`` by the mix.

    The cell stays ``input`` (the author's judgment, lowered through the
    disclosed mix); its lineage names the authored value and both rates. A
    ``per_line`` row, or a row already scaled, is left as authored. Without a
    disclosed aggregate rate there is no mix to scale by, and the row keeps
    ``sbc_pct`` as written.
    """

    overrides = item.overrides
    if aggregate is None or aggregate.value <= 0 or item.values is None or overrides is None:
        return
    for period in periods:
        cell = item.values.values.get(int(period))
        spec = overrides.get(int(period))
        lineage = None if spec is None else spec.params.get("lineage")
        if (
            cell is None
            or cell.value is None
            or cell.provenance != ValueProvenance.input
            or spec is None
            or not isinstance(lineage, dict)
            or lineage.get("forecast_method") != "aggregate_scale"
            or "authored_value" in lineage
        ):
            continue
        authored = float(cell.value)
        scaled = _disclosed_mix_rate(
            authored, disclosed_rate=line_rate.value, aggregate_rate=aggregate.value
        )
        item.values.values[int(period)] = ValueCell(
            period=int(period),
            value=scaled,
            provenance=ValueProvenance.input,
        )
        overrides[int(period)] = spec.model_copy(
            update={
                "params": {
                    **spec.params,
                    "value": scaled,
                    "lineage": {
                        **lineage,
                        "authored_value": authored,
                        "disclosed_rate": line_rate.value,
                        "disclosed_rate_period": line_rate.basis_period,
                        "aggregate_rate": aggregate.value,
                        "aggregate_rate_period": aggregate.basis_period,
                    },
                }
            }
        )


def sbc_per_line_row_split_undisclosed_findings(
    model: FinancialModel,
    values: ComputedValues,
) -> tuple[Finding, ...]:
    """One warning per authored per-line SBC row on an undisclosed split.

    The per-line controls apply only where the issuer discloses SBC by line.
    Undisclosed, a line left open sends the aggregate to its revenue rate and
    the authored line rows are unread; four authored rows set a split the
    filing does not disclose. Either way the author answers it (``sbc_pct``)
    or declines; nothing is refused. Read before the aggregate path clears the
    unallocated rows.
    """

    from .build_findings import make_finding

    historical = _historical_periods(model)
    projected = set(_projection_periods(model))
    if _discloses_sbc_line_split(model, _sbc_reported_periods(values, historical)):
        return ()
    findings: list[Finding] = []
    for rate_id in _SBC_RATE_IDS:
        item = model.get_item(rate_id)
        authored = {
            int(period): lineage["control"]
            for period, spec in (item.overrides or {}).items()
            if int(period) in projected
            and isinstance(lineage := spec.params.get("lineage"), dict)
            and lineage.get("forecast_method") == "per_line"
        }
        if not authored:
            continue
        controls = tuple(dict.fromkeys(authored.values()))
        findings.append(
            make_finding(
                "sbc_per_line_row_split_undisclosed",
                message=(
                    f"{', '.join(controls)} is authored, but the issuer discloses "
                    "no SBC split by line, so the per-line control does not "
                    "apply: with any line left open the row is unread and SBC "
                    "projects at its revenue rate; with all four authored it "
                    "sets a split the filing does not disclose. Author sbc_pct, "
                    "or decline naming the split you are asserting."
                ),
                item_id=rate_id,
                periods=tuple(authored),
                operands=(rate_id, *controls),
            )
        )
    return tuple(findings)


def _has_no_driver(item) -> bool:
    """A balance nothing in the build projects: no formula, or the template's held balance.

    The template gives every total member a prior-balance carry-forward so no
    total sums a blank; that default is not a driver a build installed.
    """

    return item.projected is None or item.projected.note == OPTIONAL_SLOT_CARRY_FORWARD_NOTE


def _set_periods(item, periods: Iterable[int]) -> None:
    item.formula_periods = sorted(
        set(item.formula_periods or ()) | {int(value) for value in periods}
    )


def _install_default_revenue_ratio(
    model: FinancialModel,
    *,
    target_id: str,
    rate_id: str,
    anchor_id: str,
    label: str,
    ratio: float | None,
    projected: tuple[int, ...],
    note: str,
    strategy: FormulaDerivationStrategy,
    lineage_by_period: Mapping[int, Mapping[str, Any]] | None = None,
) -> list[str]:
    """Project ``target_id`` as revenue times the ratio held at ``rate_id``.

    The ratio is a typed parameter row, never a formula literal (spec
    principle 5). A period the target's own authored row holds keeps that
    value and takes no ratio; every other period reads ``rate_id``, where an
    authored rate stays ``input`` and the house ratio is written ``default``
    (``judgment_absent_default_used``).
    """

    target = model.get_item(target_id)
    authored = set(_authored_periods(target, projected))
    open_periods = tuple(period for period in projected if period not in authored)
    if not open_periods:
        return []
    rate = _bind_or_insert_item(
        model,
        item_id=rate_id,
        anchor_item_id=anchor_id,
        label=label,
        item_type=ItemType.input,
        unit=Unit.percentage,
    )
    rate.model_scale = "units"
    if ratio is not None:
        _write_default_unless_authored(
            rate,
            open_periods,
            ratio,
            owner=_OWNER,
            strategy=strategy,
            dependencies=(target_id, _REVENUE),
            lineage_by_period=lineage_by_period,
        )
    _set_periods(rate, open_periods)
    target.projected = FormulaSpec(
        type=FormulaType.driver,
        subtype="base_x_rate",
        params={"base": LineItemRef(id=_REVENUE), "rate": LineItemRef(id=rate_id)},
        note=note,
        derivation=_derivation(strategy, (_REVENUE, rate_id)),
    )
    _set_periods(target, projected)
    return [target.id, rate.id]


def _apply_working_capital_policy(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> list[str]:
    if not {_OTHER_CURRENT_ASSETS, _OTHER_CURRENT_ASSETS_RATE_ANCHOR} <= set(
        model._index
    ):
        return []
    house = house_revenue_ratios(
        values, historical,
        software_included_periods=_software_inside_ppe_periods(model, historical),
    ).get(_OTHER_CURRENT_ASSETS_RATE)
    if house is None:
        return []
    return _install_default_revenue_ratio(
        model,
        target_id=_OTHER_CURRENT_ASSETS,
        rate_id=_OTHER_CURRENT_ASSETS_RATE,
        anchor_id=_OTHER_CURRENT_ASSETS_RATE_ANCHOR,
        label="Prepaid expenses and other as % of revenue",
        ratio=house.value,
        projected=projected,
        note="company-grounded latest reported other-current-assets/revenue ratio",
        strategy=house.strategy,
    )


def _face_balance_line(model: FinancialModel, item_id: str) -> str | None:
    """The face balance-sheet line that presents one role's balance item.

    A face item presents itself; a schedule item is presented by the face
    line whose historical formula references it (the deferred-contract-cost
    schedules). A schedule item no face line references is a disclosure
    component of some other face parent, not a leaf of its own.
    """

    if item_id not in model._index:
        return None
    if item_id.startswith(_FACE_BALANCE_SHEET_PREFIX):
        return item_id
    for item in model._index.values():
        spec = item.historical
        if (
            item.id.startswith(_FACE_BALANCE_SHEET_PREFIX)
            and spec is not None
            and spec.type == FormulaType.ref
            and isinstance(spec.params.get("source"), LineItemRef)
            and spec.params["source"].id == item_id
        ):
            return item.id
    return None


def _apply_other_working_capital_derivation(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    *,
    role_dispositions: RoleDispositionTable,
) -> list[str]:
    """Other working capital: the catalog's leaf identity, in every year.

    The implied cash impact (minus each asset's movement, plus each
    liability's) of the operating balance-sheet leaves the catalog derives
    (`RequiredRoleCatalog.other_working_capital_leaves`), over the face lines
    this model presents them on with reported balances. A leaf this issuer
    does not report, or reports only inside another face parent, has no such
    line and adds nothing. The first historical year has no opening balance
    in the model and stays blank. Projected years apply the same identity;
    projected cash from operations adds the own-line rows beside it
    (`_compose_projected_operating_working_capital`), and the broad
    other-noncash row stops counting the leaf lines' movements.
    """

    catalog = load_required_role_catalog()
    target_ids = [
        item_id
        for item_id in catalog.role(_OTHER_WORKING_CAPITAL_ROLE).template_items
        if item_id in model._index
    ]
    ticker = str(model.company.ticker)
    # The leaves whose terminal historical state is bound (and that
    # hold a reported balance) are the operands of every period. A leaf cited
    # not separately presented at the terminal joins the forecast operands only
    # where an author holds a forecast on its row: its movement is then modeled
    # on its own line, and its cited opening reads zero (``_endpoint``).
    terminal = _terminal_rows(role_dispositions)
    projection = frozenset(projected)
    terms: dict[str, str] = {}
    authored_terms: dict[str, str] = {}
    for leaf in catalog.other_working_capital_leaves():
        state = terminal[leaf.required_role].state
        for candidate in (
            *leaf.template_items,
            *catalog.population_item_ids(leaf.required_role, ticker=ticker),
        ):
            line = _face_balance_line(model, candidate)
            if line is None:
                continue
            if state == "bound" and any(
                values.get(line, {}).get(period) is not None for period in historical
            ):
                terms.setdefault(line, str(leaf.working_capital))
            elif state == "not_separately_presented" and _authored_periods(
                model.get_item(line), projected
            ):
                authored_terms.setdefault(line, str(leaf.working_capital))
    projected_lines = {**terms, **authored_terms}

    def _presented_inside_another_line(line: str, period: int) -> bool:
        # A cited inclusion is not a balance this delta can read. The amount
        # sits in the including line, so this side of the movement is zero.
        if line not in model._index:
            return False
        spec = (model.get_item(line).overrides or {}).get(int(period))
        return (
            spec is not None
            and spec.type is FormulaType.raw
            and spec.subtype == "not_separately_presented"
        )

    def _endpoint(line: str, t: int, period: int, prior: int) -> LineItemRef | int:
        source_period = period if t == 0 else prior
        if _presented_inside_another_line(line, source_period):
            return 0
        return LineItemRef(id=line, t=t)

    def _identity_expr(
        period: int | None = None,
        prior: int | None = None,
        *,
        lines: Mapping[str, str] = terms,
    ) -> dict[str, object]:
        args = []
        for line, side in lines.items():
            left_t = -1 if side == "asset" else 0
            right_t = 0 if side == "asset" else -1
            if period is None or prior is None:
                left: LineItemRef | int = LineItemRef(id=line, t=left_t)
                right: LineItemRef | int = LineItemRef(id=line, t=right_t)
            else:
                left = _endpoint(line, left_t, period, prior)
                right = _endpoint(line, right_t, period, prior)
            if left == 0 and right == 0:
                continue
            args.append({"op": "-", "left": left, "right": right})
        return {"op": "+", "args": args}

    def _identity(
        expr: dict[str, object] | None = None, *, lines: Mapping[str, str] = terms,
    ) -> FormulaSpec:
        return FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={"expr": _identity_expr(lines=lines) if expr is None else expr},
            note=_OTHER_WORKING_CAPITAL_IDENTITY_NOTE,
        )

    for item_id in target_ids:
        item = model.get_item(item_id)
        # The identity is the value of the years the pin leaves empty (the
        # issuer's binding is the formula). A pinned year keeps its value:
        # the renderer emits a period's formula over its value.
        pinned = {
            int(period)
            for period, cell in (item.values.values.items() if item.values else ())
            if cell.value is not None
        }
        item.historical = _identity()
        item.projected = _identity(lines=projected_lines)
        # The template's remapped first-forecast override was assembled from
        # an older row set; it must not shadow the identity.
        if item.overrides is not None:
            for period in projected:
                item.overrides.pop(int(period), None)
        item.formula_periods = sorted(
            (set(item.formula_periods or ()) - set(historical))
            | {period for period in historical[1:] if period not in pinned}
            | set(projected)
        )
        order = [*historical, *projected]
        prior_of = {period: order[index - 1] for index, period in enumerate(order) if index}
        if item.overrides is None:
            item.overrides = {}
        identity_periods: dict[int, FormulaSpec] = {}
        for period in item.formula_periods:
            prior = prior_of.get(int(period))
            if prior is None:
                continue
            lines = projected_lines if int(period) in projection else terms
            expr = _identity_expr(int(period), prior, lines=lines)
            if expr == _identity_expr(lines=lines):
                continue
            if not expr["args"]:
                identity_periods[int(period)] = FormulaSpec(
                    type=FormulaType.constant,
                    params={"value": 0},
                    note=_OTHER_WORKING_CAPITAL_IDENTITY_NOTE,
                )
            else:
                identity_periods[int(period)] = _identity(expr)
        # The identity's own period overrides are exactly those the leaves' present states call
        # for: one a leaf's earlier state called for goes when the leaf leaves.
        for period in [
            int(period)
            for period, spec in item.overrides.items()
            if spec.note == _OTHER_WORKING_CAPITAL_IDENTITY_NOTE
            and int(period) not in identity_periods
        ]:
            del item.overrides[period]
        item.overrides.update(identity_periods)
    # The template's broad other-noncash row also moves some leaf lines; the
    # identity is their owner now, so it would count them twice.
    other_non_cash = model.get_item(_OTHER_NON_CASH_ADJUSTMENTS)
    if other_non_cash.projected is not None:
        for line in projected_lines:
            _remove_formula_ref(other_non_cash.projected.params, line)
    return target_ids


def _lease_reconciliation_formula(
    shape: LeaseReconciliationShape,
    **parameters: float,
) -> FormulaSpec:
    """Bind only caller-owned ratios; FormulaSpec rehydrates its own references."""

    def bind(value: Any) -> Any:
        if isinstance(value, dict):
            if value.keys() == {"parameter"}:
                return parameters[value["parameter"]]
            return {key: bind(nested) for key, nested in value.items()}
        if isinstance(value, list):
            return [bind(nested) for nested in value]
        return value

    payload = dict(shape.formula)
    payload["params"] = bind(shape.formula["params"])
    return FormulaSpec.model_validate(payload)


def _apply_lease_policy(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    *,
    historical_basis_facts: tuple[HistoricalBasisFact, ...],
    role_dispositions: RoleDispositionTable,
) -> list[str]:
    required = {
        _REVENUE,
        _LEASE_ROU,
        _LEASE_ROU_ADDITIONS,
        _LEASE_ROU_AMORTIZATION,
        _LEASE_CURRENT,
        _LEASE_LONG,
        _LEASE_BEGINNING,
        _LEASE_ADDITIONS,
        _LEASE_PAYMENTS,
    }
    if not required <= set(model._index):
        return []
    lease_shapes = load_required_role_catalog().lease_reconciliation_shapes
    flow_facts = {
        definition: max(
            (
                fact
                for fact in historical_basis_facts
                if fact.accounting_definition == definition
            ),
            key=lambda fact: int(fact.period_id.removeprefix("FY")),
            default=None,
        )
        for definition in ("lease_cash_paid", "rou_additions")
    }
    current = house_revenue_ratios(
        values, historical,
        software_included_periods=_software_inside_ppe_periods(model, historical),
    ).get(_LEASE_CURRENT_RATE)
    lease_cash_paid_fact = flow_facts["lease_cash_paid"]
    rou_additions_fact = flow_facts["rou_additions"]
    rou_state = _terminal_state(role_dispositions, "statement.bs.operating_lease_right_of_use_assets")
    long_state = _terminal_state(role_dispositions, "statement.bs.long_term_lease_liability")
    cur_state = _terminal_state(role_dispositions, "statement.bs.current_lease_liability")
    rou_present = rou_state == "bound"
    long_present = long_state == "bound"
    cur_present = cur_state == "bound"
    long_zero = _all_historical_values_are_zero(values, item_id=_LEASE_LONG_STATEMENT, periods=historical)
    if not long_present and long_state != "unowed":
        # An unreported or cited liability has no separate balance or movement. Never
        # derive it as ending minus a separately reported current portion. An unowed
        # one leaves the model through the graph owner.
        for item_id in (_LEASE_LONG_STATEMENT, _LEASE_LONG_CASH, _LEASE_LONG):
            item = model._index.get(item_id)
            if item is None:
                continue
            item.projected = None
            for period in projected:
                if item.overrides is not None:
                    item.overrides.pop(period, None)

    def _landed_periods(item_id: str, definition: str | None = None) -> set[int]:
        item = model.get_item(item_id)
        landed_periods: set[int] = set()
        for period in historical:
            cell = None if item.values is None else item.values.values.get(int(period))
            override = (item.overrides or {}).get(int(period))
            if cell is not None or (
                override is not None and override.type is FormulaType.constant
            ):
                landed_periods.add(int(period))
        if definition is not None:
            for fact in historical_basis_facts:
                if fact.accounting_definition != definition:
                    continue
                digits = fact.period_id.removeprefix("FY")
                if digits.isdigit() and int(digits) in historical:
                    landed_periods.add(int(digits))
        return landed_periods

    if rou_present:
        rou_hist_periods = _landed_periods(_LEASE_ROU_ADDITIONS, "rou_additions")
        if rou_hist_periods:
            additions = model.get_item(lease_shapes["liability_additions_historical"].target_item)
            directly_landed = {
                int(period)
                for period in historical
                if additions.values is not None and int(period) in additions.values.values
            }
            if additions.overrides:
                for period in rou_hist_periods - directly_landed:
                    spec = additions.overrides.get(int(period))
                    if spec is not None and spec.type is FormulaType.constant:
                        additions.overrides.pop(int(period), None)
            additions.historical = _lease_reconciliation_formula(
                lease_shapes["liability_additions_historical"],
            )
            _set_periods(additions, rou_hist_periods)
            amortization_periods = [
                period for period in historical[1:] if period in rou_hist_periods
            ]
            rou_amortization = model.get_item(lease_shapes["rou_amortization_historical"].target_item)
            rou_amortization.historical = _lease_reconciliation_formula(
                lease_shapes["rou_amortization_historical"],
            )
            _set_periods(rou_amortization, amortization_periods)
    if (
        current is not None
        and lease_cash_paid_fact is not None
        and rou_additions_fact is not None
    ):
        lease_cash_paid = float(lease_cash_paid_fact.value)
        rou_additions_value = float(rou_additions_fact.value)
        wired = _install_default_revenue_ratio(
            model,
            target_id=_LEASE_CURRENT,
            rate_id=_LEASE_CURRENT_RATE,
            anchor_id=_LEASE_CURRENT,
            label="  Current operating lease liabilities as % of revenue",
            ratio=current.value,
            projected=projected,
            note="company-grounded latest current-lease-liability/revenue ratio",
            strategy=current.strategy,
        )
        grounded: tuple[
            tuple[str, float, FormulaDerivationStrategy],
            ...,
        ] = (
            (
                _LEASE_ROU_AMORTIZATION,
                rou_additions_value,
                "lease_current_portion_proxy",
            ),
            (
                _LEASE_ROU_ADDITIONS,
                rou_additions_value,
                "lease_rou_balance_reconciliation",
            ),
            (
                _LEASE_PAYMENTS,
                lease_cash_paid,
                "lease_current_portion_proxy",
            ),
            (
                _LEASE_ADDITIONS,
                rou_additions_value,
                "lease_liability_balance_reconciliation",
            ),
        )
        rows = []
        for item_id, value, strategy in grounded:
            item = model.get_item(item_id)
            item.projected = FormulaSpec(
                type=FormulaType.constant,
                params={"value": value},
                note="latest filing-native lease flow carried across forecast",
                derivation=_derivation(strategy, ()),
            )
            rows.append(item)
        for item in rows:
            _set_periods(item, projected)
        return [*wired, *(item.id for item in rows)]
    wired: list[str] = []

    graph: DependencyGraph | None = None

    def retire_cells(item_ids: tuple[str, ...], periods: tuple[int, ...]) -> None:
        """Rows no lease policy runs leave the model through the graph's one retirement owner.

        The graph is built over the model on the first retirement and serves the rest: nothing
        between them adds a formula, and ``DependencyGraph.retire`` only clears cells and strips
        refs, which leaves ``adj`` / ``time_edges`` a superset of the live edges.
        """
        nonlocal graph
        if graph is None:
            graph = DependencyGraph()
            graph.build(model)
        for item_id in item_ids:
            if item_id in model._index:
                for period in periods:
                    graph.retire(item_id, period)

    def retire_schedule(*item_ids: str, periods: tuple[int, ...] = projected) -> None:
        """A schedule whose roles the filing cites has no run: its ``periods`` cells leave.

        The roll-forward integrity gate requires every input of a canonical roll-forward
        that is present; a schedule that does not run has no such roll-forward.
        """
        retire_cells(item_ids, periods)
        for item_id in item_ids:
            if item_id in model._index:
                # The gate reads the lane's canonical roll-forward note, not its cells.
                model.get_item(item_id).projected = None

    def open_liability_without(*dropped_item_ids: str) -> None:
        """The template's total-liability opening, minus the balances the filing does not present.

        The template opens the liability schedule from the current portion plus the
        long-term balance; a balance this issuer cites or does not own leaves that
        sum, so the schedule opens from the other alone.
        """
        beginning = model.get_item(_LEASE_BEGINNING)
        if beginning.projected is not None:
            beginning.projected = beginning.projected.without(
                lambda ref: ref.ref.id in dropped_item_ids
            )
        wired.append(beginning.id)

    if not cur_present:
        # No current portion is reported: its ratio schedule has no basis in any forecast period.
        retire_cells((_LEASE_CURRENT, _LEASE_CURRENT_RATE), projected)
    if not rou_present:
        # The ROU schedule does not run: its lines are cited or unowed (or owed and
        # missing), and none is written as a number. A reported liability keeps its
        # history and continues into the ordinary projection.
        retire_schedule(_LEASE_ROU, _LEASE_ROU_ADDITIONS)
        retire_schedule(_LEASE_ROU_AMORTIZATION, periods=(*historical, *projected))
        if not (long_present or cur_present):
            retire_schedule(_LEASE_LONG, _LEASE_BEGINNING, _LEASE_PAYMENTS, _LEASE_ENDING)
            retire_schedule(_LEASE_ADDITIONS, periods=(*historical, *projected))
            return wired

    def close_current_portion_proxies(wired: list[str]) -> None:
        """The flow proxies of a current portion the filing cites (or no issuer owes).

        ROU amortization and liability payments proxy the current portion; with no
        current portion there is no proxy basis, so each projects the zero the
        declared policy has always written there, its derivation naming the cited
        current-portion line. The balances these flows roll are driven to their
        revenue ratios whatever the flows are, so the zero adds no amount to any
        balance; the cited line itself is never written. This is the declared
        closure of NSP-INTANGIBLE-AMORTIZATION (a flow with no balance to run off
        projects zero and carries the cited line as lineage), not a disposition
        zero on a cited cell. A current portion owed and missing keeps the proxy
        cells missing.
        """
        if cur_state == "unbound":
            return
        cited = (_LEASE_CURRENT,) if cur_state != "unowed" else ()
        for proxy_id, note in (
            (_LEASE_ROU_AMORTIZATION, "lease amortization proxy is zero"),
            (_LEASE_PAYMENTS, "lease payments proxy is zero"),
        ):
            installed = _install_zero_projection(
                model,
                item_id=proxy_id,
                projected=projected,
                note=note,
                strategy="historical_zero_balance_policy",
                dependencies=cited,
            )
            if installed is not None:
                wired.append(installed)

    rou_ratio = _latest_ratio(
        values,
        numerator_id=_LEASE_ROU,
        denominator_id=_REVENUE,
        periods=historical,
    )
    long_ratio = (
        _latest_ratio(
            values,
            numerator_id=_LEASE_LONG_STATEMENT,
            denominator_id=_REVENUE,
            periods=historical,
        )
        or 0.0
    )

    if not cur_present and (not long_present or long_zero):
        # No liability schedule runs: its lines are cited, unowed or reported zero,
        # and none is written as a number.
        if rou_ratio and rou_present:
            rou_add = model.get_item(lease_shapes["rou_additions_projected"].target_item)
            rou_add.projected = _lease_reconciliation_formula(
                lease_shapes["rou_additions_projected"], balance_ratio=rou_ratio,
            )
            _set_periods(rou_add, projected)
            wired.append(rou_add.id)
        close_current_portion_proxies(wired)
        return wired

    if not cur_present and long_present and not long_zero:
        # The current portion is cited or owed and missing, never a number here:
        # the liability schedule opens from the reported long-term balance alone.
        open_liability_without(_LEASE_CURRENT)
        current_row = _terminal_rows(role_dispositions)["statement.bs.current_lease_liability"]
        current_inside_long = (
            current_row.state == "not_separately_presented"
            and current_row.included_in == "statement.bs.long_term_lease_liability"
        )
        if rou_ratio and rou_present:
            rou_add = model.get_item(lease_shapes["rou_additions_projected"].target_item)
            rou_add.projected = _lease_reconciliation_formula(
                lease_shapes["rou_additions_projected"], balance_ratio=rou_ratio,
            )
            _set_periods(rou_add, projected)
            wired.append(rou_add.id)
        additions = model.get_item(lease_shapes["liability_additions_projected"].target_item)
        additions.projected = _lease_reconciliation_formula(
            lease_shapes["liability_additions_projected"], balance_ratio=long_ratio,
        )
        _set_periods(additions, projected)
        wired.append(additions.id)
        if current_inside_long:
            # The cited current portion is inside the long-term balance: the
            # long-term schedule is the one balance, never ending less a portion
            # the filing does not present.
            long_schedule = model.get_item(_LEASE_LONG)
            if long_schedule.projected is not None:
                long_schedule.projected = long_schedule.projected.without(
                    lambda ref: ref.ref.id == _LEASE_CURRENT
                )
                wired.append(long_schedule.id)
        close_current_portion_proxies(wired)
        return wired

    if cur_present and (not long_present or long_zero):
        open_liability_without(_LEASE_LONG_STATEMENT)

    if rou_ratio is None or current is None:
        return []

    wired = _install_default_revenue_ratio(
        model,
        target_id=_LEASE_CURRENT,
        rate_id=_LEASE_CURRENT_RATE,
        anchor_id=_LEASE_CURRENT,
        label="  Current operating lease liabilities as % of revenue",
        ratio=current.value,
        projected=projected,
        note="company-grounded latest current-lease-liability/revenue ratio",
        strategy=current.strategy,
    )

    rows = []
    if rou_present:
        rou_amortization = model.get_item(lease_shapes["rou_amortization_proxy"].target_item)
        rou_amortization.projected = _lease_reconciliation_formula(
            lease_shapes["rou_amortization_proxy"],
        )
        rou_additions = model.get_item(lease_shapes["rou_additions_projected"].target_item)
        rou_additions.projected = _lease_reconciliation_formula(
            lease_shapes["rou_additions_projected"], balance_ratio=rou_ratio,
        )
        rows.extend((rou_amortization, rou_additions))

    payments = model.get_item(lease_shapes["liability_payments_proxy"].target_item)
    payments.projected = _lease_reconciliation_formula(
        lease_shapes["liability_payments_proxy"],
    )
    additions = model.get_item(lease_shapes["liability_additions_projected"].target_item)
    additions.projected = _lease_reconciliation_formula(
        lease_shapes["liability_additions_projected"], balance_ratio=current.value + long_ratio,
    )
    rows.extend((payments, additions))
    for item in rows:
        _set_periods(item, projected)
    wired = [*wired, *(item.id for item in rows)]
    return wired


def _business_model_capital_return_mechanisms(
    revision: BusinessModelRevision,
) -> set[str]:
    consolidated = revision.business_model.consolidated
    sources = None if consolidated is None else consolidated.capital_sources
    capital_return = None if sources is None else sources.capital_return
    return set(() if capital_return is None else capital_return.mechanisms)


_CAPITAL_RETURN_MECHANISMS: Mapping[
    Literal["dividends", "repurchases"], re.Pattern[str]
] = MappingProxyType({
    "dividends": re.compile(r"\bdividends?\b", re.IGNORECASE),
    "repurchases": re.compile(r"\b(repurchases?|buybacks?)\b", re.IGNORECASE),
})


def capital_return_bases(
    kind: Literal["dividends", "repurchases"],
    revision: BusinessModelRevision,
    *,
    reported: bool,
) -> frozenset[Literal["business_model", "reported_history"]]:
    """What says the issuer returns capital by ``kind``: the one answer.

    The company-grounded share schedule (``_apply_share_policy``) and the
    forecast-control profile's ``dividends_paid`` / ``share_repurchases``
    applicability both read it; an empty set is an issuer that does not
    return capital that way. ``business_model``: a capital-return mechanism
    names the kind (mechanisms are free text with the amounts the
    BusinessModel cites: ``dividends $1,626M (FY2025)``, ``share repurchases
    (paused FY2025, $0)``, ``buybacks``). ``reported_history``: ``reported``
    is the caller's reading of a bound, nonzero ``statement.cf.dividends_paid``
    / ``statement.cf.share_repurchases`` in the latest historical period
    (the compile reads the value, the profile the filing coverage).
    """

    bases: set[Literal["business_model", "reported_history"]] = set()
    pattern = _CAPITAL_RETURN_MECHANISMS[kind]
    if any(
        pattern.search(mechanism)
        for mechanism in _business_model_capital_return_mechanisms(revision)
    ):
        bases.add("business_model")
    if reported:
        bases.add("reported_history")
    return frozenset(bases)


def _latest_reported_amount(
    values: ComputedValues,
    historical: tuple[int, ...],
    item_id: str,
) -> tuple[int, float] | None:
    """The terminal historical period and its nonzero amount, in dollars.

    ``None`` when the latest historical period reports no amount (an unbound,
    unknown or zero cell): the fact is read from the latest year, never an
    earlier one.
    """

    if not historical:
        return None
    period = max(int(value) for value in historical)
    amount = values.get(item_id, {}).get(period)
    if amount is None or not math.isfinite(float(amount)) or _is_zero_value(amount):
        return None
    return period, abs(float(amount))


def _business_model_has_no_acquisition_program(
    revision: BusinessModelRevision,
) -> bool:
    consolidated = revision.business_model.consolidated
    reinvestment = None if consolidated is None else consolidated.reinvestment
    acquisitions = None if reinvestment is None else reinvestment.acquisitions
    return acquisitions is None or acquisitions.pattern == "none"


def _all_historical_values_are_zero(
    values: ComputedValues,
    *,
    item_id: str,
    periods: Iterable[int],
) -> bool:
    observations = [values.get(item_id, {}).get(int(period)) for period in periods]
    return bool(observations) and all(
        value is not None and math.isfinite(float(value)) and abs(float(value)) <= 1e-12
        for value in observations
    )


def _has_known_historical_value(
    values: ComputedValues,
    *,
    item_id: str,
    periods: Iterable[int],
) -> bool:
    return any(
        value is not None and math.isfinite(float(value))
        for period in periods
        if (value := values.get(item_id, {}).get(int(period))) is not None
    )


def _discloses_sbc_line_split(
    model: FinancialModel,
    periods: tuple[int, ...],
) -> bool:
    """Whether every SBC component lane holds a reported fact in every period.

    ``periods`` are the historical years the aggregate SBC is reported; a
    year with no reported SBC has nothing to allocate, and no such year
    discloses no split.
    """

    if not periods:
        return False
    for component_id in _SBC_COMPONENT_IDS:
        cells = model.get_item(component_id).values
        for period in periods:
            cell = None if cells is None else cells.values.get(period)
            if (
                cell is None
                or cell.value is None
                or operation_kind(cell.provenance) != "fact"
            ):
                return False
    return True


def _is_zero_value(value: object) -> bool:
    return (
        isinstance(value, int | float)
        and math.isfinite(float(value))
        and abs(float(value)) <= 1e-12
    )


def _zero_projection_formula(
    *,
    note: str,
    strategy: FormulaDerivationStrategy,
    dependencies: Iterable[str] = (),
) -> FormulaSpec:
    """A constant-zero projection whose strategy the document reads off the spec.

    The strategy lives only on ``derivation.strategy`` (``_derivation``) — for
    ``terminal_zero_fx_policy`` that is the one key
    ``model_build.document.cell_dispositions`` reads to stamp the cell
    ``EvidencedZero(terminal_zero_fx_policy)``; neither ``note`` (free text)
    nor ``params`` carries it.
    """
    return FormulaSpec(
        type=FormulaType.constant,
        params={"value": 0.0},
        note=note,
        derivation=_derivation(strategy, dependencies),
    )


def _install_zero_projection(
    model: FinancialModel,
    *,
    item_id: str,
    projected: tuple[int, ...],
    note: str,
    strategy: FormulaDerivationStrategy,
    dependencies: Iterable[str] = (),
) -> str | None:
    """Install a constant-zero forecast on ``item_id``.

    ``dependencies`` are the cited-absent or Not Separately Presented lines the
    zero is evidenced by: the derivation names them, and the publisher's
    lineage reads their cited absence onto the zero cell
    (``target_build._enrich_cell_lineages_from_declarations``).
    """
    if not model._index:
        model.build_index()
    if item_id not in model._index:
        return None
    item = model.get_item(item_id)
    # An authored or reported cell that already holds zero agrees with the
    # policy and stays the published cell; a nonzero one would shadow it.
    if item.overrides is not None:
        for period in projected:
            spec = item.overrides.get(int(period))
            if spec is not None and not (
                spec.type == FormulaType.constant
                and _is_zero_value(spec.params.get("value"))
            ):
                item.overrides.pop(int(period))
    if item.values is not None:
        for period in projected:
            cell = item.values.values.get(int(period))
            if cell is not None and not _is_zero_value(cell.value):
                item.values.values.pop(int(period))
    item.projected = _zero_projection_formula(
        note=note, strategy=strategy, dependencies=dependencies
    )
    _set_periods(item, projected)
    return item.id


def _holds_authored_control(
    item: LineItem, projected: tuple[int, ...], *, nonzero_only: bool,
) -> bool:
    periods = _authored_periods(item, projected)
    if not nonzero_only or item.values is None:
        return bool(periods)
    return any(
        (value := item.values.values[period].value) is not None and value != 0.0
        for period in periods
    )


def _forecast_reads_authored_control(
    model: FinancialModel,
    item: LineItem,
    projected: tuple[int, ...],
    *,
    nonzero_only: bool = False,
) -> bool:
    """Read aliases and direct authored or scenario-layer controls, not upstream formulas.

    ``nonzero_only`` reads an authored control only where it holds a nonzero value: rule (d)
    treats an authored zero on a cited-absent optional component as non-participation.
    """

    seen: set[str] = set()
    while item.id not in seen:
        seen.add(item.id)
        if _holds_authored_control(item, projected, nonzero_only=nonzero_only):
            return True
        spec = item.projected
        if spec is None:
            return False
        source = (
            _extract_single_ref(spec.params)
            if spec.type == FormulaType.ref
            and spec.subtype in (None, "cross_sheet_ref")
            else None
        )
        if source is not None and source[1] == 0:
            dependency = model._index.get(source[0])
            if dependency is not None:
                item = dependency
                continue
        for ref_id in _extract_ref_ids(spec.params):
            dependency = model._index.get(ref_id)
            if dependency is None:
                continue
            if _holds_authored_control(dependency, projected, nonzero_only=nonzero_only):
                return True
            # Scenario selections are controls; their layer reports missing judgment.
            selection = dependency.projected
            if (
                selection is not None
                and selection.type == FormulaType.valuation
                and selection.subtype in ("offset_scenario", "scenario_select")
            ):
                return True
        return False
    return False


def _restore_unallocated_periods(item: LineItem, periods: Iterable[int]) -> None:
    """Let a refreshed disclosure or control replace this owner's unallocated marker."""
    overrides = item.overrides or {}
    for period in periods:
        spec = overrides.get(period)
        if (
            spec is None or spec.subtype != "not_separately_presented"
            or spec.derivation is None or spec.derivation.owner != _OWNER
            or spec.note not in (
                "amount unallocated because its expense base is not separately presented",
                "SBC line split unallocated (D24)",
            )
        ):
            continue
        original = spec.params.get("_not_separately_presented", {}).get("original_override")
        if original is None:
            overrides.pop(period)
        else:
            overrides[period] = FormulaSpec.model_validate_json(original)


def _install_nsp_dependent_rate_policy(
    model: FinancialModel,
    *,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
) -> list[str]:
    """Keep a forecast off a rate whose terminal-year ratio reads a line with no history.

    A rate row (a ratio row whose forecast reads its own prior ratio) divides by,
    or is, a line whose terminal historical state in the disposition table is
    ``not_separately_presented`` or ``not_applicable``, directly or through the operands of the
    row it divides. Its terminal-year ratio is unknown, so every later year
    carries the unknown: no rate is invented, the row stays absent with its
    citation and its readers read it as missing. An amount with a known
    terminal balance carries that balance forward; an amount without one stays
    explicitly nonnumeric with the same cited inclusion, never a carry of
    missing history. A forecast cell an authored row lowered stays.
    """

    if not historical:
        return []
    terminal = max(historical)
    for item in model._index.values():
        disclosed = [
            period for period in projected
            if (spec := (item.overrides or {}).get(period)) is not None
            and spec.derivation is not None
            and spec.derivation.dependencies
            and all(
                values.get(source_id, {}).get(terminal) is not None
                for source_id in spec.derivation.dependencies
            )
        ]
        _restore_unallocated_periods(item, disclosed)
    nsp_ids = _cited_terminal_lines(
        model,
        role_dispositions,
        role_items,
        "not_applicable",
        "not_separately_presented",
    )
    if not nsp_ids:
        return []

    def terminal_spec(item: LineItem) -> FormulaSpec | None:
        override = (item.overrides or {}).get(terminal)
        if override is not None:
            return override
        if item.values is not None and terminal in item.values.values:
            return None
        return item.historical

    nsp_sources: dict[str, frozenset[str]] = {}

    def nsp_lines_read(item_id: str) -> frozenset[str]:
        """The Not Separately Presented lines ``item_id`` is, or reads through its terminal formula."""
        if item_id in nsp_ids:
            return frozenset({item_id})
        if item_id in nsp_sources:
            return nsp_sources[item_id]
        nsp_sources[item_id] = frozenset()
        item = model._index.get(item_id)
        spec = None if item is None else terminal_spec(item)
        if spec is not None:
            nsp_sources[item_id] = frozenset().union(
                *(nsp_lines_read(ref_id) for ref_id in _formula_ref_ids(spec.params))
            )
        return nsp_sources[item_id]

    def reads_own_prior_ratio(item: LineItem) -> bool:
        return any(
            spec is not None and item.id in _formula_ref_ids(spec.params)
            for spec in (
                item.projected,
                *(
                    spec
                    for period, spec in (item.overrides or {}).items()
                    if period in projected
                ),
            )
        )

    rate_nsp_lines: dict[str, frozenset[str]] = {}
    for item in model._index.values():
        spec = terminal_spec(item)
        if (
            spec is not None
            and spec.type is FormulaType.ratio
            and reads_own_prior_ratio(item)
        ):
            read = frozenset().union(
                *(
                    nsp_lines_read(ref_id)
                    for key in ("numerator", "denominator")
                    for ref_id in _formula_ref_ids(spec.params.get(key))
                )
            )
            if read:
                rate_nsp_lines[item.id] = read
    rate_ids = set(rate_nsp_lines)

    wired: list[str] = []
    catalog = load_required_role_catalog()
    # A balance row: a balance-sheet role's line, a balance-sheet face, or the schedule row a
    # balance-sheet face presents.
    balance_lines = {
        line
        for role in catalog.roles_on_statement("balance_sheet")
        for line in role_items[role.required_role].all_items()
    }
    for face in model._index.values():
        if face.id.startswith(_FACE_BALANCE_SHEET_PREFIX):
            balance_lines.add(face.id)
            for lane in (face.historical, face.projected):
                if lane is not None and lane.type is FormulaType.ref:
                    balance_lines.update(_formula_ref_ids(lane.params))
    for item in model._index.values():
        if (
            item.id in nsp_ids
            or item.projected is None
            or item.projected.type is not FormulaType.driver
            or not rate_ids & _formula_ref_ids(item.projected.params.get("base"))
        ):
            continue
        if (
            values.get(item.id, {}).get(terminal) is not None
            or terminal_spec(item) is not None
            or item.required_role is not None
        ) and item.id not in balance_lines:
            # A reported flow carries its own rate control: the template's forecast
            # stands. Only a balance has a reported amount to carry.
            continue
        authored = frozenset(_authored_periods(item, projected))
        open_periods = tuple(period for period in projected if period not in authored)
        overrides = item.overrides or {}
        for period in open_periods:
            override = overrides.get(period)
            if override is not None and rate_ids & _formula_ref_ids(override.params):
                del overrides[period]
        cited = tuple(
            sorted(
                frozenset().union(
                    *(
                        rate_nsp_lines[rate_id]
                        for rate_id in (
                            rate_ids
                            & _formula_ref_ids(item.projected.params.get("base"))
                        )
                    )
                )
            )
        )
        if (
            values.get(item.id, {}).get(terminal) is not None
            or terminal_spec(item) is not None
            or item.required_role is not None
        ):
            item.projected = FormulaSpec(
                type=FormulaType.ref,
                params={"source": LineItemRef(id=item.id, t=-1)},
                note="carry_forward_reported_balance_for_nsp_denominator",
                derivation=FormulaDerivation(
                    owner="build_company_grounded_schedules",
                    strategy="historical_zero_balance_policy",
                    version=1,
                    dependencies=cited,
                ),
            )
        else:
            source = model.get_item(cited[0])
            source_spec = terminal_spec(source)
            # The citation is the cited line's own: its marker's, else its role's pin. A
            # marker with an empty citation is no citation (this policy once wrote one
            # when the source held no marker spec): the cell then stays owed, unwritten.
            citation = {} if source_spec is None else source_spec.params.get("absence_citation", {})
            if not citation:
                pinned = next(
                    (
                        row.absence_citation
                        for role, row in _terminal_rows(role_dispositions).items()
                        if row.absence_citation is not None
                        and source.id in _role_lines(model, role_items, role)
                    ),
                    None,
                )
                citation = {} if pinned is None else pinned.model_dump(mode="json")
            if not citation:
                continue
            if item.overrides is None:
                item.overrides = {}
            for period in open_periods:
                item.overrides[period] = not_separately_presented_formula(
                    source.id, citation, original_override=item.overrides.get(period),
                ).model_copy(update={
                    "note": "amount unallocated because its expense base is not separately presented",
                    "derivation": _derivation("historical_zero_balance_policy", cited),
                })
        _set_periods(item, open_periods)
        wired.append(item.id)
    return wired


def _without_slot(spec: FormulaSpec, reads_slot: Callable[[FormulaRef], bool]) -> FormulaSpec | None:
    """``spec`` minus the removed slot, in its live refs and in every formula saved for restore.

    Optional-absence lowering and deferred-tax inclusion keep the formula they
    replaced as an opaque JSON string; restoring a prior projection cannot bring
    a removed balance back, so those saved operands leave too.
    """

    stripped = spec.without(reads_slot)
    if stripped is None:
        return None
    for container, keys in saved_formula_slots(stripped.params):
        for key in keys:
            saved = container.get(key)
            if not isinstance(saved, str) or not saved:
                continue
            held = _without_slot(FormulaSpec.model_validate_json(saved), reads_slot)
            if held is None:
                container.pop(key)
            else:
                container[key] = held.model_dump_json()
    return stripped


def _remove_source_empty_placeholder(model: FinancialModel, item_id: str) -> None:
    """Take a template slot out of the model.

    Every cell of the slot retires through the graph owner first, so each reader
    is rewritten with ``FormulaSpec.without`` (and a reader left with nothing
    retires in turn). The retirement stores each rewrite as a cell override, so
    the readers' base specs are then stripped of the slot the same way; only
    then does the slot leave its sheet and the rows below it move up one.
    """

    item = model.get_item(item_id)
    graph = DependencyGraph()
    graph.build(model)
    for period in (*_historical_periods(model), *_projection_periods(model)):
        graph.retire(item_id, period)

    def reads_slot(ref: FormulaRef) -> bool:
        return ref.ref.id == item_id

    for reader in model._index.values():
        for field in ("historical", "projected"):
            spec = getattr(reader, field)
            if spec is not None:
                setattr(reader, field, _without_slot(spec, reads_slot))
        if reader.overrides:
            historical = set(_historical_periods(model))
            stripped = {
                period: _without_slot(spec, reads_slot)
                for period, spec in reader.overrides.items()
            }
            # A rewrite the retirement stored that now equals the stripped lane is
            # that lane, not a cell of its own.
            reader.overrides = {
                period: spec
                for period, spec in stripped.items()
                if spec is not None
                and spec != (reader.historical if period in historical else reader.projected)
            } or None
    (sheet,) = (
        sheet
        for sheet in model.sheets.values()
        if any(
            other is item
            for section in sheet.sections
            for other in section.line_items
        )
    )
    for section in sheet.sections:
        section.line_items[:] = [
            other for other in section.line_items if other is not item
        ]
        for other in section.line_items:
            if int(other.row) > int(item.row):
                other.row = int(other.row) - 1
    model.build_index()


def _face_row_cash_flow_leg(model: FinancialModel, row_id: str) -> str | None:
    """The cash-flow delta leg of a face balance row, when the model has one.

    The leg is the row's cash-flow twin, and it is the row's own change leg only
    while its formulas read nothing but the row or the schedule that presents
    its balance (`BALANCE_SHEET_FACE_ROWS`): a twin that reads other lines is
    another row's flow and stays.
    """

    if ".balance_sheet." not in row_id:
        return None
    leg_id = row_id.replace(".balance_sheet.", ".cash_flow.")
    leg = model._index.get(leg_id)
    if leg is None:
        return None
    balances = {row_id}.union(
        schedule_id
        for face_id, _label, _anchor_id, schedule_id in BALANCE_SHEET_FACE_ROWS
        if face_id == row_id and schedule_id is not None
    )
    read = set().union(
        *(
            _formula_ref_ids(spec.params)
            for spec in (leg.historical, leg.projected, *(leg.overrides or {}).values())
            if spec is not None
        ),
        set(),
    )
    return leg_id if read and read <= balances else None


def _remove_face_row(model: FinancialModel, row_id: str) -> None:
    """Take a face balance row out of the model with its cash-flow delta leg.

    A leg whose balance left the model would otherwise move cash against a
    balance no total holds.
    """

    leg_id = _face_row_cash_flow_leg(model, row_id)
    _remove_source_empty_placeholder(model, row_id)
    if leg_id is not None:
        _remove_source_empty_placeholder(model, leg_id)


def _face_row_schedules(model: FinancialModel, row_id: str) -> tuple[str, ...]:
    """The schedule lines that present a face balance row's balance.

    A schedule presents the row when either lane cross-references the other
    row, or when the face-row baseline pairs them (`BALANCE_SHEET_FACE_ROWS`).
    """

    paired = {
        schedule_id
        for face_id, _label, _anchor_id, schedule_id in BALANCE_SHEET_FACE_ROWS
        if face_id == row_id and schedule_id is not None
    }
    mirrors = {
        item.id
        for item in model._index.values()
        if item.id != row_id
        and any(
            spec is not None
            and spec.type is FormulaType.ref
            and spec.subtype == "cross_sheet_ref"
            and _formula_ref_ids(spec.params) == {row_id}
            for spec in (item.historical, item.projected)
        )
    }
    row = model._index.get(row_id)
    sources = {
        source_id
        for spec in (() if row is None else (row.historical, row.projected))
        if spec is not None
        and spec.type is FormulaType.ref
        and spec.subtype == "cross_sheet_ref"
        for source_id in _formula_ref_ids(spec.params)
        if source_id.startswith("tpl.a.") and source_id in model._index
    }
    return tuple(sorted(paired | mirrors | sources))


def _schedule_driver_ids(model: FinancialModel, schedule_id: str) -> set[str]:
    schedule = model._index.get(schedule_id)
    if schedule is None:
        return set()
    specs = (schedule.historical, schedule.projected, *(schedule.overrides or {}).values())
    sources = {
        driver_id
        for spec in specs
        if spec is not None
        for driver_id in _formula_ref_ids(spec.params)
        if driver_id != schedule_id
    }
    rate_sources = {
        ref.ref.id
        for spec in specs
        if spec is not None
        for ref in spec.references()
        if ref.role == "rate" and ref.ref.id != schedule_id
    }
    return {
        item.id
        for item in model._index.values()
        if item.id.startswith("tpl.a.")
        and item.item_type in (ItemType.input, ItemType.derived)
        and not any(
            role.role == "section_total" for role in item.statement_section_roles or ()
        )
        and (
            (item.item_type is ItemType.input and item.id in sources)
            or item.id in rate_sources
            or any(
                spec is not None
                and spec.type in (FormulaType.ratio, FormulaType.driver)
                and schedule_id in _formula_ref_ids(spec.params)
                for spec in (item.historical, item.projected, *(item.overrides or {}).values())
            )
        )
    }


def _remove_unowed_face_row(model: FinancialModel, row_id: str) -> None:
    """Take a face balance row the issuer does not owe out with its schedule.

    The row leaves with its cash-flow delta leg and the schedule that presents
    its balance, then orphan driver inputs and ratios of that schedule: a
    ratio left behind would read a balance that no longer exists.
    """

    schedule_ids = _face_row_schedules(model, row_id)
    drivers: set[str] = set()
    for schedule_id in schedule_ids:
        drivers.update(_schedule_driver_ids(model, schedule_id))
    _remove_face_row(model, row_id)
    for schedule_id in schedule_ids:
        if schedule_id not in model._index:
            continue
        _remove_source_empty_placeholder(model, schedule_id)
    while drivers:
        removed: set[str] = set()
        for driver_id in sorted(drivers):
            if driver_id not in model._index:
                removed.add(driver_id)
            elif not any(
                reader.id != driver_id
                and any(
                    spec is not None and driver_id in _formula_ref_ids(spec.params)
                    for spec in (reader.historical, reader.projected, *(reader.overrides or {}).values())
                )
                for reader in model._index.values()
            ):
                _remove_source_empty_placeholder(model, driver_id)
                removed.add(driver_id)
        if not removed:
            break
        drivers.difference_update(removed)


def _remove_orphan_face_row_cash_flow_legs(model: FinancialModel) -> list[str]:
    """Take out the cash-flow leg of every baseline face row the model lacks.

    A model whose face row is already gone (a frozen snapshot re-applying this
    policy) keeps the leg the schedule builders installed on the row's
    schedule; with no balance-sheet twin it counts the movement a second time.
    """

    removed: list[str] = []
    for row_id, *_rest in BALANCE_SHEET_FACE_ROWS:
        if row_id in model._index:
            continue
        leg_id = _face_row_cash_flow_leg(model, row_id)
        if leg_id is not None:
            _remove_source_empty_placeholder(model, leg_id)
            removed.append(leg_id)
    return removed


def _remove_face_rows_another_line_holds(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
) -> list[str]:
    """Take out a baseline face row whose balance another face line holds.

    The accounting baseline gives each current balance its own face row
    (`BALANCE_SHEET_FACE_ROWS`). A filer that reports such a balance inside
    another face line (for example deferred revenue and a current lease
    liability inside accrued expenses, alongside a prepaid-and-other line that
    also fills the prepaid slot) shows it in a disclosure, not as a term of
    the section total. The proof is the reported section total of the latest
    historical year: it equals the sum of the total's other lines, so those
    rows add nothing the total does not already hold and leave the model, one
    balance one term on both sides of the seam. A schedule such a row
    presented stays a disclosure schedule; a lease liability stated inside
    accrued expenses is matched `not_separately_presented` with `included_in`
    on its statement binding, never reclassified here.
    """

    terminal = max(historical)
    rows_by_total: dict[str, list[str]] = {}
    catalog = load_required_role_catalog()
    for item_id, *_rest in BALANCE_SHEET_FACE_ROWS:
        total_id = catalog.role_for_face_line(item_id).parent_total
        if total_id is not None and {item_id, total_id} <= set(model._index):
            rows_by_total.setdefault(total_id, []).append(item_id)
    removed: list[str] = []
    for total_id, row_ids in rows_by_total.items():
        reported_total = values.get(total_id, {}).get(terminal)
        spec = model.get_item(total_id).projected
        members = (
            None
            if spec is None
            else [
                ref.ref.id
                for ref in spec.references()
                if ref.role == "additive" and ref.ref.t == 0
            ]
        ) or None
        if reported_total is None or members is None:
            continue
        held = [
            row_id
            for row_id in row_ids
            if row_id in members
            and float(values.get(row_id, {}).get(terminal) or 0.0) != 0.0
        ]
        others = sum(
            float(values.get(member, {}).get(terminal) or 0.0)
            for member in members
            if member not in row_ids
        )
        if not held or not math.isclose(
            float(reported_total), others, rel_tol=1e-9, abs_tol=1e-6
        ):
            continue
        for row_id in held:
            _remove_face_row(model, row_id)
            removed.append(row_id)
    return removed


_SUM_TERM_ROLES = ("additive", "subtractive", "minuend")


def _sums_terms(spec: FormulaSpec) -> bool:
    """Whether ``spec`` sums terms: it has an additive, subtractive or minuend reference."""

    return any(ref.role in _SUM_TERM_ROLES for ref in spec.references())


def _is_difference(spec: FormulaSpec) -> bool:
    """Whether ``spec`` subtracts from a first operand (a minuend reference)."""

    return any(ref.role == "minuend" for ref in spec.references())


def _same_period_member_ids(spec: FormulaSpec) -> set[str]:
    """The lines ``spec`` sums at the same period."""

    return {
        ref.ref.id
        for ref in spec.references()
        if ref.role in _SUM_TERM_ROLES and ref.ref.t == 0
    }


def _role_item_ids(
    model: FinancialModel,
    role: str,
    role_items: Mapping[str, Collection[str]],
) -> set[str]:
    catalog = load_required_role_catalog()
    declared = catalog._row_or_none(role)
    return {
        *(declared.template_items if declared is not None else ()),
        *catalog.population_item_ids(role, ticker=model.company.ticker),
        *role_items.get(role, ()),
    } & model._index.keys()


def _is_optional_component(role: RequiredRole) -> bool:
    """Whether rule (d) drops ``role``'s flow component from an additive reader on a cited absence.

    ``optional_if_unreported`` (an optional row) or ``presentation_optional`` (a function a filer
    may present inside another line); optional balance-sheet faces have their own removal owner.
    A residual (``residual_of``) is the plug of a filed total, so its optionality only means no
    filed line is owed: an absent or included residual never leaves the total it closes.
    """

    return role.statement in {"income_statement", "cash_flow"} and bool(
        (role.optional_if_unreported and role.residual_of is None) or role.presentation_optional
    )


def _period_formula(model: FinancialModel, item: LineItem, period: int) -> FormulaSpec | None:
    """The formula that reads ``item`` in ``period``: a non-constant override, else its lane's.

    A filed constant is an observation, not the template's operand structure. The owed-operand
    annotation is not part of the formula and is left off.
    """

    override = (item.overrides or {}).get(period)
    if override is not None and override.type is not FormulaType.constant:
        return _unstamped(override)
    historical = {int(year) for year in model.time_structure.historical_periods}
    spec = item.historical if period in historical else item.projected
    return None if spec is None else _unstamped(spec)


def _owed_readout_components(
    model: FinancialModel, nonparticipating: Collection[str], period: int,
) -> set[str]:
    """The components of an additive readout whose every operand is cited non-participating.

    A sum of nothing is not zero. Such a readout is owed: it keeps its operands in every reader,
    so each reads Missing until the filer's presentation is resolved. A readout with one
    participating operand is unchanged.
    """

    owed: set[str] = set()
    for reader in model._index.values():
        spec = _period_formula(model, reader, period)
        terms = (
            []
            if spec is None
            else [ref.ref for ref in spec.references() if ref.role in _SUM_TERM_ROLES]
        )
        if terms and all(ref.t == 0 and ref.id in nonparticipating for ref in terms):
            owed.update(ref.id for ref in terms)
    return owed


#: The reason an owed readout's cell is Missing, with the owed operands it names per period:
#: ``{"reason", "periods": {"<year>": [item ids]}}`` on the formula that reads them, read by the
#: build check (``owed_operand_roles``). Re-derived by rule (d) each refresh.
OWED_READOUT_REASON = "operands_cited_non_participating"
_OWED_OPERANDS_KEY = "_owed_operands"


def _unstamped(spec: FormulaSpec) -> FormulaSpec:
    """``spec`` without any owed-operand annotation."""

    if _OWED_OPERANDS_KEY not in spec.params:
        return spec
    return spec.model_copy(
        update={"params": {k: v for k, v in spec.params.items() if k != _OWED_OPERANDS_KEY}},
        deep=True,
    )


def _restamped(
    spec: FormulaSpec, period: int, owed_ids: Collection[str],
) -> FormulaSpec:
    """``spec`` with its owed operands for ``period`` set to ``owed_ids`` (cleared when empty)."""

    record = spec.params.get(_OWED_OPERANDS_KEY)
    periods = dict(record["periods"]) if isinstance(record, dict) else {}
    if sorted(owed_ids) == periods.get(str(period), []):
        return spec
    if owed_ids:
        periods[str(period)] = sorted(owed_ids)
    else:
        periods.pop(str(period))
    params = {key: value for key, value in spec.params.items() if key != _OWED_OPERANDS_KEY}
    if periods:
        params[_OWED_OPERANDS_KEY] = {"reason": OWED_READOUT_REASON, "periods": periods}
    return spec.model_copy(update={"params": params}, deep=True)


def _stamp_owed_operands(
    model: FinancialModel, item: LineItem, period: int, owed_ids: Collection[str],
) -> None:
    """Record on ``item``'s formula for ``period`` the owed operands it reads (none clears it).

    The annotation sits on the formula that reads the cell: a non-constant override, else the lane.
    """

    override = (item.overrides or {}).get(period)
    on_override = override is not None and override.type is not FormulaType.constant
    historical = {int(year) for year in model.time_structure.historical_periods}
    lane = "historical" if period in historical else "projected"
    spec = getattr(item, lane)
    if spec is not None:
        setattr(item, lane, _restamped(spec, period, () if on_override else owed_ids))
    if on_override:
        assert item.overrides is not None and override is not None
        item.overrides[period] = _restamped(override, period, owed_ids)


def owed_operand_roles(
    model: FinancialModel,
    spec: FormulaSpec | None,
    period: int,
    *,
    is_known: Callable[[str], bool],
) -> tuple[str, ...]:
    """The roles of the owed operands that are why ``spec`` is not known in ``period``.

    Empty unless rule (d) stamped ``spec`` and every other operand is known (``is_known``):
    the owed reason is never put on a cell that is non-known for an unrelated operand.
    """

    record = None if spec is None else spec.params.get(_OWED_OPERANDS_KEY)
    owed_ids = set(record["periods"].get(str(period), ())) if isinstance(record, dict) else set()
    if spec is None or not owed_ids or any(
        member not in owed_ids and not is_known(member)
        for member in _same_period_member_ids(spec)
    ):
        return ()
    return tuple(sorted({
        role for member in owed_ids
        if (role := model.get_item(member).required_role) is not None
    }))


def _optional_statement_nonparticipation(
    model: FinancialModel,
    cited_absences: Mapping[str, Mapping[str, str]],
    *,
    period: int,
    role_items: Mapping[str, Collection[str]],
    protected_ids: Collection[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Rule (d): cited optional components leave additive readers, never become zero.

    ``cited_absences`` contains only authoritative not-applicable period pins.
    An uncited missing or required component remains owed. Projection callers
    supply only complete-history absences; an authored nonzero forecast participates.
    A component of a readout whose every operand is cited absent is owed too
    (``_owed_readout_components``): it stays in every reader, whose cell is Missing.
    The caller re-derives operands from its saved original each refresh, keeping
    the source citation and leaving the absent component itself untouched.
    Returns the components that leave and the owed ones held back, each with its role.
    """
    catalog = load_required_role_catalog()
    projected = _projection_periods(model)
    cited: dict[str, str] = {}
    for role in cited_absences:
        declared = catalog._row_or_none(role)
        if declared is None or not _is_optional_component(declared):
            continue
        for item_id in _role_item_ids(model, role, role_items):
            if item_id in protected_ids:
                continue
            item = model.get_item(item_id)
            # An authored forecast participates only where it holds a nonzero value; an authored
            # zero on a cited-absent component is non-participation.
            if period in projected and _forecast_reads_authored_control(
                model, item, projected, nonzero_only=True,
            ):
                continue
            cited[item_id] = role
    held = _owed_readout_components(model, cited, period)
    return (
        {item_id: role for item_id, role in cited.items() if item_id not in held},
        {item_id: role for item_id, role in cited.items() if item_id in held},
    )


_COMPONENT_ABSENT_PERIODS = "optional_absent_periods"


def _component_absences(item: LineItem) -> dict[str, Any]:
    """The periods (as strings) rule (d) holds ``item`` cited absent, with their citations.

    The record rides the component's own projected spec, so a reader that does
    not exist yet (net income before the tax schedule is materialized) still
    finds it.
    """

    spec = item.projected
    metadata = None if spec is None else spec.params.get("_not_separately_presented")
    record = metadata.get(_COMPONENT_ABSENT_PERIODS) if isinstance(metadata, dict) else None
    return record if isinstance(record, dict) else {}


def _record_component_absence(
    item: LineItem, period: int, citation: Mapping[str, str] | None,
) -> None:
    """Set (or, with ``citation`` None, clear) ``item``'s cited-absent record for ``period``.

    Only an arithmetic projected formula carries it: a driver formula's params
    are closed, and a component read by a host's own SUM is answered by that
    host's restore metadata.
    """

    spec = item.projected
    if spec is None or spec.type is not FormulaType.arithmetic:
        return
    record = dict(_component_absences(item))
    if citation is None:
        record.pop(str(period), None)
    else:
        record[str(period)] = dict(citation)
    metadata = {
        key: value
        for key, value in (spec.params.get("_not_separately_presented") or {}).items()
        if key != _COMPONENT_ABSENT_PERIODS
    }
    if record:
        metadata[_COMPONENT_ABSENT_PERIODS] = record
    params = {key: value for key, value in spec.params.items() if key != "_not_separately_presented"}
    if metadata:
        params["_not_separately_presented"] = metadata
    item.projected = spec.model_copy(update={"params": params}, deep=True)


def _statement_year_sourced(model: FinancialModel, year: int, *, excluding_role: str) -> bool:
    """Whether another line in the role's statement carries a filed value or pin for ``year``.

    A year none does is outside the statement's source window, not a history
    year ``excluding_role`` is owed in.
    """

    catalog = load_required_role_catalog()
    statement = catalog.role(excluding_role).statement
    for declared in catalog.required_roles:
        if declared.statement != statement or declared.required_role == excluding_role:
            continue
        for item_id in _role_item_ids(model, declared.required_role, {}):
            item = model.get_item(item_id)
            values = item.values.values if item.values is not None else {}
            if (item.overrides or {}).get(year) is not None or values.get(year) is not None:
                return True
    return False


def _component_absence_held(model: FinancialModel, item: LineItem) -> bool:
    """Whether ``item`` is cited absent in every sourced historical year and no author holds its forecast."""

    record = _component_absences(item)
    role = item.required_role
    if not record or role is None:
        return False
    if not all(
        str(year) in record or not _statement_year_sourced(model, year, excluding_role=role)
        for year in (int(year) for year in model.time_structure.historical_periods)
    ):
        return False
    return not _forecast_reads_authored_control(
        model, item, _projection_periods(model), nonzero_only=True,
    )


def _forecast_absence_split(model: FinancialModel) -> tuple[set[str], set[str]]:
    """The components held cited absent over the forecast: those that leave it, and the owed ones.

    A held component of a readout whose every operand is held is owed, never dropped.
    """

    held = {item.id for item in model._index.values() if _component_absence_held(model, item)}
    periods = _projection_periods(model)
    owed = held & _owed_readout_components(model, held, min(periods)) if periods else set()
    return held - owed, owed


def _component_absence_complete(model: FinancialModel, item: LineItem) -> bool:
    """Whether rule (d) drops ``item`` from the forecast: held cited absent and not owed."""

    return item.id in _forecast_absence_split(model)[0]


def optional_component_participates(
    model: FinancialModel, item_id: str, period: int,
) -> bool:
    """False exactly where rule (d) holds cited non-participation for ``item_id`` in ``period``.

    Rule (d) records each cited optional absence in the restore metadata of the
    formula it lowered: ``_not_separately_presented.optional_absences``, keyed by
    the component's required role, on the period's historical override or on the
    projected formula. It also records the component's own cited-absent periods
    on its projected spec, which is what answers for a reader built after rule
    (d) ran: a component cited absent in every sourced historical year does not
    participate in the forecast. A component carrying no such citation participates.
    """

    item = model.get_item(item_id)
    role = item.required_role
    if role is None:
        return True
    if not model._index:
        model.build_index()
    forecast = int(period) in _projection_periods(model)
    for reader in model._index.values():
        spec = reader.projected if forecast else (reader.overrides or {}).get(int(period))
        metadata = None if spec is None else spec.params.get("_not_separately_presented")
        if isinstance(metadata, dict) and role in (metadata.get("optional_absences") or {}):
            return False
    if forecast:
        return not _component_absence_complete(model, item)
    return str(int(period)) not in _component_absences(item)


def _apply_forecast_seam_continuity(
    model: FinancialModel,
    historical: Iterable[int],
    *,
    metadata_key: str = "_not_separately_presented",
    role_items: Mapping[str, Collection[str]] | None = None,
    protected_cells: Collection[tuple[str, int]] = (),
) -> None:
    """Re-apply terminal inclusion and optional participation pins to the forecast.

    Inclusion-only totals retain their terminal lowered formula. Optional
    components instead retain each lane's own SUM: a historical-only host must
    not replace authored child forecasts. Both paths save the projected original
    in restore metadata, so a later clear or presented binding restores operands.
    Only source reconciliation supplies role mappings and authored-cell protection;
    the later balance-policy pass must leave that optional participation intact.
    """

    if not model._index:
        model.build_index()
    # The owed-operand annotation is re-derived from the source maps each refresh.
    if role_items is not None:
        for item in model._index.values():
            if item.projected is not None and _OWED_OPERANDS_KEY in item.projected.params:
                item.projected = item.projected.model_copy(
                    update={"params": {
                        key: value for key, value in item.projected.params.items()
                        if key != _OWED_OPERANDS_KEY
                    }},
                    deep=True,
                )
    forecast_owed: set[str] = set()
    optional_roles = {
        role.required_role for role in load_required_role_catalog().required_roles
        if _is_optional_component(role)
    }
    years = [int(year) for year in historical]

    def optional_path(metadata: Mapping[str, Any]) -> bool:
        return bool(
            metadata.get("optional_absences")
            or optional_roles.intersection(metadata.get("removed_roles", {}).values())
        )

    for item in model._index.values():
        projected = item.projected
        if projected is None or metadata_key not in projected.params:
            continue
        metadata = projected.params[metadata_key]
        # Without source role maps the optional path cannot be re-derived, so a
        # lowering it wrote stays: the projected metadata records it only when
        # the absence was complete history, the terminal year's marker whenever
        # the host is on that path at all.
        terminal_override = (item.overrides or {}).get(max(years)) if years else None
        if role_items is None and (
            optional_path(metadata)
            or (
                terminal_override is not None
                and optional_path(terminal_override.params.get(metadata_key, {}))
            )
        ):
            continue
        saved = metadata.get("original_formula")
        if isinstance(saved, str):
            item.projected = FormulaSpec.model_validate_json(saved)
    if not years:
        return
    terminal = max(years)
    for item in model._index.values():
        terminal_override = (item.overrides or {}).get(terminal)
        projected = item.projected
        if (
            terminal_override is None
            or metadata_key not in terminal_override.params
            or projected is None
            or projected.type is not FormulaType.arithmetic
        ):
            continue
        metadata = terminal_override.params[metadata_key]
        if not isinstance(metadata.get("original_formula"), str):
            continue
        optional_parents = optional_roles.intersection(metadata.get("removed_roles", {}).values())
        if metadata.get("optional_absences") or optional_parents:
            if role_items is None:
                continue
            projected_periods = _projection_periods(model)
            protected_ids = {
                item_id for item_id, year in protected_cells if year in projected_periods
            }
            members = _same_period_member_ids(projected)
            removed_ids: set[str] = set()
            removed_roles: dict[str, str] = {}
            for role, parent in metadata.get("removed_roles", {}).items():
                # Filed totals did not drive legacy inclusion-only forecast rewrites.
                if (
                    (terminal_override.type is FormulaType.arithmetic or parent in optional_roles)
                    and members & _role_item_ids(model, parent, role_items)
                ):
                    children = _role_item_ids(model, role, role_items)
                    children.update(
                        line.id for line in model._index.values()
                        if (marker := (line.overrides or {}).get(terminal)) is not None
                        and marker.subtype == "not_separately_presented"
                        and marker.params.get(metadata_key, {}).get("required_role") == role
                    )
                    removed_ids.update(children)
                    removed_roles[role] = parent
            def _sourced(year: int, role: str) -> bool:
                # A year no sibling operand carries a filed value or pin for is
                # outside the statement's source window (an opening-balance
                # axis year), not a history year the role is owed in.
                owed_ids = _role_item_ids(model, role, role_items)
                for member_id in members - owed_ids:
                    member = model.get_item(member_id)
                    values = member.values.values if member.values is not None else {}
                    if (member.overrides or {}).get(year) is not None or values.get(year) is not None:
                        return True
                return False

            complete_absences = {
                role: citation
                for role, citation in metadata.get("optional_absences", {}).items()
                if all(
                    (
                        (override := (item.overrides or {}).get(year)) is not None
                        and role in override.params.get(metadata_key, {}).get("optional_absences", {})
                    ) or not _sourced(year, role)
                    for year in years
                )
            }
            optional_ids, owed_ids = _optional_statement_nonparticipation(
                model, complete_absences, period=min(projected_periods),
                role_items=role_items, protected_ids=protected_ids,
            ) if projected_periods else ({}, {})
            forecast_owed.update(owed_ids)
            removed_ids.update(optional_ids)
            lowered = projected.without(
                lambda ref: ref.ref.t == 0
                and (
                    (ref.role in ("additive", "subtractive") and ref.ref.id in optional_ids)
                    or (ref.role == "additive" and ref.ref.id in removed_ids)
                )
            )
            if lowered is not None and lowered.params != projected.params:
                projection_metadata = {
                    "original_formula": projected.model_dump_json(),
                    "was_override": False,
                    "removed_roles": removed_roles,
                }
                if optional_ids:
                    projection_metadata["optional_absences"] = {
                        role: complete_absences[role] for role in set(optional_ids.values())
                    }
                item.projected = lowered.model_copy(
                    update={"params": {**lowered.params, metadata_key: projection_metadata}},
                    deep=True,
                )
            continue
        if terminal_override.type is not FormulaType.arithmetic:
            continue
        item.projected = terminal_override.model_copy(
            update={"params": {
                **{
                    key: value
                    for key, value in terminal_override.params.items()
                    if key != metadata_key
                },
                metadata_key: {
                    "original_formula": projected.model_dump_json(),
                    "was_override": False,
                    "removed_roles": dict(metadata.get("removed_roles") or {}),
                },
            }},
            deep=True,
        )
    projected_periods = _projection_periods(model)
    if role_items is None or not projected_periods:
        return
    # A reader no historical override lowers (net income reads the optional
    # component only in the forecast) is lowered from the component's own record.
    protected_ids = {item_id for item_id, year in protected_cells if year in projected_periods}
    catalog = load_required_role_catalog()
    absence_leaving, record_owed = _forecast_absence_split(model)
    forecast_owed.update(record_owed)
    for reader in model._index.values():
        projected = reader.projected
        if projected is None or reader.id in protected_ids or not _sums_terms(projected):
            continue
        absent_ids: set[str] = set()
        for member_id in _same_period_member_ids(projected):
            component = model._index.get(member_id)
            declared = (
                None if component is None or component.required_role is None
                else catalog._row_or_none(component.required_role)
            )
            if (
                component is not None
                and declared is not None
                and member_id not in protected_ids
                and _is_optional_component(declared)
                and member_id in absence_leaving
            ):
                absent_ids.add(member_id)
        lowered = projected.without(
            lambda ref: ref.role in ("additive", "subtractive")
            and ref.ref.t == 0
            and ref.ref.id in absent_ids
        )
        if lowered is None or lowered.params == projected.params:
            continue
        dropped = absent_ids - _same_period_member_ids(lowered)
        existing = projected.params.get(metadata_key)
        restore = existing if isinstance(existing, dict) else {}
        if not isinstance(restore.get("original_formula"), str):
            restore = {
                **restore,
                "original_formula": projected.model_dump_json(),
                "was_override": False,
                "removed_roles": {},
            }
        optional_absences = dict(restore.get("optional_absences") or {})
        for member_id in dropped:
            component = model.get_item(member_id)
            record = _component_absences(component)
            optional_absences[cast(str, component.required_role)] = record[max(record, key=int)]
        reader.projected = lowered.model_copy(
            update={"params": {**lowered.params, metadata_key: {
                **restore, "optional_absences": optional_absences,
            }}},
            deep=True,
        )
    for reader in model._index.values():
        projected = reader.projected
        if projected is None or not _sums_terms(projected):
            continue
        owed_members = _same_period_member_ids(projected) & forecast_owed
        if owed_members:
            for period in projected_periods:
                _stamp_owed_operands(model, reader, period, owed_members)


def not_separately_presented_formula(
    included_in: str,
    absence_citation: Mapping[str, str],
    *,
    original_override: FormulaSpec | None = None,
) -> FormulaSpec:
    """The nonnumeric source cell, retaining an intentionally replaced formula."""
    params: dict[str, Any] = {
        "formula": "=NA()",
        "included_in": included_in,
        "absence_citation": dict(absence_citation),
    }
    metadata_key = "_not_separately_presented"
    if original_override is not None:
        if original_override.subtype == "not_separately_presented":
            metadata = original_override.params.get(metadata_key)
            if metadata is not None:
                params[metadata_key] = dict(metadata)
        elif original_override.type is not FormulaType.constant:
            params[metadata_key] = {
                "original_override": original_override.model_dump_json(),
            }
    return FormulaSpec(
        type=FormulaType.raw,
        subtype="not_separately_presented",
        params=params,
    )


def apply_not_separately_presented_periods(
    model: FinancialModel,
    requirements: Iterable[AnnualSourceRequirement],
    *,
    cleared_role_periods: Iterable[tuple[str, int]] = (),
    role_items: Mapping[str, Collection[str]] | None = None,
    protected_cells: Collection[tuple[str, int]] = (),
) -> None:
    """Reconcile authoritative role-period pins without relaxing ordinary SUM.

    Generated overrides retain their original period formula so refresh can
    restore an operand or change its declared parent. Formula JSON stays opaque
    to dependency extraction: the saved formula is not a current dependency.
    Other roles' inclusion and unmentioned periods keep their existing state.
    Explicit refresh clears restore inclusion structure without inventing a pin.
    ``role_items`` carries the full role-to-line identity for restricted refills,
    including inclusion parents that have no catalog template line.
    Filed constants stay authoritative until formula-first reconciles the saved
    lowered expression; both it and the original source survive a later rebind.
    ``protected_cells`` are authored document cells, not filed constants: they
    retain their values, formulas and operand membership rather than taking a
    source inclusion marker or its generated additive expression. The rewires
    that re-read these pins honor the same cells: a protected period keeps its
    value and formula, and a line protected in every period keeps its projected
    formula. A forecast total is re-lowered from the terminal year's active
    pins, and the projected formula that lowering replaces is recorded in the
    same restore metadata, so a clear puts the operand back.
    Generated inclusion metadata is restored before that protection applies:
    a later authored formula must not stay hidden by an older lowered override.
    """
    model.build_index()
    catalog = load_required_role_catalog()
    requirements = tuple(requirements)
    mapped_items = {role: set(ids) for role, ids in (role_items or {}).items()}
    for requirement in requirements:
        mapped_items.setdefault(requirement.required_role, set()).add(requirement.model_item)
    historical = frozenset(int(year) for year in model.time_structure.historical_periods)
    cells_by_period: dict[int, dict[str, PeriodPinCell | None]] = {}
    for requirement in requirements:
        for period_id in requirement.period_ids:
            cells_by_period.setdefault(int(period_id.removeprefix("FY")), {})[
                requirement.required_role
            ] = requirement.cell_for_period(period_id)
    for role, period in cleared_role_periods:
        cells_by_period.setdefault(period, {})[role] = None
    metadata_key = "_not_separately_presented"
    protected = frozenset(protected_cells)


    for period, cells in cells_by_period.items():
        protected_ids = {item_id for item_id, year in protected if year == period}
        inclusions: dict[str, str] = {}
        optional_absences: dict[str, dict[str, str]] = {}
        originals: dict[str, tuple[FormulaSpec, bool]] = {}
        role_ids: dict[str, set[str]] = {role: set() for role in cells}
        for requirement in requirements:
            if requirement.required_role in role_ids:
                role_ids[requirement.required_role].add(requirement.model_item)
        for declared in catalog.required_roles:
            if declared.required_role in role_ids:
                role_ids[declared.required_role].update(declared.template_items)
                role_ids[declared.required_role].update(
                    catalog.population_item_ids(
                        declared.required_role, ticker=model.company.ticker,
                    )
                )
        for item in model._index.values():
            if item.overrides is None:
                continue
            override = item.overrides.get(period)
            if override is None:
                continue
            metadata = override.params.get(metadata_key, {})
            if override.subtype == "not_separately_presented":
                role = metadata.get("required_role")
                if role is None:
                    role = next(
                        (role for role, ids in role_ids.items() if item.id in ids),
                        None,
                    )
                if role in cells:
                    original_override = metadata.get("original_override")
                    if original_override is not None:
                        item.overrides[period] = FormulaSpec.model_validate_json(
                            original_override
                        )
                    else:
                        item.overrides.pop(period)
                elif role is not None:
                    inclusions[role] = override.params["included_in"]
            elif metadata:
                inclusions.update(metadata["removed_roles"])
                optional_absences.update(metadata.get("optional_absences", {}))
                original = FormulaSpec.model_validate_json(metadata["original_formula"])
                was_override = metadata["was_override"]
                originals[item.id] = (original, was_override)
                if was_override:
                    item.overrides[period] = original
                else:
                    item.overrides.pop(period)
                if item.values is not None:
                    item.values.values.pop(period, None)

        for role, cell in cells.items():
            inclusions.pop(role, None)
            optional_absences.pop(role, None)
            if (
                cell is not None
                and cell.binding_state == "not_applicable"
                and cell.absence_citation is not None
            ):
                optional_absences[role] = cell.absence_citation.model_dump(mode="json")
            if cell is not None and cell.binding_state == "not_separately_presented":
                # An account cannot include its own canonical alias home when they are distinct roles;
                # such an inclusion is an alias-homing loop that erases the presented line.
                included_in = cast(str, cell.included_in)
                role_row = catalog._row_or_none(role)
                role_canon = (role_row.alias_of if role_row is not None else None) or role
                parent_canon = catalog.role(included_in).alias_of or included_in
                if role == included_in or role_canon != parent_canon:
                    inclusions[role] = included_in

        child_ids: dict[str, set[str]] = {}
        parent_ids: dict[str, set[str]] = {}
        for role, parent_role in inclusions.items():
            children = _role_item_ids(model, role, mapped_items) - protected_ids
            parents = _role_item_ids(model, parent_role, mapped_items)
            for item in model._index.values():
                if item.id in protected_ids:
                    continue
                override = (item.overrides or {}).get(period)
                if (
                    override is not None
                    and override.subtype == "not_separately_presented"
                    and override.params.get(metadata_key, {}).get("required_role") == role
                ):
                    children.add(item.id)
            # Resolve transitive face aliases independently of worksheet order.
            previous: tuple[int, int] | None = None
            while previous != (len(children), len(parents)):
                previous = (len(children), len(parents))
                for item in model._index.values():
                    spec = _period_formula(model, item, period)
                    if spec is not None and spec.type is FormulaType.ref:
                        ref = spec.params.get("source")
                        if isinstance(ref, LineItemRef) and ref.t == 0:
                            if ref.id in children and item.id not in protected_ids:
                                children.add(item.id)
                            if ref.id in parents:
                                parents.add(item.id)
            parents.difference_update(children)
            child_ids[role] = children
            parent_ids[role] = parents

        for role, cell in cells.items():
            if cell is None or cell.binding_state != "not_separately_presented":
                continue
            citation = cast(AbsenceCitation, cell.absence_citation).model_dump(mode="json")
            for item_id in child_ids.get(role, ()):
                item = model.get_item(item_id)
                override = (item.overrides or {}).get(period)
                marker = not_separately_presented_formula(
                    cast(str, cell.included_in), citation, original_override=override,
                )
                metadata = {
                    **marker.params.get(metadata_key, {}),
                    "required_role": role,
                }
                if item.values is not None:
                    item.values.values.pop(period, None)
                if item.overrides is None:
                    item.overrides = {}
                item.overrides[period] = marker.model_copy(
                    update={"params": {**marker.params, metadata_key: metadata}},
                    deep=True,
                )
        optional_ids, owed_ids = _optional_statement_nonparticipation(
            model, optional_absences, period=period,
            role_items=mapped_items, protected_ids=protected_ids,
        )
        for role in cells:
            declared = catalog._row_or_none(role)
            if declared is None or not _is_optional_component(declared):
                continue
            for item_id in _role_item_ids(model, role, mapped_items):
                _record_component_absence(
                    model.get_item(item_id), period,
                    optional_absences[role] if item_id in optional_ids else None,
                )

        for item in model._index.values():
            if item.id in protected_ids:
                continue
            spec = _period_formula(model, item, period)
            if spec is None:
                continue
            if not _sums_terms(spec):
                continue
            same_period = _same_period_member_ids(spec)
            if _is_difference(spec):
                # Inclusion is additive; a difference's terms leave only on a cited optional absence.
                removed_roles: dict[str, str] = {}
                removed_ids = set(optional_ids)
            else:
                removed_roles = {
                    role: parent for role, parent in inclusions.items()
                    if same_period & parent_ids[role] and same_period & child_ids[role]
                }
                removed_ids = {
                    item_id for role in removed_roles for item_id in child_ids[role]
                }
                removed_ids.update(optional_ids)
            # An inclusion term leaves additive refs only: a subtractive ref of an
            # included child stays (a residual needs it). A cited optional absence
            # leaves either way.
            lowered = spec.without(
                lambda ref: ref.ref.t == 0
                and (
                    (ref.role in ("additive", "subtractive") and ref.ref.id in optional_ids)
                    or (ref.role == "additive" and ref.ref.id in removed_ids)
                )
            )
            if lowered is None or lowered.params == spec.params:
                continue
            removed_optional = {
                optional_ids[item_id]
                for item_id in same_period - _same_period_member_ids(lowered)
                if item_id in optional_ids
            }
            override = (item.overrides or {}).get(period)
            original, was_override = originals.get(
                item.id, (override or spec, override is not None)
            )
            metadata = {
                "original_formula": original.model_dump_json(),
                "was_override": was_override,
                "removed_roles": removed_roles,
            }
            if removed_optional:
                metadata["optional_absences"] = {
                    role: optional_absences[role] for role in removed_optional
                }
            if override is not None and override.type is FormulaType.constant:
                metadata["lowered_formula"] = lowered.model_dump_json()
                lowered = override
            if item.values is not None:
                item.values.values.pop(period, None)
            if item.overrides is None:
                item.overrides = {}
            item.overrides[period] = lowered.model_copy(
                update={"params": {
                    **lowered.params,
                    metadata_key: metadata,
                }},
                deep=True,
            )
        # A reader of an owed component stays Missing; it records which operands, per period.
        for item in model._index.values():
            spec = None if item.id in protected_ids else _period_formula(model, item, period)
            _stamp_owed_operands(
                model, item, period,
                _same_period_member_ids(spec) & owed_ids.keys() if spec is not None else (),
            )

    _apply_forecast_seam_continuity(
        model, historical, metadata_key=metadata_key,
        role_items=mapped_items, protected_cells=protected,
    )
    # Pins land on the interest row after the template declares it; lower the
    # other-income formulas by the fact the row now carries.
    lower_interest_inclusion(model, protected_cells=protected)
    _rewire_deferred_tax_cash_flow_for_asset_fact(model, protected_cells=protected)


def _declared_balance_nodes(
    revision: BusinessModelRevision,
    compiled_registry: CompiledDriverRegistry,
) -> dict[str, tuple[DriverNode, str]]:
    """Each ``balance_of`` role's declaring node and its compiled line."""

    declared: dict[str, tuple[DriverNode, str]] = {}
    for segment in revision.business_model.segments:
        pending = list(segment.revenue_model.decomposition)
        while pending:
            node = pending.pop()
            pending.extend(node.children or ())
            balance_role = declared_balance_role(node)
            if balance_role is not None:
                declared[balance_role] = (
                    node,
                    compiled_registry.node_items[f"{segment.id}.{node.id}"],
                )
    return declared


def _apply_opening_balances(
    model: FinancialModel,
    revision: BusinessModelRevision,
    compiled_registry: CompiledDriverRegistry,
    historical: tuple[int, ...],
    *,
    opening_balances: Mapping[str, OpeningBalance],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Open the first historical year on the pin's prior-year balances.

    A declared period-average balance averages its first year over the
    opening balance, where the axis alone leaves that year formula-less, and
    opening cash is the prior year's reported ending cash rather than ending
    cash less net change. Each such cell cites its opening evidence; a balance
    the pin carries no opening for keeps the axis-only form.
    """

    first = historical[0]
    wired: list[str] = []
    coverage: list[dict[str, Any]] = []
    catalog = load_required_role_catalog()
    for role, (node, node_id) in _declared_balance_nodes(
        revision, compiled_registry
    ).items():
        opening = opening_balances.get(role)
        if opening is None or node.observation_basis != "period_average":
            continue
        balance_id = catalog.balance(role).target_item
        if balance_id is None:
            continue
        node_item = model.get_item(node_id)
        if node_item.overrides is None:
            node_item.overrides = {}
        node_item.overrides[first] = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="period_average_from_reported_period_ends",
            params={
                "expr": {
                    "op": "*",
                    "args": [
                        {
                            "op": "+",
                            "args": [
                                float(opening.evidence.value),
                                LineItemRef(id=balance_id),
                            ],
                        },
                        0.5,
                    ],
                }
            },
            note="first-year period-average balance over the pinned opening balance",
            derivation=_derivation("declared_balance_roll", (balance_id,)),
        )
        node_item.formula_periods = sorted(
            set(node_item.formula_periods or ()) | {first}
        )
        _restore_node_readers(model, node_item.id, historical)
        wired.append(node_item.id)
        coverage.append(opening.coverage(resolved_item_id=node_item.id, period=first))
    cash_opening = opening_balances.get(
        catalog.role_for_template_item(_CASH_ENDING).required_role
    )
    if cash_opening is not None:
        cash_beginning = model.get_item(_CASH_BEGINNING)
        if cash_beginning.overrides is None:
            cash_beginning.overrides = {}
        cash_beginning.overrides[first] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": float(cash_opening.evidence.value)},
            note="opening cash is the prior year's reported ending cash",
        )
        wired.append(cash_beginning.id)
        coverage.append(
            cash_opening.coverage(resolved_item_id=cash_beginning.id, period=first)
        )
    return wired, coverage


def _optional_face_slots(
    ticker: str,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Face rows of the catalog's optional-if-unreported balance-sheet and income-statement roles.

    A balance-sheet row is paired with the schedule items its role's population
    plane binds, an income-statement row with the rows it drives
    (``_OPTIONAL_INCOME_STATEMENT_ROW_SCHEDULES``).
    """

    catalog = load_required_role_catalog()
    return (
        *(
            (
                item_id,
                catalog.population_item_ids(row.required_role, ticker=ticker),
            )
            for row in catalog.roles_on_statement("balance_sheet")
            if row.optional_if_unreported
            for item_id in row.template_items
            if item_id.startswith(_FACE_BALANCE_SHEET_PREFIX)
        ),
        *(
            (item_id, _OPTIONAL_INCOME_STATEMENT_ROW_SCHEDULES.get(item_id, ()))
            for row in catalog.roles_on_statement("income_statement")
            if row.optional_if_unreported
            for item_id in row.template_items
            if item_id.startswith(_FACE_INCOME_STATEMENT_PREFIX)
        ),
    )



def _restore_node_readers(
    model: FinancialModel, node_id: str, historical: tuple[int, ...],
) -> None:
    """Give each historical reader of a period-average node its years back.

    History bound y/y rates before the period-average base existed, so those
    years were dropped. Restore each non-constant historical formula that reads
    the node, for years the node and its prior year can both stand as operands.
    Every pass that widens the node's supported years (the bridge formula, the
    pinned opening balance that opens its first year) restores the readers for
    them; the first historical year stays out unless its own prior year is
    supported.
    """

    supported = _supported_item_periods(model.get_item(node_id))
    for other in model._index.values():
        if other.id == node_id:
            continue
        spec = other.historical
        if spec is None or spec.type is FormulaType.constant:
            continue
        if not any(ref.ref.id == node_id for ref in spec.references()):
            continue
        years = [
            year
            for year in historical
            if year in supported and (year - 1) in supported
        ]
        if years:
            _set_periods(other, years)


def _supported_item_periods(item) -> set[int]:
    """Years an item can stand as a historical operand after its formula lands."""

    supported: set[int] = set()
    if item.formula_periods is not None:
        supported.update(int(period) for period in item.formula_periods)
    supported.update(int(period) for period in (item.overrides or {}))
    if item.values is not None:
        supported.update(
            int(cell.period)
            for cell in item.values.values.values()
            if cell.value is not None
        )
    return supported



def _apply_balance_sheet_policy(
    model: FinancialModel,
    values: ComputedValues,
    revision: BusinessModelRevision,
    compiled_registry: CompiledDriverRegistry,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    *,
    declared_rows: Mapping[str, str],
    diagnostics: list[BuildDiagnostic],
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
) -> list[str]:
    wired: list[str] = []
    for statement_id, schedule_id in (
        (
            _CURRENT_DEFERRED_CONTRACT_COSTS,
            _CURRENT_DEFERRED_CONTRACT_COSTS_SCHEDULE,
        ),
        (
            _DEFERRED_CONTRACT_COSTS,
            _LONG_TERM_DEFERRED_CONTRACT_COSTS_SCHEDULE,
        ),
    ):
        if {statement_id, schedule_id} <= set(model._index):
            has_known = _has_known_historical_value(values, item_id=schedule_id, periods=historical) or _has_known_historical_value(values, item_id=statement_id, periods=historical)
            if has_known:
                statement = model.get_item(statement_id)
                statement.historical = FormulaSpec(
                    type=FormulaType.ref,
                    subtype="reported_deferred_contract_cost_balance",
                    params={"source": LineItemRef(id=schedule_id)},
                    note="filing-native deferred contract cost statement bridge",
                )
                if statement.overrides and all(
                    getattr(spec, "type", None) == FormulaType.constant
                    and spec.params.get("value") == 0.0
                    for spec in statement.overrides.values()
                ):
                    statement.overrides.clear()
                    statement.values = None
                _set_periods(statement, historical)
    # A node declaring ``balance_of`` is that balance-sheet balance, observed
    # at its observation basis; the declaration, not the node's factors, unit
    # or driver form, names it. The reported year-end moves with the node,
    # end[t] = end[t-1] * node[t] / node[t-1], reading only earlier or
    # same-period cells so the graph and the workbook agree. A period-end node
    # observes the year-end itself; a period-average node's history is by
    # definition the mean of the two reported year-ends,
    # avg[t] = (end[t-1] + end[t]) / 2 (the identity the HFF opening seed uses).
    # The liability the balance is held against (the model kind's declared
    # row for the balance's catalog counterpart) moves by the same ratio, so
    # its change in financing offsets the balance's change in investing.
    catalog = load_required_role_catalog()
    declared = _declared_balance_nodes(revision, compiled_registry)
    node_driven = node_driven_balance_roles()
    for role in sorted(balance_roles()):
        declared_balance = catalog.balance(role)
        balance_id = declared_balance.target_item
        if role not in declared:
            if role not in node_driven or balance_id is None or balance_id not in model._index:
                # A balance the template schedules own has its own driver;
                # only a node-driven balance reported without a declaring
                # node is left with none.
                continue
            if _all_historical_values_are_zero(
                values, item_id=balance_id, periods=historical
            ):
                continue
            if (
                _has_known_historical_value(
                    values, item_id=balance_id, periods=historical
                )
                and _has_no_driver(model.get_item(balance_id))
            ):
                # The balance arm of the admission the business-model door
                # runs before acceptance (`admit_business_model`).
                from schema.build_bm_node_history import bm_balance_refusals

                diagnostics.extend(
                    BuildDiagnostic(
                        code="compile_failure",
                        message=f"{balance_id}: {refusal.message}",
                        semantic_item_id=balance_id,
                        period_id=f"FY{min(projected)}",
                    )
                    for refusal in bm_balance_refusals(
                        revision.business_model, roles_with_history=(role,)
                    )
                )
            continue
        node, node_id = declared[role]
        if balance_id is None or balance_id not in model._index:
            diagnostics.append(
                BuildDiagnostic(
                    code="compile_failure",
                    message=(
                        f"{role}: the business model declares this balance but the "
                        "model has no balance-sheet row to move with it"
                    ),
                    semantic_item_id=node_id,
                    period_id=f"FY{min(projected)}",
                )
            )
            continue
        node_item = model.get_item(node_id)
        if node.observation_basis == "period_average":
            node_item.historical = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="period_average_from_reported_period_ends",
                params={
                    "expr": {
                        "op": "*",
                        "args": [
                            {
                                "op": "+",
                                "args": [
                                    LineItemRef(id=balance_id, t=-1),
                                    LineItemRef(id=balance_id),
                                ],
                            },
                            0.5,
                        ],
                    }
                },
                note="period-average balance as the mean of the adjacent reported period-end balances",
                derivation=_derivation(
                    "declared_balance_roll",
                    (balance_id,),
                ),
            )
            # The mean of two period-end balances stands only in a year both ends do; a year
            # whose ends are not reported is no formula period, on every pass alike.
            ends = _supported_item_periods(model.get_item(balance_id))
            node_item.formula_periods = sorted(
                set(
                    node_item.formula_periods
                    if node_item.formula_periods is not None
                    else projected
                )
                | {period for period in historical[1:] if period in ends and period - 1 in ends}
            )
            wired.append(node_item.id)
            _restore_node_readers(model, node_id, historical)
        balance = model.get_item(balance_id)
        balance.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="period_end_from_declared_balance_node",
            params={
                "expr": {
                    "op": "/",
                    "left": {
                        "op": "*",
                        "args": [
                            LineItemRef(id=balance_id, t=-1),
                            LineItemRef(id=node_id),
                        ],
                    },
                    "right": LineItemRef(id=node_id, t=-1),
                }
            },
            note=f"reported period-end balance moved with its declared {node.observation_basis} node",
            derivation=_derivation(
                "declared_balance_roll",
                (balance_id, node_id),
            ),
        )
        _set_periods(balance, projected)
        wired.append(balance.id)
        obligation_id = (
            declared_rows.get(catalog.balance(declared_balance.counterpart).leaf)
            if declared_balance.counterpart is not None
            else None
        )
        if obligation_id is not None:
            obligation = model.get_item(obligation_id)
            obligation.projected = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="obligation_held_against_declared_balance",
                params={
                    "expr": {
                        "op": "/",
                        "left": {
                            "op": "*",
                            "args": [
                                LineItemRef(id=obligation_id, t=-1),
                                LineItemRef(id=balance_id),
                            ],
                        },
                        "right": LineItemRef(id=balance_id, t=-1),
                    }
                },
                note="obligation held against the declared balance moves with it",
                derivation=_derivation(
                    "declared_balance_roll",
                    (obligation_id, balance_id),
                ),
            )
            _set_periods(obligation, projected)
            wired.append(obligation.id)

    # Optional catalog balances keep their source state and existing forecast.
    # Reported schedule-backed balances span the full projection horizon;
    # role-less face aliases leave only when their schedule holds no history.
    ticker = str(model.company.ticker)
    for item_id, schedule_ids in (
        *((item_id, ()) for item_id in _ROLELESS_FACE_BALANCE_PLACEHOLDERS),
        *_optional_face_slots(ticker),
    ):
        if item_id not in model._index:
            continue
        cf_id = item_id.replace(".balance_sheet.", ".cash_flow.")
        if _has_known_historical_value(
            values,
            item_id=item_id,
            periods=historical,
        ):
            if not item_id.startswith(_FACE_INCOME_STATEMENT_PREFIX):
                for reported_id in (cf_id, *schedule_ids):
                    reported = model._index.get(reported_id)
                    if reported is not None and reported.projected is not None:
                        _set_periods(reported, projected)
            continue
        if _forecast_reads_authored_control(
            model, model.get_item(item_id), projected, nonzero_only=True,
        ):
            continue
        if model.get_item(item_id).required_role is None:
            _remove_face_row(model, item_id)
        # A role-bearing slot with no reported history is not a zero observation:
        # the template's held balance and any authored forecast remain
        # authoritative, and an unowed role leaves through the graph owner.

    _NON_ACCOUNT_BALANCE_SHEET_ITEMS = {
        *load_required_role_catalog().statement_totals(),
        # Presentation headers and ratios have no accounting role or parent total.
        "tpl.fm.balance_sheet.balance_sheet_m",
        "tpl.fm.balance_sheet.assets",
        "tpl.fm.balance_sheet.current_assets",
        "tpl.fm.balance_sheet.noncurrent_assets",
        "tpl.fm.balance_sheet.liabilities",
        "tpl.fm.balance_sheet.current_liabilities",
        "tpl.fm.balance_sheet.noncurrent_liabilities",
        "tpl.fm.balance_sheet.equity",
        "tpl.fm.balance_sheet.balance",
        "tpl.fm.balance_sheet.net_cash",
        "tpl.fm.balance_sheet.net_cash_per_share",
        "tpl.fm.balance_sheet.net_cash_per_share_growth",
        "tpl.fm.balance_sheet.book_value_per_share",
        "tpl.fm.balance_sheet.free_cash_flow_per_share",
        "tpl.fm.balance_sheet.debt_ebitda_ratio",
        "tpl.fm.balance_sheet.return_on_equity_roe",
        "tpl.fm.balance_sheet.return_on_invested_capital_roic",
        "tpl.fm.balance_sheet.incremental_operating_margins",
        "tpl.fm.balance_sheet.incremental_ebitda_margins",
        "tpl.fm.balance_sheet.change",
        "tpl.fm.balance_sheet.y_y_change",
        "tpl.fm.balance_sheet.cash_and_marketable_securities",
    }
    _DECLARED_BALANCE_ROW_IDS = {
        *(
            catalog.balance(role).target_item
            for role in (*node_driven, *declared)
            if catalog.balance(role).target_item is not None
        ),
        *(
            declared_rows[catalog.balance(counterpart).leaf]
            for role in (*node_driven, *declared)
            if (counterpart := catalog.balance(role).counterpart) is not None
            and catalog.balance(counterpart).leaf in declared_rows
        ),
    }
    bs_sheet = model.sheets.get("Financial_model")
    if bs_sheet is not None:
        for section in bs_sheet.sections:
            if section.id != "balance_sheet":
                continue
            for item in section.line_items:
                if item.id in _NON_ACCOUNT_BALANCE_SHEET_ITEMS or item.id in _DECLARED_BALANCE_ROW_IDS:
                    continue
                if _all_historical_values_are_zero(values, item_id=item.id, periods=historical):
                    for target_id in (item.id, item.id.replace(".balance_sheet.", ".cash_flow.")):
                        target = model._index.get(target_id)
                        if target is not None and _has_no_driver(target):
                            target.projected = _zero_projection_formula(
                                note="complete reported history is zero for balance-sheet account; zero carry-forward",
                                strategy="historical_zero_balance_policy",
                                dependencies=(item.id,),
                            )
                            _set_periods(target, projected)
                            wired.append(target.id)

        # When a statement total line item (e.g. Total Liabilities) has no direct reported
        # values, clear any stale zero constant overrides so its arithmetic formula computes.
        for total_item_id in (_TOTAL_LIABILITIES, _TOTAL_LIABILITIES_AND_EQUITY):
            tot_item = model.get_item(total_item_id)
            if tot_item is not None and (tot_item.values is None or not tot_item.values.values):
                if tot_item.overrides and all(ov.params.get("value") == 0.0 for ov in tot_item.overrides.values() if ov.params):
                    tot_item.overrides = None

    # NSP-INTANGIBLE-AMORTIZATION: a sub-schedule flow runs off its own balance.
    # The catalog declares which flow rows run off which balance
    # (``nsp_balance_runoff_rows``). A balance cited not separately presented
    # lives inside a host row, so its declared flow projects 0: its economics
    # stay in total D&A through the host, and a flow with no base is never
    # forecast. The derivation names the cited balance, so the publisher's
    # lineage reads that absence onto the zero cell. Nothing else is written on
    # a cited balance: its cell stays cited and its readers drop it.
    nsp_lines = _cited_terminal_lines(
        model, role_dispositions, role_items, "not_separately_presented"
    )
    for balance_id, flow_ids in load_required_role_catalog().nsp_balance_runoff_rows.items():
        if balance_id not in nsp_lines or _forecast_reads_authored_control(
            model, model.get_item(balance_id), projected
        ):
            continue
        for flow_id in flow_ids:
            if flow_id not in model._index:
                continue
            authored_flow = frozenset(_authored_periods(model.get_item(flow_id), projected))
            installed_flow = _install_zero_projection(
                model,
                item_id=flow_id,
                projected=tuple(period for period in projected if period not in authored_flow),
                note=(
                    "not separately presented; amortization carried in "
                    f"{_TOTAL_DEPRECIATION_AND_AMORTIZATION}"
                ),
                strategy="historical_zero_balance_policy",
                dependencies=(balance_id,),
            )
            if installed_flow is not None:
                wired.append(installed_flow)

    wired.extend(
        _install_nsp_dependent_rate_policy(
            model,
            values=values,
            historical=historical,
            projected=projected,
            role_dispositions=role_dispositions,
            role_items=role_items,
        )
    )

    _apply_forecast_seam_continuity(model, historical)

    # Purchases and sales/maturities of investments: if complete reported history
    # is zero, install zero projection so missing forecasts do not cascade into cash.
    for item_id in (
        "tpl.fm.cash_flow.purchases_of_investments",
        "tpl.fm.cash_flow.sales_maturities_of_investments",
    ):
        if item_id in model._index and _all_historical_values_are_zero(
            values, item_id=item_id, periods=historical
        ):
            installed = _install_zero_projection(
                model,
                item_id=item_id,
                projected=projected,
                note="complete reported history is zero for investments flow; zero cash flow",
                strategy="historical_zero_balance_policy",
                dependencies=(item_id,),
            )
            if installed is not None:
                wired.append(installed)

    if _business_model_has_no_acquisition_program(revision):
        for item_id in (_GOODWILL_ADDITIONS, _INTANGIBLE_ADDITIONS):
            installed = _install_zero_projection(
                model,
                item_id=item_id,
                projected=projected,
                note="accepted BusinessModel has no modeled acquisition program",
                strategy="business_model_no_acquisition_policy",
            )
            if installed is not None:
                wired.append(installed)

    # The canonical debt schedule already owns current and long-term debt.  A
    # source-empty commercial-paper placeholder must not become a second debt lane.
    if (
        _COMMERCIAL_PAPER in model._index
        and _CURRENT_DEBT in model._index
        and _COMMERCIAL_PAPER
        not in _cited_terminal_lines(
            model,
            role_dispositions,
            role_items,
            "not_applicable",
            "not_separately_presented",
            "unowed",
        )
        and not _has_known_historical_value(
            values,
            item_id=_COMMERCIAL_PAPER,
            periods=historical,
        )
    ):
        installed = _install_zero_projection(
            model,
            item_id=_COMMERCIAL_PAPER,
            projected=projected,
            note="source-empty commercial-paper placeholder excluded by canonical debt schedule",
            strategy="canonical_debt_lane_exclusion",
        )
        if installed is not None:
            wired.append(installed)

    # A zero-debt issuer whose terminal long-term debt balance evidences zero debt
    # and whose filings report no current portion owes no current portion: the debt
    # schedule's current-debt row projects zero cited to the evidenced zero debt line.
    terminal_period = max(historical)
    terminal_debt_rows = _terminal_rows(role_dispositions)
    short_debt_terminal = terminal_debt_rows.get(_SHORT_TERM_DEBT_ROLE)
    short_debt_lines = _role_lines(model, role_items, _SHORT_TERM_DEBT_ROLE)
    long_debt_zero_lines = tuple(
        item_id
        for item_id in _role_lines(model, role_items, _LONG_TERM_DEBT_ROLE)
        if _is_zero_value(values.get(item_id, {}).get(terminal_period))
    )
    if (
        _CURRENT_DEBT in model._index
        and short_debt_lines
        and all(
            values.get(item_id, {}).get(terminal_period) is None
            for item_id in short_debt_lines
        )
        and values.get(_CURRENT_DEBT, {}).get(terminal_period) is None
        and not _forecast_reads_authored_control(
            model, model.get_item(_CURRENT_DEBT), projected, nonzero_only=True
        )
        and short_debt_terminal is not None
        and (
            short_debt_terminal.state in ("not_applicable", "unowed")
            or short_debt_terminal.not_disclosed_in_vintage
        )
        and long_debt_zero_lines
    ):
        cited_short_debt = (
            short_debt_lines if short_debt_terminal.state == "not_applicable" else ()
        )
        installed = _install_zero_projection(
            model,
            item_id=_CURRENT_DEBT,
            projected=projected,
            note="zero-debt issuer has no current portion of long-term debt",
            strategy="historical_zero_balance_policy",
            dependencies=(*cited_short_debt, *long_debt_zero_lines),
        )
        if installed is not None:
            wired.append(installed)

    # Some normalized vendor balance-sheet fields are inclusive aggregates even
    # though the canonical model presents their components on separate rows.
    # Activate the bridge only when the exact terminal statement identity proves
    # that the overstatement equals those separately modeled components.  This is
    # a source-shape normalization, not a forecast plug: future periods continue
    # to use the ordinary PPE and intangible roll-forwards.
    overlap_source_rows = {
        _TOTAL_ASSETS,
        _TOTAL_CURRENT_ASSETS,
        _SOFTWARE_BALANCE,
        _PPE_BALANCE,
        _ROU_BALANCE,
        _INTANGIBLE_BALANCE,
        _GOODWILL,
        _DEFERRED_CONTRACT_COSTS,
        _OTHER_NONCURRENT_ASSETS,
        _DEFERRED_TAX_ASSETS,
    }
    overlap_schedule_rows = {
        _PPE_SCHEDULE,
        _PPE_ADDITIONS,
        _PPE_LESS_DEPRECIATION,
        _INTANGIBLE_SCHEDULE,
        _INTANGIBLE_ADDITIONS,
        _INTANGIBLE_AMORTIZATION,
    }
    if (overlap_source_rows | overlap_schedule_rows) <= set(model._index):
        terminal = max(historical)
        terminal_values = {
            item_id: values.get(item_id, {}).get(terminal)
            for item_id in overlap_source_rows
        }
        if _values_are_present(terminal_values):
            component_total = sum(
                float(terminal_values[item_id])
                for item_id in (
                    _TOTAL_CURRENT_ASSETS,
                    _SOFTWARE_BALANCE,
                    _PPE_BALANCE,
                    _ROU_BALANCE,
                    _INTANGIBLE_BALANCE,
                    _GOODWILL,
                    _DEFERRED_CONTRACT_COSTS,
                    _OTHER_NONCURRENT_ASSETS,
                    _DEFERRED_TAX_ASSETS,
                )
            )
            reported_total = float(terminal_values[_TOTAL_ASSETS])
            proven_overlap = sum(
                float(terminal_values[item_id])
                for item_id in (
                    _SOFTWARE_BALANCE,
                    _ROU_BALANCE,
                    _CONTRACT_COST_NONCURRENT,
                )
            )
            ppe_residual = (
                float(terminal_values[_PPE_BALANCE])
                - float(terminal_values[_SOFTWARE_BALANCE])
                - float(terminal_values[_ROU_BALANCE])
            )
            intangible_residual = (
                float(terminal_values[_INTANGIBLE_BALANCE])
                - float(terminal_values[_CONTRACT_COST_NONCURRENT])
            )
            if (
                ppe_residual >= 0
                and intangible_residual >= 0
                and math.isclose(
                    component_total - reported_total,
                    proven_overlap,
                    rel_tol=1e-9,
                    abs_tol=1e-6,
                )
            ):
                first_projection = min(projected)
                ppe = model.get_item(_PPE_SCHEDULE)
                if ppe.overrides is None:
                    ppe.overrides = {}
                ppe.overrides[first_projection] = FormulaSpec(
                    type=FormulaType.arithmetic,
                    subtype="composite_ppe_opening_normalization",
                    params={
                        "expr": {
                            "op": "+",
                            "args": [
                                LineItemRef(id=_PPE_BALANCE, t=-1),
                                {"op": "NEG", "arg": LineItemRef(id=_SOFTWARE_BALANCE, t=-1)},
                                {"op": "NEG", "arg": LineItemRef(id=_ROU_BALANCE, t=-1)},
                                LineItemRef(id=_PPE_ADDITIONS),
                                {"op": "NEG", "arg": LineItemRef(id=_PPE_LESS_DEPRECIATION)},
                            ],
                        }
                    },
                    note=(
                        "normalize a proven inclusive reported PPE opening by "
                        "removing separately modeled software and lease assets"
                    ),
                    derivation=_derivation(
                        "composite_ppe_opening_normalization",
                        (
                            _PPE_BALANCE,
                            _SOFTWARE_BALANCE,
                            _ROU_BALANCE,
                            _PPE_ADDITIONS,
                            _PPE_LESS_DEPRECIATION,
                        ),
                    ),
                )
                intangible = model.get_item(_INTANGIBLE_SCHEDULE)
                if intangible.overrides is None:
                    intangible.overrides = {}
                intangible.overrides[first_projection] = FormulaSpec(
                    type=FormulaType.arithmetic,
                    subtype="composite_intangible_opening_normalization",
                    params={
                        "expr": {
                            "op": "+",
                            "args": [
                                LineItemRef(id=_INTANGIBLE_BALANCE, t=-1),
                                {
                                    "op": "NEG",
                                    "arg": LineItemRef(
                                        id=_CONTRACT_COST_NONCURRENT,
                                        t=-1,
                                    ),
                                },
                                LineItemRef(id=_INTANGIBLE_ADDITIONS),
                                {
                                    "op": "NEG",
                                    "arg": LineItemRef(id=_INTANGIBLE_AMORTIZATION),
                                },
                            ],
                        }
                    },
                    note=(
                        "normalize a proven inclusive reported intangible opening by "
                        "removing separately modeled deferred contract costs"
                    ),
                    derivation=_derivation(
                        "composite_intangible_opening_normalization",
                        (
                            _INTANGIBLE_BALANCE,
                            _CONTRACT_COST_NONCURRENT,
                            _INTANGIBLE_ADDITIONS,
                            _INTANGIBLE_AMORTIZATION,
                        ),
                    ),
                )
                wired.extend((ppe.id, intangible.id))

    # The depreciation policy owns both the ungrounded residual and its
    # company-grounded replacement below.
    if {_PPE_LESS_DEPRECIATION, _TOTAL_DEPRECIATION_AND_AMORTIZATION} <= set(model._index):
        less_depreciation = model.get_item(_PPE_LESS_DEPRECIATION)
        amortization_ids = [
            item_id
            for item_id in (
                _INTANGIBLE_AMORTIZATION,
                "tpl.a.capital_investments.less_amortization",
            )
            if item_id in model._index
        ]
        less_depreciation.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="physical_depreciation_residual",
            params={
                "expr": {
                    "op": "+",
                    "args": [
                        LineItemRef(id=_TOTAL_DEPRECIATION_AND_AMORTIZATION),
                        *(
                            {"op": "NEG", "arg": LineItemRef(id=item_id)}
                            for item_id in amortization_ids
                        ),
                    ],
                }
            },
            note="canonical_physical_depreciation_bridge",
        )
        _set_periods(less_depreciation, projected)
        wired.append(less_depreciation.id)

    depreciation_rows = {
        _PHYSICAL_DEPRECIATION,
        _TOTAL_DEPRECIATION_AND_AMORTIZATION,
        _SOFTWARE_AMORTIZATION,
        _ACQUIRED_INTANGIBLE_EXPENSE,
        _PPE_LESS_DEPRECIATION,
    }
    if depreciation_rows <= set(model._index):
        software_included = _software_inside_ppe_periods(model, historical)
        # An unowed software amortization is no reported amount: whatever its cells hold
        # is what retirement or an absence lane left, never a filing's number, so the
        # basis reads none of it (a cold pass and a re-run then choose alike).
        unowed_amortization = {
            int(row.period_id.removeprefix("FY"))
            for row in role_dispositions.by_role(_SOFTWARE_AMORTIZATION_ROLE)
            if row.state == "unowed"
        }
        basis_values: ComputedValues = {
            **values,
            _SOFTWARE_AMORTIZATION: {
                period: amount
                for period, amount in values.get(_SOFTWARE_AMORTIZATION, {}).items()
                if period not in unowed_amortization
            },
        }
        house = house_revenue_ratios(
            basis_values, historical, software_included_periods=software_included,
        ).get(_PHYSICAL_DEPRECIATION_RATE)
        # A presented software balance whose amortization is included in total
        # D&A rolls forward on its own numeric amortization, which that total
        # contains and its history cannot isolate: physical depreciation then
        # has no house residual, only its authored revenue ratio.
        unisolated = (
            house is None
            and not _software_inside_ppe_at_terminal(model)
            and _software_inclusion_fact(model, _SOFTWARE_AMORTIZATION, historical[-1])
            is not None
        )
        if house is None and not unisolated:
            return wired
        if house is None:
            wired.extend(
                _install_default_revenue_ratio(
                    model,
                    target_id=_PHYSICAL_DEPRECIATION,
                    rate_id=_PHYSICAL_DEPRECIATION_RATE,
                    anchor_id=_PHYSICAL_DEPRECIATION,
                    label="  Depreciation as % of revenue",
                    ratio=None,
                    projected=projected,
                    note=(
                        "authored physical depreciation revenue ratio: software "
                        "amortization is included in reported D&A"
                    ),
                    strategy="historical_physical_depreciation_residual_ratio",
                )
            )
        else:
            basis_period = house.basis_period
            physical_depreciation_ratio = house.value
            physical_note = (
                "latest reported total D&A, inclusive of the capitalized-software "
                "amortization the filer presents inside it, less acquired-intangible "
                "amortization, scaled with projected revenue"
                if basis_period in software_included
                else "latest reported total D&A less separately reported software and "
                "acquired-intangible amortization, scaled with projected revenue"
            )
            # Each forecast year's rate keeps the reported-year operands it was
            # computed from beside it.
            component_ids = [_ACQUIRED_INTANGIBLE_EXPENSE]
            if basis_period not in software_included:
                component_ids.append(_SOFTWARE_AMORTIZATION)
            lineage_by_period = {
                period: {
                    "value": physical_depreciation_ratio,
                    "expr": {
                        "op": "/",
                        "left": {
                            "op": "+",
                            "args": [
                                LineItemRef(
                                    id=_TOTAL_DEPRECIATION_AND_AMORTIZATION,
                                    t=basis_period - period,
                                ),
                                *(
                                    {"op": "NEG", "arg": LineItemRef(
                                        id=item_id, t=basis_period - period,
                                    )}
                                    for item_id in component_ids
                                ),
                            ],
                        },
                        "right": LineItemRef(id=_REVENUE, t=basis_period - period),
                    },
                }
                for period in projected
            }
            wired.extend(
                _install_default_revenue_ratio(
                    model,
                    target_id=_PHYSICAL_DEPRECIATION,
                    rate_id=_PHYSICAL_DEPRECIATION_RATE,
                    anchor_id=_PHYSICAL_DEPRECIATION,
                    label="  Depreciation as % of revenue",
                    ratio=physical_depreciation_ratio,
                    projected=projected,
                    note=physical_note,
                    strategy=house.strategy,
                    lineage_by_period=lineage_by_period,
                )
            )
        less_depreciation = model.get_item(_PPE_LESS_DEPRECIATION)
        less_depreciation.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="physical_depreciation_bridge",
            params={"source": LineItemRef(id=_PHYSICAL_DEPRECIATION)},
            note="physical-depreciation-only bridge to PPE roll-forward",
            derivation=_derivation(
                "physical_depreciation_bridge",
                (_PHYSICAL_DEPRECIATION,),
            ),
        )
        _set_periods(less_depreciation, projected)
        wired.append(less_depreciation.id)
    return wired


def _deferred_tax_liability_forecast_kept_periods(
    model: FinancialModel,
    liability_row: LineItem,
    protected: Collection[tuple[str, int]],
) -> tuple[int, ...]:
    """The forecast periods whose liability cell is an authored dollar the total must sum.

    ``_remove_formula_ref`` drops the liability from unauthored forecast periods of the
    total. A protected cell, or a period the analyst lowered as ``input``, is a
    known dollar, not the cited inclusion, so the total keeps the operand in that period.
    """

    projection = tuple(
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or ()
        )
    )
    authored = frozenset(_authored_periods(liability_row, projection))
    return tuple(
        period
        for period in projection
        if _protected_cell(protected, liability_row.id, period) or period in authored
    )


def _rewire_deferred_tax_cash_flow_for_asset_fact(
    model: FinancialModel,
    *,
    protected_cells: Collection[tuple[str, int]] = (),
) -> list[str]:
    """Re-read the filed deferred-tax inclusions on the template's forecast lanes.

    The tax-split owner supplies each row's cited inclusion and the cash terms
    that remain. A balance included in another line, in the net liability or in
    an other-noncurrent line, projects as that cited fact: it is no standalone
    dollar, so its change leaves the noncash bridge (the including line holds
    it) and the included liability leaves total liabilities. A forecast period
    the analyst authored, or a protected cell, keeps that operand: the total
    still sums the known liability. Historical amounts and analyst-authored
    projected formulas are unchanged. A protected line's projected formula is
    untouched, and a protected period keeps its override.
    """

    if _DEFERRED_TAX_ASSETS not in model._index:
        return []
    cash_terms = _deferred_tax_cash_flow_terms(model)
    metadata_key = "_deferred_tax_asset_inclusion_projection"
    wired: list[str] = []
    protected = frozenset(
        (str(item_id), int(period)) for item_id, period in protected_cells
    )

    for row_id, schedule_id in (
        (_DEFERRED_TAX_ASSETS, "tpl.a.other_assets.deferred_income_tax_assets"),
        (_DEFERRED_TAX_LIABILITY_ITEM_ID, "tpl.a.capital_sources.deferred_income_tax_liabilities"),
    ):
        row = model._index.get(row_id)
        if row is None or _protected_line(model, protected, row_id):
            continue
        original_row = row.projected
        if original_row is not None and metadata_key in original_row.params:
            original_row = FormulaSpec.model_validate_json(original_row.params[metadata_key])
        if (
            original_row is not None
            and original_row.type is FormulaType.ref
            and original_row.params.get("source") == LineItemRef(id=schedule_id)
        ):
            fact = _deferred_tax_inclusion_fact(model, row_id)
            if fact is not None:
                row.projected = fact.model_copy(
                    update={"params": {
                        **fact.params,
                        metadata_key: original_row.model_dump_json(),
                    }},
                    deep=True,
                )
            else:
                row.projected = original_row
            wired.append(row.id)

    liabilities = model._index.get(_TOTAL_LIABILITIES)
    if (
        liabilities is not None
        and liabilities.projected is not None
        and not _protected_line(model, protected, liabilities.id)
    ):
        original_total = liabilities.projected
        if metadata_key in original_total.params:
            original_total = FormulaSpec.model_validate_json(original_total.params[metadata_key])
        liability_row = model._index.get(_DEFERRED_TAX_LIABILITY_ITEM_ID)
        projection = tuple(
            int(period)
            for period in (
                model.time_structure.projection_periods
                or model.time_structure.projection_years
                or ()
            )
        )
        if liabilities.overrides is not None:
            for period in projection:
                override = liabilities.overrides.get(period)
                if override is not None and metadata_key in override.params:
                    saved_override = override.params[metadata_key]
                    if isinstance(saved_override, str) and saved_override:
                        liabilities.overrides[period] = FormulaSpec.model_validate_json(
                            saved_override
                        )
                    else:
                        liabilities.overrides.pop(period, None)
        projected_total = original_total.model_copy(deep=True)
        kept_periods = (
            _deferred_tax_liability_forecast_kept_periods(
                model, liability_row, protected,
            )
            if liability_row is not None
            else ()
        )
        has_inclusion = (
            liability_row is not None
            and liability_row.projected is not None
            and metadata_key in liability_row.projected.params
            and _deferred_tax_inclusion_fact(model, _DEFERRED_TAX_LIABILITY_ITEM_ID) is not None
        )
        if has_inclusion and len(kept_periods) < len(projection):
            _remove_formula_ref(projected_total.params, _DEFERRED_TAX_LIABILITY_ITEM_ID)
            if kept_periods:
                if liabilities.overrides is None:
                    liabilities.overrides = {}
                for period in kept_periods:
                    if _protected_cell(protected, liabilities.id, period):
                        continue
                    prior = liabilities.overrides.get(period)
                    with_dtl = original_total.model_copy(
                        update={"params": {
                            **original_total.params,
                            metadata_key: "" if prior is None else prior.model_dump_json(),
                        }},
                        deep=True,
                    )
                    liabilities.overrides[period] = with_dtl
        if projected_total.params != original_total.params:
            projected_total.params[metadata_key] = original_total.model_dump_json()
            liabilities.projected = projected_total
            wired.append(liabilities.id)
        else:
            liabilities.projected = original_total

    noncash = model._index.get(_OTHER_NON_CASH_ADJUSTMENTS)
    if noncash is not None and noncash.projected is not None:
        installed = noncash.projected
        original = installed
        if metadata_key in original.params:
            original = FormulaSpec.model_validate_json(original.params[metadata_key])
        expr = original.params.get("expr", {})
        args = expr.get("args", []) if isinstance(expr, dict) else []
        gross = [_liability_change(), _asset_change()]
        if (
            not _protected_line(model, protected, noncash.id)
            and original.note is None
            and isinstance(expr, dict)
            and expr.get("op") == "+"
            and all(term in args for term in gross)
        ):
            remaining = [term for term in args if term not in gross]
            if metadata_key in installed.params:
                # The snapshot restores the tax lanes only. A lane another owner
                # retired from the installed expression since the snapshot (the
                # other-working-capital identity takes over its leaf lines) stays
                # retired; restoring it would count that movement twice.
                held = installed.params["expr"].get("args", [])
                remaining = [term for term in remaining if term in held]
            noncash.projected = original.model_copy(
                update={"params": {
                    **original.params,
                    "expr": {"op": "+", "args": [*cash_terms, *remaining]},
                    metadata_key: original.model_dump_json(),
                }},
                deep=True,
            )
            wired.append(noncash.id)
        # The template's first forecast column is a SUM override, not the
        # projected expression above. Replace its two tax cash-flow lanes with
        # the balance-defined terms while retaining the other lanes.
        tax_items = [
            LineItemRef(id="tpl.fm.cash_flow.deferred_income_tax_assets"),
            LineItemRef(id=_FM_DEFERRED_TAX_CASH_FLOW),
        ]
        overrides = noncash.overrides or {}
        for period in model.time_structure.projection_periods:
            override = overrides.get(period)
            if override is None or _protected_cell(protected, noncash.id, int(period)):
                continue
            if metadata_key in override.params:
                override = FormulaSpec.model_validate_json(override.params[metadata_key])
            items = override.params.get("items", [])
            if (
                override.note is None
                and override.type is FormulaType.arithmetic
                and override.params.get("function") == "SUM"
                and all(ref in items for ref in tax_items)
            ):
                overrides[period] = FormulaSpec(
                    type=FormulaType.arithmetic,
                    subtype="add_subtract",
                    params={
                        "expr": {"op": "+", "args": [
                            *cash_terms, *(ref for ref in items if ref not in tax_items),
                        ]},
                        metadata_key: override.model_dump_json(),
                    },
                )

    _refresh_deferred_tax_formulas(model)
    return wired


_COUNTERPART_ROLL_FORWARD_NOTE = "balance_rolled_forward_by_counterpart_cash_flow"



def _carry_flow_on_cash_bridge(
    model: FinancialModel, flow_id: str, projection: set[int]
) -> bool:
    """Append the flow to the cash bridge's projected roll-forward additions.

    The flow keeps its signed cash-flow convention (purchases negative, sales
    positive). Projected-period overrides of the bridge that are themselves
    roll-forwards take the flow too; any other authored override is judgment
    and is left as written. Returns whether a term was added.
    """

    bridge = model._index.get(_CASH_BRIDGE_ITEM_ID)
    if bridge is None:
        return False
    specs = [bridge.projected] + [
        spec for period, spec in (bridge.overrides or {}).items() if int(period) in projection
    ]
    added = False
    for spec in specs:
        if spec is None or spec.type is not FormulaType.roll_forward:
            continue
        additions = spec.params.get("additions")
        if not isinstance(additions, list):
            continue
        if any(isinstance(a, LineItemRef) and a.id == flow_id and a.t == 0 for a in additions):
            continue
        additions.append(LineItemRef(id=flow_id))
        added = True
    return added


def _remove_flow_from_cash_bridge(
    model: FinancialModel, flow_id: str, projection: set[int]
) -> bool:
    """Take the flow off the cash bridge's roll-forward additions: its counterpart balance rolls it.

    The inverse of ``_carry_flow_on_cash_bridge``, over the same specs. Returns whether
    a term was removed.
    """

    bridge = model._index.get(_CASH_BRIDGE_ITEM_ID)
    if bridge is None:
        return False
    specs = [bridge.projected] + [
        spec for period, spec in (bridge.overrides or {}).items() if int(period) in projection
    ]
    removed = False
    for spec in specs:
        if spec is None or spec.type is not FormulaType.roll_forward:
            continue
        additions = spec.params.get("additions")
        if not isinstance(additions, list):
            continue
        kept = [a for a in additions if not (isinstance(a, LineItemRef) and a.id == flow_id and a.t == 0)]
        if len(kept) != len(additions):
            spec.params["additions"] = kept
            removed = True
    return removed


def _template_projected_spec(item_id: str) -> FormulaSpec | None:
    """The projected formula the generic template gives ``item_id``: what a build leaves on a
    balance no counterpart roll-forward reaches."""

    from schema.templates.template_builder import load_sia_generic_template

    template = load_sia_generic_template()
    template.build_index()
    spec = template.get_item(item_id).projected
    return None if spec is None else spec.model_copy(deep=True)


def _flow_projects_nonzero(flow: LineItem, projected: tuple[int, ...]) -> bool:
    """Whether a flow's forecast is an authored nonzero amount or a modeled nonzero formula.

    Reads the authored control cells, the projected-period overrides (a model-spec
    assumption is a constant override) and the row's projected lane; a template-zero
    default, a zero constant or a cited-absence marker (``=NA()``, an amount nobody
    forecast) moves nothing.
    """

    if _holds_authored_control(flow, projected, nonzero_only=True):
        return True

    def moves(spec: FormulaSpec | None) -> bool:
        return (
            spec is not None
            and spec.note != TEMPLATE_PROJECTED_ZERO_DEFAULT_NOTE
            and not (
                spec.type is FormulaType.constant and _is_zero_value(spec.params.get("value"))
            )
            and not (
                spec.type is FormulaType.raw
                and spec.subtype in ("not_applicable", "not_separately_presented")
            )
        )

    overrides = flow.overrides or {}
    return any(moves(overrides.get(int(period))) for period in projected) or moves(flow.projected)


def _route_flow_into_included_in(
    model: FinancialModel, *, term_id: str, included_in: RequiredRole,
) -> str | None:
    """Move the line a cited-included balance is included in by the flow it would have moved.

    The including line's own roll-forward gains ``NEG(term)``, the sign the
    counterpart would have carried (retained earnings falls by the repurchase
    cash); no separate roll runs for the counterpart. Returns the including line,
    or ``None`` when it has no arithmetic roll-forward to carry the term. A term
    the roll already subtracts is not added twice.
    """

    host_id = next((item for item in face_lines(included_in) if item in model._index), None)
    if host_id is None:
        return None
    host = model.get_item(host_id)
    spec = host.projected
    if spec is None or spec.type is not FormulaType.arithmetic or "expr" not in spec.params:
        return None
    if any(
        ref.ref.id == term_id and ref.ref.t == 0 and ref.role == "subtractive"
        for ref in spec.references()
    ):
        return host_id
    host.projected = spec.model_copy(
        update={
            "params": {
                **spec.params,
                "expr": {
                    "op": "+",
                    "args": [
                        spec.params["expr"],
                        {"op": "NEG", "arg": LineItemRef(id=term_id)},
                    ],
                },
            }
        },
        deep=True,
    )
    return host_id


def _roll_contra_equity_counterpart(
    model: FinancialModel,
    projected: tuple[int, ...],
    *,
    counterpart: RequiredRole,
    amount_id: str,
) -> list[str]:
    """Roll a contra-equity counterpart (treasury stock) forward by the repurchase amount.

    ``prev + NEG(amount)``, opening from the terminal historical balance. The
    roll reads only that terminal balance: an uncited blank there stays missing
    and the forecast propagates it with its binding gap (schema/README.md "an
    uncited blank stays missing"); a cited absence routes through
    ``_wire_flow_counterpart_balances`` before this runs and never reaches it.
    """

    schedule_id = next(
        (item for item in counterpart.population_items if item in model._index), None
    )
    if schedule_id is None:
        return []
    balance = model.get_item(schedule_id)
    balance.item_type = ItemType.derived
    balance.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="treasury_stock_roll_forward",
        params={
            "expr": {
                "op": "+",
                "args": [
                    LineItemRef(id=schedule_id, t=-1),
                    {"op": "NEG", "arg": LineItemRef(id=amount_id)},
                ],
            }
        },
        note="cumulative treasury-stock contra-equity from accepted repurchase cash",
        derivation=_derivation(
            "treasury_stock_repurchase_roll_forward", (schedule_id, amount_id)
        ),
    )
    _set_periods(balance, projected)
    return [balance.id]


def _wire_flow_counterpart_balances(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    *,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    diagnostics: list[BuildDiagnostic],
) -> list[str]:
    """Route every catalog flow to the one balance it lands on.

    This function is the single owner of flow destination: the spine never
    routes a flow. A flow's catalog row names its ``counterpart`` balance, and
    the counterpart's terminal historical state decides where the flow lands:

    * ``bound`` / ``formula`` / ``unbound``: the counterpart's roll-forward
      (a contra-equity counterpart rolls ``prev + NEG(amount)``; an
      investment balance rolls as below), a flow no balance can roll moving
      the cash bridge instead (``_carry_flow_on_cash_bridge``), never both.
    * ``not_separately_presented`` with ``included_in = P``: P's own
      roll-forward gains the flow term; the counterpart's schedule row is not
      wired and its face stays the cited marker.
    * ``unowed`` / ``not_applicable``: nothing for a zero or absent flow; an
      investment flow with a nonzero forecast moves the cash bridge (the
      securities sit inside the combined cash line); any other nonzero flow is
      a missing-source diagnostic naming the counterpart role, never an
      invented balance line.

    The catalog names, for a cash line no schedule backs (other investing
    activities), the balance-sheet role that absorbs its flow
    (``counterpart``). An authored flow with no balance behind it breaks the
    balance-sheet identity, so the counterpart rolls forward by the flow
    (``opening + schedule movement - flow``). The balance's working-capital
    movement then reads its schedule, the organic path, in every period, so
    the flow's cash moves once, on its own line. A flow a formula already
    carries (the client-fund bridge is the funds-held asset's own movement)
    has its counterpart and is left alone. Re-applying finds the balance
    already rolled and re-reads the working-capital movement from it.

    A counterpart with no reported value in any historical period has no
    opening to roll from, and none is invented: the balance keeps the
    template's projection, an investment flow carries on the cash bridge, any
    other nonzero flow is the missing-source diagnostic. Every run decides
    from the balance's present history: a balance that has history carries
    the roll-forward and the flow leaves the bridge, one that has none
    carries neither, whatever an earlier run installed.
    """

    catalog = load_required_role_catalog()
    wired: list[str] = []
    projection = {int(period) for period in projected}
    terminal = _terminal_rows(role_dispositions)

    def carry_on_bridge(flow_id: str) -> None:
        if flow_id in _CASH_BRIDGE_FLOW_ITEM_IDS and _carry_flow_on_cash_bridge(
            model, flow_id, projection
        ):
            wired.append(_CASH_BRIDGE_ITEM_ID)

    def flow_without_balance(flow_id: str, role: RequiredRole, state: str) -> None:
        if flow_id in _CASH_BRIDGE_FLOW_ITEM_IDS:
            carry_on_bridge(flow_id)
            return
        diagnostics.append(
            BuildDiagnostic(
                code="missing_source",
                message=(
                    f"schedule_input_missing: {flow_id} carries a nonzero forecast but its "
                    f"counterpart {role.counterpart} is {state}; the model invents no "
                    "balance line for it"
                ),
                semantic_item_id=flow_id,
                period_id=f"FY{min(projected)}",
            )
        )

    for role in catalog.required_roles:
        flow_id = next((item for item in role.template_items if item in model._index), None)
        if flow_id is None:
            continue
        if role.counterpart is None:
            carry_on_bridge(flow_id)
            continue
        flow = model.get_item(flow_id)
        counterpart = catalog.role(role.counterpart)
        disposition = terminal[role.counterpart]
        # A balance with a working-capital side moves by the flow itself; a
        # contra-equity balance (treasury stock) moves by the flow's amount row.
        term_id = flow_id
        if counterpart.working_capital is None:
            term_id = next(
                (item for item in role_items[role.required_role].hff_fact_items if item in model._index),
                "",
            )
        if disposition.state in ("unowed", "not_applicable"):
            schedule_id = next(
                (item for item in counterpart.population_items if item in model._index), None
            )
            if (
                schedule_id is not None
                and model.get_item(schedule_id).item_type in (ItemType.header, ItemType.spacer)
                and any(
                    row.state == "bound" for row in role_dispositions.by_role(role.counterpart)
                )
            ):
                # A balance the filing reported in earlier years and then stopped reporting
                # keeps those bound cells: they render as numbers, not as a header's blanks.
                model.get_item(schedule_id).item_type = ItemType.input
            if not _flow_projects_nonzero(flow, projected):
                continue
            # Applied default (DISCLOSURE-ADAPTS; revert site: this branch): a contra-equity
            # counterpart with no line (retired shares) leaves the flow to reduce retained
            # earnings, as the cited-inclusion route does. The binding should cite
            # ``included_in``; the warning says so.
            host = (
                _route_flow_into_included_in(
                    model, term_id=term_id, included_in=catalog.role(_RETAINED_EARNINGS_ROLE),
                )
                if counterpart.working_capital is None
                and flow_id not in _CASH_BRIDGE_FLOW_ITEM_IDS
                and term_id
                else None
            )
            if host is None:
                flow_without_balance(flow_id, role, disposition.state)
                continue
            wired.append(host)
            diagnostics.append(
                BuildDiagnostic(
                    code="source_absence_evidenced",
                    message=(
                        f"{flow_id} carries a nonzero forecast but its counterpart "
                        f"{role.counterpart} is {disposition.state}: the flow reduces "
                        f"{_RETAINED_EARNINGS_ROLE}. The binding for {role.counterpart} "
                        "should cite `included_in`."
                    ),
                    semantic_item_id=flow_id,
                    period_id=f"FY{min(projected)}",
                )
            )
            continue
        if disposition.state == "not_separately_presented":
            host = (
                _route_flow_into_included_in(
                    model,
                    term_id=term_id,
                    included_in=catalog.role(cast(str, disposition.included_in)),
                )
                if term_id
                else None
            )
            if host is not None:
                wired.append(host)
            elif _flow_projects_nonzero(flow, projected):
                flow_without_balance(flow_id, role, disposition.state)
            continue
        if counterpart.working_capital is None:
            if term_id:
                wired.extend(
                    _roll_contra_equity_counterpart(
                        model,
                        projected,
                        counterpart=counterpart,
                        amount_id=term_id,
                    )
                )
            continue
        if (
            flow.projected is not None
            and flow.projected.note != TEMPLATE_PROJECTED_ZERO_DEFAULT_NOTE
        ) or not projection & {
            int(period) for period in (flow.overrides or {})
        }:
            carry_on_bridge(flow_id)
            continue
        balance_id = next(
            (item for item in face_lines(counterpart) if item in model._index), None
        )
        schedule_id = next(
            (item for item in counterpart.population_items if item in model._index), None
        )
        if balance_id is None:
            carry_on_bridge(flow_id)
            continue
        balance = model.get_item(balance_id)
        current = balance.projected
        rolled = current is not None and current.note == _COUNTERPART_ROLL_FORWARD_NOTE
        organic = (
            schedule_id is not None
            and current is not None
            and (
                rolled
                and schedule_id in _formula_ref_ids(current.params)
                or current.type is FormulaType.ref
                and isinstance(current.params.get("source"), LineItemRef)
                and current.params["source"].id == schedule_id
            )
        )
        held = (
            current is not None
            and current.type is FormulaType.constant
            and _is_zero_value(current.params.get("value"))
        )
        if not rolled and not organic and not held:
            carry_on_bridge(flow_id)
            continue
        if not _has_known_historical_value(values, item_id=balance_id, periods=historical):
            # A counterpart with no reported value in any historical period has no
            # opening to roll from, and the forecast never invents one: the balance
            # keeps the template's projection (a roll-forward installed while history
            # stood goes with it), an investment flow carries on the cash bridge, any
            # other nonzero flow is the missing-source diagnostic naming its
            # counterpart; a zero flow moves nothing.
            if rolled:
                balance.projected = _template_projected_spec(balance_id)
                wired.append(balance.id)
            if _flow_projects_nonzero(flow, projected):
                flow_without_balance(flow_id, role, f"{disposition.state} with no reported history")
            continue
        organic_schedule = schedule_id if organic else None
        if not rolled:
            terms = [
                LineItemRef(id=balance_id, t=-1),
                {"op": "NEG", "arg": LineItemRef(id=flow_id)},
            ]
            if organic_schedule is not None:
                terms[1:1] = [
                    LineItemRef(id=organic_schedule),
                    {"op": "NEG", "arg": LineItemRef(id=organic_schedule, t=-1)},
                ]
            balance.projected = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="add_subtract",
                params={"expr": {"op": "+", "args": terms}},
                note=_COUNTERPART_ROLL_FORWARD_NOTE,
            )
            _set_periods(balance, projected)
            wired.append(balance.id)
        else:
            # If already rolled by a counterpart flow, append this additional counterpart flow term
            if current is not None and isinstance(current.params.get("expr"), dict):
                args = current.params["expr"].get("args")
                if isinstance(args, list) and not any(
                    isinstance(a, dict) and isinstance(a.get("arg"), LineItemRef) and a["arg"].id == flow_id
                    for a in args
                ):
                    args.append({"op": "NEG", "arg": LineItemRef(id=flow_id)})
                    wired.append(balance.id)
        # The balance rolls this flow, so the cash bridge does not carry it as well.
        if flow_id in _CASH_BRIDGE_FLOW_ITEM_IDS and _remove_flow_from_cash_bridge(
            model, flow_id, projection
        ):
            wired.append(_CASH_BRIDGE_ITEM_ID)
        for aggregate_id in catalog.role(_OTHER_WORKING_CAPITAL_ROLE).template_items:
            aggregate = model._index.get(aggregate_id)
            if aggregate is None:
                continue
            for spec in (aggregate.historical, aggregate.projected):
                expr = None if spec is None else spec.params.get("expr")
                if not isinstance(expr, dict) or not isinstance(expr.get("args"), list):
                    continue
                args: list = []
                for arg in expr["args"]:
                    if _formula_ref_ids(arg) != {balance_id}:
                        args.append(arg)
                    elif organic_schedule is not None:
                        args.append(
                            {
                                "op": "-",
                                "left": LineItemRef(id=organic_schedule, t=-1),
                                "right": LineItemRef(id=organic_schedule),
                            }
                        )
                expr["args"] = args
            wired.append(aggregate.id)
    return wired


def _apply_cash_flow_policy(
    model: FinancialModel,
    values: ComputedValues,
    revision: BusinessModelRevision,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    *,
    declared_rows: Mapping[str, str],
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    diagnostics: list[BuildDiagnostic],
) -> list[str]:
    wired = _rewire_deferred_tax_cash_flow_for_asset_fact(model)
    if _business_model_has_no_acquisition_program(revision):
        installed = _install_zero_projection(
            model,
            item_id=_CASH_ACQUISITIONS,
            projected=projected,
            note="accepted BusinessModel has no modeled acquisition program",
            strategy="business_model_no_acquisition_policy",
            dependencies=(_CASH_ACQUISITIONS,),
        )
        if installed is not None:
            wired.append(installed)

    # A pass-through balance's gross investment purchases and maturities are
    # not forecast judgments.  Financing carries the exact counterpart delta;
    # investing must independently carry the balance's own delta.  Using the
    # liability delta in both places silently discarded changes in the net
    # pass-through asset and broke the balance-sheet identity.  Gross
    # placeholder rows are zeroed to prevent duplication.
    catalog = load_required_role_catalog()
    if any(
        balance.pass_through
        and balance.counterpart is not None
        and balance.target_item is not None
        and {
            declared_rows.get(catalog.balance(balance.counterpart).change_role),
            balance.target_item,
        } <= set(model._index)
        for balance in catalog.balances.values()
    ):
        for item_id in (_INVESTMENT_PURCHASES, _INVESTMENT_SALES):
            installed = _install_zero_projection(
                model,
                item_id=item_id,
                projected=projected,
                note="gross pass-through portfolio turnover omitted under exact net pass-through bridge",
                strategy="pass_through_gross_turnover_exclusion",
                dependencies=(item_id,),
            )
            if installed is not None:
                wired.append(installed)

    wired.extend(
        _wire_flow_counterpart_balances(
            model,
            values,
            historical,
            projected,
            role_dispositions=role_dispositions,
            role_items=role_items,
            diagnostics=diagnostics,
        )
    )

    terminal_period = max(historical)
    terminal_fx = values.get(_CASH_FX, {}).get(terminal_period)
    fx = model._index.get(_CASH_FX)
    if (
        isinstance(terminal_fx, int | float)
        and math.isfinite(float(terminal_fx))
        and fx is not None
        and _has_no_driver(fx)
    ):
        # A known reported terminal FX effect, zero or not, supplies a default
        # forecast, not authority to remove authored period values or formulas.
        # Cash translation has no forecastable operating relationship, so an
        # unanswered row holds it at zero; unknown history stays Missing. The
        # zero rests on the FX line itself: its reported history is the
        # derivation's dependency, and when every historical period of it is
        # cited absent the forecast zero reads that citation (``target_build``
        # evidenced zero).
        fx.projected = _zero_projection_formula(
            note=(
                "latest exact reported cash-flow FX effect is zero"
                if _is_zero_value(terminal_fx)
                else "reported cash-flow FX effect has no forecast driver; held at zero"
            ),
            strategy="terminal_zero_fx_policy",
            dependencies=(_CASH_FX,),
        )
        _set_periods(fx, projected)
        wired.append(fx.id)
    return wired


def _apply_non_gaap_policy(
    model: FinancialModel,
    values: ComputedValues,
    revision: BusinessModelRevision,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    sbc_add_back_disclosed_periods: frozenset[int],
) -> list[str]:
    """Close structurally absent acquisition and non-GAAP adjustment lanes.

    Acquisition zeros require an accepted BusinessModel without an acquisition
    program and a complete historical bridge of zero. The projected other-items
    exclusion is a forward modeling choice, independent of historical coverage.
    When the issuer's filed bridge landed its stock-compensation add-back row,
    the forecast add-back keeps that definition.
    """

    wired: list[str] = []
    no_acquisition_program = _business_model_has_no_acquisition_program(revision)
    acquired_amortization_is_zero = _all_historical_values_are_zero(
        values,
        item_id=_FM_ACQUIRED_INTANGIBLE_AMORTIZATION,
        periods=historical,
    )
    if no_acquisition_program and acquired_amortization_is_zero:
        for item_id in (
            _ACQUISITION_ADDITIONS,
            _ACQUIRED_INTANGIBLE_AMORTIZATION,
            _ACQUIRED_INTANGIBLE_EXPENSE,
        ):
            if item_id not in model._index:
                continue
            item = model.get_item(item_id)
            item.projected = _zero_projection_formula(
                note=(
                    "accepted BusinessModel has no modeled acquisition program and "
                    "the complete reported acquired-intangible-amortization bridge is zero"
                ),
                strategy="business_model_no_acquisition_policy",
            )
            _set_periods(item, projected)
            wired.append(item.id)

    # Retained: protect authored ``non_gaap_other_items`` cells and mark unauthored
    # judgments ``default``; the template owns the house value.
    if _ADJUSTED_EBITDA_OTHER_ITEMS in model._index:
        other_items = model.get_item(_ADJUSTED_EBITDA_OTHER_ITEMS)
        written = _write_default_unless_authored(
            other_items,
            projected,
            float(cast(FormulaSpec, other_items.projected).params["value"]),
            owner=_OWNER,
            strategy="projected_non_gaap_other_items_exclusion",
        )
        if written:
            _set_periods(other_items, written)
            wired.append(other_items.id)

    # The issuer's bridge may add back SBC together with related items such as
    # payroll taxes. The forecast keeps that one definition: the projected SBC
    # path times the last filed year's add-back / SBC ratio, a live formula
    # whose operands stay at that year. This block is the revert site for the
    # alternative (forecast SBC alone, the template ref).
    addback_id = _FM_SBC_ADD_BACK
    if (
        sbc_add_back_disclosed_periods
        and addback_id in model._index
        and _SBC_TOTAL in model._index
    ):
        basis = max(sbc_add_back_disclosed_periods)
        addback = model.get_item(addback_id)
        if addback.overrides is None:
            addback.overrides = {}
        for period in projected:
            offset = basis - period
            addback.overrides[period] = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="issuer_sbc_add_back_ratio",
                params={
                    "expr": {
                        "op": "*",
                        "args": [
                            LineItemRef(id=_SBC_TOTAL),
                            {
                                "op": "/",
                                "left": LineItemRef(id=addback_id, t=offset),
                                "right": LineItemRef(id=_SBC_TOTAL, t=offset),
                            },
                        ],
                    }
                },
                note=(
                    "projected SBC times the last filed Adjusted EBITDA "
                    "SBC add-back / SBC ratio"
                ),
                derivation=_derivation(
                    "issuer_ebitda_reconciliation",
                    (_SBC_TOTAL, addback_id),
                ),
            )
        _set_periods(addback, projected)
        wired.append(addback.id)
    return wired


def _latest_repurchases_beyond_sbc_offset_ratio(
    values: ComputedValues,
    periods: Iterable[int],
) -> float | None:
    """Latest reported buybacks beyond the SBC-offset lane, over revenue.

    The template's SBC-offset lane (``_EQUITY_SBC_OFFSET``, -SBC) carries the
    buyback that offsets SBC dilution, and the issuer's reported repurchases
    already contain that buyback. The repurchase lane is the remainder, so the
    projected equity outflow counts each reported buyback dollar once.
    """

    from .scenario_bridge import normalize_control_magnitude

    for period in sorted({int(value) for value in periods}, reverse=True):
        repurchases = values.get(_REPURCHASE_AMOUNT, {}).get(period)
        sbc = values.get(_CASH_FLOW_SBC, {}).get(period)
        revenue = values.get(_REVENUE, {}).get(period)
        if repurchases is None or sbc is None or revenue is None or revenue <= 0:
            continue
        magnitude = normalize_control_magnitude(_REPURCHASE_AMOUNT, repurchases)
        ratio = max(0.0, magnitude - float(sbc)) / float(revenue)
        if math.isfinite(ratio):
            return ratio
    return None


def _latest_employee_plan_proceeds_ratio(
    values: ComputedValues,
    periods: Iterable[int],
) -> float | None:
    """Latest reported employee-stock-plan cash over revenue, from either role.

    The catalog binds one economic fact to two roles: the face cash-flow line
    (``_FM_EMPLOYEE_PLAN_PROCEEDS``) and the filing's ESPP disclosure
    (``_EMPLOYEE_PLAN_PROCEEDS``, the schedule row itself). An issuer
    reports under either, so each period reads whichever row carries it.
    """

    for period in sorted({int(value) for value in periods}, reverse=True):
        revenue = values.get(_REVENUE, {}).get(period)
        if revenue is None or revenue <= 0:
            continue
        for row_id in (_FM_EMPLOYEE_PLAN_PROCEEDS, _EMPLOYEE_PLAN_PROCEEDS):
            proceeds = values.get(row_id, {}).get(period)
            if proceeds is None:
                continue
            ratio = float(proceeds) / float(revenue)
            if math.isfinite(ratio) and ratio >= 0:
                return ratio
    return None


def _apply_net_dilution_default(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> list[str]:
    """Write the house net dilution where no ValuationPolicy rate landed.

    The ValuationPolicy ``net_share_dilution`` rates are the one authored writer of
    the rate the basic and diluted share curves compound on
    (``build_valuation_inputs``); every other projection year takes the house
    value as ``default`` (``judgment_absent_default_used``).
    """

    if _DILUTION_RATE not in model._index:
        return []
    house = house_net_dilution(values, historical)
    if house is None:
        return []
    written = _write_default_unless_authored(
        model.get_item(_DILUTION_RATE),
        projected,
        house.value,
        owner=_OWNER,
        strategy=house.strategy,
        dependencies=(_DILUTED_SHARES,),
        lineage_by_period={
            period: {
                "value": house.value,
                "expr": {
                    "op": "-",
                    "left": {
                        "op": "/",
                        "left": LineItemRef(
                            id=_DILUTED_SHARES, t=house.basis_period - period
                        ),
                        "right": LineItemRef(
                            id=_DILUTED_SHARES, t=house.prior_period - period
                        ),
                    },
                    "right": 1,
                },
            }
            for period in projected
        },
    )
    return [_DILUTION_RATE] if written else []


def _apply_nci_share_policy(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> list[str]:
    """Hold the latest reported NCI share of consolidated net income (NCI-FORECAST-HELD-SHARE).

    The attributable row projects consolidated net income (pretax less provision)
    times the share held at ``NCI_SHARE_RATE_ITEM_ID``. An authored cell on that
    row stays ``input``; every other open projection year takes the house share
    as ``default`` (``judgment_absent_default_used``). An authored forecast on
    the attributable row keeps that value and takes no share. The row survives
    only where the issuer reports it, or an author holds it, so a model without
    the row writes nothing.
    """

    if not {NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID, NCI_SHARE_RATE_ITEM_ID} <= set(model._index):
        return []
    house = house_nci_share(values, historical)
    if house is None:
        return []
    nci = model.get_item(NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID)
    rate = model.get_item(NCI_SHARE_RATE_ITEM_ID)
    authored = set(_authored_periods(nci, projected))
    open_periods = tuple(period for period in projected if period not in authored)
    if not open_periods:
        return []
    _write_default_unless_authored(
        rate,
        open_periods,
        house.value,
        owner=_OWNER,
        strategy=house.strategy,
        dependencies=(NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID, _PRETAX_INCOME, _INCOME_TAX_PROVISION),
        lineage_by_period={
            period: {
                "value": house.value,
                "expr": {
                    "op": "/",
                    "left": LineItemRef(
                        id=NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID, t=house.basis_period - period
                    ),
                    "right": {
                        "op": "-",
                        "left": LineItemRef(id=_PRETAX_INCOME, t=house.basis_period - period),
                        "right": LineItemRef(id=_INCOME_TAX_PROVISION, t=house.basis_period - period),
                    },
                },
            }
            for period in open_periods
        },
    )
    _set_periods(rate, open_periods)
    _set_periods(nci, projected)
    return [nci.id, rate.id]


def _route_combined_common_stock_credit(
    model: FinancialModel,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    role_dispositions: RoleDispositionTable | None,
) -> list[str]:
    """Credit the issuer's combined common-stock/APIC presentation for non-cash SBC."""
    apic_role = "statement.bs.additional_paid_in_capital"
    common_role = "statement.bs.common_stock"
    terminal = (
        None if role_dispositions is None else _terminal_rows(role_dispositions).get(apic_role)
    )
    combined = (
        terminal is not None
        and terminal.state == "not_separately_presented"
        and terminal.included_in == common_role
    )
    last = max(historical, default=None)
    for item_id in (
        "tpl.fm.balance_sheet.additional_paid_in_capital",
        "tpl.a.dividends_shares.additional_paid_in_capital",
    ):
        item = model._index.get(item_id)
        marker = None if item is None or last is None else (item.overrides or {}).get(last)
        combined = combined or (
            marker is not None
            and marker.subtype == "not_separately_presented"
            and marker.params.get("included_in") == common_role
        )
    face = model._index.get("tpl.fm.balance_sheet.common_stock")
    if not combined or face is None or _CASH_FLOW_SBC not in model._index:
        return []
    schedule_id = "tpl.a.dividends_shares.common_stock"
    target = face
    if (
        face.projected is not None
        and face.projected.type is FormulaType.ref
        and face.projected.subtype == "cross_sheet_ref"
        and _formula_ref_ids(face.projected.params) == {schedule_id}
        and schedule_id in model._index
    ):
        target = model.get_item(schedule_id)
    additions = [LineItemRef(id=_CASH_FLOW_SBC)]
    proceeds = model._index.get(_EMPLOYEE_PLAN_PROCEEDS)
    if (
        proceeds is not None
        and (
            proceeds.formula_periods is None
            or bool(set(projected) & set(proceeds.formula_periods))
        )
        and (
            proceeds.projected is None
            or proceeds.projected.subtype not in ("not_applicable", "not_separately_presented")
        )
    ):
        additions.append(LineItemRef(id=proceeds.id))
    target.projected = FormulaSpec(
        type=FormulaType.roll_forward,
        subtype="ending_balance",
        params={
            "beginning": LineItemRef(id=target.id, t=-1),
            "additions": additions,
        },
        note="SBC equity credit follows the filed combined common-stock/APIC presentation",
    )
    # The template's first forecast column can hold its own lagged-reference
    # override; it must follow the same equity credit as the lane.
    for period in projected:
        override = (target.overrides or {}).get(period)
        if (
            override is not None
            and override.type is FormulaType.ref
            and override.params.get("source") == LineItemRef(id=target.id, t=-1)
            and not override.params.get("adjustment")
        ):
            target.overrides = {
                **(target.overrides or {}),
                period: target.projected.model_copy(deep=True),
            }
    _set_periods(target, (*historical, *projected) if target.historical is not None else projected)
    return [target.id]


def _apply_share_policy(
    model: FinancialModel,
    values: ComputedValues,
    revision: BusinessModelRevision,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    *,
    role_dispositions: RoleDispositionTable | None = None,
) -> list[str]:
    required = {
        _REVENUE,
        _BASIC_SHARES,
        _DILUTED_SHARES,
        _DILUTIVE_INCREMENT,
        _END_SHARES,
        _SHARES_ISSUED,
        _DILUTION_RATE,
        _REPURCHASE_AMOUNT,
        _CASH_FLOW_SBC,
        _DIVIDENDS,
        _EMPLOYEE_PLAN_PROCEEDS,
        _FM_EMPLOYEE_PLAN_PROCEEDS,
    }
    if not required <= set(model._index):
        return _route_combined_common_stock_credit(
            model, historical, projected, role_dispositions,
        )
    repurchase_ratio = _latest_repurchases_beyond_sbc_offset_ratio(
        values, historical
    )
    employee_plan_ratio = _latest_employee_plan_proceeds_ratio(values, historical)
    reported_repurchases = _latest_reported_amount(
        values, historical, _CASH_FLOW_SHARE_REPURCHASES
    ) or _latest_reported_amount(
        values, historical, _REPURCHASE_AMOUNT
    )
    repurchase_bases = capital_return_bases(
        "repurchases", revision, reported=reported_repurchases is not None
    )

    # Retained: early generation lets filed diluted-share pins shadow the basic-share history proxy.
    basic = model.get_item(_BASIC_SHARES)
    basic.projected = FormulaSpec(
        type=FormulaType.growth,
        subtype="compound",
        params={
            "base": LineItemRef(id=_BASIC_SHARES, t=-1),
            "rate": LineItemRef(id=_DILUTION_RATE),
        },
        note="accepted net-dilution control applied to reported basic shares",
        derivation=_derivation("net_dilution_share_curve", (_BASIC_SHARES, _DILUTION_RATE)),
    )
    increment = model.get_item(_DILUTIVE_INCREMENT)
    increment.item_type = ItemType.derived
    increment.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                LineItemRef(id=_DILUTED_SHARES),
                LineItemRef(id=_BASIC_SHARES),
            ]
        },
        note="dilutive increment derived from accepted diluted less basic share curves",
        derivation=_derivation("dilutive_share_increment", (_DILUTED_SHARES, _BASIC_SHARES)),
    )
    end_shares = model.get_item(_END_SHARES)
    end_shares.item_type = ItemType.derived
    end_shares.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="reported_weighted_average_proxy",
        params={"source": LineItemRef(id=_BASIC_SHARES)},
        note="reported basic weighted-average shares used as bounded end-share proxy",
        derivation=_derivation("end_share_proxy", (_BASIC_SHARES,)),
    )
    end_shares.projected = end_shares.historical.model_copy(deep=True)

    # Retained: authored legacy gross-issuance cells must not shadow the net-change identity.
    shares_issued = model.get_item(_SHARES_ISSUED)
    for period in projected:
        if shares_issued.overrides is not None:
            shares_issued.overrides.pop(int(period), None)
        if shares_issued.values is not None:
            shares_issued.values.values.pop(int(period), None)
    # Net dilution includes repurchases and non-cash dilution; multiplying
    # it by a market price is not employee-plan cash proceeds. Authored
    # period inputs remain on the row, and a reported cash/revenue ratio
    # below supplies a projection only when that observed basis exists.
    employee_plan_proceeds = model.get_item(_EMPLOYEE_PLAN_PROCEEDS)
    employee_plan_proceeds.item_type = ItemType.input
    employee_plan_proceeds.projected = None
    rows = [basic, increment, end_shares, employee_plan_proceeds]
    for item in rows:
        _set_periods(
            item,
            (*historical, *projected) if item in (end_shares, employee_plan_proceeds) else projected,
        )
    # The proceeds row is an input row over history and forecast alike: a historical
    # cell no filing value reached is that input's own gap (a cited absence when the
    # role is dispositioned unreported, else a missing input), and the plan-proceeds
    # ratio reading it keeps its periods and computes once the input arrives -
    # construction retires no ratio for a missing operand.
    wired = [item.id for item in rows]
    if repurchase_bases and repurchase_ratio is not None:
        wired.extend(
            _install_default_revenue_ratio(
                model,
                target_id=_REPURCHASE_AMOUNT,
                rate_id=_REPURCHASE_RATE,
                anchor_id=_REPURCHASE_AMOUNT,
                label="  Share repurchases beyond SBC offset as % of revenue",
                ratio=repurchase_ratio,
                projected=projected,
                note=(
                    "company-grounded latest share repurchases beyond the "
                    "SBC-offset buyback, over revenue"
                ),
                strategy="share_repurchase_revenue_ratio",
            )
        )
    elif repurchase_bases and not _authored_periods(
        model.get_item(_REPURCHASE_AMOUNT), projected
    ):
        # An authorized repurchaser with no computable beyond-SBC ratio gets
        # the dividends shape's zero/hold: the row holds at zero with a
        # formula (no rate row), and the sibling rows above and below are
        # wired regardless. ``repurchase_ratio`` is None only when no
        # historical period carries the repurchase row, SBC and revenue
        # together; a reported zero yields a 0.0 ratio, not None.
        repurchases = model.get_item(_REPURCHASE_AMOUNT)
        repurchases.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": 0.0},
            note=(
                "BusinessModel capital-return policy names share repurchases "
                "but no computable latest repurchase ratio; repurchases "
                "hold at zero"
                if "business_model" in repurchase_bases
                else "latest reported share repurchases have no computable "
                "beyond-SBC ratio; repurchases hold at zero"
            ),
            derivation=_derivation("business_model_no_repurchase_policy", ()),
        )
        _set_periods(repurchases, projected)
        wired.append(repurchases.id)
    if employee_plan_ratio is not None:
        wired.extend(
            _install_default_revenue_ratio(
                model,
                target_id=_EMPLOYEE_PLAN_PROCEEDS,
                rate_id=_EMPLOYEE_PLAN_PROCEEDS_RATE,
                anchor_id=_EMPLOYEE_PLAN_PROCEEDS,
                label="  Employee stock plan proceeds as % of revenue",
                ratio=employee_plan_ratio,
                projected=projected,
                note="company-grounded latest employee-stock-plan-proceeds/revenue ratio",
                strategy="employee_stock_plan_revenue_ratio",
            )
        )
    reported_dividends = _latest_reported_amount(
        values, historical, _CASH_FLOW_DIVIDENDS
    )
    dividend_bases = capital_return_bases(
        "dividends", revision, reported=reported_dividends is not None
    )
    payer_revenue = (
        values.get(_REVENUE, {}).get(reported_dividends[0])
        if reported_dividends is not None else None
    )
    if (
        reported_dividends is not None
        and payer_revenue is not None
        and math.isfinite(float(payer_revenue))
        and payer_revenue > 0
    ):
        # A reported latest-year amount projects at its revenue ratio, revised by
        # the ``dividends_paid`` control.
        period, paid = reported_dividends
        wired.extend(
            _install_default_revenue_ratio(
                model,
                target_id=_DIVIDENDS,
                rate_id=_DIVIDENDS_RATE,
                anchor_id=_DIVIDENDS,
                label="  Dividends paid as % of revenue",
                ratio=paid / float(payer_revenue),
                projected=projected,
                note="company-grounded latest dividends-paid/revenue ratio",
                strategy="latest_historical_revenue_ratio",
            )
        )
        dividends = model.get_item(_DIVIDENDS)
        dividends.historical = FormulaSpec(
            type=FormulaType.ref,
            subtype="negated_ref",
            params={
                "source": LineItemRef(id=_CASH_FLOW_DIVIDENDS),
                "negate": True,
            },
            note="filed cash-flow dividends paid",
        )
        _set_periods(dividends, (*historical, *projected))
    else:
        # No reported latest-year amount to project from, whether or not the
        # BusinessModel names a dividend mechanism: the rows hold at zero so every
        # reader has a formula; a named mechanism still offers the control.
        no_dividend_note = (
            "BusinessModel capital-return policy names dividends but the latest "
            "historical period reports none"
            if "business_model" in dividend_bases
            else "BusinessModel capital-return policy contains no dividend mechanism"
        )
        dividends = model.get_item(_DIVIDENDS)
        dividends.historical = FormulaSpec(
            type=FormulaType.ref,
            subtype="negated_ref",
            params={
                "source": LineItemRef(id=_CASH_FLOW_DIVIDENDS),
                "negate": True,
            },
            note=(
                "filed cash-flow dividends under a BusinessModel with no dividend mechanism"
            ),
            derivation=_derivation(
                "business_model_no_dividend_policy",
                (_CASH_FLOW_DIVIDENDS,),
            ),
        )
        dividends.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": 0.0},
            note=no_dividend_note,
            derivation=_derivation("business_model_no_dividend_policy", ()),
        )
        _set_periods(dividends, (*historical, *projected))
        wired.append(dividends.id)
        if _DIVIDEND_PAYABLE in model._index:
            payable = model.get_item(_DIVIDEND_PAYABLE)
            payable_zero = FormulaSpec(
                type=FormulaType.constant,
                params={"value": 0.0},
                note=no_dividend_note,
                derivation=_derivation(
                    "business_model_no_dividend_policy",
                    (_DIVIDENDS,),
                ),
            )
            payable.historical = payable_zero
            payable.projected = payable_zero.model_copy(deep=True)
            _set_periods(payable, (*historical, *projected))
            wired.append(payable.id)
        cash_dividends = model._index.get(_CASH_FLOW_DIVIDENDS)
        if cash_dividends is not None and cash_dividends.projected is not None:
            cash_dividends.projected = cash_dividends.projected.model_copy(
                update={
                    "derivation": _derivation(
                        "business_model_no_dividend_policy",
                        (_DIVIDENDS,),
                    )
                }
            )
    wired.extend(_route_combined_common_stock_credit(
        model, historical, projected, role_dispositions,
    ))
    return wired


def _set_exact_formula_periods(item, periods: Iterable[int]) -> None:
    item.formula_periods = sorted({int(value) for value in periods})


def _apply_non_gaap_tax_effect_policy(
    model: FinancialModel,
    historical: tuple[int, ...],
    disclosed_periods: frozenset[int],
) -> list[str]:
    """Ground the historical tax effect on adjustments per Henry's 2026-09-25 ruling.

    Management discloses the tax effect or it is estimated from the historical
    rate effect on adjustments; zero is neither. One identity serves history
    and projection: tax effect = −add-backs × the adjustment tax rate. A
    disclosed year keeps its reported amount (installed by the filing
    disclosures) and grounds its rate as −amount ÷ add-backs. A later
    undisclosed year carries the prior year's rate forward, exactly as the
    template's projection does, so it holds the latest disclosed rate. A year
    no disclosure precedes takes its own GAAP effective tax rate: the graph
    evaluates period by period, so no year can read a later year's rate.
    """

    adjustment_rate = model.get_item(_ADJUSTMENT_TAX_RATE)
    add_backs = {
        "op": "SUM",
        "args": [LineItemRef(id=item_id) for item_id in _ADJUSTMENT_TAX_BASE],
    }
    disclosed_so_far = False
    if adjustment_rate.overrides is None:
        adjustment_rate.overrides = {}
    for period in historical:
        if period in disclosed_periods:
            disclosed_so_far = True
            adjustment_rate.overrides[period] = FormulaSpec(
                type=FormulaType.ratio,
                subtype="divide",
                params={
                    "numerator": {"op": "NEG", "arg": LineItemRef(id=_FM_NON_GAAP_TAX_EFFECT)},
                    "denominator": add_backs,
                },
                note="disclosed tax effect on adjustments over the add-backs",
                derivation=_derivation(
                    "historical_non_gaap_tax_rate_disclosed",
                    (_FM_NON_GAAP_TAX_EFFECT, *_ADJUSTMENT_TAX_BASE),
                ),
            )
        elif disclosed_so_far:
            adjustment_rate.overrides[period] = FormulaSpec(
                type=FormulaType.ref,
                subtype="carry_forward",
                params={"source": LineItemRef(id=_ADJUSTMENT_TAX_RATE, t=-1)},
                note="estimated at the latest disclosed rate on adjustments",
                derivation=_derivation(
                    "historical_non_gaap_tax_rate_disclosed_year_estimate",
                    (_ADJUSTMENT_TAX_RATE,),
                ),
            )
        else:
            adjustment_rate.overrides[period] = FormulaSpec(
                type=FormulaType.ref,
                subtype="cell_ref",
                params={"source": LineItemRef(id=_GAAP_TAX_RATE)},
                note="no disclosed tax effect on adjustments up to this year: estimated at the GAAP effective tax rate",
                derivation=_derivation(
                    "historical_non_gaap_tax_rate_effective_rate_estimate",
                    (_GAAP_TAX_RATE,),
                ),
            )
    _set_periods(adjustment_rate, historical)
    return [adjustment_rate.id]


def _apply_historical_schedule_closure(
    model: FinancialModel,
    values: ComputedValues,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    disclosed_tax_effect_periods: frozenset[int],
    *,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
) -> list[str]:
    """Close exact historical schedule algebra without adding forecast judgment."""

    required = {
        _PHYSICAL_DEPRECIATION,
        _TOTAL_DEPRECIATION_AND_AMORTIZATION,
        _SOFTWARE_AMORTIZATION,
        _ACQUIRED_INTANGIBLE_EXPENSE,
        _SOFTWARE_ENDING,
        _CAPITALIZED_RESEARCH,
        _SOFTWARE_ADDITIONS,
        _CAPITALIZED_RESEARCH_RATIO,
        _SBC_TOTAL,
        _SBC_ALLOCATED_TOTAL,
        *_SBC_RATE_IDS,
        *_SBC_COMPONENT_IDS,
        *_SBC_BASE_IDS,
        _ASSUMPTION_RETAINED_EARNINGS,
        _RETAINED_EARNINGS,
        _EQUITY_SBC_OFFSET,
        _CASH_FLOW_SBC,
        _SBC_YOY_GROWTH,
        _DEBT_AVERAGE,
        _DEBT_INTEREST_RATE,
        _CASH_BEGINNING,
        _CASH_ENDING,
        _CASH_NET_CHANGE,
        _FM_NON_GAAP_TAX_EFFECT,
        _ADJUSTMENT_TAX_EFFECT,
        _ADJUSTMENT_TAX_RATE,
        _GAAP_TAX_RATE,
        *_ADJUSTMENT_TAX_BASE,
        _FM_DEFERRED_TAX_CASH_FLOW,
    }
    if not required <= set(model._index):
        return []

    wired: list[str] = []
    physical = model.get_item(_PHYSICAL_DEPRECIATION)
    # The residual has the filing's own period definition. A cited software
    # inclusion belongs to inclusive PPE depreciation, not to a second
    # numerical subtraction from total D&A. Earlier separate years retain
    # the ordinary residual above.
    for period in historical:
        if not _software_inside_ppe(model, period):
            continue
        inclusive = cast(FormulaSpec, physical.historical).without(
            lambda ref: ref.ref.id == _SOFTWARE_AMORTIZATION
        )
        if inclusive is None:
            continue
        inclusive = inclusive.model_copy(update={
            "note": "historical_inclusive_ppe_depreciation",
            "derivation": _derivation(
                "historical_physical_depreciation_residual",
                (_TOTAL_DEPRECIATION_AND_AMORTIZATION, _ACQUIRED_INTANGIBLE_EXPENSE),
            ),
        })
        if physical.overrides is None:
            physical.overrides = {}
        physical.overrides[period] = inclusive
    wired.append(physical.id)

    software_ending = model.get_item(_SOFTWARE_ENDING)
    # Retained: filed software overrides yield to the reported schedule balance.
    if software_ending.overrides is not None:
        for period in historical:
            software_ending.overrides.pop(period, None)
    wired.append(software_ending.id)
    # Capitalized software is written once, from its roles' dispositions, by
    # ``complete_capitalized_software_schedule``; closure never zeroes it.
    # A software cash line included in capex or held cited absent by rule (d)
    # leaves the investing total. Its other-investing residual must omit the
    # same nonnumeric operand; separately presented years keep the ordinary residual.
    if _OTHER_INVESTING in model._index:
        other_investing = model.get_item(_OTHER_INVESTING)
        if other_investing.historical is not None:
            # The residual installer owns this row's history years (``_install_residual_histories``:
            # a year a filed value stands in is not a formula period); this helper declares the
            # years it writes a lowering for and no others.
            lowered: list[int] = []
            for period in historical:
                if (
                    (other_investing.overrides or {}).get(period) is not None
                    or (
                        _software_inclusion_fact(
                            model, _SOFTWARE_INVESTING_CASH_ITEM_ID, period
                        ) is None
                        and optional_component_participates(
                            model, _SOFTWARE_INVESTING_CASH_ITEM_ID, period
                        )
                    )
                ):
                    continue
                inclusive = other_investing.historical.without(
                    lambda ref: ref.ref.id == _SOFTWARE_INVESTING_CASH_ITEM_ID
                )
                if inclusive is None:
                    continue
                if other_investing.overrides is None:
                    other_investing.overrides = {}
                other_investing.overrides[period] = inclusive
                lowered.append(period)
            _set_periods(other_investing, lowered)
            wired.append(other_investing.id)

    # Retained: reported software additions must not enter the earlier R&D house-basis read.
    capitalized_research = model.get_item(_CAPITALIZED_RESEARCH)
    capitalized_research.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="capitalized_software_additions_bridge",
        params={"source": LineItemRef(id=_SOFTWARE_ADDITIONS)},
        note="capitalized internal-use software is the disclosed capitalized R&D lane",
        derivation=_derivation(
            "capitalized_software_additions_bridge",
            (_SOFTWARE_ADDITIONS,),
        ),
    )
    _set_periods(capitalized_research, historical)
    wired.append(capitalized_research.id)

    # The bridged lane over the additions it bridges is the additions' own share. Where the
    # filing cites the additions absent in every year there is no amount to divide, and an
    # author's forecast of them at 0% makes the ratio 0/0: the ratio of an absent line is itself
    # absent in every period, retired through the graph owner like any reader of a cited line.
    if _SOFTWARE_ADDITIONS_ROLE in role_dispositions.roles_in("not_applicable"):
        graph = DependencyGraph()
        graph.build(model)
        for period in (*historical, *projected):
            graph.retire(_CAPITALIZED_RESEARCH_RATIO, period)

    # D24: SBC is allocated across income-statement lines only as the issuer
    # discloses the split, never spread evenly. Disclosed, each component lane
    # holds its filed fact in every year the aggregate SBC is reported, each
    # rate is that fact over the same year's line expense, and the allocated
    # total sums the lanes. The forecast rates follow the SBC forecast method
    # the authored rows carry (``build_mbc_entry`` records it on each row): a
    # ``per_line`` row is the line's judgment as written; an ``aggregate_scale``
    # row (``sbc_pct``) scales the disclosed mix, rate_i x sbc_pct / r_agg; a
    # row nothing authored is the line's latest disclosed rate, written
    # ``default`` (``sbc_allocation``) so the house carry surfaces as a finding
    # and stays authorable. Undisclosed, the component lanes, their rates and
    # their sum stay unallocated (no value) unless authored rows set the
    # rates; the aggregate is otherwise projected from its own rate
    # (``sbc_allocation``, build_non_gaap_addbacks).
    sbc_periods = _sbc_reported_periods(values, historical)
    if _discloses_sbc_line_split(model, sbc_periods):
        allocated_total = model.get_item(_SBC_ALLOCATED_TOTAL)
        _restore_unallocated_periods(allocated_total, projected)
        allocated_total.historical = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="sum_range",
            params={
                "function": "SUM",
                "items": [LineItemRef(id=item_id) for item_id in _SBC_COMPONENT_IDS],
            },
            note="sum of the issuer-disclosed SBC line allocation",
            derivation=_derivation(
                "aggregate_sbc_historical_allocation_sum", _SBC_COMPONENT_IDS
            ),
        )
        mix = house_sbc_mix(values, sbc_periods)
        for rate_id, component_id, base_id in zip(
            _SBC_RATE_IDS, _SBC_COMPONENT_IDS, _SBC_BASE_IDS, strict=True
        ):
            rate = model.get_item(rate_id)
            rate.historical = FormulaSpec(
                type=FormulaType.ratio,
                subtype="divide",
                params={
                    "numerator": LineItemRef(id=component_id),
                    "denominator": LineItemRef(id=base_id),
                },
                note="issuer-disclosed SBC on this line over the same year's line expense",
                derivation=_derivation(
                    "filing_schedule_disclosure", (component_id, base_id)
                ),
            )
            line_rate = mix.line_rates.get(rate_id)
            if line_rate is not None:
                _write_default_unless_authored(
                    rate,
                    projected,
                    line_rate.value,
                    owner=_OWNER,
                    strategy="sbc_allocation",
                    dependencies=(component_id, base_id),
                )
                _scale_aggregate_sbc_rate(
                    rate,
                    projected,
                    line_rate=line_rate,
                    aggregate=mix.aggregate,
                )
        for item_id in (*_SBC_RATE_IDS, _SBC_ALLOCATED_TOTAL):
            item = model.get_item(item_id)
            periods = (
                (*historical, *projected)
                if item.formula_periods is None
                else item.formula_periods
            )
            _set_exact_formula_periods(
                item,
                (
                    *(period for period in periods if period not in historical),
                    *sbc_periods,
                ),
            )
            wired.append(item.id)
    else:
        for item_id in (*_SBC_RATE_IDS, *_SBC_COMPONENT_IDS, _SBC_ALLOCATED_TOTAL):
            item = model.get_item(item_id)
            item.historical = None
            periods = (
                (*historical, *projected)
                if item.formula_periods is None
                else item.formula_periods
            )
            _set_exact_formula_periods(
                item, (period for period in periods if period not in historical)
            )
            wired.append(item.id)
        # An authored rate does not disclose the amount of an included expense
        # base. A partial allocation is not the total SBC split (D24).
        allocated_total = model.get_item(_SBC_ALLOCATED_TOTAL)
        for period in projected:
            unallocated = [
                (component_id, spec)
                for component_id in _SBC_COMPONENT_IDS
                if (spec := (model.get_item(component_id).overrides or {}).get(period))
                is not None and spec.subtype == "not_separately_presented"
            ]
            # The marker carries the SBC role's own pin: the cited component lines it
            # sums are other lines' absences (an expense base), not this line's.
            sbc_pin = next(
                (
                    row for role, row in _terminal_rows(role_dispositions).items()
                    if _CASH_FLOW_SBC in _role_lines(model, role_items, role)
                    and row.state == "not_separately_presented"
                    and row.absence_citation is not None
                ),
                None,
            )
            if (
                unallocated
                and sbc_pin is not None
                and (sbc_citation := sbc_pin.absence_citation) is not None
                and not _holds_authored_control(allocated_total, (period,), nonzero_only=False)
            ):
                if allocated_total.overrides is None:
                    allocated_total.overrides = {}
                allocated_total.overrides[period] = not_separately_presented_formula(
                    cast(str, sbc_pin.included_in),
                    sbc_citation.model_dump(mode="json"),
                    original_override=allocated_total.overrides.get(period),
                ).model_copy(update={
                    "note": "SBC line split unallocated (D24)",
                    "derivation": _derivation(
                        "sbc_allocation", (_SBC_TOTAL, *(item_id for item_id, _ in unallocated)),
                    ),
                })

        # An issuer whose cash flow statement cites the consolidated SBC line absent in
        # every historical year reports no SBC amount: there is nothing to allocate and
        # no reported SBC / revenue rate for ``_project_unallocated_sbc_aggregate`` to
        # carry. The aggregate's template forecast reads the unallocated line sum, so the
        # cited-absent breakdown would reach every reader of SBC (the add-back, adjusted
        # EBITDA, free cash flow, the projected balance sheet and cash roll-forward) as a
        # missing operand. The aggregate holds at zero instead, resting on the consolidated
        # line's own cited absence (``target_build`` evidenced zero); an authored aggregate
        # forecast keeps its periods.
        if _CASH_FLOW_SBC_ROLE in role_dispositions.roles_in("not_applicable"):
            sbc_total = model.get_item(_SBC_TOTAL)
            rate_authored_periods = tuple(
                period
                for period in projected
                if all(
                    (spec := (model.get_item(component_id).overrides or {}).get(period))
                    is None
                    or spec.subtype != "not_separately_presented"
                    for component_id in _SBC_COMPONENT_IDS
                )
                and any(
                    _holds_authored_control(
                        model.get_item(rate_id), (period,), nonzero_only=False
                    )
                    for rate_id in _SBC_RATE_IDS
                )
            )
            open_periods = tuple(
                period
                for period in projected
                if (sbc_total.overrides or {}).get(period) is None
                and not _holds_authored_control(sbc_total, (period,), nonzero_only=False)
                and period not in rate_authored_periods
            )
            if open_periods:
                prior_projected = sbc_total.projected
                if prior_projected is not None and rate_authored_periods:
                    if sbc_total.overrides is None:
                        sbc_total.overrides = {}
                    for period in rate_authored_periods:
                        if period not in sbc_total.overrides:
                            sbc_total.overrides[period] = prior_projected.model_copy(
                                deep=True
                            )
                sbc_total.projected = _zero_projection_formula(
                    note="consolidated SBC is cited absent; the forecast holds at zero",
                    strategy="sbc_allocation",
                    dependencies=(_CASH_FLOW_SBC,),
                )
                if sbc_total.formula_periods is not None:
                    _set_periods(sbc_total, (*open_periods, *rate_authored_periods))
                wired.append(sbc_total.id)
                # Year-over-year growth of a zero held on a cited absence is 0/0: the
                # reader of an absent line is itself absent, retired through the graph
                # owner (as the capitalized-R&D ratio above).
                graph = DependencyGraph()
                graph.build(model)
                for period in sorted({*open_periods, *(year + 1 for year in open_periods)}):
                    if period in projected:
                        graph.retire(_SBC_YOY_GROWTH, period)

    # Retained: early generation propagates cited SBC NSP pins onto this separate reconciliation row.
    equity_sbc_offset = model.get_item(_EQUITY_SBC_OFFSET)
    equity_sbc_offset.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="negated_ref",
        params={"source": LineItemRef(id=_CASH_FLOW_SBC), "negate": True},
        note="noncash SBC offset in the historical equity reconciliation",
        derivation=_derivation(
            "historical_equity_sbc_offset",
            (_CASH_FLOW_SBC,),
        ),
    )
    _set_periods(equity_sbc_offset, (*historical, *projected))
    wired.append(equity_sbc_offset.id)

    # Retained: the pin/request's opening column is historical[0], not the template's first year.
    first_historical = historical[0]
    cash_beginning = model.get_item(_CASH_BEGINNING)
    if cash_beginning.overrides is None:
        cash_beginning.overrides = {}
    cash_beginning.overrides[first_historical] = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="corporate_cash_opening_bridge",
        params={
            "expr": {
                "op": "-",
                "left": LineItemRef(id=_CASH_ENDING),
                "right": LineItemRef(id=_CASH_NET_CHANGE),
            }
        },
        note="opening cash reconstructed from exact ending cash less exact net change",
        derivation=_derivation(
            "corporate_cash_opening_bridge",
            (_CASH_ENDING, _CASH_NET_CHANGE),
        ),
    )
    _set_periods(cash_beginning, (first_historical,))
    wired.append(cash_beginning.id)

    wired.extend(
        _apply_non_gaap_tax_effect_policy(
            model, historical, disclosed_tax_effect_periods
        )
    )

    # Declare the whole software readout chain before disposition retirement:
    # restoring only the average cannot cascade through already-retired endpoints.
    # An unowed balance's chain is retired outright (``_retire_unowed_roles``), so
    # retirement owns its axes alone: no period is declared on rows that hold no
    # formula once it has run.
    software_balance_state = _terminal_state(role_dispositions, _SOFTWARE_BALANCE_ROLE)
    derived_periods = (*historical[1:], *projected)
    # A cited-absent face carries a marker after retirement, not the schedule
    # reference that retired this endpoint on the cold pass.
    software_ending_periods = (
        *historical,
        *(projected if software_balance_state != "not_applicable" else ()),
    )
    for item_id, periods in (
        *((row, derived_periods) for row in _FIRST_PERIOD_DERIVED_ROWS),
        (_SOFTWARE_ENDING, software_ending_periods),
        *((row, historical[1:]) for row in _HISTORICAL_READOUT_ROWS),
    ):
        if item_id not in model._index:
            continue
        if software_balance_state == "unowed" and item_id in _SOFTWARE_READOUT_CHAIN:
            continue
        item = model.get_item(item_id)
        _set_exact_formula_periods(item, periods)
        wired.append(item.id)
    for item_id in (
        _PPE_DEPRECIATION_RATE,
        _DEPRECIATION_GROWTH,
        _SOFTWARE_AMORTIZATION_RATE,
    ):
        item = model.get_item(item_id)
        _set_exact_formula_periods(item, derived_periods)
        wired.append(item.id)
    return wired


class _FilingScheduleDisclosures(TypedDict):
    wired: tuple[str, ...]
    source_fact_coverage: tuple[dict[str, object], ...]
    tax_effect_disclosed_periods: frozenset[int]
    sbc_add_back_disclosed_periods: frozenset[int]


# An issuer bridge's components land on the adjusted-earnings rows the
# template has for them, keyed by the bridge's output metric and the
# component's role; a bridge whose metric has no entry (expense lines,
# margins, free cash flow) stays on the rail as facts. Roles sharing one row
# (acquisition-related and restructuring on other items) sum. The non-GAAP
# net income bridge's pre-tax rows land on the ``_adjustment`` twins; its
# acquired-intangible amortization shares the one amortization row. The
# Historical bridge bases and first-stage operands are retained filing facts,
# not live statement refs. A disclosed tax effect lands as reported; closure
# grounds the year's adjustment tax rate on it (Henry's ruling, 2026-09-25).
_BRIDGE_ROW_BY_METRIC_ROLE: Mapping[tuple[str, str], str] = {
    ("adjusted_ebitda", "interest"): "tpl.fm.adjusted_earnings.interest_expense",
    ("adjusted_ebitda", "income_tax"): "tpl.fm.adjusted_earnings.income_tax_expense",
    ("adjusted_ebitda", "depreciation_and_amortization"): (
        "tpl.fm.adjusted_earnings.depreciation_and_amortization"
    ),
    ("adjusted_ebitda", "stock_based_compensation"): _FM_SBC_ADD_BACK,
    ("adjusted_ebitda", "other"): _FM_OTHER_ITEMS,
    ("non_gaap_operating_income", "stock_based_compensation"): _FM_SBC_ADD_BACK,
    ("non_gaap_operating_income", "acquired_intangible_amortization"): (
        _FM_ACQUIRED_INTANGIBLE_AMORTIZATION
    ),
    ("non_gaap_operating_income", "acquisition_related"): _FM_OTHER_ITEMS,
    ("non_gaap_operating_income", "restructuring"): _FM_OTHER_ITEMS,
    ("non_gaap_operating_income", "impairment"): _FM_OTHER_ITEMS,
    ("non_gaap_operating_income", "litigation"): _FM_OTHER_ITEMS,
    ("non_gaap_operating_income", "other"): _FM_OTHER_ITEMS,
    ("non_gaap_income_from_operations", "stock_based_compensation"): _FM_SBC_ADD_BACK,
    ("non_gaap_income_from_operations", "acquired_intangible_amortization"): (
        _FM_ACQUIRED_INTANGIBLE_AMORTIZATION
    ),
    ("non_gaap_income_from_operations", "acquisition_related"): _FM_OTHER_ITEMS,
    ("non_gaap_income_from_operations", "restructuring"): _FM_OTHER_ITEMS,
    ("non_gaap_income_from_operations", "impairment"): _FM_OTHER_ITEMS,
    ("non_gaap_income_from_operations", "litigation"): _FM_OTHER_ITEMS,
    ("non_gaap_income_from_operations", "other"): _FM_OTHER_ITEMS,
    ("non_gaap_net_income", "stock_based_compensation"): _FM_SBC_ADJUSTMENT,
    ("non_gaap_net_income", "acquired_intangible_amortization"): (
        _FM_ACQUIRED_INTANGIBLE_AMORTIZATION
    ),
    ("non_gaap_net_income", "acquisition_related"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "restructuring"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "impairment"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "litigation"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "interest"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "investment_gain_loss"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "other"): _FM_OTHER_ITEMS_ADJUSTMENT,
    ("non_gaap_net_income", "income_tax"): _FM_NON_GAAP_TAX_EFFECT,
}
# Which bridge a row-year two bridges both land on takes its value from.
_BRIDGE_METRIC_PRECEDENCE: tuple[str, ...] = tuple(
    dict.fromkeys(metric for metric, _ in _BRIDGE_ROW_BY_METRIC_ROLE)
)
# A statement-line fact lands on its schedule row by the producer's own tag and
# accounting definition: acquired-intangible amortization, which the filing
# presents under an issuer-specific role, and the operating-lease flow disclosures.
_FILING_SCHEDULE_ITEM_BY_DEFINITION = {
    ("us-gaap:AmortizationOfIntangibleAssets", "acquired_intangible_amortization"): (
        _FM_ACQUIRED_INTANGIBLE_AMORTIZATION
    ),
    ("us-gaap:OperatingLeasePayments", "lease_cash_paid"): _LEASE_PAYMENTS,
    ("us-gaap:OperatingLeasePaymentsUse", "lease_cash_paid"): _LEASE_PAYMENTS,
    (
        "us-gaap:RightOfUseAssetObtainedInExchangeForOperatingLeaseLiability",
        "rou_additions",
    ): _LEASE_ROU_ADDITIONS,
}
# SBC the filing allocates by income-statement line lands on that line's
# component lane; the member names the line (D24: disclosed, not spread).
# Through the 2025 taxonomy the line is an explicit member of
# IncomeStatementLocationAxis; the 2026 taxonomy files it on the typed
# StatementOfIncomeLocationBalanceAxis, whose value is the line item's concept.
_SBC_LOCATION_MEMBERS_BY_AXIS = {
    "us-gaap:IncomeStatementLocationAxis": (
        "us-gaap:CostOfSalesMember",
        "us-gaap:SellingAndMarketingExpenseMember",
        "us-gaap:ResearchAndDevelopmentExpenseMember",
        "us-gaap:GeneralAndAdministrativeExpenseMember",
    ),
    "us-gaap:StatementOfIncomeLocationBalanceAxis": (
        "us-gaap:CostOfGoodsAndServicesSold",
        "us-gaap:SellingAndMarketingExpense",
        "us-gaap:ResearchAndDevelopmentExpense",
        "us-gaap:GeneralAndAdministrativeExpense",
    ),
}
_FILING_SCHEDULE_ITEM_BY_DIMENSION: dict[tuple[str, tuple[tuple[str, str], ...]], str] = {
    (
        "us-gaap:AllocatedShareBasedCompensationExpense",
        ((axis, member),),
    ): component_id
    for axis, members in _SBC_LOCATION_MEMBERS_BY_AXIS.items()
    for member, component_id in zip(members, _SBC_COMPONENT_IDS, strict=True)
}


def _bridge_schedule_key(fact: HistoricalBasisFact) -> tuple[str, str] | None:
    """``(output_metric_id, role)`` of an issuer bridge component fact."""

    if fact.line.statement_role != "nongaapreconciliation" or fact.line.dimensions:
        return None
    return bridge_row_key(fact.line.tag.removeprefix("issuer:"))


def _filing_schedule_item_id(fact: HistoricalBasisFact) -> str | None:
    """The schedule row one retained statement-line filing fact lands on."""

    dimensions = tuple(
        (dimension.axis_qname, dimension.member_qname)
        for dimension in fact.line.dimensions
    )
    if dimensions:
        return _FILING_SCHEDULE_ITEM_BY_DIMENSION.get((fact.line.tag, dimensions))
    return _FILING_SCHEDULE_ITEM_BY_DEFINITION.get(
        (fact.line.tag, fact.accounting_definition)
    )


def _scaled_to(value: Decimal, *, source_scale: str, target_scale: str) -> Decimal:
    source = "unit" if source_scale == "units" else source_scale
    target = "unit" if target_scale == "units" else target_scale
    return value * _SCALE_FACTORS[source] / _SCALE_FACTORS[target]


def _filing_schedule_cells(
    index: Mapping[str, LineItem],
    periods: Iterable[int],
    historical_basis_facts: Iterable[HistoricalBasisFact],
) -> dict[tuple[str, int], tuple[Decimal, tuple[HistoricalBasisFact, ...]]]:
    """Each schedule row-year's retained filing value and the facts it sums.
    A statement-line fact wins its row-year over any bridge row. A bridge
    component lands as it contributes to its output: its value times the sign
    its output's binding carries for it. Bridge rows sharing a row-year sum
    within their bridge, and the earliest metric in
    ``_BRIDGE_ROW_BY_METRIC_ROLE`` wins between bridges.
    """

    admitted = frozenset(periods)
    facts = tuple(historical_basis_facts)
    component_signs = {
        fact_id: sign
        for fact in facts
        if fact.reconciliation is not None
        for fact_id, sign in zip(
            fact.reconciliation.component_fact_ids,
            fact.reconciliation.component_signs,
            strict=True,
        )
    }
    # Row-year → the facts landing on it, each with its bridge output metric
    # (``None`` for a statement-line fact), in first-seen order.
    cells: dict[
        tuple[str, int], list[tuple[str | None, Decimal, HistoricalBasisFact]]
    ] = {}
    for fact in facts:
        bridge_key = _bridge_schedule_key(fact)
        item_id = (
            _filing_schedule_item_id(fact)
            if bridge_key is None
            else _BRIDGE_ROW_BY_METRIC_ROLE.get(bridge_key)
        )
        digits = fact.period_id.removeprefix("FY")
        if item_id is None or item_id not in index or not digits.isdigit():
            continue
        period = int(digits)
        if period not in admitted:
            continue
        value = component_signs.get(fact.fact_id, 1) * _scaled_to(
            fact.value,
            source_scale=fact.scale,
            target_scale=index[item_id].model_scale,
        )
        cells.setdefault((item_id, period), []).append(
            (None if bridge_key is None else bridge_key[0], value, fact)
        )
    landed: dict[tuple[str, int], tuple[Decimal, tuple[HistoricalBasisFact, ...]]] = {}
    for cell, rows in cells.items():
        statement_rows = [row for row in rows if row[0] is None]
        if statement_rows:
            _, value, fact = statement_rows[-1]
            landed[cell] = (value, (fact,))
            continue
        metric = min(
            {name for name, _, _ in rows if name is not None},
            key=_BRIDGE_METRIC_PRECEDENCE.index,
        )
        chosen = [row for row in rows if row[0] == metric]
        landed[cell] = (
            sum((value for _, value, _ in chosen), Decimal(0)),
            tuple(fact for _, _, fact in chosen),
        )
    return landed

_BRIDGE_BASE_AND_OUTPUT: Mapping[str, tuple[str, str]] = {
    "adjusted_ebitda": (
        "tpl.fm.adjusted_earnings.adjusted_ebitda__net_income_base",
        "tpl.fm.adjusted_earnings.adjusted_ebitda",
    ),
    "non_gaap_net_income": (
        "tpl.fm.adjusted_earnings.net_income",
        "tpl.fm.adjusted_earnings.adjusted_net_income",
    ),
    "non_gaap_operating_income": (
        "tpl.fm.adjusted_earnings.operating_income_base",
        "tpl.fm.adjusted_earnings.adjusted_operating_income",
    ),
    "non_gaap_income_from_operations": (
        "tpl.fm.adjusted_earnings.operating_income_base",
        "tpl.fm.adjusted_earnings.adjusted_operating_income",
    ),
}
_BRIDGE_BASE_COUNTERPART: Mapping[str, str] = {
    "tpl.fm.adjusted_earnings.adjusted_ebitda__net_income_base": (
        "tpl.fm.adjusted_earnings.net_income_base"
    ),
    "tpl.fm.adjusted_earnings.operating_income_base": (
        "tpl.fm.income_statement.operating_income"
    ),
}
_BRIDGE_BASE_MIRRORS: Mapping[str, tuple[str, ...]] = {
    "adjusted_ebitda": ("tpl.a.adj_ebitda.net_income",),
}


def _land_historical_bridge_operands(
    model: FinancialModel,
    historical: tuple[int, ...],
    facts: tuple[HistoricalBasisFact, ...],
    landed: dict[tuple[str, int], tuple[Decimal, tuple[HistoricalBasisFact, ...]]],
    derived_cells: dict[tuple[str, int], tuple[Decimal, tuple[str, ...]]] | None = None,
) -> None:
    """Lower each bound bridge from its own vintage, preserving statement rows.

    Existing schedule rows remain the visible operands where their facts agree.
    Shared rows with a different statement/bridge definition get an isolated
    operand in this bridge. Only bound historical formulas are replaced.
    """
    if derived_cells is None:
        derived_cells = {}
    facts_by_id = {fact.fact_id: fact for fact in facts}
    pre_graph = DependencyGraph()
    pre_graph.build(model)
    pre_values = pre_graph.compute({})

    def _has_component_value(comp_id: str, yr: int) -> bool:
        if (comp_id, yr) in landed or (comp_id, yr) in derived_cells:
            return True
        if comp_id == _FM_OTHER_ITEMS_ADJUSTMENT and (
            (_FM_OTHER_ITEMS, yr) in landed or (_FM_OTHER_ITEMS, yr) in derived_cells
        ):
            return True
        comp_item = model._index.get(comp_id)
        if comp_item is None:
            return False
        if pre_values.get(comp_id, {}).get(yr) is not None:
            return True
        if comp_item.values is not None and yr in comp_item.values.values and comp_item.values.values[yr].value is not None:
            return True
        if comp_item.overrides is not None and yr in comp_item.overrides:
            spec = comp_item.overrides[yr]
            if spec.type is FormulaType.constant and isinstance(spec.params.get("value"), (int, float)):
                return True
        return False

    def _existing_component_value(comp_id: str, yr: int, target_scale: str) -> Decimal:
        if (comp_id, yr) in landed:
            return landed[(comp_id, yr)][0]
        if (comp_id, yr) in derived_cells:
            return derived_cells[(comp_id, yr)][0]
        if comp_id == _FM_OTHER_ITEMS_ADJUSTMENT and (_FM_OTHER_ITEMS, yr) in landed:
            return landed[(_FM_OTHER_ITEMS, yr)][0]
        if comp_id == _FM_OTHER_ITEMS_ADJUSTMENT and (_FM_OTHER_ITEMS, yr) in derived_cells:
            return derived_cells[(_FM_OTHER_ITEMS, yr)][0]
        comp_item = model._index.get(comp_id)
        if comp_item is None:
            return Decimal(0)
        raw_val = pre_values.get(comp_id, {}).get(yr)
        if raw_val is None and comp_item.values is not None and yr in comp_item.values.values:
            raw_val = comp_item.values.values[yr].value
        if raw_val is None and comp_item.overrides is not None and yr in comp_item.overrides:
            spec = comp_item.overrides[yr]
            if spec.type is FormulaType.constant and isinstance(spec.params.get("value"), (int, float)):
                raw_val = float(spec.params["value"])
        if raw_val is None:
            return Decimal(0)
        return _scaled_to(
            Decimal(str(raw_val)),
            source_scale=comp_item.model_scale,
            target_scale=target_scale,
        )
    for output in sorted(
        facts,
        key=lambda f: (
            f.reconciliation is not None
            and f.reconciliation.output_metric == "non_gaap_net_income"
        ),
    ):
        binding = output.reconciliation
        if binding is None or binding.output_metric not in _BRIDGE_BASE_AND_OUTPUT:
            continue
        base_id, output_id = _BRIDGE_BASE_AND_OUTPUT[binding.output_metric]
        period = int(output.period_id.removeprefix("FY"))
        if period not in historical or output_id not in model._index:
            continue
        output_item = model.get_item(output_id)
        metric = binding.output_metric
        projected_refs: dict[str, str] = {}

        def operand(
            item_id: str, rows: tuple[HistoricalBasisFact, ...], signs: tuple[int, ...]
        ) -> str:
            value = sum(
                (
                    sign * _scaled_to(
                        fact.value, source_scale=fact.scale,
                        target_scale=output_item.model_scale,
                    )
                    for fact, sign in zip(rows, signs, strict=True)
                ),
                Decimal(0),
            )
            counterpart_id = _BRIDGE_BASE_COUNTERPART.get(item_id, item_id)
            existing = landed.get((item_id, period))
            if existing is not None and existing != (value, rows):
                item_id = (
                    f"tpl.fm.adjusted_earnings.{metric}__"
                    f"{item_id.rsplit('.', 1)[-1]}"
                )
            if item_id not in model._index:
                item = _bind_or_insert_item(
                    model, item_id=item_id, anchor_item_id=output_id,
                    label=f"{metric.replace('_', ' ')}: {rows[0].line.tag}",
                    item_type=ItemType.input, unit=Unit.dollars,
                )
                item.model_scale = output_item.model_scale
                if counterpart_id in model._index and counterpart_id != item_id:
                    item.projected = FormulaSpec(
                        type=FormulaType.ref,
                        params={"source": LineItemRef(id=counterpart_id)},
                        note="standard forecast counterpart of the retained bridge operand",
                    )
                    item.formula_periods = list(model.time_structure.projection_periods)
                else:
                    zero_spec = _zero_projection_formula(
                        note="projected_non_gaap_other_items_exclusion",
                        strategy="projected_non_gaap_other_items_exclusion",
                    )
                    item.historical = zero_spec.model_copy(deep=True)
                    item.projected = zero_spec
                    item.formula_periods = [
                        *historical,
                        *model.time_structure.projection_periods,
                    ]
            if counterpart_id != item_id and counterpart_id in model._index:
                projected_refs[counterpart_id] = item_id
            landed[(item_id, period)] = (value, rows)
            return item_id

        base = operand(base_id, (facts_by_id[binding.starting_fact_id],), (1,))
        for mirror_id in _BRIDGE_BASE_MIRRORS.get(metric, ()):
            mirror = model._index.get(mirror_id)
            if mirror is not None:
                overrides = dict(mirror.overrides or {})
                overrides[period] = FormulaSpec(
                    type=FormulaType.ref, params={"source": LineItemRef(id=base)},
                    note="visible base of the same retained issuer bridge",
                )
                mirror.overrides = overrides
                if mirror.values is not None:
                    mirror.values.values.pop(period, None)
        stages = [(output_id, len(binding.component_fact_ids))]
        if binding.output_metric == "adjusted_ebitda" and binding.subtotal_fact_ids:
            if any(
                _bridge_schedule_key(facts_by_id[fact_id]) == (
                    "adjusted_ebitda", "depreciation_and_amortization"
                )
                for fact_id in binding.component_fact_ids[
                    : binding.subtotal_component_counts[0]
                ]
            ):
                stages.insert(0, (
                    "tpl.fm.adjusted_earnings.ebitda", binding.subtotal_component_counts[0]
                ))
        for stage_id, count in stages:
            groups: dict[str, list[tuple[HistoricalBasisFact, int]]] = {}
            for fact_id, sign in zip(
                binding.component_fact_ids[:count], binding.component_signs[:count],
                strict=True,
            ):
                fact = facts_by_id[fact_id]
                key = _bridge_schedule_key(fact)
                item_id = _BRIDGE_ROW_BY_METRIC_ROLE.get(
                    key, f"tpl.fm.adjusted_earnings.{fact.line.tag.removeprefix('issuer:')}"
                ) if key is not None else (
                    f"tpl.fm.adjusted_earnings.{binding.output_metric}__{fact.line.tag}"
                )
                groups.setdefault(item_id, []).append((fact, sign))
            refs = [LineItemRef(id=base)]
            for item_id, rows in groups.items():
                refs.append(LineItemRef(id=operand(
                    item_id, tuple(fact for fact, _ in rows),
                    tuple(sign for _, sign in rows),
                )))
            item = model.get_item(stage_id)
            condensed = (
                stage_id in (
                    "tpl.fm.adjusted_earnings.adjusted_operating_income",
                    "tpl.fm.adjusted_earnings.adjusted_net_income",
                )
                and set(groups) <= {
                    _FM_OTHER_ITEMS, _FM_OTHER_ITEMS_ADJUSTMENT, _FM_NON_GAAP_TAX_EFFECT
                }
            )
            explicit_tax: list[str] = []
            if condensed:
                if stage_id == "tpl.fm.adjusted_earnings.adjusted_operating_income":
                    other_target = refs[1].id
                    summary_val, summary_rows = landed[(other_target, period)]
                    already = (
                        _existing_component_value(_FM_SBC_ADD_BACK, period, output_item.model_scale)
                        + _existing_component_value(_FM_ACQUIRED_INTANGIBLE_AMORTIZATION, period, output_item.model_scale)
                    )
                    if already != 0:
                        landed.pop((other_target, period), None)
                        derived_cells[(other_target, period)] = (
                            summary_val - already,
                            (_FM_SBC_ADD_BACK, _FM_ACQUIRED_INTANGIBLE_AMORTIZATION),
                        )
                    else:
                        landed[(other_target, period)] = (summary_val, summary_rows)
                    refs = [
                        LineItemRef(id=base),
                        LineItemRef(id=_FM_SBC_ADD_BACK),
                        LineItemRef(id=other_target),
                        LineItemRef(id=_FM_ACQUIRED_INTANGIBLE_AMORTIZATION),
                    ]
                else:
                    if _FM_OTHER_ITEMS_ADJUSTMENT in groups:
                        adj_target = next(r.id for r in refs[1:] if r.id.endswith("other_items_adjustment"))
                        summary_val, summary_rows = landed[(adj_target, period)]
                        already = (
                            _existing_component_value(_FM_SBC_ADJUSTMENT, period, output_item.model_scale)
                            + _existing_component_value(_FM_ACQUIRED_INTANGIBLE_AMORTIZATION, period, output_item.model_scale)
                        )
                        explicit_tax = [r.id for r in refs[1:] if not r.id.endswith("other_items_adjustment")]
                        if already != 0:
                            landed.pop((adj_target, period), None)
                            derived_cells[(adj_target, period)] = (
                                summary_val - already,
                                (_FM_SBC_ADJUSTMENT, _FM_ACQUIRED_INTANGIBLE_AMORTIZATION),
                            )
                        else:
                            landed[(adj_target, period)] = (summary_val, summary_rows)
                        refs = [
                            LineItemRef(id=base),
                            LineItemRef(id=_FM_SBC_ADJUSTMENT),
                            LineItemRef(id=_FM_ACQUIRED_INTANGIBLE_AMORTIZATION),
                            LineItemRef(id=adj_target),
                            *([LineItemRef(id=explicit_tax[0])] if explicit_tax else []),
                        ]
                    else:
                        tax_target = refs[1].id
                        summary_val, summary_rows = landed[(tax_target, period)]
                        already = (
                            _existing_component_value(_FM_SBC_ADJUSTMENT, period, output_item.model_scale)
                            + _existing_component_value(_FM_ACQUIRED_INTANGIBLE_AMORTIZATION, period, output_item.model_scale)
                            + _existing_component_value(_FM_OTHER_ITEMS_ADJUSTMENT, period, output_item.model_scale)
                        )
                        if already != 0:
                            landed.pop((tax_target, period), None)
                            derived_cells[(tax_target, period)] = (
                                summary_val - already,
                                (
                                    _FM_SBC_ADJUSTMENT,
                                    _FM_ACQUIRED_INTANGIBLE_AMORTIZATION,
                                    _FM_OTHER_ITEMS_ADJUSTMENT,
                                ),
                            )
                        else:
                            landed[(tax_target, period)] = (summary_val, summary_rows)
                        refs = [
                            LineItemRef(id=base),
                            LineItemRef(id=_FM_SBC_ADJUSTMENT),
                            LineItemRef(id=_FM_ACQUIRED_INTANGIBLE_AMORTIZATION),
                            LineItemRef(id=_FM_OTHER_ITEMS_ADJUSTMENT),
                            LineItemRef(id=tax_target),
                        ]
            overrides = dict(item.overrides or {})
            overrides[period] = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="sum_range",
                params={"function": "SUM", "items": refs},
                note="same-vintage retained issuer bridge operands",
                derivation=(
                    FormulaDerivation(
                        owner="build_company_grounded_schedules",
                        strategy="cited_absence_omitted",
                        omitted_operands=((_FM_NON_GAAP_TAX_EFFECT, 0),),
                    )
                    if condensed
                    and stage_id == "tpl.fm.adjusted_earnings.adjusted_net_income"
                    and _FM_OTHER_ITEMS_ADJUSTMENT in groups
                    and not explicit_tax
                    else None
                ),
            )
            item.overrides = overrides
            if item.values is not None:
                item.values.values.pop(period, None)
            if (
                period == historical[-1]
                and not condensed
                and stage_id != "tpl.fm.adjusted_earnings.adjusted_ebitda"
                and item.projected is not None
                and {
                    _BRIDGE_BASE_COUNTERPART.get(ref.id, ref.id) for ref in refs
                } != {
                    _BRIDGE_BASE_COUNTERPART.get(ref_id, ref_id)
                    for ref_id in _formula_ref_ids(item.projected.params)
                }
            ):
                item.projected = FormulaSpec(
                    type=FormulaType.arithmetic,
                    subtype="sum_range",
                    params={
                        "function": "SUM",
                        "items": [LineItemRef(id=ref.id) for ref in refs],
                    },
                    note="same-vintage retained issuer bridge operands",
                )
            # Carry the same economic operands across the forecast seam through
            # transparent refs. Their forecasts still read the standard rows,
            # never the original-release historical constants.
            if item.projected is not None and projected_refs:
                item.projected = item.projected.model_copy(deep=True)

                def rewire(value: object) -> None:
                    if isinstance(value, dict):
                        for key, nested in value.items():
                            if isinstance(nested, LineItemRef) and nested.id in projected_refs:
                                value[key] = nested.model_copy(
                                    update={"id": projected_refs[nested.id]}
                                )
                            else:
                                rewire(nested)
                    elif isinstance(value, list):
                        for position, nested in enumerate(value):
                            if isinstance(nested, LineItemRef) and nested.id in projected_refs:
                                value[position] = nested.model_copy(
                                    update={"id": projected_refs[nested.id]}
                                )
                            else:
                                rewire(nested)

                rewire(item.projected.params)


def _apply_filing_schedule_disclosures(
    model: FinancialModel,
    historical: tuple[int, ...],
    historical_basis_facts: tuple[HistoricalBasisFact, ...],
) -> _FilingScheduleDisclosures:
    """Populate optional schedule inputs from their exact retained filing lines."""

    catalog = load_required_role_catalog()
    for relation in catalog.model_scale_relations:
        if relation.source_item in model._index:
            lease_scale = model._index[relation.source_item].model_scale
            for lease_id in relation.target_items:
                if lease_id in model._index:
                    model._index[lease_id].model_scale = lease_scale
    landed = _filing_schedule_cells(
        model._index, historical, historical_basis_facts
    )
    derived_cells: dict[tuple[str, int], tuple[Decimal, tuple[str, ...]]] = {}
    _land_historical_bridge_operands(
        model, historical, historical_basis_facts, landed, derived_cells
    )
    wired: set[str] = set()
    tax_effect_disclosed_periods: set[int] = set()
    sbc_add_back_disclosed_periods: set[int] = set()
    coverage: list[dict[str, object]] = []
    for (item_id, period), (value, facts) in landed.items():
        item = model.get_item(item_id)
        # The retained filing fact is this cell's one value: the renderer emits
        # the period override and the graph seeds the value, so both carry it
        # (a template historical constant otherwise wins in the workbook).
        _set_imported_value(
            item,
            period,
            float(value),
            provenance=ValueProvenance.imported_edgar,
        )
        _set_constant_override(
            item,
            period,
            float(value),
            origin=_derivation("filing_schedule_disclosure", ()),
        )
        wired.add(item.id)
        if item.id == _FM_NON_GAAP_TAX_EFFECT:
            tax_effect_disclosed_periods.add(period)
        if item.id == _FM_SBC_ADD_BACK:
            sbc_add_back_disclosed_periods.add(period)
        recipient_ids = [item.id]
        for companion_id, expected_src_id in catalog.disclosure_companions.get(item.id, ()):
            if expected_src_id not in recipient_ids or companion_id not in model._index:
                continue
            companion_item = model.get_item(companion_id)
            override = (companion_item.overrides or {}).get(period)
            if override is not None:
                companion_spec = override
            else:
                if (
                    companion_item.formula_periods is not None
                    and period not in companion_item.formula_periods
                ):
                    continue
                companion_spec = companion_item.historical
            if companion_spec is None or companion_spec.type is not FormulaType.ref:
                continue
            src_ref = (companion_spec.params or {}).get("source")
            src_id = (
                src_ref.id
                if isinstance(src_ref, LineItemRef)
                else src_ref.get("id")
                if isinstance(src_ref, dict)
                else None
            )
            src_period_offset = (
                src_ref.t
                if isinstance(src_ref, LineItemRef)
                else int(src_ref.get("t", 0))
                if isinstance(src_ref, dict)
                else 1
            )
            src_period_anchor = (
                src_ref.period_anchor
                if isinstance(src_ref, LineItemRef)
                else src_ref.get("period_anchor", "first")
                if isinstance(src_ref, dict)
                else None
            )
            if (
                src_id == expected_src_id
                and src_period_offset == 0
                and src_period_anchor in (None, "first")
            ):
                recipient_ids.append(companion_id)
        for recipient_id in recipient_ids:
            coverage.extend(
                {
                    "source_kind": "source_bundle_schedule_disclosure",
                    "resolved_item_id": recipient_id,
                    "fact_id": fact.fact_id,
                    "source_id": str(fact.source_id),
                    "periods": [period],
                }
                for fact in facts
            )
    for (item_id, period), (value, components) in derived_cells.items():
        item = model.get_item(item_id)
        _set_imported_value(
            item,
            period,
            float(value),
            provenance=ValueProvenance.derived,
        )
        _set_constant_override(
            item,
            period,
            float(value),
            origin=_derivation("historical_bridge_summary_residual", components),
        )
        wired.add(item.id)
    return {
        "wired": tuple(sorted(wired)),
        "source_fact_coverage": tuple(coverage),
        "tax_effect_disclosed_periods": frozenset(tax_effect_disclosed_periods),
        "sbc_add_back_disclosed_periods": frozenset(sbc_add_back_disclosed_periods),
    }


def _period_year(period_id: str) -> int:
    return int(period_id.removeprefix("FY"))


def _terminal_rows(role_dispositions: RoleDispositionTable) -> dict[str, RoleDisposition]:
    """Each role's disposition in the latest historical period the table carries.

    A projection period has no pin of its own, so every forecast decision reads
    this row; the pin's opening column is outside the table.
    """

    latest: dict[str, RoleDisposition] = {}
    for row in role_dispositions.rows:
        held = latest.get(row.required_role)
        if held is None or _period_year(row.period_id) > _period_year(held.period_id):
            latest[row.required_role] = row
    return latest


def _terminal_state(role_dispositions: RoleDispositionTable, role: str) -> RoleDispositionState:
    return _terminal_rows(role_dispositions)[role].state


def _role_lines(
    model: FinancialModel, role_items: Mapping[str, RoleItems], role: str,
) -> tuple[str, ...]:
    """The model lines a role's cells live on: its template, population and face lines.

    The role's HFF fact targets are shared landing rows, not the role's own cells.
    """

    lines = role_items[role]
    return tuple(
        item_id
        for item_id in dict.fromkeys(
            (*lines.template_items, *lines.population_items, *lines.face_items)
        )
        if item_id in model._index
    )


def _cited_terminal_lines(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    *states: RoleDispositionState,
) -> frozenset[str]:
    """The lines of every role whose terminal historical state is one of ``states``.

    A cited-inclusion marker the document persisted on a line that is no role's own
    (an alias of the role's face row, an expense base the role's amount is read
    from) is the same cited cell, so ``not_separately_presented`` also names the
    lines that carry the marker at the terminal period.
    """

    lines = {
        item_id
        for role, row in _terminal_rows(role_dispositions).items()
        if row.state in states
        for item_id in _role_lines(model, role_items, role)
    }
    if "not_separately_presented" in states:
        terminal = max(_historical_periods(model), default=None)
        lines.update(
            item.id
            for item in model._index.values()
            if terminal is not None
            and getattr(
                (item.overrides or {}).get(terminal), "subtype", None
            ) == "not_separately_presented"
        )
    return frozenset(lines)


def _cited_absence_cells(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
) -> frozenset[tuple[str, int]]:
    """The ``(item_id, year)`` cells the pin cites ``not_applicable``."""

    return frozenset(
        (item_id, _period_year(period_id))
        for role, period_id in role_dispositions.cells_in("not_applicable")
        for item_id in _role_lines(model, role_items, role)
    )


def _no_live_claimant(
    item_id: str,
    role_items: Mapping[str, RoleItems],
    terminal_rows: Mapping[str, RoleDisposition],
) -> bool:
    """Whether every role that claims ``item_id`` is absent or unowed at the terminal period."""

    return all(
        terminal_rows[claimant].state in ("not_applicable", "not_separately_presented", "unowed")
        for claimant, claimed in role_items.items()
        if item_id in claimed.all_items()
    )


def _retire_unowed_roles(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> None:
    """Step 7: an unowed role's lines leave the model, every period, through the graph owner.

    ``DependencyGraph.retire`` clears each cell and rewrites every reader with
    ``FormulaSpec.without``; a reader left with no formula retires in turn. A
    face balance row all of whose cells are gone then leaves the sheet with its
    cash-flow leg and schedule (``_remove_unowed_face_row``).
    """

    unowed = sorted({role for role, _period_id in role_dispositions.cells_in("unowed")})
    if not unowed:
        return
    terminal_rows = _terminal_rows(role_dispositions)
    graph = DependencyGraph()
    graph.build(model)
    for role in unowed:
        for item_id in role_items[role].all_items():
            if item_id not in model._index or not _no_live_claimant(item_id, role_items, terminal_rows):
                continue
            for period in (*historical, *projected):
                graph.retire(item_id, period)
    for role in unowed:
        lines = role_items[role]
        for item_id in dict.fromkeys((*lines.template_items, *lines.face_items)):
            item = model._index.get(item_id)
            if (
                item is not None
                and item_id.startswith((_FACE_BALANCE_SHEET_PREFIX, _FACE_INCOME_STATEMENT_PREFIX))
                and item.formula_periods == []
            ):
                _remove_unowed_face_row(model, item_id)


def _retire_absent_role_schedules(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> None:
    """Step 7b: a statement role the filing cites has no schedule.

    A role whose terminal state is ``not_applicable`` keeps only its cited face
    cells: every other line of the role (population and schedule rows) retires
    through the graph owner in each forecast period and in each historical period
    cited absent, so no schedule row is left owning cells it never wrote. A
    ``not_separately_presented`` role keeps what the inclusion lanes wrote; a
    schedule cell of the role that owns a period and holds no formula, override or
    value retires. A role an author holds a forecast on keeps its lines
    (``resolve_build_dispositions`` already keeps such a role from ``unowed``).
    """

    catalog = load_required_role_catalog()
    rows = {(row.required_role, row.period_id): row for row in role_dispositions.rows}
    historical_set = frozenset(historical)
    terminal_rows = _terminal_rows(role_dispositions)
    graph = DependencyGraph()
    graph.build(model)
    computed = graph.compute({})
    for role, terminal in terminal_rows.items():
        if terminal.state not in ("not_applicable", "not_separately_presented"):
            continue
        if catalog.role(role).statement not in ("balance_sheet", "cash_flow", "income_statement"):
            continue
        lines = role_items[role]
        # A role's non-face schedule row (``population_items`` or ``hff_fact_items``) is its
        # own schedule row when no role that claims it is owed: a shared landing row stays
        # with its live role.
        landing = tuple(
            item_id
            for item_id in lines.hff_fact_items
            if item_id in model._index
            and not item_id.startswith("tpl.fm.")
            and _no_live_claimant(item_id, role_items, terminal_rows)
        )
        owned = tuple(
            item_id
            for item_id in dict.fromkeys((*_role_lines(model, role_items, role), *landing))
            if item_id.startswith("tpl.fm.") or _no_live_claimant(item_id, role_items, terminal_rows)
        )
        if any(_authored_periods(model.get_item(item_id), projected) for item_id in owned):
            continue
        face = {
            item_id
            for item_id in (*lines.template_items, *lines.face_items)
            if item_id.startswith("tpl.fm.")
        }
        for item_id in owned:
            if item_id in face:
                continue
            item = model.get_item(item_id)
            for period in (*historical, *projected):
                if terminal.state == "not_applicable":
                    cell = rows.get((role, f"FY{period}"))
                    retires = period not in historical_set or (
                        cell is not None and cell.state == "not_applicable"
                    )
                else:
                    retires = _owns_cell_without_formula(item, period, period in historical_set)
                # A landing row a writer projects as a number (the debt schedule) is that
                # number in the forecast; only a cell with no amount leaves.
                kept_number = (
                    item_id in landing
                    and period not in historical_set
                    and computed.get(item_id, {}).get(period) is not None
                )
                if retires and not kept_number:
                    graph.retire(item_id, period)
        if terminal.state == "not_applicable":
            citation = cast(AbsenceCitation, terminal.absence_citation).model_dump(mode="json")
            for item_id in sorted(face & model._index.keys()):
                item = model.get_item(item_id)
                if (
                    (
                        item_id.startswith(_FACE_BALANCE_SHEET_PREFIX)
                        or (item_id.startswith("tpl.fm.cash_flow.") and item.projected is None)
                        or (item_id.startswith(_FACE_INCOME_STATEMENT_PREFIX) and item.projected is None)
                    )
                    and item.historical is None
                    and not any(
                        (
                            (override := (item.overrides or {}).get(period)) is not None
                            and override.subtype not in ("not_applicable", "not_separately_presented")
                        )
                        or (
                            item.values is not None
                            and (cell := item.values.values.get(period)) is not None
                            and cell.value is not None
                        )
                        for period in historical
                    )
                ):
                    # The face cell stays cited in the forecast: the marker is the
                    # citation, never a number, and the tax bridge reads it as absent.
                    # An income-statement or cash-flow flow with no forecast gets
                    # the marker too, rather than an input with no provenance.
                    item.projected = FormulaSpec(
                        type=FormulaType.raw,
                        subtype="not_applicable",
                        params={
                            "formula": "=NA()",
                            "absence_citation": citation,
                            "required_role": role,
                        },
                    )
                    _set_periods(item, projected)


def _retire_unclaimed_face_slots(
    model: FinancialModel,
    business_model_revision: BusinessModelRevision,
    compiled_registry: CompiledDriverRegistry,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> None:
    """Step 7c: a role-less face slot nothing claims has no cells.

    A renamed template slot that no role presents, no balance node declares and
    no filed value or authored forecast reaches is a slot with no line behind it
    (it carries no disposition, so ``role_dispositions`` cannot name it). Its
    cells retire through the graph owner in every period, each total that summed
    it loses the term, and the row leaves the sheet.
    """
    model.build_index()

    values: ComputedValues | None = None

    def values_before_removals() -> ComputedValues:
        """The model's values at the first candidate slot: every removal below follows a
        candidate, so they are the values of the model as it entered, and a model with no
        candidate is never evaluated."""
        nonlocal values
        if values is None:
            graph = DependencyGraph()
            graph.build(model)
            values = graph.compute({})
        return values

    catalog = load_required_role_catalog()
    declared = {
        target
        for role in _declared_balance_nodes(business_model_revision, compiled_registry)
        if (target := catalog.balance(role).target_item) is not None
    }
    for item in list(model._index.values()):
        if (
            item.id.startswith(_FACE_BALANCE_SHEET_PREFIX)
            and item.required_role is None
            and item.customization is CustomizationType.rename
            and item.id not in declared
            and catalog.presenting_role(item.id) is None
            and not _has_known_historical_value(values_before_removals(), item_id=item.id, periods=historical)
            and not _forecast_reads_authored_control(model, item, projected, nonzero_only=True)
        ):
            _remove_unowed_face_row(model, item.id)
    # A wholly unreported bridge component has no historical cells, not a series of
    # unknown inputs: its separately authored forecast still owns its projected cells.
    model.build_index()
    graph: DependencyGraph | None = None
    for bridge_id in (_FM_OTHER_ITEMS, _FM_OTHER_ITEMS_ADJUSTMENT):
        bridge = model._index.get(bridge_id)
        if (
            bridge is not None
            and bridge.historical is None
            and not any(
                (bridge.overrides or {}).get(period) is not None
                or (bridge.values is not None and period in bridge.values.values)
                for period in historical
            )
        ):
            if graph is None:
                graph = DependencyGraph()
                graph.build(model)
            for period in historical:
                graph.retire(bridge_id, period)
    payable = model._index.get(_DIVIDEND_PAYABLE)
    if (
        payable is not None
        and payable.historical is None
        and payable.projected is None
        and not any(
            (payable.overrides or {}).get(period) is not None
            or (payable.values is not None and period in payable.values.values)
            for period in (*historical, *projected)
        )
        and not any(
            reader.id != _DIVIDEND_PAYABLE
            and any(
                spec is not None and _DIVIDEND_PAYABLE in _formula_ref_ids(spec.params)
                for spec in (reader.historical, reader.projected, *(reader.overrides or {}).values())
            )
            for reader in model._index.values()
        )
    ):
        if graph is None:
            graph = DependencyGraph()
            graph.build(model)
        for period in (*historical, *projected):
            graph.retire(_DIVIDEND_PAYABLE, period)

_SGA_HOST_NOTE = "combined SG&A forecast: terminal SG&A share of revenue"
_SGA_HOST_RATE_ITEM_ID = "tpl.a.operating_leverage.selling_general_and_administrative_pct_revenue"
_SGA_ROLE = "statement.is.selling_general_and_administrative"
_SGA_CHILD_ROLES = ("statement.is.sales_and_marketing", "statement.is.general_and_administrative")


def _combined_sga_filer(model: FinancialModel, role_dispositions: RoleDispositionTable) -> bool:
    """The SG&A role is bound at the terminal period and every functional child is cited there."""

    terminal = _terminal_rows(role_dispositions)
    return (
        all(role in terminal for role in (_SGA_ROLE, *_SGA_CHILD_ROLES))
        and terminal[_SGA_ROLE].state == "bound"
        and all(
            terminal[role].state in ("not_applicable", "not_separately_presented")
            for role in _SGA_CHILD_ROLES
        )
        and SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID in model._index
    )


def _install_sga_host_forecast(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> list[str]:
    """Step 7d: a combined-SG&A filer forecasts the SG&A line, not its cited children.

    Applied default (SGA-DESTINATION-AND-OPTIONAL-PARTICIPATION is silent on the forecast;
    revert site: this function and the host substitution in ``_lower_cited_operands``): when
    the SG&A role is bound at the terminal period, every functional child is
    cited ``not_separately_presented`` or ``not_applicable`` there, and no child carries an
    authored forecast, the SG&A row projects as revenue times the house ratio on its
    ``pct_revenue`` row (an authorable control), and its forecast readers read it in place of
    the children (``_lower_cited_operands``).
    """

    if (
        not _combined_sga_filer(model, role_dispositions)
        or any(
            _forecast_reads_authored_control(model, model.get_item(item_id), projected)
            for role in _SGA_CHILD_ROLES
            for item_id in _role_lines(model, role_items, role)
        )
    ):
        return []
    graph = DependencyGraph()
    graph.build(model)
    ratio = _latest_ratio(
        graph.compute({}),
        numerator_id=SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
        denominator_id=_REVENUE,
        periods=historical,
    )
    if ratio is None:
        return []
    return _install_default_revenue_ratio(
        model,
        target_id=SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
        rate_id=_SGA_HOST_RATE_ITEM_ID,
        anchor_id=SGA_DRIVER_BASE_ITEM_ID,
        label="  SG&A as % of revenue",
        ratio=ratio,
        projected=projected,
        note=_SGA_HOST_NOTE,
        strategy="latest_historical_revenue_ratio",
    )


def _retire_undefined_historical_ratios(
    model: FinancialModel, historical: tuple[int, ...],
) -> None:
    """Step 9: a historical ratio over a known zero is undefined, so its cell is absent.

    An undefined ratio is never 0 (JD-a): each historical cell whose effective spec has a
    ``denominator`` that resolves to a known 0 retires through the graph owner, with every
    reader that has no other formula. A cell holding a filed value keeps it.
    """

    graph = DependencyGraph()
    graph.build(model)
    values = graph.compute({})
    for item in list(model._index.values()):
        for period in historical:
            held = None if item.values is None else item.values.values.get(period)
            if held is not None and held.value is not None:
                continue
            spec = graph._spec_for_period(item, period)
            if spec is None or spec.type is FormulaType.constant:
                continue
            for ref in spec.references():
                if ref.role != "denominator":
                    continue
                target = graph.resolve_ref_period(ref.ref, period)
                if target is not None and values.get(ref.ref.id, {}).get(target) == 0.0:
                    graph.retire(item.id, period)
                    break


def _owns_cell_without_formula(item: LineItem, period: int, is_historical: bool) -> bool:
    """Whether ``item`` owns ``period`` yet holds no override, stored value or formula there."""

    if item.overrides and period in item.overrides:
        return False
    stored = None if item.values is None else item.values.values.get(period)
    if stored is not None and stored.value is not None:
        return False
    if item.formula_periods is not None and period not in item.formula_periods:
        return False
    return (item.historical if is_historical else item.projected) is None


def _materialize_bound_population_lines(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    values: ComputedValues | None = None,
    historical: tuple[int, ...] = (),
) -> list[str]:
    """Give a bound balance-sheet role the template has no line for its line, summed by its parent total.

    The catalog names the total a balance-sheet role rolls into (``parent_total``) and,
    for a role with no template face line, the population line its history lands on
    (``population_items``). A total the filer does not print (its role is not bound) is
    the sum of its lines, so a bound amount with no line sits in no total and leaves the
    historical balance sheet out of balance by exactly that amount. When the total is
    printed (bound), a role pinned on that total's own statement role (with no dimensions)
    is a distinct face component of the total when the total's existing members do not
    already sum to the printed terminal total (e.g. ``statement.bs.deferred_costs_current``
    under ``total_current_assets``); when the existing members already sum to the printed
    terminal total (e.g. ``HD``'s ``statement.bs.tax_liabilities_current`` summed inside a
    composite ``accrued_expenses``) or the role is bound from a disclosure role or
    dimensional breakdown, no line is added. The line goes in after the last member the
    total sums, joins both lanes of the total, and holds its prior balance in the forecast
    like every optional member. History lands on it through the historical refill of the
    roles this helper lined.
    """

    from .build_semantic_rows import _add_formula_ref_after, _insert_line_item_after

    catalog = load_required_role_catalog()
    ticker = str(model.company.ticker)
    terminal = _terminal_rows(role_dispositions)
    historical_years = historical or _historical_periods(model)
    terminal_year = max(historical_years) if historical_years else None
    projected = _projection_periods(model)
    computed = values
    lined: list[str] = []
    for role in catalog.roles_on_statement("balance_sheet"):
        line_ids = catalog.population_item_ids(role.required_role, ticker=ticker)
        disposition = terminal.get(role.required_role)
        if (
            role.parent_total is None
            or role.parent_total not in model._index
            or face_lines(role)
            or not line_ids
            or disposition is None
            or disposition.state != "bound"
            or any(line_id in model._index for line_id in line_ids)
        ):
            continue
        total_role = catalog.presenting_role(role.parent_total)
        if total_role is not None and terminal[total_role.required_role].state == "bound":
            total_ref = terminal[total_role.required_role].pinned_ref
            role_ref = disposition.pinned_ref
            if (
                terminal_year is None
                or total_ref is None
                or role_ref is None
                or role_ref.statement_role != total_ref.statement_role
                or bool(role_ref.dimensions)
            ):
                continue
            if computed is None:
                graph = DependencyGraph()
                graph.build(model)
                computed = graph.compute({})
            reported_total = computed.get(role.parent_total, {}).get(terminal_year)
            total_item = model.get_item(role.parent_total)
            spec = total_item.projected or total_item.historical
            if reported_total is None or spec is None:
                continue
            member_sum = sum(
                float(computed.get(member_id, {}).get(terminal_year) or 0.0)
                for member_id in _same_period_member_ids(spec)
            )
            if math.isclose(float(reported_total), member_sum, rel_tol=1e-9, abs_tol=1e-6):
                continue
        # After the last member the total sums, by sheet row: a leaf under a liability
        # or equity total, the equity total itself under the balance-sheet total (a
        # mezzanine role such as redeemable noncontrolling interest sums there).
        anchor = max(
            (
                model._index[member_id]
                for member_id in balance_sheet_total_members(role.parent_total)
                if member_id in model._index
            ),
            key=lambda member: int(member.row),
        )
        line = _insert_line_item_after(
            model,
            anchor_item_id=anchor.id,
            new_item=LineItem(
                id=line_ids[0],
                label=role.display_name,
                row=0,
                item_type=ItemType.input,
                unit=anchor.unit,
                model_scale=anchor.model_scale,
                style=None if anchor.style is None else anchor.style.model_copy(deep=True),
                required_role=role.required_role,
            ),
        )
        if not _add_formula_ref_after(
            model, formula_item_id=role.parent_total, after_id=anchor.id, new_id=line.id
        ):
            raise ValueError(
                f"{role.parent_total} does not sum {anchor.id}: {line.id} cannot join it"
            )
        line.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="carry_forward",
            params={"source": LineItemRef(id=line.id, t=-1)},
            note=OPTIONAL_SLOT_CARRY_FORWARD_NOTE,
        )
        line.formula_periods = list(projected)
        lined.append(line.id)
    return lined


def _lower_cited_operands(
    model: FinancialModel,
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
    historical: tuple[int, ...],
    projected: tuple[int, ...],
) -> list[str]:
    """Step 8: a cited role's line leaves the additive readers that must not sum it.

    A cell cited ``not_separately_presented`` leaves a reader that also reads the
    line it is included in (the amount is already a term). A cell cited
    ``not_applicable`` leaves where rule (d) admits it (``_is_optional_component``,
    income-statement and cash-flow components, unless an author holds a nonzero
    forecast) and on a balance-sheet role, whose cell stays cited rather than
    becoming a number. A projection period reads the role's terminal state. The
    removal is ``FormulaSpec.without`` over the reader's own formula; a reader left
    with nothing keeps its operands (a sum of nothing is owed, never zero). A
    cited balance's cash-flow movement leg leaves the forecast through the graph
    owner.

    A statement total is no cited cell. Its line is the sum of its members, so a filer
    that prints no total (the role cited ``not_applicable``) still has the derived sum,
    and the readers of the total keep reading it.
    """

    catalog = load_required_role_catalog()
    # An investment flow (one that moves the cash bridge) the filing cites absent has no
    # amount to owe: its absence leaves the additive readers like an optional component's.
    optional_roles = {
        role.required_role for role in catalog.required_roles
        if _is_optional_component(role)
        or (
            role.statement == "cash_flow"
            and any(item in _CASH_BRIDGE_FLOW_ITEM_IDS for item in role.template_items)
        )
    }
    balance_roles = {
        role.required_role for role in catalog.roles_on_statement("balance_sheet")
    }
    statement_totals = frozenset(catalog.statement_totals())
    rows = {(row.required_role, row.period_id): row for row in role_dispositions.rows}
    terminal = _terminal_rows(role_dispositions)
    projection = frozenset(projected)
    lines = {role: _role_lines(model, role_items, role) for role in role_items}
    # A cited role's filing-fact (landing) row is its own cell when no live role claims it:
    # readers that sum it leave it with the role's face.
    cited_lines = {
        role: tuple(
            item_id
            for item_id in dict.fromkeys((*role_lines, *role_items[role].hff_fact_items))
            if item_id in model._index
            and (item_id.startswith("tpl.fm.") or _no_live_claimant(item_id, role_items, terminal))
        )
        for role, role_lines in lines.items()
    }
    # A cited-absent balance a writer projects as a number (the debt schedule) is that number.
    balance_lines = {item_id for role in balance_roles for item_id in lines.get(role, ())}
    wired = _route_combined_common_stock_credit(
        model, historical, projected, role_dispositions,
    )
    equity_credit_readers = frozenset(wired)

    role_lines_all = {item_id for role_lines in lines.values() for item_id in role_lines}
    marker_rows: dict[str, RoleDisposition] = {}
    last = max(historical, default=None)
    for item in model._index.values():
        marker = None if last is None else (item.overrides or {}).get(last)
        if (
            item.id not in role_lines_all
            and marker is not None
            and marker.subtype == "not_separately_presented"
            and marker.params.get("absence_citation")
            and isinstance(marker.params.get("included_in"), str)
            and marker.params["included_in"] in role_items
        ):
            marker_rows[item.id] = RoleDisposition(
                required_role=marker.params["included_in"],
                period_id=f"FY{last}",
                state="not_separately_presented",
                absence_citation=AbsenceCitation.model_validate(marker.params["absence_citation"]),
                included_in=marker.params["included_in"],
            )

    graph = DependencyGraph()
    graph.build(model)
    computed = graph.compute({})

    def cited_in(period: int) -> dict[str, RoleDisposition]:
        out: dict[str, RoleDisposition] = {}
        if period in projection:
            # A cited inclusion the document persisted on a line no role owns (an
            # assumption row of a cash-flow aggregate) is the same cited cell.
            for item_id, marker_row in marker_rows.items():
                out.setdefault(item_id, marker_row)
        for role, role_lines in cited_lines.items():
            row = terminal[role] if period in projection else rows.get((role, f"FY{period}"))
            zero_debt_short_term_absent = (
                period not in projection
                and role == _SHORT_TERM_DEBT_ROLE
                and row is not None
                and row.not_disclosed_in_vintage
                and any(
                    _is_zero_value(computed.get(item_id, {}).get(period))
                    for item_id in lines.get(_LONG_TERM_DEBT_ROLE, ())
                )
            )
            if row is None or (
                row.state not in ("not_applicable", "not_separately_presented")
                and not zero_debt_short_term_absent
            ):
                continue
            if row.state == "not_applicable" and not (
                role in optional_roles or role in balance_roles
            ):
                continue
            for item_id in role_lines:
                if item_id in statement_totals:
                    continue
                # An author holds a forecast only where it yields an amount: a rate on a
                # cited-absent base (interest on no debt) computes to nothing.
                if (
                    period in projection
                    and row.state == "not_applicable"
                    and role in optional_roles
                    and computed.get(item_id, {}).get(period) is not None
                    and _forecast_reads_authored_control(
                        model, model.get_item(item_id), projected, nonzero_only=True
                    )
                ):
                    continue
                out.setdefault(item_id, row)
        return out

    cited_by_period = {period: cited_in(period) for period in (*historical, *projected)}
    mirrors: dict[str, set[str]] = {}
    for item in model._index.values():
        for lane in (item.historical, item.projected):
            if lane is not None and lane.type is FormulaType.ref:
                for source_id in _formula_ref_ids(lane.params):
                    mirrors.setdefault(item.id, set()).add(source_id)
                    mirrors.setdefault(source_id, set()).add(item.id)

    def _is_or_mirrors(item_id: str, role_lines: Collection[str]) -> bool:
        """Whether ``item_id`` is one of ``role_lines`` or a plain reference to or from one:
        the schedule row that presents the line, so its sum is the line's own."""
        return item_id in role_lines or bool(mirrors.get(item_id, set()) & set(role_lines))

    sheet_of = {
        line.id: (sheet_name, index)
        for sheet_name, sheet in model.sheets.items()
        for index, section in enumerate(sheet.sections)
        for line in section.line_items
    }

    def _is_not_numeric(item_id: str, period: int) -> bool:
        """Whether the cell carries no amount: a cited NSP cell computes to nothing, and a
        cell a writer projected numerically is its own amount, never already a term of its host."""
        return computed.get(item_id, {}).get(period) is None

    def _holds_marker(item_id: str, period: int) -> bool:
        """Whether the cell is itself the cited marker, not a number a writer projected."""
        spec = _period_formula(model, model.get_item(item_id), period)
        return spec is not None and spec.subtype == "not_separately_presented"

    # A plain-ref mirror (through any chain of them) of a cited cell is cited for its own
    # readers in every period, historical included: it joins the cited cells of the period.
    # An NSP mirror also carries the marker in a forecast period, so it renders cited and
    # owns its cell instead of dangling a ref to a cell with no amount.
    for period in projected:
        for item in model._index.values():
            if item.id in cited_by_period[period]:
                continue
            marker = _period_formula(model, item, period)
            if (
                marker is not None
                and marker.subtype == "not_separately_presented"
                and marker.params.get("absence_citation")
                and marker.params.get("included_in") in role_items
            ):
                # A forecast marker a writer put on a line no role owns is a cited cell.
                cited_by_period[period][item.id] = RoleDisposition(
                    required_role=marker.params["included_in"],
                    period_id=f"FY{period}",
                    state="not_separately_presented",
                    absence_citation=AbsenceCitation.model_validate(marker.params["absence_citation"]),
                    included_in=marker.params["included_in"],
                )
    for period in (*historical, *projected):
        cited = cited_by_period[period]
        settled: set[str] = set()
        grew = True
        while grew:
            grew = False
            for item in model._index.values():
                if (
                    item.id in settled
                    or not _is_not_numeric(item.id, period)
                    or (item.id in cited and _holds_marker(item.id, period))
                ):
                    continue
                override = (item.overrides or {}).get(period)
                if override is not None and override.type is FormulaType.constant:
                    continue
                spec = _period_formula(model, item, period)
                if spec is None or spec.type is not FormulaType.ref or spec.params.get("adjustment"):
                    continue
                sources = [ref for ref in spec.references() if ref.role == "source"]
                if len(sources) != 1:
                    continue
                row = cited.get(sources[0].ref.id)
                if (
                    row is None
                    or graph.resolve_ref_period(sources[0].ref, period) != period
                ):
                    continue
                cited[item.id] = row
                settled.add(item.id)
                grew = True
                if period in projection and row.state == "not_separately_presented":
                    item.overrides = {
                        **(item.overrides or {}),
                        period: not_separately_presented_formula(
                            cast(str, row.included_in),
                            cast(AbsenceCitation, row.absence_citation).model_dump(mode="json"),
                            original_override=override,
                        ),
                    }
                    wired.append(item.id)

    for period in (*historical, *projected):
        for reader in model._index.values():
            if reader.id in cited_by_period[period]:
                continue
            override = (reader.overrides or {}).get(period)
            stored = None if reader.values is None else reader.values.values.get(period)
            if (override is not None and override.type is FormulaType.constant) or (
                stored is not None and stored.value is not None
            ):
                continue
            if (
                reader.formula_periods is not None
                and period not in reader.formula_periods
                and override is None
            ):
                continue
            spec = _period_formula(model, reader, period)
            if spec is None:
                continue
            # Each sum term is judged at the cell it resolves to: a lagged read
            # (an opening balance summing the prior period) reads the prior
            # period's cited cell, not this period's.
            terms = [
                (formula_ref, target_period)
                for formula_ref in spec.references()
                if formula_ref.role in ("additive", "subtractive", "minuend")
                and (target_period := graph.resolve_ref_period(formula_ref.ref, period))
                is not None
            ]
            read_cells = {(formula_ref.ref.id, target) for formula_ref, target in terms}
            def authored_sibling(row: RoleDisposition, item_id: str, target: int) -> bool:
                """A forecast reader whose other term for the same host is an authored forecast.

                The author's forecast of that sibling is the line's forecast for the host
                (a combined line sits in the sibling's row), so this child adds nothing.
                """
                if (
                    period not in projection
                    or row.state != "not_separately_presented"
                    or _forecast_reads_authored_control(model, model.get_item(item_id), projected)
                ):
                    return False
                return any(
                    other_item != item_id
                    and (other_item, target) in read_cells
                    and _forecast_reads_authored_control(
                        model, model.get_item(other_item), projected
                    )
                    for other_role, other_row in terminal.items()
                    if other_role in _SGA_CHILD_ROLES
                    and other_row.state in ("not_applicable", "not_separately_presented")
                    and other_row.included_in == row.included_in
                    for other_item in lines.get(other_role, ())
                    if other_item in model._index
                )

            def forecast_host(row: RoleDisposition, item_id: str, target: int) -> str | None:
                """The line a cell with no amount is included in, where this reader reads none.

                The child's amount lives in that line, so the reader reads it instead.
                """
                if (
                    period not in projection
                    or row.state != "not_separately_presented"
                    or not (
                        _is_not_numeric(item_id, target)
                        or _forecast_reads_authored_control(model, model.get_item(item_id), projected)
                    )
                ):
                    return None
                host_role = catalog._row_or_none(cast(str, row.included_in))
                if host_role is None or host_role.statement != "income_statement":
                    return None
                host = next(
                    (
                        line for line in lines.get(cast(str, row.included_in), ())
                        if line in model._index and line != reader.id
                        and line not in cited_by_period.get(target, {})
                        # Only an additive peer of the child (its own sheet) is a term to read.
                        and sheet_of.get(line) == sheet_of.get(item_id)
                    ),
                    None,
                )
                return None if host is None or (host, target) in read_cells else host

            def host_elsewhere(row: RoleDisposition, item_id: str) -> bool:
                """The line the child is included in is no peer a reader can read in its place.

                The aggregate's counterpart is unmodeled on both sides (a cash-flow
                aggregate under an equity roll), so the term is simply gone.
                """
                host_role = catalog._row_or_none(cast(str, row.included_in))
                return (
                    period in projection
                    and row.state == "not_separately_presented"
                    and _is_not_numeric(item_id, period)
                    # Only an income-statement host is read in the child's place.
                    and (host_role is None or host_role.statement != "income_statement")
                )

            dropped = {
                (formula_ref.ref.id, formula_ref.ref.t)
                for formula_ref, target in terms
                if formula_ref.role in ("additive", "subtractive", "minuend")
                and (row := cited_by_period.get(target, {}).get(formula_ref.ref.id)) is not None
                and not (
                    period in projection
                    and reader.id in equity_credit_readers
                    and formula_ref.ref.id in (_CASH_FLOW_SBC, _EMPLOYEE_PLAN_PROCEEDS)
                )
                and (
                    (
                        row.state == "not_applicable"
                        or (row.not_disclosed_in_vintage and reader.id not in statement_totals)
                    )
                    and not (
                        period in projection
                        and not _is_not_numeric(formula_ref.ref.id, target)
                        and (
                            formula_ref.ref.id in balance_lines
                            or computed[formula_ref.ref.id][target] != 0.0
                        )
                    )
                    # An included child leaves only as an additive term: a subtracted
                    # child sits in a residual, which needs it.
                    or formula_ref.role == "additive"
                    and (
                        (
                            _is_or_mirrors(reader.id, lines.get(cast(str, row.included_in), ()))
                            and _holds_marker(formula_ref.ref.id, target)
                        )
                        or (
                            _is_not_numeric(formula_ref.ref.id, target)
                            and any(
                                (line, target) in read_cells
                                for line in lines.get(cast(str, row.included_in), ())
                            )
                        )
                        or forecast_host(row, formula_ref.ref.id, target) is not None
                        or host_elsewhere(row, formula_ref.ref.id)
                        or authored_sibling(row, formula_ref.ref.id, target)
                    )
                )
            }
            if not dropped:
                continue
            # A forecast reader of an NSP functional child reads the line the child is
            # included in instead, once, where that line carries the forecast.
            hosts: dict[tuple[str | int, ...], str] = {}
            for formula_ref, target in terms:
                row = cited_by_period.get(target, {}).get(formula_ref.ref.id)
                host = None if row is None else forecast_host(row, formula_ref.ref.id, target)
                if (
                    host is not None
                    and (formula_ref.ref.id, formula_ref.ref.t) in dropped
                    and (host, target) not in read_cells
                    and host not in hosts.values()
                ):
                    hosts[formula_ref.path] = host
            if hosts:
                params = copy.deepcopy(spec.params)
                for path, host in hosts.items():
                    holder: Any = params
                    for key in path[:-1]:
                        holder = holder[key]
                    holder[path[-1]] = LineItemRef(id=host)
                spec = spec.model_copy(update={"params": params})
            # The formulas saved for restore lose the same terms, or a later refresh
            # would put the cited operand back.
            lowered = _without_slot(
                spec, lambda formula_ref: (formula_ref.ref.id, formula_ref.ref.t) in dropped
            )
            if lowered is None or lowered.params == spec.params:
                continue
            if (reader.overrides or {}).get(period) is None:
                # A lane's tax-restore record restores the lane; on a period override it
                # would restore the full formula over this lowering.
                lowered = lowered.model_copy(update={"params": {
                    key: value for key, value in lowered.params.items()
                    if key != "_deferred_tax_asset_inclusion_projection"
                }})
            # The terms this lowering took out, for the seam-continuity check: a term cited
            # in one column is no operand of the row's definition in either.
            omitted = tuple(sorted({
                *(() if lowered.derivation is None else lowered.derivation.omitted_operands),
                *dropped,
            }))
            lowered = lowered.model_copy(update={
                "derivation": (
                    FormulaDerivation(
                        owner="build_company_grounded_schedules",
                        strategy="cited_absence_omitted",
                        omitted_operands=omitted,
                    )
                    if lowered.derivation is None
                    else lowered.derivation.model_copy(update={"omitted_operands": omitted})
                ),
            })
            held = lowered.params.get("_not_separately_presented")
            if isinstance(held, dict) and held.get("was_override") is False:
                # This override is now the cell's formula: a restore that popped it would
                # fall back to the lane, which still reads the cited operand.
                lowered = lowered.model_copy(update={"params": {
                    **lowered.params,
                    "_not_separately_presented": {**held, "was_override": True},
                }})
            reader.overrides = {**(reader.overrides or {}), period: lowered}
            wired.append(reader.id)
    legs = {
        leg_id
        for role, row in terminal.items()
        if row.state in ("not_applicable", "not_separately_presented") and role in balance_roles
        for item_id in lines[role]
        if item_id.startswith(_FACE_BALANCE_SHEET_PREFIX)
        if (leg_id := _face_row_cash_flow_leg(model, item_id)) is not None
        and not _authored_periods(model.get_item(leg_id), projected)
        # A balance a writer projects as a number still moves cash: only a cell with no
        # amount takes its movement leg with it.
        and all(computed.get(item_id, {}).get(period) is None for period in projected)
    }
    if legs:
        graph = DependencyGraph()
        graph.build(model)
        for leg_id in sorted(legs):
            for period in projected:
                graph.retire(leg_id, period)
            wired.append(leg_id)
    return wired

def _nsp_lowered_operand_ids(item: LineItem, model: FinancialModel) -> set[str]:
    """Operands NSP lowering took out of this total on either seam column.

    The terminal historical override and the forecast formula continuity copied
    from it each keep ``_not_separately_presented.original_formula``. Operands
    in that saved formula and absent from the lowered formula were removed
    because they are included in another operand. A filed constant stores the
    lowered expression in ``lowered_formula`` and keeps the constant as the
    active spec; the diff is against that expression, not the constant. The
    integrity check reads this so a lowered operand is not read as a dropped one.
    """

    def removed(spec: FormulaSpec | None) -> set[str]:
        if spec is None:
            return set()
        metadata = spec.params.get("_not_separately_presented")
        if not isinstance(metadata, dict):
            return set()
        saved = metadata.get("original_formula")
        if not isinstance(saved, str):
            return set()
        lowered_saved = metadata.get("lowered_formula")
        current = (
            FormulaSpec.model_validate_json(lowered_saved)
            if isinstance(lowered_saved, str)
            else spec
        )
        return _same_period_member_ids(
            FormulaSpec.model_validate_json(saved)
        ) - _same_period_member_ids(current)

    removed_ids = removed(item.projected)
    historical = model.time_structure.historical_periods or ()
    if historical:
        terminal = max(int(year) for year in historical)
        removed_ids |= removed((item.overrides or {}).get(terminal))
    return removed_ids



# The two filing flows the acquisition-cost-to-goodwill share is the ratio of.
_ALLOCATION_OPERAND_DEFINITIONS = frozenset(
    {"goodwill_acquired", "acquisition_cash_paid"}
)


def _apply_acquisition_goodwill_allocation_history(
    model: FinancialModel,
    historical: tuple[int, ...],
    historical_basis_facts: tuple[HistoricalBasisFact, ...],
    *,
    role_dispositions: RoleDispositionTable | None = None,
) -> dict[str, tuple[Any, ...]]:
    """Author each filed year of the acquisition-cost-to-goodwill share from its filing facts.

    The share is the goodwill the filing attributes to the year's acquisitions
    (``goodwill_acquired``) over the cash it paid for them
    (``acquisition_cash_paid``): both minted basis facts, both cited by the
    cell. A year with positive cash paid takes that ratio. A year whose cash
    paid is evidenced zero has no acquisition cost to split, so the share is
    undefined: the cell is a cited absence (returned for the document to stamp
    ``not_applicable_evidenced``), never a zero or a division by zero. A filer
    files no ``goodwill_acquired`` line in a year it made no acquisition, so
    the cash fact alone evidences the absence; a zero-cash year whose
    ``goodwill_acquired`` is positive contradicts itself and stays
    unauthored, as does a positive-cash year missing that fact. A year with no
    ``acquisition_cash_paid`` fact is also a cited absence when
    ``statement.cf.acquisitions_of_businesses`` is cited ``not_applicable`` in
    ``role_dispositions`` for that period and no positive ``goodwill_acquired``
    fact exists: the filing's own cited absence of acquisitions evidences that
    no acquisition cost exists to allocate; the disposition's citation is
    returned in ``evidenced_absence_cells``. A ``not_disclosed_in_vintage``
    period carries no citation (its disposition is an ``unbound`` row), so it
    yields no cited absence here.

    The graph's ratio law (an absent denominator, or an all-evidenced-zero
    ``0/0``, is ``EvidencedAbsent``) does not make the zero-cash branch
    redundant: its operands are minted basis facts, not graph cells, and no
    ratio spec for this row exists for the graph to evaluate, so deleting
    the branch would leave the year unauthored rather than typed absent.
    The branch therefore stays, returning the cited absence for
    ``target_build`` to stamp (its output shape is unchanged).
    """

    empty: dict[str, tuple[Any, ...]] = {
        "wired": (),
        "source_fact_coverage": (),
        "evidenced_absence_cells": (),
    }
    item = model._index.get(ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID)
    if item is None:
        return empty
    facts: dict[tuple[str, int], HistoricalBasisFact] = {}
    for fact in historical_basis_facts:
        digits = fact.period_id.removeprefix("FY")
        if (
            fact.accounting_definition in _ALLOCATION_OPERAND_DEFINITIONS
            and digits.isdigit()
        ):
            facts[(fact.accounting_definition, int(digits))] = fact
    acq_dispositions = (
        {}
        if role_dispositions is None
        else {
            row.period_id: row
            for row in role_dispositions.rows
            if row.required_role == "statement.cf.acquisitions_of_businesses"
        }
    )
    coverage: list[dict[str, object]] = []
    absences: list[tuple[str, int, dict[str, str]]] = []
    wired = False
    for period in historical:
        goodwill = facts.get(("goodwill_acquired", period))
        cash = facts.get(("acquisition_cash_paid", period))
        if cash is None:
            disposition = acq_dispositions.get(f"FY{period}")
            if (
                disposition is not None
                and disposition.state == "not_applicable"
                and disposition.absence_citation is not None
                and (
                    goodwill is None
                    or _scaled_to(
                        goodwill.value, source_scale=goodwill.scale, target_scale="units"
                    )
                    == 0
                )
            ):
                absences.append(
                    (
                        item.id,
                        period,
                        disposition.absence_citation.model_dump(mode="json"),
                    )
                )
            continue
        cash_value = _scaled_to(
            cash.value, source_scale=cash.scale, target_scale="units"
        )
        goodwill_value = (
            None
            if goodwill is None
            else _scaled_to(
                goodwill.value, source_scale=goodwill.scale, target_scale="units"
            )
        )
        if (
            goodwill is not None
            and goodwill_value is not None
            and cash_value > 0
            and goodwill_value >= 0
        ):
            share = float(goodwill_value / cash_value)
            _set_imported_value(
                item, period, share, provenance=ValueProvenance.imported_edgar
            )
            _set_constant_override(
                item,
                period,
                share,
                origin=_derivation("filed_goodwill_acquired_share", ()),
            )
            coverage.extend(
                {
                    "source_kind": "source_bundle_schedule_disclosure",
                    "resolved_item_id": item.id,
                    "fact_id": fact.fact_id,
                    "source_id": str(fact.source_id),
                    "periods": [period],
                }
                for fact in (goodwill, cash)
            )
            wired = True
        elif cash_value == 0 and goodwill_value in (None, 0):
            reported = (cash,) if goodwill is None else (goodwill, cash)
            citation = AbsenceCitation(
                accession=reported[0].line.accession,
                presentation_role_uri=reported[0].line.statement_role,
                absence_statement=(
                    f"FY{period} "
                    + " and ".join(
                        f"filing {fact.line.accession} evidences zero for "
                        f"{fact.line.tag} ({fact.fact_id})"
                        for fact in reported
                    )
                    + ": no acquisition cost exists to allocate to goodwill"
                ),
            )
            absences.append((item.id, period, citation.model_dump(mode="json")))
    return {
        "wired": (item.id,) if wired else (),
        "source_fact_coverage": tuple(coverage),
        "evidenced_absence_cells": tuple(absences),
    }


def apply_company_grounded_schedule_policies(
    model: FinancialModel,
    *,
    business_model_revision: BusinessModelRevision,
    compiled_registry: CompiledDriverRegistry,
    historical_basis_facts: tuple[HistoricalBasisFact, ...] = (),
    grounded_semantic_rows: Mapping[str, str] | None = None,
    opening_balances: Mapping[str, OpeningBalance] = MappingProxyType({}),
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
) -> dict[str, Any]:
    """Install exact, replayable fallback formulas after historical population.

    ``grounded_semantic_rows`` is the model kind's declared semantic-role to
    workbook-item map (plan §3 row S5). Absence is feature-off: a role this
    model kind does not declare resolves to no row, and the policy that would
    have wired it does not run — which is what a workbook lacking the row
    already produced, without the mechanism having to know the row's name.

    ``opening_balances`` is the pin's opening column by role
    (``build_bm_node_history.opening_balances``); empty when it carries none.

    ``diagnostics`` carries the compile failures a policy reports at its own
    row, such as a balance the business model drives but no node declares.

    ``role_dispositions`` is the resolved six-state table
    (``role_disposition.resolve_build_dispositions`` over the pinned coverage
    profile): the one input that says whether a role is bound, cited absent,
    cited as included in another line, owed and missing, or not owed at all.
    Every policy reads a role's terminal historical state; an ``unowed`` role's
    lines leave the model through ``DependencyGraph.retire`` after the last
    policy has written, a cited role's operands leave additive readers through
    ``FormulaSpec.without``, and no policy writes a number on a cited or
    unowed cell. ``role_items`` is the one role -> lines lookup. Both are
    returned for the census beside the wired rows.
    """

    if not model._index:
        model.build_index()
    declared_rows: dict[str, str] = dict(grounded_semantic_rows or {})
    ticker = (
        getattr(business_model_revision, "ticker", None)
        or getattr(getattr(model, "company", None), "ticker", None)
    )
    if ticker:
        # The rows the bridge mints for each balance's counterpart stand in for
        # the model kind's grounded rows when it declares none.
        for counterpart in pass_through_counterparts():
            for grounded_role, minted_id in (
                (counterpart.leaf, counterpart.semantic_item(ticker)),
                (counterpart.change_role, counterpart.change_item(ticker)),
            ):
                if grounded_role not in declared_rows and minted_id in model._index:
                    declared_rows[grounded_role] = minted_id

    # BusinessModelRevision and CompiledDriverRegistry validate at construction
    # (their owners); no exact-type re-proof here (B6 ruling, 2026-08-22).
    historical = _historical_periods(model)
    projected = _projection_periods(model)
    if not historical or not projected:
        return {
            "wired": (),
            "source_fact_coverage": (),
            "diagnostics": (),
            "role_dispositions": role_dispositions,
            "role_items": role_items,
        }
    validate_historical_reconciliation_bindings(historical_basis_facts)
    filing_schedule = _apply_filing_schedule_disclosures(
        model, historical, historical_basis_facts
    )
    allocation = _apply_acquisition_goodwill_allocation_history(
        model,
        historical,
        historical_basis_facts,
        role_dispositions=role_dispositions,
    )
    graph = DependencyGraph()
    graph.build(model)
    values = graph.compute({})
    population_lines = _materialize_bound_population_lines(
        model, role_dispositions, values, historical
    )
    diagnostics: list[BuildDiagnostic] = []
    _remove_face_rows_another_line_holds(model, values, historical)
    _remove_orphan_face_row_cash_flow_legs(model)
    wired = [
        *population_lines,
        *filing_schedule["wired"],
        *_apply_working_capital_policy(model, values, historical, projected),
        *_apply_other_working_capital_derivation(
            model,
            values,
            historical,
            projected,
            role_dispositions=role_dispositions,
        ),
        *_apply_lease_policy(
            model,
            values,
            historical,
            projected,
            historical_basis_facts=historical_basis_facts,
            role_dispositions=role_dispositions,
        ),
        *_apply_net_dilution_default(model, values, historical, projected),
        *_apply_share_policy(
            model,
            values,
            business_model_revision,
            historical,
            projected,
            role_dispositions=role_dispositions,
        ),
        *_apply_balance_sheet_policy(
            model,
            values,
            business_model_revision,
            compiled_registry,
            historical,
            projected,
            declared_rows=declared_rows,
            diagnostics=diagnostics,
            role_dispositions=role_dispositions,
            role_items=role_items,
        ),
        *_apply_nci_share_policy(model, values, historical, projected),
        *_apply_cash_flow_policy(
            model,
            values,
            business_model_revision,
            historical,
            projected,
            declared_rows=declared_rows,
            role_dispositions=role_dispositions,
            role_items=role_items,
            diagnostics=diagnostics,
        ),
        *_apply_non_gaap_policy(
            model,
            values,
            business_model_revision,
            historical,
            projected,
            filing_schedule["sbc_add_back_disclosed_periods"],
        ),
        *_apply_historical_schedule_closure(
            model,
            values,
            historical,
            projected,
            filing_schedule["tax_effect_disclosed_periods"],
            role_dispositions=role_dispositions,
            role_items=role_items,
        ),
        *complete_capitalized_software_schedule(
            model,
            historical=historical,
            projected=projected,
            role_dispositions=role_dispositions,
            role_items=role_items,
        ),
    ]
    opening_wired, opening_coverage = _apply_opening_balances(
        model,
        business_model_revision,
        compiled_registry,
        historical,
        opening_balances=opening_balances,
    )
    _rewire_deferred_tax_cash_flow_for_asset_fact(model)
    lower_interest_inclusion(
        model,
        evidenced_absences=_cited_absence_cells(model, role_dispositions, role_items),
    )
    # Disclosure membership is applied after closure has authored the historical
    # tax bridge, so every historical reader shares the same retained components.
    _retire_unowed_roles(model, role_dispositions, role_items, historical, projected)
    _retire_absent_role_schedules(model, role_dispositions, role_items, historical, projected)
    _retire_unclaimed_face_slots(
        model, business_model_revision, compiled_registry, historical, projected
    )
    wired.extend(
        _install_sga_host_forecast(model, role_dispositions, role_items, historical, projected)
    )
    wired.extend(
        _lower_cited_operands(model, role_dispositions, role_items, historical, projected)
    )
    _retire_undefined_historical_ratios(model, historical)
    return {
        "wired": tuple(sorted({*wired, *opening_wired, *allocation["wired"]})),
        "source_fact_coverage": (
            *filing_schedule["source_fact_coverage"],
            *allocation["source_fact_coverage"],
            *opening_coverage,
        ),
        "evidenced_absence_cells": allocation["evidenced_absence_cells"],
        "diagnostics": tuple(diagnostics),
        "role_dispositions": role_dispositions,
        "role_items": role_items,
    }


__all__ = [
    "HOUSE_REVENUE_RATIO_RATE_ITEM_IDS",
    "HouseNetDilution",
    "HouseRevenueRatio",
    "HouseSbcMix",
    "apply_company_grounded_schedule_policies",
    "apply_not_separately_presented_periods",
    "house_net_dilution",
    "house_nci_share",
    "house_revenue_ratios",
    "house_sbc_mix",
    "capital_return_bases",
    "not_separately_presented_formula",
    "optional_component_participates",
    "reported_house_net_dilution",
    "reported_house_revenue_ratios",
    "reported_house_sbc_mix",
    "sbc_per_line_row_split_undisclosed_findings",
    "scale_aggregate_sbc_scenario_values",
]
