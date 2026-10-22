"""Canonical schedule rows the generic template declares once.

Each row below has one owner: the ``FormulaSpec`` this module writes onto the
template row (deferred contract cost, the total-debt roll-forward with its
interest identities, the income-statement interest row, other income as the
ex-interest control less interest). The capitalized-software schedule and the
physical PP&E additions are declared on their template rows.

The compile starts from these rows and applies a filing's dispositions to them:
a role cited absent or included in another line lowers the declared formula
through ``FormulaSpec.without`` / ``DependencyGraph.retire`` on the
role-disposition table, and ``lower_interest_inclusion`` lowers other income and
the interest line by the interest fact, restoring the declared formulas when
that fact clears.
"""

from __future__ import annotations

from ..models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
)
from industry_slice_core.industry_types import Unit
from .template_builder_insertions import (
    _find_section_with_item,
    _statement_items,
    _statement_periods,
)

_SALES_AND_MARKETING_ID = "tpl.a.operating_leverage.sales_and_marketing"
_OPERATING_CASH_FLOW_ID = "tpl.fm.cash_flow.operating_cash_flow"
#: ``(schedule, rate, balance sheet row, cash-flow row, label, note slug)`` of the
#: current and long-term deferred contract cost schedules.
_DEFERRED_CONTRACT_COST_ROWS = (
    (
        "tpl.a.balance_sheet_wc.current_asset_1",
        "tpl.a.balance_sheet_wc.of_operating_line_adjust_link_as_neccesary",
        "tpl.fm.balance_sheet.current_asset_1",
        "Deferred contract costs",
        "deferred_contract_costs",
    ),
    (
        "tpl.a.other_assets.long_term_asset_1",
        "tpl.a.other_assets.of_operating_line_adjust_link_as_neccesary",
        "tpl.fm.balance_sheet.long_term_asset_1",
        "Long-term deferred contract costs",
        "long_term_deferred_contract_costs",
    ),
)
_DEFERRED_CONTRACT_LONG_TERM_CASH_ID = "tpl.fm.cash_flow.long_term_asset_1"


def _install_deferred_contract_cost_schedule(model: FinancialModel) -> None:
    """Deferred contract costs: balance = sales and marketing times a carried rate.

    The filing populates the schedule cell directly; the balance-sheet row
    reads it in both lanes and the cash-flow rows move with the balance
    (their projected delta is already declared). Both balances are other
    working capital leaves the aggregate moves, so cash from operations does
    not also sum the long-term cash row historically.
    """

    historical, projection = _statement_periods(model)
    items = _statement_items(model)
    for schedule_id, rate_id, balance_id, label, note_slug in _DEFERRED_CONTRACT_COST_ROWS:
        schedule, rate, balance = items[schedule_id], items[rate_id], items[balance_id]
        schedule.label = label
        rate.label = "% of sales and marketing"
        balance.label = label
        schedule.historical = None
        schedule.projected = FormulaSpec(
            type=FormulaType.driver,
            subtype="base_x_rate",
            params={
                "base": LineItemRef(id=_SALES_AND_MARKETING_ID),
                "rate": LineItemRef(id=rate_id),
            },
            note=f"{note_slug}_from_authored_or_observed_company_sales_marketing_rate",
        )
        rate.historical = FormulaSpec(
            type=FormulaType.ratio,
            subtype="divide",
            params={
                "numerator": LineItemRef(id=schedule_id),
                "denominator": LineItemRef(id=_SALES_AND_MARKETING_ID),
            },
        )
        rate.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="carry_forward",
            params={"source": LineItemRef(id=rate_id, t=-1)},
            note=f"carry_forward_observed_company_{note_slug}_rate",
        )
        balance.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="cross_sheet_ref",
            params={"source": LineItemRef(id=schedule_id)},
            note=f"{note_slug}_schedule_to_balance_sheet",
        )
        balance.historical = FormulaSpec(
            type=FormulaType.ref,
            subtype="cross_sheet_ref",
            params={"source": LineItemRef(id=schedule_id)},
            note=f"observed_{note_slug}_schedule_to_balance_sheet",
        )
        for item in (schedule, rate, balance):
            item.formula_periods = sorted({*historical, *projection})
    operating_cash = items[_OPERATING_CASH_FLOW_ID]
    if operating_cash.historical is not None:
        lowered = operating_cash.historical.without(
            lambda ref: ref.ref.id == _DEFERRED_CONTRACT_LONG_TERM_CASH_ID
        )
        if lowered is not None:
            operating_cash.historical = lowered


_DEBT_CURRENT_ID = "tpl.a.capital_sources.current_portion_of_long_term_debt"
_DEBT_LONG_TERM_ID = "tpl.a.capital_sources.long_term_debt"
_DEBT_CHANGE_ID = "tpl.a.capital_sources.change"
_DEBT_BEGINNING_ID = "tpl.a.other_income_interest.debt_beg_of_period"
_DEBT_ENDING_ID = "tpl.a.other_income_interest.debt_end_of_period"
_DEBT_AVERAGE_ID = "tpl.a.other_income_interest.average"
_BALANCE_SHEET_CURRENT_DEBT_ID = "tpl.fm.balance_sheet.current_portion_of_long_term_debt"
_BALANCE_SHEET_LONG_TERM_DEBT_ID = "tpl.fm.balance_sheet.long_term_debt"
_DEBT_FINANCING_CASH_ID = "tpl.fm.cash_flow.proceeds_or_payment_for_debt"


def _total_debt(*, t: int) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="sum_list",
        params={
            "function": "SUM",
            "items": [
                LineItemRef(id=_BALANCE_SHEET_CURRENT_DEBT_ID, t=t),
                LineItemRef(id=_BALANCE_SHEET_LONG_TERM_DEBT_ID, t=t),
            ],
        },
    )


def _install_debt_schedule(model: FinancialModel) -> None:
    """Total debt rolls forward once: beginning + net change = ending.

    Beginning and ending are the balance sheet's current plus long-term debt
    (the filing's balances in history, the schedule's in the forecast); the
    average is their mean; the net change is the forecast input and, in
    history, ending less beginning. The long-term balance is ending total
    debt less the current portion, so an authored current portion moves
    long-term debt by the same amount and the financing cash flow (the net
    change) never drifts from the balance sheet. The interest rows
    (``interest_rate``, ``interest_expense``) read the average.
    """

    historical, projection = _statement_periods(model)
    lagged = [*historical[1:], *projection]
    items = _statement_items(model)
    current_debt = items[_DEBT_CURRENT_ID]
    current_debt.item_type = ItemType.input
    current_debt.label = "Current portion of long-term debt"
    balance_sheet_current = items[_BALANCE_SHEET_CURRENT_DEBT_ID]
    balance_sheet_current.item_type = ItemType.derived
    balance_sheet_current.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=_DEBT_CURRENT_ID)},
        note="debt_schedule_current_portion_to_balance_sheet",
    )

    beginning = items[_DEBT_BEGINNING_ID]
    beginning.historical = _total_debt(t=-1).model_copy(
        update={"note": "historical_total_debt_opening_balance"}
    )
    beginning.projected = _total_debt(t=-1).model_copy(
        update={"note": "canonical_total_debt_opening_balance"}
    )
    beginning.formula_periods = lagged

    ending = items[_DEBT_ENDING_ID]
    ending.historical = _total_debt(t=0).model_copy(
        update={"note": "historical_total_debt_ending_balance"}
    )
    ending.projected = FormulaSpec(
        type=FormulaType.roll_forward,
        subtype="schedule",
        params={
            "beginning": LineItemRef(id=_DEBT_BEGINNING_ID),
            "additions": [LineItemRef(id=_DEBT_CHANGE_ID)],
            "subtractions": [],
        },
        note="canonical_debt_balance_roll_forward",
    )
    ending.formula_periods = [*historical, *projection]

    # The opening balance is the prior ending balance, so the average reads the
    # two ending balances: the first historical year, with no prior balance, is
    # off the average's axis by construction.
    average = items[_DEBT_AVERAGE_ID]
    average.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "function": "AVERAGE",
            "items": [
                LineItemRef(id=_DEBT_ENDING_ID, t=-1),
                LineItemRef(id=_DEBT_ENDING_ID),
            ],
        },
        note="average_debt_from_opening_and_closing_balances",
    )
    average.projected = average.historical.model_copy(deep=True)
    average.formula_periods = lagged

    net_change = items[_DEBT_CHANGE_ID]
    net_change.item_type = ItemType.input
    net_change.projected = None
    net_change.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                LineItemRef(id=_DEBT_ENDING_ID),
                LineItemRef(id=_DEBT_ENDING_ID, t=-1),
            ]
        },
        note="historical_debt_ending_minus_prior_ending",
    )
    net_change.formula_periods = lagged

    long_term = items[_DEBT_LONG_TERM_ID]
    long_term.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                LineItemRef(id=_DEBT_ENDING_ID),
                LineItemRef(id=_DEBT_CURRENT_ID),
            ]
        },
        note="ending_total_debt_less_current_portion",
    )
    items[_BALANCE_SHEET_LONG_TERM_DEBT_ID].projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=_DEBT_LONG_TERM_ID)},
        note="debt_schedule_long_term_balance_to_balance_sheet",
    )
    items[_DEBT_FINANCING_CASH_ID].projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=_DEBT_CHANGE_ID)},
        note="net_debt_change_financing_cash",
    )


_OTHER_INCOME_FACE_ID = "tpl.fm.income_statement.other_income_expense"
_OTHER_INCOME_CONTROL_ID = "tpl.a.other_income_interest.other_income_expense"
_INCOME_STATEMENT_INTEREST_ID = "tpl.fm.income_statement.interest_expense"
_REPORTED_INTEREST_ID = "tpl.fm.adjusted_earnings.interest_expense"
_DEBT_INTEREST_EXPENSE_ID = "tpl.a.other_income_interest.interest_expense"
_PRETAX_INCOME_ID = "tpl.fm.income_statement.income_before_income_taxes"
_OPERATING_INCOME_ID = "tpl.fm.income_statement.operating_income"


def _insert_income_statement_interest_row(model: FinancialModel) -> None:
    """The income statement shows the debt schedule's interest below other income.

    The row reads the filing's reported interest in history and the debt
    schedule's interest expense in the forecast. It is a display line of the
    interest the other-income control nets out; no statement total sums it.
    """

    sheet = model.sheets["Financial_model"]
    section = _find_section_with_item(sheet, _OTHER_INCOME_FACE_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _OTHER_INCOME_FACE_ID
    )
    insert_row = int(anchor.row) + 1
    for sheet_section in sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    historical, projection = _statement_periods(model)
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=_INCOME_STATEMENT_INTEREST_ID,
            label="Interest expense",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            model_scale=anchor.model_scale,
            driver_category=anchor.driver_category,
            customization=anchor.customization,
            style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
            historical=FormulaSpec(
                type=FormulaType.ref,
                subtype="cross_sheet_ref",
                params={"source": LineItemRef(id=_REPORTED_INTEREST_ID)},
                note="reported_interest_expense_to_income_statement",
            ),
            projected=FormulaSpec(
                type=FormulaType.ref,
                subtype="cross_sheet_ref",
                params={"source": LineItemRef(id=_DEBT_INTEREST_EXPENSE_ID)},
                note="debt_schedule_interest_expense_to_income_statement",
            ),
            formula_periods=sorted({*historical, *projection}),
        ),
    )


def _install_other_income_schedule(model: FinancialModel) -> None:
    """Other income is the ex-interest control less the interest the statement shows.

    The control is the filing's own identity per filed period: pretax less
    operating income, with the interest the statement shows added back (a
    composite filer's other income already nets its interest, so the add-back
    restores the ex-interest definition; a filer that states interest on its
    own line has it outside other income, so the identity nets it once and the
    add-back restores that single deduction). Its forecast carries the last
    ex-interest amount, and the face line is that amount less the debt
    schedule's interest. A filing that cites its interest included in other
    income, or absent, lowers the forecast to a carry of the filed line
    (``lower_interest_inclusion``): the control's history reads the interest
    operand, which is not a number there, so the control cannot carry it.
    """

    historical, projection = _statement_periods(model)
    items = _statement_items(model)
    control = items[_OTHER_INCOME_CONTROL_ID]
    control.item_type = ItemType.input
    control.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="expression",
        params={
            "expr": {
                "op": "+",
                "args": [
                    {
                        "op": "-",
                        "left": LineItemRef(id=_PRETAX_INCOME_ID),
                        "right": LineItemRef(id=_OPERATING_INCOME_ID),
                    },
                    LineItemRef(id=_INCOME_STATEMENT_INTEREST_ID),
                ],
            },
        },
        note="other_income_expense_from_filed_pretax_identity_excluding_interest",
    )
    control.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="carry_forward",
        params={"source": LineItemRef(id=control.id, t=-1)},
        note="carry_forward_observed_other_income_excluding_interest",
    )
    control.formula_periods = sorted({*historical, *projection})
    items[_OTHER_INCOME_FACE_ID].projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                LineItemRef(id=_OTHER_INCOME_CONTROL_ID),
                LineItemRef(id=_INCOME_STATEMENT_INTEREST_ID),
            ]
        },
        note="other_income_expense_net_of_debt_schedule_interest_expense",
    )


def _install_canonical_schedules(model: FinancialModel) -> None:
    """Declare every canonical schedule row this module owns."""

    _install_deferred_contract_cost_schedule(model)
    _install_debt_schedule(model)
    _insert_income_statement_interest_row(model)
    _install_other_income_schedule(model)


__all__ = ["_install_canonical_schedules"]
