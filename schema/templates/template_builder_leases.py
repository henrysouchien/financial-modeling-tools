"""The operating-lease ROU and liability schedules, declared once in the template.

Seven rows carry the lease roll-forwards (ROU additions and amortization, the
total-liability beginning / additions / payments / ending, and the
current-portion cash bridge), and every formula that links them to the lease
balances, the balance sheet and operating cash flow is declared here. The
compile-time writers read the template's declaration and write only what an
issuer's filing and role dispositions decide: a policy input row's projection,
or a ref removed from a declared formula with ``FormulaSpec.without``.

The canonical shapes are those of the financial-schedule conventions (§4):
``ROU = ROU(t-1) + additions - amortization``, ``ending = beginning +
additions - payments`` over the total (current plus long-term) liability, and
``long-term = ending - current``.
"""

from __future__ import annotations

from typing import Any, List

from industry_slice_core.industry_types import Unit

from ..models import (
    CellColor,
    CellStyle,
    CustomizationType,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
)
from .template_builder_insertions import (
    _iter_items,
    _statement_items,
    _statement_periods,
)

ROU_SCHEDULE_ITEM_ID = "tpl.a.other_assets.operating_lease_right_of_use_assets"
ROU_RATE_ITEM_ID = "tpl.a.other_assets.of_general_and_administrative"
ROU_ADDITIONS_ITEM_ID = "tpl.a.other_assets.operating_lease_rou_asset_additions"
ROU_AMORTIZATION_ITEM_ID = "tpl.a.other_assets.operating_lease_rou_asset_amortization"
ROU_BALANCE_ITEM_ID = "tpl.fm.balance_sheet.operating_lease_right_of_use_assets"
ROU_CASH_ITEM_ID = "tpl.fm.cash_flow.operating_lease_right_of_use_assets"
LIABILITY_SCHEDULE_ITEM_ID = "tpl.a.capital_sources.long_term_operating_lease_liabilities"
LIABILITY_RATE_ITEM_ID = "tpl.a.capital_sources.of_general_and_administrative"
CURRENT_LIABILITY_SCHEDULE_ITEM_ID = "tpl.a.capital_sources.current_operating_lease_liabilities"
LIABILITY_BEGINNING_ITEM_ID = "tpl.a.capital_sources.operating_lease_liability_beginning"
LIABILITY_ADDITIONS_ITEM_ID = "tpl.a.capital_sources.operating_lease_liability_additions"
LIABILITY_PAYMENTS_ITEM_ID = "tpl.a.capital_sources.operating_lease_liability_payments"
LIABILITY_ENDING_ITEM_ID = "tpl.a.capital_sources.operating_lease_liability_ending"
LIABILITY_BALANCE_ITEM_ID = "tpl.fm.balance_sheet.long_term_operating_lease_liabilities"
LIABILITY_CASH_ITEM_ID = "tpl.fm.cash_flow.long_term_operating_lease_liabilities"
CURRENT_LIABILITY_CASH_ITEM_ID = "tpl.fm.cash_flow.current_operating_lease_liabilities"
_OPERATING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.operating_cash_flow"
_DEPRECIATION_AND_AMORTIZATION_CASH_ITEM_ID = "tpl.fm.cash_flow.depreciation_and_amortization"

#: The rows this module inserts, as ``(id, anchor it follows, label, item type,
#: row whose driver category it takes)``. Each follows the row the previous
#: insertion left, so the block reads in schedule order under its anchor.
LEASE_SCHEDULE_INSERTED_ROWS = (
    (
        ROU_ADDITIONS_ITEM_ID,
        ROU_SCHEDULE_ITEM_ID,
        "Add: Operating lease ROU asset additions",
        ItemType.input,
        ROU_SCHEDULE_ITEM_ID,
    ),
    (
        ROU_AMORTIZATION_ITEM_ID,
        ROU_ADDITIONS_ITEM_ID,
        "Less: Operating lease ROU asset amortization",
        ItemType.input,
        ROU_SCHEDULE_ITEM_ID,
    ),
    (
        LIABILITY_BEGINNING_ITEM_ID,
        CURRENT_LIABILITY_SCHEDULE_ITEM_ID,
        "Beginning operating lease liabilities",
        ItemType.derived,
        LIABILITY_SCHEDULE_ITEM_ID,
    ),
    (
        LIABILITY_ADDITIONS_ITEM_ID,
        LIABILITY_BEGINNING_ITEM_ID,
        "Add: Operating lease liability additions",
        ItemType.input,
        LIABILITY_SCHEDULE_ITEM_ID,
    ),
    (
        LIABILITY_PAYMENTS_ITEM_ID,
        LIABILITY_ADDITIONS_ITEM_ID,
        "Less: Operating lease liability payments",
        ItemType.input,
        LIABILITY_SCHEDULE_ITEM_ID,
    ),
    (
        LIABILITY_ENDING_ITEM_ID,
        LIABILITY_PAYMENTS_ITEM_ID,
        "Ending operating lease liabilities",
        ItemType.derived,
        LIABILITY_SCHEDULE_ITEM_ID,
    ),
    (
        CURRENT_LIABILITY_CASH_ITEM_ID,
        LIABILITY_CASH_ITEM_ID,
        "Current operating lease liabilities",
        ItemType.derived,
        LIABILITY_CASH_ITEM_ID,
    ),
)


def _insert_row_after(model: FinancialModel, *, anchor_id: str, row: LineItem) -> None:
    """Insert ``row`` below ``anchor_id``, shifting every later row of its sheet."""

    for sheet in model.sheets.values():
        for section in sheet.sections:
            index = next(
                (i for i, item in enumerate(section.line_items) if item.id == anchor_id),
                None,
            )
            if index is None:
                continue
            insert_row = int(section.line_items[index].row) + 1
            for sheet_section in sheet.sections:
                for item in sheet_section.line_items:
                    if int(item.row) >= insert_row:
                        item.row = int(item.row) + 1
            row.row = insert_row
            section.line_items.insert(index + 1, row)
            return
    raise AssertionError(f"lease schedule anchor {anchor_id} is not in the template")


def _balance_ref(item_id: str, t: int = 0) -> LineItemRef:
    return LineItemRef(id=item_id, t=t)


def _cross_sheet_ref(source_id: str, note: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=source_id)},
        note=note,
    )


def _roll_forward(
    beginning: LineItemRef, additions_id: str, subtractions_id: str, note: str
) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.roll_forward,
        subtype="schedule",
        params={
            "beginning": beginning,
            "additions": [LineItemRef(id=additions_id)],
            "subtractions": [LineItemRef(id=subtractions_id)],
        },
        note=note,
    )


def _delta(minuend: LineItemRef, subtrahend: LineItemRef, note: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={"operands": ["-", minuend, subtrahend]},
        note=note,
    )


def _insert_ref_after(items: List[Any], *, after_id: str, new_id: str) -> None:
    refs = [ref.id for ref in items]
    if new_id in refs:
        return
    items.insert(refs.index(after_id) + 1, LineItemRef(id=new_id))


def install_operating_lease_schedule(model: FinancialModel) -> None:
    """Declare the lease rows, their roll-forwards and their statement links.

    Runs after the cash-flow movement aggregates are generated: the lease
    balances are not ``other_noncash`` members, so their movements are the three
    lease cash rows below, each a line of projected operating cash flow.
    """

    historical, projection = _statement_periods(model)
    observed = sorted(historical)
    bridge = observed[1:]
    through_projection = sorted({*observed, *projection})
    bridge_through_projection = sorted({*bridge, *projection})

    for item_id, anchor_id, label, item_type, category_id in LEASE_SCHEDULE_INSERTED_ROWS:
        if any(item.id == item_id for _, item in _iter_items(model)):
            raise AssertionError(f"lease schedule row {item_id} already exists")
        _insert_row_after(
            model,
            anchor_id=anchor_id,
            row=LineItem(
                id=item_id,
                label=label,
                row=0,
                item_type=item_type,
                unit=Unit.dollars,
                model_scale="millions",
                driver_category=_statement_items(model)[category_id].driver_category,
                customization=CustomizationType.fixed,
                style=CellStyle(
                    color=CellColor.input_blue
                    if item_type is ItemType.input
                    else CellColor.formula_black
                ),
            ),
        )
    model.build_index()
    items = _statement_items(model)

    for item_id in (
        ROU_ADDITIONS_ITEM_ID,
        ROU_AMORTIZATION_ITEM_ID,
        LIABILITY_ADDITIONS_ITEM_ID,
        LIABILITY_PAYMENTS_ITEM_ID,
    ):
        items[item_id].formula_periods = list(projection)

    rou_schedule = items[ROU_SCHEDULE_ITEM_ID]
    rou_schedule.projected = _roll_forward(
        LineItemRef(id=ROU_SCHEDULE_ITEM_ID, t=-1),
        ROU_ADDITIONS_ITEM_ID,
        ROU_AMORTIZATION_ITEM_ID,
        "canonical_operating_lease_rou_asset_roll_forward",
    )
    rou_schedule.formula_periods = list(projection)

    items[ROU_BALANCE_ITEM_ID].projected = _cross_sheet_ref(
        ROU_SCHEDULE_ITEM_ID, "operating_lease_rou_asset_schedule_to_balance_sheet"
    )
    items[ROU_BALANCE_ITEM_ID].historical = _cross_sheet_ref(
        ROU_SCHEDULE_ITEM_ID, "observed_operating_lease_rou_asset_schedule_to_balance_sheet"
    )
    items[ROU_BALANCE_ITEM_ID].formula_periods = list(through_projection)

    # The old G&A-ratio projection identified neither additions nor amortization:
    # the rate rows read the balance back as a share of the SG&A base, in both lanes.
    for rate_id in (ROU_RATE_ITEM_ID, LIABILITY_RATE_ITEM_ID):
        rate = items[rate_id]
        if rate.historical is None:
            raise AssertionError(f"{rate_id} carries no observed balance ratio")
        rate.projected = rate.historical.model_copy(deep=True)
        rate.formula_periods = sorted({*(rate.formula_periods or []), *through_projection})
        if rate.overrides:
            rate.overrides = {
                period: spec
                for period, spec in rate.overrides.items()
                if int(period) not in projection
            } or None

    # Current lease liabilities are a schedule input seeded directly; the balance-sheet
    # face row presents the schedule, so the schedule takes no history from the face.
    current = items[CURRENT_LIABILITY_SCHEDULE_ITEM_ID]
    current.historical = None
    current.formula_periods = list(through_projection)

    beginning = items[LIABILITY_BEGINNING_ITEM_ID]
    beginning.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="sum_list",
        params={
            "function": "SUM",
            "items": [
                LineItemRef(id=CURRENT_LIABILITY_SCHEDULE_ITEM_ID, t=-1),
                LineItemRef(id=LIABILITY_BALANCE_ITEM_ID, t=-1),
            ],
        },
        note="canonical_total_operating_lease_liability_opening_balance",
    )
    beginning.formula_periods = list(projection)
    ending = items[LIABILITY_ENDING_ITEM_ID]
    ending.projected = _roll_forward(
        LineItemRef(id=LIABILITY_BEGINNING_ITEM_ID),
        LIABILITY_ADDITIONS_ITEM_ID,
        LIABILITY_PAYMENTS_ITEM_ID,
        "canonical_operating_lease_liability_roll_forward",
    )
    ending.formula_periods = list(projection)

    long_term = items[LIABILITY_SCHEDULE_ITEM_ID]
    long_term.historical = _cross_sheet_ref(
        LIABILITY_BALANCE_ITEM_ID, "observed_operating_lease_liability_statement_to_schedule"
    )
    long_term.projected = _delta(
        LineItemRef(id=LIABILITY_ENDING_ITEM_ID),
        LineItemRef(id=CURRENT_LIABILITY_SCHEDULE_ITEM_ID),
        "ending_operating_lease_liability_less_current_portion",
    )
    long_term.formula_periods = list(through_projection)

    # The filing-line value lands on the balance-sheet row and bridges into the
    # schedule; the reverse bridge formed a cycle.
    long_term_balance = items[LIABILITY_BALANCE_ITEM_ID]
    long_term_balance.historical = None
    long_term_balance.projected = _cross_sheet_ref(
        LIABILITY_SCHEDULE_ITEM_ID, "operating_lease_liability_schedule_to_balance_sheet"
    )
    long_term_balance.formula_periods = list(through_projection)

    # The three lease cash rows are the lease movement: asset decrease plus current
    # and long-term liability increases, observed and projected from the same deltas.
    rou_cash = items[ROU_CASH_ITEM_ID]
    rou_cash.historical = _delta(
        _balance_ref(ROU_BALANCE_ITEM_ID, -1),
        _balance_ref(ROU_BALANCE_ITEM_ID),
        "observed_operating_lease_rou_asset_balance_delta",
    )
    rou_cash.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                _balance_ref(ROU_BALANCE_ITEM_ID, -1),
                _balance_ref(ROU_BALANCE_ITEM_ID),
            ]
        },
    )
    liability_cash = items[LIABILITY_CASH_ITEM_ID]
    liability_cash.historical = _delta(
        _balance_ref(LIABILITY_BALANCE_ITEM_ID),
        _balance_ref(LIABILITY_BALANCE_ITEM_ID, -1),
        "observed_operating_lease_liability_balance_delta",
    )
    liability_cash.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                _balance_ref(LIABILITY_BALANCE_ITEM_ID),
                _balance_ref(LIABILITY_BALANCE_ITEM_ID, -1),
            ]
        },
    )
    current_cash = items[CURRENT_LIABILITY_CASH_ITEM_ID]
    current_cash.projected = _delta(
        _balance_ref(CURRENT_LIABILITY_SCHEDULE_ITEM_ID),
        _balance_ref(CURRENT_LIABILITY_SCHEDULE_ITEM_ID, -1),
        "current_operating_lease_schedule_roll_forward_cash_bridge",
    )
    current_cash.historical = current_cash.projected.model_copy(deep=True)
    for cash_row in (rou_cash, liability_cash, current_cash):
        cash_row.formula_periods = list(bridge_through_projection)

    # Projected operating cash flow sums the three lease rows after D&A. Historical
    # operating cash flow is a reported subtotal: the lease rows are not summed there.
    operating_cash_flow = items[_OPERATING_CASH_FLOW_ITEM_ID]
    if operating_cash_flow.historical is None or operating_cash_flow.projected is None:
        raise AssertionError(f"{_OPERATING_CASH_FLOW_ITEM_ID} carries no sum in both lanes")
    projected_items = operating_cash_flow.projected.params["items"]
    after_id = _DEPRECIATION_AND_AMORTIZATION_CASH_ITEM_ID
    for lease_cash_id in (ROU_CASH_ITEM_ID, CURRENT_LIABILITY_CASH_ITEM_ID, LIABILITY_CASH_ITEM_ID):
        _insert_ref_after(projected_items, after_id=after_id, new_id=lease_cash_id)
        after_id = lease_cash_id
    operating_cash_flow.historical.params["items"] = [
        ref
        for ref in operating_cash_flow.historical.params["items"]
        if ref.id not in (ROU_CASH_ITEM_ID, CURRENT_LIABILITY_CASH_ITEM_ID, LIABILITY_CASH_ITEM_ID)
    ]
    operating_cash_flow.formula_periods = sorted(
        {*(operating_cash_flow.formula_periods or []), *projection}
    )


__all__ = [
    "LEASE_SCHEDULE_INSERTED_ROWS",
    "install_operating_lease_schedule",
]
