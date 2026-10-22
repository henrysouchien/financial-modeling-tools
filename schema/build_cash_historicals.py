"""Cash-history build helpers for schema build orchestration."""

from __future__ import annotations

from .build_formula_eval import (
    _evaluate_formula_simple as _formula_evaluate_formula_simple,
    _lookup_formula_value as _formula_lookup_formula_value,
)
from .build_model_items import _iter_items as _model_iter_items
from .build_value_writers import _set_constant_override as _writer_set_constant_override
from .models import FinancialModel, FormulaSpec, FormulaType, LineItemRef, ValueProvenance


_CASH_BEGINNING_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period"
_CASH_END_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period"
_NET_CHANGE_IN_CASH_ITEM_ID = "tpl.fm.cash_flow.net_change_in_cash_and_cash_equivalents"
_CASH_FX_ITEM_ID = "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash"
_TOTAL_LIABILITIES_ITEM_ID = "tpl.fm.balance_sheet.total_liabilities"
_LONG_TERM_DEBT_ITEM_ID = "tpl.fm.balance_sheet.long_term_debt"
_CARRY_FORWARD_BALANCE_ITEM_IDS = (
    "tpl.fm.balance_sheet.other_current_assets",
    "tpl.fm.balance_sheet.accrued_compensation",
    "tpl.fm.balance_sheet.current_liability_1",
    "tpl.fm.balance_sheet.current_liability_2",
    "tpl.fm.balance_sheet.current_liability_3",
)
_INCOME_STATEMENT_NET_INCOME_ITEM_ID = "tpl.fm.income_statement.net_income"
_CASH_FLOW_NET_INCOME_ITEM_ID = "tpl.fm.cash_flow.net_income"
_BALANCE_SHEET_CASH_ITEM_ID = "tpl.fm.balance_sheet.cash_and_marketable_securities"
_CASH_BRIDGE_ITEM_ID = "tpl.fm.cash_flow.cash_reconciliation_adjustment"
_AOCI_SCHEDULE_ITEM_ID = (
    "tpl.a.dividends_shares.accumulated_other_comprehensive_income_loss"
)
_BALANCE_SHEET_AOCI_ITEM_ID = (
    "tpl.fm.balance_sheet.accumulated_other_comprehensive_income_loss"
)
_OPERATING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.operating_cash_flow"
_INVESTING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.investing_cash_flow"
_FINANCING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.financing_cash_flow"
_SUPPORTED_HISTORICAL_DELTA_ITEM_IDS = (
    "tpl.fm.cash_flow.long_term_asset_2",
    "tpl.fm.cash_flow.other_long_term_liabilities",
)


def _seed_cash_beginning_of_period(model: FinancialModel) -> bool:
    """Seed the oldest historical beginning cash from ending cash and net change."""

    historical_periods = [
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    ]
    if not historical_periods:
        return False
    first_year = historical_periods[0]

    try:
        beginning_item = model.get_item(_CASH_BEGINNING_ITEM_ID)
        end_item = model.get_item(_CASH_END_ITEM_ID)
        net_change_item = model.get_item(_NET_CHANGE_IN_CASH_ITEM_ID)
    except KeyError:
        return False

    existing_beginning = _formula_lookup_formula_value(
        model,
        _CASH_BEGINNING_ITEM_ID,
        first_year,
        {},
    )
    if existing_beginning is not None:
        return False

    end_cash = _formula_lookup_formula_value(
        model,
        _CASH_END_ITEM_ID,
        first_year,
        {},
    )
    if end_cash is None:
        end_cash = _formula_evaluate_formula_simple(model, end_item, first_year, {})
    if end_cash is None:
        return False

    net_change = _formula_lookup_formula_value(
        model,
        _NET_CHANGE_IN_CASH_ITEM_ID,
        first_year,
        {},
    )
    if net_change is None:
        net_change = _formula_evaluate_formula_simple(
            model,
            net_change_item,
            first_year,
            {},
        )
    if net_change is None:
        return False

    _writer_set_constant_override(
        beginning_item,
        first_year,
        float(end_cash) - float(net_change),
    )
    return True


def _normalize_projected_cash_roll_forward(model: FinancialModel) -> None:
    """Keep projected cash on the canonical statement roll-forward.

    The source template carries a first-projection override that mixes client
    funds into beginning corporate cash. Once real client-funds semantic rows
    are materialized, that override double counts restricted balances and
    breaks both the cash roll-forward and projected balance sheet. Projected
    beginning cash must always reference prior-period ending cash. FX remains
    a required cash-flow input: its declared template has no projected formula,
    and company-grounded policies own any sourced forecast assumption.
    """

    projection_periods = sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    )
    if not projection_periods:
        return

    try:
        beginning_cash = model.get_item(_CASH_BEGINNING_ITEM_ID)
    except KeyError:
        beginning_cash = None
    if beginning_cash is not None and beginning_cash.overrides:
        for period in projection_periods:
            beginning_cash.overrides.pop(period, None)
        if not beginning_cash.overrides:
            beginning_cash.overrides = None


def _normalize_three_statement_spine(model: FinancialModel) -> None:
    """Compile the canonical statement links without balance-sheet plugs.

    Every formula uses an existing presentation identity.  Required financing,
    capital-allocation, and other schedule inputs remain ordinary graph
    dependencies: if a later BM-5 slice has not supplied them, typed Missing
    propagation blocks the statement rather than manufacturing a balancing
    number.
    """

    projection_periods = sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    )
    historical_periods = sorted(
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    )
    if not projection_periods:
        return

    def _item(item_id: str):
        try:
            return model.get_item(item_id)
        except KeyError:
            return None

    cash_flow_net_income = _item(_CASH_FLOW_NET_INCOME_ITEM_ID)
    if cash_flow_net_income is not None:
        cash_flow_net_income.projected = FormulaSpec(
            type=FormulaType.ref,
            params={"source": LineItemRef(id=_INCOME_STATEMENT_NET_INCOME_ITEM_ID)},
            note="canonical_income_statement_to_cash_flow_link",
        )
        cash_flow_net_income.formula_periods = sorted(
            set(cash_flow_net_income.formula_periods or []) | set(projection_periods)
        )

    net_change = _item(_NET_CHANGE_IN_CASH_ITEM_ID)
    if net_change is not None:
        net_change.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="multi_term",
            params={
                "operands": [
                    "+",
                    LineItemRef(id=_OPERATING_CASH_FLOW_ITEM_ID),
                    LineItemRef(id=_INVESTING_CASH_FLOW_ITEM_ID),
                    LineItemRef(id=_FINANCING_CASH_FLOW_ITEM_ID),
                    LineItemRef(id=_CASH_FX_ITEM_ID),
                ]
            },
            note="canonical_cash_flow_net_change",
        )
        net_change.formula_periods = sorted(
            set(net_change.formula_periods or []) | set(projection_periods)
        )

    beginning_cash = _item(_CASH_BEGINNING_ITEM_ID)
    if beginning_cash is not None:
        beginning_cash.projected = FormulaSpec(
            type=FormulaType.ref,
            params={"source": LineItemRef(id=_CASH_END_ITEM_ID, t=-1)},
            note="canonical_cash_opening_from_prior_close",
        )
        beginning_cash.formula_periods = sorted(
            set(beginning_cash.formula_periods or []) | set(projection_periods)
        )

    ending_cash = _item(_CASH_END_ITEM_ID)
    if ending_cash is not None:
        ending_cash.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={
                "operands": [
                    "+",
                    LineItemRef(id=_CASH_BEGINNING_ITEM_ID),
                    LineItemRef(id=_NET_CHANGE_IN_CASH_ITEM_ID),
                ]
            },
            note="canonical_cash_ending_roll_forward",
        )
        ending_cash.formula_periods = sorted(
            set(ending_cash.formula_periods or []) | set(projection_periods)
        )

    # CF ending cash can include restricted/client cash but excludes marketable
    # securities in the combined BS cash line. The projected bridge carries the
    # reported difference; the investment flows that move it are appended by
    # `_wire_flow_counterpart_balances`, the one owner of investment-flow
    # destination (the counterpart balance, else this bridge).
    balance_sheet_cash = _item(_BALANCE_SHEET_CASH_ITEM_ID)
    cash_bridge = _item(_CASH_BRIDGE_ITEM_ID)
    if balance_sheet_cash is not None and cash_bridge is not None:
        cash_bridge.historical = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={
                "operands": [
                    "-",
                    LineItemRef(id=_CASH_END_ITEM_ID),
                    LineItemRef(id=_BALANCE_SHEET_CASH_ITEM_ID),
                ]
            },
            note="canonical_cash_flow_to_balance_sheet_cash_bridge",
        )
        cash_bridge.projected = FormulaSpec(
            type=FormulaType.roll_forward,
            subtype="schedule",
            params={
                "beginning": LineItemRef(id=_CASH_BRIDGE_ITEM_ID, t=-1),
                "additions": [],
                "subtractions": [],
            },
            note="canonical_cash_bridge_investment_roll_forward",
        )
        cash_bridge.formula_periods = sorted(
            set(cash_bridge.formula_periods or [])
            | set(historical_periods)
            | set(projection_periods)
        )
        balance_sheet_cash.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={
                "operands": [
                    "-",
                    LineItemRef(id=_CASH_END_ITEM_ID),
                    LineItemRef(id=_CASH_BRIDGE_ITEM_ID),
                ]
            },
            note="canonical_cash_flow_to_balance_sheet_link",
        )
        balance_sheet_cash.formula_periods = sorted(
            set(balance_sheet_cash.formula_periods or []) | set(projection_periods)
        )

    aoci = _item(_AOCI_SCHEDULE_ITEM_ID)
    if aoci is not None:
        # Only cash FX is modeled; do not rewrite filed OCI or invent other OCI.
        aoci.projected = FormulaSpec(
            type=FormulaType.roll_forward,
            subtype="schedule",
            params={
                "beginning": LineItemRef(id=_AOCI_SCHEDULE_ITEM_ID, t=-1),
                "additions": [LineItemRef(id=_CASH_FX_ITEM_ID)],
                "subtractions": [],
            },
            note="canonical_aoci_cash_fx_roll_forward",
        )
        aoci.formula_periods = sorted(
            set(aoci.formula_periods or []) | set(projection_periods)
        )

    supported_delta_periods = historical_periods[1:]
    for item_id in _SUPPORTED_HISTORICAL_DELTA_ITEM_IDS:
        item = _item(item_id)
        if item is None:
            continue
        balance_id, liability = {
            "tpl.fm.cash_flow.long_term_asset_2": (
                "tpl.fm.balance_sheet.long_term_asset_2",
                False,
            ),
            "tpl.fm.cash_flow.other_long_term_liabilities": (
                "tpl.fm.balance_sheet.other_long_term_liabilities",
                True,
            ),
        }[item_id]
        item.historical = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={
                "operands": [
                    "-",
                    LineItemRef(id=balance_id, t=0 if liability else -1),
                    LineItemRef(id=balance_id, t=-1 if liability else 0),
                ]
            },
            note="observed_working_capital_balance_delta",
        )
        item.formula_periods = sorted(
            set(supported_delta_periods) | set(projection_periods)
        )


def _normalize_projected_balance_sheet_roll_forward(model: FinancialModel) -> None:
    """Keep generic projected balance-sheet formulas complete and reconciling.

    The source template's projected liabilities formula omits long-term debt,
    and optional current-balance rows (other current assets, accrued
    compensation, the current-liability slots) lose populated historical
    balances. Repair those identities from accepted model rows instead of
    inserting company-specific constants. Company-grounded balance-sheet
    policy owns the PP&E depreciation subtraction and its historical basis.
    """

    projection_periods = sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    )
    historical_periods = sorted(
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    )
    if not projection_periods:
        return

    try:
        total_liabilities = model.get_item(_TOTAL_LIABILITIES_ITEM_ID)
    except KeyError:
        total_liabilities = None
    projected = getattr(total_liabilities, "projected", None)
    expr = projected.params.get("expr") if projected is not None else None
    args = expr.get("args") if isinstance(expr, dict) else None
    noncurrent = args[0] if isinstance(args, list) and args else None
    sum_args = noncurrent.get("args") if isinstance(noncurrent, dict) else None
    if isinstance(sum_args, list) and not any(
        isinstance(ref, LineItemRef) and ref.id == _LONG_TERM_DEBT_ITEM_ID
        for ref in sum_args
    ):
        sum_args.append(LineItemRef(id=_LONG_TERM_DEBT_ITEM_ID))

    if historical_periods:
        latest_historical = historical_periods[-1]
        for item_id in _CARRY_FORWARD_BALANCE_ITEM_IDS:
            try:
                item = model.get_item(item_id)
            except KeyError:
                continue
            if item.projected is not None:
                continue
            latest_value = _formula_lookup_formula_value(
                model,
                item_id,
                latest_historical,
                {},
            )
            if latest_value is None:
                continue
            item.projected = FormulaSpec(
                type=FormulaType.ref,
                subtype="carry_forward",
                params={"source": LineItemRef(id=item_id, t=-1)},
                note="carry_forward_latest_accepted_historical_balance",
            )
            item.formula_periods = sorted(
                set(item.formula_periods or []) | set(projection_periods)
            )

    # A balance-sheet population schedule the template marks for only its
    # first projected columns stops mid-horizon after remap. Extend that
    # schedule and the face line that reads it. The catalog names the rows.
    _extend_truncated_balance_schedules(model, projection_periods)


def _covers_only_a_projection_prefix(
    formula_periods: list[int] | None,
    projection_periods: list[int],
) -> bool:
    """The row's forecast years are a leading slice of the horizon, not all of it."""

    if not formula_periods or not projection_periods:
        return False
    owned = {int(period) for period in formula_periods}
    covered = [period for period in projection_periods if period in owned]
    if not covered or len(covered) == len(projection_periods):
        return False
    return covered == projection_periods[: len(covered)]


def _item_covers_only_a_projection_prefix(
    model: FinancialModel,
    item_id: str,
    projection_periods: list[int],
) -> bool:
    try:
        item = model.get_item(item_id)
    except KeyError:
        return False
    if item.projected is None:
        return False
    return _covers_only_a_projection_prefix(item.formula_periods, projection_periods)


def _extend_truncated_balance_schedules(
    model: FinancialModel,
    projection_periods: list[int],
) -> None:
    """Keep a truncated balance schedule, and the face line that reads it, on the horizon.

    The imported template marks some balance-sheet population schedules for
    only their first projected columns. Remap keeps that leading slice, so a
    face line summed through the horizon loses the schedule mid-forecast.
    The rows are the catalog's balance-sheet population schedules, not a
    named slot.
    """

    from .required_role_catalog import face_lines, load_required_role_catalog

    catalog = load_required_role_catalog()
    item_ids: list[str] = []
    for row in catalog.roles_on_statement("balance_sheet"):
        schedules = [
            item_id
            for item_id in row.population_items
            if "{ticker}" not in item_id
            and _item_covers_only_a_projection_prefix(
                model, item_id, projection_periods
            )
        ]
        if not schedules:
            continue
        item_ids.extend(schedules)
        item_ids.extend(face_lines(row))
    horizon = set(projection_periods)
    for item_id in dict.fromkeys(item_ids):
        try:
            item = model.get_item(item_id)
        except KeyError:
            continue
        if item.projected is None or item.formula_periods is None:
            continue
        item.formula_periods = sorted(set(item.formula_periods) | horizon)


def _has_existing_imported_historicals(model: FinancialModel, historical_periods: list[int]) -> bool:
    historical_set = {int(period) for period in historical_periods}
    for item in _model_iter_items(model):
        if not item.required_role:
            continue
        if item.values is not None:
            for year, cell in item.values.values.items():
                if (
                    int(year) in historical_set
                    and cell.provenance in {
                        ValueProvenance.imported_fmp,
                        ValueProvenance.imported_vendor,
                        ValueProvenance.imported_edgar,
                    }
                ):
                    return True
        if item.overrides:
            for year, spec in item.overrides.items():
                if int(year) in historical_set and spec.type is FormulaType.constant:
                    return True
    return False


__all__ = [
    "_CASH_BEGINNING_ITEM_ID",
    "_CASH_END_ITEM_ID",
    "_CASH_FX_ITEM_ID",
    "_NET_CHANGE_IN_CASH_ITEM_ID",
    "_has_existing_imported_historicals",
    "_normalize_projected_balance_sheet_roll_forward",
    "_normalize_projected_cash_roll_forward",
    "_normalize_three_statement_spine",
    "_seed_cash_beginning_of_period",
]
