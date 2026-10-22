"""Canonical annual software, deferred-cost, lease, debt, and equity schedules."""

from __future__ import annotations


from collections.abc import Collection, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
)
from industry_slice_core.industry_types import Unit
from .capitalized_software_inclusion import (
    capitalized_software_included,
    capitalized_software_included_at_terminal,
)
from .build_semantic_rows import (
    _add_formula_ref_after as _semantic_add_formula_ref_after,
    _insert_line_item_after,
)
from .build_working_capital_policy import (
    _extend_projected_formula_periods,
    _own_line_working_capital_items,
)
from .schedule_input_audit import SCHEDULE_INPUT_REQUIREMENTS
from schema.provider_port.adapters.edgar.wire.non_gaap_bridge_v2 import bridge_row_key

if TYPE_CHECKING:
    from .role_disposition import RoleDispositionState, RoleDispositionTable, RoleItems


_SOFTWARE_BALANCE_SCHEDULE_ITEM_ID = (
    "tpl.a.capital_investments.software_investment_if_applicable"
)
_SOFTWARE_ADDITIONS_ITEM_ID = (
    "tpl.a.capital_investments.add_capitalized_software_costs_if_applicable"
)
_SOFTWARE_ADDITIONS_RATE_ITEM_ID = "tpl.a.capital_investments.of_revenue"
_SOFTWARE_AMORTIZATION_ITEM_ID = (
    "tpl.a.depreciation_amortization."
    "amortization_of_capitalized_internal_use_software"
)
_SOFTWARE_LESS_AMORTIZATION_ITEM_ID = (
    "tpl.a.capital_investments.less_amortization"
)
_SOFTWARE_BALANCE_SHEET_ITEM_ID = (
    "tpl.fm.balance_sheet.software_investment_if_applicable"
)
_SOFTWARE_INVESTING_CASH_ITEM_ID = (
    "tpl.fm.cash_flow.capitalized_software_costs_if_applicable"
)
_DEPRECIATION_AND_AMORTIZATION_SCHEDULE_ITEM_ID = (
    "tpl.a.depreciation_amortization.depreciation_and_amortization_m"
)

_OTHER_WORKING_CAPITAL_CASH_ITEM_ID = (
    "tpl.fm.cash_flow.changes_in_operating_assets_and_liabilities"
)

_DEBT_INTEREST_RATE_ITEM_ID = "tpl.a.other_income_interest.interest_rate"
_DEBT_INTEREST_EXPENSE_ITEM_ID = "tpl.a.other_income_interest.interest_expense"
_INCOME_STATEMENT_INTEREST_EXPENSE_ITEM_ID = (
    "tpl.fm.income_statement.interest_expense"
)
_BALANCE_SHEET_CURRENT_DEBT_ITEM_ID = (
    "tpl.fm.balance_sheet.current_portion_of_long_term_debt"
)
_BALANCE_SHEET_LONG_TERM_DEBT_ITEM_ID = "tpl.fm.balance_sheet.long_term_debt"

_BASIC_SHARES_ITEM_ID = "tpl.a.dividends_shares.basic_shares_outstanding"
_DILUTED_SHARES_ITEM_ID = "tpl.a.dividends_shares.diluted_shares_outstanding"
_END_OF_PERIOD_SHARES_ITEM_ID = (
    "tpl.a.dividends_shares.end_of_period_shares_outstanding"
)
_SHARES_ISSUED_ITEM_ID = "tpl.a.dividends_shares.shares_issued"
_REPURCHASE_AMOUNT_ITEM_ID = "tpl.a.dividends_shares.total_amount"
_INCOME_STATEMENT_DILUTED_SHARES_ITEM_ID = (
    "tpl.fm.income_statement.diluted_shares_outstanding_m"
)
_TAX_SCHEDULE_DILUTED_SHARES_ITEM_ID = (
    "tpl.a.tax_net_income.diluted_shares_outstanding"
)
_SHARE_DILUTION_RATE_ITEM_ID = "tpl.a.tax_net_income.y_y_chg"
_REPURCHASE_CASH_ITEM_ID = "tpl.fm.cash_flow.share_repurchases"
_DIVIDEND_CASH_ITEM_ID = "tpl.fm.cash_flow.dividends"

_OPERATING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.operating_cash_flow"
_INVESTING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.investing_cash_flow"
_FINANCING_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.financing_cash_flow"
_EQUITY_FINANCING_CASH_ITEM_ID = (
    "tpl.fm.cash_flow.proceeds_or_payment_for_equity"
)
_PRETAX_INCOME_ITEM_ID = "tpl.fm.income_statement.income_before_income_taxes"
_OTHER_INCOME_ITEM_ID = "tpl.fm.income_statement.other_income_expense"

_UNLEVERED_FREE_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.unlevered_free_cash_flow"
_DCF_FCF_PROJECTION_ITEM_ID = "tpl.v.dcf.fcf_projection"
_REPORTED_INTEREST_EXPENSE_ITEM_ID = "tpl.fm.adjusted_earnings.interest_expense"
# Float-comparison epsilon only.  Balances and interest reach the model at the
# millions scale from filings reported to the nearest thousand, so this sits six
# orders of magnitude below the smallest figure an issuer can report: a value
# inside it is arithmetic noise, not a position.
_DEBT_OBSERVATION_EPSILON = 1e-9


def _optional_item(model: FinancialModel, item_id: str) -> LineItem | None:
    try:
        return model.get_item(item_id)
    except KeyError:
        return None


def _projection_periods(model: FinancialModel) -> list[int]:
    return sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    )


def _historical_periods(model: FinancialModel) -> list[int]:
    return sorted(
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    )


def debt_interest_rate_periods(model: FinancialModel) -> list[int]:
    """The debt rate's lagged axis, before valuation retirement removes zero-debt periods.

    The first historical year has no opening balance, so no average and no rate.
    ``retire_unused_cost_of_debt`` owns the resulting period declaration.
    """
    return sorted(set(_historical_periods(model)[1:]) | set(_projection_periods(model)))


def _set_projection_periods(item: LineItem, periods: list[int]) -> None:
    item.formula_periods = sorted(set(item.formula_periods or []) | set(periods))


def _bind_or_insert_item(
    model: FinancialModel,
    *,
    item_id: str,
    anchor_item_id: str,
    label: str,
    item_type: ItemType,
    unit: Unit,
) -> LineItem:
    item = _optional_item(model, item_id)
    if item is not None:
        item.label = label
        item.item_type = item_type
        item.unit = unit
        return item
    return _insert_line_item_after(
        model,
        anchor_item_id=anchor_item_id,
        new_item=LineItem(
            id=item_id,
            label=label,
            row=0,
            item_type=item_type,
            unit=unit,
        ),
    )


def _add_formula_ref_after(
    model: FinancialModel,
    *,
    formula_item_id: str,
    after_id: str,
    new_id: str,
) -> bool:
    inserted = bool(
        _semantic_add_formula_ref_after(
            model,
            formula_item_id=formula_item_id,
            after_id=after_id,
            new_id=new_id,
        )
    )
    # Schedule rows are forecast compiler additions.  Historical statement
    # subtotals are reported facts and must not acquire a new leading-period
    # dependency merely because the projected subtotal needs the schedule.
    historical = model.get_item(formula_item_id).historical
    if historical is not None:
        _remove_formula_ref(historical.params, new_id)
    return inserted


def _remove_formula_ref(value: Any, item_id: str) -> None:
    if isinstance(value, list):
        value[:] = [
            nested
            for nested in value
            if not (
                isinstance(nested, LineItemRef)
                and nested.id == item_id
            )
            and not (
                isinstance(nested, dict)
                and _formula_ref_ids(nested) == {item_id}
            )
        ]
        for nested in value:
            _remove_formula_ref(nested, item_id)
    elif isinstance(value, dict):
        for nested in value.values():
            _remove_formula_ref(nested, item_id)


def _formula_ref_ids(value: Any) -> set[str]:
    if isinstance(value, LineItemRef):
        return {value.id}
    if isinstance(value, list):
        return set().union(*(_formula_ref_ids(nested) for nested in value), set())
    if isinstance(value, dict):
        return set().union(*(_formula_ref_ids(nested) for nested in value.values()), set())
    return set()


def _extend(model: FinancialModel, item_id: str, periods: list[int]) -> bool:
    return bool(_extend_projected_formula_periods(model, item_id, periods))


def _software_inclusion_fact(
    model: FinancialModel, item_id: str, period: int,
) -> FormulaSpec | None:
    """Read the cited software line's own period fact, not a missing number."""
    item = _optional_item(model, item_id)
    fact = None if item is None else (item.overrides or {}).get(period)
    if (
        fact is not None
        and fact.subtype == "not_separately_presented"
        and fact.params.get("included_in")
    ):
        return fact
    return None


def _software_dispositions(
    model: FinancialModel, period: int,
) -> tuple[tuple[str | None, ...], tuple[str | None, ...]]:
    """The period's software balance and amortization dispositions."""

    def disposition(item_id: str) -> str | None:
        fact = _software_inclusion_fact(model, item_id, period)
        return None if fact is None else fact.subtype

    return (
        tuple(
            disposition(item_id)
            for item_id in (
                _SOFTWARE_BALANCE_SCHEDULE_ITEM_ID,
                _SOFTWARE_BALANCE_SHEET_ITEM_ID,
            )
        ),
        (disposition(_SOFTWARE_AMORTIZATION_ITEM_ID),),
    )


def _software_inside_ppe(model: FinancialModel, period: int) -> bool:
    """Whether ``period``'s software balance and amortization are both cited
    inclusions (``capitalized_software_included``); a historical lineage test."""

    return capitalized_software_included(*_software_dispositions(model, period))


def _software_inside_ppe_at_terminal(model: FinancialModel) -> bool:
    """Whether the model builds the inclusive row: the shared terminal-year
    decision the applicability rule calls over the coverage's dispositions."""

    by_year = {
        year: _software_dispositions(model, year)
        for year in _historical_periods(model)
    }
    return capitalized_software_included_at_terminal(
        {year: balance for year, (balance, _) in by_year.items()},
        {year: amortization for year, (_, amortization) in by_year.items()},
    )


def _as_protected(
    protected_cells: Collection[tuple[str, int]],
) -> frozenset[tuple[str, int]]:
    return frozenset((str(item_id), int(period)) for item_id, period in protected_cells)


def _protected_cell(
    protected: Collection[tuple[str, int]], item_id: str, period: int,
) -> bool:
    return (item_id, int(period)) in protected


def _protected_line(
    model: FinancialModel,
    protected: Collection[tuple[str, int]],
    item_id: str,
) -> bool:
    """Whether every declared period of ``item_id`` is an authored cell.

    ``reconcile_inclusions`` expands a protected line to every historical and
    projection period. That line's projected formula is not a source cell.
    """

    periods = (
        *model.time_structure.historical_periods,
        *model.time_structure.projection_periods,
    )
    return bool(periods) and all(
        (item_id, int(period)) in protected for period in periods
    )


_SOFTWARE_BALANCE_ROLE = "statement.bs.capitalized_software"
_SOFTWARE_ADDITIONS_ROLE = "statement.cf.capitalized_software_additions"
_SOFTWARE_AMORTIZATION_ROLE = "statement.cf.capitalized_software_amortization"
_SOFTWARE_BEGINNING_ITEM_ID = (
    "tpl.a.depreciation_amortization.beg_capitalized_internal_use_software"
)
_SOFTWARE_ENDING_ITEM_ID = (
    "tpl.a.depreciation_amortization.end_capitalized_internal_use_software"
)
_SOFTWARE_AVERAGE_ITEM_ID = (
    "tpl.a.depreciation_amortization.avg_capitalized_internal_use_software"
)
_PPE_ADDITIONS_ITEM_ID = (
    "tpl.a.capital_investments.add_purchases_of_property_and_equipment"
)
_TOTAL_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"
_PPE_ADDITIONS_RATE_ITEM_ID = (
    "tpl.a.capital_investments.property_and_equipment_pct_revenue"
)
# A software line with a pinned source or an owed-and-missing history still
# forecasts: the forecast needs the authored or observed rate, not a filed cell.
_SOFTWARE_FORECAST_STATES = frozenset({"bound", "unbound"})


def _terminal_state(table: RoleDispositionTable, role: str) -> RoleDispositionState:
    """The role's state in the latest historical period the table carries."""

    terminal = max(
        table.by_role(role), key=lambda row: int(row.period_id.removeprefix("FY"))
    )
    return terminal.state


def _software_included_at_terminal(table: RoleDispositionTable) -> bool:
    """Whether balance and amortization are both cited inclusions at the terminal year."""

    def by_year(role: str) -> dict[int, tuple[str, ...]]:
        return {
            int(row.period_id.removeprefix("FY")): (row.state,)
            for row in table.by_role(role)
        }

    return capitalized_software_included_at_terminal(
        by_year(_SOFTWARE_BALANCE_ROLE), by_year(_SOFTWARE_AMORTIZATION_ROLE),
    )


def _carry_software_inclusion_fact(
    model: FinancialModel,
    fact: FormulaSpec,
    item_ids: tuple[str, ...],
    projection_periods: list[int],
) -> list[str]:
    """Forecast each cited software line as the filing's own inclusion fact, not a number."""

    carried: list[str] = []
    for item_id in item_ids:
        item = _optional_item(model, item_id)
        if item is None:
            continue
        marker = fact.model_copy(deep=True)
        marker.note = "capitalized_software_inclusion_fact"
        item.projected = marker
        _set_projection_periods(item, projection_periods)
        carried.append(item_id)
    return carried



def _drop_software_terms(
    model: FinancialModel, total_id: str, item_ids: tuple[str, ...],
) -> None:
    """The total's forecast without the software terms a cited inclusion folds into it."""

    total = _optional_item(model, total_id)
    if total is None or total.projected is None:
        return
    lowered = total.projected.without(
        lambda ref: ref.role in ("additive", "subtractive") and ref.ref.id in item_ids
    )
    if lowered is not None:
        total.projected = lowered


def complete_capitalized_software_schedule(
    model: FinancialModel,
    *,
    historical: tuple[int, ...],
    projected: tuple[int, ...],
    role_dispositions: RoleDispositionTable,
    role_items: Mapping[str, RoleItems],
) -> list[str]:
    """Lower the template's capitalized-software schedule by the terminal state
    of the balance, additions and amortization roles; return the lowered item ids.

    The template declares the schedule whole (balance roll-forward over
    additions less amortization, additions ``revenue x of_revenue``, amortization
    ``beginning x rate``) and the PP&E additions as physical spend only
    (``revenue x ppe_pct``). This function only applies the dispositions:

    * balance/additions ``bound`` or ``unbound``: the schedule
      forecasts as declared. An additions or amortization role that does not
      forecast leaves the roll-forward (``FormulaSpec.without``): the
      roll-forward carries only the flows whose role forecasts.
    * ``not_separately_presented``: the inclusion lanes. Software lines carry
      the filing's inclusion fact; a cited additions line is funded through the
      PP&E additions, which then carry both rates
      (``revenue x ppe_pct + revenue x of_revenue``). Amortization is
      carried as a fact only where balance and amortization are both included
      (``capitalized_software_included_at_terminal``); an included amortization
      beside a presented balance still rolls forward on its numeric schedule
      (``projected_schedule_roll_forward_mismatch`` requires the roll to carry it).
    * ``not_applicable``: the cited cells stay and the readers lower their term.
    * ``unowed``: the role's items retire.

    Every other state leaves the PP&E additions as the template declares them.
    Readers of an included or cited line (``total_assets``, ``investing_cash_flow``,
    ``free_cash_flow``, ``depreciation_and_amortization_m``) lose the term where
    the cell is retired or lowered, never here.
    """

    projection_periods = sorted(int(period) for period in projected)
    historical_periods = sorted(int(period) for period in historical)
    balance_state = _terminal_state(role_dispositions, _SOFTWARE_BALANCE_ROLE)
    additions_state = _terminal_state(role_dispositions, _SOFTWARE_ADDITIONS_ROLE)
    amortization_state = _terminal_state(role_dispositions, _SOFTWARE_AMORTIZATION_ROLE)
    included = _software_included_at_terminal(role_dispositions)
    forecast_balance = balance_state in _SOFTWARE_FORECAST_STATES
    forecast_additions = additions_state in _SOFTWARE_FORECAST_STATES
    forecast_amortization = amortization_state in _SOFTWARE_FORECAST_STATES or (
        amortization_state == "not_separately_presented" and not included
    )

    wired: list[str] = []
    if balance_state != "unowed":
        # The population binds the filing's cell directly onto the schedule
        # row; the template's historical bridge/zero would make an absent or
        # cited balance look reported. An unowed balance keeps the bridge: it
        # is how the removed face row finds its schedule.
        model.get_item(_SOFTWARE_BALANCE_SCHEDULE_ITEM_ID).historical = None
        face = model.get_item(_SOFTWARE_BALANCE_SHEET_ITEM_ID)
        if face.historical is not None and face.historical.type is FormulaType.constant:
            face.historical = None
    if forecast_balance:
        unforecast = (
            *(() if forecast_additions else (_SOFTWARE_ADDITIONS_ITEM_ID,)),
            *(() if forecast_amortization else (_SOFTWARE_LESS_AMORTIZATION_ITEM_ID,)),
        )
        if unforecast:
            _drop_software_terms(model, _SOFTWARE_BALANCE_SCHEDULE_ITEM_ID, unforecast)
            wired.append(_SOFTWARE_BALANCE_SCHEDULE_ITEM_ID)

    terminal = historical_periods[-1] if historical_periods else None

    def fact(item_id: str) -> FormulaSpec | None:
        return None if terminal is None else _software_inclusion_fact(model, item_id, terminal)

    def lane(role: str, fact_item_id: str, extra: tuple[str, ...] = ()) -> FormulaSpec | None:
        cited = fact(fact_item_id)
        if cited is not None:
            wired.extend(
                _carry_software_inclusion_fact(
                    model, cited,
                    tuple(dict.fromkeys((*role_items[role].all_items(), *extra))),
                    projection_periods,
                )
            )
        return cited

    if balance_state == "not_separately_presented":
        lane(
            _SOFTWARE_BALANCE_ROLE, _SOFTWARE_BALANCE_SCHEDULE_ITEM_ID,
            (_SOFTWARE_BEGINNING_ITEM_ID, _SOFTWARE_ENDING_ITEM_ID, _SOFTWARE_AVERAGE_ITEM_ID),
        )
    if additions_state == "not_separately_presented":
        # Both controls describe physical/software investment even when the
        # filing prints only their combined capex. Do not drop software spend
        # merely because its separate statement child is nonnumeric.
        software_in_capex = (
            lane(_SOFTWARE_ADDITIONS_ROLE, _SOFTWARE_INVESTING_CASH_ITEM_ID) is not None
        )
        if software_in_capex:
            # The template's PP&E additions are physical spend only. The filing
            # funds software spend through that line, so it carries both revenue rates.
            model.get_item(_PPE_ADDITIONS_ITEM_ID).projected = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="inclusive_capital_investment",
                params={
                    "expr": {
                        "op": "+",
                        "args": [
                            {
                                "op": "*",
                                "args": [
                                    LineItemRef(id=_TOTAL_REVENUE_ITEM_ID),
                                    LineItemRef(id=_PPE_ADDITIONS_RATE_ITEM_ID),
                                ],
                            },
                            {
                                "op": "*",
                                "args": [
                                    LineItemRef(id=_TOTAL_REVENUE_ITEM_ID),
                                    LineItemRef(id=_SOFTWARE_ADDITIONS_RATE_ITEM_ID),
                                ],
                            },
                        ],
                    }
                },
                note="capital investment from the physical and software revenue rates",
            )
            wired.append(_PPE_ADDITIONS_ITEM_ID)
    else:
        software_in_capex = False
    if included:
        lane(
            _SOFTWARE_AMORTIZATION_ROLE, _SOFTWARE_AMORTIZATION_ITEM_ID,
            (_SOFTWARE_LESS_AMORTIZATION_ITEM_ID,),
        )

    # A lane whose lines are carried inclusion facts leaves the totals that sum
    # them: the amount is already inside the host line.
    if included:
        _drop_software_terms(
            model, _DEPRECIATION_AND_AMORTIZATION_SCHEDULE_ITEM_ID,
            (_SOFTWARE_AMORTIZATION_ITEM_ID, _SOFTWARE_LESS_AMORTIZATION_ITEM_ID),
        )
    if software_in_capex:
        for total_id in (_INVESTING_CASH_FLOW_ITEM_ID, "tpl.fm.cash_flow.free_cash_flow"):
            _drop_software_terms(
                model, total_id,
                (_SOFTWARE_INVESTING_CASH_ITEM_ID, _SOFTWARE_ADDITIONS_ITEM_ID),
            )
    _extend(model, _DEPRECIATION_AND_AMORTIZATION_SCHEDULE_ITEM_ID, projection_periods)
    _extend(model, _INVESTING_CASH_FLOW_ITEM_ID, projection_periods)
    return list(dict.fromkeys(wired))


def _compose_projected_operating_working_capital(
    model: FinancialModel,
    projection_periods: list[int],
) -> dict[str, list[str]]:
    """Projected cash from operations sums each operating balance movement once.

    The own-line rows (the catalog's ``change_of`` cash-flow roles) follow the
    other-working-capital aggregate, whose leaf set is every other operating
    balance (`_apply_other_working_capital_derivation`). The deferred-contract
    -cost cash rows are leaves, so they are presented but not summed.
    """

    _balance_items, cash_items = _own_line_working_capital_items()
    after_id = _OTHER_WORKING_CAPITAL_CASH_ITEM_ID
    wired: list[str] = []
    for cash_item_id in cash_items:
        _add_formula_ref_after(
            model,
            formula_item_id=_OPERATING_CASH_FLOW_ITEM_ID,
            after_id=after_id,
            new_id=cash_item_id,
        )
        after_id = cash_item_id
        wired.append(cash_item_id)
    _extend(model, _OPERATING_CASH_FLOW_ITEM_ID, projection_periods)
    return {"wired": wired, "required_inputs": []}


_INTEREST_INCLUSION_NOTE = "interest_row_included_in_aggregate"


def _interest_inclusion_fact(
    model: FinancialModel,
    period: int,
) -> FormulaSpec | None:
    """The interest row's fact for one period, when that fact is an inclusion.

    The reported line is ``statement.is.interest_expense``. A
    not-separately-presented marker is the fact: interest lives inside
    ``included_in``, and it is not a number. A presented amount is not this
    fact.
    """

    item = _optional_item(model, _REPORTED_INTEREST_EXPENSE_ITEM_ID)
    if item is None:
        return None
    spec = (item.overrides or {}).get(period)
    if (
        spec is None
        or spec.subtype != "not_separately_presented"
        or not spec.params.get("included_in")
    ):
        return None
    return spec


def _inclusion_marker(fact: FormulaSpec) -> FormulaSpec:
    """The income-statement interest line carrying the row's inclusion fact."""

    metadata = dict(fact.params.get("_not_separately_presented") or {})
    metadata.setdefault("required_role", "statement.is.interest_expense")
    return FormulaSpec(
        type=FormulaType.raw,
        subtype="not_separately_presented",
        params={
            "formula": "=NA()",
            "included_in": fact.params["included_in"],
            "absence_citation": dict(fact.params.get("absence_citation") or {}),
            "_not_separately_presented": metadata,
        },
        note=_INTEREST_INCLUSION_NOTE,
    )


def apply_disclosed_interest_bridge(
    model: FinancialModel,
    historical_basis_facts: Any,
) -> None:
    """The face line can be an inclusion while the bridge still discloses interest.

    ``statement.is.interest_expense`` not separately presented means the income
    statement has no interest line; it does not erase an issuer-disclosed
    Adjusted EBITDA bridge amount. That amount is the reported interest the
    unlevered add-back and ``unseparable_interest_periods`` read
    (``tpl.fm.adjusted_earnings.interest_expense``). The income-statement face
    line stays the inclusion marker.
    """

    from .models import ValueCell, ValueProvenance, ValueSeries

    if not model._index:
        model.build_index()
    item = _optional_item(model, _REPORTED_INTEREST_EXPENSE_ITEM_ID)
    if item is None:
        return
    disclosed: dict[int, tuple[str, Decimal]] = {}
    component_signs = {
        fact_id: sign
        for fact in historical_basis_facts or ()
        if getattr(fact, "reconciliation", None) is not None
        for fact_id, sign in zip(
            fact.reconciliation.component_fact_ids,
            fact.reconciliation.component_signs,
            strict=True,
        )
    }
    for fact in historical_basis_facts or ():
        line = getattr(fact, "line", None)
        if line is None or bridge_row_key(
            str(getattr(line, "tag", "")).removeprefix("issuer:")
        ) != ("adjusted_ebitda", "interest"):
            continue
        if getattr(line, "statement_role", None) != "nongaapreconciliation":
            continue
        period_id = str(getattr(fact, "period_id", "") or "")
        if not period_id.startswith("FY"):
            continue
        value = getattr(fact, "value", None)
        if value is None:
            continue
        year = int(period_id.removeprefix("FY"))
        source_id = str(getattr(fact, "source_id", "") or "")
        signed_val = component_signs.get(
            getattr(fact, "fact_id", ""), 1
        ) * Decimal(str(value))
        prev = disclosed.get(year)
        if prev is not None and prev[0] == source_id:
            disclosed[year] = (source_id, prev[1] + signed_val)
        else:
            disclosed[year] = (source_id, signed_val)
    if not disclosed:
        return
    if item.overrides is None:
        item.overrides = {}
    if item.values is None:
        item.values = ValueSeries()
    for year, (_src_id, amount) in disclosed.items():
        item.overrides[year] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": float(amount)},
            note="issuer_disclosed_adjusted_ebitda_bridge_interest",
        )
        item.values.values[year] = ValueCell(
            period=year,
            value=float(amount),
            provenance=ValueProvenance.imported_edgar,
        )




#: Param on a lowered projected formula holding the declared formula it replaced
#: (and whether the lowering was chosen for a cited-absent interest), so the next
#: call starts from the declared formula and reads the facts again.
_INTEREST_INCLUSION_RESTORE_KEY = "_interest_inclusion_projection"


def _declared_projection(spec: FormulaSpec) -> FormulaSpec:
    """The formula ``spec`` replaced, or ``spec`` itself when it is not a lowering."""

    restore = spec.params.get(_INTEREST_INCLUSION_RESTORE_KEY)
    if restore is None:
        return spec
    return FormulaSpec.model_validate_json(restore["declared"])


def _lowered_projection(
    lowering: FormulaSpec,
    declared: FormulaSpec,
    *,
    cited_absent: bool = False,
) -> FormulaSpec:
    restore: dict[str, Any] = {"declared": declared.model_dump_json()}
    if cited_absent:
        restore["cited_absent"] = True
    return lowering.model_copy(
        update={"params": {**lowering.params, _INTEREST_INCLUSION_RESTORE_KEY: restore}},
        deep=True,
    )


def lower_interest_inclusion(
    model: FinancialModel,
    *,
    protected_cells: Collection[tuple[str, int]] = (),
    evidenced_absences: Collection[tuple[str, int]] = (),
) -> None:
    """Apply the interest row's current fact to the template's other-income formulas.

    The template declares other income (expense), net as the ex-interest
    control less the income-statement interest line, and that line as the debt
    schedule's interest. Every call starts from those declared formulas (a
    lowered projection keeps the formula it replaced in
    ``_interest_inclusion_projection``) and lowers only what the facts now say,
    so a fact that is cleared or becomes a presented amount puts the declared
    formulas back. The forecast reads the fact in the last historical period,
    the inclusion machinery's per-period rule at the seam: a period follows its
    own fact, and a forecast period has no filing, so it reads the fact it
    carries.

    An inclusion means interest already lives in the named aggregate (other
    income, for the filing that never states interest on its own line): the
    income-statement interest line carries that fact, and other income (expense),
    net carries its filed definition, the prior period's filed line, instead of
    the ex-interest control less interest. The control's own history reads the
    interest operand, which is not a number here, so it cannot carry the forecast.
    A cited not-separately-presented fact on the face interest line, or an
    interest cited absent in the closing year (``evidenced_absences`` are the
    ``(item_id, period)`` cells the source cites ``not_applicable_evidenced``;
    the model holds no cell there, so the lowering records the choice and a
    later call without the citations keeps it while the closing year still
    reports no interest), lowers other income the same way. A presented amount
    keeps the template's subtraction.

    Historical periods are the same read, one period at a time. A protected
    period keeps its value and formula, and a protected line's projected
    formula is untouched.
    """

    interest = _optional_item(model, _INCOME_STATEMENT_INTEREST_EXPENSE_ITEM_ID)
    other_net = _optional_item(model, _OTHER_INCOME_ITEM_ID)
    if interest is None or other_net is None:
        return
    protected = _as_protected(protected_cells)
    historical = _historical_periods(model)
    for period in historical:
        if _protected_cell(protected, interest.id, period):
            continue
        fact = _interest_inclusion_fact(model, period)
        if fact is not None:
            if interest.overrides is None:
                interest.overrides = {}
            interest.overrides[period] = _inclusion_marker(fact)
            if interest.values is not None:
                interest.values.values.pop(period, None)
    closing = historical[-1] if historical else None
    carried = _interest_inclusion_fact(model, closing) if closing is not None else None
    face_fact = (interest.overrides or {}).get(closing) if closing is not None else None
    face_nsp = (
        carried is None
        and face_fact is not None
        and face_fact.subtype == "not_separately_presented"
    )
    # A filing that evidences no interest expense in the closing year gives the
    # forecast nothing to subtract from the ex-interest control: that control
    # is the filed line plus an interest that is not there.
    held_absent = (
        other_net.projected is not None
        and (other_net.projected.params.get(_INTEREST_INCLUSION_RESTORE_KEY) or {}).get(
            "cited_absent", False
        )
    )
    absent = (
        closing is not None
        and closing not in _observed_series(model, _REPORTED_INTEREST_EXPENSE_ITEM_ID)
        and (
            held_absent
            or any(
                (item_id, closing) in evidenced_absences
                for item_id in (
                    _REPORTED_INTEREST_EXPENSE_ITEM_ID,
                    _INCOME_STATEMENT_INTEREST_EXPENSE_ITEM_ID,
                )
            )
        )
    )
    included = carried if carried is not None else face_fact if face_nsp else None

    if interest.projected is not None and not _protected_line(model, protected, interest.id):
        declared = _declared_projection(interest.projected)
        interest.projected = (
            declared if included is None
            else _lowered_projection(_inclusion_marker(included), declared)
        )
    # Interest already lives in the filed line (or does not exist): that line is
    # the definition that carries.
    if other_net.projected is not None and not _protected_line(model, protected, other_net.id):
        declared = _declared_projection(other_net.projected)
        other_net.projected = (
            _lowered_projection(
                FormulaSpec(
                    type=FormulaType.ref,
                    subtype="carry_forward",
                    params={"source": LineItemRef(id=_OTHER_INCOME_ITEM_ID, t=-1)},
                    note="carry_forward_filed_other_income_expense_including_interest",
                ),
                declared,
                cited_absent=included is None,
            )
            if included is not None or absent
            else declared
        )


def _complete_share_and_capital_allocation_schedules(
    model: FinancialModel,
    projection_periods: list[int],
) -> dict[str, list[str]]:
    wired: list[str] = []
    basic_shares = _bind_or_insert_item(
        model,
        item_id=_BASIC_SHARES_ITEM_ID,
        anchor_item_id=_END_OF_PERIOD_SHARES_ITEM_ID,
        label="Basic weighted-average shares outstanding",
        item_type=ItemType.input,
        unit=Unit.share_count,
    )
    shares_issued = model.get_item(_SHARES_ISSUED_ITEM_ID)
    end_of_period = model.get_item(_END_OF_PERIOD_SHARES_ITEM_ID)
    tax_diluted = model.get_item(_TAX_SCHEDULE_DILUTED_SHARES_ITEM_ID)
    tax_diluted.projected = FormulaSpec(
        type=FormulaType.growth,
        subtype="compound",
        params={
            "base": LineItemRef(
                id=_TAX_SCHEDULE_DILUTED_SHARES_ITEM_ID,
                t=-1,
            ),
            "rate": LineItemRef(id=_SHARE_DILUTION_RATE_ITEM_ID),
        },
        note="canonical_diluted_share_count_from_net_dilution_control",
    )
    diluted = model.get_item(_DILUTED_SHARES_ITEM_ID)
    diluted.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=_TAX_SCHEDULE_DILUTED_SHARES_ITEM_ID)},
        note="canonical_net_dilution_to_share_schedule",
    )
    income_statement_diluted = model.get_item(
        _INCOME_STATEMENT_DILUTED_SHARES_ITEM_ID
    )
    income_statement_diluted.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=_DILUTED_SHARES_ITEM_ID)},
        note="canonical_net_dilution_to_income_statement",
    )
    repurchase_cash = model.get_item(_REPURCHASE_CASH_ITEM_ID)
    repurchase_cash.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="negated_ref",
        params={
            "source": LineItemRef(id=_REPURCHASE_AMOUNT_ITEM_ID),
            "negate": True,
        },
        note="share_repurchase_capital_allocation_financing_cash",
    )
    for item in (
        basic_shares,
        shares_issued,
        end_of_period,
        diluted,
        tax_diluted,
        income_statement_diluted,
        repurchase_cash,
    ):
        _set_projection_periods(item, projection_periods)
        wired.append(item.id)
    wired.append(_DIVIDEND_CASH_ITEM_ID)
    _extend(model, _EQUITY_FINANCING_CASH_ITEM_ID, projection_periods)
    _extend(model, _FINANCING_CASH_FLOW_ITEM_ID, projection_periods)
    return {
        "wired": wired,
        "required_inputs": list(
            SCHEDULE_INPUT_REQUIREMENTS["share_count_capital_allocation"]
        ),
    }


def _observed_series(model: FinancialModel, item_id: str) -> dict[int, float]:
    """The issuer's reported values for one row, by period.

    ``item.values`` is the compile population store. A document stores the same
    observation as a constant override; either is the reported number.
    """

    item = _optional_item(model, item_id)
    if item is None:
        return {}
    observed: dict[int, float] = {}
    if item.values is not None:
        observed.update(
            {
                int(cell.period): float(cell.value)
                for cell in item.values.values.values()
                if cell.value is not None
            }
        )
    for period, spec in (item.overrides or {}).items():
        if spec.type is not FormulaType.constant:
            continue
        value = spec.params.get("value")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            observed.setdefault(int(period), float(value))
    return observed


def unseparable_interest_periods(model: FinancialModel) -> list[dict[str, float | int]]:
    """The carried closing year's debt with no separable unlevered add-back.

    Runs on every completed model whose DCF discounts
    ``tpl.fm.cash_flow.unlevered_free_cash_flow``. It reads the issuer's own
    reported ``interest_expense`` and balance-sheet debt in the last historical
    period: the same closing basis ``lower_interest_inclusion``
    carries into forecast other income and the debt schedule's effective rate.

    Funded debt across that closing year with no reported interest expense
    leaves unknown interest in the carried ``other_income_expense`` aggregate.
    The projected tax base then charges for interest while the add-back is
    zero, making the discounted numerator levered under an unlevered label.
    Unsupported interest in earlier historical periods is an availability
    issue, not evidence that the forecast's carried basis is levered.

    The measure remains the debt schedule's own base: average debt from the
    closing year's reported opening and closing balances. With fewer than two
    historical periods there is no opening balance, so the basis is unmeasurable,
    just as the schedule cannot derive that year's effective rate.

    The closing period returned is the error Finding
    ``interest_expense_not_separable`` (``schema/build_findings.py``), which
    blocks publish, so no price is published on a levered strip. A closing
    basis with genuinely no debt has nothing to observe and returns none.

    Two limits are deliberate and worth stating. A reported interest expense
    that is present but understated -- part of the issuer's interest separable
    and part inside the aggregate -- is not visible here and is not caught; the
    test is for a missing observation, not an incomplete one. And an issuer
    carrying no balance-sheet debt whose aggregate nonetheless holds interest
    (finance leases, for instance) leaves no observation to measure against.
    """

    discounted = _optional_item(model, _DCF_FCF_PROJECTION_ITEM_ID)
    if discounted is None or discounted.projected is None:
        return []
    if _UNLEVERED_FREE_CASH_FLOW_ITEM_ID not in _formula_ref_ids(
        discounted.projected.params
    ):
        return []

    reported_periods = _historical_periods(model)
    if len(reported_periods) < 2:
        return []
    opening, period = reported_periods[-2:]
    current_debt = _observed_series(model, _BALANCE_SHEET_CURRENT_DEBT_ITEM_ID)
    long_term_debt = _observed_series(model, _BALANCE_SHEET_LONG_TERM_DEBT_ITEM_ID)
    average_debt = (
        current_debt.get(opening, 0.0) + long_term_debt.get(opening, 0.0)
        + current_debt.get(period, 0.0) + long_term_debt.get(period, 0.0)
    ) / 2.0
    if abs(average_debt) <= _DEBT_OBSERVATION_EPSILON:
        return []
    reported_interest = _observed_series(model, _REPORTED_INTEREST_EXPENSE_ITEM_ID)
    interest = reported_interest.get(period, 0.0)
    if abs(interest) > _DEBT_OBSERVATION_EPSILON:
        return []
    return [
        {
            "period": period,
            "average_debt": average_debt,
            "reported_interest_expense": interest,
        }
    ]


def _complete_financial_schedules(model: FinancialModel) -> dict[str, Any]:
    """Compile BM-5 schedule relationships without fabricating missing inputs."""

    if not model._index:
        model.build_index()
    projection_periods = _projection_periods(model)
    historical_periods = _historical_periods(model)
    if not projection_periods:
        return {"families": {}, "required_inputs": []}

    families = {
        "operating_working_capital": _compose_projected_operating_working_capital(
            model,
            projection_periods,
        ),
        # Debt formulas belong to the template and their usable rate axis to
        # valuation retirement, not to this earlier construction helper.
        "debt": {
            "wired": [_DEBT_INTEREST_RATE_ITEM_ID, _DEBT_INTEREST_EXPENSE_ITEM_ID],
            "required_inputs": list(SCHEDULE_INPUT_REQUIREMENTS["debt"]),
        },
        "share_count_capital_allocation": (
            _complete_share_and_capital_allocation_schedules(
                model,
                projection_periods,
            )
        ),
    }
    return {
        "families": families,
        "required_inputs": sorted(
            {
                item_id
                for result in families.values()
                for item_id in result["required_inputs"]
            }
        ),
    }


__all__ = [
    "_BASIC_SHARES_ITEM_ID",
    "_INCOME_STATEMENT_INTEREST_EXPENSE_ITEM_ID",
    "_UNLEVERED_FREE_CASH_FLOW_ITEM_ID",
    "unseparable_interest_periods",
    "apply_disclosed_interest_bridge",
    "_complete_financial_schedules",
    "complete_capitalized_software_schedule",
    "lower_interest_inclusion",
]
