"""Valuation input population helpers for schema build orchestration."""

from __future__ import annotations

from typing import Any, Iterable, TYPE_CHECKING

from .build_financial_schedules import debt_interest_rate_periods
from .build_model_product_contracts import (
    CoveredMarketInputs,
    ModelValuationPolicy,
    ValuationFamilyAuthorization,
)
from .analysis import EXIT_EARNINGS_ITEMS, ExitEarningsMeasure

from .build_model_items import _iter_items as _model_iter_items
from .model_readiness import ValuationInputReadiness, ValuationInputReadinessFlag
from .handle import ComputedValues
from .models import (
    FinancialModel,
    FormulaDerivation,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItemRef,
    PeriodAnchor,
    ValueCell,
    ValueProvenance,
    ValueSeries,
    decode_period,
)
from .thesis_shared_slice import Valuation

if TYPE_CHECKING:
    from .dependency_graph import DependencyGraphNumberValue


_COST_OF_DEBT_ROWS = (
    "tpl.v.wacc.pretax_cost_of_debt",
    "tpl.v.wacc.after_tax_cost_of_debt",
    "tpl.v.wacc.credit_spread",
)
#: The debt capital of the WACC; retired with the cost rows when the filer has no debt, so
#: ``total_capital`` sheds it, ``weight_equity`` is 1 and ``wacc`` reads a zero debt weight.
_DEBT_CAPITAL_ROWS = ("tpl.v.wacc.debt_value",)
#: The debt schedule's interest rows. A filer whose debt roles are cited absent has no rate to carry and
#: no debt to charge it on: ``interest_expense`` retires over the projection and ``interest_rate`` over the
#: projection. ``interest_rate`` also owns no historical period whose average debt is a known zero, for every
#: filer (interest over no debt defines no rate); a cited-absent filer's unknown average is no debt either.
#: Periods that carried debt keep their observed rate.
_DEBT_INTEREST_ROWS = (
    "tpl.a.other_income_interest.interest_rate",
    "tpl.a.other_income_interest.interest_expense",
)
_DEBT_AVERAGE_ROW = "tpl.a.other_income_interest.average"
#: The balance-sheet roles whose cited ``not_applicable`` / ``unowed`` terminal disposition is the
#: statement that the filer carries no debt (CITED-NOT-APPLICABLE-PROFILE-ROLES).
_DEBT_ROLES = ("statement.bs.long_term_debt", "statement.bs.short_term_debt")


def retire_unused_cost_of_debt(
    model: FinancialModel,
    readiness: ValuationInputReadiness | None = None,
    *,
    role_dispositions: Any = None,
) -> tuple[str, ...]:
    """Retire the debt rows a filer without debt does not use; return the IDs retired.

    An authored WACC in every projection period retires the debt-cost rows
    (``schema/build.py``). A filer with no debt — the debt roles' terminal
    disposition is cited ``not_applicable`` or ``unowed`` (``role_dispositions``,
    the resolved table both roots pass), or the projected debt is a known zero —
    retires the debt-cost rows and the debt capital (``tpl.v.wacc.debt_value``):
    the debt term is not an input, a missing credit spread is not a required
    value, and the capital structure is equity-only (``total_capital`` sheds the
    debt, ``weight_equity`` is 1, ``wacc`` reads a zero debt weight through
    ``FormulaSpec.without``'s ``weight_debt`` slot rule). Retirement is
    ``DependencyGraph.retire``: the rows' cells go and every reader loses the
    term. No statement cell is written. A cited absence also retires the debt schedule's
    interest rows (``_DEBT_INTEREST_ROWS``): no debt, no rate to carry and no interest
    to charge, and no zero is stamped on them. Every filer's ``interest_rate`` owns no
    historical period whose average debt is a known zero (``_periods_without_average_debt``),
    the one owner of that rule; ``interest_expense`` is untouched. When readiness is available,
    retirement also removes its populated credit-spread claim and source.
    """

    from .dependency_graph import DependencyGraph

    projection = [int(period) for period in model.time_structure.projection_periods]
    wacc = model.get_item("tpl.v.wacc.wacc")
    authored = bool(projection) and set(projection) <= set((wacc.overrides or {}))
    graph = DependencyGraph()
    graph.build(model)
    computed = graph.compute_typed({})
    cited_absent = _debt_is_absent(role_dispositions, projection, computed)
    unused = authored or cited_absent or _debt_weight_is_zero(model, computed)
    # A cited absence leaves no debt cell for ``debt_value`` to read, so the debt
    # capital goes with the cost rows; a known-zero debt keeps ``debt_value`` at 0
    # for the capital-structure readiness checks that read it.
    rows = _COST_OF_DEBT_ROWS + (
        _DEBT_CAPITAL_ROWS + _DEBT_INTEREST_ROWS if cited_absent else ()
    ) if unused else ()
    undefined_rate_periods = _periods_without_average_debt(
        model, computed, unknown_is_absent=cited_absent,
    )
    # A graph result never survives a mutation: finish all pre-retirement decisions,
    # then discard the snapshot before retiring cells or changing rate ownership.
    del computed
    # A fixed-cell row is one cell at its anchor; resolve every cell before the
    # first retirement empties a row and moves its anchor.
    cells = [
        (item_id, graph.resolve_ref_period(LineItemRef(id=item_id), period))
        for item_id in rows
        if item_id in model._index
        for period in projection
    ]
    for item_id, period in dict.fromkeys(cells):
        if period is not None:
            graph.retire(item_id, period)
    rate = model._index.get(_DEBT_INTEREST_ROWS[0])
    if rate is not None:
        # Ownership is derived each run from the declared lagged axis, so a period whose
        # average debt is later corrected to nonzero owns its rate again. A period without
        # one is no cell of the rate; its readers keep reading it as a missing operand (a
        # retirement would rewrite the projected interest's ``IF`` and drop the rate).
        retired = {period for item_id, period in cells if item_id == rate.id}
        rate.formula_periods = [
            period for period in debt_interest_rate_periods(model)
            if period not in undefined_rate_periods and period not in retired
        ]
    if unused and readiness is not None:
        readiness.populated = [
            field for field in readiness.populated if field != "credit_spread"
        ]
        readiness.sources.pop("credit_spread", None)
    return rows


def _debt_is_absent(
    role_dispositions: Any,
    projection: list[int],
    computed: dict[str, dict[int, DependencyGraphNumberValue]],
) -> bool:
    """The filer carries no debt: every debt role's latest historical disposition is a cited
    ``not_applicable`` or ``unowed``, and the debt the model projects is not a known nonzero in
    any projection period. The projected long-term debt is the debt schedule's answer
    (``schema/build_financial_schedules.py``: the balance-sheet line reads the schedule, which
    rolls forward by the author's ``debt_change`` driver), so an authored debt plan entered
    anywhere the schedule reads keeps the debt capital."""

    if role_dispositions is None:
        return False
    latest: dict[str, Any] = {}
    for row in role_dispositions.rows:
        if row.required_role not in _DEBT_ROLES:
            continue
        held = latest.get(row.required_role)
        if held is None or int(row.period_id[2:]) > int(held.period_id[2:]):
            latest[row.required_role] = row
    if not latest or any(row.state not in ("not_applicable", "unowed") for row in latest.values()):
        return False
    return not _projects_known_nonzero(
        computed, "tpl.fm.balance_sheet.long_term_debt", projection,
    )


def _periods_without_average_debt(
    model: FinancialModel,
    computed: dict[str, dict[int, DependencyGraphNumberValue]],
    *,
    unknown_is_absent: bool,
) -> set[int]:
    """The historical periods whose average debt defines no rate: a known zero, and where the filer
    cites its debt absent (``unknown_is_absent``) a value that is not known either."""

    average = computed.get(_DEBT_AVERAGE_ROW, {})

    def absent(period: int) -> bool:
        cell = average.get(period)
        if cell is None or cell.state != "known":
            return unknown_is_absent
        return abs(float(cell.value)) <= 1e-9

    return {
        int(period) for period in model.time_structure.historical_periods if absent(int(period))
    }


def _projects_known_nonzero(
    computed: dict[str, dict[int, DependencyGraphNumberValue]],
    item_id: str,
    projection: list[int],
) -> bool:
    """Whether the model computes ``item_id`` to a known nonzero number in any projection period."""

    series = computed.get(item_id, {})
    return any(
        (cell := series.get(period)) is not None
        and cell.state == "known"
        and abs(float(cell.value)) > 1e-9
        for period in projection
    )


def _debt_weight_is_zero(
    model: FinancialModel,
    computed: dict[str, dict[int, DependencyGraphNumberValue]],
) -> bool:
    from .model_readiness_quality import _projected_debt_is_zero

    values = {
        item_id: {
            int(period): float(value.value)
            for period, value in series.items()
            if value.state == "known"
        }
        for item_id, series in computed.items()
    }
    return _projected_debt_is_zero(
        values, [int(period) for period in model.time_structure.projection_periods],
    )


# House credit spread when no authored or positively derived spread is available.
# The other values remain legacy placeholders used only by readback diagnostics
# and stale-workbook detection, not build-time valuation inputs.
_VALUATION_DEFAULTS = {
    "tpl.v.cost_of_equity.risk_free_rate": 0.04,
    "tpl.v.cost_of_equity.equity_risk_premium": 0.045,
    "tpl.v.wacc.sofr_rate": 0.05,
    "tpl.v.wacc.credit_spread": 0.01,
}
_VALUATION_PLACEHOLDER_VALUES = {
    **_VALUATION_DEFAULTS,
    "tpl.v.current_valuation.stock_price": 100.0,
    "tpl.v.cost_of_equity.raw_beta": 1.0,
}
_VALUATION_ECONOMIC_INPUTS = {
    "stock_price": "tpl.v.current_valuation.stock_price",
    "risk_free_rate": "tpl.v.cost_of_equity.risk_free_rate",
    "equity_risk_premium": "tpl.v.cost_of_equity.equity_risk_premium",
    "raw_beta": "tpl.v.cost_of_equity.raw_beta",
    "beta_floor": "tpl.v.cost_of_equity.beta_floor",
    "adjusted_beta": "tpl.v.cost_of_equity.beta",
    "sofr_rate": "tpl.v.wacc.sofr_rate",
    "credit_spread": "tpl.v.wacc.credit_spread",
}
_VALUATION_DIRECT_OUTPUTS = {
    "wacc": "tpl.v.wacc.wacc",
}
_VALUATION_TERMINAL_INPUTS = {
    "terminal_growth_rate": "tpl.v.dcf.terminal_growth_base",
    "exit_multiple": "tpl.v.dcf.exit_multiple_base",
}
_VALUATION_TERMINAL_INPUT_PREFIXES = (
    "tpl.v.dcf.terminal_growth_",
    "tpl.v.dcf.exit_multiple_",
)
_VALUATION_SCENARIO_CONTROL_INPUTS = {
    "selected_pe": {
        "bull": "tpl.s.valuation_scenarios.bull_selected_pe",
        "base": "tpl.s.valuation_scenarios.base_selected_pe",
        "bear": "tpl.s.valuation_scenarios.bear_selected_pe",
    },
    "selected_ev_ebitda": {
        "bull": "tpl.s.valuation_scenarios.bull_selected_ev_ebitda",
        "base": "tpl.s.valuation_scenarios.base_selected_ev_ebitda",
        "bear": "tpl.s.valuation_scenarios.bear_selected_ev_ebitda",
    },
    "dcf_terminal_growth": {
        "bull": "tpl.v.dcf.terminal_growth_bull",
        "base": "tpl.v.dcf.terminal_growth_base",
        "bear": "tpl.v.dcf.terminal_growth_bear",
    },
    "dcf_exit_multiple": {
        "bull": "tpl.v.dcf.exit_multiple_bull",
        "base": "tpl.v.dcf.exit_multiple_base",
        "bear": "tpl.v.dcf.exit_multiple_bear",
    },
}
_VALUATION_TERMINAL_CONTROL_FAMILIES = (
    "dcf_terminal_growth",
    "dcf_exit_multiple",
)
_VALUATION_TERMINAL_DEFAULTS = {
    "tpl.v.dcf.terminal_growth_bull": 0.04,
    "tpl.v.dcf.terminal_growth_base": 0.03,
    "tpl.v.dcf.terminal_growth_bear": 0.01,
    "tpl.v.dcf.exit_multiple_bull": 25.0,
    "tpl.v.dcf.exit_multiple_base": 18.0,
    "tpl.v.dcf.exit_multiple_bear": 15.0,
}
_VALUATION_TERMINAL_DEFAULT_STRATEGY = "valuation_terminal_default"
_VALUATION_TERMINAL_DEFAULT_SOURCE = f"template.{_VALUATION_TERMINAL_DEFAULT_STRATEGY}"
_VALUATION_TERMINAL_DEFAULT_FIELDS = {
    "tpl.v.dcf.terminal_growth_base": "terminal_growth_rate",
    "tpl.v.dcf.exit_multiple_base": "exit_multiple",
}
#: The net-dilution rate row (diluted-share y/y change) the share curves
#: compound on; the ValuationPolicy ``net_share_dilution`` rates land here.
_VALUATION_NET_SHARE_DILUTION_RATE = "tpl.a.tax_net_income.y_y_chg"
_VALUATION_NET_SHARE_DILUTION_SOURCE = "model_spec.valuation_policy.net_share_dilution"
_VALUATION_REQUIRED_INPUTS = (
    "stock_price",
    "raw_beta",
    "adjusted_beta",
    "risk_free_rate",
    "equity_risk_premium",
    "sofr_rate",
    "credit_spread",
    "selected_pe",
    "selected_ev_ebitda",
)
_BLOOMBERG_STYLE_BETA_WEIGHT = 0.67
_MARKET_BETA_ANCHOR = 1.0


def _read_value(model: FinancialModel, item_id: str, period: int) -> float | None:
    """Read a populated model value; return None when the cell is absent."""

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None
    if item.values is None:
        return None
    cell = item.values.values.get(int(period))
    if cell is None or cell.value is None:
        return None
    return float(cell.value)


def _derive_credit_spread(model: FinancialModel, sofr: float | None) -> float | None:
    """Derive spread as effective yield minus SOFR for WACC consistency."""

    if sofr is None:
        return None
    try:
        hist_periods = sorted(int(period) for period in model.time_structure.historical_periods)
        if len(hist_periods) < 2:
            return None
        current_year, prior_year = hist_periods[-1], hist_periods[-2]

        interest_expense = _read_value(
            model,
            "tpl.fm.adjusted_earnings.interest_expense",
            current_year,
        )
        ltd_current = _read_value(model, "tpl.fm.balance_sheet.long_term_debt", current_year)
        ltd_prior = _read_value(model, "tpl.fm.balance_sheet.long_term_debt", prior_year)
        if interest_expense is None or ltd_current is None or ltd_prior is None:
            return None
        if ltd_current <= 0 or ltd_prior <= 0:
            return None

        avg_debt = (ltd_current + ltd_prior) / 2.0
        if avg_debt <= 0:
            return None

        effective_yield = abs(interest_expense) / avg_debt
        spread = effective_yield - sofr
        if spread <= 0 or spread > 0.20:
            return None

        return float(spread)
    except (KeyError, AttributeError, ZeroDivisionError):
        return None


def _adjust_raw_beta(raw_beta: float, beta_floor: float | None = None) -> float:
    adjusted = (_BLOOMBERG_STYLE_BETA_WEIGHT * raw_beta) + (
        (1.0 - _BLOOMBERG_STYLE_BETA_WEIGHT) * _MARKET_BETA_ANCHOR
    )
    if beta_floor is None:
        return float(adjusted)
    return max(float(adjusted), float(beta_floor))


def _clear_valuation_input_values(model: FinancialModel) -> None:
    item_ids = set(_VALUATION_ECONOMIC_INPUTS.values()) | set(_VALUATION_TERMINAL_INPUTS.values())
    item_ids.update(item_id for family in _VALUATION_SCENARIO_CONTROL_INPUTS.values() for item_id in family.values())
    for item in _model_iter_items(model):
        if item.item_type is ItemType.input and any(
            item.id.startswith(prefix) for prefix in _VALUATION_TERMINAL_INPUT_PREFIXES
        ):
            item_ids.add(item.id)

    for item_id in item_ids:
        try:
            target = model.get_item(item_id)
        except KeyError:
            continue
        target.values = None
        if target.overrides:
            target.overrides = {
                period: spec
                for period, spec in target.overrides.items()
                if spec.note != "derived.effective_yield_minus_sofr"
            } or None
    for item_id in _VALUATION_DIRECT_OUTPUTS.values():
        try:
            target = model.get_item(item_id)
        except KeyError:
            continue
        if target.overrides is None:
            continue
        target.overrides = {
            int(period): spec
            for period, spec in target.overrides.items()
            if spec.note != "model_spec.valuation_controls.wacc"
        } or None


def _set_valuation_input_value(
    model: FinancialModel,
    item_id: str,
    value: float,
    *,
    projection_periods: list[int],
    provenance: ValueProvenance,
    note: str | None = None,
) -> bool:
    try:
        target = model.get_item(item_id)
    except KeyError:
        return False
    target.values = ValueSeries()
    for period in projection_periods:
        target.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=provenance,
            note=note,
        )
    return True


def _set_valuation_direct_output(
    model: FinancialModel,
    item_id: str,
    value: float,
    *,
    projection_periods: list[int],
    source_field: str,
) -> bool:
    """Install an explicit authored valuation result as period overrides."""

    try:
        target = model.get_item(item_id)
    except KeyError:
        return False
    if target.overrides is None:
        target.overrides = {}
    for period in projection_periods:
        target.overrides[int(period)] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": float(value)},
            note=f"model_spec.valuation_controls.{source_field}",
        )
    target.formula_periods = sorted(
        set(target.formula_periods or ()) | set(projection_periods)
    )
    return True


def _valuation_input_has_value(model: FinancialModel, item_id: str) -> bool:
    try:
        item = model.get_item(item_id)
    except KeyError:
        return False
    if item.values is None:
        return False
    return any(cell.value is not None for cell in item.values.values.values())


def _set_net_share_dilution_rates(
    model: FinancialModel,
    rate_by_year: dict[int, float],
    *,
    projection_periods: list[int],
) -> bool:
    """Land each authored rate on its projection year as an ``input`` cell."""

    try:
        target = model.get_item(_VALUATION_NET_SHARE_DILUTION_RATE)
    except KeyError:
        return False
    written = False
    for period in projection_periods:
        rate = rate_by_year.get(int(period))
        if rate is None:
            continue
        if target.values is None:
            target.values = ValueSeries()
        if target.overrides is None:
            target.overrides = {}
        target.values.values[int(period)] = ValueCell(
            period=int(period),
            value=rate,
            provenance=ValueProvenance.input,
        )
        target.overrides[int(period)] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": rate},
            note=_VALUATION_NET_SHARE_DILUTION_SOURCE,
        )
        written = True
    return written


def _terminal_value_metric(measure: ExitEarningsMeasure) -> LineItemRef:
    """The exit multiple's earnings row: terminal FCF, or the declared measure's last year."""

    if measure == "terminal_fcf":
        return LineItemRef(
            id=EXIT_EARNINGS_ITEMS["terminal_fcf"],
            t=0,
            resolved=True,
            period_anchor="first",
        )
    return LineItemRef(
        id=EXIT_EARNINGS_ITEMS[measure],
        t=0,
        resolved=True,
        period_anchor="last",
    )


def _rewire_terminal_value_metric(
    model: FinancialModel, measure: ExitEarningsMeasure
) -> None:
    try:
        item = model.get_item("tpl.v.dcf.terminal_value_multiple")
    except KeyError:
        return
    projected = item.projected
    if projected is None:
        return
    params = dict(projected.params or {})
    params["metric"] = _terminal_value_metric(measure)
    item.projected = projected.model_copy(update={"params": params})



def populate_valuation_inputs(
    model: FinancialModel,
    *,
    beta_floor_rationale: str | None = None,
    valuation: Valuation | None = None,
    pinned_market_inputs: CoveredMarketInputs | None = None,
    valuation_policy: ModelValuationPolicy | None = None,
    valuation_family_authorizations: Iterable[ValuationFamilyAuthorization] = (),
) -> ValuationInputReadiness:
    """Populate sourced/explicit valuation inputs and visible house defaults."""

    if not model._index:
        model.build_index()
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not projection_periods:
        return ValuationInputReadiness(
            status="incomplete",
            missing=list(_VALUATION_REQUIRED_INPUTS),
            flags=[
                ValuationInputReadinessFlag(
                    code="valuation_projection_periods_missing",
                    severity="error",
                    message=("Cannot populate valuation inputs because the model has no projection periods."),
                )
            ],
        )

    _clear_valuation_input_values(model)
    sources: dict[str, str] = {}
    flags: list[ValuationInputReadinessFlag] = []
    populated: set[str] = set()
    stored_inputs = _stored_valuation_inputs(valuation)

    authored_wacc = stored_inputs.get("wacc")
    if authored_wacc is not None and _set_valuation_direct_output(
        model,
        _VALUATION_DIRECT_OUTPUTS["wacc"],
        authored_wacc,
        projection_periods=projection_periods,
        source_field="wacc",
    ):
        populated.add("wacc")
        sources["wacc"] = "model_spec.valuation_controls.wacc"

    pinned_market_inputs = (
        None
        if pinned_market_inputs is None
        else CoveredMarketInputs.model_validate(pinned_market_inputs)
    )
    pinned_by_field = {
        field_name: getattr(pinned_market_inputs, field_name)
        for field_name in (
            "stock_price", "raw_beta", "risk_free_rate", "equity_risk_premium"
        )
    } if pinned_market_inputs is not None else {}
    # A pinned market input counts only when its observation resolved. A bundle
    # that pins none, or whose observation is blocked, leaves the row unavailable
    # (``market_valuation_inputs_missing``); no vendor value stands in for it.
    observed: dict[str, tuple[float, str]] = {}
    for field_name in ("stock_price", "raw_beta"):
        pinned = pinned_by_field.get(field_name)
        if pinned is not None and pinned.status == "resolved":
            observed[field_name] = (float(pinned.value), f"pinned:{pinned.observation_id}")
    for field_name, (value, source) in observed.items():
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS[field_name],
            value,
            projection_periods=projection_periods,
            provenance=ValueProvenance.imported_vendor,
        ):
            populated.add(field_name)
            sources[field_name] = source
    raw_beta = observed["raw_beta"][0] if "raw_beta" in observed else None

    for field_name in ("risk_free_rate", "sofr_rate", "credit_spread"):
        pinned = pinned_by_field.get(field_name)
        authored = stored_inputs.get(field_name)
        uses_pinned = (
            authored is None
            and pinned is not None
            and pinned.status == "resolved"
        )
        stored = (
            float(pinned.value)
            if authored is None
            and pinned is not None
            and pinned.status == "resolved"
            else authored
        )
        if stored is None:
            continue
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS[field_name],
            stored,
            projection_periods=projection_periods,
            provenance=(
                ValueProvenance.imported_vendor
                if uses_pinned
                else ValueProvenance.input
            ),
        ):
            populated.add(field_name)
            sources[field_name] = (
                f"pinned:{pinned.observation_id}"
                if authored is None
                and pinned is not None
                and pinned.status == "resolved"
                else f"model_spec.valuation_controls.{field_name}"
            )
    derived_spread = None if "credit_spread" in populated else _derive_credit_spread(
        model,
        _read_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["sofr_rate"],
            projection_periods[0],
        ),
    )
    if derived_spread is not None and _set_valuation_input_value(
        model,
        _VALUATION_ECONOMIC_INPUTS["credit_spread"],
        derived_spread,
        projection_periods=projection_periods,
        provenance=ValueProvenance.derived,
    ):
        target = model.get_item(_VALUATION_ECONOMIC_INPUTS["credit_spread"])
        if target.overrides is None:
            target.overrides = {}
        historical_periods = sorted(model.time_structure.historical_periods)
        current_year, prior_year = historical_periods[-1], historical_periods[-2]
        period_indices: dict[int, int] = {}
        period_mode = model.time_structure.period_mode
        for period in (*historical_periods, *projection_periods):
            year, slot = decode_period(period, period_mode)
            period_indices[period] = (
                year if period_mode == "yearly" else year * 5 + slot
            )
        # Preserve the exact derived float while retaining the effective-yield
        # operands; valuation cells otherwise contain only a populated value.
        for period in projection_periods:
            interest_expense_ref = LineItemRef(
                id="tpl.fm.adjusted_earnings.interest_expense",
                t=period_indices[current_year] - period_indices[period],
            )
            target.overrides[period] = FormulaSpec(
                type=FormulaType.constant,
                params={
                    "value": derived_spread,
                    "expr": {
                        "op": "-",
                        "left": {
                            "op": "/",
                            "left": {
                                "op": "MAX",
                                "args": [
                                    interest_expense_ref,
                                    {"op": "NEG", "arg": interest_expense_ref},
                                ],
                            },
                            "right": {
                                "op": "AVG",
                                "args": [
                                    LineItemRef(
                                        id="tpl.fm.balance_sheet.long_term_debt",
                                        t=period_indices[year] - period_indices[period],
                                    )
                                    for year in (current_year, prior_year)
                                ],
                            },
                        },
                        "right": LineItemRef(
                            id=_VALUATION_ECONOMIC_INPUTS["sofr_rate"],
                            t=period_indices[projection_periods[0]] - period_indices[period],
                        ),
                    },
                },
                note="derived.effective_yield_minus_sofr",
            )
        populated.add("credit_spread")
        sources["credit_spread"] = "derived.effective_yield_minus_sofr"
    if "credit_spread" not in populated and _set_valuation_input_value(
        model,
        _VALUATION_ECONOMIC_INPUTS["credit_spread"],
        _VALUATION_DEFAULTS[_VALUATION_ECONOMIC_INPUTS["credit_spread"]],
        projection_periods=projection_periods,
        provenance=ValueProvenance.default,
        note="valuation_credit_spread_default",
    ):
        populated.add("credit_spread")
        sources["credit_spread"] = "template.valuation_credit_spread_default"


    pinned_erp = pinned_by_field.get("equity_risk_premium")
    authored_erp = stored_inputs.get("equity_risk_premium")
    uses_pinned_erp = (
        authored_erp is None
        and pinned_erp is not None
        and pinned_erp.status == "resolved"
    )
    stored_erp = (
        float(pinned_erp.value)
        if authored_erp is None
        and pinned_erp is not None
        and pinned_erp.status == "resolved"
        else authored_erp
    )
    if stored_erp is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["equity_risk_premium"],
            stored_erp,
            projection_periods=projection_periods,
            provenance=(
                ValueProvenance.imported_vendor
                if uses_pinned_erp
                else ValueProvenance.input
            ),
        ):
            populated.add("equity_risk_premium")
            sources["equity_risk_premium"] = (
                f"pinned:{pinned_erp.observation_id}"
                if authored_erp is None
                and pinned_erp is not None
                and pinned_erp.status == "resolved"
                else "model_spec.valuation_controls.equity_risk_premium"
            )

    stored_beta_floor = stored_inputs.get("beta_floor")
    beta_floor_value = stored_beta_floor
    if stored_beta_floor is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["beta_floor"],
            stored_beta_floor,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
            note=beta_floor_rationale,
        ):
            populated.add("beta_floor")
            sources["beta_floor"] = "model_spec.valuation_controls.beta_floor"

    if raw_beta is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["adjusted_beta"],
            _adjust_raw_beta(raw_beta, beta_floor_value),
            projection_periods=projection_periods,
            provenance=ValueProvenance.derived,
        ):
            populated.add("adjusted_beta")
            sources["adjusted_beta"] = (
                "derived.blume_adjusted_raw_beta_with_beta_floor"
                if beta_floor_value is not None
                else "derived.blume_adjusted_raw_beta"
            )

    for field_name, item_id in _VALUATION_TERMINAL_INPUTS.items():
        stored = stored_inputs.get(field_name)
        if stored is None:
            continue
        if _set_valuation_input_value(
            model,
            item_id,
            stored,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
        ):
            populated.add(field_name)
            sources[field_name] = f"model_spec.valuation_controls.{field_name}"

    typed_policy = (
        None
        if valuation_policy is None
        else ModelValuationPolicy.model_validate(valuation_policy)
    )
    measure: ExitEarningsMeasure = (
        typed_policy.exit_earnings_measure if typed_policy is not None else "terminal_fcf"
    )
    _rewire_terminal_value_metric(model, measure)
    if typed_policy is not None and typed_policy.terminal_growth_cases is not None:
        assert typed_policy.exit_multiple_cases is not None
        for family_name, cases in (
            ("dcf_terminal_growth", typed_policy.terminal_growth_cases),
            ("dcf_exit_multiple", typed_policy.exit_multiple_cases),
        ):
            for case, item_id in _VALUATION_SCENARIO_CONTROL_INPUTS[family_name].items():
                if _set_valuation_input_value(
                    model,
                    item_id,
                    float(getattr(cases, case)),
                    projection_periods=projection_periods,
                    provenance=ValueProvenance.input,
                ):
                    sources[f"{family_name}.{case}"] = (
                        f"model_spec.valuation_policy.{family_name}.{case}"
                    )
                    if case == "base":
                        field_name = _VALUATION_TERMINAL_DEFAULT_FIELDS[item_id]
                        populated.add(field_name)
                        sources[field_name] = (
                            f"model_spec.valuation_policy.{family_name}.base"
                        )

    # No ValuationPolicy value: the house terminal growth / exit multiple is
    # written ``default`` for every case, and ``judgment_absent_default_used``
    # (``build_findings.default_value_findings``) reports each defaulted item.
    for family_name in _VALUATION_TERMINAL_CONTROL_FAMILIES:
        for case, item_id in _VALUATION_SCENARIO_CONTROL_INPUTS[family_name].items():
            if _valuation_input_has_value(model, item_id):
                continue
            if _set_valuation_input_value(
                model,
                item_id,
                float(_VALUATION_TERMINAL_DEFAULTS[item_id]),
                projection_periods=projection_periods,
                provenance=ValueProvenance.default,
                note=_VALUATION_TERMINAL_DEFAULT_STRATEGY,
            ):
                sources[f"{family_name}.{case}"] = _VALUATION_TERMINAL_DEFAULT_SOURCE
                field_name = _VALUATION_TERMINAL_DEFAULT_FIELDS.get(item_id)
                if field_name is not None:
                    populated.add(field_name)
                    sources.setdefault(field_name, _VALUATION_TERMINAL_DEFAULT_SOURCE)

    # The ValuationPolicy's net share dilution is the one authored writer of the
    # net-dilution rate row the basic and diluted share curves compound on. A
    # projection year it leaves without a rate gets no cell here; the schedule
    # policy writes the house value there as ``default``
    # (``build_company_grounded_schedules``).
    if typed_policy is not None and typed_policy.net_share_dilution is not None:
        if _set_net_share_dilution_rates(
            model,
            {
                row.fiscal_year: float(row.rate)
                for row in typed_policy.net_share_dilution.rates
            },
            projection_periods=projection_periods,
        ):
            sources["net_share_dilution"] = _VALUATION_NET_SHARE_DILUTION_SOURCE

    family_by_method = {
        item.method: ValuationFamilyAuthorization.model_validate(item)
        for item in valuation_family_authorizations
    }
    family_inputs = {
        "forward_pe": ("selected_pe", family_by_method.get("forward_pe")),
        "ev_adjusted_ebitda": (
            "selected_ev_ebitda",
            family_by_method.get("ev_adjusted_ebitda"),
        ),
    }
    for method, (family_name, authorization) in family_inputs.items():
        if authorization is None:
            continue
        for case, item_id in _VALUATION_SCENARIO_CONTROL_INPUTS[family_name].items():
            if _set_valuation_input_value(
                model,
                item_id,
                float(getattr(authorization, case)),
                projection_periods=projection_periods,
                provenance=ValueProvenance.input,
            ):
                populated.add(family_name)
                sources[family_name] = (
                    f"model_spec.valuation_family_authorizations.{method}"
                )

    authorized_methods = set(family_by_method)
    effective_required_inputs = tuple(
        field
        for field in _VALUATION_REQUIRED_INPUTS
        if not (
            "wacc" in populated
            and field in {"sofr_rate", "credit_spread"}
        )
        and not (
            typed_policy is not None
            and (
                (field == "selected_pe" and "forward_pe" not in authorized_methods)
                or (
                    field == "selected_ev_ebitda"
                    and "ev_adjusted_ebitda" not in authorized_methods
                )
            )
        )
    )
    missing = [field for field in effective_required_inputs if field not in populated]
    if any(field in missing for field in ("stock_price", "raw_beta", "adjusted_beta")):
        flags.append(
            ValuationInputReadinessFlag(
                code="market_valuation_inputs_missing",
                severity="warning",
                message=("Pinned price or beta market observations are missing; stock price and beta rows were left blank."),
            )
        )
    if any(
        field in missing
        for field in ("risk_free_rate", "sofr_rate", "credit_spread")
    ):
        flags.append(
            ValuationInputReadinessFlag(
                code="macro_valuation_inputs_missing",
                severity="warning",
                message=(
                    "Macro valuation inputs are missing; refresh the ValuationPolicy "
                    "through valuation-policy or supply explicit inputs "
                    "before clean DCF acceptance."
                ),
            )
        )
    if "equity_risk_premium" in missing:
        flags.append(
            ValuationInputReadinessFlag(
                code="equity_risk_premium_missing",
                severity="warning",
                message=(
                    "Equity risk premium was not supplied; the build leaves ERP blank instead of defaulting it."
                ),
            )
        )

    try:
        discount_period = model.get_item("tpl.v.dcf.discount_period")
    except KeyError:
        discount_period = None
    if discount_period is not None:
        if discount_period.values is None:
            discount_period.values = ValueSeries()
        for index, period in enumerate(projection_periods):
            discount_period.values.values[int(period)] = ValueCell(
                period=int(period),
                value=float(0.5 + index),
                provenance=ValueProvenance.input,
            )

    try:
        ticker_item = model.get_item("tpl.v.current_valuation.ticker")
    except KeyError:
        ticker_item = None
    if ticker_item is not None:
        ticker_item.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": model.company.ticker},
            note="valuation_ticker_metadata",
            derivation=FormulaDerivation(
                owner="build_valuation_inputs",
                strategy="valuation_ticker_metadata",
                version=1,
            ),
        )
        try:
            exit_mult_item = model.get_item("tpl.v.dcf.exit_multiple")
            if exit_mult_item.projected is not None:
                exit_mult_item.projected = exit_mult_item.projected.model_copy(
                    update={
                        "derivation": FormulaDerivation(
                            owner="build_valuation_inputs",
                            strategy="dcf_exit_multiple_derivation",
                            version=1,
                        )
                    },
                    deep=True,
                )
        except KeyError:
            pass
    return ValuationInputReadiness(
        status="complete" if not missing else "incomplete",
        populated=sorted(populated),
        missing=missing,
        sources=dict(sorted(sources.items())),
        flags=flags,
    )


def normalize_zero_debt_valuation_readiness(
    model: FinancialModel,
    computed_values: ComputedValues,
    readiness: ValuationInputReadiness,
) -> ValuationInputReadiness:
    """Make debt-cost inputs and outputs inapplicable at exact zero debt."""

    periods = tuple(int(period) for period in model.time_structure.projection_periods)
    if not periods or any(
        computed_values.get(item_id, {}).get(period) != 0.0
        for item_id in (
            "tpl.fm.balance_sheet.long_term_debt",
            "tpl.v.wacc.debt_value",
        )
        for period in periods
    ):
        return readiness
    retire_unused_cost_of_debt(model, readiness)
    missing = [field for field in readiness.missing if field != "credit_spread"]
    flags = [
        flag
        for flag in readiness.flags
        if not (
            flag.code == "macro_valuation_inputs_missing"
            and not {"risk_free_rate", "sofr_rate", "credit_spread"}.intersection(
                missing
            )
        )
    ]
    if "credit_spread" in readiness.missing:
        flags.append(
            ValuationInputReadinessFlag(
                code="zero_debt_cost_of_debt_not_applicable",
                severity="info",
                message=(
                    "Credit spread is not applicable because long-term debt and "
                    "debt value are exactly zero for every projection period."
                ),
            )
        )
    return ValuationInputReadiness.model_validate(
        {
            **readiness.model_dump(mode="python"),
            "status": "complete" if not missing else "incomplete",
            "missing": missing,
            "flags": flags,
        }
    )


def silent_relative_method_limitation(method: str) -> str:
    """The limitation ``valuation_policy._relative_controls`` stores on a relative method
    the judgment never stated.

    That row is persisted with ``selection_basis`` ``none``. A persisted none row, authored
    or server-synthesized, is a cited not-applicable absence. No valuation policy, or no
    binding, remains missingness. This text is the synthesized row's limitations, not a
    discriminator against an authored none."""

    return f"The judgment states no {method} policy; the method is not selected."


def declared_unselected_relative_methods(
    valuation_policy: ModelValuationPolicy | None,
) -> dict[str, dict[str, str]]:
    """Each relative method whose persisted control has ``selection_basis`` ``none``, with its citation.

    A persisted none row, authored or server-synthesized, is a cited not-applicable absence.
    No valuation policy remains missingness (this returns nothing)."""

    if valuation_policy is None:
        return {}
    return {
        control.method: {
            "kind": "valuation_policy",
            "method": control.method,
            "selection_basis": control.selection_basis,
            "limitations": control.limitations,
            "output_label": control.output_label,
        }
        for control in valuation_policy.relative_methods
        if control.selection_basis == "none"
    }


# The expected-value block prices each case from the displayed method, and the typed
# policy's ``displayed_method`` is the closed vocabulary ``dcf_terminal_growth``
# (``ValuationSummaryPolicy``): each case price is the DCF's own per-share value with the
# terminal value re-priced at that case's terminal growth control (the sheet form of
# ``analysis.dcf_case_prices``). The template routes these rows to the forward-P/E
# ``{case}_valuation_avg``; under a policy that chain is a labeled cross-check whether
# the family is authorized or retired, never the displayed price.
_EXPECTED_VALUE_DCF_CASE_PRICES = {
    "tpl.s.expected_value.upside_price": "tpl.v.dcf.terminal_growth_bull",
    "tpl.s.expected_value.base_price": "tpl.v.dcf.terminal_growth_base",
    "tpl.s.expected_value.downside_price": "tpl.v.dcf.terminal_growth_bear",
}


def _dcf_terminal_growth_case_price_expr(terminal_growth_item_id: str) -> dict[str, Any]:
    def ref(item_id: str, anchor: PeriodAnchor = "first") -> LineItemRef:
        return LineItemRef(id=item_id, period_anchor=anchor)

    growth = ref(terminal_growth_item_id)
    wacc = ref("tpl.v.wacc.wacc")
    terminal_value = {
        "op": "/",
        "left": {
            "op": "*",
            "args": [
                ref("tpl.v.dcf.terminal_year_fcf_growth"),
                {"op": "+", "args": [1, growth]},
            ],
        },
        "right": {"op": "-", "left": wacc, "right": growth},
    }
    present_terminal_value = {
        "op": "/",
        "left": terminal_value,
        "right": {
            "op": "^",
            "left": {"op": "+", "args": [1, wacc]},
            "right": ref("tpl.v.dcf.discount_period", "last"),
        },
    }
    return {
        "op": "/",
        "left": {
            "op": "-",
            "left": {
                "op": "+",
                "args": [ref("tpl.v.dcf.pv_cash_flows"), present_terminal_value],
            },
            "right": ref("tpl.v.dcf.less_net_debt"),
        },
        "right": ref("tpl.v.dcf.shares_outstanding"),
    }


def bind_expected_value_prices_to_dcf_cases(model: FinancialModel) -> None:
    """Price the expected-value upside/base/downside rows from the DCF case controls."""

    for price_id, growth_id in _EXPECTED_VALUE_DCF_CASE_PRICES.items():
        item = model._index.get(price_id)
        if item is None:
            continue
        item.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            params={"expr": _dcf_terminal_growth_case_price_expr(growth_id)},
            note="expected-value case price: DCF at the case terminal growth",
        )


def suppress_unauthorized_relative_valuation_outputs(
    model: FinancialModel,
    *,
    valuation_policy: ModelValuationPolicy | None,
    valuation_family_authorizations: Iterable[ValuationFamilyAuthorization],
) -> dict[str, dict[str, str]]:
    """Remove formula surfaces for relative methods the exact policy did not select.

    Returns the suppressed scenario multiple INPUT items of each method whose persisted
    control has ``selection_basis`` ``none``, keyed by item id to that row's citation
    (:func:`declared_unselected_relative_methods`). A persisted none row, authored or
    server-synthesized, is a cited not-applicable absence. No valuation policy returns
    nothing: those cells stay missing."""

    if valuation_policy is None:
        return {}
    authorized = {
        ValuationFamilyAuthorization.model_validate(item).method
        for item in valuation_family_authorizations
    }
    declared = declared_unselected_relative_methods(valuation_policy)
    declared_inputs: dict[str, dict[str, str]] = {}
    # The typed policy always assigns the displayed result to terminal-growth
    # DCF.  Relative methods remain labeled cross-checks, so the legacy blended
    # target is not an authorized output even when one of those cross-checks is
    # populated.
    disabled_ids: set[str] = {
        item.id
        for item in model._index.values()
        if item.id.startswith("tpl.v.blended_target.")
    }
    if "forward_pe" not in authorized:
        disabled_ids.add("tpl.v.forward_pe.method_price")
        for case in ("bull", "base", "bear"):
            disabled_ids.update(
                {
                    f"tpl.s.valuation_scenarios.{case}_{suffix}"
                    for suffix in (
                        "selected_pe",
                        "pe_multiple",
                        "pe_valuation",
                        "peg_multiple",
                        "peg_implied_pe",
                        "peg_eps",
                        "peg_valuation",
                        "peg_check",
                        "valuation_avg",
                        "divergence_pct",
                        "divergence_flag",
                    )
                }
            )
            if "forward_pe" in declared:
                declared_inputs[
                    _VALUATION_SCENARIO_CONTROL_INPUTS["selected_pe"][case]
                ] = declared["forward_pe"]
    if "ev_adjusted_ebitda" not in authorized:
        for case in ("bull", "base", "bear"):
            item_id = _VALUATION_SCENARIO_CONTROL_INPUTS["selected_ev_ebitda"][case]
            disabled_ids.add(item_id)
            if "ev_adjusted_ebitda" in declared:
                declared_inputs[item_id] = declared["ev_adjusted_ebitda"]
        disabled_ids.update(
            item.id
            for item in model._index.values()
            if item.id.startswith("tpl.v.forward_ev_ebitda.method_")
        )
    for item_id in sorted(disabled_ids):
        item = model._index.get(item_id)
        if item is None:
            continue
        item.historical = None
        item.projected = None
        item.overrides = None
        item.values = None
        # A declared-unselected scenario input is a cited absence: it keeps its anchor cell
        # (``item_owns_cell``: an unauthored input slot owns its anchor), which the build stamps
        # with the policy's citation. Every other disabled row leaves the model.
        if item_id not in declared_inputs:
            item.formula_periods = []
    bind_expected_value_prices_to_dcf_cases(model)
    return {
        item_id: citation
        for item_id, citation in declared_inputs.items()
        if item_id in model._index
    }


def _stored_valuation_inputs(
    valuation: Valuation | None,
) -> dict[str, float]:
    if not valuation:
        return {}
    artifact = Valuation.model_validate(valuation)
    result: dict[str, float] = {}
    for field_name in (
        "wacc",
        "risk_free_rate",
        "sofr_rate",
        "credit_spread",
        "equity_risk_premium",
        "beta_floor",
    ):
        value = getattr(artifact, field_name)
        if value is not None:
            result[field_name] = float(value)
    if artifact.terminal_growth_rate is not None:
        result["terminal_growth_rate"] = float(artifact.terminal_growth_rate)
    if artifact.terminal_multiple is not None:
        result["exit_multiple"] = float(artifact.terminal_multiple)
    return result
