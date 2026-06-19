"""Add the F2a-val Valuation and Scenarios sheets to sia_generic.json."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..models import (
    CellColor,
    CellStyle,
    CustomizationType,
    DriverCategory,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    Section,
    Sheet,
    SheetLayout,
    SheetType,
    Unit,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from ..refs import line_item_ref_from_obj
from .template_builder import SIA_GENERIC_TEMPLATE_PATH


SCENARIO_EPS_LIMIT = 4

STYLE_HEADER = CellStyle(color=CellColor.header, bold=True)
STYLE_INPUT = CellStyle(color=CellColor.input_blue)
STYLE_FORMULA = CellStyle(color=CellColor.formula_black)
STYLE_KEY = CellStyle(color=CellColor.key_driver)


def ref(item_id: str, t: int = 0, *, period_anchor: str = "first") -> LineItemRef:
    return LineItemRef(id=item_id, t=t, period_anchor=period_anchor)


def const(value: Any) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.constant, params={"value": value})


def ref_formula(
    item_id: str,
    t: int = 0,
    *,
    negate: bool = False,
    period_anchor: str = "first",
) -> FormulaSpec:
    params: dict[str, Any] = {"source": ref(item_id, t, period_anchor=period_anchor)}
    if negate:
        params["negate"] = True
    return FormulaSpec(type=FormulaType.ref, params=params)


def raw(formula: str) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.raw, params={"formula": formula})


def expr_formula(expr: Any, *, blank_if_error: bool = False) -> FormulaSpec:
    params: dict[str, Any] = {"expr": expr}
    if blank_if_error:
        params["blank_if_error"] = True
    return FormulaSpec(type=FormulaType.arithmetic, params=params)


def sum_formula(items: Iterable[Any]) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.arithmetic, params={"function": "SUM", "items": list(items)})


def sum_range_formula(target: LineItemRef) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.arithmetic, params={"function": "SUM_RANGE", "target": target})


def avg_formula(items: Iterable[Any]) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.arithmetic, params={"function": "AVERAGE", "items": list(items)})


def median_formula(items: Iterable[Any], *, blank_if_error: bool = False) -> FormulaSpec:
    params: dict[str, Any] = {"function": "MEDIAN", "items": list(items)}
    if blank_if_error:
        params["blank_if_error"] = True
    return FormulaSpec(type=FormulaType.arithmetic, params=params)


def ratio_formula(numerator: Any, denominator: Any, *, subtract_one: bool = False) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.ratio,
        params={
            "numerator": numerator,
            "denominator": denominator,
            "subtract_one": subtract_one,
        },
    )


def growth_formula(base: Any, rate: Any) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.growth, params={"base": base, "rate": rate})


def valuation_formula(subtype: str, **params: Any) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.valuation, subtype=subtype, params=params)


def values_for(period_values: dict[int, float]) -> ValueSeries:
    return ValueSeries(
        values={
            int(period): ValueCell(
                period=int(period),
                value=float(value),
                provenance=ValueProvenance.input,
            )
            for period, value in period_values.items()
        }
    )


def default_values(periods: list[int], value: float) -> ValueSeries:
    return values_for({int(period): float(value) for period in periods})


def item(
    item_id: str,
    label: str,
    row: int,
    *,
    column: str | None = None,
    label_column: str | None = None,
    item_type: ItemType = ItemType.derived,
    unit: Unit = Unit.dollars,
    formula: FormulaSpec | None = None,
    historical: FormulaSpec | None = None,
    values: ValueSeries | None = None,
    formula_periods: list[int] | None = None,
    data_concept_id: str | None = None,
    style: CellStyle | None = None,
    customization: CustomizationType = CustomizationType.fixed,
    template_token: str | None = None,
    repeat_group_id: str | None = None,
    repeat_group_role: str | None = None,
    build_notes: str | None = None,
) -> LineItem:
    return LineItem(
        id=item_id,
        label=label,
        row=row,
        column=column,
        label_column=label_column,
        item_type=item_type,
        unit=unit,
        historical=historical,
        projected=formula,
        values=values,
        formula_periods=formula_periods,
        data_concept_id=data_concept_id,
        style=style,
        driver_category=DriverCategory.valuation if item_id.startswith(("tpl.v.", "tpl.s.")) else None,
        customization=customization,
        template_token=template_token,
        repeat_group_id=repeat_group_id,
        repeat_group_role=repeat_group_role,
        build_notes=build_notes,
    )


def header(item_id: str, label: str, row: int, *, label_column: str | None = None, column: str | None = None) -> LineItem:
    return item(
        item_id,
        label,
        row,
        column=column,
        label_column=label_column,
        item_type=ItemType.header,
        style=STYLE_HEADER,
        unit=Unit.dollars,
    )


def input_value(
    item_id: str,
    label: str,
    row: int,
    column: str,
    periods: list[int],
    value: float,
    *,
    label_column: str | None = None,
    unit: Unit = Unit.dollars,
    data_concept_id: str | None = None,
    key_driver: bool = False,
    customization: CustomizationType = CustomizationType.fixed,
    template_token: str | None = None,
    repeat_group_id: str | None = None,
    repeat_group_role: str | None = None,
) -> LineItem:
    return item(
        item_id,
        label,
        row,
        column=column,
        label_column=label_column,
        item_type=ItemType.input,
        unit=unit,
        values=default_values(periods, value),
        data_concept_id=data_concept_id,
        style=STYLE_KEY if key_driver else STYLE_INPUT,
        customization=customization,
        template_token=template_token,
        repeat_group_id=repeat_group_id,
        repeat_group_role=repeat_group_role,
    )


def blank_input(
    item_id: str,
    label: str,
    row: int,
    column: str,
    *,
    label_column: str | None = None,
    unit: Unit = Unit.dollars,
    key_driver: bool = False,
    customization: CustomizationType = CustomizationType.fixed,
    template_token: str | None = None,
    repeat_group_id: str | None = None,
    repeat_group_role: str | None = None,
) -> LineItem:
    return item(
        item_id,
        label,
        row,
        column=column,
        label_column=label_column,
        item_type=ItemType.input,
        unit=unit,
        style=STYLE_KEY if key_driver else STYLE_INPUT,
        customization=customization,
        template_token=template_token,
        repeat_group_id=repeat_group_id,
        repeat_group_role=repeat_group_role,
    )


def _iferror_zero(expr: Any) -> dict[str, Any]:
    return {"op": "IFERROR", "expr": expr, "fallback": 0}


def _symmetric_divergence_expr(left: Any, right: Any) -> dict[str, Any]:
    return {
        "op": "MAX",
        "args": [
            _iferror_zero({"op": "-", "left": {"op": "/", "left": left, "right": right}, "right": 1}),
            _iferror_zero({"op": "-", "left": {"op": "/", "left": right, "right": left}, "right": 1}),
        ],
    }


def build_valuation_sheet(model: FinancialModel) -> Sheet:
    projection_periods = [int(p) for p in model.time_structure.projection_periods]
    first_projection = projection_periods[0]
    fy2_formula_periods = projection_periods[1:2]

    sections: list[Section] = []

    sections.append(
        Section(
            id="header",
            label="Header",
            driver_category=DriverCategory.other,
            line_items=[
                header("tpl.v.header.title", "Stock Investor Accelerator Valuation Sheet", 1),
                item(
                    "tpl.v.header.scenario_value",
                    "Scenario",
                    2,
                    column="B",
                    item_type=ItemType.derived,
                    formula=ref_formula("tpl.a.header.scenario_value"),
                    unit=Unit.count,
                    style=STYLE_FORMULA,
                ),
                item(
                    "tpl.v.header.units",
                    "Millions except per-share data",
                    3,
                    column="B",
                    item_type=ItemType.input,
                    formula=const("Millions except per-share data"),
                    style=STYLE_INPUT,
                ),
            ],
        )
    )

    sections.append(
        Section(
            id="current_valuation",
            label="Current Valuation",
            driver_category=DriverCategory.valuation,
            line_items=[
                item(
                    "tpl.v.current_valuation.ticker",
                    "",
                    4,
                    column="A",
                    item_type=ItemType.input,
                    formula=const("TPL"),
                    style=STYLE_INPUT,
                    template_token="[TICKER]",
                ),
                input_value(
                    "tpl.v.current_valuation.stock_price",
                    "Price",
                    5,
                    "B",
                    projection_periods,
                    100.0,
                    unit=Unit.per_share,
                ),
                item(
                    "tpl.v.current_valuation.shares_outstanding",
                    "Shares O/S (M)",
                    6,
                    column="B",
                    formula=ref_formula("tpl.fm.income_statement.diluted_shares_outstanding_m"),
                    unit=Unit.count,
                    style=STYLE_FORMULA,
                ),
                item(
                    "tpl.v.current_valuation.market_cap",
                    "Market Cap",
                    7,
                    column="B",
                    formula=expr_formula(
                        {"op": "*", "args": [ref("tpl.v.current_valuation.stock_price"), ref("tpl.v.current_valuation.shares_outstanding")]}
                    ),
                    style=STYLE_FORMULA,
                ),
                item(
                    "tpl.v.current_valuation.net_debt",
                    "Net Debt (Cash)",
                    8,
                    column="B",
                    formula=ref_formula("tpl.fm.balance_sheet.net_cash", negate=True),
                    style=STYLE_FORMULA,
                ),
                item(
                    "tpl.v.current_valuation.enterprise_value",
                    "Enterprise Value",
                    9,
                    column="B",
                    formula=sum_formula(
                        [
                            ref("tpl.v.current_valuation.market_cap"),
                            ref("tpl.v.current_valuation.net_debt"),
                        ]
                    ),
                    style=STYLE_FORMULA,
                ),
            ],
        )
    )

    sections.append(
        Section(
            id="forward_pe",
            label="Forward P/E Analysis",
            driver_category=DriverCategory.valuation,
            line_items=[
                header("tpl.v.forward_pe.header", "Forward P/E", 11),
                header("tpl.v.forward_pe.ref_year_header", "Year", 12, label_column="D"),
                item("tpl.v.forward_pe.eps_current_ref", "EPS", 13, column="F", label_column="D", formula=ref_formula("tpl.fm.adjusted_earnings.adjusted_eps"), unit=Unit.per_share, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.eps_fy2_ref", "", 13, column="G", formula=ref_formula("tpl.fm.adjusted_earnings.adjusted_eps"), formula_periods=fy2_formula_periods, unit=Unit.per_share, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.pe_current_ref", "P/E", 14, column="F", label_column="D", formula=ratio_formula(ref("tpl.v.current_valuation.stock_price"), ref("tpl.v.forward_pe.eps_current_ref")), unit=Unit.multiple, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.pe_fy2_ref", "", 14, column="G", formula=ratio_formula(ref("tpl.v.current_valuation.stock_price"), ref("tpl.v.forward_pe.eps_fy2_ref")), formula_periods=fy2_formula_periods, unit=Unit.multiple, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.implied_eps_growth", "Implied EPS Growth", 15, column="G", label_column="D", formula=ratio_formula(ref("tpl.v.forward_pe.eps_fy2_ref"), ref("tpl.v.forward_pe.eps_current_ref"), subtract_one=True), formula_periods=fy2_formula_periods, unit=Unit.percentage, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.eps_current", "EPS", 13, column="B", formula=ref_formula("tpl.v.forward_pe.eps_current_ref"), unit=Unit.per_share, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.forward_pe", "Forward P/E", 14, column="B", formula=ref_formula("tpl.v.forward_pe.pe_current_ref"), unit=Unit.multiple, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.fy2_eps", "FY2 EPS", 15, column="B", formula=ref_formula("tpl.v.forward_pe.eps_fy2_ref"), formula_periods=fy2_formula_periods, unit=Unit.per_share, style=STYLE_FORMULA),
                item("tpl.v.forward_pe.forward_pe_price", "Forward P/E Price", 16, column="B", formula=valuation_formula("multiple", multiple=ref("tpl.v.forward_pe.forward_pe"), metric=ref("tpl.v.forward_pe.fy2_eps")), formula_periods=fy2_formula_periods, unit=Unit.per_share, style=STYLE_FORMULA),
            ],
        )
    )

    sections.append(
        Section(
            id="forward_ev_ebitda",
            label="Forward EV/EBITDA Analysis",
            driver_category=DriverCategory.valuation,
            line_items=[
                header("tpl.v.forward_ev_ebitda.header", "Forward EV/EBITDA", 18),
                header("tpl.v.forward_ev_ebitda.ref_year_header", "Year", 19, label_column="D"),
                item("tpl.v.forward_ev_ebitda.ebitda_current_ref", "EBITDA", 20, column="F", label_column="D", formula=ref_formula("tpl.fm.adjusted_earnings.adjusted_ebitda"), style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.ebitda_fy2_ref", "", 20, column="G", formula=ref_formula("tpl.fm.adjusted_earnings.adjusted_ebitda"), formula_periods=fy2_formula_periods, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.ev_ebitda_current_ref", "EV/EBITDA", 21, column="F", label_column="D", formula=ratio_formula(ref("tpl.v.current_valuation.enterprise_value"), ref("tpl.v.forward_ev_ebitda.ebitda_current_ref")), unit=Unit.multiple, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.ev_ebitda_fy2_ref", "", 21, column="G", formula=ratio_formula(ref("tpl.v.current_valuation.enterprise_value"), ref("tpl.v.forward_ev_ebitda.ebitda_fy2_ref")), formula_periods=fy2_formula_periods, unit=Unit.multiple, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.implied_ebitda_growth", "Implied EBITDA Growth", 22, column="G", label_column="D", formula=ratio_formula(ref("tpl.v.forward_ev_ebitda.ebitda_fy2_ref"), ref("tpl.v.forward_ev_ebitda.ebitda_current_ref"), subtract_one=True), formula_periods=fy2_formula_periods, unit=Unit.percentage, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.ebitda_current", "EBITDA", 20, column="B", formula=ref_formula("tpl.v.forward_ev_ebitda.ebitda_current_ref"), style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.forward_ev_ebitda", "Forward EV/EBITDA", 21, column="B", formula=ref_formula("tpl.v.forward_ev_ebitda.ev_ebitda_current_ref"), unit=Unit.multiple, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.fy2_ebitda", "FY2 EBITDA", 22, column="B", formula=ref_formula("tpl.v.forward_ev_ebitda.ebitda_fy2_ref"), formula_periods=fy2_formula_periods, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.ev_ebitda_price", "EV/EBITDA Price", 23, column="B", formula=ref_formula("tpl.v.forward_ev_ebitda.implied_share_price"), formula_periods=fy2_formula_periods, unit=Unit.per_share, style=STYLE_FORMULA),
                header("tpl.v.forward_ev_ebitda.implied_price_header", "EV/EBITDA Implied Price", 25, label_column="D"),
                item("tpl.v.forward_ev_ebitda.implied_ev", "Implied EV", 26, column="E", label_column="D", formula=valuation_formula("multiple", multiple=ref("tpl.v.forward_ev_ebitda.forward_ev_ebitda"), metric=ref("tpl.v.forward_ev_ebitda.fy2_ebitda")), formula_periods=fy2_formula_periods, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.net_debt_fy2", "Less: Net Debt", 27, column="E", label_column="D", formula=ref_formula("tpl.fm.balance_sheet.net_cash"), formula_periods=fy2_formula_periods, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.implied_equity_value", "Implied Equity Value", 28, column="E", label_column="D", formula=sum_formula([ref("tpl.v.forward_ev_ebitda.implied_ev"), ref("tpl.v.forward_ev_ebitda.net_debt_fy2")]), formula_periods=fy2_formula_periods, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.shares_fy2", "Shares Outstanding", 29, column="E", label_column="D", formula=ref_formula("tpl.fm.income_statement.diluted_shares_outstanding_m"), formula_periods=fy2_formula_periods, unit=Unit.count, style=STYLE_FORMULA),
                item("tpl.v.forward_ev_ebitda.implied_share_price", "Implied Share Price", 30, column="E", label_column="D", formula=ratio_formula(ref("tpl.v.forward_ev_ebitda.implied_equity_value"), ref("tpl.v.forward_ev_ebitda.shares_fy2")), formula_periods=fy2_formula_periods, unit=Unit.per_share, style=STYLE_FORMULA),
            ],
        )
    )

    dcf_items = [
        item("tpl.v.dcf.dcf_wacc", "DCF WACC", 26, column="B", formula=ref_formula("tpl.v.wacc.wacc"), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.v.dcf.terminal_growth_summary", "Terminal Growth", 27, column="B", formula=ref_formula("tpl.v.dcf.terminal_growth_rate"), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.v.dcf.dcf_price_summary", "DCF Price", 28, column="B", formula=ref_formula("tpl.v.dcf.dcf_price"), unit=Unit.per_share, style=STYLE_FORMULA),
        header("tpl.v.dcf.header", "DCF", 33, label_column="E"),
        item("tpl.v.dcf.fcf_projection", "FCF", 35, formula=ref_formula("tpl.fm.cash_flow.free_cash_flow"), formula_periods=projection_periods, style=STYLE_FORMULA),
        item("tpl.v.dcf.pv_fcf_projection", "PV of FCF", 36, formula=valuation_formula("dcf_discount", cash_flow=ref("tpl.v.dcf.fcf_projection"), discount_rate=ref("tpl.v.wacc.wacc"), period=ref("tpl.v.dcf.discount_period")), formula_periods=projection_periods, style=STYLE_FORMULA),
        item("tpl.v.dcf.discount_period", "Discount Period", 37, item_type=ItemType.input, values=values_for({period: 0.5 + index for index, period in enumerate(projection_periods)}), formula_periods=projection_periods, unit=Unit.count, style=STYLE_INPUT),
        item("tpl.v.dcf.pv_cash_flows", "PV of Cash Flows", 40, column="E", label_column="D", formula=sum_range_formula(ref("tpl.v.dcf.pv_fcf_projection")), style=STYLE_FORMULA),
        item("tpl.v.dcf.pv_terminal_value", "PV of Terminal Value", 41, column="E", label_column="D", formula=valuation_formula("dcf_discount", cash_flow=ref("tpl.v.dcf.terminal_value_growth"), discount_rate=ref("tpl.v.wacc.wacc"), period=ref("tpl.v.dcf.discount_period", period_anchor="last")), style=STYLE_FORMULA),
        item("tpl.v.dcf.total_ev", "Total Enterprise Value", 42, column="E", label_column="D", formula=sum_formula([ref("tpl.v.dcf.pv_cash_flows"), ref("tpl.v.dcf.pv_terminal_value")]), style=STYLE_FORMULA),
        item("tpl.v.dcf.less_net_debt", "Less: Net Debt", 43, column="E", label_column="D", formula=ref_formula("tpl.v.current_valuation.net_debt"), style=STYLE_FORMULA),
        item("tpl.v.dcf.equity_value", "Equity Value", 44, column="E", label_column="D", formula=expr_formula({"op": "-", "left": ref("tpl.v.dcf.total_ev"), "right": ref("tpl.v.dcf.less_net_debt")}), style=STYLE_FORMULA),
        item("tpl.v.dcf.shares_outstanding", "Shares Outstanding", 45, column="E", label_column="D", formula=ref_formula("tpl.v.current_valuation.shares_outstanding"), unit=Unit.count, style=STYLE_FORMULA),
        item("tpl.v.dcf.equity_value_per_share", "Equity Value per Share", 46, column="E", label_column="D", formula=ratio_formula(ref("tpl.v.dcf.equity_value"), ref("tpl.v.dcf.shares_outstanding")), unit=Unit.per_share, style=STYLE_FORMULA),
        item("tpl.v.dcf.dcf_price", "DCF Price", 47, column="E", label_column="D", formula=growth_formula(ref("tpl.v.dcf.equity_value_per_share"), ref("tpl.v.wacc.wacc")), unit=Unit.per_share, style=STYLE_FORMULA),
        header("tpl.v.dcf.terminal_value_multiple_header", "Terminal Value Multiple", 40, label_column="G"),
        item("tpl.v.dcf.terminal_year_fcf", "Terminal Year FCF", 41, column="H", label_column="G", formula=ref_formula("tpl.v.dcf.fcf_projection", period_anchor="last"), style=STYLE_FORMULA),
        item("tpl.v.dcf.exit_multiple", "Exit Multiple", 42, column="H", label_column="G", formula=valuation_formula("offset_scenario", anchor=ref("tpl.v.dcf.exit_multiple_scenario_table_anchor"), selector=ref("tpl.a.header.scenario_value"), column_offset=0), unit=Unit.multiple, style=STYLE_FORMULA),
        item("tpl.v.dcf.terminal_value_multiple", "Terminal Value (Multiple)", 43, column="H", label_column="G", formula=valuation_formula("multiple", multiple=ref("tpl.v.dcf.exit_multiple"), metric=ref("tpl.v.dcf.terminal_year_fcf")), style=STYLE_FORMULA),
        header("tpl.v.dcf.exit_multiple_scenario_table_anchor", "Exit Multiple", 40, label_column="K", column="K"),
        input_value("tpl.v.dcf.exit_multiple_bull", "Bull", 41, "K", [first_projection], 25.0, label_column="J", unit=Unit.multiple, key_driver=True),
        input_value("tpl.v.dcf.exit_multiple_base", "Base", 42, "K", [first_projection], 18.0, label_column="J", unit=Unit.multiple, key_driver=True),
        input_value("tpl.v.dcf.exit_multiple_bear", "Bear", 43, "K", [first_projection], 15.0, label_column="J", unit=Unit.multiple, key_driver=True),
        header("tpl.v.dcf.constant_growth_header", "Constant Growth", 45, label_column="G"),
        item("tpl.v.dcf.terminal_year_fcf_growth", "Terminal Year FCF", 46, column="H", label_column="G", formula=ref_formula("tpl.v.dcf.terminal_year_fcf"), style=STYLE_FORMULA),
        item("tpl.v.dcf.terminal_growth_rate", "Terminal Growth Rate", 47, column="H", label_column="G", formula=valuation_formula("offset_scenario", anchor=ref("tpl.v.dcf.terminal_growth_scenario_table_anchor"), selector=ref("tpl.a.header.scenario_value"), column_offset=0), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.v.dcf.terminal_discount_rate", "Discount Rate (WACC)", 48, column="H", label_column="G", formula=ref_formula("tpl.v.wacc.wacc"), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.v.dcf.terminal_value_growth", "Terminal Value (Growth)", 49, column="H", label_column="G", formula=valuation_formula("terminal_value", final_cf=ref("tpl.v.dcf.terminal_year_fcf_growth"), growth=ref("tpl.v.dcf.terminal_growth_rate"), discount=ref("tpl.v.dcf.terminal_discount_rate")), style=STYLE_FORMULA),
        header("tpl.v.dcf.terminal_growth_scenario_table_anchor", "Terminal Growth", 45, label_column="K", column="K"),
        input_value("tpl.v.dcf.terminal_growth_bull", "Bull", 46, "K", [first_projection], 0.04, label_column="J", unit=Unit.percentage, key_driver=True),
        input_value("tpl.v.dcf.terminal_growth_base", "Base", 47, "K", [first_projection], 0.03, label_column="J", unit=Unit.percentage, key_driver=True),
        input_value("tpl.v.dcf.terminal_growth_bear", "Bear", 48, "K", [first_projection], 0.01, label_column="J", unit=Unit.percentage, key_driver=True),
    ]
    sections.append(Section(id="dcf", label="DCF Analysis", driver_category=DriverCategory.valuation, line_items=dcf_items))

    sections.append(
        Section(
            id="blended_target",
            label="Blended Target & Return",
            driver_category=DriverCategory.valuation,
            line_items=[
                item("tpl.v.blended_target.blended_price", "Blended Target Price", 30, column="B", formula=avg_formula([ref("tpl.v.dcf.dcf_price_summary"), ref("tpl.v.forward_ev_ebitda.ev_ebitda_price"), ref("tpl.v.forward_pe.forward_pe_price")]), unit=Unit.per_share, style=STYLE_FORMULA),
                item("tpl.v.blended_target.expected_return", "Expected Return", 31, column="B", formula=ratio_formula(ref("tpl.v.blended_target.blended_price"), ref("tpl.v.current_valuation.stock_price"), subtract_one=True), unit=Unit.percentage, style=STYLE_FORMULA),
            ],
        )
    )

    sections.append(
        Section(
            id="cost_of_equity",
            label="Cost of Equity (CAPM)",
            driver_category=DriverCategory.valuation,
            line_items=[
                header("tpl.v.cost_of_equity.header", "Cost of Equity (CAPM)", 51, label_column="D"),
                input_value("tpl.v.cost_of_equity.risk_free_rate", "Risk-Free Rate (Rf)", 52, "E", projection_periods, 0.04, label_column="D", unit=Unit.percentage),
                input_value("tpl.v.cost_of_equity.equity_risk_premium", "Equity Risk Premium", 53, "E", projection_periods, 0.045, label_column="D", unit=Unit.percentage, key_driver=True),
                input_value("tpl.v.cost_of_equity.beta_floor", "Beta Floor", 53, "G", projection_periods, 1.0, label_column="F", unit=Unit.ratio),
                item(
                    "tpl.v.cost_of_equity.beta",
                    "Adjusted Beta",
                    54,
                    column="E",
                    label_column="D",
                    formula=expr_formula(
                        {
                            "op": "MAX",
                            "args": [
                                {
                                    "op": "+",
                                    "args": [
                                        {
                                            "op": "*",
                                            "args": [
                                                ref("tpl.v.cost_of_equity.raw_beta"),
                                                0.67,
                                            ],
                                        },
                                        0.33,
                                    ],
                                },
                                ref("tpl.v.cost_of_equity.beta_floor"),
                            ],
                        }
                    ),
                    unit=Unit.ratio,
                    style=STYLE_FORMULA,
                    build_notes="Blume-style beta adjustment with analyst override floor.",
                ),
                input_value("tpl.v.cost_of_equity.raw_beta", "Raw Beta", 54, "G", projection_periods, 1.0, label_column="F", unit=Unit.ratio),
                item("tpl.v.cost_of_equity.cost_of_equity", "Cost of Equity (Ke)", 55, column="E", label_column="D", formula=valuation_formula("capm", risk_free=ref("tpl.v.cost_of_equity.risk_free_rate"), beta=ref("tpl.v.cost_of_equity.beta"), erp=ref("tpl.v.cost_of_equity.equity_risk_premium")), unit=Unit.percentage, style=STYLE_FORMULA),
            ],
        )
    )

    sections.append(
        Section(
            id="wacc",
            label="Cost of Debt & WACC",
            driver_category=DriverCategory.valuation,
            line_items=[
                header("tpl.v.wacc.cost_of_debt_header", "Cost of Debt", 56, label_column="D"),
                input_value("tpl.v.wacc.sofr_rate", "SOFR Rate", 57, "E", projection_periods, 0.05, label_column="D", unit=Unit.percentage),
                input_value("tpl.v.wacc.credit_spread", "Credit Spread / Margin", 58, "E", projection_periods, 0.01, label_column="D", unit=Unit.percentage, key_driver=True),
                item("tpl.v.wacc.pretax_cost_of_debt", "Pre-tax Cost of Debt", 59, column="E", label_column="D", formula=sum_formula([ref("tpl.v.wacc.sofr_rate"), ref("tpl.v.wacc.credit_spread")]), unit=Unit.percentage, style=STYLE_FORMULA),
                item("tpl.v.wacc.tax_shield", "(1 - Tax Rate)", 60, column="E", label_column="D", formula=expr_formula({"op": "-", "left": 1, "right": ref("tpl.fm.margins.tax_rate")}), unit=Unit.percentage, style=STYLE_FORMULA),
                item("tpl.v.wacc.after_tax_cost_of_debt", "After-tax Cost of Debt (Kd)", 61, column="E", label_column="D", formula=expr_formula({"op": "*", "args": [ref("tpl.v.wacc.pretax_cost_of_debt"), ref("tpl.v.wacc.tax_shield")]}), unit=Unit.percentage, style=STYLE_FORMULA),
                header("tpl.v.wacc.capital_structure_header", "Capital Structure", 63, label_column="D"),
                item("tpl.v.wacc.equity_value", "Equity Value", 64, column="E", label_column="D", formula=ref_formula("tpl.v.current_valuation.market_cap"), style=STYLE_FORMULA),
                item("tpl.v.wacc.weight_equity", "", 64, column="F", formula=ratio_formula(ref("tpl.v.wacc.equity_value"), ref("tpl.v.wacc.total_capital")), unit=Unit.percentage, style=STYLE_FORMULA),
                item("tpl.v.wacc.debt_value", "Debt Value", 65, column="E", label_column="D", formula=ref_formula("tpl.fm.balance_sheet.long_term_debt"), style=STYLE_FORMULA),
                item("tpl.v.wacc.weight_debt", "", 65, column="F", formula=ratio_formula(ref("tpl.v.wacc.debt_value"), ref("tpl.v.wacc.total_capital")), unit=Unit.percentage, style=STYLE_FORMULA),
                item("tpl.v.wacc.total_capital", "Total Capital", 66, column="E", label_column="D", formula=sum_formula([ref("tpl.v.wacc.equity_value"), ref("tpl.v.wacc.debt_value")]), style=STYLE_FORMULA),
                item("tpl.v.wacc.wacc", "WACC", 68, column="E", label_column="D", formula=valuation_formula("wacc", cost_equity=ref("tpl.v.cost_of_equity.cost_of_equity"), weight_equity=ref("tpl.v.wacc.weight_equity"), cost_debt=ref("tpl.v.wacc.after_tax_cost_of_debt"), weight_debt=ref("tpl.v.wacc.weight_debt")), unit=Unit.percentage, style=STYLE_FORMULA),
            ],
        )
    )

    return Sheet(
        name="Valuation",
        sheet_type=SheetType.valuation,
        description="F2a-val valuation model sheet.",
        layout=SheetLayout(
            label_column="A",
            first_data_column="E",
            column_width_label=34.0,
            column_width_data=14.0,
            header_rows=3,
            freeze_panes="E4",
            period_scope="projection",
        ),
        sections=sections,
    )


def build_scenarios_sheet(model: FinancialModel) -> Sheet:
    projection_periods = [int(p) for p in model.time_structure.projection_periods[:SCENARIO_EPS_LIMIT]]

    sections: list[Section] = [
        Section(
            id="header",
            label="Header",
            driver_category=DriverCategory.other,
            line_items=[
                header("tpl.s.header.title", "Scenarios", 1, label_column="B"),
                item("tpl.s.header.ticker", "", 2, column="B", formula=ref_formula("tpl.v.current_valuation.ticker"), style=STYLE_FORMULA),
                item("tpl.s.header.scenario_value", "Scenario", 3, column="B", formula=ref_formula("tpl.a.header.scenario_value"), unit=Unit.count, style=STYLE_FORMULA),
            ],
        )
    ]

    pe_items: list[LineItem] = [
        header("tpl.s.comp_table_pe.header", "Multiple evidence (populate via valuation comps)", 5, label_column="B"),
        header("tpl.s.comp_table_pe.table_header", "NTM P/E", 7, label_column="B"),
        header("tpl.s.comp_table_pe.low_header", "Low", 7, label_column="D"),
        header("tpl.s.comp_table_pe.high_header", "High", 7, label_column="E"),
        header("tpl.s.comp_table_pe.median_header", "Median", 7, label_column="F"),
    ]
    pe_rows = [
        ("target", "[TICKER]", 8, CustomizationType.rename),
        ("comp_1", "COMP 1", 9, CustomizationType.repeatable),
        ("comp_2", "COMP 2", 10, CustomizationType.repeatable),
        ("comp_3", "COMP 3", 11, CustomizationType.repeatable),
        ("comp_4", "COMP 4", 12, CustomizationType.repeatable),
        ("comp_5", "COMP 5", 13, CustomizationType.repeatable),
        ("comp_6", "COMP 6", 14, CustomizationType.repeatable),
    ]
    for role, label, row, customization in pe_rows:
        if role == "target":
            pe_items.append(item("tpl.s.comp_table_pe.target_ticker", "", row, column="B", formula=ref_formula("tpl.v.current_valuation.ticker"), style=STYLE_FORMULA))
        else:
            pe_items.append(item(f"tpl.s.comp_table_pe.{role}_ticker", "", row, column="B", item_type=ItemType.input, formula=const(label), style=STYLE_INPUT, customization=customization, template_token=f"[{label}]", repeat_group_id="pe_comps", repeat_group_role="ticker"))
        for suffix, column in zip(("low", "high", "median"), ("D", "E", "F")):
            pe_items.append(
                blank_input(
                    f"tpl.s.comp_table_pe.{role}_{suffix}",
                    label if suffix == "low" else "",
                    row,
                    column,
                    label_column="B" if suffix == "low" else None,
                    unit=Unit.multiple,
                    customization=customization,
                    repeat_group_id="pe_comps" if role.startswith("comp_") else None,
                    repeat_group_role=suffix,
                )
            )
    pe_items.extend(
        [
            header("tpl.s.comp_table_pe.median_label", "Median", 15, label_column="B"),
            item("tpl.s.comp_table_pe.median_low", "", 15, column="D", formula=median_formula([ref(f"tpl.s.comp_table_pe.{role}_low") for role, *_ in pe_rows], blank_if_error=True), unit=Unit.multiple, style=STYLE_FORMULA),
            item("tpl.s.comp_table_pe.median_high", "", 15, column="E", formula=median_formula([ref(f"tpl.s.comp_table_pe.{role}_high") for role, *_ in pe_rows], blank_if_error=True), unit=Unit.multiple, style=STYLE_FORMULA),
            item("tpl.s.comp_table_pe.median_median", "", 15, column="F", formula=median_formula([ref(f"tpl.s.comp_table_pe.{role}_median") for role, *_ in pe_rows], blank_if_error=True), unit=Unit.multiple, style=STYLE_FORMULA),
        ]
    )
    sections.append(Section(id="comp_table_pe", label="NTM P/E Comps", driver_category=DriverCategory.valuation, line_items=pe_items))

    peg_items: list[LineItem] = [
        header("tpl.s.comp_table_peg.header", "Multiple evidence (populate via valuation comps)", 17, label_column="B"),
        header("tpl.s.comp_table_peg.table_header", "NTM PEG", 19, label_column="B"),
        header("tpl.s.comp_table_peg.low_header", "Low", 19, label_column="D"),
        header("tpl.s.comp_table_peg.high_header", "High", 19, label_column="E"),
        header("tpl.s.comp_table_peg.median_header", "Median", 19, label_column="F"),
    ]
    peg_rows = [
        ("target", 20),
        ("comp_1", 21),
        ("comp_2", 22),
        ("comp_3", 23),
        ("comp_4", 24),
        ("comp_5", 25),
        ("comp_6", 26),
    ]
    for role, row in peg_rows:
        if role == "target":
            peg_items.append(item("tpl.s.comp_table_peg.target_ticker", "", row, column="B", formula=ref_formula("tpl.v.current_valuation.ticker"), style=STYLE_FORMULA))
            customization = CustomizationType.rename
        else:
            peg_items.append(item(f"tpl.s.comp_table_peg.{role}_ticker", "", row, column="B", formula=ref_formula(f"tpl.s.comp_table_pe.{role}_ticker"), style=STYLE_FORMULA))
            customization = CustomizationType.repeatable
        for suffix, column in zip(("low", "high", "median"), ("D", "E", "F")):
            peg_items.append(
                blank_input(
                    f"tpl.s.comp_table_peg.{role}_{suffix}",
                    "",
                    row,
                    column,
                    unit=Unit.ratio,
                    customization=customization,
                    repeat_group_id="peg_comps" if role.startswith("comp_") else None,
                    repeat_group_role=suffix,
                )
            )
    peg_items.extend(
        [
            header("tpl.s.comp_table_peg.median_label", "Median", 27, label_column="B"),
            item("tpl.s.comp_table_peg.median_low", "", 27, column="D", formula=median_formula([ref(f"tpl.s.comp_table_peg.{role}_low") for role, *_ in peg_rows], blank_if_error=True), unit=Unit.ratio, style=STYLE_FORMULA),
            item("tpl.s.comp_table_peg.median_high", "", 27, column="E", formula=median_formula([ref(f"tpl.s.comp_table_peg.{role}_high") for role, *_ in peg_rows], blank_if_error=True), unit=Unit.ratio, style=STYLE_FORMULA),
            item("tpl.s.comp_table_peg.median_median", "", 27, column="F", formula=median_formula([ref(f"tpl.s.comp_table_peg.{role}_median") for role, *_ in peg_rows], blank_if_error=True), unit=Unit.ratio, style=STYLE_FORMULA),
        ]
    )
    sections.append(Section(id="comp_table_peg", label="NTM PEG Comps", driver_category=DriverCategory.valuation, line_items=peg_items))

    earnings_items: list[LineItem] = [
        header("tpl.s.earnings_scenarios.header", "Earnings Scenarios", 29, label_column="B"),
        header("tpl.s.earnings_scenarios.estimated_eps_header", "Estimated EPS", 31, label_column="B"),
    ]
    for index, (period, column) in enumerate(zip(projection_periods, ("C", "D", "E", "F")), start=1):
        earnings_items.append(item(f"tpl.s.earnings_scenarios.year_{index}", "", 31, column=column, item_type=ItemType.input, formula=const(period), unit=Unit.count, style=STYLE_INPUT, formula_periods=[period]))
    case_rows = {"bull": 32, "base": 33, "bear": 34}
    for case, row in case_rows.items():
        for index, (period, column) in enumerate(zip(projection_periods, ("C", "D", "E", "F")), start=1):
            earnings_items.append(
                item(
                    f"tpl.s.earnings_scenarios.eps_{case}_{index}",
                    f"{case.title()} case" if index == 1 else "",
                    row,
                    column=column,
                    label_column="B" if index == 1 else None,
                    item_type=ItemType.input,
                    formula_periods=[period],
                    unit=Unit.per_share,
                    style=STYLE_INPUT,
                    build_notes="Populated at build time via Contract 5 scenario EPS overrides.",
                )
            )
    earnings_items.append(header("tpl.s.earnings_scenarios.eps_growth_header", "% change in EPS", 36, label_column="B"))
    scenario_anchor_period = projection_periods[0] if projection_periods else None
    for case, row in {"bull": 37, "base": 38, "bear": 39}.items():
        for index, column in enumerate(("D", "E", "F"), start=2):
            earnings_items.append(
                item(
                    f"tpl.s.earnings_scenarios.eps_growth_{case}_{index}",
                    f"{case.title()} case" if index == 2 else "",
                    row,
                    column=column,
                    label_column="B" if index == 2 else None,
                    formula=ratio_formula(
                        ref(f"tpl.s.earnings_scenarios.eps_{case}_{index}"),
                        ref(f"tpl.s.earnings_scenarios.eps_{case}_{index - 1}"),
                        subtract_one=True,
                    ),
                    formula_periods=[scenario_anchor_period] if scenario_anchor_period is not None else None,
                    unit=Unit.percentage,
                    style=STYLE_FORMULA,
                )
            )
    earnings_items.extend(
        [
            header("tpl.s.earnings_scenarios.dcf_header", "Estimated DCF", 41, label_column="B"),
            item("tpl.s.earnings_scenarios.scenario_selector_label", "Scenario (Base = 2, Bull = 1, Bear = 3)", 42, column="D", label_column="B", formula=ref_formula("tpl.a.header.scenario_value"), unit=Unit.count, style=STYLE_FORMULA),
            item("tpl.s.earnings_scenarios.dcf_valuation", "DCF valuation", 43, column="D", label_column="B", formula=ref_formula("tpl.v.dcf.equity_value_per_share"), unit=Unit.per_share, style=STYLE_FORMULA),
        ]
    )
    sections.append(Section(id="earnings_scenarios", label="Earnings Scenarios", driver_category=DriverCategory.valuation, line_items=earnings_items))

    valuation_items: list[LineItem] = [
        item("tpl.s.valuation_scenarios.title", "", 29, column="H", formula=raw('Valuation!$A$4&" Valuation Estimate"'), style=STYLE_FORMULA),
    ]
    first_projection_year = projection_periods[0] if projection_periods else None
    fy1_eps_label = f"FY1 Adj. EPS ({first_projection_year})" if first_projection_year is not None else "FY1 Adj. EPS"
    valuation_specs = {
        "bull": {"label": "Bull case", "label_col": "H", "value_col": "I", "legacy_col": "J", "eps": "tpl.s.earnings_scenarios.eps_bull_1", "peg": "tpl.s.comp_table_peg.target_high", "growth": "tpl.s.earnings_scenarios.eps_growth_bull_2"},
        "base": {"label": "Base", "label_col": "K", "value_col": "L", "legacy_col": "M", "eps": "tpl.s.earnings_scenarios.eps_base_1", "peg": "tpl.s.comp_table_peg.median_median", "growth": "tpl.s.earnings_scenarios.eps_growth_base_2"},
        "bear": {"label": "Bear case", "label_col": "N", "value_col": "O", "legacy_col": "P", "eps": "tpl.s.earnings_scenarios.eps_bear_1", "peg": "tpl.s.comp_table_peg.median_median", "growth": "tpl.s.earnings_scenarios.eps_growth_bear_2"},
    }
    for case, spec in valuation_specs.items():
        lc = spec["label_col"]
        vc = spec["value_col"]
        legacy_col = spec["legacy_col"]
        selected_pe_ref = ref(f"tpl.s.valuation_scenarios.{case}_selected_pe")
        pe_valuation_ref = ref(f"tpl.s.valuation_scenarios.{case}_pe_valuation")
        peg_valuation_ref = ref(f"tpl.s.valuation_scenarios.{case}_peg_valuation")
        dcf_valuation_ref = ref("tpl.s.earnings_scenarios.dcf_valuation")
        valuation_items.extend(
            [
                header(f"tpl.s.valuation_scenarios.{case}_header", spec["label"], 31, label_column=lc),
                blank_input(f"tpl.s.valuation_scenarios.{case}_selected_pe", "Selected NTM P/E multiple", 32, vc, label_column=lc, unit=Unit.multiple),
                item(f"tpl.s.valuation_scenarios.{case}_pe_multiple", "", 32, column=legacy_col, formula=ref_formula(f"tpl.s.valuation_scenarios.{case}_selected_pe"), unit=Unit.multiple, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_eps", fy1_eps_label, 33, column=vc, label_column=lc, formula=ref_formula(spec["eps"]), unit=Unit.per_share, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_pe_valuation", "P/E-based valuation", 34, column=vc, label_column=lc, formula=valuation_formula("multiple", multiple=selected_pe_ref, metric=ref(f"tpl.s.valuation_scenarios.{case}_eps"), blank_if_missing=True), unit=Unit.per_share, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_peg_multiple", "Estimated NTM PEG", 36, column=vc, label_column=lc, formula=ref_formula(spec["peg"]), unit=Unit.ratio, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_growth", "FY1-FY2 EPS growth", 37, column=vc, label_column=lc, formula=ref_formula(spec["growth"]), unit=Unit.percentage, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_peg_implied_pe", "PEG-implied NTM P/E multiple", 38, column=vc, label_column=lc, formula=expr_formula({"op": "*", "args": [ref(f"tpl.s.valuation_scenarios.{case}_peg_multiple"), ref(f"tpl.s.valuation_scenarios.{case}_growth"), 100]}), unit=Unit.multiple, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_peg_eps", fy1_eps_label, 39, column=vc, label_column=lc, formula=ref_formula(spec["eps"]), unit=Unit.per_share, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_peg_valuation", "PEG-implied valuation (cross-check)", 40, column=vc, label_column=lc, formula=valuation_formula("multiple", multiple=ref(f"tpl.s.valuation_scenarios.{case}_peg_implied_pe"), metric=ref(f"tpl.s.valuation_scenarios.{case}_peg_eps"), blank_if_missing=True), unit=Unit.per_share, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_peg_check", "PEG vs selected P/E divergence", 41, column=vc, label_column=lc, formula=expr_formula(_symmetric_divergence_expr(selected_pe_ref, ref(f"tpl.s.valuation_scenarios.{case}_peg_implied_pe"))), unit=Unit.percentage, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_valuation_avg", "Selected scenario valuation", 42, column=vc, label_column=lc, formula=ref_formula(f"tpl.s.valuation_scenarios.{case}_pe_valuation"), unit=Unit.per_share, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_divergence_pct", "P/E vs cross-check divergence", 44, column=vc, label_column=lc, formula=expr_formula({"op": "MAX", "args": [_symmetric_divergence_expr(pe_valuation_ref, peg_valuation_ref), _symmetric_divergence_expr(pe_valuation_ref, dcf_valuation_ref)]}), unit=Unit.percentage, style=STYLE_FORMULA),
                item(f"tpl.s.valuation_scenarios.{case}_divergence_flag", "Divergence excess >25%", 45, column=vc, label_column=lc, formula=expr_formula({"op": "MAX", "args": [0, {"op": "-", "left": ref(f"tpl.s.valuation_scenarios.{case}_divergence_pct"), "right": 0.25}]}), unit=Unit.percentage, style=STYLE_FORMULA),
            ]
        )
    sections.append(Section(id="valuation_scenarios", label="Valuation Scenarios", driver_category=DriverCategory.valuation, line_items=valuation_items))

    expected_items = [
        header("tpl.s.expected_value.header", "Expected value estimate (1-3 years)", 5, label_column="H"),
        item("tpl.s.expected_value.current_price", "Current price", 7, column="I", label_column="H", formula=ref_formula("tpl.v.current_valuation.stock_price"), unit=Unit.per_share, style=STYLE_FORMULA),
        item("tpl.s.expected_value.upside_price", "Upside price", 9, column="I", label_column="H", formula=ref_formula("tpl.s.valuation_scenarios.bull_valuation_avg"), unit=Unit.per_share, style=STYLE_FORMULA),
        item("tpl.s.expected_value.upside_return", "Estimated return", 10, column="I", label_column="H", formula=ratio_formula(ref("tpl.s.expected_value.upside_price"), ref("tpl.s.expected_value.current_price"), subtract_one=True), unit=Unit.percentage, style=STYLE_FORMULA),
        input_value("tpl.s.expected_value.bull_probability", "Probability", 11, "I", projection_periods[:1], 0.20, label_column="H", unit=Unit.percentage, key_driver=True),
        item("tpl.s.expected_value.estimated_upside", "Estimated upside", 12, column="I", label_column="H", formula=valuation_formula("probability_weighted", value=ref("tpl.s.expected_value.upside_price"), current=ref("tpl.s.expected_value.current_price"), probability=ref("tpl.s.expected_value.bull_probability")), style=STYLE_FORMULA),
        item("tpl.s.expected_value.base_price", "Base price", 14, column="I", label_column="H", formula=ref_formula("tpl.s.valuation_scenarios.base_valuation_avg"), unit=Unit.per_share, style=STYLE_FORMULA),
        item("tpl.s.expected_value.base_return", "Estimated return", 15, column="I", label_column="H", formula=ratio_formula(ref("tpl.s.expected_value.base_price"), ref("tpl.s.expected_value.current_price"), subtract_one=True), unit=Unit.percentage, style=STYLE_FORMULA),
        input_value("tpl.s.expected_value.base_probability", "Probability", 16, "I", projection_periods[:1], 0.60, label_column="H", unit=Unit.percentage, key_driver=True),
        item("tpl.s.expected_value.estimated_base", "Estimated upside", 17, column="I", label_column="H", formula=valuation_formula("probability_weighted", value=ref("tpl.s.expected_value.base_price"), current=ref("tpl.s.expected_value.current_price"), probability=ref("tpl.s.expected_value.base_probability")), style=STYLE_FORMULA),
        item("tpl.s.expected_value.downside_price", "Downside price", 19, column="I", label_column="H", formula=ref_formula("tpl.s.valuation_scenarios.bear_valuation_avg"), unit=Unit.per_share, style=STYLE_FORMULA),
        item("tpl.s.expected_value.downside_return", "Estimated return", 20, column="I", label_column="H", formula=ratio_formula(ref("tpl.s.expected_value.downside_price"), ref("tpl.s.expected_value.current_price"), subtract_one=True), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.s.expected_value.bear_probability", "Probability", 21, column="I", label_column="H", formula=expr_formula({"op": "MAX", "args": [0, {"op": "-", "left": 1, "right": {"op": "+", "args": [ref("tpl.s.expected_value.bull_probability"), ref("tpl.s.expected_value.base_probability")]}}]}), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.s.expected_value.estimated_risk", "Estimated risk", 22, column="I", label_column="H", formula=valuation_formula("probability_weighted", value=ref("tpl.s.expected_value.downside_price"), current=ref("tpl.s.expected_value.current_price"), probability=ref("tpl.s.expected_value.bear_probability")), style=STYLE_FORMULA),
        item("tpl.s.expected_value.expected_value", "Expected value", 24, column="I", label_column="H", formula=expr_formula({"op": "+", "args": [{"op": "*", "args": [ref("tpl.s.expected_value.upside_price"), ref("tpl.s.expected_value.bull_probability")]}, {"op": "*", "args": [ref("tpl.s.expected_value.base_price"), ref("tpl.s.expected_value.base_probability")]}, {"op": "*", "args": [ref("tpl.s.expected_value.downside_price"), ref("tpl.s.expected_value.bear_probability")]}]}), unit=Unit.per_share, style=STYLE_FORMULA),
        item("tpl.s.expected_value.expected_return", "Estimated return", 25, column="I", label_column="H", formula=ratio_formula(ref("tpl.s.expected_value.expected_value"), ref("tpl.s.expected_value.current_price"), subtract_one=True), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.s.expected_value.return_to_risk", "Return-to-risk ratio", 26, column="I", label_column="H", formula=ratio_formula({"op": "+", "args": [ref("tpl.s.expected_value.estimated_upside"), ref("tpl.s.expected_value.estimated_base")]}, {"op": "NEG", "arg": ref("tpl.s.expected_value.estimated_risk")}), unit=Unit.ratio, style=STYLE_FORMULA),
        header("tpl.s.expected_value.kelly_header", "Kelly criterion", 7, label_column="K"),
        item("tpl.s.expected_value.kelly_expected_value", "Expected value", 8, column="L", label_column="K", formula=sum_formula([ref("tpl.s.expected_value.estimated_upside"), ref("tpl.s.expected_value.estimated_base"), ref("tpl.s.expected_value.estimated_risk")]), style=STYLE_FORMULA),
        item("tpl.s.expected_value.kelly_total_win", "Total win", 9, column="L", label_column="K", formula=sum_formula([ref("tpl.s.expected_value.upside_price"), ref("tpl.s.expected_value.current_price")]), style=STYLE_FORMULA),
        item("tpl.s.expected_value.kelly_position_size", "% position size", 10, column="L", label_column="K", formula=valuation_formula("kelly", expected_value=ref("tpl.s.expected_value.kelly_expected_value"), total_win=ref("tpl.s.expected_value.kelly_total_win")), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.s.expected_value.kelly_upside", "Upside", 15, column="L", label_column="K", formula=expr_formula({"op": "-", "left": ref("tpl.s.expected_value.upside_price"), "right": ref("tpl.s.expected_value.current_price")}), style=STYLE_FORMULA),
        item("tpl.s.expected_value.kelly_downside", "Downside", 16, column="L", label_column="K", formula=expr_formula({"op": "-", "left": ref("tpl.s.expected_value.downside_price"), "right": ref("tpl.s.expected_value.current_price")}), style=STYLE_FORMULA),
        item("tpl.s.expected_value.kelly_odds", "Odds", 17, column="L", label_column="K", formula=ratio_formula(ref("tpl.s.expected_value.kelly_upside"), {"op": "NEG", "arg": ref("tpl.s.expected_value.kelly_downside")}), unit=Unit.ratio, style=STYLE_FORMULA),
        header("tpl.s.expected_value.implied_probability_header", "Implied probability", 19, label_column="K"),
        item("tpl.s.expected_value.implied_probability_upside", "Upside", 20, column="L", label_column="K", formula=ratio_formula(1, {"op": "+", "args": [ref("tpl.s.expected_value.kelly_odds"), 1]}), unit=Unit.percentage, style=STYLE_FORMULA),
        item("tpl.s.expected_value.implied_probability_downside", "Downside", 21, column="L", label_column="K", formula=expr_formula({"op": "-", "left": 1, "right": ref("tpl.s.expected_value.implied_probability_upside")}), unit=Unit.percentage, style=STYLE_FORMULA),
    ]
    sections.append(Section(id="expected_value", label="Expected Value & Kelly", driver_category=DriverCategory.valuation, line_items=expected_items))

    current_last_row = max(
        int(line_item.row)
        for section in sections
        for line_item in section.line_items
        if line_item.row is not None
    )
    snapshot_start_row = current_last_row + 2
    snapshot_items: list[LineItem] = [
        header("tpl.s.thesis_snapshot.header", "Thesis Snapshot (from earnings-scenarios)", snapshot_start_row, label_column="B"),
    ]
    snapshot_case_columns = {
        "bull": ("H", "I", "Bull case"),
        "base": ("K", "L", "Base case"),
        "bear": ("N", "O", "Bear case"),
    }
    for case, (label_col, _value_col, label) in snapshot_case_columns.items():
        snapshot_items.append(
            header(f"tpl.s.thesis_snapshot.{case}_header", label, snapshot_start_row + 2, label_column=label_col)
        )
    snapshot_fields = [
        ("adj_eps", "Adj. EPS (terminal year)", Unit.per_share),
        ("revenue_m", "Revenue ($M)", Unit.dollars),
        ("op_margin_pct", "Operating margin (%)", Unit.percentage),
        ("ebitda_margin_pct", "EBITDA margin (%)", Unit.percentage),
        ("fcf_per_share", "FCF per share", Unit.per_share),
    ]
    for row_offset, (field_name, label, unit) in enumerate(snapshot_fields, start=3):
        row = snapshot_start_row + row_offset
        for case, (label_col, value_col, _case_label) in snapshot_case_columns.items():
            snapshot_items.append(
                blank_input(
                    f"tpl.s.thesis_snapshot.{case}_{field_name}",
                    label,
                    row,
                    value_col,
                    label_column=label_col,
                    unit=unit,
                    key_driver=True,
                )
            )
    snapshot_items.append(
        blank_input(
            "tpl.s.thesis_snapshot.terminal_year",
            "Terminal year",
            snapshot_start_row + 9,
            "I",
            label_column="H",
            unit=Unit.count,
            key_driver=True,
        )
    )
    sections.append(
        Section(
            id="thesis_snapshot",
            label="Thesis Snapshot",
            driver_category=DriverCategory.valuation,
            line_items=snapshot_items,
        )
    )

    return Sheet(
        name="Scenarios",
        sheet_type=SheetType.scenarios,
        description="F2a-val scenario valuation sheet.",
        layout=SheetLayout(
            label_column="B",
            first_data_column="C",
            column_width_label=34.0,
            column_width_data=14.0,
            header_rows=3,
            freeze_panes="C5",
            period_scope="all",
        ),
        sections=sections,
    )


def _iter_line_items(model: FinancialModel) -> Iterable[LineItem]:
    for sheet in model.sheets.values():
        for section in sheet.sections:
            yield from section.line_items


def _iter_formula_specs(item: LineItem) -> Iterable[FormulaSpec]:
    if item.historical is not None:
        yield item.historical
    if item.projected is not None:
        yield item.projected
    if item.overrides:
        yield from item.overrides.values()


def _extract_refs(obj: Any) -> list[LineItemRef]:
    if obj is None:
        return []
    ref_obj = line_item_ref_from_obj(obj)
    if ref_obj is not None:
        return [ref_obj]
    if isinstance(obj, dict):
        refs: list[LineItemRef] = []
        for value in obj.values():
            refs.extend(_extract_refs(value))
        return refs
    if isinstance(obj, list):
        refs: list[LineItemRef] = []
        for value in obj:
            refs.extend(_extract_refs(value))
        return refs
    return []


def validate_refs(model: FinancialModel) -> None:
    model.build_index()
    missing: list[tuple[str, str]] = []
    for item_obj in _iter_line_items(model):
        for spec in _iter_formula_specs(item_obj):
            for ref_obj in _extract_refs(spec.params):
                if ref_obj.resolved and ref_obj.id not in model._index:
                    missing.append((item_obj.id, ref_obj.id))
    if missing:
        details = ", ".join(f"{source}->{target}" for source, target in missing[:20])
        raise ValueError(f"Missing LineItemRef targets: {details}")


def build_valuation_scenarios_template(
    template_path: Path = SIA_GENERIC_TEMPLATE_PATH,
    *,
    write: bool = True,
) -> FinancialModel:
    model = FinancialModel.model_validate_json(template_path.read_text(encoding="utf-8"))
    model.build_index()

    scenario_value = model.get_item("tpl.a.header.scenario_value")
    scenario_value.column = "C"

    model.sheets.pop("Valuation", None)
    model.sheets.pop("Scenarios", None)
    model.sheets["Valuation"] = build_valuation_sheet(model)
    model.sheets["Scenarios"] = build_scenarios_sheet(model)
    model.build_index()
    validate_refs(model)

    if write:
        template_path.write_text(model.model_dump_json(indent=2), encoding="utf-8")
    return model


if __name__ == "__main__":
    build_valuation_scenarios_template()
