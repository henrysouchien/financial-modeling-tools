from __future__ import annotations

from typing import NamedTuple


class _CellSpec(NamedTuple):
    field: str
    label: str
    source_field: str
    required: bool


class _DirectCellSpec(NamedTuple):
    field: str
    label: str
    sheet: str
    cell: str
    source_field: str
    required: bool


class _VisualCleanupSpec(NamedTuple):
    field: str
    label: str
    sheet: str
    cell: str


_VALUATION_SUMMARY_SPECS: tuple[_CellSpec, ...] = (
    _CellSpec("summary.valuation.current_price", "Current Price", "summary.current_price", True),
    _CellSpec(
        "summary.valuation.blended_target_price",
        "Blended Target Price",
        "summary.blended_target_price",
        True,
    ),
    _CellSpec("summary.valuation.expected_return", "Expected Return", "summary.expected_return", True),
    _CellSpec("summary.valuation.dcf_price", "DCF Price", "valuation.dcf_price", False),
    _CellSpec("summary.valuation.forward_pe_price", "Forward P/E Price", "valuation.forward_pe_price", False),
    _CellSpec(
        "summary.valuation.forward_ev_ebitda_price",
        "EV/EBITDA Price",
        "valuation.forward_ev_ebitda_price",
        False,
    ),
    _CellSpec("summary.valuation.wacc", "WACC", "valuation.wacc", False),
    _CellSpec(
        "summary.valuation.terminal_growth",
        "Terminal Growth",
        "valuation.terminal_growth",
        False,
    ),
)

_VALUATION_DIRECT_CELL_SPECS: tuple[_DirectCellSpec, ...] = (
    _DirectCellSpec(
        "valuation.current_price",
        "Current Price",
        "Valuation",
        "B5",
        "summary.current_price",
        False,
    ),
    _DirectCellSpec(
        "valuation.forward_pe_price",
        "Forward P/E Price",
        "Valuation",
        "B16",
        "valuation.forward_pe_price",
        False,
    ),
    _DirectCellSpec(
        "valuation.forward_ev_ebitda_price",
        "EV/EBITDA Price",
        "Valuation",
        "B23",
        "valuation.forward_ev_ebitda_price",
        False,
    ),
    _DirectCellSpec(
        "valuation.terminal_growth",
        "Terminal Growth",
        "Valuation",
        "B27",
        "valuation.terminal_growth",
        False,
    ),
    _DirectCellSpec("valuation.dcf_price", "DCF Price", "Valuation", "B28", "valuation.dcf_price", False),
    _DirectCellSpec(
        "valuation.blended_target_price",
        "Blended Target Price",
        "Valuation",
        "B30",
        "summary.blended_target_price",
        False,
    ),
    _DirectCellSpec(
        "valuation.expected_return",
        "Expected Return",
        "Valuation",
        "B31",
        "summary.expected_return",
        False,
    ),
    _DirectCellSpec(
        "valuation.dcf_price.detail",
        "DCF Price Detail",
        "Valuation",
        "E47",
        "valuation.dcf_price",
        False,
    ),
    _DirectCellSpec("valuation.wacc", "WACC", "Valuation", "E68", "valuation.wacc", False),
    _DirectCellSpec(
        "valuation.terminal_growth.detail",
        "Terminal Growth Detail",
        "Valuation",
        "H47",
        "valuation.terminal_growth",
        False,
    ),
)

_SCENARIO_DIRECT_CELL_SPECS: tuple[_DirectCellSpec, ...] = (
    _DirectCellSpec(
        "scenarios.current_price",
        "Scenario Current Price",
        "Scenarios",
        "I7",
        "summary.current_price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bull.upside_price",
        "Bull Upside Price",
        "Scenarios",
        "I9",
        "summary.scenario.bull.target_price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bull.estimated_return",
        "Bull Estimated Return",
        "Scenarios",
        "I10",
        "summary.scenario.bull.return_pct",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bull.probability",
        "Bull Probability",
        "Scenarios",
        "I11",
        "summary.scenario.bull.probability",
        False,
    ),
    _DirectCellSpec(
        "scenarios.base.price",
        "Base Price",
        "Scenarios",
        "I14",
        "summary.scenario.base.target_price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.base.estimated_return",
        "Base Estimated Return",
        "Scenarios",
        "I15",
        "summary.scenario.base.return_pct",
        False,
    ),
    _DirectCellSpec(
        "scenarios.base.probability",
        "Base Probability",
        "Scenarios",
        "I16",
        "summary.scenario.base.probability",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bear.downside_price",
        "Bear Downside Price",
        "Scenarios",
        "I19",
        "summary.scenario.bear.target_price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bear.estimated_return",
        "Bear Estimated Return",
        "Scenarios",
        "I20",
        "summary.scenario.bear.return_pct",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bear.probability",
        "Bear Probability",
        "Scenarios",
        "I21",
        "summary.scenario.bear.probability",
        False,
    ),
    _DirectCellSpec(
        "scenarios.expected_value.price",
        "Expected Value",
        "Scenarios",
        "I24",
        "summary.expected_value.price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.expected_value.return",
        "Expected Return",
        "Scenarios",
        "I25",
        "summary.expected_value.return_pct",
        False,
    ),
    _DirectCellSpec(
        "scenarios.expected_value.return_to_risk",
        "Return-to-Risk Ratio",
        "Scenarios",
        "I26",
        "summary.expected_value.return_to_risk",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bull.selected_target_price",
        "Bull Selected Scenario Valuation",
        "Scenarios",
        "I42",
        "summary.scenario.bull.target_price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.base.selected_target_price",
        "Base Selected Scenario Valuation",
        "Scenarios",
        "L42",
        "summary.scenario.base.target_price",
        False,
    ),
    _DirectCellSpec(
        "scenarios.bear.selected_target_price",
        "Bear Selected Scenario Valuation",
        "Scenarios",
        "O42",
        "summary.scenario.bear.target_price",
        False,
    ),
)

_VISUAL_CLEANUP_SPECS: tuple[_VisualCleanupSpec, ...] = tuple(
    _VisualCleanupSpec(f"valuation.visual_cleanup.{cell.lower()}", label, "Valuation", cell)
    for label, cells in (
        ("Capitalization Detail", ("B6", "B7", "B8", "B9")),
        ("Forward P/E Detail", ("B13", "B14", "B15", "F13", "G13", "F14", "G14", "G15")),
        ("Forward EV/EBITDA Detail", ("B20", "B21", "B22", "F20", "G20", "F21", "G21", "G22")),
        ("EV/EBITDA Implied Price Detail", ("E26", "E27", "E28", "E29", "E30")),
        (
            "DCF Cash Flow Detail",
            (
                "E35",
                "F35",
                "G35",
                "H35",
                "I35",
                "J35",
                "K35",
                "L35",
                "M35",
                "N35",
                "O35",
                "P35",
                "E36",
                "F36",
                "G36",
                "H36",
                "I36",
                "J36",
                "K36",
                "L36",
                "M36",
                "N36",
                "O36",
                "P36",
                "E40",
                "E41",
                "H41",
                "E42",
                "H42",
                "E43",
                "H43",
                "E44",
                "E45",
                "E46",
                "H46",
                "H48",
                "H49",
            ),
        ),
        ("WACC Detail", ("E54", "E55", "E59", "E60", "E61", "E64", "F64", "E65", "F65", "E66")),
    )
    for cell in cells
)

_SCENARIO_CASES: tuple[str, ...] = ("bull", "base", "bear")


__all__ = [
    "_CellSpec",
    "_DirectCellSpec",
    "_SCENARIO_CASES",
    "_SCENARIO_DIRECT_CELL_SPECS",
    "_VALUATION_DIRECT_CELL_SPECS",
    "_VALUATION_SUMMARY_SPECS",
    "_VISUAL_CLEANUP_SPECS",
    "_VisualCleanupSpec",
]
