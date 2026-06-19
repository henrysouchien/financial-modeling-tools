"""Summary-sheet helpers for schema rendering."""

from __future__ import annotations

from typing import Any, Dict, List

from .formatter import ModelFormatter
from .models import FinancialModel, LineItem, Sheet
from .renderer_columns import AbsoluteColumnMapper, _historical_periods, _projection_periods
from .renderer_formula import _quote_sheet_name
from .renderer_payload import _col_to_index, _index_to_col, _split_range
from .renderer_types import CellFormat, CellWrite, RenderPlan, SheetSetup


_SUMMARY_OUTPUT_ROWS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Revenue", ("tpl.fm.income_statement.total_revenue",)),
    ("Gross Profit", ("tpl.fm.income_statement.gross_profit",)),
    ("Adjusted EBITDA", ("tpl.fm.adjusted_earnings.adjusted_ebitda",)),
    ("Adjusted EPS", ("tpl.fm.adjusted_earnings.adjusted_eps",)),
    ("Free Cash Flow", ("tpl.fm.cash_flow.free_cash_flow",)),
    (
        "FCF Per Share",
        (
            "tpl.fm.cash_flow.free_cash_flow_per_share",
            "tpl.fm.balance_sheet.free_cash_flow_per_share",
        ),
    ),
)
_SUMMARY_DRIVER_ROWS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Revenue Growth", ("tpl.a.revenue_drivers.total_revenue_growth",)),
    ("Gross Margin", ("tpl.a.unit_economics.gross_margin",)),
    ("Operating Margin", ("tpl.a.operating_leverage.operating_margin",)),
    ("Adjusted Operating Margin", ("tpl.a.adj_operating_income.adj_operating_margin",)),
    ("Tax Rate", ("tpl.a.tax_net_income.tax_rate",)),
    ("Free Cash Flow", ("tpl.a.free_cash_flow.free_cash_flow",)),
    ("FCF Conversion", ("tpl.a.free_cash_flow.as_of_adjusted_ebitda",)),
    ("Share Count Growth", ("tpl.a.tax_net_income.y_y_chg",)),
)
_SUMMARY_SCENARIO_ROWS: tuple[tuple[str, tuple[tuple[str, tuple[str, ...]], ...]], ...] = (
    (
        "Bull",
        (
            ("Adj. EPS", ("tpl.s.thesis_snapshot.bull_adj_eps",)),
            ("Revenue", ("tpl.s.thesis_snapshot.bull_revenue_m",)),
            ("Operating Margin", ("tpl.s.thesis_snapshot.bull_op_margin_pct",)),
            ("EBITDA Margin", ("tpl.s.thesis_snapshot.bull_ebitda_margin_pct",)),
            ("FCF/Share", ("tpl.s.thesis_snapshot.bull_fcf_per_share",)),
            ("Target Price", ("tpl.s.valuation_scenarios.bull_valuation_avg",)),
            ("Probability", ("tpl.s.expected_value.bull_probability",)),
        ),
    ),
    (
        "Base",
        (
            ("Adj. EPS", ("tpl.s.thesis_snapshot.base_adj_eps",)),
            ("Revenue", ("tpl.s.thesis_snapshot.base_revenue_m",)),
            ("Operating Margin", ("tpl.s.thesis_snapshot.base_op_margin_pct",)),
            ("EBITDA Margin", ("tpl.s.thesis_snapshot.base_ebitda_margin_pct",)),
            ("FCF/Share", ("tpl.s.thesis_snapshot.base_fcf_per_share",)),
            ("Target Price", ("tpl.s.valuation_scenarios.base_valuation_avg",)),
            ("Probability", ("tpl.s.expected_value.base_probability",)),
        ),
    ),
    (
        "Bear",
        (
            ("Adj. EPS", ("tpl.s.thesis_snapshot.bear_adj_eps",)),
            ("Revenue", ("tpl.s.thesis_snapshot.bear_revenue_m",)),
            ("Operating Margin", ("tpl.s.thesis_snapshot.bear_op_margin_pct",)),
            ("EBITDA Margin", ("tpl.s.thesis_snapshot.bear_ebitda_margin_pct",)),
            ("FCF/Share", ("tpl.s.thesis_snapshot.bear_fcf_per_share",)),
            ("Target Price", ("tpl.s.valuation_scenarios.bear_valuation_avg",)),
            ("Probability", ("tpl.s.expected_value.bear_probability",)),
        ),
    ),
)
_SUMMARY_VALUATION_ROWS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Current Price", ("tpl.v.current_valuation.stock_price",)),
    ("Blended Target Price", ("tpl.v.blended_target.blended_price",)),
    ("Expected Return", ("tpl.v.blended_target.expected_return",)),
    ("DCF Price", ("tpl.v.dcf.dcf_price_summary", "tpl.v.dcf.dcf_price")),
    ("Forward P/E Price", ("tpl.v.forward_pe.forward_pe_price",)),
    ("EV/EBITDA Price", ("tpl.v.forward_ev_ebitda.ev_ebitda_price",)),
    ("WACC", ("tpl.v.wacc.wacc", "tpl.v.dcf.dcf_wacc")),
    ("Terminal Growth", ("tpl.v.dcf.terminal_growth_summary", "tpl.v.dcf.terminal_growth_rate")),
)


def _append_summary_sheet(
    model: FinancialModel,
    plan: RenderPlan,
    mappers: Dict[str, AbsoluteColumnMapper],
    formatter: ModelFormatter,
) -> None:
    sheet_name = _unique_sheet_name(formatter.summary_sheet_name or "Summary", model.sheets)
    locations = _item_locations(model)
    rendered_detail_cells = {(write.sheet, write.cell) for write in plan.writes}
    historical_periods = _historical_periods(model.time_structure)
    projection_periods = _projection_periods(model.time_structure)
    latest_actual = historical_periods[-1] if historical_periods else None
    first_forecast = projection_periods[0] if projection_periods else None
    final_forecast = projection_periods[-1] if projection_periods else None

    plan.sheet_setups.append(
        SheetSetup(
            sheet=sheet_name,
            column_widths={
                "A": 32.0,
                "B": 30.0,
                "C": 18.0,
                "D": 18.0,
                "E": 32.0,
                "F": 16.0,
                "G": 18.0,
                "H": 18.0,
            },
            freeze_panes="A4",
            first_data_column="B",
        )
    )

    writes: List[CellWrite] = []
    formats: List[CellFormat] = []

    def write(cell: str, value: Any) -> None:
        writes.append(CellWrite(sheet=sheet_name, cell=cell, value=value))

    def bold_row(row: int, last_column: str = "H") -> None:
        formats.append(CellFormat(sheet=sheet_name, range=f"A{row}:{last_column}{row}", bold=True))

    def section_row(row: int, last_column: str = "H") -> None:
        formats.append(
            CellFormat(
                sheet=sheet_name,
                range=f"A{row}:{last_column}{row}",
                bold=True,
                bottom_border_color=formatter.section_header_border_color,
            )
        )

    def number_format(row: int, start_column: str, end_column: str, fmt: str | None) -> None:
        if fmt:
            formats.append(
                CellFormat(
                    sheet=sheet_name,
                    range=f"{start_column}{row}:{end_column}{row}",
                    number_format=fmt,
                )
            )

    write("A1", f"{model.company.ticker} Financial Model Summary")
    write("A2", model.company.name)
    write("D2", "Style guide")
    write("E2", formatter.style_guide_id)
    bold_row(1)

    row = 4
    write(f"A{row}", "Model Identity")
    section_row(row, "E")
    row += 1
    identity_rows = (
        ("Company", model.company.name),
        ("Ticker", model.company.ticker),
        (
            "Fiscal year end",
            model.company.fiscal_year_end
            or model.time_structure.fiscal_year_end
            or formatter.summary_missing_value,
        ),
        ("Historical periods", _summary_period_list(historical_periods)),
        ("Projection periods", _summary_period_list(projection_periods)),
        ("Template version", model.metadata.template_version or formatter.summary_missing_value),
        ("Style source", formatter.style_guide_source_reference),
    )
    for label, value in identity_rows:
        write(f"A{row}", label)
        write(f"B{row}", value)
        row += 1

    row += 1
    write(f"A{row}", "Key Outputs")
    section_row(row, "E")
    row += 1
    for offset, header in enumerate(("Metric", "Latest Actual", "First Forecast", "Final Forecast", "Source")):
        write(f"{_index_to_col(1 + offset)}{row}", header)
    section_row(row, "E")
    row += 1
    for label, item_ids in _SUMMARY_OUTPUT_ROWS:
        item_ref = _summary_item_ref(item_ids, locations)
        if item_ref is None:
            write(f"A{row}", label)
            write(f"B{row}", formatter.summary_missing_value)
            write(f"C{row}", formatter.summary_missing_value)
            write(f"D{row}", formatter.summary_missing_value)
            write(f"E{row}", "Missing canonical row")
            row += 1
            continue
        item_sheet, item = item_ref
        write(f"A{row}", label)
        write(
            f"B{row}",
            _summary_formula_or_missing(
                item_sheet,
                item,
                mappers,
                latest_actual,
                rendered_detail_cells,
                formatter,
            ),
        )
        write(
            f"C{row}",
            _summary_formula_or_missing(
                item_sheet,
                item,
                mappers,
                first_forecast,
                rendered_detail_cells,
                formatter,
            ),
        )
        write(
            f"D{row}",
            _summary_formula_or_missing(
                item_sheet,
                item,
                mappers,
                final_forecast,
                rendered_detail_cells,
                formatter,
            ),
        )
        write(f"E{row}", _summary_source_label(item_sheet, item))
        number_format(row, "B", "D", formatter.number_format_for(item))
        row += 1

    row += 1
    write(f"A{row}", "Key Forecast Drivers")
    section_row(row, "E")
    row += 1
    for offset, header in enumerate(("Driver", "Category", "First Forecast", "Final Forecast", "Source")):
        write(f"{_index_to_col(1 + offset)}{row}", header)
    section_row(row, "E")
    row += 1
    for label, item_ids in _SUMMARY_DRIVER_ROWS[: max(0, formatter.summary_max_key_drivers)]:
        item_ref = _summary_item_ref(item_ids, locations)
        if item_ref is None:
            write(f"A{row}", label)
            write(f"B{row}", formatter.summary_missing_value)
            write(f"C{row}", formatter.summary_missing_value)
            write(f"D{row}", formatter.summary_missing_value)
            write(f"E{row}", "Missing canonical row")
            row += 1
            continue
        item_sheet, item = item_ref
        write(f"A{row}", label)
        write(f"B{row}", item.driver_category.value if item.driver_category else formatter.summary_missing_value)
        write(
            f"C{row}",
            _summary_formula_or_missing(
                item_sheet,
                item,
                mappers,
                first_forecast,
                rendered_detail_cells,
                formatter,
            ),
        )
        write(
            f"D{row}",
            _summary_formula_or_missing(
                item_sheet,
                item,
                mappers,
                final_forecast,
                rendered_detail_cells,
                formatter,
            ),
        )
        write(f"E{row}", _summary_source_label(item_sheet, item))
        number_format(row, "C", "D", formatter.number_format_for(item))
        row += 1

    row += 1
    write(f"A{row}", "Scenario Outputs")
    section_row(row, "H")
    row += 1
    scenario_headers = (
        "Case",
        "Adj. EPS",
        "Revenue",
        "Op. Margin",
        "EBITDA Margin",
        "FCF/Share",
        "Target Price",
        "Probability",
    )
    for offset, header in enumerate(scenario_headers):
        write(f"{_index_to_col(1 + offset)}{row}", header)
    section_row(row, "H")
    row += 1
    for case_name, metrics in _SUMMARY_SCENARIO_ROWS:
        write(f"A{row}", case_name)
        for offset, (_metric_label, item_ids) in enumerate(metrics, start=2):
            item_ref = _summary_item_ref(item_ids, locations)
            if item_ref is None:
                write(f"{_index_to_col(offset)}{row}", formatter.summary_missing_value)
                continue
            item_sheet, item = item_ref
            write(
                f"{_index_to_col(offset)}{row}",
                _summary_formula_or_missing(
                    item_sheet,
                    item,
                    mappers,
                    None,
                    rendered_detail_cells,
                    formatter,
                ),
            )
            number_format(row, _index_to_col(offset), _index_to_col(offset), formatter.number_format_for(item))
        row += 1

    row += 1
    write(f"A{row}", "Valuation")
    section_row(row, "D")
    row += 1
    for offset, header in enumerate(("Metric", "Value", "Source")):
        write(f"{_index_to_col(1 + offset)}{row}", header)
    section_row(row, "D")
    row += 1
    for label, item_ids in _SUMMARY_VALUATION_ROWS:
        item_ref = _summary_item_ref(item_ids, locations)
        if item_ref is None:
            write(f"A{row}", label)
            write(f"B{row}", formatter.summary_missing_value)
            write(f"C{row}", "Missing canonical row")
            row += 1
            continue
        item_sheet, item = item_ref
        write(f"A{row}", label)
        write(
            f"B{row}",
            _summary_formula_or_missing(
                item_sheet,
                item,
                mappers,
                None,
                rendered_detail_cells,
                formatter,
            ),
        )
        write(f"C{row}", _summary_source_label(item_sheet, item))
        number_format(row, "B", "B", formatter.number_format_for(item))
        row += 1

    row += 1
    write(f"A{row}", "Model Health")
    section_row(row, "D")
    row += 1
    health_rows = _summary_health_rows(model, plan, formatter)
    for label, value in health_rows:
        write(f"A{row}", label)
        write(f"B{row}", value)
        if isinstance(value, (int, float)):
            number_format(row, "B", "B", "#,##0")
        row += 1

    plan.writes.extend(writes)
    plan.formats.extend(formats)


def _item_locations(model: FinancialModel) -> Dict[str, tuple[str, LineItem]]:
    locations: Dict[str, tuple[str, LineItem]] = {}
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                locations[item.id] = (sheet_name, item)
    return locations


def _summary_item_ref(
    item_ids: tuple[str, ...],
    item_locations: Dict[str, tuple[str, LineItem]],
) -> tuple[str, LineItem] | None:
    for item_id in item_ids:
        item_ref = item_locations.get(item_id)
        if item_ref is not None:
            return item_ref
    return None


def _summary_formula_or_missing(
    sheet_name: str,
    item: LineItem,
    mappers: Dict[str, AbsoluteColumnMapper],
    period: int | None,
    rendered_detail_cells: set[tuple[str, str]],
    formatter: ModelFormatter,
) -> str:
    cell = _summary_cell_for_item(sheet_name, item, mappers, period)
    if cell is None or (sheet_name, cell) not in rendered_detail_cells:
        return formatter.summary_missing_value
    return f"={_quote_sheet_name(sheet_name)}!{cell}"


def _summary_cell_for_item(
    sheet_name: str,
    item: LineItem,
    mappers: Dict[str, AbsoluteColumnMapper],
    period: int | None,
) -> str | None:
    if item.column is not None:
        return f"{item.column.upper()}{item.row}"
    if period is None:
        period = _summary_default_period(mappers.get(sheet_name))
    if period is None:
        return None
    mapper = mappers.get(sheet_name)
    if mapper is None or int(period) not in set(mapper.all_periods()):
        return None
    return f"{mapper.col_for_period(int(period))}{item.row}"


def _summary_default_period(mapper: AbsoluteColumnMapper | None) -> int | None:
    if mapper is None:
        return None
    projection_periods = mapper.projection_periods()
    if projection_periods:
        return projection_periods[-1]
    periods = mapper.all_periods()
    return periods[-1] if periods else None


def _summary_source_label(sheet_name: str, item: LineItem) -> str:
    if item.column is not None:
        return f"{sheet_name}!{item.column.upper()}{item.row}"
    return f"{sheet_name}!row {item.row}"


def _summary_period_list(periods: List[int]) -> str:
    if not periods:
        return "None"
    if len(periods) == 1:
        return str(periods[0])
    return f"{periods[0]}-{periods[-1]}"


def _summary_health_rows(
    model: FinancialModel,
    plan: RenderPlan,
    formatter: ModelFormatter,
) -> tuple[tuple[str, Any], ...]:
    formula_count = sum(
        1
        for write in plan.writes
        if isinstance(write.value, str) and write.value.startswith("=")
    )
    highlighted_input_cells = sum(
        _format_cell_count(cell_format.range)
        for cell_format in plan.formats
        if (
            cell_format.fill_color == formatter.input_fill_color
            and cell_format.bold is None
            and cell_format.font_color is None
            and cell_format.number_format is None
            and cell_format.underline is None
            and cell_format.top_border_color is None
            and cell_format.bottom_border_color is None
        )
    )
    placeholder_labels = sum(
        1
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
        if "[" in item.label and "]" in item.label
    )
    return (
        ("Rendered detail sheets", len(model.sheets)),
        ("Formula-linked cells", formula_count),
        ("Highlighted input/review cells", highlighted_input_cells),
        ("Placeholder labels requiring review", placeholder_labels),
        ("Readiness diagnostics", "Use current-model projection_readiness sidecar"),
    )


def _format_cell_count(range_text: str) -> int:
    start_col, start_row, end_col, end_row = _split_range(range_text)
    return (_col_to_index(end_col) - _col_to_index(start_col) + 1) * (end_row - start_row + 1)


def _unique_sheet_name(base_name: str, existing_sheets: Dict[str, Sheet]) -> str:
    if base_name not in existing_sheets:
        return base_name
    suffix = 2
    while f"{base_name}_{suffix}" in existing_sheets:
        suffix += 1
    return f"{base_name}_{suffix}"


append_summary_sheet = _append_summary_sheet
format_cell_count = _format_cell_count
item_locations = _item_locations
summary_cell_for_item = _summary_cell_for_item
summary_default_period = _summary_default_period
summary_formula_or_missing = _summary_formula_or_missing
summary_health_rows = _summary_health_rows
summary_item_ref = _summary_item_ref
summary_period_list = _summary_period_list
summary_source_label = _summary_source_label
unique_sheet_name = _unique_sheet_name
