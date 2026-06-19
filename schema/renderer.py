"""Render FinancialModel templates back into Excel-oriented cell plans."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional

from .formatter import ModelFormatter, SIA_FORMATTER
from .models import (
    CellColor,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef as LineItemRef,
    PERIOD_MODE_YEARLY as PERIOD_MODE_YEARLY,
    Sheet,
    SheetLayout,
    TimeStructure as TimeStructure,
    shift_period as shift_period,
)
from .refs import line_item_ref_from_obj as line_item_ref_from_obj
from .renderer_columns import (
    AbsoluteColumnMapper as AbsoluteColumnMapper,
    _historical_periods as _historical_periods,
    _projection_periods as _projection_periods,
    _scoped_periods as _scoped_periods,
    _selector_column as _selector_column,
    _time_order as _time_order,
)
from .renderer_formula import (
    ExcelFormulaCompiler as ExcelFormulaCompiler,
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _apply_adjustment as _apply_adjustment,
    _cell_ref as _cell_ref,
    _excel_number_literal as _excel_number_literal,
    _is_operator_expr as _is_operator_expr,
    _normalize_literal as _normalize_literal,
    _quote_sheet_name as _quote_sheet_name,
    _wrap_if_operator_expr as _wrap_if_operator_expr,
)
from .renderer_summary import (
    _SUMMARY_DRIVER_ROWS as _SUMMARY_DRIVER_ROWS,
    _SUMMARY_OUTPUT_ROWS as _SUMMARY_OUTPUT_ROWS,
    _SUMMARY_SCENARIO_ROWS as _SUMMARY_SCENARIO_ROWS,
    _SUMMARY_VALUATION_ROWS as _SUMMARY_VALUATION_ROWS,
    _append_summary_sheet as _append_summary_sheet,
    _format_cell_count as _format_cell_count,
    _item_locations as _item_locations,
    _summary_cell_for_item as _summary_cell_for_item,
    _summary_default_period as _summary_default_period,
    _summary_formula_or_missing as _summary_formula_or_missing,
    _summary_health_rows as _summary_health_rows,
    _summary_item_ref as _summary_item_ref,
    _summary_period_list as _summary_period_list,
    _summary_source_label as _summary_source_label,
    _unique_sheet_name as _unique_sheet_name,
)
from .renderer_payload import (
    _batch_sheet_formats as _batch_sheet_formats,
    _batch_sheet_writes as _batch_sheet_writes,
    _col_to_index as _col_to_index,
    _data_column_bounds as _data_column_bounds,
    _index_to_col as _index_to_col,
    _split_cell as _split_cell,
    _split_range as _split_range,
    plan_to_mcp_calls as plan_to_mcp_calls,
    render_plan_to_addin_payload as render_plan_to_addin_payload,
)
from .renderer_types import CellFormat, CellWrite, RenderPlan, SheetSetup


CELL_COLOR_MAP = {
    CellColor.input_blue: "#0000FF",
    CellColor.formula_black: "#000000",
    CellColor.key_driver: "#008000",
    CellColor.header: "#000000",
}

_SCENARIO_SELECTOR_ID = "tpl.a.header.scenario_value"
_FIXED_LABEL_COLUMN_MIN_WIDTH = 16.0
_FIXED_LABEL_COLUMN_MAX_WIDTH = 42.0
_FIXED_LABEL_COLUMN_PADDING = 2.0


def render_model(
    model: FinancialModel,
    formatter: Optional[ModelFormatter] = None,
) -> RenderPlan:
    """Produce a pure render plan from a FinancialModel."""

    model.build_index()
    formatter = formatter or SIA_FORMATTER

    plan = RenderPlan()
    layouts: Dict[str, SheetLayout] = {}
    mappers: Dict[str, AbsoluteColumnMapper] = {}
    for sheet_name, sheet in model.sheets.items():
        layout = sheet.layout or SheetLayout(label_column="A", first_data_column="B")
        first_data_column = (layout.first_data_column or "B").upper()
        layouts[sheet_name] = layout
        mappers[sheet_name] = AbsoluteColumnMapper(
            model.time_structure,
            first_data_column,
            period_scope=layout.period_scope,
        )

    for sheet_name, sheet in model.sheets.items():
        layout = layouts[sheet_name]
        label_column = (layout.label_column or "A").upper()
        first_data_column = (layout.first_data_column or "B").upper()
        label_end_column = _label_end_column(label_column, first_data_column)
        mapper = mappers[sheet_name]
        compiler = ExcelFormulaCompiler(model, mappers, sheet_name)
        last_column = mapper.last_column()

        plan.sheet_setups.append(_build_sheet_setup(sheet_name, sheet, layout, mapper))

        for section in sheet.sections:
            label_writes: List[CellWrite] = []
            metadata_writes: List[CellWrite] = []
            data_writes: List[CellWrite] = []
            row_cell_types: DefaultDict[int, Dict[str, bool]] = defaultdict(dict)
            rows_with_fixed_items = {
                int(item.row)
                for item in section.line_items
                if item.column is not None
            }
            default_label_rows_written: set[int] = set()

            for item in section.line_items:
                if item.label != "":
                    item_label_column = (item.label_column or label_column).upper()
                    if item.label_column is not None or int(item.row) not in default_label_rows_written:
                        label_writes.append(
                            CellWrite(sheet=sheet_name, cell=f"{item_label_column}{item.row}", value=item.label)
                        )
                        if item.label_column is None:
                            default_label_rows_written.add(int(item.row))
                if item.id == _SCENARIO_SELECTOR_ID:
                    selector_column = (item.column or _selector_column(first_data_column)).upper()
                    metadata_writes.append(
                        CellWrite(
                            sheet=sheet_name,
                            cell=f"{selector_column}{item.row}",
                            value=_scenario_selector_value(item),
                        )
                    )
                    row_cell_types[int(item.row)][selector_column] = False
                if item.item_type in {ItemType.header, ItemType.spacer}:
                    fixed_columns = {
                        fixed_item.column.upper()
                        for fixed_item in section.line_items
                        if fixed_item.column is not None and int(fixed_item.row) == int(item.row)
                    }
                    for format_range in _row_ranges_excluding_columns(
                        label_column,
                        last_column,
                        item.row,
                        fixed_columns,
                    ):
                        plan.formats.append(
                            CellFormat(
                                sheet=sheet_name,
                                range=format_range,
                                bold=_item_bold(item),
                                font_color=formatter.label_color_for(item) or formatter.header_color,
                                fill_color=(
                                    formatter.section_header_fill_color
                                    if item.item_type == ItemType.header
                                    and _format_sheet_enabled(
                                        sheet_name,
                                        formatter.section_header_fill_sheets,
                                    )
                                    else None
                                ),
                                bottom_border_color=(
                                    formatter.section_header_border_color
                                    if item.item_type == ItemType.header
                                    and _format_sheet_enabled(
                                        sheet_name,
                                        formatter.section_header_border_sheets,
                                    )
                                    else None
                                ),
                            )
                        )

            for item in section.line_items:
                if item.item_type in {ItemType.header, ItemType.spacer}:
                    continue
                if item.column is not None:
                    rendered = _render_fixed_cell(model, item, compiler)
                    column = item.column.upper()
                    if rendered is None or rendered == "":
                        if item.item_type == ItemType.input and _fixed_cell_is_user_input(model, item):
                            row_cell_types[int(item.row)][column] = False
                        continue
                    data_writes.append(
                        CellWrite(
                            sheet=sheet_name,
                            cell=f"{column}{item.row}",
                            value=rendered,
                        )
                    )
                    row_cell_types[int(item.row)][column] = isinstance(rendered, str) and rendered.startswith("=")
                    continue
                for period in mapper.all_periods():
                    rendered = _render_cell_value(model, item, period, compiler)
                    column = mapper.col_for_period(period)
                    if rendered is None or rendered == "":
                        if item.item_type == ItemType.input and _period_cell_is_user_input(model, item, period):
                            row_cell_types[int(item.row)][column] = False
                        continue
                    data_writes.append(
                        CellWrite(
                            sheet=sheet_name,
                            cell=f"{column}{item.row}",
                            value=rendered,
                        )
                    )
                    row_cell_types[int(item.row)][column] = isinstance(rendered, str) and rendered.startswith("=")

            plan.writes.extend(label_writes)
            plan.writes.extend(metadata_writes)
            plan.writes.extend(data_writes)

            for item in section.line_items:
                if item.item_type in {ItemType.header, ItemType.spacer}:
                    continue

                label_format = CellFormat(
                    sheet=sheet_name,
                    range=_label_range(label_column, label_end_column, item.row),
                    bold=_item_bold(item),
                    font_color=formatter.label_color_for(item),
                )
                if label_format.bold is not None or label_format.font_color is not None:
                    plan.formats.append(label_format)
                if int(item.row) not in rows_with_fixed_items:
                    plan.formats.append(
                        CellFormat(
                            sheet=sheet_name,
                            range=f"{label_column}{item.row}:{last_column}{item.row}",
                            number_format=formatter.number_format_for(item),
                        )
                    )
                elif item.column is not None:
                    column = item.column.upper()
                    plan.formats.append(
                        CellFormat(
                            sheet=sheet_name,
                            range=f"{column}{item.row}:{column}{item.row}",
                            number_format=formatter.number_format_for(item),
                        )
                    )
                plan.formats.extend(
                    _data_color_formats(
                        sheet_name,
                        item,
                        item.row,
                        mapper,
                        row_cell_types.get(int(item.row), {}),
                        formatter,
                    )
                )
                plan.formats.extend(
                    _input_fill_formats(
                        sheet_name,
                        item,
                        item.row,
                        mapper,
                        row_cell_types.get(int(item.row), {}),
                        formatter,
                    )
                )

            total_separator_border_color = (
                formatter.total_separator_border_color
                if _format_sheet_enabled(sheet_name, formatter.total_separator_border_sheets)
                else None
            )
            if formatter.underline_before_totals or total_separator_border_color is not None:
                for current_item, next_item in zip(section.line_items, section.line_items[1:]):
                    if current_item.item_type in {ItemType.header, ItemType.spacer}:
                        continue
                    if int(current_item.row) in rows_with_fixed_items:
                        continue
                    if not formatter.is_total_row(next_item.id):
                        continue
                    plan.formats.append(
                        CellFormat(
                            sheet=sheet_name,
                            range=f"{first_data_column}{current_item.row}:{last_column}{current_item.row}",
                            underline=True if formatter.underline_before_totals else None,
                            bottom_border_color=total_separator_border_color,
                        )
                    )

    if formatter.summary_sheet_enabled:
        _append_summary_sheet(model, plan, mappers, formatter)

    return plan


def _build_sheet_setup(
    sheet_name: str,
    sheet: Sheet,
    layout: SheetLayout,
    mapper: AbsoluteColumnMapper,
) -> SheetSetup:
    widths: Dict[str, float] = {}
    if layout.label_column and layout.column_width_label is not None:
        widths[layout.label_column.upper()] = float(layout.column_width_label)
    if layout.column_width_data is not None:
        for column in [mapper.col_for_period(period) for period in mapper.all_periods()]:
            widths[column] = float(layout.column_width_data)
    _add_fixed_label_column_widths(sheet, widths)
    return SheetSetup(
        sheet=sheet_name,
        column_widths=widths,
        freeze_panes=layout.freeze_panes,
        first_data_column=(layout.first_data_column or "B").upper(),
    )


def _add_fixed_label_column_widths(sheet: Sheet, widths: Dict[str, float]) -> None:
    for section in sheet.sections:
        for item in section.line_items:
            if not item.label:
                continue
            candidate_columns = []
            if item.label_column is not None:
                candidate_columns.append(item.label_column.upper())
            if item.column is not None and _fixed_cell_writes_label_text(item):
                candidate_columns.append(item.column.upper())
            if not candidate_columns:
                continue
            width = _fixed_label_column_width(item.label)
            for column in candidate_columns:
                widths[column] = max(widths.get(column, 0.0), width)


def _fixed_label_column_width(label: str) -> float:
    return min(
        _FIXED_LABEL_COLUMN_MAX_WIDTH,
        max(
            _FIXED_LABEL_COLUMN_MIN_WIDTH,
            float(len(label)) + _FIXED_LABEL_COLUMN_PADDING,
        ),
    )


def _fixed_cell_writes_label_text(item: LineItem) -> bool:
    if item.column is None:
        return False
    label = str(item.label or "").strip()
    if not label:
        return False
    for spec in (item.projected, item.historical):
        if spec is None or spec.type is not FormulaType.constant:
            continue
        value = spec.params.get("value")
        if isinstance(value, str) and value.strip() == label:
            return True
    if item.values is not None:
        for value_cell in item.values.values.values():
            if isinstance(value_cell.value, str) and value_cell.value.strip() == label:
                return True
    return False


def _render_cell_value(
    model: FinancialModel,
    item: LineItem,
    period: int,
    compiler: ExcelFormulaCompiler,
) -> Any:
    spec = _spec_for_period(model, item, period)
    if spec is not None:
        return compiler.compile_formula(spec, period=period, item_id=item.id)

    if item.values is not None:
        value_cell = item.values.values.get(int(period))
        if value_cell is not None and value_cell.value is not None:
            return float(value_cell.value)

    return None


def _render_fixed_cell(
    model: FinancialModel,
    item: LineItem,
    compiler: ExcelFormulaCompiler,
) -> Any:
    period = _fixed_cell_anchor_period(model, item)
    if period is None:
        return None

    spec = _spec_for_period(model, item, period)
    if spec is not None:
        return compiler.compile_formula(spec, period=period, item_id=item.id)

    if item.values is not None:
        value_cell = item.values.values.get(int(period))
        if value_cell is not None and value_cell.value is not None:
            return float(value_cell.value)
        for value_period in sorted(item.values.values):
            value_cell = item.values.values[value_period]
            if value_cell.value is not None:
                return float(value_cell.value)

    return None


def _fixed_cell_anchor_period(model: FinancialModel, item: LineItem) -> Optional[int]:
    if item.formula_periods:
        return int(item.formula_periods[0])
    projection_periods = _projection_periods(model.time_structure)
    if projection_periods:
        return int(projection_periods[0])
    periods = _time_order(model.time_structure)
    return int(periods[0]) if periods else None


def _item_bold(item: LineItem) -> Optional[bool]:
    style = item.style
    if style is not None and style.bold is not None:
        return bool(style.bold)
    if item.item_type == ItemType.header:
        return True
    return None


def _label_end_column(label_column: str, first_data_column: str) -> str:
    label_index = _col_to_index(label_column)
    first_data_index = _col_to_index(first_data_column)
    return _index_to_col(max(label_index, first_data_index - 1))


def _label_range(label_column: str, label_end_column: str, row: int) -> str:
    return f"{label_column}{row}:{label_end_column}{row}"


def _data_color_formats(
    sheet_name: str,
    item: LineItem,
    row: int,
    mapper: AbsoluteColumnMapper,
    row_cell_types: Dict[str, bool],
    formatter: ModelFormatter,
) -> List[CellFormat]:
    formats: List[CellFormat] = []

    if item.column is not None:
        column = item.column.upper()
        cell_type = row_cell_types.get(column)
        if cell_type is None:
            return formats
        formats.append(
            CellFormat(
                sheet=sheet_name,
                range=f"{column}{row}:{column}{row}",
                font_color=formatter.data_color_for(cell_type, item),
            )
        )
        return formats

    run_start: Optional[str] = None
    run_end: Optional[str] = None
    run_type: Optional[bool] = None

    def flush() -> None:
        nonlocal run_start, run_end, run_type
        if run_start is None or run_end is None or run_type is None:
            return
        formats.append(
            CellFormat(
                sheet=sheet_name,
                range=f"{run_start}{row}:{run_end}{row}",
                font_color=formatter.data_color_for(run_type, item),
            )
        )
        run_start = None
        run_end = None
        run_type = None

    for period in mapper.all_periods():
        column = mapper.col_for_period(period)
        cell_type = row_cell_types.get(column)
        if cell_type is None:
            flush()
            continue

        if run_start is None:
            run_start = column
            run_end = column
            run_type = cell_type
            continue

        if run_type == cell_type and _col_to_index(column) == _col_to_index(run_end) + 1:
            run_end = column
            continue

        flush()
        run_start = column
        run_end = column
        run_type = cell_type

    flush()
    return formats


def _input_fill_formats(
    sheet_name: str,
    item: LineItem,
    row: int,
    mapper: AbsoluteColumnMapper,
    row_cell_types: Dict[str, bool],
    formatter: ModelFormatter,
) -> List[CellFormat]:
    if item.item_type is not ItemType.input or formatter.input_fill_color is None:
        return []
    if not _format_sheet_enabled(sheet_name, formatter.input_fill_sheets):
        return []

    formats: List[CellFormat] = []

    if item.column is not None:
        column = item.column.upper()
        cell_type = row_cell_types.get(column)
        if cell_type is False:
            formats.append(
                CellFormat(
                    sheet=sheet_name,
                    range=f"{column}{row}:{column}{row}",
                    fill_color=formatter.input_fill_color,
                )
            )
        return formats

    run_start: Optional[str] = None
    run_end: Optional[str] = None

    def flush() -> None:
        nonlocal run_start, run_end
        if run_start is None or run_end is None:
            return
        formats.append(
            CellFormat(
                sheet=sheet_name,
                range=f"{run_start}{row}:{run_end}{row}",
                fill_color=formatter.input_fill_color,
            )
        )
        run_start = None
        run_end = None

    for period in mapper.all_periods():
        column = mapper.col_for_period(period)
        cell_type = row_cell_types.get(column)
        if cell_type is not False:
            flush()
            continue

        if run_start is None:
            run_start = column
            run_end = column
            continue

        if _col_to_index(column) == _col_to_index(run_end) + 1:
            run_end = column
            continue

        flush()
        run_start = column
        run_end = column

    flush()
    return formats


def _fixed_cell_is_user_input(model: FinancialModel, item: LineItem) -> bool:
    period = _fixed_cell_anchor_period(model, item)
    if period is None:
        return True
    return _spec_for_period(model, item, period) is None


def _period_cell_is_user_input(model: FinancialModel, item: LineItem, period: int) -> bool:
    return _spec_for_period(model, item, period) is None


def _format_sheet_enabled(sheet_name: str, enabled_sheets: tuple[str, ...] | None) -> bool:
    return enabled_sheets is None or sheet_name in enabled_sheets


def _spec_for_period(
    model: FinancialModel,
    item: LineItem,
    period: int,
) -> Optional[FormulaSpec]:
    if item.overrides and int(period) in item.overrides:
        return item.overrides[int(period)]

    if item.formula_periods is not None and int(period) not in {int(p) for p in item.formula_periods}:
        return None

    historical_periods = _historical_periods(model.time_structure)
    projection_periods = _projection_periods(model.time_structure)
    if int(period) in historical_periods:
        return item.historical
    if int(period) in projection_periods:
        return item.projected
    return item.projected or item.historical


def _scenario_selector_value(item: LineItem) -> Any:
    if item.values is not None:
        for value_cell in item.values.values.values():
            if value_cell.value is not None:
                return float(value_cell.value)

    try:
        return int(item.label)
    except ValueError:
        try:
            return float(item.label)
        except ValueError:
            return item.label


def _row_ranges_excluding_columns(
    start_column: str,
    end_column: str,
    row: int,
    excluded_columns: set[str],
) -> List[str]:
    start_index = _col_to_index(start_column)
    end_index = _col_to_index(end_column)
    excluded = {
        _col_to_index(column)
        for column in excluded_columns
        if start_index <= _col_to_index(column) <= end_index
    }
    ranges: List[str] = []
    run_start: Optional[int] = None
    run_end: Optional[int] = None

    def flush() -> None:
        nonlocal run_start, run_end
        if run_start is None or run_end is None:
            return
        ranges.append(f"{_index_to_col(run_start)}{row}:{_index_to_col(run_end)}{row}")
        run_start = None
        run_end = None

    for index in range(start_index, end_index + 1):
        if index in excluded:
            flush()
            continue
        if run_start is None:
            run_start = index
        run_end = index
    flush()
    return ranges




__all__ = [
    "AbsoluteColumnMapper",
    "CELL_COLOR_MAP",
    "CellFormat",
    "CellWrite",
    "ExcelFormulaCompiler",
    "RenderPlan",
    "SheetSetup",
    "plan_to_mcp_calls",
    "render_plan_to_addin_payload",
    "render_model",
]
