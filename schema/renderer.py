"""Render a ``FinancialModel`` into a pure, Excel-oriented cell plan.

``render_model`` computes formula-owned values through the dependency graph,
delegates formula and payload details to the focused ``renderer_*.py``
modules, and returns a ``RenderPlan`` without writing a workbook or durable
product state. ``schema/excel_writer.py`` owns XLSX serialization.

Owner and render-pipeline guide: ``schema/README.md``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional

from .build_formula_refs import _extract_ref_ids
from .build_company_info import _YEAR_HEADER_ID
from .dependency_graph import DependencyGraph, formula_owned_recompute_ids
from .formatter import ModelFormatter, SIA_FORMATTER
from .models import (
    CellColor,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef as LineItemRef,
    Sheet,
    SheetLayout,
)
from .presentation_captions import (
    PresentationCaptionSpecError,
    WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS,
    item_renders_text,
)
from .renderer_columns import (
    AbsoluteColumnMapper as AbsoluteColumnMapper,
    _fixed_cell_anchor_period as _fixed_cell_anchor_period,
    _historical_periods as _historical_periods,
    _projection_periods as _projection_periods,
    _selector_column as _selector_column,
    _time_order as _time_order,
)
from .renderer_formula import (
    ExcelFormulaCompiler as ExcelFormulaCompiler,
    _normalize_literal,
)
from . import renderer_summary as _renderer_summary
from .renderer_payload import (
    _col_to_index as _col_to_index,
    _index_to_col as _index_to_col,
    plan_to_mcp_calls as plan_to_mcp_calls,
    render_plan_to_addin_payload as render_plan_to_addin_payload,
)
from .published_model_readback import WRITER_NUMERIC_TOLERANCE
from .renderer_types import (
    AnnualRenderContext,
    CellFormat,
    CellWrite,
    RenderPlan,
    SemanticCellRender,
    SheetSetup,
)


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
    *,
    annual_context: AnnualRenderContext | None = None,
) -> RenderPlan:
    """Produce a pure render plan or raise on an invalid render contract.

    The supported extension points are ``ModelFormatter`` and the focused
    renderer helpers; persistence belongs to the caller.
    """

    model.build_index()
    _validate_presentation_caption_specs(model)
    formatter = formatter or SIA_FORMATTER
    graph = DependencyGraph()
    graph.build(model)
    typed_values = graph.compute_typed(
        {},
        recompute=formula_owned_recompute_ids(model),
        propagate_roots=set(),
    )

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
                    period = _fixed_cell_anchor_period(model, item)
                    rendered = _render_fixed_cell(model, item, compiler)
                    column = item.column.upper()
                    if (
                        (rendered is None or rendered == "")
                        and period is not None
                        and annual_context is not None
                        and (item.id, period) in annual_context.required_unavailable
                    ):
                        rendered = "=NA()"
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
                    if period is not None and item.id != _YEAR_HEADER_ID:
                        plan.semantic_cells.append(
                            _semantic_cell_render(
                                model,
                                sheet_name,
                                f"{column}{item.row}",
                                item,
                                period,
                                rendered,
                                typed_values,
                                formatter,
                                annual_context,
                            )
                        )
                    continue
                for period in mapper.all_periods():
                    rendered = _render_cell_value(model, item, period, compiler)
                    column = mapper.col_for_period(period)
                    if (
                        (rendered is None or rendered == "")
                        and annual_context is not None
                        and (item.id, period) in annual_context.required_unavailable
                    ):
                        rendered = "=NA()"
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
                    if item.id != _YEAR_HEADER_ID:
                        plan.semantic_cells.append(
                            _semantic_cell_render(
                                model,
                                sheet_name,
                                f"{column}{item.row}",
                                item,
                                period,
                                rendered,
                                typed_values,
                                formatter,
                                annual_context,
                            )
                        )

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
        _renderer_summary._append_summary_sheet(
            model,
            plan,
            mappers,
            formatter,
            annual_context=annual_context,
        )

    finalize_render_contract(plan)
    return plan


def finalize_render_contract(plan: RenderPlan) -> None:
    """Complete presentation-cell entries after all existing plan writers run."""

    semantic = {
        (entry.sheet, entry.cell): entry
        for entry in plan.semantic_cells
        if entry.cell_kind != "presentation"
        or entry.semantic_item_id != "presentation"
    }
    final_writes = {
        (write.sheet, write.cell.upper()): write for write in plan.writes
    }
    presentations = [
        SemanticCellRender(
            sheet=sheet,
            cell=cell,
            semantic_item_id="presentation",
            semantic_label=(
                str(write.value)
                if isinstance(write.value, str) and not write.value.startswith("=")
                else "Presentation cell"
            ),
            period=0,
            cell_kind="presentation",
            expected_state="known",
            expected_value=write.value,
            origin="presentation",
            dependencies=(),
            unit=None,
            scale="units",
            sign="natural",
            style=None,
            tolerance=0.0,
            number_format=None,
        )
        for (sheet, cell), write in final_writes.items()
        if (sheet, cell) not in semantic
    ]
    plan.semantic_cells = [*semantic.values(), *presentations]


def reconcile_render_contract(
    model: FinancialModel,
    plan: RenderPlan,
    *,
    formatter: ModelFormatter,
    annual_context: AnnualRenderContext | None,
) -> None:
    """Add semantic metadata for late writes made by existing render helpers."""

    final_writes = {
        (write.sheet, write.cell.upper()): write.value for write in plan.writes
    }
    existing = {
        (entry.sheet, entry.cell)
        for entry in plan.semantic_cells
        if entry.cell_kind != "presentation"
        or entry.semantic_item_id != "presentation"
    }
    graph = DependencyGraph()
    graph.build(model)
    typed_values = graph.compute_typed(
        {},
        recompute=formula_owned_recompute_ids(model),
        propagate_roots=set(),
    )
    for sheet_name, sheet in model.sheets.items():
        layout = sheet.layout or SheetLayout(
            label_column="A",
            first_data_column="B",
        )
        mapper = AbsoluteColumnMapper(
            model.time_structure,
            layout.first_data_column or "B",
            period_scope=layout.period_scope,
        )
        for section in sheet.sections:
            for item in section.line_items:
                if item.item_type is ItemType.header or item.id == _YEAR_HEADER_ID:
                    continue
                periods = tuple(mapper.all_periods())
                if item.column is not None:
                    periods = (
                        tuple(item.formula_periods or ())
                        or tuple(model.time_structure.projection_periods)
                        or periods
                    )[:1]
                for period in periods:
                    cell = (
                        f"{item.column}{item.row}"
                        if item.column is not None
                        else f"{mapper.col_for_period(period)}{item.row}"
                    ).upper()
                    identity = (sheet_name, cell)
                    if identity in existing or identity not in final_writes:
                        continue
                    plan.semantic_cells.append(
                        _semantic_cell_render(
                            model,
                            sheet_name,
                            cell,
                            item,
                            int(period),
                            final_writes[identity],
                            typed_values,
                            formatter,
                            annual_context,
                        )
                    )
                    existing.add(identity)
    finalize_render_contract(plan)


def _semantic_cell_render(
    model: FinancialModel,
    sheet: str,
    cell: str,
    item: LineItem,
    period: int,
    rendered: Any,
    typed_values: dict[str, dict[int, Any]],
    formatter: ModelFormatter,
    annual_context: AnnualRenderContext | None,
) -> SemanticCellRender:
    spec = _spec_for_period(model, item, period)
    if item.id in WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS:
        dependencies = (
            tuple(sorted(_extract_ref_ids(spec.params))) if spec is not None else ()
        )
        return SemanticCellRender(
            sheet=sheet,
            cell=cell.upper(),
            semantic_item_id=item.id,
            semantic_label=item.label,
            period=int(period),
            cell_kind="presentation",
            expected_state="known",
            expected_value=rendered,
            origin=(
                "compiled_formula"
                if isinstance(rendered, str) and rendered.startswith("=")
                else "compiled_literal"
            ),
            dependencies=dependencies,
            unit=item.unit.value if item.unit is not None else None,
            scale=item.model_scale,
            sign="natural",
            style=item.style.model_dump_json() if item.style is not None else None,
            tolerance=0.0,
            number_format=formatter.number_format_for(item),
        )
    typed = typed_values.get(item.id, {}).get(period)
    expected_state = str(getattr(typed, "state", "known"))
    expected_value = getattr(typed, "value", rendered)
    required_unavailable = (
        annual_context is not None
        and (item.id, period) in annual_context.required_unavailable
    )
    if (
        required_unavailable
        and expected_state != "known"
        and not item_renders_text(item.id, lambda item_id: _spec_for_period(model, model.get_item(item_id), period))
    ):
        cell_kind = "unavailable"
    elif isinstance(rendered, str) and rendered.startswith("="):
        cell_kind = "formula"
    else:
        cell_kind = "hardcode"
    if spec is not None:
        dependencies = tuple(sorted(_extract_ref_ids(spec.params)))
        origin = "compiled_formula" if cell_kind != "hardcode" else "compiled_literal"
    else:
        dependencies = ()
        origin = "observed_or_analyst_input"
    return SemanticCellRender(
        sheet=sheet,
        cell=cell.upper(),
        semantic_item_id=item.id,
        semantic_label=item.label,
        period=int(period),
        cell_kind=cell_kind,
        expected_state=expected_state,
        expected_value=expected_value,
        origin=origin,
        dependencies=dependencies,
        unit=item.unit.value if item.unit is not None else None,
        scale=item.model_scale,
        sign="natural",
        style=item.style.model_dump_json() if item.style is not None else None,
        tolerance=WRITER_NUMERIC_TOLERANCE,
        number_format=formatter.number_format_for(item),
    )


def _is_presentation_caption_spec(spec: FormulaSpec) -> bool:
    if spec.type is FormulaType.constant:
        return isinstance(spec.params.get("value"), str)
    if spec.type is not FormulaType.ref:
        return False
    source = spec.params.get("source")
    return (
        isinstance(source, LineItemRef)
        and source.id in WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS
        and source.t == 0
        and source.period_anchor == "first"
        and spec.params.get("adjustment") is None
        and not spec.params.get("negate")
    )


def _is_legacy_presentation_caption_spec(item: LineItem, spec: FormulaSpec) -> bool:
    if spec.type is not FormulaType.ref or spec.subtype != "ref_with_adjustment":
        return False
    if set(spec.params) != {"source", "adjustment"}:
        return False
    source = spec.params.get("source")
    adjustment = spec.params.get("adjustment")
    return (
        isinstance(source, LineItemRef)
        and source.id == item.id
        and source.t == -1
        and source.resolved is True
        and source.period_anchor == "first"
        and isinstance(adjustment, (int, float))
        and not isinstance(adjustment, bool)
        and adjustment == 1.0
    )


def _validate_presentation_caption_specs(model: FinancialModel) -> None:
    for sheet in model.sheets.values():
        layout = sheet.layout or SheetLayout(
            label_column="A",
            first_data_column="B",
        )
        mapper = AbsoluteColumnMapper(
            model.time_structure,
            layout.first_data_column or "B",
            period_scope=layout.period_scope,
        )
        for section in sheet.sections:
            for item in section.line_items:
                if item.id not in WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS:
                    continue
                if item.values is not None and any(
                    value.value is not None for value in item.values.values.values()
                ):
                    raise PresentationCaptionSpecError(
                        "presentation_caption_spec_violation: caption "
                        f"{item.id!r} must not carry values"
                    )
                for period, override in (item.overrides or {}).items():
                    if not _is_presentation_caption_spec(override):
                        raise PresentationCaptionSpecError(
                            "presentation_caption_spec_violation: caption "
                            f"{item.id!r} override for {period} violates P"
                        )
                periods = tuple(mapper.all_periods())
                if item.column is not None:
                    periods = (
                        tuple(item.formula_periods or ())
                        or tuple(model.time_structure.projection_periods)
                        or periods
                    )[:1]
                for period in periods:
                    spec = _spec_for_period(model, item, int(period))
                    if spec is None:
                        continue
                    if not (
                        _is_presentation_caption_spec(spec)
                        or _is_legacy_presentation_caption_spec(item, spec)
                    ):
                        raise PresentationCaptionSpecError(
                            "presentation_caption_spec_violation: active caption "
                            f"spec for {item.id!r} period {period} violates P or L"
                        )


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


def cell_render_input(
    model: FinancialModel,
    item: LineItem,
    period: int,
) -> FormulaSpec | float | str | None:
    """Select the renderer's input without compiling formulas or computing values.

    Fixed-column items always select at their anchor, irrespective of ``period``,
    and fall back to the earliest stored non-null value only when no spec or
    anchor value exists. A selected spec wins over stored values, including a
    constant whose compiler-normalized value is ``None`` or ``""``. Those blank
    constants return the blank itself; all other specs are returned uncompiled.
    """
    if item.column is not None:
        anchor = _fixed_cell_anchor_period(model, item)
        if anchor is None:
            return None
        period = anchor

    spec = _spec_for_period(model, item, period)
    if spec is not None:
        if spec.type is FormulaType.constant:
            literal = _normalize_literal((spec.params or {}).get("value"))
            if literal is None or literal == "":
                return literal
        return spec

    if item.values is not None:
        value_cell = item.values.values.get(int(period))
        if value_cell is not None and value_cell.value is not None:
            return float(value_cell.value)
        if item.column is not None:
            for value_period in sorted(item.values.values):
                value_cell = item.values.values[value_period]
                if value_cell.value is not None:
                    return float(value_cell.value)

    return None


def _render_cell_value(
    model: FinancialModel,
    item: LineItem,
    period: int,
    compiler: ExcelFormulaCompiler,
) -> Any:
    selected = cell_render_input(model, item, period)
    if isinstance(selected, FormulaSpec):
        return compiler.compile_formula(selected, period=period, item_id=item.id)
    return selected


def _render_fixed_cell(
    model: FinancialModel,
    item: LineItem,
    compiler: ExcelFormulaCompiler,
) -> Any:
    period = _fixed_cell_anchor_period(model, item)
    if period is None:
        return None

    return _render_cell_value(model, item, period, compiler)


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

    run: Optional[tuple[str, str, bool]] = None

    def flush() -> None:
        nonlocal run
        if run is None:
            return
        run_start, run_end, run_type = run
        formats.append(
            CellFormat(
                sheet=sheet_name,
                range=f"{run_start}{row}:{run_end}{row}",
                font_color=formatter.data_color_for(run_type, item),
            )
        )
        run = None

    for period in mapper.all_periods():
        column = mapper.col_for_period(period)
        cell_type = row_cell_types.get(column)
        if cell_type is None:
            flush()
            continue

        if run is None:
            run = (column, column, cell_type)
            continue

        run_start, run_end, run_type = run
        if run_type == cell_type and _col_to_index(column) == _col_to_index(run_end) + 1:
            run = (run_start, column, run_type)
            continue

        flush()
        run = (column, column, cell_type)

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

    run: Optional[tuple[str, str]] = None

    def flush() -> None:
        nonlocal run
        if run is None:
            return
        run_start, run_end = run
        formats.append(
            CellFormat(
                sheet=sheet_name,
                range=f"{run_start}{row}:{run_end}{row}",
                fill_color=formatter.input_fill_color,
            )
        )
        run = None

    for period in mapper.all_periods():
        column = mapper.col_for_period(period)
        cell_type = row_cell_types.get(column)
        if cell_type is not False:
            flush()
            continue

        if run is None:
            run = (column, column)
            continue

        run_start, run_end = run
        if _col_to_index(column) == _col_to_index(run_end) + 1:
            run = (run_start, column)
            continue

        flush()
        run = (column, column)

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
    "finalize_render_contract",
    "plan_to_mcp_calls",
    "render_plan_to_addin_payload",
    "reconcile_render_contract",
    "render_model",
]
