from __future__ import annotations

import re
import sys
from typing import Any, Dict, Iterable, List, Optional

from .renderer_types import CellFormat, CellWrite, RenderPlan, SheetSetup


_PARENT_MODULE = "schema.renderer"
_CELL_RE = re.compile(r"^([A-Z]+)(\d+)$")
_RANGE_RE = re.compile(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$")


def _compat(name: str, fallback: Any) -> Any:
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is not None and hasattr(parent, name):
        return getattr(parent, name)
    return fallback


def plan_to_mcp_calls(
    plan: RenderPlan,
    *,
    existing_sheets: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """Translate a RenderPlan into Excel MCP backend payloads."""

    existing = set(existing_sheets or [])
    setup_by_sheet = {setup.sheet: setup for setup in plan.sheet_setups}
    writes_by_sheet = plan.writes_by_sheet()
    formats_by_sheet = plan.formats_by_sheet()

    ordered_sheets = [setup.sheet for setup in plan.sheet_setups]
    for sheet_name in (*writes_by_sheet, *formats_by_sheet):
        if sheet_name not in setup_by_sheet and sheet_name not in ordered_sheets:
            ordered_sheets.append(sheet_name)

    data_column_bounds = _compat("_data_column_bounds", _data_column_bounds)
    batch_sheet_writes = _compat("_batch_sheet_writes", _batch_sheet_writes)
    batch_sheet_formats = _compat("_batch_sheet_formats", _batch_sheet_formats)

    calls: List[Dict[str, Any]] = []
    for sheet_name in ordered_sheets:
        if sheet_name not in existing:
            calls.append(
                {
                    "tool_name": "create_sheet",
                    "tool_input": {"sheet_name": sheet_name, "activate": True},
                }
            )
            existing.add(sheet_name)

        calls.append({"tool_name": "switch_sheet", "tool_input": {"sheet_name": sheet_name}})

        first_data_col_index, last_data_col_index = data_column_bounds(setup_by_sheet.get(sheet_name))
        for payload in batch_sheet_writes(
            writes_by_sheet.get(sheet_name, []),
            first_data_col_index,
            last_data_col_index,
        ):
            calls.append({"tool_name": "write_cells", "tool_input": payload})

        for payload in batch_sheet_formats(formats_by_sheet.get(sheet_name, [])):
            calls.append({"tool_name": "format_cells", "tool_input": payload})

    return calls


def render_plan_to_addin_payload(
    plan: RenderPlan,
    conflict_strategy: str = "fail_on_collision",
) -> Dict[str, Any]:
    """Serialize a RenderPlan into the apply_render_plan tool input."""

    if conflict_strategy not in ("fail_on_collision", "overwrite"):
        raise ValueError(
            "v1 supports conflict_strategy in {fail_on_collision, overwrite}; "
            f"got {conflict_strategy!r}. (`create_new` is a v2 follow-up.)"
        )

    setup_sheets = [setup.sheet for setup in plan.sheet_setups]
    writes_by_sheet = plan.writes_by_sheet()
    formats_by_sheet = plan.formats_by_sheet()

    ordered: List[str] = list(setup_sheets)
    seen = set(ordered)
    for sheet_name in (*writes_by_sheet.keys(), *formats_by_sheet.keys()):
        if sheet_name not in seen:
            ordered.append(sheet_name)
            seen.add(sheet_name)

    setup_by_sheet = {setup.sheet: setup for setup in plan.sheet_setups}
    sheets = []
    for name in ordered:
        sheet_payload: Dict[str, Any] = {"name": name}
        setup = setup_by_sheet.get(name)
        if setup is not None:
            if setup.column_widths:
                sheet_payload["column_widths"] = dict(setup.column_widths)
            if setup.freeze_panes:
                sheet_payload["freeze_panes"] = setup.freeze_panes
        sheets.append(sheet_payload)

    plan_to_calls = _compat("plan_to_mcp_calls", plan_to_mcp_calls)
    calls = plan_to_calls(plan, existing_sheets=None)
    writes: List[Dict[str, Any]] = []
    formats: List[Dict[str, Any]] = []
    current_sheet: Optional[str] = None
    for call in calls:
        if call["tool_name"] == "switch_sheet":
            current_sheet = call["tool_input"]["sheet_name"]
        elif call["tool_name"] == "write_cells":
            writes.append({"sheet": current_sheet, **call["tool_input"]})
        elif call["tool_name"] == "format_cells":
            formats.append({"sheet": current_sheet, **call["tool_input"]})

    declared = {s["name"] for s in sheets}
    for write in writes:
        if write["sheet"] not in declared:
            raise ValueError(
                f"render_plan_to_addin_payload: write targets undeclared sheet {write['sheet']!r}"
            )
    for cell_format in formats:
        if cell_format["sheet"] not in declared:
            raise ValueError(
                "render_plan_to_addin_payload: "
                f"format targets undeclared sheet {cell_format['sheet']!r}"
            )

    return {
        "sheets": sheets,
        "writes": writes,
        "formats": formats,
        "conflict_strategy": conflict_strategy,
    }


def _data_column_bounds(setup: Optional[SheetSetup]) -> tuple[int, int]:
    col_to_index = _compat("_col_to_index", _col_to_index)
    if setup is None:
        return 1, 1
    if setup.first_data_column:
        first_index = col_to_index(setup.first_data_column)
        data_width_indices = [
            col_to_index(column)
            for column in setup.column_widths
            if col_to_index(column) >= first_index
        ]
        return first_index, max(data_width_indices, default=first_index)
    if not setup.column_widths:
        return 1, 1
    indices = sorted(col_to_index(column) for column in setup.column_widths)
    if len(indices) == 1:
        return indices[0], indices[0]
    return indices[1], indices[-1]


def _batch_sheet_writes(
    writes: List[CellWrite],
    first_data_col_index: int,
    last_data_col_index: int,
) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    pending_vertical: List[tuple[int, int, Any]] = []
    pending_row: List[tuple[int, int, Any]] = []
    split_cell = _compat("_split_cell", _split_cell)
    col_to_index = _compat("_col_to_index", _col_to_index)
    index_to_col = _compat("_index_to_col", _index_to_col)

    def flush_vertical() -> None:
        nonlocal pending_vertical
        if not pending_vertical:
            return
        start_col, start_row, first_value = pending_vertical[0]
        end_row = pending_vertical[-1][1]
        cell_col = index_to_col(start_col)
        if len(pending_vertical) == 1:
            payloads.append(
                {
                    "range": f"{cell_col}{start_row}",
                    "values": first_value,
                }
            )
        else:
            payloads.append(
                {
                    "range": f"{cell_col}{start_row}:{cell_col}{end_row}",
                    "values": [[value] for _col, _row, value in pending_vertical],
                }
            )
        pending_vertical = []

    def flush_row() -> None:
        nonlocal pending_row
        if not pending_row:
            return
        row = pending_row[0][1]
        ordered = sorted(pending_row, key=lambda entry: entry[0])
        runs: List[List[tuple[int, int, Any]]] = []
        for entry in ordered:
            if runs and entry[0] == runs[-1][-1][0] + 1:
                runs[-1].append(entry)
            else:
                runs.append([entry])

        for run in runs:
            start_col = run[0][0]
            end_col = run[-1][0]
            values = [value for _col, _row, value in run]
            if len(run) == 1:
                payloads.append(
                    {
                        "range": f"{index_to_col(start_col)}{row}",
                        "values": values[0],
                    }
                )
            else:
                payloads.append(
                    {
                        "range": f"{index_to_col(start_col)}{row}:{index_to_col(end_col)}{row}",
                        "values": [values],
                    }
                )
        pending_row = []

    for write in writes:
        col_letters, row = split_cell(write.cell)
        col_idx = col_to_index(col_letters)
        is_data = col_idx >= first_data_col_index
        if is_data:
            flush_vertical()
            if pending_row and pending_row[0][1] == row:
                pending_row.append((col_idx, row, write.value))
            else:
                flush_row()
                pending_row = [(col_idx, row, write.value)]
            continue

        flush_row()
        if (
            pending_vertical
            and pending_vertical[-1][0] == col_idx
            and pending_vertical[-1][1] + 1 == row
        ):
            pending_vertical.append((col_idx, row, write.value))
        else:
            flush_vertical()
            pending_vertical = [(col_idx, row, write.value)]

    flush_vertical()
    flush_row()
    return payloads


def _batch_sheet_formats(formats: List[CellFormat]) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    current: Optional[CellFormat] = None
    current_start_row = 0
    current_end_row = 0
    current_start_col = ""
    current_end_col = ""
    split_range = _compat("_split_range", _split_range)

    def flush() -> None:
        nonlocal current, current_start_row, current_end_row, current_start_col, current_end_col
        if current is None:
            return
        range_text = (
            f"{current_start_col}{current_start_row}:{current_end_col}{current_end_row}"
            if current_start_row != current_end_row
            else f"{current_start_col}{current_start_row}:{current_end_col}{current_start_row}"
        )
        payload: Dict[str, Any] = {"range": range_text}
        if current.bold is not None:
            payload["bold"] = current.bold
        if current.font_color is not None:
            payload["font_color"] = current.font_color
        if current.number_format is not None:
            payload["number_format"] = current.number_format
        if current.underline is not None:
            payload["underline"] = current.underline
        if current.fill_color is not None:
            payload["fill_color"] = current.fill_color
        if current.top_border_color is not None:
            payload["top_border_color"] = current.top_border_color
        if current.bottom_border_color is not None:
            payload["bottom_border_color"] = current.bottom_border_color
        payloads.append(payload)
        current = None

    for fmt in formats:
        start_col, start_row, end_col, end_row = split_range(fmt.range)
        if (
            current is not None
            and current.bold == fmt.bold
            and current.font_color == fmt.font_color
            and current.number_format == fmt.number_format
            and current.underline == fmt.underline
            and current.fill_color == fmt.fill_color
            and current.top_border_color == fmt.top_border_color
            and current.bottom_border_color == fmt.bottom_border_color
            and current_start_col == start_col
            and current_end_col == end_col
            and current_end_row + 1 == start_row
        ):
            current_end_row = end_row
            continue

        flush()
        current = fmt
        current_start_row = start_row
        current_end_row = end_row
        current_start_col = start_col
        current_end_col = end_col

    flush()
    return payloads


def _split_cell(cell: str) -> tuple[str, int]:
    match = _CELL_RE.match(cell)
    if match is None:
        raise ValueError(f"Invalid cell reference: {cell}")
    return match.group(1), int(match.group(2))


def _split_range(range_text: str) -> tuple[str, int, str, int]:
    match = _RANGE_RE.match(range_text)
    if match is None:
        raise ValueError(f"Invalid range reference: {range_text}")
    return match.group(1), int(match.group(2)), match.group(3), int(match.group(4))


def _col_to_index(col: str) -> int:
    index = 0
    for ch in col.upper():
        if not ch.isalpha():
            continue
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index


def _index_to_col(idx: int) -> str:
    letters: List[str] = []
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters.append(chr(rem + ord("A")))
    return "".join(reversed(letters))


__all__ = [
    "_batch_sheet_formats",
    "_batch_sheet_writes",
    "_col_to_index",
    "_data_column_bounds",
    "_index_to_col",
    "_split_cell",
    "_split_range",
    "plan_to_mcp_calls",
    "render_plan_to_addin_payload",
]
