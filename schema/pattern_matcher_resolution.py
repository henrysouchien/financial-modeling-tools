from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import Node, Number, Range, Ref
from .models import LineItemRef


ColToIndex = Callable[[str], int]
PeriodOffset = Callable[[Any, str, int], tuple[int, bool]]


def to_line_item_ref(
    node: Node,
    context: Any,
    *,
    col_to_index_fn: ColToIndex,
    period_offset_fn: PeriodOffset,
) -> Optional[LineItemRef]:
    """Resolve a cell reference to a semantic LineItemRef with time offset."""
    if isinstance(node, Ref):
        sheet = node.sheet or context.sheet
        row_map = context.sheet_row_to_item.get(sheet, {})
        line_item_id = row_map.get(node.row)
        if not line_item_id:
            return None

        col_idx = col_to_index_fn(node.col)
        t, resolved = period_offset_fn(context, sheet, col_idx)

        return LineItemRef(id=line_item_id, t=t, resolved=resolved)

    if isinstance(node, Number):
        return None

    return None


def range_to_refs(
    node: Range,
    context: Any,
    *,
    col_to_index_fn: ColToIndex,
    period_offset_fn: PeriodOffset,
) -> list[LineItemRef]:
    """Resolve a range into a list of LineItemRefs (vertical or horizontal)."""
    sheet = node.start.sheet or context.sheet
    row_map = context.sheet_row_to_item.get(sheet, {})
    refs: list[LineItemRef] = []

    start_col_idx = col_to_index_fn(node.start.col)
    end_col_idx = col_to_index_fn(node.end.col)
    start_row = min(node.start.row, node.end.row)
    end_row = max(node.start.row, node.end.row)

    if node.start.col == node.end.col:
        t, resolved = period_offset_fn(context, sheet, start_col_idx)
        for row in range(start_row, end_row + 1):
            line_item_id = row_map.get(row)
            if line_item_id:
                refs.append(LineItemRef(id=line_item_id, t=t, resolved=resolved))
        return refs

    if node.start.row == node.end.row:
        line_item_id = row_map.get(node.start.row)
        if not line_item_id:
            return []
        for col_idx in range(min(start_col_idx, end_col_idx), max(start_col_idx, end_col_idx) + 1):
            t, resolved = period_offset_fn(context, sheet, col_idx)
            if not resolved:
                continue
            refs.append(LineItemRef(id=line_item_id, t=t, resolved=resolved))
        return refs

    return refs


def period_offset(context: Any, target_sheet: str, target_col: int) -> tuple[int, bool]:
    """Return time offset when both columns resolve to known periods."""
    target_period = context.sheet_col_to_period.get(target_sheet, {}).get(target_col)
    current_period = context.sheet_col_to_period.get(context.sheet, {}).get(context.col)
    if target_period is None or current_period is None:
        return 0, False
    time_index = {period: idx for idx, period in enumerate(context.time_order)}
    if target_period not in time_index or current_period not in time_index:
        return 0, False
    return time_index[target_period] - time_index[current_period], True


def col_to_index(col: str) -> int:
    col = col.upper()
    index = 0
    for ch in col:
        if not ch.isalpha():
            continue
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index
