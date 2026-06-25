from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import Node
from .models import FormulaSpec, FormulaType, LineItemRef


FlattenAddSub = Callable[[Node], Optional[list[tuple[str, Node]]]]
ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
HasSameRowRef = Callable[[Any, list[LineItemRef]], bool]


def match_roll_forward(
    ast: Node,
    context: Any,
    *,
    flatten_add_sub_fn: FlattenAddSub,
    to_line_item_ref_fn: ToLineItemRef,
    has_same_row_ref_fn: HasSameRowRef,
) -> Optional[FormulaSpec]:
    """Match roll-forward schedules: beginning + adds - subs."""
    terms = flatten_add_sub_fn(ast)
    if terms is None:
        return None

    additions: list[LineItemRef] = []
    subtractions: list[LineItemRef] = []
    beginning: Optional[LineItemRef] = None
    for sign, node in terms:
        ref = to_line_item_ref_fn(node, context)
        if ref is None:
            return None
        if beginning is None:
            beginning = ref
            continue
        if sign == "+":
            additions.append(ref)
        else:
            subtractions.append(ref)

    if beginning and (additions or subtractions):
        if not has_same_row_ref_fn(context, [beginning] + additions + subtractions):
            return None
        return FormulaSpec(
            type=FormulaType.roll_forward,
            subtype="schedule",
            params={
                "beginning": beginning,
                "additions": additions,
                "subtractions": subtractions,
            },
        )
    return None


def has_same_row_ref(context: Any, refs: list[LineItemRef]) -> bool:
    row_map = context.sheet_row_to_item.get(context.sheet, {})
    if not row_map:
        return False
    id_to_row = {item_id: row for row, item_id in row_map.items()}
    for ref in refs:
        row = id_to_row.get(ref.id)
        if row == context.row:
            return True
    return False
