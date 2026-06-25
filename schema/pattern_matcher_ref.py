from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import BinaryOp, Node, Number, Ref, UnaryOp
from .models import FormulaSpec, FormulaType, LineItemRef


ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
RefSubtype = Callable[..., str]
ColToIndex = Callable[[str], int]


def match_ref(
    ast: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
    ref_subtype_fn: RefSubtype,
) -> Optional[FormulaSpec]:
    """Match a direct reference with optional adjustment or negation."""
    if isinstance(ast, Ref):
        ref = to_line_item_ref_fn(ast, context)
        if ref is None:
            return None
        return FormulaSpec(
            type=FormulaType.ref,
            subtype=ref_subtype_fn(ast, context),
            params={"source": ref},
        )

    if isinstance(ast, UnaryOp) and ast.op == "-" and isinstance(ast.expr, Ref):
        ref = to_line_item_ref_fn(ast.expr, context)
        if ref is None:
            return None
        return FormulaSpec(
            type=FormulaType.ref,
            subtype=ref_subtype_fn(ast.expr, context, negated=True),
            params={"source": ref, "negate": True},
        )

    if isinstance(ast, BinaryOp) and ast.op in {"+", "-"}:
        left_ref = ast.left if isinstance(ast.left, Ref) else None
        right_ref = ast.right if isinstance(ast.right, Ref) else None
        left_num = ast.left if isinstance(ast.left, Number) else None
        right_num = ast.right if isinstance(ast.right, Number) else None
        if left_ref and right_num:
            ref = to_line_item_ref_fn(left_ref, context)
            if ref is None:
                return None
            adjustment = right_num.value if ast.op == "+" else -right_num.value
            return FormulaSpec(
                type=FormulaType.ref,
                subtype=ref_subtype_fn(left_ref, context, adjusted=True),
                params={"source": ref, "adjustment": adjustment},
            )
        if right_ref and left_num:
            if ast.op == "-":
                # Number - Ref is NOT a ref+adjustment pattern.
                # It means -(Ref) + Number, which is semantically different.
                # Let it fall through to arithmetic matching.
                pass
            else:
                ref = to_line_item_ref_fn(right_ref, context)
                if ref is None:
                    return None
                adjustment = left_num.value
                return FormulaSpec(
                    type=FormulaType.ref,
                    subtype=ref_subtype_fn(right_ref, context, adjusted=True),
                    params={"source": ref, "adjustment": adjustment},
                )
    return None


def ref_subtype(
    node: Ref,
    context: Any,
    *,
    col_to_index_fn: ColToIndex,
    adjusted: bool = False,
    negated: bool = False,
) -> str:
    if adjusted:
        return "ref_with_adjustment"
    if negated:
        return "negated_ref"

    source_col = col_to_index_fn(node.col)
    if node.sheet is not None:
        if context.col == 1 and source_col == 1:
            return "label_mirror"
        return "cross_sheet_ref"

    if node.row == context.row and source_col == context.col - 1:
        # In the canonical model H is the first projection/input-default
        # column copied from the last historical column; later columns are
        # true prior-period carry-forward formulas.
        if context.col > 8:
            return "carry_forward"
        return "cell_ref"
    return "cell_ref"
