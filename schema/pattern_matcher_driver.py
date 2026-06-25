from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import BinaryOp, Node, Number, Ref
from .models import FormulaSpec, FormulaType, LineItemRef


ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
ExprFromNode = Callable[[Node, Any], Any]
ContainsRef = Callable[[Any], bool]
IsRateXBase = Callable[[Node, Node, Any], bool]
IsRateLikeId = Callable[[str], bool]
IdTokens = Callable[[str], list[str]]


def match_driver(
    ast: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
    expr_from_node_fn: ExprFromNode,
    contains_ref_fn: ContainsRef,
    is_rate_x_base_fn: IsRateXBase,
) -> Optional[FormulaSpec]:
    """Match driver formulas (base * rate), with nested arithmetic support."""
    if not isinstance(ast, BinaryOp) or ast.op != "*":
        return None

    # Handle (ref / number) * ref patterns (e.g., revenue/365 * dso)
    left_div = ast.left if isinstance(ast.left, BinaryOp) and ast.left.op == "/" else None
    right_div = ast.right if isinstance(ast.right, BinaryOp) and ast.right.op == "/" else None
    if (
        left_div
        and isinstance(left_div.left, Ref)
        and isinstance(left_div.right, Number)
        and isinstance(ast.right, Ref)
    ):
        base_ref = to_line_item_ref_fn(left_div.left, context)
        rate_ref = to_line_item_ref_fn(ast.right, context)
        if base_ref and rate_ref:
            return FormulaSpec(
                type=FormulaType.driver,
                subtype="base_x_rate",
                params={
                    "base": base_ref,
                    "rate": rate_ref,
                    "scale": left_div.right.value,
                },
            )

    if (
        right_div
        and isinstance(right_div.left, Ref)
        and isinstance(right_div.right, Number)
        and isinstance(ast.left, Ref)
    ):
        base_ref = to_line_item_ref_fn(ast.left, context)
        rate_ref = to_line_item_ref_fn(right_div.left, context)
        if base_ref and rate_ref:
            return FormulaSpec(
                type=FormulaType.driver,
                subtype="base_x_rate",
                params={
                    "base": base_ref,
                    "rate": rate_ref,
                    "scale": right_div.right.value,
                },
            )
    left_ref = to_line_item_ref_fn(ast.left, context)
    right_ref = to_line_item_ref_fn(ast.right, context)
    left_num = ast.left.value if isinstance(ast.left, Number) else None
    right_num = ast.right.value if isinstance(ast.right, Number) else None

    if left_ref and right_ref:
        if is_rate_x_base_fn(ast.left, ast.right, context):
            return FormulaSpec(
                type=FormulaType.driver,
                subtype="rate_x_base",
                params={"base": right_ref, "rate": left_ref},
            )
        return FormulaSpec(
            type=FormulaType.driver,
            subtype="base_x_rate",
            params={"base": left_ref, "rate": right_ref},
        )
    if left_ref and right_num is not None:
        return FormulaSpec(
            type=FormulaType.driver,
            subtype="base_x_rate",
            params={"base": left_ref, "rate": right_num},
        )
    if right_ref and left_num is not None:
        return FormulaSpec(
            type=FormulaType.driver,
            subtype="base_x_rate",
            params={"base": right_ref, "rate": left_num},
        )

    left_expr = expr_from_node_fn(ast.left, context)
    right_expr = expr_from_node_fn(ast.right, context)
    if left_expr is None or right_expr is None:
        return None
    if not (contains_ref_fn(left_expr) or contains_ref_fn(right_expr)):
        return None
    return FormulaSpec(
        type=FormulaType.driver,
        subtype="base_x_rate",
        params={"base": left_expr, "rate": right_expr},
    )


def is_rate_x_base(
    left_node: Node,
    right_node: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
    is_rate_like_id_fn: IsRateLikeId,
) -> bool:
    if not isinstance(left_node, Ref) or not isinstance(right_node, Ref):
        return False

    left_ref = to_line_item_ref_fn(left_node, context)
    right_ref = to_line_item_ref_fn(right_node, context)
    if left_ref is None or right_ref is None:
        return False

    return is_rate_like_id_fn(left_ref.id) and not is_rate_like_id_fn(right_ref.id)


def is_rate_like_id(line_item_id: str, *, id_tokens_fn: IdTokens) -> bool:
    rate_tokens = {"rate", "pct", "percent", "percentage", "margin", "ratio", "yield"}
    return bool(rate_tokens & set(id_tokens_fn(line_item_id)))
