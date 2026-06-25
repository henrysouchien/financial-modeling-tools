from __future__ import annotations

from collections.abc import Callable
import re
from typing import Any, Optional

from .formula_ast import BinaryOp, Node, Number, Ref
from .models import FormulaSpec, FormulaType, LineItemRef


ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
ExprFromNode = Callable[[Node, Any], Any]
IsDeltaExpr = Callable[[Node, Any], bool]
RatioSubtype = Callable[[Node, Node, Any], str]
IsShareDenominator = Callable[[Node, Any], bool]
IsShareCountId = Callable[[str], bool]
IdTokens = Callable[[str], list[str]]
HasTokenSequence = Callable[[list[str], tuple[str, ...]], bool]


def match_ratio(
    ast: Node,
    context: Any,
    *,
    expr_from_node_fn: ExprFromNode,
    is_delta_expr_fn: IsDeltaExpr,
    ratio_subtype_fn: RatioSubtype,
) -> Optional[FormulaSpec]:
    """Match ratio patterns, including YoY growth and incremental margins."""
    if isinstance(ast, BinaryOp) and ast.op == "-":
        if (
            isinstance(ast.right, Number)
            and ast.right.value == 1
            and isinstance(ast.left, BinaryOp)
            and ast.left.op == "/"
        ):
            numerator = expr_from_node_fn(ast.left.left, context)
            denominator = expr_from_node_fn(ast.left.right, context)
            if numerator and denominator:
                return FormulaSpec(
                    type=FormulaType.ratio,
                    subtype="yoy_growth",
                    params={
                        "numerator": numerator,
                        "denominator": denominator,
                        "subtract_one": True,
                    },
                )

    if isinstance(ast, BinaryOp) and ast.op == "/":
        numerator = expr_from_node_fn(ast.left, context)
        denominator = expr_from_node_fn(ast.right, context)
        if numerator and denominator:
            subtype = None
            if is_delta_expr_fn(ast.left, context) and is_delta_expr_fn(ast.right, context):
                subtype = "incremental_margin"
            return FormulaSpec(
                type=FormulaType.ratio,
                subtype=subtype or ratio_subtype_fn(ast.left, ast.right, context),
                params={"numerator": numerator, "denominator": denominator},
            )
    return None


def ratio_subtype(
    numerator_node: Node,
    denominator_node: Node,
    context: Any,
    *,
    is_share_denominator_fn: IsShareDenominator,
) -> str:
    if is_share_denominator_fn(denominator_node, context):
        return "per_share"
    return "divide"


def is_share_denominator(
    node: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
    is_share_count_id_fn: IsShareCountId,
) -> bool:
    if not isinstance(node, Ref):
        return False
    ref = to_line_item_ref_fn(node, context)
    if ref is None:
        return False
    return is_share_count_id_fn(ref.id)


def is_share_count_id(
    line_item_id: str,
    *,
    id_tokens_fn: IdTokens,
    has_token_sequence_fn: HasTokenSequence,
) -> bool:
    tokens = id_tokens_fn(line_item_id)
    if has_token_sequence_fn(tokens, ("diluted", "shares")):
        return True
    if has_token_sequence_fn(tokens, ("dilutive", "shares")):
        return True
    if has_token_sequence_fn(tokens, ("weighted", "average", "diluted", "shares")):
        return True
    if has_token_sequence_fn(tokens, ("shares", "outstanding")):
        return True
    if has_token_sequence_fn(tokens, ("shares", "issued")):
        return True
    if has_token_sequence_fn(tokens, ("share", "count")):
        return True
    return any(
        token == "shares" and idx + 1 < len(tokens) and tokens[idx + 1].startswith("fy")
        for idx, token in enumerate(tokens)
    )


def id_tokens(line_item_id: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", line_item_id.lower()) if token]


def has_token_sequence(tokens: list[str], sequence: tuple[str, ...]) -> bool:
    if len(tokens) < len(sequence):
        return False
    end = len(tokens) - len(sequence) + 1
    return any(tuple(tokens[idx : idx + len(sequence)]) == sequence for idx in range(end))


def is_delta_expr(
    node: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
) -> bool:
    """Check for (A[t] - A[t-1]) shapes for incremental ratios."""
    if not isinstance(node, BinaryOp) or node.op != "-":
        return False
    if not isinstance(node.left, Ref) or not isinstance(node.right, Ref):
        return False
    left_ref = to_line_item_ref_fn(node.left, context)
    right_ref = to_line_item_ref_fn(node.right, context)
    if not left_ref or not right_ref:
        return False
    return left_ref.id == right_ref.id and left_ref.t != right_ref.t
