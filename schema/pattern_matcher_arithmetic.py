from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import BinaryOp, Empty, FuncCall, Node, Range
from .models import FormulaSpec, FormulaType, LineItemRef


RangeToRefs = Callable[[Range, Any], list[LineItemRef]]
ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
ExprFromNode = Callable[[Node, Any], Any]
FunctionArithmeticSubtype = Callable[[FuncCall], Optional[str]]
BinaryArithmeticSubtype = Callable[[BinaryOp, Any], Optional[str]]
FlattenBinary = Callable[[Node, str], Optional[list[Node]]]
FlattenAddSub = Callable[[Node], Optional[list[tuple[str, Node]]]]


def match_arithmetic(
    ast: Node,
    context: Any,
    *,
    range_to_refs_fn: RangeToRefs,
    to_line_item_ref_fn: ToLineItemRef,
    expr_from_node_fn: ExprFromNode,
    function_arithmetic_subtype_fn: FunctionArithmeticSubtype,
    binary_arithmetic_subtype_fn: BinaryArithmeticSubtype,
    flatten_binary_fn: FlattenBinary,
) -> Optional[FormulaSpec]:
    """Match arithmetic patterns (SUM/AVERAGE, add/sub/mul/div chains)."""
    if isinstance(ast, FuncCall) and ast.name in {"SUM", "AVERAGE"}:
        items = []
        for arg in ast.args:
            if isinstance(arg, Empty):
                continue
            if isinstance(arg, Range):
                items.extend(r for r in range_to_refs_fn(arg, context) if r.resolved)
                continue
            ref = to_line_item_ref_fn(arg, context)
            if ref is not None and ref.resolved:
                items.append(ref)
                continue
            if ref is not None and not ref.resolved:
                continue
            expr = expr_from_node_fn(arg, context)
            if expr is None:
                return None
            items.append(expr)
        if items:
            return FormulaSpec(
                type=FormulaType.arithmetic,
                subtype=function_arithmetic_subtype_fn(ast),
                params={"function": ast.name, "items": items},
            )

    if isinstance(ast, BinaryOp) and ast.op in {"+", "-", "*", "/"}:
        subtype = binary_arithmetic_subtype_fn(ast, context)
        values = flatten_binary_fn(ast, ast.op)
        if values is not None:
            operands = [ast.op]
            ok = True
            for node in values:
                ref = to_line_item_ref_fn(node, context)
                if ref is None or not ref.resolved:
                    ok = False
                    break
                operands.append(ref)
            if ok:
                return FormulaSpec(
                    type=FormulaType.arithmetic,
                    subtype=subtype,
                    params={"operands": operands},
                )

    expr = expr_from_node_fn(ast, context)
    if expr is not None and not isinstance(expr, (LineItemRef, int, float)):
        subtype = binary_arithmetic_subtype_fn(ast, context) if isinstance(ast, BinaryOp) else None
        return FormulaSpec(type=FormulaType.arithmetic, subtype=subtype, params={"expr": expr})

    return None


def function_arithmetic_subtype(ast: FuncCall) -> Optional[str]:
    if ast.name != "SUM":
        return None
    if any(isinstance(arg, Range) for arg in ast.args):
        return "sum_range"
    return "sum_list"


def binary_arithmetic_subtype(
    ast: BinaryOp,
    context: Any,
    *,
    flatten_add_sub_fn: FlattenAddSub,
    to_line_item_ref_fn: ToLineItemRef,
) -> Optional[str]:
    if ast.op not in {"+", "-"}:
        return None
    terms = flatten_add_sub_fn(ast)
    if not terms or len(terms) < 2:
        return None
    for _sign, node in terms:
        ref = to_line_item_ref_fn(node, context)
        if ref is None or not ref.resolved:
            return None
    if len(terms) > 2 and all(sign == "+" for sign, _node in terms):
        return "multi_term"
    return "add_subtract"
