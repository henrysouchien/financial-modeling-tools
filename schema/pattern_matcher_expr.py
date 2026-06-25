from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import BinaryOp, Empty, FuncCall, Node, Number, Range, Ref, UnaryOp
from .models import LineItemRef


ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
RangeToRefs = Callable[[Range, Any], list[LineItemRef]]
ExprFromNode = Callable[[Node, Any], Any]
AppendExprArg = Callable[[list[Any], Any, str], None]
ContainsRef = Callable[[Any], bool]


def expr_from_node(
    node: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
    range_to_refs_fn: RangeToRefs,
    expr_from_node_fn: ExprFromNode,
    append_expr_arg_fn: AppendExprArg,
) -> Any:
    """Convert an AST node into a small evaluable expression tree."""
    if isinstance(node, Ref):
        ref = to_line_item_ref_fn(node, context)
        if ref is not None and not ref.resolved:
            return 0.0
        return ref
    if isinstance(node, Number):
        return float(node.value)
    if isinstance(node, UnaryOp):
        expr = expr_from_node_fn(node.expr, context)
        if expr is None:
            return None
        if node.op == "+":
            return expr
        if node.op == "-":
            return {"op": "NEG", "arg": expr}
        return None
    if isinstance(node, BinaryOp):
        left = expr_from_node_fn(node.left, context)
        right = expr_from_node_fn(node.right, context)
        if left is None or right is None:
            return None
        if node.op in {"+", "*"}:
            args: list[Any] = []
            append_expr_arg_fn(args, left, node.op)
            append_expr_arg_fn(args, right, node.op)
            return {"op": node.op, "args": args}
        if node.op in {"-", "/", "^"}:
            return {"op": node.op, "left": left, "right": right}
        return None
    if isinstance(node, FuncCall) and node.name in {"SUM", "AVERAGE"}:
        args: list[Any] = []
        for arg in node.args:
            if isinstance(arg, Empty):
                continue
            if isinstance(arg, Range):
                args.extend(r for r in range_to_refs_fn(arg, context) if r.resolved)
                continue
            expr = expr_from_node_fn(arg, context)
            if expr is None:
                return None
            if isinstance(expr, float) and expr == 0.0 and isinstance(arg, Ref):
                ref = to_line_item_ref_fn(arg, context)
                if ref is not None and not ref.resolved:
                    continue
            args.append(expr)
        if not args:
            if node.name == "SUM":
                return 0.0
            return None
        return {"op": "SUM" if node.name == "SUM" else "AVG", "args": args}
    return None


def append_expr_arg(args: list[Any], expr: Any, op: str) -> None:
    """Flatten associative operations in expression trees."""
    if isinstance(expr, dict) and expr.get("op") == op and isinstance(expr.get("args"), list):
        args.extend(expr["args"])
    else:
        args.append(expr)


def contains_ref(expr: Any, *, contains_ref_fn: ContainsRef) -> bool:
    """Check whether an expression tree contains any LineItemRef."""
    if isinstance(expr, LineItemRef):
        return True
    if isinstance(expr, dict):
        for value in expr.values():
            if contains_ref_fn(value):
                return True
    if isinstance(expr, (list, tuple, set)):
        for value in expr:
            if contains_ref_fn(value):
                return True
    return False
