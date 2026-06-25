from __future__ import annotations

from typing import Optional

from .formula_ast import BinaryOp, FuncCall, Node, Number, UnaryOp


def constant_value(ast: Node) -> Optional[float]:
    """Evaluate constant-only formulas.

    If any reference or unsupported node appears, returns None so pattern
    matching can continue through the non-constant paths.
    """
    if isinstance(ast, Number):
        return ast.value
    if isinstance(ast, UnaryOp) and ast.op in {"+", "-"}:
        inner = constant_value(ast.expr)
        if inner is None:
            return None
        return inner if ast.op == "+" else -inner
    if isinstance(ast, BinaryOp) and ast.op in {"+", "-", "*", "/", "^"}:
        left = constant_value(ast.left)
        right = constant_value(ast.right)
        if left is None or right is None:
            return None
        if ast.op == "+":
            return left + right
        if ast.op == "-":
            return left - right
        if ast.op == "*":
            return left * right
        if ast.op == "/":
            return left / right if right != 0 else None
        if ast.op == "^":
            try:
                return left ** right
            except (ValueError, OverflowError):
                return None
    if isinstance(ast, FuncCall) and ast.name in {"SUM", "AVERAGE"}:
        total = 0.0
        for arg in ast.args:
            value = constant_value(arg)
            if value is None:
                return None
            total += value
        if ast.name == "AVERAGE":
            return total / len(ast.args) if ast.args else None
        return total
    return None
