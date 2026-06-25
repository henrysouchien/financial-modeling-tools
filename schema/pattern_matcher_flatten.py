from __future__ import annotations

from collections.abc import Callable

from .formula_ast import BinaryOp, Node


FlattenAddSub = Callable[[Node], list[tuple[str, Node]] | None]
FlattenBinary = Callable[[Node, str], list[Node] | None]


def flatten_add_sub(ast: Node, *, flatten_add_sub_fn: FlattenAddSub) -> list[tuple[str, Node]] | None:
    """Flatten add/sub trees into signed term lists."""
    if isinstance(ast, BinaryOp) and ast.op in {"+", "-"}:
        left = flatten_add_sub_fn(ast.left)
        right = flatten_add_sub_fn(ast.right)
        if left is None or right is None:
            return None
        if ast.op == "+":
            return left + right
        inverted = [("-" if sign == "+" else "+", node) for sign, node in right]
        return left + inverted
    return [("+", ast)]


def flatten_binary(ast: Node, op: str, *, flatten_binary_fn: FlattenBinary) -> list[Node] | None:
    """Flatten associative binary operators into a single list."""
    if isinstance(ast, BinaryOp) and ast.op == op:
        left = flatten_binary_fn(ast.left, op)
        right = flatten_binary_fn(ast.right, op)
        if left is None or right is None:
            return None
        return left + right
    return [ast]
