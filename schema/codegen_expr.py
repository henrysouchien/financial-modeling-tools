"""Emitter and expression compiler helpers for :mod:`schema.codegen`."""

from __future__ import annotations

from contextlib import contextmanager
import json
import sys
from typing import Iterator, List, Optional, Set

from .models import FormulaSpec, FormulaType, LineItemRef
from .refs import line_item_ref_from_obj


class CodeEmitter:
    """Indent-aware source emitter."""

    def __init__(self, indent: str = "    ") -> None:
        self._indent = indent
        self._level = 0
        self._lines: List[str] = []

    def line(self, text: str = "") -> None:
        if not text:
            self._lines.append("")
            return
        self._lines.append(f"{self._indent * self._level}{text}")

    def blank(self) -> None:
        self._lines.append("")

    def comment(self, text: str) -> None:
        self.line(f"# {text}")

    @contextmanager
    def indent(self) -> Iterator[None]:
        self._level += 1
        try:
            yield
        finally:
            self._level -= 1

    def to_string(self) -> str:
        return "\n".join(self._lines).rstrip() + "\n"


class ExprCompiler:
    """Compile FormulaSpec and expression nodes into Python expression strings."""

    def __init__(self, missing_refs: Optional[Set[str]] = None) -> None:
        self._missing_refs = set(missing_refs or set())

    def compile_formula(self, spec: Optional[FormulaSpec], item_id: Optional[str] = None) -> str:
        if spec is None:
            return "None"
        params = spec.params or {}

        if spec.type == FormulaType.constant:
            return _float_literal(params.get("value"))

        if spec.type == FormulaType.ref:
            value = self.compile_expr(params.get("source"))
            adjustment = params.get("adjustment")
            if adjustment is not None:
                value = f"_adjust({value}, {_float_literal(adjustment)})"
            if params.get("negate"):
                value = f"_negate({value})"
            return value

        if spec.type == FormulaType.arithmetic:
            if "expr" in params:
                return self.compile_expr(params.get("expr"))

            function = params.get("function")
            if function == "SUM_RANGE":
                target_obj = params.get("target")
                if line_item_ref_from_obj(target_obj) is None:
                    raise NotImplementedError(
                        f"SUM_RANGE in item {item_id!r} has malformed 'target' "
                        f"(expected LineItemRef-coercible; got {type(target_obj).__name__}: "
                        f"{target_obj!r})."
                    )
                raise NotImplementedError(
                    "SUM_RANGE is not supported in codegen-compute path. "
                    "Use dependency_graph evaluator."
                )
            if function in {"SUM", "AVERAGE", "MEDIAN"}:
                items = params.get("items", [])
                compiled = [self.compile_expr(it) for it in items]
                helper = (
                    "safe_avg"
                    if function == "AVERAGE"
                    else "safe_median"
                    if function == "MEDIAN"
                    else "safe_sum"
                )
                return f"{helper}({', '.join(compiled)})" if compiled else f"{helper}()"

            operands = params.get("operands")
            if isinstance(operands, list) and operands:
                operator = "+"
                start = 0
                if isinstance(operands[0], str) and operands[0] in {"+", "-", "*", "/"}:
                    operator = operands[0]
                    start = 1
                values = [self.compile_expr(op) for op in operands[start:]]
                if not values:
                    return "None"
                if operator == "+":
                    return f"safe_sum({', '.join(values)})"
                if operator == "-":
                    return f"safe_chain_sub({', '.join(values)})"
                if operator == "*":
                    return f"safe_mul({', '.join(values)})"
                if operator == "/":
                    return f"safe_chain_div({', '.join(values)})"
                return "None"

            items = params.get("items")
            if isinstance(items, list):
                compiled = [self.compile_expr(it) for it in items]
                return f"safe_items({', '.join(compiled)})" if compiled else "safe_items()"

            return "None"

        if spec.type == FormulaType.driver:
            base = self.compile_expr(params.get("base"))
            rate = self.compile_expr(params.get("rate"))
            kwargs: List[str] = []
            if "scale" in params:
                kwargs.append(f"scale={_float_literal(params.get('scale'))}")
            if "scale_fn" in params:
                kwargs.append(f"scale_fn={_quote(params.get('scale_fn'))}")
            if kwargs:
                return f"_driver({base}, {rate}, {', '.join(kwargs)})"
            return f"_driver({base}, {rate})"

        if spec.type == FormulaType.ratio:
            numerator = self.compile_expr(params.get("numerator"))
            denominator = self.compile_expr(params.get("denominator"))
            if params.get("subtract_one"):
                return f"_ratio_sub1({numerator}, {denominator})"
            return f"safe_div({numerator}, {denominator})"

        if spec.type == FormulaType.growth:
            base = self.compile_expr(params.get("base"))
            rate = self.compile_expr(params.get("rate"))
            return f"_growth({base}, {rate})"

        if spec.type == FormulaType.roll_forward:
            beginning = self.compile_expr(params.get("beginning"))
            additions = [self.compile_expr(x) for x in params.get("additions", [])]
            subtractions = [self.compile_expr(x) for x in params.get("subtractions", [])]
            add_expr = f"[{', '.join(additions)}]" if additions else "[]"
            sub_expr = f"[{', '.join(subtractions)}]" if subtractions else "[]"
            return f"_roll_fwd({beginning}, {add_expr}, {sub_expr})"

        if spec.type in {FormulaType.raw, FormulaType.valuation} and item_id:
            return f"ALL_CACHED.get({_quote(item_id)}, {{}}).get(p)"

        return "None"

    def compile_expr(self, expr) -> str:
        if expr is None:
            return "None"

        ref = line_item_ref_from_obj(expr)
        if ref is not None:
            return self._compile_ref(ref)

        if isinstance(expr, (int, float)) and not isinstance(expr, bool):
            return _float_literal(expr)

        if isinstance(expr, dict):
            op = expr.get("op")
            if op in {"+", "SUM", "AVG", "MAX", "*"}:
                args = [self.compile_expr(arg) for arg in expr.get("args", [])]
                if op == "+":
                    return f"expr_add({', '.join(args)})" if args else "expr_add()"
                if op == "SUM":
                    return f"safe_sum({', '.join(args)})" if args else "safe_sum()"
                if op == "AVG":
                    return f"safe_avg({', '.join(args)})" if args else "safe_avg()"
                if op == "MAX":
                    return f"expr_max({', '.join(args)})" if args else "expr_max()"
                return f"expr_mul({', '.join(args)})" if args else "expr_mul()"

            if op == "IFERROR":
                inner = self.compile_expr(expr.get("expr"))
                fallback = self.compile_expr(expr.get("fallback", 0))
                return f"expr_iferror({inner}, {fallback})"

            if op in {"-", "/", "^"}:
                left = self.compile_expr(expr.get("left"))
                right = self.compile_expr(expr.get("right"))
                if op == "-":
                    return f"safe_sub({left}, {right})"
                if op == "/":
                    return f"safe_div({left}, {right})"
                return f"_pow({left}, {right})"

            if op == "NEG":
                inner = self.compile_expr(expr.get("arg"))
                return f"_negate({inner})"

        return "None"

    def _compile_ref(self, ref: LineItemRef) -> str:
        if ref.period_anchor != "first":
            raise NotImplementedError(
                f"LineItemRef {ref.id!r} uses period_anchor={ref.period_anchor!r}; "
                "only 'first' is supported in codegen-compute path."
            )
        if ref.id in self._missing_refs:
            return "None"
        item = _quote(ref.id)
        if ref.t:
            return f"val(r, {item}, p, t={int(ref.t)})"
        return f"val(r, {item}, p)"


def _quote(value) -> str:
    override = _parent_override("_quote")
    if override is not None:
        return override(value)
    return json.dumps(value)


def _float_literal(value) -> str:
    override = _parent_override("_float_literal")
    if override is not None:
        return override(value)
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "1.0" if value else "0.0"
    return repr(float(value))


def _parent_override(name: str):
    parent = sys.modules.get("schema.codegen")
    if parent is None:
        return None
    candidate = getattr(parent, name, None)
    if candidate is not None and candidate is not globals()[name]:
        return candidate
    return None


__all__ = ["CodeEmitter", "ExprCompiler", "_float_literal", "_quote"]
