"""Generate standalone Python compute modules from FinancialModel schema objects."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from pprint import pformat
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

from .dependency_graph import DependencyGraph
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    PERIOD_MODE_YEARLY,
    ValueProvenance,
)

_INPUT_PROVENANCE = {
    ValueProvenance.input,
    ValueProvenance.imported_other,
    ValueProvenance.imported_edgar,
    ValueProvenance.imported_fmp,
}


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
            if function in {"SUM", "AVERAGE"}:
                items = params.get("items", [])
                compiled = [self.compile_expr(it) for it in items]
                helper = "safe_avg" if function == "AVERAGE" else "safe_sum"
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

        ref = _line_item_ref_from_obj(expr)
        if ref is not None:
            return self._compile_ref(ref)

        if isinstance(expr, (int, float)) and not isinstance(expr, bool):
            return _float_literal(expr)

        if isinstance(expr, dict):
            op = expr.get("op")
            if op in {"+", "SUM", "AVG", "*"}:
                args = [self.compile_expr(arg) for arg in expr.get("args", [])]
                if op == "+":
                    return f"expr_add({', '.join(args)})" if args else "expr_add()"
                if op == "SUM":
                    return f"safe_sum({', '.join(args)})" if args else "safe_sum()"
                if op == "AVG":
                    return f"safe_avg({', '.join(args)})" if args else "safe_avg()"
                return f"expr_mul({', '.join(args)})" if args else "expr_mul()"

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
        if ref.id in self._missing_refs:
            return "None"
        item = _quote(ref.id)
        if ref.t:
            return f"val(r, {item}, p, t={int(ref.t)})"
        return f"val(r, {item}, p)"


def generate_python(
    model: FinancialModel,
    output_path: Optional[str] = None,
) -> str:
    """Generate a standalone Python model module from a FinancialModel."""

    model.build_index()
    graph = DependencyGraph()
    graph.build(model)

    periods = _time_order(model)
    historical_periods = _historical_periods(model)
    projection_periods = _projection_periods(model)
    period_mode = model.time_structure.period_mode or PERIOD_MODE_YEARLY

    compiler = ExprCompiler(graph.missing_refs)
    emitter = CodeEmitter()

    _emit_header(emitter, model)
    _emit_time_axis(emitter, periods, historical_periods, projection_periods, period_mode)

    input_cached, all_cached, cached_computed = _build_cached_dicts(model)
    _emit_cached_values(emitter, input_cached, all_cached, cached_computed)

    _emit_helpers(emitter)

    non_derived_ids = [item.id for _sheet, _section, item in _iter_items(model) if item.item_type != ItemType.derived]
    _emit_assumptions(emitter, model)
    _emit_compute_metadata(emitter, model, graph, non_derived_ids, periods)

    fn_by_item = _build_function_names(model)
    _emit_item_functions(emitter, model, fn_by_item, compiler)
    _emit_compute_function_map(emitter, fn_by_item)
    emitter.line("_DEFAULT_ASSUMPTIONS = default_assumptions()")
    emitter.line("_PROPAGATE: set = set()")
    emitter.blank()
    emitter.line("def _downstream_of(start_ids):")
    with emitter.indent():
        emitter.line("visited = set()")
        emitter.line("stack = list(start_ids)")
        emitter.line("while stack:")
        with emitter.indent():
            emitter.line("node = stack.pop()")
            emitter.line("if node in visited:")
            with emitter.indent():
                emitter.line("continue")
            emitter.line("visited.add(node)")
            emitter.line("for dep in _ADJ.get(node, []):")
            with emitter.indent():
                emitter.line("if dep not in visited:")
                with emitter.indent():
                    emitter.line("stack.append(dep)")
        emitter.line("return visited")
    emitter.blank()

    _emit_compute(emitter)
    _emit_entry_point(emitter)

    source = emitter.to_string()
    if output_path:
        Path(output_path).write_text(source, encoding="utf-8")
    return source


def _emit_header(emitter: CodeEmitter, model: FinancialModel) -> None:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    emitter.line('"""Standalone generated financial model."""')
    emitter.blank()
    emitter.comment(f"Model: {model.company.ticker} - {model.company.name}")
    emitter.comment(f"Generated at: {generated_at}")
    emitter.comment("Usage: python generated_model.py --json | --csv output.csv --items item_a item_b")
    emitter.blank()
    emitter.line("from __future__ import annotations")
    emitter.blank()
    emitter.line("import argparse")
    emitter.line("import csv")
    emitter.line("import json")
    emitter.line("import math")
    emitter.line("from typing import Callable, Dict, List, Optional")
    emitter.blank()


def _emit_time_axis(
    emitter: CodeEmitter,
    periods: List[int],
    historical_periods: List[int],
    projection_periods: List[int],
    period_mode: str,
) -> None:
    emitter.comment("Time axis")
    _emit_assignment(emitter, "PERIOD_MODE", period_mode)
    _emit_assignment(emitter, "PERIODS", periods)
    _emit_assignment(emitter, "HISTORICAL_PERIODS", historical_periods)
    _emit_assignment(emitter, "PROJECTION_PERIODS", projection_periods)
    _emit_assignment(emitter, "_PERIOD_INDEX", {period: i for i, period in enumerate(periods)})
    emitter.line("_HISTORICAL_SET = set(HISTORICAL_PERIODS)")
    emitter.line("_PROJECTION_SET = set(PROJECTION_PERIODS)")
    emitter.blank()


def _emit_cached_values(
    emitter: CodeEmitter,
    input_cached: Dict[str, Dict[int, float]],
    all_cached: Dict[str, Dict[int, float]],
    cached_computed: Dict[str, Dict[int, float]],
) -> None:
    emitter.comment("Cached values")
    _emit_assignment(emitter, "INPUT_CACHED", input_cached)
    _emit_assignment(emitter, "ALL_CACHED", all_cached)
    _emit_assignment(emitter, "_CACHED_COMPUTED", cached_computed)
    emitter.blank()


def _emit_helpers(emitter: CodeEmitter) -> None:
    emitter.comment("Helpers")

    emitter.line("def _bootstrap_period(period: int, t: int) -> Optional[int]:")
    with emitter.indent():
        emitter.line("if PERIOD_MODE == 'yearly':")
        with emitter.indent():
            emitter.line("return period + t")
        emitter.line("if PERIOD_MODE != 'quarterly5':")
        with emitter.indent():
            emitter.line("raise ValueError(f'Unknown period mode: {PERIOD_MODE}')")
        emitter.line("if t == 0:")
        with emitter.indent():
            emitter.line("return period")
        emitter.line("year = period // 10")
        emitter.line("slot = period % 10")
        emitter.line("if slot < 1 or slot > 5:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("index = year * 5 + (slot - 1)")
        emitter.line("shifted = index + t")
        emitter.line("if shifted < 0:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("shifted_year = shifted // 5")
        emitter.line("shifted_slot = shifted % 5 + 1")
        emitter.line("return shifted_year * 10 + shifted_slot")
    emitter.blank()

    emitter.line("def val(r: Dict[str, Dict[int, Optional[float]]], item_id: str, period: int, t: int = 0) -> Optional[float]:")
    with emitter.indent():
        emitter.line("idx = _PERIOD_INDEX.get(period)")
        emitter.line("if idx is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if t == 0:")
        with emitter.indent():
            emitter.line("v = r.get(item_id, {}).get(period)")
            emitter.line("if v is not None:")
            with emitter.indent():
                emitter.line("return v")
            emitter.line("return INPUT_CACHED.get(item_id, {}).get(period)")
        emitter.line("target_idx = idx + t")
        emitter.line("if 0 <= target_idx < len(PERIODS):")
        with emitter.indent():
            emitter.line("target_period = PERIODS[target_idx]")
            emitter.line("v = r.get(item_id, {}).get(target_period)")
            emitter.line("if v is not None:")
            with emitter.indent():
                emitter.line("return v")
            emitter.line("return INPUT_CACHED.get(item_id, {}).get(target_period)")
        emitter.line("if target_idx < 0:")
        with emitter.indent():
            emitter.line("target_period = _bootstrap_period(period, t)")
            emitter.line("if target_period is None:")
            with emitter.indent():
                emitter.line("return None")
            emitter.line("return INPUT_CACHED.get(item_id, {}).get(target_period)")
        emitter.line("return None")
    emitter.blank()

    emitter.line("def safe_sum(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("non_none = [v for v in values if v is not None]")
        emitter.line("if not non_none:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return sum(non_none)")
    emitter.blank()

    emitter.line("def safe_avg(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("non_none = [v for v in values if v is not None]")
        emitter.line("if not non_none:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return sum(non_none) / len(non_none)")
    emitter.blank()

    emitter.line("def safe_items(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return sum(values)")
    emitter.blank()

    emitter.line("def expr_add(*values: Optional[float]) -> float:")
    with emitter.indent():
        emitter.line("return sum(v or 0 for v in values)")
    emitter.blank()

    emitter.line("def expr_mul(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = 1.0")
        emitter.line("for value in values:")
        with emitter.indent():
            emitter.line("result *= value")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def safe_sub(left: Optional[float], right: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if left is None or right is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return left - right")
    emitter.blank()

    emitter.line("def safe_chain_sub(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if not values:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return values[0] - sum(values[1:])")
    emitter.blank()

    emitter.line("def safe_mul(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if not values:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = 1.0")
        emitter.line("for value in values:")
        with emitter.indent():
            emitter.line("result *= value")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def safe_div(left: Optional[float], right: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if left is None or right is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if right == 0:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return left / right")
    emitter.blank()

    emitter.line("def safe_chain_div(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if not values:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = values[0]")
        emitter.line("for value in values[1:]:")
        with emitter.indent():
            emitter.line("if value == 0:")
            with emitter.indent():
                emitter.line("return None")
            emitter.line("result /= value")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def _negate(value: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("return None if value is None else -value")
    emitter.blank()

    emitter.line("def _adjust(value: Optional[float], adjustment: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if value is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if adjustment is None:")
        with emitter.indent():
            emitter.line("return value")
        emitter.line("return value + adjustment")
    emitter.blank()

    emitter.line("def _apply_scale_fn(value: float, scale_fn: str) -> float:")
    with emitter.indent():
        emitter.line("scale_fn = scale_fn.strip()")
        emitter.line("if not scale_fn:")
        with emitter.indent():
            emitter.line("return value")
        emitter.line("if scale_fn.startswith('/'):")
        with emitter.indent():
            emitter.line("return value / float(scale_fn[1:])")
        emitter.line("if scale_fn.startswith('*'):")
        with emitter.indent():
            emitter.line("return value * float(scale_fn[1:])")
        emitter.line("return value")
    emitter.blank()

    emitter.line(
        "def _driver(base: Optional[float], rate: Optional[float], scale: Optional[float] = None, scale_fn: Optional[str] = None) -> Optional[float]:"
    )
    with emitter.indent():
        emitter.line("if base is None or rate is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = base * rate")
        emitter.line("if scale:")
        with emitter.indent():
            emitter.line("result /= float(scale)")
        emitter.line("if isinstance(scale_fn, str):")
        with emitter.indent():
            emitter.line("result = _apply_scale_fn(result, scale_fn)")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def _ratio_sub1(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("value = safe_div(numerator, denominator)")
        emitter.line("if value is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return value - 1")
    emitter.blank()

    emitter.line("def _growth(base: Optional[float], rate: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if base is None or rate is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return base * (1 + rate)")
    emitter.blank()

    emitter.line(
        "def _roll_fwd(beginning: Optional[float], additions: List[Optional[float]], subtractions: List[Optional[float]]) -> Optional[float]:"
    )
    with emitter.indent():
        emitter.line("if beginning is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return beginning + sum(v or 0 for v in additions) - sum(v or 0 for v in subtractions)")
    emitter.blank()

    emitter.line("def _pow(left: Optional[float], right: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if left is None or right is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("try:")
        with emitter.indent():
            emitter.line("return left ** right")
        emitter.line("except (ValueError, OverflowError):")
        with emitter.indent():
            emitter.line("return None")
    emitter.blank()


def _emit_assumptions(emitter: CodeEmitter, model: FinancialModel) -> None:
    emitter.comment("Assumptions")
    emitter.line("def default_assumptions() -> Dict[str, Dict[int, float]]:")
    with emitter.indent():
        emitter.line("assumptions: Dict[str, Dict[int, float]] = {}")
        for sheet_name, sheet in model.sheets.items():
            emitter.comment(f"Sheet: {sheet_name}")
            for section in sheet.sections:
                emitter.comment(f"Section: {section.label}")
                for item in section.line_items:
                    if item.item_type != ItemType.input:
                        continue
                    values = _value_dict_from_item(item)
                    emitter.comment(f"{item.label}")
                    _emit_inline_assignment(emitter, f"assumptions[{_quote(item.id)}]", values)
        emitter.line("return assumptions")
    emitter.blank()


def _emit_compute_metadata(
    emitter: CodeEmitter,
    model: FinancialModel,
    graph: DependencyGraph,
    non_derived_ids: List[str],
    periods: List[int],
) -> None:
    emitter.comment("Compute metadata")

    raw_plans: Dict[int, List[Tuple[str, object]]] = {}
    for period in periods:
        components, order = graph._components_for_period(period)
        plan: List[Tuple[str, object]] = []
        for comp_id in order:
            comp = components[comp_id]
            if comp.is_cycle:
                plan.append(("cycle", list(comp.nodes)))
            else:
                plan.append(("node", comp.nodes[0]))
        raw_plans[period] = plan

    plan_key_to_index: Dict[str, int] = {}
    unique_plans: List[List[Tuple[str, object]]] = []
    period_to_plan: Dict[int, int] = {}
    for period in periods:
        key = repr(raw_plans[period])
        if key not in plan_key_to_index:
            plan_key_to_index[key] = len(unique_plans)
            unique_plans.append(raw_plans[period])
        period_to_plan[period] = plan_key_to_index[key]

    hist_type: Dict[str, str] = {}
    proj_type: Dict[str, str] = {}
    override_type: Dict[str, Dict[int, str]] = {}
    formula_periods: Dict[str, Set[int]] = {}
    non_constant_override_periods: Dict[str, Set[int]] = {}
    item_deps_hist: Dict[str, List[Tuple[str, int]]] = {}
    item_deps_proj: Dict[str, List[Tuple[str, int]]] = {}
    item_deps_override: Dict[str, Dict[int, List[Tuple[str, int]]]] = {}
    adj_dict = {item_id: sorted(deps) for item_id, deps in graph.adj.items() if deps}
    has_non_constant_projected = sorted(
        item_id
        for item_id, item in model._index.items()
        if item.projected is not None and item.projected.type != FormulaType.constant
    )

    for item_id, item in model._index.items():
        if item.historical:
            hist_type[item_id] = item.historical.type.value
        if item.projected:
            proj_type[item_id] = item.projected.type.value
        if item.formula_periods is not None:
            formula_periods[item_id] = set(item.formula_periods)
        if item.item_type == ItemType.derived:
            if item.historical:
                hist_refs = sorted({(ref.id, int(ref.t)) for ref in graph._extract_refs(item.historical.params)})
                if hist_refs:
                    item_deps_hist[item_id] = hist_refs
            if item.projected:
                proj_refs = sorted({(ref.id, int(ref.t)) for ref in graph._extract_refs(item.projected.params)})
                if proj_refs:
                    item_deps_proj[item_id] = proj_refs
            if item.overrides:
                for period, spec in item.overrides.items():
                    if spec.type == FormulaType.constant:
                        continue
                    refs = sorted({(ref.id, int(ref.t)) for ref in graph._extract_refs(spec.params)})
                    if refs:
                        item_deps_override.setdefault(item_id, {})[int(period)] = refs
        if item.overrides:
            otypes: Dict[int, str] = {}
            non_constant: Set[int] = set()
            for period, spec in item.overrides.items():
                otypes[int(period)] = spec.type.value
                if spec.type != FormulaType.constant:
                    non_constant.add(int(period))
            override_type[item_id] = otypes
            if non_constant:
                non_constant_override_periods[item_id] = non_constant

    _emit_assignment(emitter, "_NON_DERIVED_IDS", non_derived_ids)
    for i, plan in enumerate(unique_plans):
        _emit_assignment(emitter, f"_PLAN_{i}", plan)
    emitter.line(f"_PLANS = [{', '.join(f'_PLAN_{i}' for i in range(len(unique_plans)))}]")
    _emit_assignment(emitter, "_PERIOD_TO_PLAN_INDEX", period_to_plan)
    _emit_assignment(emitter, "_HIST_SPEC_TYPE", hist_type)
    _emit_assignment(emitter, "_PROJ_SPEC_TYPE", proj_type)
    _emit_assignment(emitter, "_OVERRIDE_SPEC_TYPE", override_type)
    _emit_assignment(emitter, "_FORMULA_PERIODS", formula_periods)
    _emit_assignment(emitter, "_NON_CONSTANT_OVERRIDE_PERIODS", non_constant_override_periods)
    _emit_assignment(emitter, "_ITEM_DEPS_HIST", item_deps_hist)
    _emit_assignment(emitter, "_ITEM_DEPS_PROJ", item_deps_proj)
    _emit_assignment(emitter, "_ITEM_DEPS_OVERRIDE", item_deps_override)
    _emit_assignment(emitter, "_ADJ", adj_dict)
    _emit_assignment(emitter, "_HAS_NON_CONSTANT_PROJECTED", has_non_constant_projected)
    emitter.line("_HAS_NON_CONSTANT_PROJECTED = set(_HAS_NON_CONSTANT_PROJECTED)")

    emitter.line("def _seed_inputs(period: int, assumptions: Dict[str, Dict[int, float]], r: Dict[str, Dict[int, Optional[float]]]) -> None:")
    with emitter.indent():
        emitter.line("for item_id, by_period in assumptions.items():")
        with emitter.indent():
            emitter.line("if period in by_period:")
            with emitter.indent():
                emitter.line("r.setdefault(item_id, {})[period] = by_period[period]")
        emitter.line("for item_id in _NON_DERIVED_IDS:")
        with emitter.indent():
            emitter.line("if r.get(item_id, {}).get(period) is not None:")
            with emitter.indent():
                emitter.line("continue")
            emitter.line("cached = ALL_CACHED.get(item_id, {}).get(period)")
            emitter.line("if cached is not None:")
            with emitter.indent():
                emitter.line("r.setdefault(item_id, {})[period] = cached")
    emitter.blank()

    emitter.line("def _spec_type_for_period(item_id: str, period: int) -> Optional[str]:")
    with emitter.indent():
        emitter.line("override = _OVERRIDE_SPEC_TYPE.get(item_id)")
        emitter.line("if override and period in override:")
        with emitter.indent():
            emitter.line("return override[period]")
        emitter.line("periods = _FORMULA_PERIODS.get(item_id)")
        emitter.line("if periods is not None and period not in periods:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("hist = _HIST_SPEC_TYPE.get(item_id)")
        emitter.line("proj = _PROJ_SPEC_TYPE.get(item_id)")
        emitter.line("if period in _HISTORICAL_SET:")
        with emitter.indent():
            emitter.line("return hist")
        emitter.line("if period in _PROJECTION_SET:")
        with emitter.indent():
            emitter.line("return proj")
        emitter.line("return proj or hist")
    emitter.blank()

    emitter.line(
        "def _cached_value_for_period(item_id: str, period: int, r: Dict[str, Dict[int, Optional[float]]]) -> Optional[float]:"
    )
    with emitter.indent():
        emitter.line("cached = ALL_CACHED.get(item_id, {}).get(period)")
        emitter.line("if cached is not None:")
        with emitter.indent():
            emitter.line("return cached")
        emitter.line("return r.get(item_id, {}).get(period)")
    emitter.blank()

    emitter.line("def _has_missing_dep(item_id: str, p: int, r: Dict[str, Dict[int, Optional[float]]]) -> bool:")
    with emitter.indent():
        emitter.line("override_type = _OVERRIDE_SPEC_TYPE.get(item_id, {}).get(p)")
        emitter.line("if override_type is not None:")
        with emitter.indent():
            emitter.line("if override_type == 'constant':")
            with emitter.indent():
                emitter.line("return False")
            emitter.line("deps = _ITEM_DEPS_OVERRIDE.get(item_id, {}).get(p, [])")
            emitter.line("return any(val(r, dep_id, p, t=t) is None for dep_id, t in deps)")
        emitter.line("fp = _FORMULA_PERIODS.get(item_id)")
        emitter.line("if fp is not None and p not in fp:")
        with emitter.indent():
            emitter.line("return False")
        emitter.line("if p in _HISTORICAL_SET:")
        with emitter.indent():
            emitter.line("deps = _ITEM_DEPS_HIST.get(item_id, [])")
        emitter.line("elif p in _PROJECTION_SET:")
        with emitter.indent():
            emitter.line("deps = _ITEM_DEPS_PROJ.get(item_id, [])")
        emitter.line("else:")
        with emitter.indent():
            emitter.line("deps = _ITEM_DEPS_PROJ.get(item_id, _ITEM_DEPS_HIST.get(item_id, []))")
        emitter.line("return any(val(r, dep_id, p, t=t) is None for dep_id, t in deps)")
    emitter.blank()

    emitter.line(
        "def _eval_singleton(item_id: str, p: int, r: Dict[str, Dict[int, Optional[float]]], fn: Callable[[Dict[str, Dict[int, Optional[float]]], int], None]) -> bool:"
    )
    with emitter.indent():
        emitter.line("fn(r, p)")
        emitter.line("v = r.get(item_id, {}).get(p)")
        emitter.line("is_invalid = v is None or (isinstance(v, float) and not math.isfinite(v))")
        emitter.line("missing_dep = _has_missing_dep(item_id, p, r)")
        emitter.line("if missing_dep or is_invalid:")
        with emitter.indent():
            emitter.line("if item_id not in _PROPAGATE:")
            with emitter.indent():
                emitter.line("cc = _CACHED_COMPUTED.get(item_id, {}).get(p)")
                emitter.line("if cc is not None:")
                with emitter.indent():
                    emitter.line("r.setdefault(item_id, {})[p] = cc")
                    emitter.line("return r.get(item_id, {}).get(p) is not None")
        emitter.line("return v is not None")
    emitter.blank()

    emitter.line(
        "def _converged(prev: Dict[str, Optional[float]], nodes: List[str], period: int, r: Dict[str, Dict[int, Optional[float]]], tol: float) -> (bool, Optional[float]):"
    )
    with emitter.indent():
        emitter.line("max_residual = 0.0")
        emitter.line("for node in nodes:")
        with emitter.indent():
            emitter.line("current = r.get(node, {}).get(period)")
            emitter.line("prior = prev.get(node)")
            emitter.line("if prior is None or current is None:")
            with emitter.indent():
                emitter.line("return False, None")
            emitter.line("residual = abs(current - prior)")
            emitter.line("if residual > max_residual:")
            with emitter.indent():
                emitter.line("max_residual = residual")
            emitter.line("if residual > tol:")
            with emitter.indent():
                emitter.line("return False, max_residual")
        emitter.line("return True, max_residual")
    emitter.blank()

    emitter.line("def _solve_cycle_block(nodes: List[str], p: int, r: Dict[str, Dict[int, Optional[float]]], assumptions: Optional[Dict[str, Dict[int, float]]] = None) -> None:")
    with emitter.indent():
        emitter.line("frozen_nodes = set()")
        emitter.line("for node in nodes:")
        with emitter.indent():
            emitter.line("# If user provided an explicit override, keep it and freeze.")
            emitter.line("if assumptions and node in assumptions and p in assumptions[node]:")
            with emitter.indent():
                emitter.line("frozen_nodes.add(node)")
                emitter.line("continue")
            emitter.line("spec_type = _spec_type_for_period(node, p)")
            emitter.line("if spec_type == 'constant':")
            with emitter.indent():
                emitter.line("is_override_constant = _OVERRIDE_SPEC_TYPE.get(node, {}).get(p) == 'constant'")
                emitter.line(
                    "if not (is_override_constant and node in _PROPAGATE and node in _HAS_NON_CONSTANT_PROJECTED):"
                )
                with emitter.indent():
                    emitter.line("fn = _COMPUTE_FUNCS.get(node)")
                    emitter.line("if fn is not None:")
                    with emitter.indent():
                        emitter.line("fn(r, p)")
                    emitter.line("frozen_nodes.add(node)")
                    emitter.line("continue")
            emitter.line("if spec_type is None and r.get(node, {}).get(p) is not None:")
            with emitter.indent():
                emitter.line("frozen_nodes.add(node)")
        emitter.line("active_nodes = [node for node in nodes if node not in frozen_nodes]")
        emitter.line("for node in active_nodes:")
        with emitter.indent():
            emitter.line("if r.get(node, {}).get(p) is None:")
            with emitter.indent():
                emitter.line("cached = _cached_value_for_period(node, p, r)")
                emitter.line("r.setdefault(node, {})[p] = cached if cached is not None else 0.0")
        emitter.line("if not active_nodes:")
        with emitter.indent():
            emitter.line("return")
        emitter.line("converged = False")
        emitter.line("for _ in range(100):")
        with emitter.indent():
            emitter.line("prev = {node: r.get(node, {}).get(p) for node in active_nodes}")
            emitter.line("for node in active_nodes:")
            with emitter.indent():
                emitter.line("fn = _COMPUTE_FUNCS.get(node)")
                emitter.line("if fn is None:")
                with emitter.indent():
                    emitter.line("r.setdefault(node, {})[p] = None")
                    emitter.line("continue")
                emitter.line("fn(r, p)")
            emitter.line("converged, _residual = _converged(prev, active_nodes, p, r, 1e-6)")
            emitter.line("if converged:")
            with emitter.indent():
                emitter.line("break")
        emitter.line("pathological = all(r.get(node, {}).get(p) is None for node in active_nodes)")
        emitter.line("if not converged or pathological:")
        with emitter.indent():
            emitter.line("for node in active_nodes:")
            with emitter.indent():
                emitter.line("if node in _PROPAGATE:")
                with emitter.indent():
                    emitter.line("continue")
                emitter.line("cached = _cached_value_for_period(node, p, r)")
                emitter.line("if cached is not None:")
                with emitter.indent():
                    emitter.line("r.setdefault(node, {})[p] = cached")
    emitter.blank()


def _emit_item_functions(
    emitter: CodeEmitter,
    model: FinancialModel,
    fn_by_item: Dict[str, str],
    compiler: ExprCompiler,
) -> None:
    emitter.comment("Per-item compute functions")

    location = _item_locations(model)
    for item_id, item in model._index.items():
        if item.item_type != ItemType.derived:
            continue

        fn_name = fn_by_item[item_id]
        sheet_name, section_label = location.get(item_id, ("", ""))
        emitter.line(f"def {fn_name}(r: Dict[str, Dict[int, Optional[float]]], p: int) -> None:")
        with emitter.indent():
            emitter.line(f'"""{item.label} (Sheet: {sheet_name} / {section_label}, Row {item.row})"""')

            if item.overrides:
                for period in sorted(item.overrides.keys()):
                    override = item.overrides[period]
                    needs_guard = (
                        override.type == FormulaType.constant
                        and item.projected is not None
                        and item.projected.type != FormulaType.constant
                    )
                    emitter.line(f"if p == {int(period)}:")
                    with emitter.indent():
                        if override.type == FormulaType.constant:
                            expr = compiler.compile_formula(override, item_id=item.id)
                            if needs_guard:
                                emitter.line(f"if {_quote(item.id)} not in _PROPAGATE:")
                                with emitter.indent():
                                    emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = {expr}")
                                    emitter.line("return")
                            else:
                                emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = {expr}")
                                emitter.line("return")
                        else:
                            expr = compiler.compile_formula(override, item_id=item.id)
                            emitter.line(f"v = {expr}")
                            emitter.line("if v is not None:")
                            with emitter.indent():
                                emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = v")
                            emitter.line("elif r.get(" + _quote(item.id) + ", {}).get(p) is None:")
                            with emitter.indent():
                                emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = None")
                            emitter.line("return")

            if item.formula_periods is not None:
                _emit_inline_assignment(emitter, "_periods", set(item.formula_periods))
                skippable_override_periods: Set[int] = set()
                if item.overrides and item.projected and item.projected.type != FormulaType.constant:
                    for period, override in item.overrides.items():
                        if override.type == FormulaType.constant:
                            skippable_override_periods.add(int(period))
                if skippable_override_periods:
                    _emit_inline_assignment(emitter, "_skippable", sorted(skippable_override_periods))
                    emitter.line("_skippable = set(_skippable)")
                emitter.line("if p not in _periods:")
                with emitter.indent():
                    if skippable_override_periods:
                        emitter.line(f"if p not in _skippable or {_quote(item.id)} not in _PROPAGATE:")
                        with emitter.indent():
                            emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = None")
                            emitter.line("return")
                    else:
                        emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = None")
                        emitter.line("return")

            historical_expr = compiler.compile_formula(item.historical, item_id=item.id)
            projected_expr = compiler.compile_formula(item.projected, item_id=item.id)
            fallback_expr = projected_expr if item.projected is not None else historical_expr

            emitter.line("if p in _HISTORICAL_SET:")
            with emitter.indent():
                emitter.line(f"v = {historical_expr}")
            emitter.line("elif p in _PROJECTION_SET:")
            with emitter.indent():
                emitter.line(f"v = {projected_expr}")
            emitter.line("else:")
            with emitter.indent():
                emitter.line(f"v = {fallback_expr}")
            emitter.line(f"r.setdefault({_quote(item.id)}, {{}})[p] = v")
        emitter.blank()


def _emit_compute_function_map(emitter: CodeEmitter, fn_by_item: Dict[str, str]) -> None:
    mapping = {item_id: fn_by_item[item_id] for item_id in sorted(fn_by_item.keys())}
    emitter.line("_COMPUTE_FUNCS: Dict[str, Callable[[Dict[str, Dict[int, Optional[float]]], int], None]] = {")
    with emitter.indent():
        for item_id, fn_name in mapping.items():
            emitter.line(f"{_quote(item_id)}: {fn_name},")
    emitter.line("}")
    emitter.blank()


def _emit_compute(emitter: CodeEmitter) -> None:
    emitter.comment("Compute")
    emitter.line("def compute(assumptions: Optional[Dict[str, Dict[int, float]]] = None) -> Dict[str, Dict[int, Optional[float]]]:")
    with emitter.indent():
        emitter.line("global _PROPAGATE")
        emitter.line("assumptions = default_assumptions() if assumptions is None else assumptions")
        emitter.line("overridden = set()")
        emitter.line("for item_id, by_period in assumptions.items():")
        with emitter.indent():
            emitter.line("default_periods = _DEFAULT_ASSUMPTIONS.get(item_id, {})")
            emitter.line("for p_key, v in by_period.items():")
            with emitter.indent():
                emitter.line("if default_periods.get(p_key) != v:")
                with emitter.indent():
                    emitter.line("overridden.add(item_id)")
                    emitter.line("break")
        emitter.line("_PROPAGATE = _downstream_of(overridden) if overridden else set()")
        emitter.line("try:")
        with emitter.indent():
            emitter.line("r: Dict[str, Dict[int, Optional[float]]] = {}")
            emitter.line("for p in PERIODS:")
            with emitter.indent():
                emitter.line("_seed_inputs(p, assumptions, r)")
                emitter.line("plan = _PLANS[_PERIOD_TO_PLAN_INDEX[p]]")
                emitter.line("for kind, payload in plan:")
                with emitter.indent():
                    emitter.line("if kind == 'cycle':")
                    with emitter.indent():
                        emitter.line("_solve_cycle_block(payload, p, r, assumptions)")
                        emitter.line("continue")
                    emitter.line("item_id = payload")
                    emitter.line("if r.get(item_id, {}).get(p) is not None:")
                    with emitter.indent():
                        emitter.line("continue")
                    emitter.line("fn = _COMPUTE_FUNCS.get(item_id)")
                    emitter.line("if fn is None:")
                    with emitter.indent():
                        emitter.line("continue")
                    emitter.line("_eval_singleton(item_id, p, r, fn)")
                emitter.line("singleton_ids = [payload for kind, payload in plan if kind == 'node']")
                emitter.line("for _sweep in range(len(singleton_ids)):")
                with emitter.indent():
                    emitter.line("unresolved = 0")
                    emitter.line("progressed = False")
                    emitter.line("for item_id in singleton_ids:")
                    with emitter.indent():
                        emitter.line("if r.get(item_id, {}).get(p) is not None:")
                        with emitter.indent():
                            emitter.line("continue")
                        emitter.line("unresolved += 1")
                        emitter.line("fn = _COMPUTE_FUNCS.get(item_id)")
                        emitter.line("if fn is not None:")
                        with emitter.indent():
                            emitter.line("if _eval_singleton(item_id, p, r, fn):")
                            with emitter.indent():
                                emitter.line("progressed = True")
                    emitter.line("if unresolved == 0 or not progressed:")
                    with emitter.indent():
                        emitter.line("break")
            emitter.line("for p in PERIODS:")
            with emitter.indent():
                emitter.line("for item_id, periods in _NON_CONSTANT_OVERRIDE_PERIODS.items():")
                with emitter.indent():
                    emitter.line("if p not in periods:")
                    with emitter.indent():
                        emitter.line("continue")
                    emitter.line("fn = _COMPUTE_FUNCS.get(item_id)")
                    emitter.line("if fn is None:")
                    with emitter.indent():
                        emitter.line("continue")
                    emitter.line("fn(r, p)")
        emitter.line("finally:")
        with emitter.indent():
            emitter.line("_PROPAGATE = set()")
        emitter.line("return r")
    emitter.blank()


def _emit_entry_point(emitter: CodeEmitter) -> None:
    emitter.comment("Entry point")

    emitter.line("def _filter_items(results: Dict[str, Dict[int, Optional[float]]], item_ids: Optional[List[str]]) -> Dict[str, Dict[int, Optional[float]]]:")
    with emitter.indent():
        emitter.line("if not item_ids:")
        with emitter.indent():
            emitter.line("return results")
        emitter.line("return {item_id: results.get(item_id, {}) for item_id in item_ids}")
    emitter.blank()

    emitter.line("def _write_csv(path: str, results: Dict[str, Dict[int, Optional[float]]]) -> None:")
    with emitter.indent():
        emitter.line("with open(path, 'w', newline='', encoding='utf-8') as f:")
        with emitter.indent():
            emitter.line("writer = csv.writer(f)")
            emitter.line("writer.writerow(['item_id'] + [str(period) for period in PERIODS])")
            emitter.line("for item_id in sorted(results.keys()):")
            with emitter.indent():
                emitter.line("row = [item_id] + [results.get(item_id, {}).get(period) for period in PERIODS]")
                emitter.line("writer.writerow(row)")
    emitter.blank()

    emitter.line("def main() -> None:")
    with emitter.indent():
        emitter.line("parser = argparse.ArgumentParser(description='Run generated financial model.')")
        emitter.line("parser.add_argument('--json', action='store_true', help='Print JSON output.')")
        emitter.line("parser.add_argument('--csv', type=str, help='Write CSV output to the given path.')")
        emitter.line("parser.add_argument('--items', nargs='*', help='Limit output to listed item IDs.')")
        emitter.line("args = parser.parse_args()")
        emitter.line("results = compute(default_assumptions())")
        emitter.line("results = _filter_items(results, args.items)")
        emitter.line("if args.csv:")
        with emitter.indent():
            emitter.line("_write_csv(args.csv, results)")
        emitter.line("if args.json or not args.csv:")
        with emitter.indent():
            emitter.line("print(json.dumps(results, sort_keys=True, indent=2))")
    emitter.blank()

    emitter.line("if __name__ == '__main__':")
    with emitter.indent():
        emitter.line("main()")


def _time_order(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    if ts.historical_periods or ts.projection_periods:
        return list(ts.historical_periods) + list(ts.projection_periods)
    return list(ts.historical_years) + list(ts.projection_years)


def _historical_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.historical_periods) or list(ts.historical_years)


def _projection_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.projection_periods) or list(ts.projection_years)


def _iter_items(model: FinancialModel) -> Iterable[Tuple[str, str, LineItem]]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, section.label, item


def _build_cached_dicts(
    model: FinancialModel,
) -> Tuple[Dict[str, Dict[int, float]], Dict[str, Dict[int, float]], Dict[str, Dict[int, float]]]:
    input_cached: Dict[str, Dict[int, float]] = {}
    all_cached: Dict[str, Dict[int, float]] = {}
    cached_computed: Dict[str, Dict[int, float]] = {}
    period_mode = model.time_structure.period_mode or PERIOD_MODE_YEARLY

    for _sheet, _section, item in _iter_items(model):
        if not item.values:
            continue
        for period, value_cell in item.values.values.items():
            if value_cell.value is None:
                continue
            all_cached.setdefault(item.id, {})[int(period)] = float(value_cell.value)
            if value_cell.provenance in _INPUT_PROVENANCE:
                input_cached.setdefault(item.id, {})[int(period)] = float(value_cell.value)
            if value_cell.provenance == ValueProvenance.computed:
                cached_computed.setdefault(item.id, {})[int(period)] = float(value_cell.value)
            elif period_mode != PERIOD_MODE_YEARLY and value_cell.provenance in {
                ValueProvenance.imported_other,
                ValueProvenance.imported_edgar,
                ValueProvenance.imported_fmp,
            }:
                cached_computed.setdefault(item.id, {})[int(period)] = float(value_cell.value)

    return input_cached, all_cached, cached_computed


def _build_function_names(model: FinancialModel) -> Dict[str, str]:
    used: Dict[str, int] = {}
    names: Dict[str, str] = {}

    for _sheet, _section, item in _iter_items(model):
        if item.item_type != ItemType.derived:
            continue
        base = re.sub(r"[^A-Za-z0-9_]", "_", item.id).strip("_")
        if not base:
            base = "item"
        if base[0].isdigit():
            base = f"item_{base}"
        base = base.lower()
        count = used.get(base, 0) + 1
        used[base] = count
        if count == 1:
            names[item.id] = f"_compute_{base}"
        else:
            names[item.id] = f"_compute_{base}_{count}"
    return names


def _item_locations(model: FinancialModel) -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                out[item.id] = (sheet_name, section.label)
    return out


def _value_dict_from_item(item: LineItem) -> Dict[int, float]:
    values: Dict[int, float] = {}
    if not item.values:
        return values
    for period, value_cell in item.values.values.items():
        if value_cell.value is None:
            continue
        values[int(period)] = float(value_cell.value)
    return values


def _line_item_ref_from_obj(obj) -> Optional[LineItemRef]:
    if isinstance(obj, LineItemRef):
        return obj
    if isinstance(obj, dict) and "id" in obj:
        try:
            return LineItemRef(id=str(obj["id"]), t=int(obj.get("t", 0)))
        except Exception:
            return None
    return None


def _quote(value) -> str:
    return json.dumps(value)


def _float_literal(value) -> str:
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "1.0" if value else "0.0"
    return repr(float(value))


def _emit_assignment(emitter: CodeEmitter, name: str, value) -> None:
    literal = pformat(value, sort_dicts=True, width=100)
    lines = literal.splitlines()
    if not lines:
        emitter.line(f"{name} = {{}}")
        return
    emitter.line(f"{name} = {lines[0]}")
    for line in lines[1:]:
        emitter.line(line)


def _emit_inline_assignment(emitter: CodeEmitter, lhs: str, value) -> None:
    literal = pformat(value, sort_dicts=True, width=100)
    lines = literal.splitlines()
    if len(lines) == 1:
        emitter.line(f"{lhs} = {lines[0]}")
        return
    emitter.line(f"{lhs} = {lines[0]}")
    for line in lines[1:]:
        emitter.line(line)


__all__ = ["CodeEmitter", "ExprCompiler", "generate_python"]
