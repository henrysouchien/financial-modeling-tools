"""Generate standalone Python compute modules from FinancialModel schema objects."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Optional

from .codegen_expr import (
    CodeEmitter as CodeEmitter,
    ExprCompiler as ExprCompiler,
    _float_literal as _float_literal,
    _quote as _quote,
)
from .codegen_compute_metadata import _emit_compute_metadata
from .codegen_item_functions import _emit_item_functions
from .codegen_preflight import (
    _all_specs_with_paths as _all_specs_with_paths,  # noqa: F401 - compatibility alias
    _scan_formula_spec as _scan_formula_spec,  # noqa: F401 - compatibility alias
    _walk_params_for_unsupported as _walk_params_for_unsupported,  # noqa: F401 - compatibility alias
    codegen_preflight_scan,
)
from .codegen_model_helpers import (
    _INPUT_PROVENANCE as _INPUT_PROVENANCE,  # noqa: F401 - compatibility alias
    _build_cached_dicts,
    _build_function_names,
    _historical_periods,
    _item_locations as _item_locations,  # noqa: F401 - compatibility alias
    _iter_items,
    _projection_periods,
    _time_order,
    _value_dict_from_item,
)
from .codegen_runtime_helpers import _emit_entry_point, _emit_helpers
from .dependency_graph import DependencyGraph
from .models import (
    FinancialModel,
    FormulaType as FormulaType,  # noqa: F401 - compatibility alias
    ItemType,
    PERIOD_MODE_YEARLY,
)


def generate_python(
    model: FinancialModel,
    output_path: Optional[str] = None,
) -> str:
    """Generate a standalone Python model module from a FinancialModel."""

    model.build_index()
    codegen_preflight_scan(model)
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


__all__ = ["CodeEmitter", "ExprCompiler", "codegen_preflight_scan", "generate_python"]
