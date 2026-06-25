"""Compute metadata emitters for :mod:`schema.codegen`."""

from __future__ import annotations

from pprint import pformat
import sys
from typing import Any, Dict, List, Set, Tuple

from .dependency_graph import DependencyGraph
from .models import FinancialModel, FormulaType, ItemType


def _parent_attr(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.codegen")
    if parent is None:
        parent = sys.modules.get("codegen")
    return getattr(parent, name, default) if parent is not None else default


def _emit_assignment(emitter: Any, name: str, value: Any) -> None:
    literal = pformat(value, sort_dicts=True, width=100)
    lines = literal.splitlines()
    if not lines:
        emitter.line(f"{name} = {{}}")
        return
    emitter.line(f"{name} = {lines[0]}")
    for line in lines[1:]:
        emitter.line(line)


def _emit_compute_metadata(
    emitter: Any,
    model: FinancialModel,
    graph: DependencyGraph,
    non_derived_ids: List[str],
    periods: List[int],
) -> None:
    emit_assignment = _parent_attr("_emit_assignment", _emit_assignment)

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

    emit_assignment(emitter, "_NON_DERIVED_IDS", non_derived_ids)
    for i, plan in enumerate(unique_plans):
        emit_assignment(emitter, f"_PLAN_{i}", plan)
    emitter.line(f"_PLANS = [{', '.join(f'_PLAN_{i}' for i in range(len(unique_plans)))}]")
    emit_assignment(emitter, "_PERIOD_TO_PLAN_INDEX", period_to_plan)
    emit_assignment(emitter, "_HIST_SPEC_TYPE", hist_type)
    emit_assignment(emitter, "_PROJ_SPEC_TYPE", proj_type)
    emit_assignment(emitter, "_OVERRIDE_SPEC_TYPE", override_type)
    emit_assignment(emitter, "_FORMULA_PERIODS", formula_periods)
    emit_assignment(emitter, "_NON_CONSTANT_OVERRIDE_PERIODS", non_constant_override_periods)
    emit_assignment(emitter, "_ITEM_DEPS_HIST", item_deps_hist)
    emit_assignment(emitter, "_ITEM_DEPS_PROJ", item_deps_proj)
    emit_assignment(emitter, "_ITEM_DEPS_OVERRIDE", item_deps_override)
    emit_assignment(emitter, "_ADJ", adj_dict)
    emit_assignment(emitter, "_HAS_NON_CONSTANT_PROJECTED", has_non_constant_projected)
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


__all__ = ["_emit_compute_metadata"]
