"""Lightweight agent-facing tools for schema financial models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from difflib import get_close_matches
import math
import re
from typing import Dict, Iterable, List, Literal, Optional, Set, Tuple

from .analysis import _default_period, _downstream_nodes, _upstream_nodes
from .dependency_graph import DependencyGraph
from .models import (
    PERIOD_MODE_QUARTERLY5,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
)
from .reader import read_model


@dataclass
class _ModelBundle:
    model: FinancialModel
    graph: DependencyGraph
    base_results: Dict[str, Dict[int, float]]
    all_items: List[LineItem]
    derived_ids: Set[str]


_cache: Dict[Tuple[str, int], _ModelBundle] = {}

_KEY_METRIC_PATTERNS = [
    "revenue",
    "net_income",
    "free_cash_flow",
    "ebitda",
    "eps",
    "gross_profit",
    "operating_income",
]

_SENSITIVITY_CANDIDATE_FILTERS = {"drivers", "inputs_only", "all"}
_SENSITIVITY_DRIVER_FORMULA_TYPES = {
    FormulaType.ref,
    FormulaType.growth,
    FormulaType.driver,
    FormulaType.valuation,
    FormulaType.constant,
    FormulaType.roll_forward,
    FormulaType.ratio,
}
_SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY = 40
_SCENARIO_DIVERGENCE_ABS_LIMIT = 1e20


def clear_cache() -> None:
    _cache.clear()


def load(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> _ModelBundle:
    cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
    key = (file_path, cutoff)

    if model is None and key in _cache:
        return _cache[key]

    if model is None:
        loaded = read_model(file_path, mode="full", historical_cutoff_year=cutoff)
        if not isinstance(loaded, FinancialModel):
            raise TypeError("read_model(..., mode='full') did not return FinancialModel")
        model = loaded

    if not model._index:
        model.build_index()

    graph = DependencyGraph()
    graph.build(model)

    all_items = list(model._index.values())
    derived_ids = {item.id for item in all_items if item.item_type == ItemType.derived}
    base_results = graph.compute({}, recompute=derived_ids)

    bundle = _ModelBundle(
        model=model,
        graph=graph,
        base_results=base_results,
        all_items=all_items,
        derived_ids=derived_ids,
    )
    _cache[key] = bundle
    return bundle


def summarize(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    model_obj = bundle.model
    all_periods = _all_periods(model_obj)
    default_period = _default_period(model_obj)

    sheets_summary = []
    for sheet in model_obj.sheets.values():
        section_rows = []
        total_items = 0
        for section in sheet.sections:
            count = len(section.line_items)
            total_items += count
            section_rows.append(
                {
                    "id": section.id,
                    "label": section.label,
                    "item_count": count,
                }
            )
        sheets_summary.append(
            {
                "name": sheet.name,
                "item_count": total_items,
                "sections": section_rows,
            }
        )

    key_metrics = []
    for item in _find_key_metrics(bundle.all_items):
        values = bundle.base_results.get(item.id, {})
        proj_annual = _annual_projection_periods(model_obj)
        hist_annual = _annual_historical_periods(model_obj, n=3)

        key_metrics.append(
            {
                "id": item.id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "projection_series": {period: values.get(period) for period in proj_annual},
                "historical_series": {period: values.get(period) for period in hist_annual},
            }
        )

    return {
        "sheets": sheets_summary,
        "line_item_count": len(bundle.all_items),
        "time_range": {
            "historical_periods": _historical_periods(model_obj),
            "projection_periods": _projection_periods(model_obj),
            "all_periods": all_periods,
            "default_period": default_period,
        },
        "key_metrics": key_metrics,
    }


def find(
    file_path: str,
    query: str,
    limit: int = 20,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> List[Dict]:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    if not query:
        return []

    all_periods = _all_periods(bundle.model)
    needle = query.lower()
    item_locs = _item_locations(bundle.model)
    parent_headers = _parent_headers(bundle.model)
    ambiguous = _ambiguous_labels(bundle.all_items)
    rows = []
    for item in bundle.all_items:
        haystack = f"{item.id} {item.label}".lower()
        if needle not in haystack:
            continue
        context = item_locs.get(item.id)
        parent_header = parent_headers.get(item.id)
        context_label = _format_find_context(context, parent_header)
        display_label = item.label
        if _label_key(item.label) in ambiguous:
            if context_label:
                display_label = f"{item.label} ({context_label})"
            else:
                display_label = f"{item.label} ({item.id})"
        rows.append(
            {
                "id": item.id,
                "label": item.label,
                "display_label": display_label,
                "label_context": context_label,
                "parent_header": parent_header,
                "sheet": context[0] if context else None,
                "section": context[1] if context else None,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "sample_values": _sample_values(bundle.base_results.get(item.id, {}), all_periods),
            }
        )

    rows.sort(key=lambda row: (row["id"]))
    return rows[: max(limit, 0)]


def values(
    file_path: str,
    item_ids: List[str],
    periods: str = "all",
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> Dict:
    deduped_item_ids: List[str] = []
    seen: Set[str] = set()
    for item_id in item_ids:
        if item_id in seen:
            continue
        deduped_item_ids.append(item_id)
        seen.add(item_id)

    if len(deduped_item_ids) > 10:
        raise ValueError("item_ids must contain at most 10 unique IDs")

    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    period_list, period_label = _resolve_period_list(periods, bundle.model)

    rows = []
    for item_id in deduped_item_ids:
        try:
            item = bundle.model.get_item(item_id)
        except KeyError:
            suggestions = _suggest_items(bundle.model._index, item_id)
            error_row = {"id": item_id, "error": f"Unknown item_id: {item_id}"}
            if suggestions:
                error_row["suggestions"] = suggestions
            rows.append(error_row)
            continue

        item_values = bundle.base_results.get(item_id, {})
        rows.append(
            {
                "id": item.id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "values": {period: item_values.get(period) for period in period_list},
            }
        )

    return {
        "items": rows,
        "periods_returned": period_label,
        "period_count": len(period_list),
    }


def drivers(
    file_path: str,
    item_id: str,
    depth: int = 3,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    if item_id not in bundle.model._index:
        raise ValueError(_format_unknown_id_error(bundle.model._index, item_id))

    all_periods = _all_periods(bundle.model)
    node_depth: Dict[str, int] = {}
    edge_set: Set[Tuple[str, str, int]] = set()
    stack: List[Tuple[str, int]] = [(item_id, 0)]

    while stack:
        node_id, current_depth = stack.pop()
        if current_depth > depth:
            continue
        known_depth = node_depth.get(node_id)
        if known_depth is not None and known_depth <= current_depth:
            continue
        node_depth[node_id] = current_depth

        if current_depth == depth:
            continue

        for dep_id in bundle.graph.get_dependencies(node_id):
            edge_set.add((dep_id, node_id, 0))
            stack.append((dep_id, current_depth + 1))

        for ref in bundle.graph.time_edges.get(node_id, set()):
            if ref.t == 0:
                continue
            edge_set.add((ref.id, node_id, ref.t))
            stack.append((ref.id, current_depth + 1))

    nodes = []
    for node_id, node_dist in sorted(node_depth.items(), key=lambda x: (x[1], x[0])):
        item = bundle.model.get_item(node_id)
        nodes.append(
            {
                "id": node_id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "distance": node_dist,
                "sample_values": _sample_values(bundle.base_results.get(node_id, {}), all_periods),
            }
        )

    edges = [
        {
            "from": src,
            "to": dst,
            "lag": lag,
        }
        for src, dst, lag in sorted(edge_set)
    ]

    return {
        "item_id": item_id,
        "depth": depth,
        "nodes": nodes,
        "edges": edges,
    }


def sensitivity(
    file_path: str,
    target_id: str,
    n: int = 15,
    bump_pct: float = 0.10,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
    candidate_filter: Literal["drivers", "inputs_only", "all"] = "drivers",
    include_derived: Optional[bool] = None,
    max_candidates: Optional[int] = None,
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    if target_id not in bundle.model._index:
        raise ValueError(_format_unknown_id_error(bundle.model._index, target_id, "target_id"))

    projection_periods = _projection_periods(bundle.model)
    if not projection_periods:
        projection_periods = [_default_period(bundle.model)]
    target_period = projection_periods[-1]
    base_target_value = bundle.base_results.get(target_id, {}).get(target_period)

    candidate_filter = _resolve_candidate_filter(candidate_filter, include_derived)
    upstream = _upstream_nodes(bundle.graph, target_id)
    upstream.discard(target_id)
    upstream = _filter_sensitivity_candidates(bundle.model, upstream, candidate_filter)
    alias_group_by_id = _build_ref_alias_groups(bundle.model)
    item_locations = _item_locations(bundle.model)
    ambiguous_labels = _ambiguous_labels(bundle.all_items)

    candidates = sorted(upstream)
    candidates, precollapsed_aliases = _collapse_alias_candidates(candidates, bundle.model, alias_group_by_id)
    candidate_count_total = len(candidates)
    selected_max_candidates = _resolve_max_candidates(bundle.model, candidate_filter, max_candidates)
    if selected_max_candidates is not None and candidate_count_total > selected_max_candidates:
        candidates = _rank_candidates_for_sensitivity(candidates, bundle.model, bundle.graph, target_id)
        candidates = candidates[:selected_max_candidates]
    candidate_count_evaluated = len(candidates)

    recompute_cache = {candidate_id: _downstream_nodes(bundle.graph, candidate_id) for candidate_id in candidates}
    impacts = []
    for candidate_id in candidates:
        base_candidate_values = bundle.base_results.get(candidate_id, {})
        bumped_values: Dict[int, float] = {}
        for period in projection_periods:
            base_val = base_candidate_values.get(period)
            if base_val is None:
                continue
            bumped_values[period] = base_val * (1.0 + bump_pct)
        if not bumped_values:
            continue

        recompute_ids = recompute_cache[candidate_id]
        scenario_inputs = {candidate_id: bumped_values}
        promoted_projected = _promote_projected_fallbacks(bundle.model, recompute_ids)
        active_periods = {period for period in projection_periods if period >= min(bumped_values)}
        try:
            scenario_results = bundle.graph.compute(
                scenario_inputs,
                recompute=recompute_ids,
                cycle_fallback_policy="auto_propagate",
                periods=active_periods,
                seed_results=bundle.base_results,
            )
        finally:
            for item_id, original_projected in promoted_projected.items():
                bundle.model.get_item(item_id).projected = original_projected
        scenario_target_value = scenario_results.get(target_id, {}).get(target_period)

        delta = None
        pct_change = None
        leverage_ratio = None
        high_leverage = False
        abs_impact = -1.0
        if base_target_value is not None and scenario_target_value is not None:
            delta = scenario_target_value - base_target_value
            abs_impact = abs(delta)
            if base_target_value != 0:
                pct_change = delta / base_target_value
        if pct_change is not None and bump_pct != 0:
            leverage_ratio = abs(pct_change) / abs(bump_pct)
            high_leverage = leverage_ratio > 3.0

        item = bundle.model.get_item(candidate_id)
        context = item_locations.get(candidate_id)
        context_label = _format_context_label(context)
        display_label = item.label
        if _label_key(item.label) in ambiguous_labels:
            if context_label:
                display_label = f"{item.label} ({context_label})"
            else:
                display_label = f"{item.label} ({candidate_id})"
        impacts.append(
            {
                "id": candidate_id,
                "label": item.label,
                "display_label": display_label,
                "label_context": context_label,
                "sheet": context[0] if context else None,
                "section": context[1] if context else None,
                "row": context[2] if context else None,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "base": base_target_value,
                "scenario": scenario_target_value,
                "delta": delta,
                "pct_change": pct_change,
                "leverage_ratio": leverage_ratio,
                "high_leverage": high_leverage,
                "abs_impact": abs_impact,
            }
        )

    impacts = _dedupe_sensitivity_impacts(impacts, bundle.model, alias_group_by_id)
    for row in impacts:
        aliases = set(row.get("alias_ids", []))
        aliases.update(precollapsed_aliases.get(row["id"], []))
        if aliases:
            row["alias_ids"] = sorted(aliases)
    impacts.sort(key=lambda row: row["abs_impact"], reverse=True)
    top_impacts = impacts[: max(n, 0)]
    high_leverage_count = sum(1 for row in top_impacts if row.get("high_leverage"))
    return {
        "target_id": target_id,
        "target_period": target_period,
        "bump_pct": bump_pct,
        "candidate_filter": candidate_filter,
        "candidate_count_total": candidate_count_total,
        "candidate_count_evaluated": candidate_count_evaluated,
        "max_candidates": selected_max_candidates,
        "base_value": base_target_value,
        "high_leverage_count": high_leverage_count,
        "results": top_impacts,
    }


def scenario(
    file_path: str,
    overrides: Dict[str, Dict[int, float]],
    compare_items: Optional[List[str]] = None,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    model_obj = bundle.model

    normalized: Dict[str, Dict[int, float]] = {}
    for item_id, values in (overrides or {}).items():
        if item_id not in model_obj._index:
            raise ValueError(_format_unknown_id_error(model_obj._index, item_id, "override item_id"))
        normalized[item_id] = {int(period): float(value) for period, value in values.items()}

    if compare_items is None:
        compare_ids = [item.id for item in _find_key_metrics(bundle.all_items)]
    else:
        compare_ids = compare_items
        for item_id in compare_ids:
            if item_id not in model_obj._index:
                raise ValueError(_format_unknown_id_error(model_obj._index, item_id, "compare item_id"))

    recompute_ids: Set[str] = set()
    for item_id in normalized:
        recompute_ids |= _downstream_nodes(bundle.graph, item_id)

    scenario_results = _compute_scenario_results(bundle, normalized, recompute_ids)
    period = _default_period(model_obj)
    all_periods = _all_periods(model_obj)

    comparisons = []
    for item_id in compare_ids:
        item = model_obj.get_item(item_id)
        base_val = bundle.base_results.get(item_id, {}).get(period)
        scenario_val = scenario_results.get(item_id, {}).get(period)
        delta = None
        pct_change = None
        if base_val is not None and scenario_val is not None:
            delta = scenario_val - base_val
            if base_val != 0:
                pct_change = delta / base_val
        comparisons.append(
            {
                "id": item_id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "base": base_val,
                "scenario": scenario_val,
                "delta": delta,
                "pct_change": pct_change,
                "base_sample_values": _sample_values(bundle.base_results.get(item_id, {}), all_periods),
                "scenario_sample_values": _sample_values(scenario_results.get(item_id, {}), all_periods),
            }
        )

    return {
        "period": period,
        "overrides": normalized,
        "comparisons": comparisons,
    }


def _find_key_metrics(items: Iterable[LineItem]) -> List[LineItem]:
    by_id = list(items)
    selected: List[LineItem] = []
    taken: Set[str] = set()
    for pattern in _KEY_METRIC_PATTERNS:
        matches = []
        for item in by_id:
            if item.id in taken:
                continue
            item_id = item.id.lower()
            label = item.label.lower()
            haystack = f"{item_id} {label}"
            if pattern not in haystack:
                continue
            id_leaf = item_id.split(".")[-1]
            label_norm = label.replace(" ", "_")
            is_exact = id_leaf == pattern or label_norm == pattern
            is_total = not is_exact and (
                id_leaf in (f"total_{pattern}", f"total_{pattern}s")
                or label_norm in (f"total_{pattern}", f"total_{pattern}s")
            )
            is_plural = not is_exact and not is_total and (
                id_leaf == f"{pattern}s" or label_norm == f"{pattern}s"
            )
            is_prefixed_base = id_leaf.startswith(f"base_{pattern}") or label.startswith(f"base {pattern}")
            tier = 0 if is_exact else (1 if is_total else (2 if is_plural else 3))
            first_idx = haystack.index(pattern)
            is_header_penalty = 1 if item.item_type == ItemType.header else 0
            matches.append(
                (
                    is_header_penalty,
                    tier,
                    1 if is_prefixed_base else 0,
                    first_idx,
                    item.id,
                    item,
                )
            )

        if matches:
            matches.sort(key=lambda row: (row[0], row[1], row[2], row[3], row[4]))
            winner = matches[0][5]
            selected.append(winner)
            taken.add(winner.id)
    return selected


def _historical_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.historical_periods) or list(ts.historical_years)


def _projection_periods(model: FinancialModel) -> List[int]:
    ts = model.time_structure
    return list(ts.projection_periods) or list(ts.projection_years)


def _all_periods(model: FinancialModel) -> List[int]:
    return _historical_periods(model) + _projection_periods(model)


def _period_year(period: int, mode: str) -> int:
    """Extract the year component from a period key."""
    if mode == PERIOD_MODE_QUARTERLY5:
        return period // 10
    return period


def _resolve_period_list(periods: str, model: FinancialModel) -> Tuple[List[int], str]:
    """Return (period_list, label) for the given periods spec."""
    if periods == "projection":
        return _projection_periods(model), "projection"
    if periods == "historical":
        return _historical_periods(model), "historical"
    if periods == "all":
        return _all_periods(model), "all"

    match = re.fullmatch(r"(\d{4}):(\d{4})", periods)
    if match:
        start_year = int(match.group(1))
        end_year = int(match.group(2))
        if start_year > end_year:
            raise ValueError(f"Invalid year range: start ({start_year}) > end ({end_year})")
        all_periods = _all_periods(model)
        mode = model.time_structure.period_mode
        filtered = [period for period in all_periods if start_year <= _period_year(period, mode) <= end_year]
        return filtered, periods

    raise ValueError(
        "periods must be one of: all, projection, historical, or a year range like '2023:2027'"
    )


def _annual_projection_periods(model: FinancialModel) -> List[int]:
    """Return one period per projection year."""
    projection_periods = _projection_periods(model)
    mode = model.time_structure.period_mode
    if mode == PERIOD_MODE_QUARTERLY5:
        return [period for period in projection_periods if period % 10 == 5]
    return projection_periods


def _annual_historical_periods(model: FinancialModel, n: int = 3) -> List[int]:
    """Return the last n annual historical periods."""
    historical_periods = _historical_periods(model)
    mode = model.time_structure.period_mode
    if mode == PERIOD_MODE_QUARTERLY5:
        annual_periods = [period for period in historical_periods if period % 10 == 5]
    else:
        annual_periods = historical_periods
    return annual_periods[-max(n, 0) :]


def _suggest_items(
    index: Dict[str, LineItem],
    bad_id: str,
    limit: int = 5,
) -> List[str]:
    """Return up to `limit` item IDs that are similar to `bad_id`."""
    needle_full = bad_id.strip().lower()
    needle_leaf = needle_full.split(".")[-1]
    if not needle_leaf:
        return []

    scored: List[Tuple[int, str]] = []
    for item_id, item in index.items():
        id_lower = item_id.lower()
        leaf = id_lower.split(".")[-1]
        label_norm = item.label.strip().lower().replace(" ", "_")

        # Prefer natural leaf/label matches over broad ID substring matches.
        if leaf == needle_leaf:
            scored.append((0, item_id))
        elif needle_leaf in leaf:
            scored.append((1, item_id))
        elif needle_full in id_lower:
            scored.append((2, item_id))
        elif needle_leaf in label_norm:
            scored.append((3, item_id))

    scored.sort(key=lambda row: (row[0], row[1]))
    if scored:
        return [item_id for _, item_id in scored[: max(limit, 0)]]

    if limit <= 0:
        return []

    leaf_to_ids: Dict[str, List[str]] = {}
    for item_id in index:
        leaf_to_ids.setdefault(item_id.lower().split(".")[-1], []).append(item_id)
    for leaf_ids in leaf_to_ids.values():
        leaf_ids.sort()

    suggestions: List[str] = []
    close_leafs = get_close_matches(needle_leaf, list(leaf_to_ids), n=limit, cutoff=0.6)
    for leaf in close_leafs:
        for item_id in leaf_to_ids.get(leaf, []):
            suggestions.append(item_id)
            if len(suggestions) >= limit:
                return suggestions
    return suggestions


def _format_unknown_id_error(
    index: Dict[str, LineItem],
    bad_id: str,
    id_role: str = "item_id",
) -> str:
    suggestions = _suggest_items(index, bad_id)
    msg = f"Unknown {id_role}: {bad_id}"
    if suggestions:
        msg += f". Did you mean: {', '.join(suggestions)}?"
    return msg


def _formula_type(item: LineItem) -> Optional[str]:
    spec = item.projected or item.historical
    return spec.type.value if spec else None


def _fallback_formula(item: LineItem) -> Optional[FormulaSpec]:
    # Preserve existing projected behavior first when available.
    if item.projected is not None and item.projected.type != FormulaType.constant:
        return item.projected
    # Some models keep the only usable formulas in historical period overrides.
    if item.overrides:
        for period in sorted(item.overrides.keys(), reverse=True):
            spec = item.overrides[period]
            if spec.type != FormulaType.constant:
                return spec
    if item.historical is not None and item.historical.type != FormulaType.constant:
        return item.historical
    return None


def _candidate_formula_type(item: LineItem) -> Optional[FormulaType]:
    spec = _fallback_formula(item)
    if spec is not None:
        return spec.type

    if item.projected is not None and item.projected.type == FormulaType.constant:
        return FormulaType.constant
    if item.historical is not None and item.historical.type == FormulaType.constant:
        return FormulaType.constant
    if item.overrides:
        for period in sorted(item.overrides.keys(), reverse=True):
            if item.overrides[period].type == FormulaType.constant:
                return FormulaType.constant
    return None


def _resolve_candidate_filter(
    candidate_filter: str,
    include_derived: Optional[bool],
) -> str:
    resolved = candidate_filter
    if include_derived is not None:
        legacy = "all" if include_derived else "inputs_only"
        if resolved != "drivers" and resolved != legacy:
            raise ValueError("Conflicting sensitivity filters: use either include_derived or candidate_filter")
        resolved = legacy

    if resolved not in _SENSITIVITY_CANDIDATE_FILTERS:
        allowed = ", ".join(sorted(_SENSITIVITY_CANDIDATE_FILTERS))
        raise ValueError(f"candidate_filter must be one of: {allowed}")
    return resolved


def _filter_sensitivity_candidates(
    model: FinancialModel,
    candidate_ids: Set[str],
    candidate_filter: str,
) -> Set[str]:
    if candidate_filter == "all":
        return set(candidate_ids)
    if candidate_filter == "inputs_only":
        return {
            item_id
            for item_id in candidate_ids
            if model.get_item(item_id).item_type == ItemType.input
        }

    filtered: Set[str] = set()
    for item_id in candidate_ids:
        item = model.get_item(item_id)
        if item.item_type == ItemType.input:
            filtered.add(item_id)
            continue
        formula_type = _candidate_formula_type(item)
        if formula_type in _SENSITIVITY_DRIVER_FORMULA_TYPES:
            filtered.add(item_id)
    return filtered


def _promote_projected_fallbacks(
    model: FinancialModel,
    item_ids: Set[str],
) -> Dict[str, Optional[FormulaSpec]]:
    promoted: Dict[str, Optional[FormulaSpec]] = {}
    for item_id in item_ids:
        item = model.get_item(item_id)
        if item.projected is not None:
            continue
        fallback = _fallback_formula(item)
        if fallback is None:
            continue
        promoted[item_id] = item.projected
        item.projected = fallback
    return promoted


def _sample_values(values: Dict[int, float], periods: List[int]) -> Dict[int, Optional[float]]:
    if not periods:
        return {}
    idxs = sorted({0, len(periods) // 2, len(periods) - 1})
    sample_periods = [periods[i] for i in idxs]
    return {period: values.get(period) for period in sample_periods}


def _build_ref_alias_groups(model: FinancialModel) -> Dict[str, str]:
    neighbors: Dict[str, Set[str]] = {item_id: set() for item_id in model._index}
    for item_id, item in model._index.items():
        source_id = _same_period_ref_source(item)
        if source_id is None or source_id not in neighbors:
            continue
        neighbors[item_id].add(source_id)
        neighbors[source_id].add(item_id)

    group_by_id: Dict[str, str] = {}
    visited: Set[str] = set()
    for item_id in sorted(neighbors):
        if item_id in visited:
            continue
        stack = [item_id]
        component: List[str] = []
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            component.append(node)
            for nxt in neighbors.get(node, set()):
                if nxt not in visited:
                    stack.append(nxt)
        group_id = min(component)
        for node in component:
            group_by_id[node] = group_id
    return group_by_id


def _same_period_ref_source(item: LineItem) -> Optional[str]:
    spec = _fallback_formula(item)
    if spec is None or spec.type != FormulaType.ref:
        return None
    source = spec.params.get("source")
    if isinstance(source, LineItemRef) and source.t == 0:
        return source.id
    return None


def _dedupe_sensitivity_impacts(
    impacts: List[Dict],
    model: FinancialModel,
    alias_group_by_id: Dict[str, str],
) -> List[Dict]:
    grouped: Dict[str, List[Dict]] = {}
    for row in impacts:
        group_id = alias_group_by_id.get(row["id"], row["id"])
        grouped.setdefault(group_id, []).append(row)

    deduped: List[Dict] = []
    for rows in grouped.values():
        clusters: List[List[Dict]] = []
        for row in rows:
            placed = False
            for cluster in clusters:
                if _impacts_equivalent(row, cluster[0]):
                    cluster.append(row)
                    placed = True
                    break
            if not placed:
                clusters.append([row])

        for cluster in clusters:
            representative = min(
                cluster,
                key=lambda entry: _sensitivity_representative_rank(model.get_item(entry["id"]), entry),
            )
            if len(cluster) == 1:
                deduped.append(representative)
                continue
            aliases = sorted(entry["id"] for entry in cluster if entry["id"] != representative["id"])
            row_copy = dict(representative)
            row_copy["alias_ids"] = aliases
            deduped.append(row_copy)
    return deduped


def _sensitivity_representative_rank(item: LineItem, row: Dict) -> Tuple[int, int, int, int, str]:
    is_input_penalty = 0 if item.item_type == ItemType.input else 1
    is_ref_penalty = 1 if row.get("formula_type") == FormulaType.ref.value else 0
    row_suffix_penalty = 1 if re.search(r"_r\d+$", item.id) else 0
    return (is_input_penalty, is_ref_penalty, row_suffix_penalty, len(item.id), item.id)


def _impacts_equivalent(left: Dict, right: Dict) -> bool:
    left_delta = left.get("delta")
    right_delta = right.get("delta")
    if left_delta is None or right_delta is None:
        return left_delta is None and right_delta is None
    return _float_close(left_delta, right_delta)


def _float_close(a: float, b: float, abs_eps: float = 1e-9, rel_eps: float = 1e-6) -> bool:
    diff = abs(a - b)
    scale = max(abs(a), abs(b), 1.0)
    return diff <= max(abs_eps, rel_eps * scale)


def _item_locations(model: FinancialModel) -> Dict[str, Tuple[str, Optional[str], int]]:
    locations: Dict[str, Tuple[str, Optional[str], int]] = {}
    for sheet in model.sheets.values():
        for section in sheet.sections:
            section_label = section.label or section.id
            for item in section.line_items:
                locations[item.id] = (sheet.name, section_label, item.row)
    return locations


def _parent_headers(model: FinancialModel) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {}
    for sheet in model.sheets.values():
        for section in sheet.sections:
            current_header: Optional[str] = None
            for item in section.line_items:
                if item.item_type == ItemType.header:
                    current_header = item.label.strip()
                result[item.id] = current_header
    return result


def _format_find_context(
    context: Optional[Tuple[str, Optional[str], int]],
    parent_header: Optional[str],
) -> Optional[str]:
    if context is None:
        return None
    sheet_name, _section_label, row = context
    if parent_header:
        return f"{sheet_name} / {parent_header} / row {row}"
    return f"{sheet_name} / row {row}"


def _format_context_label(context: Optional[Tuple[str, Optional[str], int]]) -> Optional[str]:
    if context is None:
        return None
    sheet_name, section_label, row = context
    if section_label:
        return f"{sheet_name} / {section_label} / row {row}"
    return f"{sheet_name} / row {row}"


def _ambiguous_labels(items: Iterable[LineItem]) -> Set[str]:
    counts: Dict[str, int] = {}
    for item in items:
        key = _label_key(item.label)
        counts[key] = counts.get(key, 0) + 1
    return {key for key, count in counts.items() if count > 1}


def _label_key(label: str) -> str:
    return " ".join(label.strip().lower().split())


def _resolve_max_candidates(
    model: FinancialModel,
    candidate_filter: str,
    max_candidates: Optional[int],
) -> Optional[int]:
    if max_candidates is not None:
        return max(max_candidates, 0)
    if model.time_structure.period_mode == PERIOD_MODE_QUARTERLY5 and candidate_filter == "drivers":
        return _SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY
    return None


def _collapse_alias_candidates(
    candidates: List[str],
    model: FinancialModel,
    alias_group_by_id: Dict[str, str],
) -> Tuple[List[str], Dict[str, List[str]]]:
    groups: Dict[str, List[str]] = {}
    for candidate_id in candidates:
        group_id = alias_group_by_id.get(candidate_id, candidate_id)
        groups.setdefault(group_id, []).append(candidate_id)

    collapsed: List[str] = []
    aliases_by_representative: Dict[str, List[str]] = {}
    for group_ids in groups.values():
        representative = min(
            group_ids,
            key=lambda candidate_id: _sensitivity_representative_rank(
                model.get_item(candidate_id),
                {"formula_type": _formula_type(model.get_item(candidate_id))},
            ),
        )
        collapsed.append(representative)
        aliases = sorted(candidate_id for candidate_id in group_ids if candidate_id != representative)
        if aliases:
            aliases_by_representative[representative] = aliases
    return sorted(collapsed), aliases_by_representative


def _rank_candidates_for_sensitivity(
    candidates: List[str],
    model: FinancialModel,
    graph: DependencyGraph,
    target_id: str,
) -> List[str]:
    distances = _candidate_distances_to_target(graph, target_id)
    ranked = sorted(
        candidates,
        key=lambda candidate_id: (
            distances.get(candidate_id, 10**9),
            _sensitivity_representative_rank(
                model.get_item(candidate_id),
                {"formula_type": _formula_type(model.get_item(candidate_id))},
            ),
            candidate_id,
        ),
    )
    return ranked


def _candidate_distances_to_target(graph: DependencyGraph, target_id: str) -> Dict[str, int]:
    reverse: Dict[str, Set[str]] = {node: set() for node in graph.nodes}
    for src, dsts in graph.adj.items():
        for dst in dsts:
            reverse.setdefault(dst, set()).add(src)

    distances: Dict[str, int] = {target_id: 0}
    queue: List[str] = [target_id]
    while queue:
        node = queue.pop(0)
        next_distance = distances[node] + 1
        for upstream in reverse.get(node, set()):
            if upstream in distances:
                continue
            distances[upstream] = next_distance
            queue.append(upstream)
    return distances


def _compute_scenario_results(
    bundle: _ModelBundle,
    overrides: Dict[str, Dict[int, float]],
    recompute_ids: Set[str],
) -> Dict[str, Dict[int, float]]:
    model = bundle.model
    graph = bundle.graph
    period_mode = model.time_structure.period_mode
    if period_mode != PERIOD_MODE_QUARTERLY5:
        return graph.compute(
            overrides,
            recompute=recompute_ids,
            cycle_fallback_policy="auto_propagate",
        )

    all_periods = _all_periods(model)
    override_periods = sorted({period for values in overrides.values() for period in values})
    if not override_periods:
        return bundle.base_results

    start_period = min(override_periods)
    active_periods = [period for period in all_periods if period >= start_period]
    if not active_periods:
        return bundle.base_results

    scenario_results: Dict[str, Dict[int, float]] = {
        item_id: dict(values)
        for item_id, values in bundle.base_results.items()
    }
    seed_results: Dict[str, Dict[int, float]] = {
        item_id: dict(values)
        for item_id, values in bundle.base_results.items()
    }
    for period in active_periods:
        period_inputs = {
            item_id: {period: values[period]}
            for item_id, values in overrides.items()
            if period in values
        }
        period_results = graph.compute(
            period_inputs,
            recompute=recompute_ids,
            cycle_fallback_policy="auto_propagate",
            periods={period},
            seed_results=seed_results,
        )
        if _period_is_unstable(period_results, period, recompute_ids):
            period_results = graph.compute(
                period_inputs,
                recompute=recompute_ids,
                cycle_fallback_policy="auto",
                periods={period},
                seed_results=seed_results,
            )
        _merge_period_results(seed_results, period_results, period)
        _merge_period_results(scenario_results, period_results, period)
    return scenario_results


def _period_is_unstable(
    results: Dict[str, Dict[int, float]],
    period: int,
    item_ids: Set[str],
) -> bool:
    for item_id in item_ids:
        value = results.get(item_id, {}).get(period)
        if not isinstance(value, (int, float)):
            continue
        if not math.isfinite(value):
            return True
        if abs(float(value)) > _SCENARIO_DIVERGENCE_ABS_LIMIT:
            return True
    return False


def _merge_period_results(
    destination: Dict[str, Dict[int, float]],
    period_results: Dict[str, Dict[int, float]],
    period: int,
) -> None:
    for item_id, values in period_results.items():
        if period not in values:
            continue
        value = values[period]
        if value is None:
            continue
        destination.setdefault(item_id, {})[period] = value
