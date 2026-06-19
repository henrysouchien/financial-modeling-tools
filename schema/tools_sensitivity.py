"""Sensitivity and scenario compute helpers for schema model tools."""

from __future__ import annotations

import math
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .analysis import _downstream_nodes
from .dependency_graph import DependencyGraph
from .models import (
    PERIOD_MODE_QUARTERLY5,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
)
from .refs import line_item_ref_from_obj
from .tools_periods import _all_periods


_SENSITIVITY_CANDIDATE_FILTERS = {"drivers", "inputs_only", "all"}
_SENSITIVITY_MODES = {"workbook_explicit", "workbook_global", "legacy_global"}
_SENSITIVITY_RECOMPUTE_POLICIES = {"projection_safe", "legacy_global"}
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
_PARENT_MODULE = "schema.tools"


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


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
    fallback_formula = _compat("_fallback_formula", _fallback_formula)
    spec = fallback_formula(item)
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


def _resolve_candidate_filter(candidate_filter: str) -> str:
    resolved = candidate_filter
    candidate_filters = _compat("_SENSITIVITY_CANDIDATE_FILTERS", _SENSITIVITY_CANDIDATE_FILTERS)
    if resolved not in candidate_filters:
        allowed = ", ".join(sorted(candidate_filters))
        raise ValueError(f"candidate_filter must be one of: {allowed}")
    return resolved


def _resolve_sensitivity_semantics(
    explicit_candidate_ids: Optional[List[str]],
    sensitivity_mode: Optional[str],
    recompute_policy: str,
) -> Tuple[str, str]:
    recompute_policies = _compat("_SENSITIVITY_RECOMPUTE_POLICIES", _SENSITIVITY_RECOMPUTE_POLICIES)
    sensitivity_modes = _compat("_SENSITIVITY_MODES", _SENSITIVITY_MODES)
    if recompute_policy not in recompute_policies:
        allowed = ", ".join(sorted(recompute_policies))
        raise ValueError(f"recompute_policy must be one of: {allowed}")
    if sensitivity_mode is not None and sensitivity_mode not in sensitivity_modes:
        allowed = ", ".join(sorted(sensitivity_modes))
        raise ValueError(f"sensitivity_mode must be one of: {allowed}")

    if recompute_policy == "legacy_global":
        return "legacy_global", "legacy_global"
    if sensitivity_mode == "legacy_global":
        return "legacy_global", "legacy_global"
    if sensitivity_mode is None:
        sensitivity_mode = "workbook_explicit" if explicit_candidate_ids is not None else "workbook_global"
    if sensitivity_mode == "workbook_explicit" and explicit_candidate_ids is None:
        raise ValueError("sensitivity_mode='workbook_explicit' requires candidate_ids")
    return sensitivity_mode, "projection_safe"


def _normalize_candidate_ids(candidate_ids: Optional[Iterable[str]]) -> Optional[List[str]]:
    if candidate_ids is None:
        return None

    normalized: List[str] = []
    seen: Set[str] = set()
    for raw_candidate_id in candidate_ids:
        candidate_id = str(raw_candidate_id or "").strip()
        if not candidate_id or candidate_id in seen:
            continue
        seen.add(candidate_id)
        normalized.append(candidate_id)
    return normalized


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
    candidate_formula_type = _compat("_candidate_formula_type", _candidate_formula_type)
    driver_formula_types = _compat(
        "_SENSITIVITY_DRIVER_FORMULA_TYPES",
        _SENSITIVITY_DRIVER_FORMULA_TYPES,
    )
    for item_id in candidate_ids:
        item = model.get_item(item_id)
        if item.item_type == ItemType.input:
            filtered.add(item_id)
            continue
        formula_type = candidate_formula_type(item)
        if formula_type in driver_formula_types:
            filtered.add(item_id)
    return filtered


def _promote_projected_fallbacks(
    model: FinancialModel,
    item_ids: Set[str],
) -> Dict[str, Optional[FormulaSpec]]:
    promoted: Dict[str, Optional[FormulaSpec]] = {}
    fallback_formula = _compat("_fallback_formula", _fallback_formula)
    for item_id in item_ids:
        item = model.get_item(item_id)
        if item.projected is not None:
            continue
        fallback = fallback_formula(item)
        if fallback is None:
            continue
        promoted[item_id] = item.projected
        item.projected = fallback
    return promoted


def _build_ref_alias_groups(model: FinancialModel) -> Dict[str, str]:
    same_period_ref_source = _compat("_same_period_ref_source", _same_period_ref_source)
    neighbors: Dict[str, Set[str]] = {item_id: set() for item_id in model._index}
    for item_id, item in model._index.items():
        source_id = same_period_ref_source(item)
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
    fallback_formula = _compat("_fallback_formula", _fallback_formula)
    spec = fallback_formula(item)
    if spec is None or spec.type != FormulaType.ref:
        return None
    source = spec.params.get("source")
    ref = line_item_ref_from_obj(source)
    if ref is not None and ref.t == 0:
        return ref.id
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
    impacts_equivalent = _compat("_impacts_equivalent", _impacts_equivalent)
    representative_rank = _compat("_sensitivity_representative_rank", _sensitivity_representative_rank)
    for rows in grouped.values():
        clusters: List[List[Dict]] = []
        for row in rows:
            placed = False
            for cluster in clusters:
                if impacts_equivalent(row, cluster[0]):
                    cluster.append(row)
                    placed = True
                    break
            if not placed:
                clusters.append([row])

        for cluster in clusters:
            representative = min(
                cluster,
                key=lambda entry: representative_rank(model.get_item(entry["id"]), entry),
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
    return _compat("_float_close", _float_close)(left_delta, right_delta)


def _float_close(a: float, b: float, abs_eps: float = 1e-9, rel_eps: float = 1e-6) -> bool:
    diff = abs(a - b)
    scale = max(abs(a), abs(b), 1.0)
    return diff <= max(abs_eps, rel_eps * scale)


def _resolve_max_candidates(
    model: FinancialModel,
    candidate_filter: str,
    max_candidates: Optional[int],
) -> Optional[int]:
    if max_candidates is not None:
        return max(max_candidates, 0)
    default_quarterly = _compat(
        "_SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY",
        _SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY,
    )
    if model.time_structure.period_mode == PERIOD_MODE_QUARTERLY5 and candidate_filter == "drivers":
        return default_quarterly
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
    representative_rank = _compat("_sensitivity_representative_rank", _sensitivity_representative_rank)
    formula_type = _compat("_formula_type", _formula_type)
    for group_ids in groups.values():
        representative = min(
            group_ids,
            key=lambda candidate_id: representative_rank(
                model.get_item(candidate_id),
                {"formula_type": formula_type(model.get_item(candidate_id))},
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
    candidate_distances_to_target = _compat("_candidate_distances_to_target", _candidate_distances_to_target)
    representative_rank = _compat("_sensitivity_representative_rank", _sensitivity_representative_rank)
    formula_type = _compat("_formula_type", _formula_type)
    distances = candidate_distances_to_target(graph, target_id)
    ranked = sorted(
        candidates,
        key=lambda candidate_id: (
            distances.get(candidate_id, 10**9),
            representative_rank(
                model.get_item(candidate_id),
                {"formula_type": formula_type(model.get_item(candidate_id))},
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


def _scenario_recompute_ids(
    bundle: Any,
    overrides: Dict[str, Dict[int, float]],
    *,
    recompute_policy: str,
) -> Set[str]:
    recompute_ids: Set[str] = set()
    downstream_nodes = _compat("_downstream_nodes", _downstream_nodes)
    if recompute_policy == "legacy_global":
        for item_id in overrides:
            recompute_ids |= downstream_nodes(bundle.graph, item_id)
        return recompute_ids

    all_periods = _compat("_all_periods", _all_periods)(bundle.model)
    override_periods = sorted({period for values in overrides.values() for period in values})
    active_periods = (
        {period for period in all_periods if period >= min(override_periods)}
        if override_periods
        else set(all_periods)
    )
    for item_id in overrides:
        recompute_ids |= bundle.graph.downstream_for_periods([item_id], active_periods)
    return recompute_ids


def _compute_scenario_results(
    bundle: Any,
    overrides: Dict[str, Dict[int, float]],
    recompute_ids: Set[str],
    *,
    recompute_policy: str = "projection_safe",
) -> Dict[str, Dict[int, float]]:
    model = bundle.model
    graph = bundle.graph
    period_mode = model.time_structure.period_mode
    compute_kwargs: Dict[str, Any] = (
        {"propagate_roots": set()}
        if recompute_policy == "projection_safe"
        else {}
    )
    if period_mode != PERIOD_MODE_QUARTERLY5:
        return graph.compute(
            overrides,
            recompute=recompute_ids,
            cycle_fallback_policy="auto_propagate",
            seed_results=bundle.base_results,
            **compute_kwargs,
        )

    all_periods = _compat("_all_periods", _all_periods)(model)
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
    period_is_unstable = _compat("_period_is_unstable", _period_is_unstable)
    merge_period_results = _compat("_merge_period_results", _merge_period_results)
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
            **compute_kwargs,
        )
        if period_is_unstable(period_results, period, recompute_ids):
            period_results = graph.compute(
                period_inputs,
                recompute=recompute_ids,
                cycle_fallback_policy="auto",
                periods={period},
                seed_results=seed_results,
                **compute_kwargs,
            )
        merge_period_results(seed_results, period_results, period)
        merge_period_results(scenario_results, period_results, period)
    return scenario_results


def _period_is_unstable(
    results: Dict[str, Dict[int, float]],
    period: int,
    item_ids: Set[str],
) -> bool:
    divergence_limit = _compat("_SCENARIO_DIVERGENCE_ABS_LIMIT", _SCENARIO_DIVERGENCE_ABS_LIMIT)
    for item_id in item_ids:
        value = results.get(item_id, {}).get(period)
        if not isinstance(value, (int, float)):
            continue
        if not math.isfinite(value):
            return True
        if abs(float(value)) > divergence_limit:
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


_ORIGINALS = {
    "_SCENARIO_DIVERGENCE_ABS_LIMIT": _SCENARIO_DIVERGENCE_ABS_LIMIT,
    "_SENSITIVITY_CANDIDATE_FILTERS": _SENSITIVITY_CANDIDATE_FILTERS,
    "_SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY": _SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY,
    "_SENSITIVITY_DRIVER_FORMULA_TYPES": _SENSITIVITY_DRIVER_FORMULA_TYPES,
    "_SENSITIVITY_MODES": _SENSITIVITY_MODES,
    "_SENSITIVITY_RECOMPUTE_POLICIES": _SENSITIVITY_RECOMPUTE_POLICIES,
    "_all_periods": _all_periods,
    "_candidate_distances_to_target": _candidate_distances_to_target,
    "_candidate_formula_type": _candidate_formula_type,
    "_build_ref_alias_groups": _build_ref_alias_groups,
    "_collapse_alias_candidates": _collapse_alias_candidates,
    "_compute_scenario_results": _compute_scenario_results,
    "_dedupe_sensitivity_impacts": _dedupe_sensitivity_impacts,
    "_downstream_nodes": _downstream_nodes,
    "_fallback_formula": _fallback_formula,
    "_filter_sensitivity_candidates": _filter_sensitivity_candidates,
    "_float_close": _float_close,
    "_formula_type": _formula_type,
    "_impacts_equivalent": _impacts_equivalent,
    "_merge_period_results": _merge_period_results,
    "_normalize_candidate_ids": _normalize_candidate_ids,
    "_period_is_unstable": _period_is_unstable,
    "_promote_projected_fallbacks": _promote_projected_fallbacks,
    "_rank_candidates_for_sensitivity": _rank_candidates_for_sensitivity,
    "_resolve_candidate_filter": _resolve_candidate_filter,
    "_resolve_max_candidates": _resolve_max_candidates,
    "_resolve_sensitivity_semantics": _resolve_sensitivity_semantics,
    "_same_period_ref_source": _same_period_ref_source,
    "_scenario_recompute_ids": _scenario_recompute_ids,
    "_sensitivity_representative_rank": _sensitivity_representative_rank,
}


__all__ = [
    "_SCENARIO_DIVERGENCE_ABS_LIMIT",
    "_SENSITIVITY_CANDIDATE_FILTERS",
    "_SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY",
    "_SENSITIVITY_DRIVER_FORMULA_TYPES",
    "_SENSITIVITY_MODES",
    "_SENSITIVITY_RECOMPUTE_POLICIES",
    "_candidate_distances_to_target",
    "_candidate_formula_type",
    "_build_ref_alias_groups",
    "_collapse_alias_candidates",
    "_compute_scenario_results",
    "_dedupe_sensitivity_impacts",
    "_fallback_formula",
    "_filter_sensitivity_candidates",
    "_float_close",
    "_impacts_equivalent",
    "_merge_period_results",
    "_normalize_candidate_ids",
    "_period_is_unstable",
    "_promote_projected_fallbacks",
    "_rank_candidates_for_sensitivity",
    "_resolve_candidate_filter",
    "_resolve_max_candidates",
    "_resolve_sensitivity_semantics",
    "_same_period_ref_source",
    "_scenario_recompute_ids",
    "_sensitivity_representative_rank",
]
