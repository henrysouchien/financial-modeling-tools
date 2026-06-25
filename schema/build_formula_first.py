"""Formula-first reconciliation helpers for schema build orchestration."""

from __future__ import annotations

import logging
import sys

from .build_formula_eval import (
    _constant_override_value as _formula_constant_override_value,
    _evaluate_formula_simple as _formula_evaluate_formula_simple,
)
from .build_formula_refs import (
    _all_refs_same_period as _formula_all_refs_same_period,
    _extract_ref_ids as _formula_extract_ref_ids,
)
from .build_model_items import _iter_items as _model_iter_items
from .build_real_data import (
    _item_has_direct_real_data as _real_item_has_direct_real_data,
    _item_has_real_data as _real_item_has_real_data,
)
from .models import FinancialModel, FormulaType, LineItem


_SYNTHETIC_FAST_PATH_TYPES = frozenset({
    FormulaType.arithmetic,
    FormulaType.ref,
    FormulaType.ratio,
})
_FORMULA_FIRST_EXCLUDED_ITEM_IDS = frozenset({
    # Direct CF ending-cash actuals are statement-basis values. The historical
    # formula is a no-source fallback bridge from BS cash, not an equivalence
    # proof when restricted cash moves separately.
    "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period",
})


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _active_formula_first_periods(
    item: LineItem,
    periods: list[int],
) -> set[int]:
    active_periods = None
    if item.formula_periods is not None:
        active_periods = {int(period) for period in item.formula_periods}
    return {
        int(period)
        for period in periods
        if active_periods is None or int(period) in active_periods
    }


def _formula_first_node_periods(
    model: FinancialModel,
    historical_periods: list[int],
) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """Return removable override periods plus formula-only bridge periods."""

    model.build_index()
    periods = [int(period) for period in historical_periods]
    iter_items = _parent_attr("_iter_items", _model_iter_items)
    item_has_real_data = _parent_attr("_item_has_real_data", _real_item_has_real_data)
    active_periods_for_item = _parent_attr(
        "_active_formula_first_periods",
        _active_formula_first_periods,
    )
    extract_ref_ids = _parent_attr("_extract_ref_ids", _formula_extract_ref_ids)
    all_refs_same_period = _parent_attr(
        "_all_refs_same_period",
        _formula_all_refs_same_period,
    )
    constant_override_value = _parent_attr(
        "_constant_override_value",
        _formula_constant_override_value,
    )
    item_has_direct_real_data = _parent_attr(
        "_item_has_direct_real_data",
        _real_item_has_direct_real_data,
    )
    synthetic_types = _parent_attr(
        "_SYNTHETIC_FAST_PATH_TYPES",
        _SYNTHETIC_FAST_PATH_TYPES,
    )
    excluded_item_ids = _parent_attr(
        "_FORMULA_FIRST_EXCLUDED_ITEM_IDS",
        _FORMULA_FIRST_EXCLUDED_ITEM_IDS,
    )

    items_by_id = {item.id: item for item in iter_items(model)}
    has_real_data = {
        item.id: {
            period
            for period in periods
            if item_has_real_data(item, period, model=model)
        }
        for item in items_by_id.values()
    }
    removable_candidates: set[tuple[str, int]] = set()

    for item in items_by_id.values():
        if item.historical is None:
            continue
        if item.id in excluded_item_ids:
            continue

        active_periods = active_periods_for_item(item, periods)
        if not active_periods:
            continue

        if (
            item.historical.type in synthetic_types
            and not all_refs_same_period(item.historical.params)
        ):
            continue

        if item.data_concept_id:
            is_constant_formula = item.historical.type is FormulaType.constant
            ref_ids = extract_ref_ids(item.historical.params)
            if not ref_ids and not is_constant_formula:
                continue
            for period in active_periods:
                if item.overrides is None or int(period) not in item.overrides:
                    continue
                override = item.overrides[int(period)]
                if override.type is not FormulaType.constant:
                    continue
                if is_constant_formula:
                    formula_value = constant_override_value(item.historical)
                    override_value = constant_override_value(override)
                    if (
                        formula_value is None
                        or override_value is None
                        or abs(formula_value - override_value) >= 1e-6
                    ):
                        continue
                removable_candidates.add((item.id, int(period)))

    bridge_candidates: set[tuple[str, int]] = set()

    def add_bridge_dependency(item_id: str, period: int) -> None:
        if period in has_real_data.get(item_id, set()):
            return
        node = (item_id, int(period))
        if node in removable_candidates or node in bridge_candidates:
            return
        item = items_by_id.get(item_id)
        if item is None or item.data_concept_id or item.historical is None:
            return
        if int(period) not in active_periods_for_item(item, periods):
            return
        if (
            item.historical.type not in synthetic_types
            or not all_refs_same_period(item.historical.params)
            or item_has_direct_real_data(item, period)
        ):
            return

        bridge_candidates.add(node)
        for ref_id in extract_ref_ids(item.historical.params):
            add_bridge_dependency(ref_id, int(period))

    for item_id, period in sorted(removable_candidates):
        item = items_by_id[item_id]
        if item.overrides is None or int(period) not in item.overrides:
            continue
        if item.overrides[int(period)].note != "synthetic":
            continue
        for ref_id in extract_ref_ids(item.historical.params if item.historical else None):
            add_bridge_dependency(ref_id, int(period))

    removable: dict[str, set[int]] = {}
    bridges: dict[str, set[int]] = {}
    unresolved = removable_candidates | bridge_candidates

    while unresolved:
        newly_derivable: list[tuple[str, int]] = []
        for item_id, period in sorted(unresolved):
            item = items_by_id[item_id]
            ref_ids = extract_ref_ids(item.historical.params if item.historical else None)
            if all(period in has_real_data.get(ref_id, set()) for ref_id in ref_ids):
                newly_derivable.append((item_id, period))

        if not newly_derivable:
            break

        for item_id, period in newly_derivable:
            unresolved.discard((item_id, period))
            if (item_id, period) in removable_candidates:
                removable.setdefault(item_id, set()).add(period)
            else:
                bridges.setdefault(item_id, set()).add(period)
            has_real_data.setdefault(item_id, set()).add(period)

    return removable, bridges


def _compute_derivable_periods(
    model: FinancialModel,
    historical_periods: list[int],
) -> dict[str, set[int]]:
    """Return candidate historical periods where formula-backed overrides can be removed."""

    node_periods = _parent_attr("_formula_first_node_periods", _formula_first_node_periods)
    removable, _bridges = node_periods(model, historical_periods)
    return removable


def _reconcile_override(
    item: LineItem,
    period: int,
    computed_value: float,
    tolerance: float = 0.01,
) -> bool:
    """Return True when the formula value matches the override within tolerance."""

    if item.overrides is None or int(period) not in item.overrides:
        return False
    spec = item.overrides[int(period)]
    synthetic_types = _parent_attr(
        "_SYNTHETIC_FAST_PATH_TYPES",
        _SYNTHETIC_FAST_PATH_TYPES,
    )
    all_refs_same_period = _parent_attr(
        "_all_refs_same_period",
        _formula_all_refs_same_period,
    )
    if (
        spec.note == "synthetic"
        and item.historical is not None
        and item.historical.type in synthetic_types
        and all_refs_same_period(item.historical.params)
    ):
        # Synthetic markers are placeholders, not data. When the historical
        # formula is a real derivation over same-period refs and produced a
        # value, the formula should win. Cross-period refs fail closed because
        # derivability currently validates dependencies at the candidate period.
        return computed_value is not None

    constant_override_value = _parent_attr(
        "_constant_override_value",
        _formula_constant_override_value,
    )
    override_value = constant_override_value(spec)
    if override_value is None:
        return False

    diff = abs(float(computed_value) - override_value)
    if abs(override_value) < 1e-6:
        return diff < 1e-6
    return diff / abs(override_value) <= tolerance


def apply_formula_first(
    model: FinancialModel,
    historical_periods: list[int],
) -> dict[str, set[int]]:
    """Remove reconciled constant overrides from derivable formula-backed items."""

    log = _parent_attr("logging", logging)
    model.build_index()
    node_periods = _parent_attr("_formula_first_node_periods", _formula_first_node_periods)
    candidate_periods, bridge_periods = node_periods(
        model,
        historical_periods,
    )
    if not candidate_periods and not bridge_periods:
        return {}

    removable_nodes = {
        (item_id, int(period))
        for item_id, periods in candidate_periods.items()
        for period in periods
    }
    bridge_nodes = {
        (item_id, int(period))
        for item_id, periods in bridge_periods.items()
        for period in periods
    }
    candidate_nodes = removable_nodes | bridge_nodes
    dependencies: dict[tuple[str, int], set[tuple[str, int]]] = {}
    reverse_dependencies: dict[tuple[str, int], set[tuple[str, int]]] = {}
    extract_ref_ids = _parent_attr("_extract_ref_ids", _formula_extract_ref_ids)

    graph_periods: dict[str, set[int]] = {}
    for item_id, periods in candidate_periods.items():
        graph_periods.setdefault(item_id, set()).update({int(period) for period in periods})
    for item_id, periods in bridge_periods.items():
        graph_periods.setdefault(item_id, set()).update({int(period) for period in periods})

    for item_id, periods in graph_periods.items():
        item = model.get_item(item_id)
        ref_ids = extract_ref_ids(item.historical.params if item.historical else None)
        for period in periods:
            node = (item_id, int(period))
            deps = {
                (ref_id, int(period))
                for ref_id in ref_ids
                if (ref_id, int(period)) in candidate_nodes
            }
            dependencies[node] = deps
            for dep in deps:
                reverse_dependencies.setdefault(dep, set()).add(node)

    indegree = {node: len(deps) for node, deps in dependencies.items()}
    ready = sorted(node for node, degree in indegree.items() if degree == 0)
    ordered: list[tuple[str, int]] = []

    while ready:
        node = ready.pop(0)
        ordered.append(node)
        for child in sorted(reverse_dependencies.get(node, set())):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
                ready.sort()

    for node in sorted(candidate_nodes - set(ordered)):
        ordered.append(node)

    computed_values: dict[tuple[str, int], float] = {}
    validated: dict[str, set[int]] = {}
    rejected: set[tuple[str, int]] = set()
    evaluate_formula_simple = _parent_attr(
        "_evaluate_formula_simple",
        _formula_evaluate_formula_simple,
    )
    reconcile_override = _parent_attr("_reconcile_override", _reconcile_override)

    def reject_with_downstream(node: tuple[str, int]) -> None:
        stack = [node]
        while stack:
            current = stack.pop()
            if current in rejected:
                continue
            rejected.add(current)
            stack.extend(sorted(reverse_dependencies.get(current, set())))

    for node in ordered:
        if node in rejected:
            continue

        if any(dep not in computed_values for dep in dependencies.get(node, set())):
            log.warning(
                "Formula-first reconciliation skipped for %s period=%s due to unresolved derivable deps",
                node[0],
                node[1],
            )
            reject_with_downstream(node)
            continue

        item_id, period = node
        item = model.get_item(item_id)
        computed_value = evaluate_formula_simple(model, item, period, computed_values)
        if node in bridge_nodes:
            if computed_value is None:
                log.warning(
                    "Formula-first bridge evaluation failed for %s period=%s",
                    item_id,
                    period,
                )
                reject_with_downstream(node)
                continue
            computed_values[node] = computed_value
            continue

        override_value = None
        if item.overrides is not None and int(period) in item.overrides:
            override_value = item.overrides[int(period)].params.get("value")

        if computed_value is None or not reconcile_override(item, period, computed_value):
            log.warning(
                "Formula-first reconciliation failed for %s period=%s: computed=%s override=%s",
                item_id,
                period,
                computed_value,
                override_value,
            )
            reject_with_downstream(node)
            continue

        computed_values[node] = computed_value
        validated.setdefault(item_id, set()).add(int(period))

    for item_id, periods in validated.items():
        item = model.get_item(item_id)
        if item.overrides is None:
            continue
        for period in periods:
            item.overrides.pop(int(period), None)
        if not item.overrides:
            item.overrides = None

    return validated


__all__ = [
    "_FORMULA_FIRST_EXCLUDED_ITEM_IDS",
    "_SYNTHETIC_FAST_PATH_TYPES",
    "_active_formula_first_periods",
    "_compute_derivable_periods",
    "_formula_first_node_periods",
    "_reconcile_override",
    "apply_formula_first",
]
