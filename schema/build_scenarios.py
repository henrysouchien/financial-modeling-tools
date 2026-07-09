"""Scenario output compute helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
import logging
import math
import re
import sys
from typing import Any

from .build_formula_refs import _extract_ref_ids as _formula_extract_ref_ids
from .build_model_items import _iter_items as _model_iter_items
from .dependency_graph import DependencyGraph
from .model_readiness_scenario_output import (
    _SCENARIO_OUTPUT_REQUIREMENTS as _MODEL_SCENARIO_OUTPUT_REQUIREMENTS,
)
from .models import (
    FinancialModel,
    FormulaType,
    ItemType,
    LineItem,
    ScenarioInputs,
    Section,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .refs import line_item_ref_from_obj as _line_item_ref_from_obj


_SCENARIO_CASE_SELECTOR = {"bull": 1.0, "base": 2.0, "bear": 3.0}
_SCENARIO_CASE_PATTERNS = {
    "bull": {"bull", "bullcase", "upside", "bullscenario"},
    "base": {"base", "basecase", "central", "basescenario"},
    "bear": {"bear", "bearcase", "downside", "bearscenario"},
}
_SCENARIO_EPS_LIMIT = 4
_SCENARIO_EPS_ITEM_RE = re.compile(r"^tpl\.s\.earnings_scenarios\.eps_(bull|base|bear)_(\d+)$")


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _finite_projection_values(
    values: dict[int, Any],
    projection_periods: Iterable[int],
) -> dict[int, float]:
    projected: dict[int, float] = {}
    for period in projection_periods:
        raw_value = values.get(int(period))
        if raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            projected[int(period)] = value
    return projected


def _select_scenario_output_item_id(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    candidate_item_ids: Iterable[str],
    projection_periods: Iterable[int],
) -> str | None:
    finite_projection_values = _parent_attr(
        "_finite_projection_values",
        _finite_projection_values,
    )
    present_item_ids = [item_id for item_id in candidate_item_ids if item_id in model._index]
    if not present_item_ids:
        return None
    periods = [int(period) for period in projection_periods]
    for item_id in present_item_ids:
        if finite_projection_values(values.get(item_id, {}), periods):
            return item_id
    return present_item_ids[0]


def compute_scenario_outputs(model: FinancialModel) -> dict[str, dict[str, dict[int, float]]]:
    """Run Bull/Base/Bear scenario computations for decision-critical outputs."""

    if not model._index:
        model.build_index()
    dependency_graph_cls = _parent_attr("DependencyGraph", DependencyGraph)
    graph = dependency_graph_cls()
    graph.build(model)
    base_results = graph.compute({})
    scenario_outputs: dict[str, dict[str, dict[int, float]]] = {}
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    scenario_output_requirements = _parent_attr(
        "_SCENARIO_OUTPUT_REQUIREMENTS",
        _MODEL_SCENARIO_OUTPUT_REQUIREMENTS,
    )
    build_scenario_overrides = _parent_attr(
        "_build_scenario_overrides",
        _build_scenario_overrides,
    )
    downstream_item_ids = _parent_attr("_downstream_item_ids", _downstream_item_ids)
    scenario_compute_inputs = _parent_attr(
        "_scenario_compute_inputs",
        _scenario_compute_inputs,
    )
    select_scenario_output_item_id = _parent_attr(
        "_select_scenario_output_item_id",
        _select_scenario_output_item_id,
    )
    finite_projection_values = _parent_attr(
        "_finite_projection_values",
        _finite_projection_values,
    )

    for case in ("bull", "base", "bear"):
        overrides = build_scenario_overrides(model, case, base_results=base_results)
        recompute = downstream_item_ids(graph, set(overrides))
        scenario_inputs = scenario_compute_inputs(model, overrides)
        results = graph.compute(
            scenario_inputs,
            recompute=recompute,
            seed_results=base_results,
            propagate_roots=set(overrides),
            periods=set(projection_periods),
        )
        case_outputs: dict[str, dict[int, float]] = {}
        for field_name, candidate_item_ids in scenario_output_requirements:
            item_id = select_scenario_output_item_id(
                model,
                results,
                candidate_item_ids,
                projection_periods,
            )
            if item_id is None:
                continue
            field_values = finite_projection_values(results.get(item_id, {}), projection_periods)
            if field_values:
                case_outputs[field_name] = field_values
        scenario_outputs[case] = case_outputs

    return scenario_outputs


def compute_scenario_eps(model: FinancialModel) -> dict[str, dict[int, float]]:
    """Run Bull/Base/Bear EPS computations using direct scenario-table overrides."""

    compute_outputs = _parent_attr("compute_scenario_outputs", compute_scenario_outputs)
    return {
        case: dict(fields.get("adj_eps", {}))
        for case, fields in compute_outputs(model).items()
    }


def _populate_scenario_eps(model: FinancialModel, eps_by_case: dict[str, dict[int, float]]) -> None:
    if not model._index:
        model.build_index()
    scenario_eps_limit = _parent_attr("_SCENARIO_EPS_LIMIT", _SCENARIO_EPS_LIMIT)
    scenario_eps_item_re = _parent_attr("_SCENARIO_EPS_ITEM_RE", _SCENARIO_EPS_ITEM_RE)
    iter_items = _parent_attr("_iter_items", _model_iter_items)
    value_series_cls = _parent_attr("ValueSeries", ValueSeries)
    value_cell_cls = _parent_attr("ValueCell", ValueCell)
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    projection_periods = [int(period) for period in model.time_structure.projection_periods[:scenario_eps_limit]]

    for item_obj in iter_items(model):
        match = scenario_eps_item_re.match(item_obj.id)
        if match is None:
            continue
        case = match.group(1)
        index = int(match.group(2)) - 1
        if index < 0 or index >= len(projection_periods):
            continue
        period = projection_periods[index]
        value = eps_by_case.get(case, {}).get(period)
        if value is None:
            continue
        if item_obj.values is None:
            item_obj.values = value_series_cls()
        item_obj.values.values[int(period)] = value_cell_cls(
            period=int(period),
            value=float(value),
            provenance=value_provenance.computed,
        )


def _populate_scenario_inputs(model: FinancialModel) -> None:
    scenario_inputs_cls = _parent_attr("ScenarioInputs", ScenarioInputs)
    model.scenarios = {
        "bull": scenario_inputs_cls(
            name="Bull",
            assumptions={"tpl.a.header.scenario_value": 1},
            description="Bull case: higher growth, higher multiples",
        ),
        "base": scenario_inputs_cls(
            name="Base",
            assumptions={"tpl.a.header.scenario_value": 2},
            description="Base case: consensus growth, median multiples",
        ),
        "bear": scenario_inputs_cls(
            name="Bear",
            assumptions={"tpl.a.header.scenario_value": 3},
            description="Bear case: lower growth, lower multiples",
        ),
    }


def _build_scenario_overrides(
    model: FinancialModel,
    case: str,
    base_results: dict[str, dict[int, float]] | None = None,
) -> dict[str, dict[int, float]]:
    """Build per-period direct overrides for OFFSET-targeted assumption rows."""

    if not model._index:
        model.build_index()
    case = case.lower()
    scenario_case_selector = _parent_attr(
        "_SCENARIO_CASE_SELECTOR",
        _SCENARIO_CASE_SELECTOR,
    )
    if case not in scenario_case_selector:
        raise ValueError(f"Unknown scenario case: {case}")

    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not projection_periods:
        return {}

    dependency_graph_cls = _parent_attr("DependencyGraph", DependencyGraph)
    if base_results is None:
        scenario_graph = dependency_graph_cls()
        scenario_graph.build(model)
        base_results = scenario_graph.compute({})

    formula_type = _parent_attr("FormulaType", FormulaType)
    line_item_ref_from_obj = _parent_attr(
        "line_item_ref_from_obj",
        _line_item_ref_from_obj,
    )
    find_scenario_value_row = _parent_attr(
        "_find_scenario_value_row",
        _find_scenario_value_row,
    )

    overrides: dict[str, dict[int, float]] = {}
    for sheet in model.sheets.values():
        if sheet.name != "Assumptions":
            continue
        for section in sheet.sections:
            for item_obj in section.line_items:
                spec = item_obj.projected
                if spec is None or spec.type != formula_type.valuation or spec.subtype != "offset_scenario":
                    continue
                params = spec.params or {}
                anchor_ref = line_item_ref_from_obj(params.get("anchor"))
                if anchor_ref is None:
                    raise ValueError(f"OFFSET scenario {item_obj.id} is missing anchor ref")
                anchor_id = anchor_ref.id
                value_row_id = find_scenario_value_row(model, anchor_id, case)
                if value_row_id is None:
                    continue
                period_values = {
                    period: value
                    for period in projection_periods
                    if (value := base_results.get(value_row_id, {}).get(period)) is not None
                }
                if period_values:
                    overrides[item_obj.id] = period_values
    return overrides


def _scenario_compute_inputs(
    model: FinancialModel,
    overrides: dict[str, dict[int, float]],
) -> dict[str, dict[int, float]]:
    """Pin explicit assumption constants while scenario drivers propagate."""

    scenario_inputs: dict[str, dict[int, float]] = {
        item_id: {int(period): float(value) for period, value in period_values.items()}
        for item_id, period_values in overrides.items()
    }
    projection_periods = {int(period) for period in model.time_structure.projection_periods}
    if not projection_periods:
        return scenario_inputs
    scenario_root_ids = set(overrides)
    projected_formula_refs_any = _parent_attr(
        "_projected_formula_refs_any",
        _projected_formula_refs_any,
    )
    formula_type = _parent_attr("FormulaType", FormulaType)

    for sheet in model.sheets.values():
        if sheet.name != "Assumptions":
            continue
        for section in sheet.sections:
            for item_obj in section.line_items:
                if item_obj.id in scenario_inputs or not item_obj.overrides:
                    continue
                if projected_formula_refs_any(item_obj, scenario_root_ids):
                    continue
                constants: dict[int, float] = {}
                for raw_period, spec in item_obj.overrides.items():
                    period = int(raw_period)
                    if period not in projection_periods:
                        continue
                    if spec.type != formula_type.constant:
                        continue
                    value = (spec.params or {}).get("value")
                    if value is None:
                        continue
                    try:
                        constants[period] = float(value)
                    except (TypeError, ValueError):
                        continue
                if constants:
                    scenario_inputs[item_obj.id] = constants

    return scenario_inputs


def _projected_formula_refs_any(item_obj: LineItem, item_ids: set[str]) -> bool:
    """Return true when an item's projected formula directly consumes a scenario root."""

    spec = item_obj.projected
    if spec is None or not item_ids:
        return False
    extract_ref_ids = _parent_attr("_extract_ref_ids", _formula_extract_ref_ids)
    return bool(extract_ref_ids(spec.params or {}) & item_ids)


def _find_scenario_value_row(model: FinancialModel, anchor_id: str, case: str) -> str | None:
    """Find the Bull/Base/Bear value row immediately below a scenario-table anchor."""

    find_item_location = _parent_attr("_find_item_location", _find_item_location)
    offset_anchor_ids = _parent_attr("_offset_anchor_ids", _offset_anchor_ids)
    normalize_scenario_label = _parent_attr(
        "_normalize_scenario_label",
        _normalize_scenario_label,
    )
    scenario_case_patterns = _parent_attr(
        "_SCENARIO_CASE_PATTERNS",
        _SCENARIO_CASE_PATTERNS,
    )
    item_type = _parent_attr("ItemType", ItemType)

    anchor_location = find_item_location(model, anchor_id)
    if anchor_location is None:
        logging.warning("Scenario override anchor not found: %s", anchor_id)
        return None
    _sheet_name, section, anchor_index = anchor_location
    anchor_row = int(section.line_items[anchor_index].row)

    all_anchor_ids = offset_anchor_ids(model)
    candidates: list[LineItem] = []
    for candidate in section.line_items[anchor_index + 1:]:
        if int(candidate.row) <= anchor_row:
            continue
        if candidate.id in all_anchor_ids:
            break
        if candidate.item_type in {item_type.header, item_type.spacer}:
            break
        candidates.append(candidate)

    patterns = scenario_case_patterns[case]
    for candidate in candidates:
        if normalize_scenario_label(candidate.label) in patterns:
            return candidate.id

    if len(candidates) != 3:
        logging.warning(
            "Scenario override anchor %s is ambiguous for %s: observed %d candidate rows",
            anchor_id,
            case,
            len(candidates),
        )
        return None

    sorted_candidates = sorted(candidates, key=lambda item_obj: int(item_obj.row))
    rows = [int(item_obj.row) for item_obj in sorted_candidates]
    expected_rows = list(range(rows[0], rows[0] + 3))
    if rows != expected_rows:
        logging.warning(
            "Scenario override anchor %s is ambiguous for %s: candidate rows are not contiguous (%s)",
            anchor_id,
            case,
            rows,
        )
        return None

    position = {"bull": 0, "base": 1, "bear": 2}[case]
    return sorted_candidates[position].id


def _find_item_location(model: FinancialModel, item_id: str) -> tuple[str, Section, int] | None:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item_obj in enumerate(section.line_items):
                if item_obj.id == item_id:
                    return sheet_name, section, index
    return None


def _offset_anchor_ids(model: FinancialModel) -> set[str]:
    anchors: set[str] = set()
    iter_items = _parent_attr("_iter_items", _model_iter_items)
    formula_type = _parent_attr("FormulaType", FormulaType)
    line_item_ref_from_obj = _parent_attr(
        "line_item_ref_from_obj",
        _line_item_ref_from_obj,
    )
    for item_obj in iter_items(model):
        for spec in (item_obj.historical, item_obj.projected):
            if spec is None or spec.type != formula_type.valuation or spec.subtype != "offset_scenario":
                continue
            anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
            if anchor_ref is not None:
                anchors.add(anchor_ref.id)
    return anchors


def _normalize_scenario_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(label or "").strip().lower())


def _downstream_item_ids(graph: DependencyGraph, roots: set[str]) -> set[str]:
    downstream: set[str] = set()
    stack = list(roots)
    while stack:
        node = stack.pop()
        for child in graph.adj.get(node, set()):
            if child in downstream:
                continue
            downstream.add(child)
            stack.append(child)
    return downstream


__all__ = [
    "_SCENARIO_CASE_PATTERNS",
    "_SCENARIO_CASE_SELECTOR",
    "_SCENARIO_EPS_ITEM_RE",
    "_SCENARIO_EPS_LIMIT",
    "_build_scenario_overrides",
    "_downstream_item_ids",
    "_find_item_location",
    "_find_scenario_value_row",
    "_finite_projection_values",
    "_normalize_scenario_label",
    "_offset_anchor_ids",
    "_populate_scenario_eps",
    "_populate_scenario_inputs",
    "_projected_formula_refs_any",
    "_scenario_compute_inputs",
    "_select_scenario_output_item_id",
    "compute_scenario_eps",
    "compute_scenario_outputs",
]
