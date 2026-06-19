"""Lightweight agent-facing tools for schema financial models."""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, Tuple

from . import serialization
from .analysis import _default_period, _downstream_nodes, _upstream_nodes
from .dependency_graph import DependencyGraph
from .model_readiness import (
    compute_model_quality_readiness,
    compute_model_projection_readiness,
    compute_model_scenario_bridge_readiness,
    compute_model_scenario_output_readiness,
    compute_valuation_input_readiness,
)
from .models import (
    FinancialModel,
    ItemType,
    LineItem,
)
from .reader import read_model
from .renderer import _fixed_cell_anchor_period
from .tools_periods import (
    _VALUES_MAX_RESPONSE_CELLS,  # noqa: F401 - compatibility alias for schema.tools imports
    _all_periods,
    _annual_historical_periods,
    _annual_projection_periods,
    _historical_periods,
    _period_guidance,
    _period_token_to_matches,  # noqa: F401 - compatibility alias for schema.tools imports
    _period_year,  # noqa: F401 - compatibility alias for schema.tools imports
    _projection_periods,
    _resolve_period_list,
    _resolve_period_token_list,  # noqa: F401 - compatibility alias for schema.tools imports
    _validate_values_response_size,
)
from .tools_workbook import (
    _PARSED_WORKBOOK_MODEL_SOURCES,  # noqa: F401 - compatibility alias for schema.tools imports
    _is_known_non_model_workbook_sheet,  # noqa: F401 - compatibility alias for schema.tools imports
    _non_model_workbook_sheet_names,  # noqa: F401 - compatibility alias for schema.tools imports
    _workbook_inventory,
    workbook_presentation_fingerprint,  # noqa: F401 - compatibility alias for schema.tools imports
)
from .tools_items import (
    _ambiguous_labels,
    _describe_item_suggestions,  # noqa: F401 - compatibility alias for schema.tools imports
    _format_context_label,
    _format_find_context,
    _format_unknown_id_error,  # noqa: F401 - compatibility alias for schema.tools imports
    _item_locations,
    _label_key,
    _parent_headers,
    _sample_values,
    _suggest_items,
    _unknown_item_error,
)
from .tools_sensitivity import (
    _SCENARIO_DIVERGENCE_ABS_LIMIT,  # noqa: F401 - compatibility alias for schema.tools imports
    _SENSITIVITY_CANDIDATE_FILTERS,  # noqa: F401 - compatibility alias for schema.tools imports
    _SENSITIVITY_DEFAULT_MAX_CANDIDATES_QUARTERLY,  # noqa: F401 - compatibility alias for schema.tools imports
    _SENSITIVITY_DRIVER_FORMULA_TYPES,  # noqa: F401 - compatibility alias for schema.tools imports
    _SENSITIVITY_MODES,  # noqa: F401 - compatibility alias for schema.tools imports
    _SENSITIVITY_RECOMPUTE_POLICIES,
    _build_ref_alias_groups,
    _candidate_distances_to_target,  # noqa: F401 - compatibility alias for schema.tools imports
    _candidate_formula_type,  # noqa: F401 - compatibility alias for schema.tools imports
    _collapse_alias_candidates,
    _compute_scenario_results,
    _dedupe_sensitivity_impacts,
    _fallback_formula,  # noqa: F401 - compatibility alias for schema.tools imports
    _filter_sensitivity_candidates,
    _float_close,  # noqa: F401 - compatibility alias for schema.tools imports
    _impacts_equivalent,  # noqa: F401 - compatibility alias for schema.tools imports
    _merge_period_results,  # noqa: F401 - compatibility alias for schema.tools imports
    _normalize_candidate_ids,
    _period_is_unstable,  # noqa: F401 - compatibility alias for schema.tools imports
    _promote_projected_fallbacks,
    _rank_candidates_for_sensitivity,
    _resolve_candidate_filter,
    _resolve_max_candidates,
    _resolve_sensitivity_semantics,
    _same_period_ref_source,  # noqa: F401 - compatibility alias for schema.tools imports
    _scenario_recompute_ids,
    _sensitivity_representative_rank,  # noqa: F401 - compatibility alias for schema.tools imports
)
from .tools_summary import (
    _KEY_METRIC_PATTERNS,  # noqa: F401 - compatibility alias for schema.tools imports
    _find_key_metrics,
    _formula_type,  # noqa: F401 - compatibility alias for schema.tools imports
    _summarize_line_items,
)


@dataclass
class _ModelBundle:
    model: FinancialModel
    graph: DependencyGraph
    base_results: Dict[str, Dict[int, float]]
    all_items: List[LineItem]
    derived_ids: Set[str]
    source: str
    file_signature: Tuple[int, int] | None


_cache: Dict[Tuple[str, int], _ModelBundle] = {}

class ModelToolError(ValueError):
    """Structured agent-facing error for model-tool recovery."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Optional[Dict[str, Any]] = None,
        recovery: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.recovery = recovery or {}

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": "error",
            "error": self.message,
            "error_type": type(self).__name__,
            "error_code": self.code,
        }
        if self.details:
            payload["details"] = self.details
        if self.recovery:
            payload["recovery"] = self.recovery
        return payload


def model_tool_error_payload(exc: Exception) -> Dict[str, Any]:
    if isinstance(exc, ModelToolError):
        return exc.to_payload()
    return {
        "status": "error",
        "error": str(exc),
        "error_type": type(exc).__name__,
    }


def clear_cache(*, disk: bool = False) -> None:
    _cache.clear()
    if disk:
        serialization.clear_disk()


def _file_signature(file_path: str) -> Tuple[int, int] | None:
    try:
        stat = Path(file_path).stat()
    except OSError:
        return None
    return (stat.st_mtime_ns, stat.st_size)


def load(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
    persist: bool = False,
) -> _ModelBundle:
    """Load a model bundle, optionally persisting an explicit file-backed model.

    Passing ``model`` warms the in-memory cache with canonical schema state. Set
    ``persist=True`` only after the corresponding workbook bytes have been
    written to ``file_path`` so future MCP processes can reload that canonical
    state instead of falling back to the lossy Excel reader.
    """

    cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
    key = (file_path, cutoff)
    signature = _file_signature(file_path)

    if model is None and key in _cache:
        cached = _cache[key]
        if cached.file_signature == signature:
            return cached
        _cache.pop(key, None)

    cached_base_results = None
    sidecar_needs_refresh = False
    model_source = "explicit" if model is not None else "unknown"
    if model is None:
        sidecar_hit = serialization.try_load_sidecar(file_path)
        if sidecar_hit is not None:
            model, cached_base_results = sidecar_hit
            sidecar_needs_refresh = cached_base_results is None
            model_source = "sidecar_recomputed" if sidecar_needs_refresh else "sidecar"
        else:
            disk_hit = serialization.try_load(file_path, cutoff)
            if disk_hit is not None:
                model, cached_base_results = disk_hit
                model_source = "disk_cache"

    parsed_fresh = False
    if model is None:
        loaded = read_model(file_path, mode="full", historical_cutoff_year=cutoff)
        if not isinstance(loaded, FinancialModel):
            raise TypeError("read_model(..., mode='full') did not return FinancialModel")
        model = loaded
        parsed_fresh = True
        model_source = "parsed_workbook"

    graph = DependencyGraph()
    graph.build(model)

    all_items = list(model._index.values())
    derived_ids = {item.id for item in all_items if item.item_type == ItemType.derived}
    if cached_base_results is not None:
        base_results = cached_base_results
    else:
        base_results = graph.compute({}, recompute=derived_ids)

    if parsed_fresh or sidecar_needs_refresh or (persist and model is not None):
        serialization.save(file_path, cutoff, model, base_results)
    if sidecar_needs_refresh or (persist and model is not None):
        serialization.save_sidecar(file_path, model, base_results)

    bundle = _ModelBundle(
        model=model,
        graph=graph,
        base_results=base_results,
        all_items=all_items,
        derived_ids=derived_ids,
        source=model_source,
        file_signature=_file_signature(file_path),
    )
    _cache[key] = bundle
    return bundle


def summarize(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
    include_items: bool = False,
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    model_obj = bundle.model
    all_periods = _all_periods(model_obj)
    default_period = _default_period(model_obj) if all_periods else None

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

    workbook_inventory = _workbook_inventory(file_path, sheets_summary, model_source=bundle.source)
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

    valuation_input_readiness = compute_valuation_input_readiness(
        model_obj,
        computed_values=bundle.base_results,
    )
    result = {
        "sheets": sheets_summary,
        **workbook_inventory,
        "line_item_count": len(bundle.all_items),
        "time_range": {
            "historical_periods": _historical_periods(model_obj),
            "projection_periods": _projection_periods(model_obj),
            "all_periods": all_periods,
            "default_period": default_period,
        },
        "key_metrics": key_metrics,
        "projection_readiness": compute_model_projection_readiness(
            model_obj,
            computed_values=bundle.base_results,
        ).model_dump(mode="json"),
        "scenario_output_readiness": compute_model_scenario_output_readiness(
            model_obj,
            computed_values=bundle.base_results,
        ).model_dump(mode="json"),
        "scenario_bridge_readiness": compute_model_scenario_bridge_readiness(
            model_obj,
        ).model_dump(mode="json"),
        "model_quality_readiness": compute_model_quality_readiness(
            model_obj,
            computed_values=bundle.base_results,
            valuation_input_readiness=valuation_input_readiness,
        ).model_dump(mode="json"),
    }
    if include_items:
        result["items"] = _summarize_line_items(model_obj, bundle.all_items)
    return result


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
    periods: str | List[str | int] = "all",
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

    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    period_list, period_label = _resolve_period_list(periods, bundle.model)
    _validate_values_response_size(
        deduped_item_ids,
        period_list,
        periods=periods,
        model=bundle.model,
    )

    rows = []
    for item_id in deduped_item_ids:
        try:
            item = bundle.model.get_item(item_id)
        except KeyError:
            suggestions = _suggest_items(bundle.model._index, item_id)
            structured_error = _unknown_item_error(bundle.model, item_id, "item_id")
            error_row = {
                "id": item_id,
                "error": structured_error.message,
                "error_code": structured_error.code,
            }
            if suggestions:
                error_row["suggestions"] = suggestions
                error_row["suggestion_details"] = structured_error.details.get("suggestions", [])
            error_row["recovery"] = structured_error.recovery
            rows.append(error_row)
            continue

        item_values = bundle.base_results.get(item_id, {})
        value_periods = period_list
        if item.column is not None:
            anchor_period = _fixed_cell_anchor_period(bundle.model, item)
            value_periods = [anchor_period] if anchor_period in set(period_list) else []
        rows.append(
            {
                "id": item.id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "values": {
                    period: _fixed_cell_value(item, item_values, period)
                    for period in value_periods
                },
            }
        )

    return {
        "items": rows,
        "periods_returned": period_label,
        "period_count": len(period_list),
    }


def _fixed_cell_value(item: LineItem, computed_values: Dict[int, float], period: int) -> Optional[float]:
    value = computed_values.get(period)
    if value is not None or item.column is None:
        return value
    if item.values is None:
        return None
    for value_period in sorted(item.values.values):
        value_cell = item.values.values[value_period]
        if value_cell.value is not None:
            return value_cell.value
    return None


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
        raise _unknown_item_error(bundle.model, item_id, "item_id")

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
    max_candidates: Optional[int] = None,
    candidate_ids: Optional[Iterable[str]] = None,
    sensitivity_mode: Optional[
        Literal["workbook_explicit", "workbook_global", "legacy_global"]
    ] = None,
    recompute_policy: Literal["projection_safe", "legacy_global"] = "projection_safe",
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    if target_id not in bundle.model._index:
        raise _unknown_item_error(bundle.model, target_id, "target_id")

    projection_periods = _projection_periods(bundle.model)
    if not projection_periods:
        projection_periods = [_default_period(bundle.model)]
    target_period = projection_periods[-1]
    base_target_value = bundle.base_results.get(target_id, {}).get(target_period)

    candidate_filter = _resolve_candidate_filter(candidate_filter)
    explicit_candidate_ids = _normalize_candidate_ids(candidate_ids)
    sensitivity_mode, recompute_policy = _resolve_sensitivity_semantics(
        explicit_candidate_ids,
        sensitivity_mode,
        recompute_policy,
    )
    if recompute_policy == "legacy_global":
        upstream = _upstream_nodes(bundle.graph, target_id)
    else:
        upstream = bundle.graph.upstream_for_periods([target_id], projection_periods)
    upstream.discard(target_id)
    filtered_upstream = _filter_sensitivity_candidates(bundle.model, upstream, candidate_filter)
    alias_group_by_id = _build_ref_alias_groups(bundle.model)
    item_locations = _item_locations(bundle.model)
    ambiguous_labels = _ambiguous_labels(bundle.all_items)
    candidate_scope = (
        "explicit_workbook_ids"
        if explicit_candidate_ids is not None
        else "legacy_global"
        if sensitivity_mode == "legacy_global"
        else "global_ranked"
    )

    def build_row(
        candidate_id: str,
        *,
        computation_status: str,
        scenario_target_value: Optional[float],
        delta: Optional[float],
        pct_change: Optional[float],
        leverage_ratio: Optional[float],
        high_leverage: bool,
        abs_impact: float,
        status_reason: Optional[str] = None,
        shock_periods: Optional[List[int]] = None,
        impact_per_unit: Optional[float] = None,
        impact_basis: Optional[str] = None,
    ) -> Dict[str, Any]:
        item = bundle.model.get_item(candidate_id)
        context = item_locations.get(candidate_id)
        context_label = _format_context_label(context)
        display_label = item.label
        if _label_key(item.label) in ambiguous_labels:
            if context_label:
                display_label = f"{item.label} ({context_label})"
            else:
                display_label = f"{item.label} ({candidate_id})"
        return {
            "id": candidate_id,
            "shocked_item_id": candidate_id,
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
            "target_period": target_period,
            "bump_pct": bump_pct,
            "delta": delta,
            "pct_change": pct_change,
            "leverage_ratio": leverage_ratio,
            "impact_per_unit": impact_per_unit,
            "impact_basis": impact_basis or f"target_delta_for_{bump_pct:.6g}_relative_bump",
            "high_leverage": high_leverage,
            "abs_impact": abs_impact,
            "rank": None,
            "computation_status": computation_status,
            "status_reason": status_reason,
            "sensitivity_mode": sensitivity_mode,
            "recompute_policy": recompute_policy,
            "candidate_scope": candidate_scope,
            "shock_periods": shock_periods or [],
            "readout_item_ids": [target_id],
            "pinned_ids": [],
            "warnings": [],
        }

    if explicit_candidate_ids is not None:
        for candidate_id in explicit_candidate_ids:
            if candidate_id not in bundle.model._index:
                raise _unknown_item_error(bundle.model, candidate_id, "candidate_id")
        candidates = list(explicit_candidate_ids)
        precollapsed_aliases: Dict[str, List[str]] = {}
    else:
        candidates = sorted(filtered_upstream)
        candidates, precollapsed_aliases = _collapse_alias_candidates(
            candidates,
            bundle.model,
            alias_group_by_id,
        )
    candidate_count_total = len(explicit_candidate_ids) if explicit_candidate_ids is not None else len(candidates)
    selected_max_candidates = (
        None
        if explicit_candidate_ids is not None
        else _resolve_max_candidates(bundle.model, candidate_filter, max_candidates)
    )
    if selected_max_candidates is not None and len(candidates) > selected_max_candidates:
        candidates = _rank_candidates_for_sensitivity(candidates, bundle.model, bundle.graph, target_id)
        candidates = candidates[:selected_max_candidates]
    candidate_count_evaluated = len(candidates)

    recompute_cache = (
        {candidate_id: _downstream_nodes(bundle.graph, candidate_id) for candidate_id in candidates}
        if recompute_policy == "legacy_global"
        else {}
    )
    impacts = []
    for candidate_id in candidates:
        if candidate_id not in upstream:
            impacts.append(
                build_row(
                    candidate_id,
                    computation_status="not_upstream",
                    scenario_target_value=None,
                    delta=None,
                    pct_change=None,
                    leverage_ratio=None,
                    high_leverage=False,
                    abs_impact=-1.0,
                    status_reason="candidate is not upstream of target under the selected sensitivity semantics",
                    impact_basis="not_computed",
                )
            )
            continue

        base_candidate_values = bundle.base_results.get(candidate_id, {})
        bumped_values: Dict[int, float] = {}
        for period in projection_periods:
            base_val = base_candidate_values.get(period)
            if base_val is None:
                continue
            bumped_values[period] = base_val * (1.0 + bump_pct)
        if not bumped_values:
            if explicit_candidate_ids is not None:
                impacts.append(
                    build_row(
                        candidate_id,
                        computation_status="unavailable",
                        scenario_target_value=None,
                        delta=None,
                        pct_change=None,
                        leverage_ratio=None,
                        high_leverage=False,
                        abs_impact=-1.0,
                        status_reason="candidate has no numeric base values in projection periods",
                        impact_basis="not_computed",
                    )
                )
            continue

        scenario_inputs = {candidate_id: bumped_values}
        active_periods = {period for period in projection_periods if period >= min(bumped_values)}
        if recompute_policy == "legacy_global":
            recompute_ids = recompute_cache[candidate_id]
            promoted_projected = _promote_projected_fallbacks(bundle.model, recompute_ids)
            compute_kwargs: Dict[str, Any] = {}
        else:
            recompute_ids = bundle.graph.downstream_for_periods([candidate_id], active_periods)
            promoted_projected = {}
            compute_kwargs = {"propagate_roots": set()}
        try:
            scenario_results = bundle.graph.compute(
                scenario_inputs,
                recompute=recompute_ids,
                cycle_fallback_policy="auto_propagate",
                periods=active_periods,
                seed_results=bundle.base_results,
                **compute_kwargs,
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

        if delta is None:
            impacts.append(
                build_row(
                    candidate_id,
                    computation_status="unavailable",
                    scenario_target_value=scenario_target_value,
                    delta=None,
                    pct_change=None,
                    leverage_ratio=None,
                    high_leverage=False,
                    abs_impact=-1.0,
                    status_reason="target impact could not be computed for the selected scenario",
                    impact_basis="not_computed",
                    shock_periods=sorted(bumped_values),
                )
            )
            continue

        impact_per_unit = None
        candidate_base_at_target = base_candidate_values.get(target_period)
        candidate_scenario_at_target = bumped_values.get(target_period)
        if (
            delta is not None
            and candidate_base_at_target is not None
            and candidate_scenario_at_target is not None
            and candidate_scenario_at_target != candidate_base_at_target
        ):
            impact_per_unit = delta / (candidate_scenario_at_target - candidate_base_at_target)
        impacts.append(
            build_row(
                candidate_id,
                computation_status="computed",
                scenario_target_value=scenario_target_value,
                delta=delta,
                pct_change=pct_change,
                leverage_ratio=leverage_ratio,
                impact_per_unit=impact_per_unit,
                high_leverage=high_leverage,
                abs_impact=abs_impact,
                shock_periods=sorted(bumped_values),
            )
        )

    if explicit_candidate_ids is None:
        impacts = _dedupe_sensitivity_impacts(impacts, bundle.model, alias_group_by_id)
        for row in impacts:
            aliases = set(row.get("alias_ids", []))
            aliases.update(precollapsed_aliases.get(row["id"], []))
            if aliases:
                row["alias_ids"] = sorted(aliases)
    impacts.sort(key=lambda row: row["abs_impact"], reverse=True)
    top_impacts = impacts if explicit_candidate_ids is not None else impacts[: max(n, 0)]
    rank = 0
    for row in top_impacts:
        if row.get("computation_status") != "computed":
            row["rank"] = None
            continue
        rank += 1
        row["rank"] = rank
    high_leverage_count = sum(1 for row in top_impacts if row.get("high_leverage"))
    return {
        "target_id": target_id,
        "target_period": target_period,
        "bump_pct": bump_pct,
        "sensitivity_mode": sensitivity_mode,
        "recompute_policy": recompute_policy,
        "candidate_scope": candidate_scope,
        "candidate_filter": candidate_filter,
        "candidate_count_total": candidate_count_total,
        "candidate_count_evaluated": candidate_count_evaluated,
        "candidate_count_computed": sum(1 for row in top_impacts if row.get("computation_status") == "computed"),
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
    recompute_policy: Literal["projection_safe", "legacy_global"] = "projection_safe",
) -> Dict:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    model_obj = bundle.model
    if recompute_policy not in _SENSITIVITY_RECOMPUTE_POLICIES:
        allowed = ", ".join(sorted(_SENSITIVITY_RECOMPUTE_POLICIES))
        raise ValueError(f"recompute_policy must be one of: {allowed}")

    normalized: Dict[str, Dict[int, float]] = {}
    for item_id, values in (overrides or {}).items():
        if item_id not in model_obj._index:
            raise _unknown_item_error(model_obj, item_id, "override item_id")
        normalized[item_id] = {int(period): float(value) for period, value in values.items()}

    if compare_items is None:
        compare_ids = [item.id for item in _find_key_metrics(bundle.all_items)]
    else:
        compare_ids = compare_items
        for item_id in compare_ids:
            if item_id not in model_obj._index:
                raise _unknown_item_error(model_obj, item_id, "compare item_id")

    recompute_ids = _scenario_recompute_ids(bundle, normalized, recompute_policy=recompute_policy)

    scenario_results = _compute_scenario_results(
        bundle,
        normalized,
        recompute_ids,
        recompute_policy=recompute_policy,
    )
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
        "recompute_policy": recompute_policy,
        "overrides": normalized,
        "comparisons": comparisons,
    }


def period_guidance(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> Dict[str, Any]:
    bundle = load(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    return _period_guidance(bundle.model)


def invalid_override_period_error(
    raw_period: object,
    *,
    file_path: Optional[str] = None,
    historical_cutoff_year: Optional[int] = None,
    cause: Optional[str] = None,
) -> ModelToolError:
    details: Dict[str, Any] = {"period_key": raw_period}
    if cause:
        details["cause"] = cause
    if file_path:
        try:
            details["period_guidance"] = period_guidance(
                file_path,
                historical_cutoff_year=historical_cutoff_year,
            )
        except Exception:
            pass
    return ModelToolError(
        "invalid_override_period",
        (
            f"Invalid override period key: {raw_period!r}. "
            "Use fiscal year keys such as 2026 or '2026'; call model_summarize to inspect available periods."
        ),
        details=details,
        recovery={
            "next_actions": [
                "Use model_summarize(file_path=...) and read time_range.projection_periods.",
                "Retry model_scenario with overrides shaped as {item_id: {2026: value, 2027: value}}.",
                "Do not use labels such as FY1, FY2026, terminal, or projection unless you first convert them to concrete period keys.",
            ]
        },
    )


def invalid_override_value_error(
    item_id: str,
    raw_period: object,
    raw_value: object,
    *,
    cause: Optional[str] = None,
) -> ModelToolError:
    details: Dict[str, Any] = {
        "item_id": item_id,
        "period_key": raw_period,
        "value": raw_value,
    }
    if cause:
        details["cause"] = cause
    return ModelToolError(
        "invalid_override_value",
        f"Invalid override value for {item_id}[{raw_period!r}]: {raw_value!r}. Expected a numeric value.",
        details=details,
        recovery={
            "next_actions": [
                "Retry model_scenario with numeric override values only.",
                "Keep explanatory text in the skill artifact or Decisions Log, not inside the override value payload.",
            ]
        },
    )
