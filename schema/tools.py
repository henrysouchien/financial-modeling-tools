"""Lightweight agent-facing tools for schema financial models."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from . import serialization
from .analysis import (
    _default_period,
    _downstream_nodes,  # noqa: F401 - compatibility alias for schema.tools imports
    _upstream_nodes,  # noqa: F401 - compatibility alias for schema.tools imports
)
from .model_readiness import (
    compute_model_quality_readiness,
    compute_model_projection_readiness,
    compute_model_scenario_bridge_readiness,
    compute_model_scenario_output_readiness,
    compute_valuation_input_readiness,
)
from schema.handle import load_handle
from schema.load_core import _ModelBundle
from .models import (
    FinancialModel,
    LineItem,
)
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
    _format_context_label,  # noqa: F401 - compatibility alias for schema.tools imports
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
    _SENSITIVITY_RECOMPUTE_POLICIES,  # noqa: F401 - compatibility alias for schema.tools imports
    _build_ref_alias_groups,  # noqa: F401 - compatibility alias for schema.tools imports
    _candidate_distances_to_target,  # noqa: F401 - compatibility alias for schema.tools imports
    _candidate_formula_type,  # noqa: F401 - compatibility alias for schema.tools imports
    _collapse_alias_candidates,  # noqa: F401 - compatibility alias for schema.tools imports
    _compute_scenario_results,  # noqa: F401 - compatibility alias for schema.tools imports
    _dedupe_sensitivity_impacts,  # noqa: F401 - compatibility alias for schema.tools imports
    _fallback_formula,  # noqa: F401 - compatibility alias for schema.tools imports
    _filter_sensitivity_candidates,  # noqa: F401 - compatibility alias for schema.tools imports
    _float_close,  # noqa: F401 - compatibility alias for schema.tools imports
    _impacts_equivalent,  # noqa: F401 - compatibility alias for schema.tools imports
    _merge_period_results,  # noqa: F401 - compatibility alias for schema.tools imports
    _normalize_candidate_ids,  # noqa: F401 - compatibility alias for schema.tools imports
    _period_is_unstable,  # noqa: F401 - compatibility alias for schema.tools imports
    _promote_projected_fallbacks,  # noqa: F401 - compatibility alias for schema.tools imports
    _rank_candidates_for_sensitivity,  # noqa: F401 - compatibility alias for schema.tools imports
    _resolve_candidate_filter,  # noqa: F401 - compatibility alias for schema.tools imports
    _resolve_max_candidates,  # noqa: F401 - compatibility alias for schema.tools imports
    _resolve_sensitivity_semantics,  # noqa: F401 - compatibility alias for schema.tools imports
    _apply_scenario_case_selection,  # noqa: F401 - compatibility alias for schema.tools imports
    _scenario_case_recompute_ids,  # noqa: F401 - compatibility alias for schema.tools imports
    _scenario_case_selection_for_overrides,  # noqa: F401 - compatibility alias for schema.tools imports
    _same_period_ref_source,  # noqa: F401 - compatibility alias for schema.tools imports
    _scenario_recompute_ids,  # noqa: F401 - compatibility alias for schema.tools imports
    _sensitivity_representative_rank,  # noqa: F401 - compatibility alias for schema.tools imports
)
from .tools_sensitivity_api import sensitivity  # noqa: F401 - public facade for schema.tools
from .tools_scenarios import (
    _NEGATIVE_SCENARIO_OUTPUT_TOKENS,  # noqa: F401 - compatibility alias for schema.tools imports
    _POSITIVE_SCENARIO_OUTPUT_TOKENS,  # noqa: F401 - compatibility alias for schema.tools imports
    _is_positive_scenario_output,  # noqa: F401 - compatibility alias for schema.tools imports
    _scenario_case_direction_warnings,  # noqa: F401 - compatibility alias for schema.tools imports
    run_scenario as _run_scenario,
)
from .tools_summary import (
    _KEY_METRIC_PATTERNS,  # noqa: F401 - compatibility alias for schema.tools imports
    _find_key_metrics,
    _formula_type,  # noqa: F401 - compatibility alias for schema.tools imports
    _summarize_line_items,
)
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
    payload = {
        "status": "error",
        "error": str(exc),
        "error_type": type(exc).__name__,
    }
    error_code = getattr(exc, "error_code", None)
    if error_code:
        payload["error_code"] = str(error_code)
    model_quality_readiness = getattr(exc, "model_quality_readiness", None)
    if model_quality_readiness is not None:
        if hasattr(model_quality_readiness, "to_dict"):
            payload["model_quality_readiness"] = model_quality_readiness.to_dict()
        elif hasattr(model_quality_readiness, "model_dump"):
            payload["model_quality_readiness"] = model_quality_readiness.model_dump(mode="json")
        else:
            payload["model_quality_readiness"] = model_quality_readiness
    return payload


def clear_cache(*, disk: bool = False) -> None:
    from schema.handle import _handle_memo

    _handle_memo.clear()
    if disk:
        serialization.clear_disk()


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

    return load_handle(
        file_path,
        model=model,
        historical_cutoff_year=historical_cutoff_year,
        persist=persist,
    ).to_bundle()


def summarize(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
    include_items: bool = False,
) -> Dict:
    handle = load_handle(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    model_obj = handle.model
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

    workbook_inventory = _workbook_inventory(file_path, sheets_summary, model_source=handle.source)
    key_metrics = []
    for item in _find_key_metrics(handle.all_items):
        values = handle.computed.get(item.id, {})
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
        computed_values=handle.computed,
    )
    model_quality_readiness = compute_model_quality_readiness(
        model_obj,
        computed_values=handle.computed,
        valuation_input_readiness=valuation_input_readiness,
    ).model_dump(mode="json")
    result = {
        "sheets": sheets_summary,
        **workbook_inventory,
        "line_item_count": len(handle.all_items),
        "time_range": {
            "historical_periods": _historical_periods(model_obj),
            "projection_periods": _projection_periods(model_obj),
            "all_periods": all_periods,
            "default_period": default_period,
        },
        "key_metrics": key_metrics,
        "projection_readiness": compute_model_projection_readiness(
            model_obj,
            computed_values=handle.computed,
        ).model_dump(mode="json"),
        "scenario_output_readiness": compute_model_scenario_output_readiness(
            model_obj,
            computed_values=handle.computed,
        ).model_dump(mode="json"),
        "scenario_bridge_readiness": compute_model_scenario_bridge_readiness(
            model_obj,
        ).model_dump(mode="json"),
        "model_quality_readiness": model_quality_readiness,
    }
    if include_items:
        result["items"] = _summarize_line_items(model_obj, handle.all_items)
    return result


def find(
    file_path: str,
    query: str,
    limit: int = 20,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> List[Dict]:
    handle = load_handle(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    if not query:
        return []

    all_periods = _all_periods(handle.model)
    needle = query.lower()
    item_locs = _item_locations(handle.model)
    parent_headers = _parent_headers(handle.model)
    ambiguous = _ambiguous_labels(handle.all_items)
    rows = []
    for item in handle.all_items:
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
                "sample_values": _sample_values(handle.computed.get(item.id, {}), all_periods),
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

    handle = load_handle(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    period_list, period_label = _resolve_period_list(periods, handle.model)
    _validate_values_response_size(
        deduped_item_ids,
        period_list,
        periods=periods,
        model=handle.model,
    )

    rows = []
    for item_id in deduped_item_ids:
        try:
            item = handle.model.get_item(item_id)
        except KeyError:
            suggestions = _suggest_items(handle.model._index, item_id)
            structured_error = _unknown_item_error(handle.model, item_id, "item_id")
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

        item_values = handle.computed.get(item_id, {})
        value_periods = period_list
        if item.column is not None:
            anchor_period = _fixed_cell_anchor_period(handle.model, item)
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
    handle = load_handle(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    if item_id not in handle.model._index:
        raise _unknown_item_error(handle.model, item_id, "item_id")

    all_periods = _all_periods(handle.model)
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

        for dep_id in handle.graph.get_dependencies(node_id):
            edge_set.add((dep_id, node_id, 0))
            stack.append((dep_id, current_depth + 1))

        for ref in handle.graph.time_edges.get(node_id, set()):
            if ref.t == 0:
                continue
            edge_set.add((ref.id, node_id, ref.t))
            stack.append((ref.id, current_depth + 1))

    nodes = []
    for node_id, node_dist in sorted(node_depth.items(), key=lambda x: (x[1], x[0])):
        item = handle.model.get_item(node_id)
        nodes.append(
            {
                "id": node_id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": _formula_type(item),
                "distance": node_dist,
                "sample_values": _sample_values(handle.computed.get(node_id, {}), all_periods),
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


def scenario(
    file_path: str,
    overrides: Dict[str, Dict[int, float]],
    compare_items: Optional[List[str]] = None,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
    recompute_policy: Literal["projection_safe", "legacy_global"] = "projection_safe",
) -> Dict:
    handle = load_handle(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    return _run_scenario(
        handle,
        overrides,
        compare_items,
        recompute_policy=recompute_policy,
    )


def period_guidance(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    historical_cutoff_year: Optional[int] = None,
) -> Dict[str, Any]:
    handle = load_handle(file_path, model=model, historical_cutoff_year=historical_cutoff_year)
    return _period_guidance(handle.model)


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
