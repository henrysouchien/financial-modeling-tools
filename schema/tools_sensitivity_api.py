"""Agent-facing sensitivity entrypoints for schema model tools.

``sensitivity`` ranks workbook items by a bumped target. ``judgment_analysis``
exposes the Engine's derived views over one compiled ModelVersion —
``sensitize`` over the judgment rows its cells cite, ``compare`` against the
SourceBundle's References and consensus Claims, ``imply`` at the pinned price.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional

from .analysis import (
    DCF_PRICE_ITEM_ID,
    _default_period,
    _downstream_nodes,
    _upstream_nodes,
    compare,
    compare_consensus,
    imply,
    judgment_drivers,
    sensitize,
)
from .collected_claims import Claim, Reference
from .handle import load_handle
from .models import FinancialModel
from .tools_items import (
    _ambiguous_labels,
    _format_context_label,
    _item_locations,
    _label_key,
    _unknown_item_error,
)
from .tools_periods import _projection_periods
from .tools_sensitivity import (
    SensitivityImpactRow,
    _build_ref_alias_groups,
    _collapse_alias_candidates,
    _dedupe_sensitivity_impacts,
    _filter_sensitivity_candidates,
    _normalize_candidate_ids,
    _promote_projected_fallbacks,
    _rank_candidates_for_sensitivity,
    _resolve_candidate_filter,
    _resolve_max_candidates,
    _resolve_sensitivity_semantics,
)
from .tools_summary import _formula_type

if TYPE_CHECKING:
    from .build_model_product_contracts import ProvenanceMap
    from .thesis_shared_slice import Assumption


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
    refresh_stale_sidecar: bool = True,
    use_cache: bool = True,
) -> Dict:
    handle = load_handle(
        file_path,
        model=model,
        historical_cutoff_year=historical_cutoff_year,
        refresh_stale_sidecar=refresh_stale_sidecar,
        use_cache=use_cache,
    )
    if target_id not in handle.model._index:
        raise _unknown_item_error(handle.model, target_id, "target_id")

    projection_periods = _projection_periods(handle.model)
    if not projection_periods:
        projection_periods = [_default_period(handle.model)]
    target_period = projection_periods[-1]
    base_target_value = handle.computed.get(target_id, {}).get(target_period)

    resolved_candidate_filter = _resolve_candidate_filter(candidate_filter)
    explicit_candidate_ids = _normalize_candidate_ids(candidate_ids)
    resolved_sensitivity_mode, resolved_recompute_policy = _resolve_sensitivity_semantics(
        explicit_candidate_ids,
        sensitivity_mode,
        recompute_policy,
    )
    if resolved_recompute_policy == "legacy_global":
        upstream = _upstream_nodes(handle.graph, target_id)
    else:
        upstream = handle.graph.upstream_for_periods([target_id], projection_periods)
    upstream.discard(target_id)
    filtered_upstream = _filter_sensitivity_candidates(
        handle.model,
        upstream,
        resolved_candidate_filter,
    )
    alias_group_by_id = _build_ref_alias_groups(handle.model)
    item_locations = _item_locations(handle.model)
    ambiguous_labels = _ambiguous_labels(handle.all_items)
    candidate_scope = (
        "explicit_workbook_ids"
        if explicit_candidate_ids is not None
        else "legacy_global"
        if resolved_sensitivity_mode == "legacy_global"
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
    ) -> SensitivityImpactRow:
        item = handle.model.get_item(candidate_id)
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
            "sensitivity_mode": resolved_sensitivity_mode,
            "recompute_policy": resolved_recompute_policy,
            "candidate_scope": candidate_scope,
            "shock_periods": shock_periods or [],
            "readout_item_ids": [target_id],
            "pinned_ids": [],
            "warnings": [],
        }

    if explicit_candidate_ids is not None:
        for candidate_id in explicit_candidate_ids:
            if candidate_id not in handle.model._index:
                raise _unknown_item_error(handle.model, candidate_id, "candidate_id")
        candidates = list(explicit_candidate_ids)
        precollapsed_aliases: Dict[str, List[str]] = {}
    else:
        candidates = sorted(filtered_upstream)
        candidates, precollapsed_aliases = _collapse_alias_candidates(
            candidates,
            handle.model,
            alias_group_by_id,
        )
    candidate_count_total = len(explicit_candidate_ids) if explicit_candidate_ids is not None else len(candidates)
    selected_max_candidates = (
        None
        if explicit_candidate_ids is not None
        else _resolve_max_candidates(handle.model, resolved_candidate_filter, max_candidates)
    )
    if selected_max_candidates is not None and len(candidates) > selected_max_candidates:
        candidates = _rank_candidates_for_sensitivity(candidates, handle.model, handle.graph, target_id)
        candidates = candidates[:selected_max_candidates]
    candidate_count_evaluated = len(candidates)

    recompute_cache = (
        {candidate_id: _downstream_nodes(handle.graph, candidate_id) for candidate_id in candidates}
        if resolved_recompute_policy == "legacy_global"
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

        base_candidate_values = handle.computed.get(candidate_id, {})
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
        if resolved_recompute_policy == "legacy_global":
            recompute_ids = recompute_cache[candidate_id]
            promoted_projected = _promote_projected_fallbacks(handle.model, recompute_ids)
            compute_kwargs: Dict[str, Any] = {}
        else:
            recompute_ids = handle.graph.downstream_for_periods([candidate_id], active_periods)
            promoted_projected = {}
            compute_kwargs = {"propagate_roots": set()}
        try:
            scenario_results = handle.graph.compute(
                scenario_inputs,
                recompute=recompute_ids,
                cycle_fallback_policy="auto_propagate",
                periods=active_periods,
                seed_results=handle.computed,
                **compute_kwargs,
            )
        finally:
            for item_id, original_projected in promoted_projected.items():
                handle.model.get_item(item_id).projected = original_projected
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
        impacts = _dedupe_sensitivity_impacts(impacts, handle.model, alias_group_by_id)
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
        "sensitivity_mode": resolved_sensitivity_mode,
        "recompute_policy": resolved_recompute_policy,
        "candidate_scope": candidate_scope,
        "candidate_filter": resolved_candidate_filter,
        "candidate_count_total": candidate_count_total,
        "candidate_count_evaluated": candidate_count_evaluated,
        "candidate_count_computed": sum(1 for row in top_impacts if row.get("computation_status") == "computed"),
        "max_candidates": selected_max_candidates,
        "base_value": base_target_value,
        "high_leverage_count": high_leverage_count,
        "results": top_impacts,
    }


@dataclass(frozen=True)
class JudgmentBasis:
    """One compiled ModelVersion's model, cell lineage, judgment rows and References."""

    model: FinancialModel
    provenance: "ProvenanceMap"
    forecast_assumptions: Sequence["Assumption"]
    claims: Sequence[Claim]
    references: Sequence[Reference]


JudgmentView = Literal["judgments", "compare", "imply"]


def judgment_analysis(
    view: JudgmentView,
    *,
    basis: JudgmentBasis,
    target_id: Optional[str] = None,
    bump_pct: float = 0.01,
) -> Dict[str, Any]:
    """Compute one derived view over the compiled model, recomputed on read.

    ``judgments`` ranks every cited judgment row by the target's response
    (default target: the compiled DCF value per share); ``compare`` holds the
    model against the pinned price Reference and every consensus Claim;
    ``imply`` states the pinned price as the value of each row alone.
    """

    model = basis.model.model_copy(deep=True)
    target = target_id or DCF_PRICE_ITEM_ID
    if view == "compare":
        prices = [reference for reference in basis.references if reference.kind == "price"]
        variances = [compare(model, reference) for reference in prices]
        variances.extend(compare_consensus(model, basis.claims))
        return {
            "view": view,
            "variances": [row.model_dump(mode="json") for row in variances],
        }
    drivers = judgment_drivers(
        model,
        basis.provenance,
        forecast_assumption_drivers={
            str(row.assumption_id): row.driver
            for row in basis.forecast_assumptions
            if row.assumption_id
        },
    )
    if view == "judgments":
        rows = sensitize(model, drivers, target_id=target, bump_pct=bump_pct)
        return {
            "view": view,
            "target_id": target,
            "sensitivities": [row.model_dump(mode="json") for row in rows],
        }
    if view == "imply":
        prices = [reference for reference in basis.references if reference.kind == "price"]
        if len(prices) != 1:
            raise ValueError(
                f"imply needs exactly one pinned price Reference; the SourceBundle has {len(prices)}"
            )
        implied = imply(prices[0], model, drivers, target_id=target)
        return {"view": view, "implied": implied.model_dump(mode="json")}
    raise ValueError(f"unknown judgment view {view!r}; use judgments, compare, or imply")


__all__ = ["JudgmentBasis", "JudgmentView", "judgment_analysis", "sensitivity"]
