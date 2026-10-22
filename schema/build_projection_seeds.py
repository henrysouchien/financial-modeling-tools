"""Compile authenticated stable-ID control snapshots into model inputs.

This module owns the one renderer projection for forecast and scenario control
state.  It accepts only :class:`CanonicalProjectionCurve` values derived from
the exact current canonical control-snapshot heads.  The retired
``TickerOverrides.projections`` JSON shape is neither imported nor decoded.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from .control_snapshot_projection import (
    AUTHENTICATED_SCENARIO_PROJECTION_NOTE,
    CanonicalProjectionCurve,
    canonical_projection_render_target,
    validate_canonical_projection_target_uniqueness,
)
from .handle import ComputedValues
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItemRef,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .refs import line_item_ref_from_obj

if TYPE_CHECKING:
    from .business_model_revision import BusinessModelRevision
    from .business_model_compiler import CompiledDriverRegistry


@dataclass(frozen=True, slots=True)
class ControlSnapshotProjectionResult:
    """Deterministic renderer projection of exact canonical control state."""

    projected_curve_count: int = 0
    available_curve_count: int = 0
    coverage: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "projected_curve_count": self.projected_curve_count,
            "available_curve_count": self.available_curve_count,
            "coverage": list(self.coverage),
            "source_authority": "canonical_control_snapshot",
        }


@dataclass(frozen=True, slots=True)
class CandidateModelCompileResult:
    """Rebuild-equivalent compile against exact stable-ID control curves."""

    model: FinancialModel
    control_snapshot_projection: ControlSnapshotProjectionResult
    computed_values: ComputedValues


def _require_canonical_curve_order(
    curves: tuple[CanonicalProjectionCurve, ...] | None,
    *,
    field_name: str,
) -> tuple[CanonicalProjectionCurve, ...]:
    """Hold a curve tuple to the canonical form the persisted artifact assumes.

    SchemaAudit PS-2 is a KEEP: sortedness and uniqueness are a determinism and
    data-integrity invariant on the canonical form, not a construction-pedigree
    re-proof.  PS-1 -- the exact-container/exact-class pin that used to sit
    beside it -- is retired under the B6 ruling and is deliberately not
    reinstated here.
    """

    if curves is None:
        return ()
    keys = tuple(
        (
            item.template_item_id.encode("utf-8"),
            item.domain,
            item.scenario.encode("utf-8"),
            item.control_id,
        )
        for item in curves
    )
    if keys != tuple(sorted(set(keys))):
        raise ValueError(f"{field_name} must be canonically sorted and unique")
    return curves


def _remove_control_snapshot_effects(
    model: FinancialModel,
    curves: tuple[CanonicalProjectionCurve, ...],
) -> None:
    """Remove only exact cells previously owned by canonical control curves."""

    for curve in curves:
        target_id = canonical_projection_render_target(model, curve)
        try:
            item = model.get_item(target_id)
        except KeyError as exc:
            raise ValueError(
                "prior canonical control binding targets an absent template item: "
                f"{target_id}"
            ) from exc
        if curve.domain == "scenario":
            if item.values is None:
                continue
            for year, _value in curve.values:
                item.values.values.pop(year, None)
            if not item.values.values:
                item.values = None
            continue
        if item.overrides is not None:
            for year, _value in curve.values:
                item.overrides.pop(year, None)
            if not item.overrides:
                item.overrides = None


def _ref_offsets(params: Any) -> tuple[int, ...]:
    offsets: list[int] = []

    def walk(value: Any) -> None:
        if isinstance(value, LineItemRef):
            offsets.append(int(value.t or 0))
            return
        if isinstance(value, dict):
            item_id = value.get("id")
            if isinstance(item_id, str) and set(value).issubset(
                {"id", "t", "resolved", "period_anchor"}
            ):
                try:
                    offsets.append(int(value.get("t") or 0))
                except (TypeError, ValueError):
                    return
                return
            for nested in value.values():
                walk(nested)
            return
        if isinstance(value, (list, tuple)):
            for nested in value:
                walk(nested)

    walk(params)
    return tuple(offsets)


def _scenario_historical_formula_periods(
    model: FinancialModel,
    item: Any,
    most_recent_fy: int,
) -> set[int]:
    """Historical years a scenario control row may still formula-cover.

    A cell_ref (t=0) to a growth row covers only the years that row covers.
    A referenced formula that reads a prior period cannot cover the first
    axis year when that prior period is off the axis.
    """

    historical_periods = {
        int(period)
        for period in item.formula_periods
        if int(period) <= most_recent_fy
    }
    historical = item.historical
    if historical is None or historical.type is not FormulaType.ref:
        return historical_periods
    source = line_item_ref_from_obj((historical.params or {}).get("source"))
    if source is None or int(source.t or 0) != 0:
        return historical_periods
    if not model._index:
        model.build_index()
    target_item = model._index.get(source.id)
    if target_item is None or target_item.formula_periods is None:
        return historical_periods
    historical_periods &= {int(period) for period in target_item.formula_periods}
    target_historical = target_item.historical
    if target_historical is None or not any(
        offset < 0 for offset in _ref_offsets(target_historical.params)
    ):
        return historical_periods
    axis = model.time_structure
    axis_historical = [int(period) for period in (axis.historical_periods or ())]
    if not axis_historical:
        return historical_periods
    first = min(axis_historical)
    on_axis = set(axis_historical) | {
        int(period) for period in (axis.projection_periods or ())
    }
    if (first - 1) not in on_axis:
        historical_periods.discard(first)
    return historical_periods


def _project_control_snapshots(
    model: FinancialModel,
    curves: tuple[CanonicalProjectionCurve, ...],
    most_recent_fy: int,
) -> ControlSnapshotProjectionResult:
    """Seed exact canonical stable-ID curves into their bound workbook targets."""

    # The exact-container/exact-class pin is retired (B6 ruling, 2026-08-22);
    # the canonical-form invariant (PS-2) is a KEEP and still holds here.
    exact_curves = _require_canonical_curve_order(curves, field_name="curves")
    validate_canonical_projection_target_uniqueness(model, exact_curves)
    coverage: list[dict[str, Any]] = []
    seeded_count = 0
    scenario_targets: dict[str, dict[str, str]] = {}
    for curve in exact_curves:
        target_id = canonical_projection_render_target(model, curve)
        if curve.domain == "scenario":
            scenario_targets.setdefault(curve.template_item_id, {})[
                curve.scenario
            ] = target_id
        try:
            item = model.get_item(target_id)
        except KeyError as exc:
            raise ValueError(
                "canonical compile binding targets an absent template item: "
                f"{target_id}"
            ) from exc
        if curve.domain == "scenario":
            # Authenticated case rows are input controls for the forecast
            # years: retire the inherited projected path and overrides so none
            # competes with the verified snapshot values.  The inherited
            # historical spec (a cell_ref to the driver's own history) stays,
            # so the row's reported-year cells keep their lineage; snapshot
            # values are written only for years > most_recent_fy and never
            # meet it.
            item.projected = None
            item.item_type = ItemType.input
            item.overrides = None
        values = tuple(
            (year, value)
            for year, value in curve.values
            if year > most_recent_fy
        )
        if not values:
            continue
        if curve.domain == "scenario":
            if item.values is None:
                item.values = ValueSeries()
            for year, value in values:
                item.values.values[year] = ValueCell(
                    period=year,
                    value=value,
                    provenance=ValueProvenance.input,
                    note=AUTHENTICATED_SCENARIO_PROJECTION_NOTE,
                )
            if item.formula_periods is not None:
                item.formula_periods = sorted(
                    _scenario_historical_formula_periods(
                        model, item, most_recent_fy,
                    )
                    | {year for year, _value in values}
                )
        else:
            if item.overrides is None:
                item.overrides = {}
            for year, value in values:
                item.overrides[year] = FormulaSpec(
                    type=FormulaType.constant,
                    params={"value": value},
                )
        seeded_count += 1
        coverage.append(
            {
                "canonical_driver_key": (
                    curve.driver_id if curve.driver_id is not None else curve.control_id
                ),
                "resolved_item_id": item.id,
                "periods": [year for year, _value in values],
                "source_kind": "projection_override",
                "control_id": curve.control_id,
                "binding_id": curve.binding_id,
                "scenario": curve.scenario,
            }
        )
    for owner_id, targets in scenario_targets.items():
        owner = model.get_item(owner_id)
        # The owner is selected exclusively from authenticated scenario rows.
        # Remove any base-case ModelSpec assumption seeds on the owner so there
        # is no parallel, unauthenticated curve path.
        scenario_periods = {
            year
            for curve in exact_curves
            if curve.domain == "scenario" and curve.template_item_id == owner_id
            for year, _value in curve.values
        }
        if owner.overrides is not None:
            for year in scenario_periods:
                owner.overrides.pop(year, None)
            if not owner.overrides:
                owner.overrides = None
        original = owner.projected
        selector = (
            None
            if original is None
            else line_item_ref_from_obj((original.params or {}).get("selector"))
        )
        if selector is None:
            raise ValueError(
                "authenticated scenario projection owner has no selector"
            )
        owner.projected = FormulaSpec(
            type=FormulaType.valuation,
            subtype="scenario_select",
            params={
                "selector": selector,
                **{
                    case: LineItemRef(id=target_id)
                    for case, target_id in sorted(targets.items())
                },
            },
            note=AUTHENTICATED_SCENARIO_PROJECTION_NOTE,
        )
    return ControlSnapshotProjectionResult(
        projected_curve_count=seeded_count,
        available_curve_count=len(exact_curves),
        coverage=tuple(coverage),
    )


def compile_candidate_model(
    base_model: FinancialModel,
    prior_curves: tuple[CanonicalProjectionCurve, ...] | None,
    candidate_curves: tuple[CanonicalProjectionCurve, ...] | None,
    compiled_registry: "CompiledDriverRegistry | None",
    most_recent_fy: int,
) -> CandidateModelCompileResult:
    """Compile a candidate after erasing the exact prior canonical controls."""

    del compiled_registry
    prior = _require_canonical_curve_order(prior_curves, field_name="prior_curves")
    candidate = _require_canonical_curve_order(
        candidate_curves, field_name="candidate_curves"
    )
    model = copy.deepcopy(base_model)
    model.build_index()
    _remove_control_snapshot_effects(model, prior)
    projection_result = _project_control_snapshots(
        model,
        candidate,
        most_recent_fy,
    )
    model.build_index()
    from .dependency_graph import DependencyGraph, formula_owned_recompute_ids

    graph = DependencyGraph()
    graph.build(model)
    computed_values = graph.compute(
        {},
        recompute=formula_owned_recompute_ids(model),
    )
    return CandidateModelCompileResult(
        model=model,
        control_snapshot_projection=projection_result,
        computed_values=computed_values,
    )


def reconstruct_compiled_registry(
    compiler_inputs: Mapping[str, Any],
    *,
    business_model_revision: "BusinessModelRevision",
) -> "CompiledDriverRegistry":
    """Recompile the canonical ModelSpec against a fresh template."""

    model_spec_input = compiler_inputs["model_spec"]
    model_spec_payload = model_spec_input.get("payload")
    expected_hash = str(model_spec_input.get("content_hash") or "")

    from .build import load_template
    from .build_model_product_contracts import resolve_model_spec_business_model_revision
    from .model_spec_decoder import (
        VerifiedModelSpecDecodeRequest,
        decode_verified_model_spec_compiler_input,
    )
    from .business_model_revision import BusinessModelRevision
    from .business_model_compiler import (
        bind_driver_assumption_plan,
        compile_business_model,
    )

    verified_model_spec = decode_verified_model_spec_compiler_input(
        VerifiedModelSpecDecodeRequest(
            payload_mapping=model_spec_payload,
            compiler_input_hash=expected_hash,
            external_model_version_digest=None,
        )
    )
    model_spec = verified_model_spec.model_spec
    revision = BusinessModelRevision.model_validate(
        business_model_revision
    )
    revision = resolve_model_spec_business_model_revision(
        model_spec,
        revision,
    )
    return bind_driver_assumption_plan(
        compile_business_model(load_template(), revision.business_model),
        revision.driver_assumption_plan,
    )


__all__ = [
    "CandidateModelCompileResult",
    "ControlSnapshotProjectionResult",
    "_project_control_snapshots",
    "compile_candidate_model",
    "reconstruct_compiled_registry",
]
