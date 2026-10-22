"""Scenario output compute helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import logging
import math
import re

from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypedDict

from .build_formula_refs import _extract_ref_ids as _formula_extract_ref_ids
from .build_model_items import _iter_items as _model_iter_items
from .dependency_graph import DependencyGraph
from .handle import ComputedValues
from .build_value_writers import _write_default_unless_authored
from .required_role_catalog import pass_through_counterparts
from .model_kind_capabilities import (
    ScenarioControlDeclaration,
    SemanticRowMaterializationDeclaration,
    SensitivityBindingDeclaration,
    ShareConversionDeclaration,
)
from .model_kind_dispatch import (
    CapabilityUndeclaredForArtifact,
    resolve_sensitivity_binding,
)
from .model_readiness_scenario_output import (
    _SCENARIO_OUTPUT_REQUIREMENTS as _MODEL_SCENARIO_OUTPUT_REQUIREMENTS,
)
from .models import (
    FinancialModel,
    FormulaDerivation,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ScenarioInputs,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from industry_slice_core.industry_types import Unit
from .refs import line_item_ref_from_obj as _line_item_ref_from_obj
from .build_semantic_rows import _insert_line_item_after
from .renderer_columns import _fixed_cell_anchor_period
from .scenario_rows import find_scenario_value_row

if TYPE_CHECKING:
    from .build_model_product_contracts import ModelSpecScenarioSetBinding


_SCENARIO_EPS_LIMIT = 4
_SCENARIO_EPS_ITEM_RE = re.compile(r"^tpl\.s\.earnings_scenarios\.eps_(bull|base|bear)_(\d+)$")

class _SensitivityRun(TypedDict):
    row_value: float
    column_value: float
    period: int
    outputs: dict[str, dict[str, object]]


_SCENARIO_PUBLICATION_EPS_ITEM_IDS = (
    "tpl.fm.income_statement.eps",
    "tpl.a.tax_net_income.eps",
)
# S4/S5 + §14 IG-4(m) (plan §3): the semantic control rows this pass mints, the
# grounded-schedule rows it wires, and the provenance notes their formulas carry
# were issuer-named module constants here.  They are now declared by the model
# kind (`ModelKindCapabilities.semantic_row_materializations` and
# `.grounded_semantic_rows`) and arrive as parameters, so this mechanism owns
# the *shape* of a minted control row and no model kind's vocabulary.
# A1/A8 (plan §3): the control-to-workbook binding tables that used to live
# here as issuer-named module constants are now declared by the model kind
# (`ScenarioControlDeclaration.workbook_targets` for scenario lowering and
# `.forecast_control_targets` for the registry-bound pass).  Every function
# below that needs one takes it as a declared input.





def _finite_projection_values(
    values: Mapping[int, float | None],
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
    values: ComputedValues,
    candidate_item_ids: Iterable[str],
    projection_periods: Iterable[int],
) -> str | None:
    finite_projection_values = _finite_projection_values
    present_item_ids = [item_id for item_id in candidate_item_ids if item_id in model._index]
    if not present_item_ids:
        return None
    periods = [int(period) for period in projection_periods]
    for item_id in present_item_ids:
        if finite_projection_values(values.get(item_id, {}), periods):
            return item_id
    return present_item_ids[0]


class _ScenarioOutputsDict(dict[str, dict[str, dict[int, float]]]):
    base_eval_reads: dict[tuple[str, int], tuple[tuple[str, int], ...]]
    unavailable_sources: dict[tuple[str, str], str]


def compute_scenario_outputs(
    model: FinancialModel,
    *,
    unresolved_cases: frozenset[str],
) -> tuple[_ScenarioOutputsDict, dict[str, dict[str, str]]]:
    """Run Bull/Base/Bear scenario computations for decision-critical outputs.

    Returns each resolved case's output values by field and, beside them, the row
    each field was read from. Unresolved cases are not computed. A field whose
    selected source row holds no finite projection value is dropped and named in
    ``unavailable_sources[(case, field)]`` by that source row, so the writer can
    stamp the unwritten output cell with why it is unwritten.
    """

    if not model._index:
        model.build_index()
    dependency_graph_cls = DependencyGraph
    graph = dependency_graph_cls()
    graph.build(model)
    base_results = graph.compute({})
    initial_eval_reads = dict(graph.last_eval_reads)
    scenario_outputs = _ScenarioOutputsDict()
    scenario_outputs.base_eval_reads = dict(initial_eval_reads)
    scenario_outputs.unavailable_sources = {}
    scenario_output_item_ids: dict[str, dict[str, str]] = {}
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    scenario_output_requirements = _MODEL_SCENARIO_OUTPUT_REQUIREMENTS
    build_scenario_overrides = _build_scenario_overrides
    downstream_item_ids = _downstream_item_ids
    scenario_compute_inputs = _scenario_compute_inputs
    select_scenario_output_item_id = _select_scenario_output_item_id
    finite_projection_values = _finite_projection_values

    for case in ("bull", "base", "bear"):
        if case in unresolved_cases:
            continue
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
        if case == "base":
            scenario_outputs.base_eval_reads = {
                **initial_eval_reads,
                **graph.last_eval_reads,
            }
        case_outputs: dict[str, dict[int, float]] = {}
        case_item_ids: dict[str, str] = {}
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
                case_item_ids[field_name] = item_id
            else:
                scenario_outputs.unavailable_sources[(case, field_name)] = item_id
        publication_eps_item_id = select_scenario_output_item_id(
            model,
            results,
            _SCENARIO_PUBLICATION_EPS_ITEM_IDS,
            projection_periods,
        )
        if publication_eps_item_id is not None:
            publication_eps = finite_projection_values(
                results.get(publication_eps_item_id, {}),
                projection_periods,
            )
            if publication_eps:
                case_outputs["publication_eps"] = publication_eps
                case_item_ids["publication_eps"] = publication_eps_item_id
            else:
                scenario_outputs.unavailable_sources[(case, "publication_eps")] = publication_eps_item_id
        scenario_outputs[case] = case_outputs
        scenario_output_item_ids[case] = case_item_ids

    return scenario_outputs, scenario_output_item_ids


def compute_scenario_eps(
    model: FinancialModel,
    *,
    unresolved_cases: frozenset[str],
) -> dict[str, dict[int, float]]:
    """Run resolved Bull/Base/Bear EPS computations using direct overrides."""

    compute_outputs = compute_scenario_outputs
    scenario_outputs, _ = compute_outputs(model, unresolved_cases=unresolved_cases)
    return {
        case: dict(fields.get("adj_eps", {}))
        for case, fields in scenario_outputs.items()
    }


def scenario_results_payload(
    outputs: Mapping[str, dict[str, dict[int, float]]],
    unresolved_cases: frozenset[str],
) -> dict[str, object]:
    """Publish all three case statuses without substituting unresolved outputs."""

    return {
        case: {
            "status": (
                "not_computed"
                if case in unresolved_cases
                else "computed" if outputs[case] else "blocked"
            ),
            "outputs": {} if case in unresolved_cases else outputs[case],
        }
        for case in ("bull", "base", "bear")
    }


def _populate_scenario_eps(
    model: FinancialModel,
    eps_by_case: dict[str, dict[int, float]],
    base_source_id: str | None,
    *,
    existing_seed_coverage: Sequence[Mapping[str, Any]] = (),
    base_eval_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] | None = None,
) -> list[dict[str, object]]:
    if not model._index:
        model.build_index()
    scenario_eps_limit = _SCENARIO_EPS_LIMIT
    scenario_eps_item_re = _SCENARIO_EPS_ITEM_RE
    iter_items = _model_iter_items
    value_series_cls = ValueSeries
    value_cell_cls = ValueCell
    value_provenance = ValueProvenance
    projection_periods = [int(period) for period in model.time_structure.projection_periods[:scenario_eps_limit]]
    # The base snapshot is the publication row itself at its year, so it keeps
    # that row (the one compute_scenario_outputs read it from) as its input
    # expression beside the value, as a constant does. Bull/bear substitute case
    # controls that no rendered cell holds.
    period_index = {
        int(period): index
        for index, period in enumerate(
            (
                *model.time_structure.historical_periods,
                *model.time_structure.projection_periods,
            )
        )
    }

    for item_obj in iter_items(model):
        match = scenario_eps_item_re.match(item_obj.id)
        if match is None:
            continue
        case = match.group(1)
        index = int(match.group(2)) - 1
        if index < 0 or index >= len(projection_periods):
            continue
        value_period = projection_periods[index]
        value = eps_by_case.get(case, {}).get(value_period)
        if value is None:
            continue
        period = (
            int(item_obj.formula_periods[0])
            if item_obj.formula_periods
            else value_period
        )
        if item_obj.values is None:
            item_obj.values = value_series_cls()
        storage_periods = {int(period)}
        if item_obj.column is not None:
            storage_periods.add(int(projection_periods[0]))
        for storage_period in storage_periods:
            item_obj.values.values[storage_period] = value_cell_cls(
                period=storage_period,
                value=float(value),
                provenance=value_provenance.computed,
            )
            if case == "base" and base_source_id is not None:
                if item_obj.overrides is None:
                    item_obj.overrides = {}
                item_obj.overrides[storage_period] = FormulaSpec(
                    type=FormulaType.constant,
                    params={
                        "value": float(value),
                        "expr": LineItemRef(
                            id=base_source_id,
                            t=period_index[value_period] - period_index[storage_period],
                        ),
                    },
                    note="base_scenario_eps_snapshot",
                )
    coverage: list[dict[str, object]] = []
    if base_source_id is not None and existing_seed_coverage:
        if base_eval_reads is None:
            dependency_graph_cls = DependencyGraph
            graph = dependency_graph_cls()
            graph.build(model)
            base_results = graph.compute({})
            eval_reads = dict(graph.last_eval_reads)
            build_scenario_overrides = _build_scenario_overrides
            downstream_item_ids = _downstream_item_ids
            scenario_compute_inputs = _scenario_compute_inputs
            overrides = build_scenario_overrides(model, "base", base_results=base_results)
            recompute = downstream_item_ids(graph, set(overrides))
            scenario_inputs = scenario_compute_inputs(model, overrides)
            graph.compute(
                scenario_inputs,
                recompute=recompute,
                seed_results=base_results,
                propagate_roots=set(overrides),
                periods={int(p) for p in model.time_structure.projection_periods},
            )
            eval_reads.update(graph.last_eval_reads)
        else:
            eval_reads = dict(base_eval_reads)
        rows_by_cell: dict[tuple[str, int], list[Mapping[str, Any]]] = {}
        for row in existing_seed_coverage:
            resolved_id = str(row.get("resolved_item_id") or "")
            source_kind = str(row.get("source_kind") or "")
            if not resolved_id or source_kind not in {
                "business_model_node_identity",
                "source_bundle_adjusted_ebitda_bridge",
                "source_bundle_opening_balance",
                "source_bundle_schedule_disclosure",
            }:
                continue
            for cov_period in row.get("periods", ()):
                rows_by_cell.setdefault((resolved_id, int(cov_period)), []).append(row)

        def _consumed_cells(start_cells: Iterable[tuple[str, int]]) -> set[tuple[str, int]]:
            visited: set[tuple[str, int]] = set()
            stack = list(start_cells)
            while stack:
                curr = stack.pop()
                if curr in visited:
                    continue
                visited.add(curr)
                for nxt in eval_reads.get(curr, ()):
                    if nxt not in visited:
                        stack.append(nxt)
            return visited

        target_roots: list[tuple[str, list[int], list[tuple[str, int]]]] = []
        for idx, val_period in enumerate(projection_periods[:2], start=1):
            snap_id = f"tpl.s.earnings_scenarios.eps_base_{idx}"
            if snap_id in model._index:
                snap_item = model.get_item(snap_id)
                snap_periods = (
                    [int(p) for p in snap_item.formula_periods]
                    if snap_item.formula_periods
                    else [val_period]
                )
                if snap_item.column is not None and projection_periods:
                    snap_periods = sorted({*snap_periods, int(projection_periods[0])})
                target_roots.append((snap_id, snap_periods, [(base_source_id, val_period)]))
        if len(projection_periods) >= 2:
            fy12_roots = [
                (base_source_id, int(projection_periods[0])),
                (base_source_id, int(projection_periods[1])),
            ]
            for growth_id in (
                "tpl.s.earnings_scenarios.eps_growth_base_2",
                "tpl.s.valuation_scenarios.base_growth",
            ):
                if growth_id in model._index:
                    g_item = model.get_item(growth_id)
                    g_periods = (
                        [int(p) for p in g_item.formula_periods]
                        if g_item.formula_periods
                        else [int(projection_periods[0])]
                    )
                    if g_item.column is not None and projection_periods:
                        g_periods = sorted({*g_periods, int(projection_periods[0])})
                    target_roots.append((growth_id, g_periods, fy12_roots))

        seen_records: set[tuple[str, str, str, int]] = set()
        for target_id, target_periods, roots in target_roots:
            reached = _consumed_cells(roots)
            for cell_key in sorted(reached):
                for row in rows_by_cell.get(cell_key, ()):
                    source_kind = str(row.get("source_kind") or "")
                    ident_key = str(
                        row.get("evidence_id")
                        or row.get("fact_id")
                        or row.get("canonical_driver_key")
                        or ""
                    )
                    for target_period in target_periods:
                        dedup = (target_id, source_kind, ident_key, target_period)
                        if dedup in seen_records:
                            continue
                        seen_records.add(dedup)
                        coverage.append(
                            {
                                **dict(row),
                                "resolved_item_id": target_id,
                                "periods": [target_period],
                            }
                        )
    return coverage

_THESIS_SNAPSHOT_FIELDS = (
    "adj_eps",
    "revenue_m",
    "op_margin_pct",
    "ebitda_margin_pct",
    "fcf_per_share",
)
_THESIS_SNAPSHOT_TERMINAL_YEAR_ID = "tpl.s.thesis_snapshot.terminal_year"


def _populate_thesis_snapshot(
    model: FinancialModel,
    scenario_output_values: Mapping[str, Mapping[str, Mapping[int, float]]],
) -> None:
    """Write each case's terminal-year scenario outputs into the Thesis Snapshot.

    The terminal year is the last projection period, the year scenario-output
    readiness judges. A field ``compute_scenario_outputs`` did not produce for
    that year stays unwritten.
    """

    projection_periods = [
        int(period) for period in model.time_structure.projection_periods
    ]
    if not projection_periods:
        return
    terminal = projection_periods[-1]
    if not model._index:
        model.build_index()

    def write(item_id: str, value: float) -> None:
        if item_id not in model._index:
            return
        item_obj = model.get_item(item_id)
        storage_period = _fixed_cell_anchor_period(model, item_obj)
        if storage_period is None:
            return
        item_obj.values = ValueSeries(
            values={
                storage_period: ValueCell(
                    period=storage_period,
                    value=float(value),
                    provenance=ValueProvenance.computed,
                )
            }
        )

    for case in ("bull", "base", "bear"):
        case_outputs = scenario_output_values.get(case, {})
        for field_name in _THESIS_SNAPSHOT_FIELDS:
            value = case_outputs.get(field_name, {}).get(terminal)
            if value is not None:
                write(f"tpl.s.thesis_snapshot.{case}_{field_name}", value)
    write(_THESIS_SNAPSHOT_TERMINAL_YEAR_ID, float(terminal))


def _materialize_declared_scenario_controls(
    model: FinancialModel,
    semantic_rows: Sequence[SemanticRowMaterializationDeclaration] = (),
) -> None:
    """Materialize this model kind's declared controls, absent from the template.

    ``semantic_rows`` is the model kind's declared minting table (plan §14 IG-4
    item 15(m)): each row names the workbook anchor it lands after, its label,
    and its unit contract.  Declared order is insertion order, which is why the
    declaration is not sorted.  Absence is feature-off — no declared row, no
    minted row.
    """

    if not model._index:
        model.build_index()
    insert_line_item_after = _insert_line_item_after

    for declared_row in semantic_rows:
        if (
            declared_row.anchor_item_id not in model._index
            or declared_row.item_id in model._index
        ):
            continue
        if insert_line_item_after is None:
            raise RuntimeError("canonical semantic row insertion helper is unavailable")
        insert_line_item_after(
            model,
            anchor_item_id=declared_row.anchor_item_id,
            new_item=LineItem(
                id=declared_row.item_id,
                label=declared_row.label,
                row=0,
                item_type=ItemType.input,
                unit=declared_row.unit,
                model_scale=declared_row.scale,
            ),
        )

    dso_id = "tpl.a.balance_sheet_wc.days_sales_outstanding_dso"
    if dso_id in model._index:
        # The template historically mislabeled this day-count control as
        # dollars.  Scenario binding makes the semantic unit operational.
        model.get_item(dso_id).unit = Unit.days
    model.build_index()


def _bind_model_spec_control_units(model: FinancialModel) -> None:
    """Bind the canonical control units and structural bridges every kind shares.

    Both control-binding modes run this pass: the unit-bound mode runs it
    alone, and the registry-bound pass runs it before its declared controls.
    The acquisition bridges carry the acquisitions cash-flow control into the
    goodwill and intangible roll-forwards, and the per-share outputs derive
    from reported and projected balances alike, over every historical and
    projection period; none of it is a model-kind declaration.

    Cash paid for acquisitions is negative in the cash-flow sign and returns
    to the balance sheet once: the goodwill addition and the acquired-intangible
    addition together equal the negated cash-flow value.  This binding is the
    standing all-to-goodwill proposal: no split is declared, so unauthored
    projection periods of ``acquisition_goodwill_allocation`` are the house
    value 1, written ``default`` (``judgment_absent_default_used``) with the
    proposal strategy.  An authored period keeps its input cell.  The goodwill
    addition is the negated acquisitions cash-flow row times that fraction, and
    acquired intangibles are the identity's remainder.  The judgment value
    belongs to FAS; this pass reads an authored cell only by leaving it.
    Reported years of the goodwill addition are the schedule balance's own
    movement, goodwill at t=0 less goodwill at t=-1, from the second historical
    period; the allocation row's history is authored from its two filing facts
    by ``build_company_grounded_schedules``.
    """

    if not model._index:
        model.build_index()
    for day_id in (
        "tpl.a.balance_sheet_wc.days_sales_outstanding_dso",
        "tpl.a.balance_sheet_wc.days_payable_outstanding_dpo",
    ):
        if day_id in model._index:
            model.get_item(day_id).unit = Unit.days

    historical_periods = [
        int(period) for period in model.time_structure.historical_periods
    ]
    projection_periods = [
        int(period) for period in model.time_structure.projection_periods
    ]
    acquisitions_id = "tpl.fm.cash_flow.acquisitions_of_businesses"
    goodwill_add_id = (
        "tpl.a.intangibles_goodwill.add_less_acquisitions_write_offs"
    )
    intangibles_add_id = (
        "tpl.a.intangibles_goodwill.add_acquisitions_of_businesses"
    )
    # Admitted by the forecast-control profile.  An unauthored projection
    # period is the house proposal (1), written default so the unanswered
    # split is a finding; an authored period is left alone.
    goodwill_allocation_id = (
        "tpl.a.intangibles_goodwill.acquisition_goodwill_allocation"
    )
    if (
        acquisitions_id in model._index
        and goodwill_add_id in model._index
        and intangibles_add_id in model._index
    ):
        goodwill_add = model.get_item(goodwill_add_id)
        if goodwill_allocation_id in model._index:
            allocation = model.get_item(goodwill_allocation_id)
            allocation.projected = None
            _write_default_unless_authored(
                allocation,
                projection_periods,
                1,
                owner="build_scenarios",
                strategy="acquisition_goodwill_allocation_proposal",
                dependencies=(acquisitions_id,),
            )
            goodwill_add.projected = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="acquisition_goodwill_allocation",
                params={
                    "expr": {
                        "op": "*",
                        "args": [
                            {
                                "op": "NEG",
                                "arg": LineItemRef(id=acquisitions_id),
                            },
                            LineItemRef(id=goodwill_allocation_id),
                        ],
                    }
                },
                note=(
                    "proposal_all_to_goodwill_negated_acquisition_cash_paid"
                ),
            )
        else:
            goodwill_add.projected = FormulaSpec(
                type=FormulaType.ref,
                subtype="negated_ref",
                params={
                    "source": LineItemRef(id=acquisitions_id),
                    "negate": True,
                },
                note="canonical_acquisition_cash_paid_to_goodwill_roll_forward",
            )
        goodwill_add.historical = FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "expr": {
                    "op": "-",
                    "left": LineItemRef(
                        id="tpl.a.intangibles_goodwill.goodwill",
                        t=0,
                    ),
                    "right": LineItemRef(
                        id="tpl.a.intangibles_goodwill.goodwill",
                        t=-1,
                    ),
                }
            },
            note="historical_goodwill_movement_from_reported_balance",
        )
        goodwill_add.formula_periods = sorted(
            set(projection_periods) | set(historical_periods[1:])
        )
        intangibles_add = model.get_item(intangibles_add_id)
        intangibles_add.item_type = ItemType.derived
        intangibles_add.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="acquisition_cash_paid_remainder",
            params={
                "expr": {
                    "op": "-",
                    "left": {"op": "NEG", "arg": LineItemRef(id=acquisitions_id)},
                    "right": LineItemRef(id=goodwill_add_id),
                }
            },
            note=(
                "canonical_acquisition_cash_paid_less_goodwill_bridge_to_"
                "intangibles_roll_forward"
            ),
        )
        intangibles_add.formula_periods = list(projection_periods)

    per_share_outputs = {
        "tpl.fm.balance_sheet.book_value_per_share": (
            "tpl.fm.balance_sheet.total_equity"
        ),
        "tpl.fm.balance_sheet.net_cash_per_share": (
            "tpl.fm.balance_sheet.cash_and_marketable_securities"
        ),
    }
    diluted_shares_id = (
        "tpl.fm.income_statement.diluted_shares_outstanding_m"
    )
    for output_id, numerator_id in per_share_outputs.items():
        if (
            output_id not in model._index
            or numerator_id not in model._index
            or diluted_shares_id not in model._index
        ):
            continue
        output = model.get_item(output_id)
        formula = FormulaSpec(
            type=FormulaType.ratio,
            subtype="per_share",
            params={
                "numerator": LineItemRef(id=numerator_id),
                "denominator": LineItemRef(id=diluted_shares_id),
            },
            note="canonical_balance_sheet_per_share_output",
        )
        output.historical = formula.model_copy(deep=True)
        output.projected = formula
        output.formula_periods = sorted(
            set(historical_periods) | set(projection_periods)
        )


def _require_authenticated_dependent_rows(
    grounded_semantic_rows: Mapping[str, str] | None,
) -> tuple[tuple[str, str], ...]:
    """Return each pass-through counterpart's (balance row, change row) or fail with the owner."""

    declared_rows: Mapping[str, str] = grounded_semantic_rows or {}
    pairs: list[tuple[str, str]] = []
    for counterpart in pass_through_counterparts():
        balance_role, change_role = counterpart.leaf, counterpart.change_role
        balance_id = declared_rows.get(balance_role)
        change_id = declared_rows.get(change_role)
        if balance_id is None or change_id is None:
            raise CapabilityUndeclaredForArtifact(
                capability="grounded_semantic_rows",
                artifact="the compiled model's authenticated dependent pass",
                detail=(
                    "the declared share conversion runs its dependent pass over the "
                    f"grounded {counterpart.display_name.lower()} rows, and this model "
                    f"kind declares {sorted(declared_rows)} — both "
                    f"{balance_role!r} and {change_role!r} are required"
                ),
            )
        pairs.append((balance_id, change_id))
    return tuple(pairs)


def _bind_declared_model_spec_controls(
    model: FinancialModel,
    compiled_registry: Any | None,
    *,
    forecast_control_targets: Mapping[str, str],
    semantic_rows: Sequence[SemanticRowMaterializationDeclaration] = (),
    grounded_semantic_rows: Mapping[str, str] | None = None,
) -> None:
    """Bind a model kind's declared controls to canonical, overridable rows.

    ``forecast_control_targets`` is the model kind's declared control-key to
    workbook-item binding (plan §3 row A8): the registry-bound pass installs
    exactly the declared rows that exist in this workbook onto the compiled
    driver registry, and declares nothing of its own.  ``semantic_rows`` and
    ``grounded_semantic_rows`` are the declared minting table and the declared
    grounded-schedule roles (plan §3 rows S4/S5 and §14 IG-4 item 15(m)); each
    is feature-off when the model kind declares none.
    """

    if compiled_registry is None:
        return
    _materialize_declared_scenario_controls(model, semantic_rows)
    declared_rows: Mapping[str, str] = grounded_semantic_rows or {}
    insert_line_item_after = _insert_line_item_after
    if insert_line_item_after is None:
        raise RuntimeError("canonical semantic row insertion helper is unavailable")

    for declared_row in semantic_rows:
        target_id = declared_row.growth_target_item_id
        if target_id is None or target_id not in model._index:
            continue
        if declared_row.item_id not in model._index:
            continue
        target = model.get_item(target_id)
        target.projected = FormulaSpec(
            type=FormulaType.growth,
            params={
                "base": LineItemRef(id=target_id, t=-1),
                "rate": LineItemRef(id=declared_row.item_id),
            },
            note=declared_row.growth_note,
        )
        target.formula_periods = sorted(
            set(target.formula_periods or ())
            | {int(period) for period in model.time_structure.projection_periods}
        )

    projection_periods = [
        int(period) for period in model.time_structure.projection_periods
    ]
    if any(
        declared_rows.get(counterpart.change_role) in model._index
        for counterpart in pass_through_counterparts()
    ):
        for obligation_id, obligation_change_id in _require_authenticated_dependent_rows(
            grounded_semantic_rows
        ):
            obligation_change = model.get_item(obligation_change_id)
            obligation_change.projected = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="add_subtract",
                params={
                    "operands": [
                        "-",
                        LineItemRef(id=obligation_id),
                        LineItemRef(id=obligation_id, t=-1),
                    ]
                },
                note="counterpart_balance_current_less_prior",
            )
            obligation_change.formula_periods = projection_periods

    _bind_model_spec_control_units(model)
    if "tpl.a.balance_sheet_wc.of_rev" in model._index:
        deferred_ratio = model.get_item("tpl.a.balance_sheet_wc.of_rev")
        deferred_ratio.item_type = ItemType.input
        deferred_ratio.unit = Unit.percentage

    prepaid_control_id = "tpl.a.balance_sheet_wc.prepaid_pct_revenue"
    prepaid_anchor_id = "tpl.a.balance_sheet_wc.of_rev"
    prepaid_balance_id = "tpl.fm.balance_sheet.current_asset_3"
    if prepaid_control_id not in model._index and prepaid_anchor_id in model._index:
        insert_line_item_after(
            model,
            anchor_item_id=prepaid_anchor_id,
            new_item=LineItem(
                id=prepaid_control_id,
                label="Prepaid expenses and other as % of revenue",
                row=0,
                item_type=ItemType.input,
                unit=Unit.percentage,
                model_scale="units",
            ),
        )
    if prepaid_balance_id in model._index and prepaid_control_id in model._index:
        prepaid = model.get_item(prepaid_balance_id)
        prepaid.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "operands": [
                    "*",
                    LineItemRef(id="tpl.fm.income_statement.total_revenue"),
                    LineItemRef(id=prepaid_control_id),
                ]
            },
            note="prepaid_expenses_from_revenue_ratio",
        )
        prepaid.formula_periods = list(model.time_structure.projection_periods)
    prepaid_schedule_id = "tpl.a.balance_sheet_wc.current_asset_2"
    if prepaid_schedule_id in model._index:
        prepaid_schedule = model.get_item(prepaid_schedule_id)
        if prepaid_schedule.projected is not None:
            prepaid_schedule.formula_periods = sorted(
                set(prepaid_schedule.formula_periods or ())
                | set(projection_periods)
            )

    structural_zero_id = "tpl.fm.cash_flow.other_cash_flows_from_financing"
    if structural_zero_id in model._index:
        zero_item = model.get_item(structural_zero_id)
        zero_item.overrides = {
            int(period): FormulaSpec(
                type=FormulaType.constant,
                params={"value": 0.0},
                note="structural_zero_other_financing_cash_flow",
            )
            for period in model.time_structure.projection_periods
        }
        zero_item.formula_periods = projection_periods

    current_liability_3_id = "tpl.fm.balance_sheet.current_liability_3"
    if current_liability_3_id in model._index:
        current_liability_3 = model.get_item(current_liability_3_id)
        current_liability_3.item_type = ItemType.derived
        current_liability_3.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": 0.0},
            note=(
                "structural_zero_no_commercial_paper_in_pinned_"
                "capital_structure"
            ),
        )
        current_liability_3.formula_periods = projection_periods

    deferred_revenue_noncurrent_id = (
        "tpl.fm.balance_sheet.deferred_revenue_noncurrent"
    )
    if deferred_revenue_noncurrent_id in model._index:
        deferred_revenue_noncurrent = model.get_item(
            deferred_revenue_noncurrent_id
        )
        deferred_revenue_noncurrent.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="carry_forward",
            params={
                "source": LineItemRef(
                    id=deferred_revenue_noncurrent_id,
                    t=-1,
                )
            },
            note="carry_forward_observed_noncurrent_deferred_revenue",
        )
        deferred_revenue_noncurrent.formula_periods = projection_periods

    dividend_payable_id = "tpl.a.dividends_shares.dividend_payable"
    dividend_paid_id = "tpl.a.dividends_shares.dividend_paid"
    if dividend_payable_id in model._index and dividend_paid_id in model._index:
        dividend_payable = model.get_item(dividend_payable_id)
        dividend_payable.item_type = ItemType.derived
        dividend_payable.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="cross_sheet_ref",
            params={"source": LineItemRef(id=dividend_paid_id)},
            note="zero_dividend_policy_to_dividend_payable",
        )
        dividend_payable.formula_periods = projection_periods
    model.build_index()
    for control_id, item_id in forecast_control_targets.items():
        if item_id in model._index:
            compiled_registry.driver_keys[control_id] = item_id


def _materialize_declared_share_conversion(
    model: FinancialModel,
    pinned_market_inputs: Any,
    declaration: ShareConversionDeclaration,
) -> None:
    """Bind the declared quote to the repurchase-conversion price row.

    Plan §3 rows A5/A11: the four operands are the model kind's declared
    workbook rows.  The conversion equation itself (``total_amount`` over the
    average price, ``number_of_shares_repurchased``) is the template's; this
    binds only the price it divides by to the pinned quote.  An unusable
    pinned quote — absent, unresolved, blocked, or not a finite positive
    price — is a routine coverage state, so the binding degrades the way every
    other quote consumer does: it leaves the price row unbound and returns, and
    the compile continues.  The valuation-input readiness already reports that
    gap as a missing ``stock_price``.  The template gives the price row and the
    count no period, so an unbound price leaves neither a cell; binding the quote
    gives both their projection periods.  No price is written into a formula
    here, so the count never carries a fabricated number.
    """

    stock_price = getattr(pinned_market_inputs, "stock_price", None)
    if stock_price is None or stock_price.status != "resolved" or stock_price.value is None:
        logging.warning(
            "Declared share conversion left unbound: no resolved pinned quote for %s",
            declaration.quote_source_item_id,
        )
        return
    price = float(stock_price.value)
    if not math.isfinite(price) or price <= 0:
        logging.warning(
            "Declared share conversion left unbound: pinned quote for %s is not a "
            "finite positive price: %r",
            declaration.quote_source_item_id,
            price,
        )
        return

    price_id = declaration.average_price_item_id
    price_item = model.get_item(price_id)
    periods = [int(period) for period in model.time_structure.projection_periods]
    price_item.item_type = ItemType.derived
    price_item.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={
            "source": LineItemRef(id=declaration.quote_source_item_id)
        },
        note=f"covered_bundle_quote:{stock_price.observation_id}",
    )
    price_item.formula_periods = periods
    model.get_item(declaration.shares_repurchased_item_id).formula_periods = list(periods)
    if price_item.values is not None:
        for period in periods:
            price_item.values.values.pop(period, None)
    if price_item.overrides is not None:
        for period in periods:
            price_item.overrides.pop(period, None)
    model.build_index()


def _finalize_authenticated_dependents(
    model: FinancialModel,
    *,
    grounded_semantic_rows: Mapping[str, str] | None = None,
) -> None:
    """Finalize software and historical links after authenticated projection.

    The declared balance-sheet policy owns client-fund obligation projection.
    This pass retains the independent software and historical presentation
    controls without replacing that economic link.
    """

    projection_periods = [
        int(period) for period in model.time_structure.projection_periods
    ]
    historical_periods = [
        int(period) for period in model.time_structure.historical_periods
    ]

    dependent_rows = _require_authenticated_dependent_rows(grounded_semantic_rows)
    model.build_index()
    for obligation_id, obligation_change_id in dependent_rows:
        if (
            obligation_id not in model._index
            or obligation_change_id not in model._index
        ):
            return
        obligation_change = model.get_item(obligation_change_id)
        obligation_change.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={
                "operands": [
                    "-",
                    LineItemRef(id=obligation_id),
                    LineItemRef(id=obligation_id, t=-1),
                ]
            },
            note="counterpart_balance_current_less_prior",
        )
        obligation_change.formula_periods = sorted(
            set(historical_periods[1:]) | set(projection_periods)
        )

    software_schedule = model.get_item(
        "tpl.a.capital_investments.software_investment_if_applicable"
    )
    software_balance = model.get_item(
        "tpl.fm.balance_sheet.software_investment_if_applicable"
    )
    software_balance.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=software_schedule.id)},
        note="reported_capitalized_software_schedule_to_balance_sheet",
    )
    supported_historical_periods = set(software_schedule.overrides or {})
    if software_schedule.values is not None:
        supported_historical_periods.update(software_schedule.values.values)
    software_balance.formula_periods = sorted(
        (set(historical_periods) & supported_historical_periods)
        | set(projection_periods)
    )
    software_balance.values = None

    software_ending = model.get_item(
        "tpl.a.depreciation_amortization.end_capitalized_internal_use_software"
    )
    software_ending.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="reported_capitalized_software_balance",
        params={"source": LineItemRef(id=software_schedule.id)},
        note="exact reported capitalized-software balance",
    )
    if software_schedule.values is not None:
        if software_ending.values is None:
            software_ending.values = ValueSeries()
        for period, cell in software_schedule.values.values.items():
            if int(period) in historical_periods:
                software_ending.values.values[int(period)] = cell.model_copy(
                    deep=True
                )

    current_liability_3 = model.get_item(
        "tpl.fm.balance_sheet.current_liability_3"
    )
    current_liability_3.historical = None
    current_liability_3.formula_periods = list(projection_periods)

    historical_exclusions = {
        "tpl.fm.balance_sheet.current_liability_3",
        "tpl.fm.cash_flow.long_term_asset_2",
        "tpl.fm.cash_flow.other_long_term_liabilities",
        "tpl.fm.cash_flow.other_cash_flows_from_financing",
        *(change_id for _balance_id, change_id in dependent_rows),
    }

    def remove_historical_refs(value: Any) -> None:
        if isinstance(value, list):
            value[:] = [
                nested
                for nested in value
                if not (
                    isinstance(nested, LineItemRef)
                    and nested.id in historical_exclusions
                )
            ]
            for nested in value:
                remove_historical_refs(nested)
        elif isinstance(value, dict):
            for key, nested in tuple(value.items()):
                if (
                    isinstance(nested, LineItemRef)
                    and nested.id in historical_exclusions
                ):
                    value.pop(key)
                    continue
                remove_historical_refs(nested)

    for item in _model_iter_items(model):
        if item.historical is not None:
            remove_historical_refs(item.historical.params)

    model.build_index()

def _lower_scenario_case_probabilities(
    model: FinancialModel,
    scenario_set_binding: ModelSpecScenarioSetBinding | None,
) -> None:
    """Write the bound ScenarioSet's authored bull/base/bear weights.

    The accepted ScenarioSet owns scenario probabilities; there is no house
    default.  A build with no bound set, or a set that authored no weights,
    writes nothing and the expected-value probability cells stay unsourced.
    """

    if scenario_set_binding is None or scenario_set_binding.case_probabilities is None:
        return
    probabilities = scenario_set_binding.case_probabilities
    first_projection = int(model.time_structure.projection_periods[0])
    for item_id, value in (
        ("tpl.s.expected_value.bull_probability", probabilities.bull),
        ("tpl.s.expected_value.base_probability", probabilities.base),
        ("tpl.s.expected_value.bear_probability", probabilities.bear),
    ):
        item = model.get_item(item_id)
        item.item_type = ItemType.input
        item.values = ValueSeries(
            values={
                first_projection: ValueCell(
                    period=first_projection,
                    value=value,
                    provenance=ValueProvenance.input,
                    note=(
                        "scenario_set_case_probability:"
                        f"{scenario_set_binding.scenario_set_id}"
                    ),
                    lineage_reference=scenario_set_binding.scenario_set_id,
                )
            }
        )
        item.formula_periods = [first_projection]
        item.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": value},
            note="scenario_case_probability",
            derivation=FormulaDerivation(
                owner="build_scenarios",
                strategy="scenario_case_probability",
                version=1,
            ),
        )
    selector_label = model.get_item("tpl.s.earnings_scenarios.scenario_selector_label")
    if selector_label.projected is not None:
        selector_label.projected = selector_label.projected.model_copy(
            update={
                "derivation": FormulaDerivation(
                    owner="build_scenarios",
                    strategy="scenario_control_label",
                    version=1,
                )
            },
            deep=True,
        )
    model.build_index()


def _scenario_lowering_diagnostics(
    model: FinancialModel,
    scenarios: Iterable[ScenarioInputs],
    *,
    bound_control_ids: Iterable[str] = (),
    workbook_targets: Mapping[str, str] = MappingProxyType({}),
) -> tuple[Any, ...]:
    """Return blocking diagnostics for every unbound authored control.

    ``workbook_targets`` is the model kind's declared scenario control plane
    (plan §3 row A1); a model kind that declares none resolves controls only
    by their own item ids, which is the generic arm.
    """

    from .build_model_product_contracts import BuildDiagnostic

    diagnostics: list[BuildDiagnostic] = []
    authenticated_controls = set(bound_control_ids)
    for scenario in sorted(scenarios, key=lambda item: item.name):
        for control_id in sorted(scenario.assumptions):
            if control_id in authenticated_controls:
                continue
            item_id = workbook_targets.get(
                control_id,
                control_id if control_id in model._index else None,
            )
            if item_id is not None and item_id in model._index:
                continue
            diagnostics.append(
                BuildDiagnostic(
                    code="compile_failure",
                    message=(
                        f"Scenario {scenario.name!r} declares unbound control "
                        f"{control_id!r}; the scenario cannot fall back to base."
                    ),
                    semantic_item_id=control_id,
                )
            )
    return tuple(diagnostics)


def _compute_model_spec_sensitivity_results(
    model: FinancialModel,
    model_spec: Any,
    compiled_registry: Any | None,
    *,
    scenario_controls: ScenarioControlDeclaration | None = None,
    sensitivity_bindings: Mapping[str, SensitivityBindingDeclaration] | None = None,
) -> dict[str, object]:
    """Evaluate authored sensitivity grids on the strict dependency graph.

    ``scenario_controls`` is the model kind's declared control plane (plan §3
    row A8): it supplies the fallback control-to-workbook binding and the
    legacy assumption namespace whose ids may stand in for their drivers. A
    model kind that declares none resolves controls through the compiled
    driver registry alone.
    """

    forecast_control_targets: Mapping[str, str] = (
        MappingProxyType({})
        if scenario_controls is None
        else scenario_controls.forecast_control_target_map()
    )
    legacy_assumption_namespace = (
        None if scenario_controls is None else scenario_controls.legacy_assumption_namespace
    )

    policy = getattr(model_spec, "valuation_policy", None)
    if policy is None:
        return {}
    projection_periods = [
        int(period) for period in model.time_structure.projection_periods
    ]
    if not projection_periods:
        return {}
    assumptions = {
        assumption.assumption_id: assumption
        for assumption in model_spec.forecast_assumptions
    }
    declared_gaps = {
        gap.semantic_item_id for gap in model_spec.declared_gaps
    }
    node_items = (
        compiled_registry.node_items
        if compiled_registry is not None
        else {}
    )
    driver_keys = (
        compiled_registry.driver_keys
        if compiled_registry is not None
        else {}
    )
    # §14 IG-4 item 15(m): the authored grid names its axes and outputs with
    # this model kind's own selector ids.  Which compiled row each one resolves
    # to is declared, not hand-mapped here; a kind that declares none resolves
    # its axes through the compiled driver registry alone and reports no
    # declared output row.
    bindings: Mapping[str, SensitivityBindingDeclaration] = (
        sensitivity_bindings or {}
    )
    declared_outputs = {
        selector_id: binding
        for selector_id, binding in bindings.items()
        if binding.role == "output"
    }
    declared_controls = {
        selector_id: binding
        for selector_id, binding in bindings.items()
        if binding.role == "control"
    }

    def resolve_declared(
        binding: SensitivityBindingDeclaration | None,
    ) -> str | None:
        return resolve_sensitivity_binding(
            binding,
            node_items=node_items,
            driver_keys=driver_keys,
        )

    def resolve_control(control_id: str) -> str | None:
        if control_id in declared_controls:
            return resolve_declared(declared_controls[control_id])
        assumption = assumptions.get(control_id)
        if assumption is None:
            return None
        if compiled_registry is not None:
            resolved = compiled_registry.driver_keys.get(assumption.driver)
            if resolved is not None:
                return resolved
        return forecast_control_targets.get(assumption.driver)

    graph = DependencyGraph()
    graph.build(model)
    baseline_results = graph.compute({})
    results: dict[str, object] = {}
    for sensitivity in policy.sensitivities:
        row_item_id = resolve_control(sensitivity.row_axis.control_id)
        column_item_id = resolve_control(sensitivity.column_axis.control_id)
        output_bindings = {
            output_id: resolve_declared(declared_outputs.get(output_id))
            for output_id in sensitivity.output_ids
        }
        blocking_controls = [
            control_id
            for control_id in (
                sensitivity.row_axis.control_id,
                sensitivity.column_axis.control_id,
            )
            if (
                control_id in declared_gaps
                or (
                    legacy_assumption_namespace is not None
                    and control_id.startswith(legacy_assumption_namespace)
                    and assumptions.get(control_id) is not None
                    and assumptions[control_id].driver in declared_gaps
                )
            )
        ]
        unbound = [
            semantic_id
            for semantic_id, item_id in (
                (sensitivity.row_axis.control_id, row_item_id),
                (sensitivity.column_axis.control_id, column_item_id),
                *tuple(output_bindings.items()),
            )
            if item_id is None or item_id not in model._index
        ]
        if blocking_controls or unbound:
            results[sensitivity.sensitivity_id] = {
                "status": "blocked",
                "blocking_controls": sorted(blocking_controls),
                "unbound_semantic_ids": sorted(unbound),
                "runs": (),
            }
            continue

        runs: list[_SensitivityRun] = []
        for row_value in sensitivity.row_axis.ordered_values:
            for column_value in sensitivity.column_axis.ordered_values:
                assert row_item_id is not None
                assert column_item_id is not None
                inputs = {
                    row_item_id: {
                        period: float(row_value)
                        for period in projection_periods
                    },
                    column_item_id: {
                        period: float(column_value)
                        for period in projection_periods
                    },
                }
                recompute = _downstream_item_ids(
                    graph,
                    {row_item_id, column_item_id},
                )
                computed = graph.compute_typed(
                    inputs,
                    recompute=recompute,
                    periods=set(projection_periods),
                    seed_results=baseline_results,
                    # The policy pins every non-varied ModelSpec input.  Do
                    # not bypass unrelated FAS constant overrides merely
                    # because broad accounting cycles place them downstream
                    # of a sensitivity root.
                    propagate_roots=set(),
                )
                target_period = projection_periods[-1]
                output_states: dict[str, dict[str, object]] = {}
                for output_id, item_id in output_bindings.items():
                    assert item_id is not None
                    value = computed.get(item_id, {}).get(target_period)
                    output_states[output_id] = (
                        {
                            "state": "missing",
                            "reason": "source_value_unavailable",
                        }
                        if value is None
                        else {
                            "state": value.state,
                            "value": (
                                value.value if value.state == "known" else None
                            ),
                            "reason": getattr(value, "primary_reason", None),
                        }
                    )
                runs.append(
                    {
                        "row_value": float(row_value),
                        "column_value": float(column_value),
                        "period": target_period,
                        "outputs": output_states,
                    }
                )
        results[sensitivity.sensitivity_id] = {
            "status": (
                "computed"
                if all(
                    output["state"] == "known"
                    for run in runs
                    for output in run["outputs"].values()
                )
                else "blocked"
            ),
            "row_item_id": row_item_id,
            "column_item_id": column_item_id,
            "runs": tuple(runs),
        }
    return results


def _populate_scenario_inputs(
    model: FinancialModel,
    configured: Iterable[ScenarioInputs] = (),
    *,
    authenticated_scenario_curves: bool = False,
) -> None:
    scenario_inputs_cls = ScenarioInputs
    defaults = {
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
    configured_by_name = {
        str(scenario.name).strip().lower(): scenario
        for scenario in configured
    }
    if authenticated_scenario_curves:
        # Authenticated control curves replace case factors, not the
        # template's case selector. Preserve the selector in every case and
        # pin the rendered model to Base so typed validation resolves the same
        # exact case that the workbook presents by default.
        model.scenarios = {
            case: (
                default
                if (authored := configured_by_name.get(case)) is None
                else authored.model_copy(
                    update={
                        "assumptions": {
                            **default.assumptions,
                            **authored.assumptions,
                        }
                    }
                )
            )
            for case, default in defaults.items()
        }
        selector_id = "tpl.a.header.scenario_value"
        if selector_id in model._index:
            selector = model.get_item(selector_id)
            if selector.overrides is None:
                selector.overrides = {}
            for period in model.time_structure.projection_periods:
                selector.overrides[int(period)] = FormulaSpec(
                    type=FormulaType.constant,
                    params={"value": 2.0},
                    note="base_scenario_selector",
                )
    else:
        model.scenarios = {
            case: (
                default
                if (authored := configured_by_name.get(case)) is None
                else authored.model_copy(
                    update={
                        "assumptions": {
                            **default.assumptions,
                            **authored.assumptions,
                        }
                    }
                )
            )
            for case, default in defaults.items()
        }


def _build_scenario_overrides(
    model: FinancialModel,
    case: str,
    base_results: ComputedValues | None = None,
) -> dict[str, dict[int, float]]:
    """Build per-period direct overrides for OFFSET-targeted assumption rows."""

    if not model._index:
        model.build_index()
    case = case.lower()

    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not projection_periods:
        return {}

    configured = {
        str(name).strip().lower(): scenario
        for name, scenario in (model.scenarios or {}).items()
    }
    authored = configured.get(case)
    overrides: dict[str, dict[int, float]] = {}
    if authored is not None:
        # An ``sbc_pct`` scenario value on a disclosed SBC split scales the
        # disclosed mix as the base case does (one rule, its owner's).
        from .build_company_grounded_schedules import (
            scale_aggregate_sbc_scenario_values,
        )
        from .scenario_bridge import normalize_control_magnitude

        for control_id, raw_value in sorted(authored.assumptions.items()):
            target_id = control_id if control_id in model._index else None
            if target_id is None or target_id not in model._index:
                continue
            if isinstance(raw_value, dict):
                values = {
                    int(period): float(value)
                    for period, value in raw_value.items()
                    if int(period) in projection_periods
                    and math.isfinite(float(value))
                }
                if set(values) != set(projection_periods):
                    continue
            else:
                value = float(raw_value)
                if not math.isfinite(value):
                    continue
                values = {period: value for period in projection_periods}
            values = {
                period: normalize_control_magnitude(target_id, value)
                for period, value in values.items()
            }
            overrides[target_id] = scale_aggregate_sbc_scenario_values(
                model.get_item(target_id), values
            )

    dependency_graph_cls = DependencyGraph
    if base_results is None:
        scenario_graph = dependency_graph_cls()
        scenario_graph.build(model)
        base_results = scenario_graph.compute({})
    assert base_results is not None

    formula_type = FormulaType
    line_item_ref_from_obj = _line_item_ref_from_obj


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
                    continue  # the evaluator leaves an anchorless OFFSET cell unresolved
                anchor_id = anchor_ref.id
                value_row_id = find_scenario_value_row(model, anchor_id, case)
                if value_row_id is None:
                    continue
                row_results = base_results.get(value_row_id)
                if row_results is not None:
                    period_values = {
                        period: value
                        for period in projection_periods
                        if (value := row_results.get(period)) is not None
                    }
                    if period_values:
                        overrides.setdefault(item_obj.id, period_values)
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
    projected_formula_refs_any = _projected_formula_refs_any
    formula_type = FormulaType

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
    extract_ref_ids = _formula_extract_ref_ids
    return bool(extract_ref_ids(spec.params or {}) & item_ids)


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
    "_SCENARIO_EPS_ITEM_RE",
    "_SCENARIO_EPS_LIMIT",
    "_build_scenario_overrides",
    "_bind_declared_model_spec_controls",
    "_bind_model_spec_control_units",
    "_compute_model_spec_sensitivity_results",
    "_finalize_authenticated_dependents",
    "_lower_scenario_case_probabilities",
    "_materialize_declared_share_conversion",
    "_downstream_item_ids",
    "_finite_projection_values",
    "_populate_scenario_eps",
    "_populate_thesis_snapshot",
    "_populate_scenario_inputs",
    "_scenario_lowering_diagnostics",
    "_projected_formula_refs_any",
    "_scenario_compute_inputs",
    "_select_scenario_output_item_id",
    "compute_scenario_eps",
    "compute_scenario_outputs",
    "scenario_results_payload",
]
