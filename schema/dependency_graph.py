"""Dependency graph engine for schema-based financial models.

Purpose:
- Build a directed graph of line-item dependencies from FormulaSpecs.
- Compute values period-by-period without relying on Excel.

Core mechanics:
- Same-period edges (t=0) form a DAG plus possible cycles.
- Prior-period refs (t=-1) are treated as time recursion, not graph cycles.
- Strongly connected components (SCCs) are solved via fixed-point iteration.

Example:
If gross_profit = revenue - cogs and operating_income = gross_profit - opex,
the graph orders revenue/cogs/opex before gross_profit and operating_income.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import logging
import math
from typing import Any, Callable, Dict, Iterable, Iterator, List, Literal, Optional, Set, Tuple, TYPE_CHECKING, TypeAlias

from .dependency_graph_algorithms import (
    _Component,
    _components_from_adj,
    _downstream_of,
    _tarjan_sccs,
)
from .coverage_profile import AbsenceCitation
from .dependency_graph_eval import (
    _apply_scale_fn,
    _eval,
    _eval_expr,
    _eval_offset_scenario,
    _eval_valuation,
)
from .dependency_graph_periods import (
    _active_adjs_for_period,
    _active_adjs_for_period_subset,
    _components_for_period,
    _reachable_for_periods,
    _static_spec_for_period,
    active_adjacency_for_periods,
    downstream_for_periods,
    upstream_for_periods,
)
from .dependency_graph_values import (
    _bootstrap_period,
    _cached_computed_value_for_period,
    _cached_value_for_period,
    _column_for_period,
    _cycle_cached_fallback_enabled,
    _fixed_cell_anchor_period,
    _historical_periods,
    _input_value_fallback,
    _period_for_anchor_last,
    _projection_periods,
    _ratio_cached_fallback_enabled,
    _read_with_fallback,
    _row_item_id,
    _seed_inputs,
    _spec_for_period,
    _sum_range_target_periods,
    _target_sheet_all_periods,
    _target_sheet_projection_periods,
    _time_order,
    _value_of,
)
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ValueProvenance,
)
from .model_number_value import (
    AbsenceDisposition,
    AbsentCause,
    EvidencedAbsentNumberValue,
    InvalidCause,
    InvalidNumberValue,
    KnownNumberValueBase,
    MissingCause,
    MissingNumberValue,
    UnsupportedCause,
    UnsupportedNumberValue,
    ZeroDisposition,
    _cause_key,
    is_evidenced_zero,
)
from .refs import FormulaRef, line_item_ref_from_obj
from .renderer_columns import item_owns_cell
from .stable_control_identity import FiscalPeriod

if TYPE_CHECKING:
    from .handle import ComputedValues

logger = logging.getLogger(__name__)


@dataclass
class CycleBlock:
    nodes: List[str]
    max_iter: int = 100
    tol: float = 1e-6


class DependencyGraphKnownNumberValue(KnownNumberValueBase):
    """Known in-process graph value with the existing provenance vocabulary.

    The legacy graph does not own immutable compiled-artifact refs, so this
    projection intentionally does not fabricate a ``FormulaOriginBinding``.
    Acceptance-authoritative serialized values continue to use the stricter
    ``KnownNumberValue`` variants in :mod:`schema.model_number_value`.
    """

    provenance: ValueProvenance


DependencyGraphNumberValue: TypeAlias = (
    DependencyGraphKnownNumberValue
    | MissingNumberValue
    | UnsupportedNumberValue
    | InvalidNumberValue
    | EvidencedAbsentNumberValue
)

# Valuation subtypes that are partial functions by ruling: outside the domain
# the quantity does not exist, so the cell is an evidenced absence rather than
# a failed evaluation (``break_even_probability`` ruled 2026-09-30).  Law 6 of
# the cell algebra in :meth:`DependencyGraph.compute_typed` reads this set.
DECLARED_DOMAIN_VALUATION_SUBTYPES = frozenset({"break_even_probability"})

# A present node-period entry may be None when evaluation completed without a
# usable number; cold or unevaluated node-periods remain absent from the matrix.
DependencyGraphResults: TypeAlias = Dict[str, Dict[int, Optional[float]]]


def formula_owned_recompute_ids(model: FinancialModel) -> set[str]:
    """Return rows whose formulas, rather than cached forecast values, own output.

    BusinessModel growth-level rows intentionally retain historical input cells
    while carrying code-owned projection formulas. A derived-only forced pass
    therefore drops their forecast roll-forwards. Recompute those projected
    formula rows as well; historical periods still retain their accepted seed
    because no historical formula is selected for those periods.
    """

    if not model._index:
        model.build_index()
    return {
        item.id
        for item in model._index.values()
        if item.item_type is ItemType.derived or item.projected is not None
    }


class DependencyGraph:
    _apply_scale_fn = _apply_scale_fn
    _bootstrap_period = _bootstrap_period
    _cached_computed_value_for_period = _cached_computed_value_for_period
    _cached_value_for_period = _cached_value_for_period
    _column_for_period = _column_for_period
    _components_from_adj = _components_from_adj
    _components_for_period = _components_for_period
    _cycle_cached_fallback_enabled = _cycle_cached_fallback_enabled
    _downstream_of = _downstream_of
    _eval = _eval
    _eval_expr = _eval_expr
    _eval_offset_scenario = _eval_offset_scenario
    _eval_valuation = _eval_valuation
    _fixed_cell_anchor_period = _fixed_cell_anchor_period
    _historical_periods = _historical_periods
    _input_value_fallback = _input_value_fallback
    _period_for_anchor_last = _period_for_anchor_last
    _projection_periods = _projection_periods
    _ratio_cached_fallback_enabled = _ratio_cached_fallback_enabled
    _reachable_for_periods = _reachable_for_periods
    _read_with_fallback = _read_with_fallback
    _row_item_id = _row_item_id
    _seed_inputs = _seed_inputs
    _spec_for_period = _spec_for_period
    _static_spec_for_period = _static_spec_for_period
    _sum_range_target_periods = _sum_range_target_periods
    _tarjan_sccs = _tarjan_sccs
    _target_sheet_all_periods = _target_sheet_all_periods
    _target_sheet_projection_periods = _target_sheet_projection_periods
    _time_order = _time_order
    _value_of = _value_of
    _active_adjs_for_period = _active_adjs_for_period
    _active_adjs_for_period_subset = _active_adjs_for_period_subset
    active_adjacency_for_periods = active_adjacency_for_periods
    downstream_for_periods = downstream_for_periods
    upstream_for_periods = upstream_for_periods

    def __init__(self) -> None:
        self.model: Optional[FinancialModel] = None
        self.nodes: Set[str] = set()
        self.adj: Dict[str, Set[str]] = {}
        self.time_edges: Dict[str, Set[LineItemRef]] = {}
        self.cycle_blocks: List[CycleBlock] = []
        self.components: List[_Component] = []
        self.component_order: List[int] = []
        self.missing_refs: Set[str] = set()
        self._compute_has_recompute: bool = False
        self._compute_recompute: Optional[Set[str]] = None
        self._compute_propagate: Optional[Set[str]] = None  # downstream of user inputs
        self._ratio_zero_denominator_policy: str = "strict"
        self._cycle_fallback_policy: str = "auto"
        self._compute_periods: Optional[Set[int]] = None
        self._compute_seed_results: Optional[ComputedValues] = None
        self._global_cycle_node_ids: Set[str] = set()
        self._item_sheet_map: Dict[str, str] = {}
        self._item_row_map: Dict[str, int] = {}
        self._item_column_map: Dict[str, str] = {}
        self._row_item_map: Dict[Tuple[str, int], List[str]] = {}
        self._cell_item_map: Dict[Tuple[str, int, str], str] = {}
        self._deferred_nodes: Set[str] = set()
        self._deferred_cone: Set[str] = set()
        self._sum_range_nodes: Set[str] = set()
        self._unresolved_sum_range_nodes: Set[str] = set()
        self._unresolved_cone: Set[str] = set()
        self._period_anchor_last_nodes: Set[str] = set()
        self._current_eval_cell: Optional[Tuple[str, int]] = None
        self.last_eval_reads: Dict[Tuple[str, int], Tuple[Tuple[str, int], ...]] = {}
        self.last_typed_results: Dict[
            str, Dict[int, DependencyGraphNumberValue]
        ] = {}

    @property
    def _loaded_model(self) -> FinancialModel:
        """Return the model after enforcing the graph's build invariant."""
        model = self.model
        if model is None:
            raise ValueError("DependencyGraph.build() must be called before compute().")
        return model

    def build(self, model: FinancialModel) -> None:
        """Build the dependency graph from FormulaSpecs.

        Steps:
        - Extract all LineItemRef dependencies from each FormulaSpec.
        - Build adjacency lists for same-period refs.
        - Detect SCCs and build a component DAG for ordering.
        """
        self.model = model
        self._global_cycle_node_ids = set()
        self._item_sheet_map = {}
        self._item_row_map = {}
        self._item_column_map = {}
        self._row_item_map = {}
        self._cell_item_map = {}
        self._deferred_nodes = set()
        self._deferred_cone = set()
        self._sum_range_nodes = set()
        self._unresolved_sum_range_nodes = set()
        self._unresolved_cone = set()
        self._period_anchor_last_nodes = set()
        if not model.sheets:
            self.nodes = set()
            return

        model.build_index()
        self.nodes = set(model._index.keys())
        self._item_sheet_map = {
            item.id: sheet_name
            for sheet_name, sheet in model.sheets.items()
            for section in sheet.sections
            for item in section.line_items
        }
        for sheet_name, sheet in model.sheets.items():
            for section in sheet.sections:
                for item in section.line_items:
                    row = int(item.row)
                    self._item_row_map[item.id] = row
                    self._row_item_map.setdefault((sheet_name, row), []).append(item.id)
                    if item.column:
                        column = str(item.column).upper()
                        self._item_column_map[item.id] = column
                        self._cell_item_map[(sheet_name, row, column)] = item.id
        self.adj = {node: set() for node in self.nodes}
        self.time_edges = {node: set() for node in self.nodes}
        self.missing_refs = set()

        for item in model._index.values():
            for spec in self._iter_formula_specs(item):
                for _carrier, ref in self._read_refs(spec):
                    if ref.id not in self.nodes:
                        self.missing_refs.add(ref.id)
                        continue
                    if ref.t == 0:
                        self.adj[ref.id].add(item.id)
                    else:
                        self.time_edges[item.id].add(ref)

        self._identify_deferred_nodes()
        self._validate_deferred_cone_invariant()

        self.components, self.component_order = self._components_from_adj(self.adj)
        self.cycle_blocks = [
            CycleBlock(nodes=comp.nodes)
            for comp in self.components
            if comp.is_cycle
        ]
        for cb in self.cycle_blocks:
            self._global_cycle_node_ids.update(cb.nodes)

    def compute(
        self,
        inputs: Dict[str, Dict[int, float]],
        recompute: Optional[Set[str]] = None,
        cycle_fallback_policy: Literal["off", "auto", "on", "auto_propagate"] = "auto",
        ratio_zero_denominator_policy: Literal[
            "strict", "auto_fallback_cached", "fallback_cached"
        ] = "strict",
        periods: Optional[Set[int]] = None,
        seed_results: Optional[ComputedValues] = None,
        propagate_roots: Optional[Set[str]] = None,
    ) -> DependencyGraphResults:
        """Compute model values period-by-period given input overrides.

        For each period:
        - Seed input values and any stored ValueSeries (unless recompute).
        - Evaluate DAG components in topological order.
        - Solve any cycle blocks with fixed-point iteration.

        recompute:
        - Optional set of line_item_ids to force recomputation even if
          ValueSeries has existing values.

        periods:
        - Optional subset of periods to actively evaluate. Periods outside this
          set are resolved from input/value cache and optional seed_results.

        seed_results:
        - Optional baseline matrix used as fallback when a scoped compute needs
          values from non-evaluated periods.

        propagate_roots:
        - Optional subset of input ids whose downstream constant overrides
          should be bypassed. Defaults to all input ids.
        """
        model = self._loaded_model
        if cycle_fallback_policy not in {"off", "auto", "on", "auto_propagate"}:
            raise ValueError("cycle_fallback_policy must be one of: off, auto, on, auto_propagate")
        if ratio_zero_denominator_policy not in {"strict", "auto_fallback_cached", "fallback_cached"}:
            raise ValueError(
                "ratio_zero_denominator_policy must be one of: strict, auto_fallback_cached, fallback_cached"
            )

        time_order = self._time_order()
        time_index = {period: idx for idx, period in enumerate(time_order)}
        eval_periods = time_order if not periods else [period for period in time_order if period in periods]
        results: DependencyGraphResults = {}

        self._compute_has_recompute = bool(recompute)
        self._compute_recompute = recompute
        self._compute_periods = set(eval_periods) if periods else None
        self._compute_seed_results = seed_results
        # Compute downstream set of user input overrides — these items should
        # have their constant overrides bypassed so formula propagation works.
        if inputs and recompute:
            roots = set(inputs.keys()) if propagate_roots is None else set(propagate_roots)
            self._compute_propagate = self._downstream_of(roots) if roots else set()
        else:
            self._compute_propagate = None
        self._ratio_zero_denominator_policy = ratio_zero_denominator_policy
        self._cycle_fallback_policy = cycle_fallback_policy
        self.last_eval_reads = {}

        try:
            for period in eval_periods:
                self._seed_inputs(period, inputs, results, recompute)
                components, order = self._components_for_period(period)
                singleton_node_ids: List[str] = []
                for comp_id in order:
                    comp = components[comp_id]
                    if comp.is_cycle:
                        active_nodes = [node for node in comp.nodes if node not in self._deferred_nodes]
                        if not active_nodes:
                            continue
                        if len(active_nodes) != len(comp.nodes):
                            self._solve_period_active_cycle_component(
                                active_nodes,
                                period,
                                results,
                                time_index,
                                time_order,
                                inputs=inputs,
                            )
                            singleton_node_ids.extend(active_nodes)
                            continue
                        self._solve_cycle_block(
                            comp.nodes,
                            period,
                            results,
                            time_index,
                            time_order,
                            inputs=inputs,
                        )
                        continue
                    node_id = comp.nodes[0]
                    if node_id in self._deferred_nodes:
                        continue
                    singleton_node_ids.append(node_id)
                    self._eval_singleton_node(
                        node_id, period, results, time_index, time_order
                    )

                # Re-sweep unresolved singleton nodes to recover values when
                # unresolved ordering edges force an arbitrary first pass.
                for _ in range(len(singleton_node_ids)):
                    unresolved = 0
                    progressed = False
                    for node_id in singleton_node_ids:
                        if results.get(node_id, {}).get(period) is not None:
                            continue
                        unresolved += 1
                        resolved_now = self._eval_singleton_node(
                            node_id, period, results, time_index, time_order
                        )
                        if resolved_now:
                            progressed = True
                    if unresolved == 0 or not progressed:
                        break

            for period in eval_periods:
                for item_id, item in model._index.items():
                    if item_id in self._deferred_nodes:
                        continue
                    if not item.overrides or period not in item.overrides:
                        continue
                    spec = item.overrides[period]
                    if spec.type == FormulaType.constant:
                        continue
                    value = self._eval(item_id, period, results, time_index, time_order)
                    if value is not None:
                        results.setdefault(item_id, {})[period] = value
                    else:
                        logger.debug("Override for %s period=%s returned None (missing deps)", item_id, period)

            self._evaluate_deferred_nodes(
                results,
                time_index,
                time_order,
                eval_periods,
                inputs,
            )
        finally:
            self._compute_periods = None
            self._compute_seed_results = None

        return results

    def compute_typed(
        self,
        inputs: Dict[str, Dict[int, float]],
        recompute: Optional[Set[str]] = None,
        cycle_fallback_policy: Literal["off", "auto", "on", "auto_propagate"] = "auto",
        ratio_zero_denominator_policy: Literal[
            "strict", "auto_fallback_cached", "fallback_cached"
        ] = "strict",
        periods: Optional[Set[int]] = None,
        seed_results: Optional[ComputedValues] = None,
        propagate_roots: Optional[Set[str]] = None,
        *,
        absences: Mapping[Tuple[str, int], EvidencedAbsentNumberValue] = {},
        evidenced_zeros: Mapping[Tuple[str, int], ZeroDisposition] = {},
    ) -> Dict[str, Dict[int, DependencyGraphNumberValue]]:
        """Compute and project every requested node-period into a typed state.

        This is the strict compiler-facing view of the live graph.  Numeric
        callers may continue to use :meth:`compute`, but a missing,
        unsupported, or invalid critical node remains explicit here and is
        recursively propagated to its dependents.

        FormulaSpec has no optional-edge authority.  Consequently every live
        graph dependency is required.  Authorized optional substitution is
        represented only by ``DependencyEdgePolicy`` in the compiled semantic
        graph contracts and is never inferred from a blank or ``None``.
        """

        results = self.compute(
            inputs,
            recompute=recompute,
            cycle_fallback_policy=cycle_fallback_policy,
            ratio_zero_denominator_policy=ratio_zero_denominator_policy,
            periods=periods,
            seed_results=seed_results,
            propagate_roots=propagate_roots,
        )
        time_order = self._time_order()
        time_index = {period: index for index, period in enumerate(time_order)}
        eval_periods = (
            time_order
            if not periods
            else [period for period in time_order if period in periods]
        )
        memo: Dict[Tuple[str, int], DependencyGraphNumberValue] = {}
        visiting: Set[Tuple[str, int]] = set()

        historical_periods = tuple(int(p) for p in self._historical_periods())
        historical_periods_set = set(historical_periods)
        projection_periods_set = {int(p) for p in self._projection_periods()}
        law4_all_history_absent: Dict[str, bool] = {}

        def fiscal_period(period: int) -> FiscalPeriod:
            return FiscalPeriod(
                period_id=str(period),
                fiscal_year=int(period),
                period_type="annual",
                is_estimate=int(period) in projection_periods_set,
            )

        def unavailable(
            state: Literal["missing", "unsupported", "invalid"],
            reason: str,
            item_id: str,
            period: int,
        ) -> DependencyGraphNumberValue:
            fields = {
                "node_id": item_id,
                "period": fiscal_period(period),
                "expression_path": "root",
                "reason_code": reason,
            }
            if state == "missing":
                cause = MissingCause(state="missing", **fields)
                return MissingNumberValue(
                    state="missing",
                    primary_reason=cause.reason_code,
                    causes=(cause,),
                )
            if state == "unsupported":
                cause = UnsupportedCause(state="unsupported", **fields)
                return UnsupportedNumberValue(
                    state="unsupported",
                    primary_reason=cause.reason_code,
                    causes=(cause,),
                )
            cause = InvalidCause(state="invalid", **fields)
            return InvalidNumberValue(
                state="invalid",
                primary_reason=cause.reason_code,
                causes=(cause,),
            )
        # Cells a law-2 growth absence was derived for: their readers are
        # ``invalid/domain_error`` with the growth cell as cause, never absent.
        law2_absent_cells: Set[Tuple[str, int]] = set()

        def absent_cause(
            item_id: str, period: int, reason: AbsenceDisposition
        ) -> AbsentCause:
            return AbsentCause(
                state="evidenced_absent",
                node_id=item_id,
                period=fiscal_period(period),
                expression_path="root",
                reason_code=reason,
            )

        def evidenced_absent(
            causes: Iterable[AbsentCause],
            *,
            citation: Optional[AbsenceCitation] = None,
            included_in: Optional[str] = None,
        ) -> EvidencedAbsentNumberValue:
            canonical = tuple(sorted(set(causes), key=_cause_key))
            return EvidencedAbsentNumberValue(
                state="evidenced_absent",
                primary_reason=canonical[0].reason_code,
                causes=canonical,
                citation=citation,
                included_in=included_in,
            )

        def target_period(ref: LineItemRef, period: int) -> Optional[int]:
            try:
                target_item = self._loaded_model.get_item(ref.id)
            except KeyError:
                target_item = None
            if target_item is not None and target_item.column is not None:
                if int(ref.t) != 0:
                    return None
                return self._fixed_cell_anchor_period(target_item)
            if ref.period_anchor == "last":
                return self._period_for_anchor_last(ref.id, int(ref.t))
            try:
                index = time_order.index(period) + int(ref.t)
            except ValueError:
                return None
            if index < 0 or index >= len(time_order):
                return None
            return int(time_order[index])

        def known_provenance(item_id: str, period: int) -> ValueProvenance:
            if item_id in inputs and period in inputs[item_id]:
                return ValueProvenance.input
            item = self._loaded_model.get_item(item_id)
            if item.values is not None:
                cell = item.values.values.get(period)
                if cell is not None and cell.value is not None:
                    return cell.provenance
            return ValueProvenance.computed

        def classify(item_id: str, period: int) -> DependencyGraphNumberValue:
            key = (item_id, int(period))
            if key in memo:
                return memo[key]
            if key in visiting:
                return unavailable(
                    "missing", "cycle_seed_missing", item_id, int(period)
                )

            visiting.add(key)
            try:
                item = self._loaded_model.get_item(item_id)
                spec = self._spec_for_period(item, int(period))
                numeric = results.get(item_id, {}).get(int(period))
                if (
                    numeric is None
                    and seed_results is not None
                    and (
                        not recompute
                        or item_id not in recompute
                        or int(period) not in eval_periods
                    )
                ):
                    numeric = seed_results.get(item_id, {}).get(int(period))
                if (
                    numeric is not None
                    and math.isfinite(numeric)
                    and item_id in inputs
                    and int(period) in inputs[item_id]
                ):
                    value = DependencyGraphKnownNumberValue(
                        state="known",
                        value=float(numeric),
                        provenance=ValueProvenance.input,
                        evidenced_zero=(
                            evidenced_zeros.get(key) if float(numeric) == 0.0 else None
                        ),
                    )
                    memo[key] = value
                    return value
                if spec is None:
                    if numeric is not None and math.isfinite(numeric):
                        value = DependencyGraphKnownNumberValue(
                            state="known",
                            value=float(numeric),
                            provenance=known_provenance(item_id, int(period)),
                            evidenced_zero=(
                                evidenced_zeros.get(key) if float(numeric) == 0.0 else None
                            ),
                        )
                    elif numeric is not None:
                        value = unavailable(
                            "invalid", "non_finite_result", item_id, int(period)
                        )
                    elif key in absences:
                        # A declared absence stands where the document holds no
                        # value: a stamped cell whose formula or snapshot gives a
                        # number keeps it (the zero policy's explicit ``0`` over a
                        # cited-absent history is an evidenced zero, never absent).
                        value = absences[key]
                    else:
                        value = unavailable(
                            "missing",
                            "source_value_unavailable",
                            item_id,
                            int(period),
                        )
                    memo[key] = value
                    return value
                if spec.type is FormulaType.raw:
                    params = spec.params or {}
                    has_lineage = bool(
                        params.get("required_role")
                        or params.get("constituent_role")
                        or params.get("included_in")
                        or params.get("absence_citation")
                    )
                    if spec.subtype in {
                        "not_separately_presented",
                        "not_applicable",
                    } and has_lineage:
                        citation_raw = params.get("absence_citation")
                        citation = (
                            citation_raw
                            if isinstance(citation_raw, AbsenceCitation)
                            else AbsenceCitation.model_validate(citation_raw)
                            if citation_raw
                            else None
                        )
                        included_in = params.get("included_in")
                        value = evidenced_absent(
                            (
                                absent_cause(
                                    item_id,
                                    int(period),
                                    (
                                        "not_separately_presented"
                                        if included_in
                                        else "not_applicable_evidenced"
                                    ),
                                ),
                            ),
                            citation=citation,
                            included_in=included_in,
                        )
                    else:
                        value = unavailable(
                            "unsupported",
                            "raw_excel_formula_semantics_unavailable",
                            item_id,
                            int(period),
                        )
                    memo[key] = value
                    return value

                # Inspect required dependencies even when ``compute`` found a
                # cached numeric value.  A cache is not authority to turn an
                # incomplete current formula into Known.
                child_entries: List[
                    Tuple[Optional[Tuple[str, int]], DependencyGraphNumberValue]
                ] = []
                prior_unavailable = False
                required_refs = self._extract_refs(
                    spec,
                    evaluate_condition=lambda expr: self._eval_expr(
                        expr, int(period), results, time_index, time_order
                    ),
                )
                if (
                    spec.type is FormulaType.valuation
                    and spec.subtype == "offset_scenario"
                ):
                    # ``offset_scenario`` resolves a physical table cell from
                    # the anchor, selector, and row/column geometry.  The
                    # anchor is a header rather than a numeric dependency, but
                    # the selector remains a required numeric input.  Keeping
                    # only that dependency when evaluation failed classifies
                    # an absent selector as Missing rather than a domain error.
                    # A numeric result already proves that the fixed or
                    # period-relative selector and selected cell resolved.
                    selector_ref = line_item_ref_from_obj(
                        (spec.params or {}).get("selector")
                    )
                    required_refs = (
                        []
                        if numeric is not None or selector_ref is None
                        else [selector_ref]
                    )
                if spec.type is FormulaType.valuation and spec.subtype == "scenario_select":
                    params = spec.params or {}
                    selector_ref = line_item_ref_from_obj(params.get("selector"))
                    if selector_ref is None:
                        required_refs = []
                    else:
                        selector_period = target_period(selector_ref, int(period))
                        selector_state = (
                            None
                            if selector_period is None or selector_ref.id not in self.nodes
                            else classify(selector_ref.id, selector_period)
                        )
                        if selector_state is None:
                            required_refs = [selector_ref]
                        elif selector_state.state != "known":
                            child_entries.append(
                                (
                                    None
                                    if selector_period is None
                                    else (selector_ref.id, int(selector_period)),
                                    selector_state,
                                )
                            )
                            required_refs = []
                        else:
                            selector_value = selector_state.value
                            selector_int = int(selector_value)
                            if selector_value != selector_int or selector_int not in {1, 2, 3}:
                                value = unavailable(
                                    "invalid", "domain_error", item_id, int(period)
                                )
                                memo[key] = value
                                return value
                            case = {1: "bull", 2: "base", 3: "bear"}[selector_int]
                            selected_ref = line_item_ref_from_obj(params.get(case))
                            required_refs = [] if selected_ref is None else [selected_ref]
                if spec.type is FormulaType.valuation and spec.subtype == "wacc":
                    params = spec.params or {}
                    weight_refs = {
                        name: line_item_ref_from_obj(params.get(name))
                        for name in ("weight_equity", "weight_debt")
                    }
                    required_refs = [
                        ref for ref in weight_refs.values() if ref is not None
                    ]
                    for weight_name, cost_name in (
                        ("weight_equity", "cost_equity"),
                        ("weight_debt", "cost_debt"),
                    ):
                        weight_ref = weight_refs[weight_name]
                        if weight_ref is None or weight_ref.id not in self.nodes:
                            continue
                        weight_period = target_period(weight_ref, int(period))
                        if weight_period is None:
                            continue
                        weight_state = classify(weight_ref.id, weight_period)
                        if weight_state.state != "known" or weight_state.value != 0.0:
                            cost_ref = line_item_ref_from_obj(params.get(cost_name))
                            if cost_ref is not None:
                                required_refs.append(cost_ref)
                for ref in required_refs:
                    if ref.id not in self.nodes:
                        missing_state = unavailable(
                            "missing",
                            "required_dependency_missing",
                            item_id,
                            int(period),
                        )
                        child_entries.append((None, missing_state))
                        continue
                    resolved_period = target_period(ref, int(period))
                    if resolved_period is None:
                        prior_unavailable = True
                        continue
                    child_state = classify(ref.id, resolved_period)
                    child_entries.append(
                        ((ref.id, int(resolved_period)), child_state)
                    )

                # Causes keep naming the node-period where the value became
                # unavailable. Same-rank failing children are aggregated so one
                # cited absence cannot hide another missing cause. Propagation
                # rank: ``evidenced_absent`` below ``missing`` below
                # ``unsupported`` below ``invalid`` — a cell with one owed
                # (unresolved) operand is owed, so the laws below run only
                # when no operand is unresolved. A reader of a law-2 growth
                # absence is not absent: it is ``invalid/domain_error`` with
                # the growth cell as cause, exactly the pre-algebra reader.
                def operand_state(
                    param_name: str,
                ) -> Tuple[Optional[Tuple[str, int]], Optional[DependencyGraphNumberValue]]:
                    operand_ref = line_item_ref_from_obj(
                        (spec.params or {}).get(param_name)
                    )
                    if operand_ref is None:
                        return None, None
                    operand_period = target_period(operand_ref, int(period))
                    if operand_period is None:
                        return None, None
                    operand_key = (operand_ref.id, int(operand_period))
                    for entry_key, entry_state in child_entries:
                        if entry_key == operand_key:
                            return operand_key, entry_state
                    if operand_key == key or operand_ref.id not in self.nodes:
                        return operand_key, None
                    return operand_key, classify(operand_ref.id, operand_period)

                absent_operands = [
                    (entry_key, entry_state)
                    for entry_key, entry_state in child_entries
                    if isinstance(entry_state, EvidencedAbsentNumberValue)
                ]
                failing: List[DependencyGraphNumberValue] = []
                for entry_key, entry_state in child_entries:
                    if isinstance(entry_state, EvidencedAbsentNumberValue):
                        if entry_key is not None and entry_key in law2_absent_cells:
                            failing.append(
                                InvalidNumberValue(
                                    state="invalid",
                                    primary_reason="domain_error",
                                    causes=(
                                        InvalidCause(
                                            state="invalid",
                                            node_id=entry_key[0],
                                            period=fiscal_period(entry_key[1]),
                                            expression_path="root",
                                            reason_code="domain_error",
                                        ),
                                    ),
                                )
                            )
                    elif entry_state.state != "known":
                        failing.append(entry_state)
                if not failing and not prior_unavailable:
                    if spec.type is FormulaType.ratio and spec.subtype == "yoy_growth":
                        denom_key, denom_state = operand_state("denominator")
                        if (
                            denom_key is not None
                            and denom_state is not None
                            and (
                                denom_state.state == "evidenced_absent"
                                or is_evidenced_zero(denom_state)
                                or (
                                    denom_state.state == "known"
                                    and denom_state.value == 0.0
                                )
                            )
                        ):
                            law2_absent_cells.add(key)
                            value = evidenced_absent(
                                (
                                    absent_cause(
                                        denom_key[0],
                                        denom_key[1],
                                        "not_applicable_evidenced",
                                    ),
                                ),
                                citation=(
                                    denom_state.citation
                                    if isinstance(
                                        denom_state, EvidencedAbsentNumberValue
                                    )
                                    else None
                                ),
                            )
                            memo[key] = value
                            return value
                    if spec.type is FormulaType.ratio and spec.subtype != "yoy_growth":
                        denom_key, denom_state = operand_state("denominator")
                        _num_key, num_state = operand_state("numerator")
                        if (
                            denom_key is not None
                            and denom_state is not None
                            and (
                                denom_state.state == "evidenced_absent"
                                or (
                                    is_evidenced_zero(denom_state)
                                    and num_state is not None
                                    and is_evidenced_zero(num_state)
                                )
                            )
                        ):
                            value = evidenced_absent(
                                (
                                    absent_cause(
                                        denom_key[0],
                                        denom_key[1],
                                        "not_applicable_evidenced",
                                    ),
                                )
                            )
                            memo[key] = value
                            return value
                    if absent_operands:
                        # Law 4 (BM-FOUR-STATE-CELL-ABSENCE-ALGEBRA): a forecast
                        # period inherits a historical absence only when the
                        # originating node is evidenced absent (and not merely
                        # undisclosed in one vintage) across every historical
                        # period; a one-period historical absence carried into
                        # forecast by ref(t-1) leaves the forecast unresolved.
                        def is_all_history_absent(node_id: str) -> bool:
                            if node_id not in law4_all_history_absent:
                                law4_all_history_absent[node_id] = False
                                node_item = (
                                    self._loaded_model.get_item(node_id)
                                    if node_id in self.nodes
                                    else None
                                )
                                owned_history = (
                                    ()
                                    if node_item is None
                                    else historical_periods
                                    if node_item.historical is None
                                    else tuple(
                                        hy
                                        for hy in historical_periods
                                        if item_owns_cell(
                                            self._loaded_model, node_item, hy
                                        )
                                    )
                                )
                                law4_all_history_absent[node_id] = bool(
                                    owned_history
                                ) and all(
                                    isinstance(
                                        (hv := classify(node_id, hy)),
                                        EvidencedAbsentNumberValue,
                                    )
                                    and all(
                                        c.reason_code != "not_disclosed_in_vintage"
                                        for c in hv.causes
                                    )
                                    for hy in owned_history
                                )
                            return law4_all_history_absent[node_id]

                        if int(period) in projection_periods_set and historical_periods:
                            for entry_key, entry_state in absent_operands:
                                if entry_key is None:
                                    continue
                                for cause in entry_state.causes:
                                    cause_year = int(cause.period.fiscal_year)
                                    if cause_year not in historical_periods_set:
                                        continue
                                    # A Law-3 0/0 ratio stamps the evidenced-zero
                                    # denominator as cause.node_id (state == "known"),
                                    # so its originating absence row is entry_key[0];
                                    # every absent-leaf cause checks cause.node_id
                                    # itself, never a derived sum over mixed-year
                                    # leaves.
                                    origin_id = (
                                        entry_key[0]
                                        if (
                                            cause.node_id in self.nodes
                                            and is_evidenced_zero(
                                                classify(cause.node_id, cause_year)
                                            )
                                        )
                                        else cause.node_id
                                    )
                                    if not is_all_history_absent(origin_id):
                                        failing.append(
                                            unavailable(
                                                "missing",
                                                "required_dependency_missing",
                                                entry_key[0],
                                                entry_key[1],
                                            )
                                        )
                                        break
                        if not failing:
                            absent_causes = [
                                cause
                                for _entry_key, entry_state in absent_operands
                                for cause in entry_state.causes
                            ]
                            single = (
                                absent_operands[0][1]
                                if len(absent_operands) == 1
                                else None
                            )
                            value = evidenced_absent(
                                absent_causes,
                                citation=None if single is None else single.citation,
                                included_in=(
                                    None if single is None else single.included_in
                                ),
                            )
                            memo[key] = value
                            return value
                if failing:
                    rank = {"missing": 1, "unsupported": 2, "invalid": 3}
                    selected_state = max(
                        failing, key=lambda state: rank[state.state]
                    ).state
                    peers = [
                        state
                        for state in failing
                        if state.state == selected_state
                        and isinstance(
                            state,
                            (
                                MissingNumberValue,
                                UnsupportedNumberValue,
                                InvalidNumberValue,
                            ),
                        )
                    ]
                    if len(peers) == 1:
                        value = peers[0]
                    else:
                        seen_cause_keys: Set[
                            Tuple[str, str, Tuple[int, str, bool], str, str]
                        ] = set()
                        if selected_state == "missing":
                            missing_causes: List[MissingCause] = []
                            for state in peers:
                                for cause in state.causes:
                                    if not isinstance(cause, MissingCause):
                                        continue
                                    cause_key = _cause_key(cause)
                                    if cause_key in seen_cause_keys:
                                        continue
                                    seen_cause_keys.add(cause_key)
                                    missing_causes.append(cause)
                            missing_causes.sort(key=_cause_key)
                            canonical_missing = tuple(missing_causes)
                            value = MissingNumberValue(
                                state="missing",
                                primary_reason=canonical_missing[0].reason_code,
                                causes=canonical_missing,
                            )
                        elif selected_state == "unsupported":
                            unsupported_causes: List[UnsupportedCause] = []
                            for state in peers:
                                for cause in state.causes:
                                    if not isinstance(cause, UnsupportedCause):
                                        continue
                                    cause_key = _cause_key(cause)
                                    if cause_key in seen_cause_keys:
                                        continue
                                    seen_cause_keys.add(cause_key)
                                    unsupported_causes.append(cause)
                            unsupported_causes.sort(key=_cause_key)
                            canonical_unsupported = tuple(unsupported_causes)
                            value = UnsupportedNumberValue(
                                state="unsupported",
                                primary_reason=canonical_unsupported[0].reason_code,
                                causes=canonical_unsupported,
                            )
                        else:
                            invalid_causes: List[InvalidCause] = []
                            for state in peers:
                                for cause in state.causes:
                                    if not isinstance(cause, InvalidCause):
                                        continue
                                    cause_key = _cause_key(cause)
                                    if cause_key in seen_cause_keys:
                                        continue
                                    seen_cause_keys.add(cause_key)
                                    invalid_causes.append(cause)
                            invalid_causes.sort(key=_cause_key)
                            canonical_invalid = tuple(invalid_causes)
                            value = InvalidNumberValue(
                                state="invalid",
                                primary_reason=canonical_invalid[0].reason_code,
                                causes=canonical_invalid,
                            )
                    memo[key] = value
                    return value
                if prior_unavailable:
                    value = unavailable(
                        "missing",
                        "prior_period_unavailable",
                        item_id,
                        int(period),
                    )
                    memo[key] = value
                    return value


                if numeric is not None and math.isfinite(numeric):
                    # A formula's zero is a plain number unless the document
                    # derived its evidence (law 4: the zero policy's explicit
                    # ``0`` carries ``terminal_zero_fx_policy``).
                    value = DependencyGraphKnownNumberValue(
                        state="known",
                        value=float(numeric),
                        provenance=known_provenance(item_id, int(period)),
                        evidenced_zero=(
                            evidenced_zeros.get(key) if float(numeric) == 0.0 else None
                        ),
                    )
                    memo[key] = value
                    return value
                if numeric is not None:
                    value = unavailable(
                        "invalid", "non_finite_result", item_id, int(period)
                    )
                    memo[key] = value
                    return value
                if (
                    spec.type is FormulaType.valuation
                    and spec.subtype in DECLARED_DOMAIN_VALUATION_SUBTYPES
                ):
                    # Law 6: a declared-domain valuation outside its domain
                    # does not exist for these inputs; it is an evidenced
                    # absence of this cell, not a failed evaluation.
                    value = evidenced_absent(
                        (
                            absent_cause(
                                item_id,
                                int(period),
                                "not_applicable_evidenced",
                            ),
                        )
                    )
                    memo[key] = value
                    return value

                if (
                    spec.type is FormulaType.constant
                    and isinstance((spec.params or {}).get("value"), str)
                ):
                    if spec is (item.overrides or {}).get(int(period)):
                        # An override carrying a string is the analyst's text
                        # in a number slot: no number and no absence evidence,
                        # so the cell is missing like a no-spec cell — never
                        # an arithmetic failure, never a declared absence.
                        value = unavailable(
                            "missing",
                            "source_value_unavailable",
                            item_id,
                            int(period),
                        )
                        memo[key] = value
                        return value
                    # An authored text constant (a year label, a ticker) is
                    # not a number that failed: the formula law's absence,
                    # the same shape as the declared-domain valuation above.
                    # A numeric string already evaluates to a float and
                    # returns known above, so it never reaches here.
                    value = evidenced_absent(
                        (
                            absent_cause(
                                item_id,
                                int(period),
                                "not_applicable_evidenced",
                            ),
                        )
                    )
                    memo[key] = value
                    return value

                if spec.type is FormulaType.constant:
                    reason = "non_finite_input"
                elif spec.type is FormulaType.ratio:
                    # A yoy growth over a zero prior base does not exist: the
                    # formula declares what it is (`_yoy_formula`), so the
                    # quantity is outside its domain, never a bare division.
                    reason = (
                        "domain_error"
                        if spec.subtype == "yoy_growth"
                        else "division_by_zero"
                    )
                elif spec.type is FormulaType.valuation and spec.subtype not in {
                    "dcf_discount",
                    "terminal_value",
                    "capm",
                    "wacc",
                    "multiple",
                    "probability_weighted",
                    "kelly",
                    "break_even_probability",
                    "offset_scenario",
                    "scenario_select",
                }:
                    value = unavailable(
                        "unsupported",
                        "valuation_method_unsupported",
                        item_id,
                        int(period),
                    )
                    memo[key] = value
                    return value
                else:
                    reason = "domain_error"
                value = unavailable("invalid", reason, item_id, int(period))
                memo[key] = value
                return value
            finally:
                visiting.discard(key)

        typed = {
            item_id: {
                int(period): classify(item_id, int(period))
                for period in eval_periods
            }
            for item_id in sorted(self.nodes)
        }
        self.last_typed_results = typed
        return typed

    def _eval_singleton_node(
        self,
        node_id: str,
        period: int,
        results: DependencyGraphResults,
        time_index: Dict[int, int],
        time_order: List[int],
    ) -> bool:
        """Evaluate a singleton component node for one period.

        Returns True when the node has a non-None value after evaluation.
        """
        existing = results.get(node_id, {}).get(period)
        if existing is not None:
            return True

        if node_id in self._unresolved_cone:
            # A SUM_RANGE target that is not a line-item ref cannot resolve: the
            # cell keeps what the evaluator reports (None, with its warning) and
            # everything computed from it stays None. Neither takes a cached value.
            value = (
                self._eval(node_id, period, results, time_index, time_order)
                if node_id in self._unresolved_sum_range_nodes
                else None
            )
            results.setdefault(node_id, {})[period] = value
            return value is not None

        if node_id in self._global_cycle_node_ids and self._cycle_cached_fallback_enabled():
            if self._compute_has_recompute and self._compute_propagate is None:
                cached_forced = self._cached_value_for_period(node_id, period, results)
                if cached_forced is not None:
                    results.setdefault(node_id, {})[period] = cached_forced
                    return True
            item = self._loaded_model.get_item(node_id)
            spec = self._spec_for_period(item, period)
            missing_dep = False
            if spec is not None and not self._is_sum_range_spec(spec):
                for ref in self._extract_refs(spec):
                    if self._value_of(ref, period, results, time_index, time_order) is None:
                        missing_dep = True
                        break
            value = self._eval(node_id, period, results, time_index, time_order)
            is_invalid = value is None or not math.isfinite(value)
            if (missing_dep or is_invalid):
                if not (
                    self._cycle_fallback_policy == "auto_propagate"
                    and self._compute_propagate
                    and node_id in self._compute_propagate
                ):
                    cached = self._cached_value_for_period(node_id, period, results)
                    if cached is not None:
                        results.setdefault(node_id, {})[period] = cached
                        return True
            if value is not None:
                results.setdefault(node_id, {})[period] = value
                return True
            return False

        item = self._loaded_model.get_item(node_id)
        spec = self._spec_for_period(item, period)
        missing_dep = False
        if spec is not None and not self._is_sum_range_spec(spec):
            for ref in self._extract_refs(spec):
                if self._value_of(ref, period, results, time_index, time_order) is None:
                    missing_dep = True
                    break
        value = self._eval(node_id, period, results, time_index, time_order)
        is_invalid = value is None or not math.isfinite(value)
        cached_computed = self._cached_computed_value_for_period(node_id, period)
        if (
            cached_computed is not None
            and self._compute_has_recompute
            and self._compute_propagate is None
        ):
            results.setdefault(node_id, {})[period] = cached_computed
            return True
        if missing_dep or is_invalid:
            if cached_computed is not None:
                results.setdefault(node_id, {})[period] = cached_computed
                return True
        results.setdefault(node_id, {})[period] = value
        return results.get(node_id, {}).get(period) is not None

    def _solve_period_active_cycle_component(
        self,
        nodes: List[str],
        period: int,
        results: DependencyGraphResults,
        time_index: Dict[int, int],
        time_order: List[int],
        inputs: Optional[Dict[str, Dict[int, float]]] = None,
    ) -> None:
        """Solve a global cycle component using period-active subcomponents."""
        order_adj, cycle_adj = self._active_adjs_for_period_subset(period, nodes)
        components, order = self._components_from_adj(cycle_adj, order_adj=order_adj)
        for comp_id in order:
            comp = components[comp_id]
            if comp.is_cycle:
                self._solve_cycle_block(comp.nodes, period, results, time_index, time_order, inputs)
                continue
            node_id = comp.nodes[0]
            if results.get(node_id, {}).get(period) is not None:
                continue
            item = self._loaded_model.get_item(node_id)
            spec = self._spec_for_period(item, period)
            missing_dep = False
            if spec is not None and not self._is_sum_range_spec(spec):
                for ref in self._extract_refs(spec):
                    if self._value_of(ref, period, results, time_index, time_order) is None:
                        missing_dep = True
                        break

            value = self._eval(node_id, period, results, time_index, time_order)
            is_invalid = value is None or not math.isfinite(value)
            if (missing_dep or is_invalid) and self._cycle_cached_fallback_enabled():
                if (
                    self._cycle_fallback_policy == "auto_propagate"
                    and self._compute_propagate
                    and node_id in self._compute_propagate
                ):
                    pass
                else:
                    cached = self._cached_value_for_period(node_id, period, results)
                    if cached is not None:
                        results.setdefault(node_id, {})[period] = cached
                        continue
            if value is not None:
                results.setdefault(node_id, {})[period] = value

    def _identify_deferred_nodes(self) -> None:
        """Classify SUM_RANGE and period_anchor='last' nodes plus downstream closure.

        A SUM_RANGE whose target is not a line-item ref is also recorded, with its
        downstream closure, as the cone that compute leaves unresolved.
        """
        if not self.model:
            return
        sum_range_nodes: Set[str] = set()
        unresolved_sum_range_nodes: Set[str] = set()
        period_anchor_last_nodes: Set[str] = set()

        for item in self.model._index.values():
            for _path, spec in self._iter_all_formula_specs_with_paths(item):
                if self._is_sum_range_spec(spec):
                    sum_range_nodes.add(item.id)
                    if line_item_ref_from_obj((spec.params or {}).get("target")) is None:
                        unresolved_sum_range_nodes.add(item.id)

                for _carrier, ref in self._read_refs(spec):
                    if ref.period_anchor != "last":
                        continue
                    try:
                        target_item = self.model.get_item(ref.id)
                    except KeyError:
                        continue
                    if target_item.column is None:
                        period_anchor_last_nodes.add(item.id)

        direct = sum_range_nodes | period_anchor_last_nodes
        deferred = self._downstream_of(direct) if direct else set()
        self._sum_range_nodes = sum_range_nodes
        self._period_anchor_last_nodes = period_anchor_last_nodes
        self._deferred_nodes = deferred
        self._deferred_cone = deferred - direct
        self._unresolved_sum_range_nodes = unresolved_sum_range_nodes
        self._unresolved_cone = self._downstream_of(unresolved_sum_range_nodes)

    def _evaluate_deferred_nodes(
        self,
        results: DependencyGraphResults,
        time_index: Dict[int, int],
        time_order: List[int],
        eval_periods: List[int],
        inputs: Dict[str, Dict[int, float]],
    ) -> None:
        if not self._deferred_nodes:
            return

        eval_set = set(self._compute_periods) if self._compute_periods is not None else set(eval_periods)
        if not eval_set:
            return

        for node_id in self._deferred_nodes:
            inputs_for_node = (inputs or {}).get(node_id, {})
            if node_id not in results:
                continue
            for period in list(results[node_id].keys()):
                if period not in inputs_for_node:
                    del results[node_id][period]
            if not results[node_id]:
                del results[node_id]

        ordered_nodes = self._ordered_deferred_nodes()
        ordered_periods = sorted(eval_set, key=lambda p: time_index.get(p, len(time_order)))
        for node_id in ordered_nodes:
            for period in ordered_periods:
                if period not in time_index:
                    continue
                self._eval_singleton_node(node_id, period, results, time_index, time_order)

    def _ordered_deferred_nodes(self) -> List[str]:
        deferred = set(self._deferred_nodes)
        if not deferred:
            return []
        order_adj: Dict[str, Set[str]] = {node: set() for node in deferred}
        indegree: Dict[str, int] = {node: 0 for node in deferred}
        for src in sorted(deferred):
            for dst in self.adj.get(src, set()):
                if dst in deferred:
                    order_adj[src].add(dst)
                    indegree[dst] += 1

        queue = sorted(node for node, degree in indegree.items() if degree == 0)
        ordered: List[str] = []
        while queue:
            node = queue.pop(0)
            ordered.append(node)
            for dst in sorted(order_adj.get(node, set())):
                indegree[dst] -= 1
                if indegree[dst] == 0:
                    queue.append(dst)
                    queue.sort()

        if len(ordered) != len(deferred):
            ordered.extend(sorted(deferred - set(ordered)))
        return ordered

    def _validate_deferred_cone_invariant(self) -> None:
        if not self.model:
            return
        for node_id in sorted(self._deferred_nodes):
            try:
                item = self._loaded_model.get_item(node_id)
            except KeyError:
                continue
            if item.column is None:
                continue
            for path, spec in self._iter_all_formula_specs_with_paths(item):
                self._validate_period_invariant_obj(spec, node_id, path)

    def _validate_period_invariant_obj(self, obj: Any, node_id: str, path: str) -> None:
        if obj is None:
            return
        if isinstance(obj, FormulaSpec):
            params = obj.params or {}
            if self._is_sum_range_spec(obj):
                for key, value in params.items():
                    if key == "target":
                        continue
                    self._validate_period_invariant_obj(value, node_id, f"{path}.params.{key}")
                return
            self._validate_period_invariant_obj(params, node_id, f"{path}.params")
            return

        ref = line_item_ref_from_obj(obj)
        if ref is not None:
            if ref.period_anchor == "last":
                return
            try:
                target_item = self._loaded_model.get_item(ref.id)
            except KeyError:
                return
            if target_item is not None and target_item.column is not None:
                return
            raise ValueError(
                f"Fixed-cell SUM_RANGE cone member {node_id!r} uses first-anchor "
                f"time-axis ref to {ref.id!r} via {path}; fixed-cell cone members "
                "must be period-invariant."
            )

        if isinstance(obj, dict):
            for key, value in obj.items():
                self._validate_period_invariant_obj(value, node_id, f"{path}.{key}")
            return
        if isinstance(obj, (list, tuple, set)):
            for index, value in enumerate(obj):
                self._validate_period_invariant_obj(value, node_id, f"{path}[{index}]")

    def cell_operands(self, item: LineItem, period: int) -> Set[Tuple[str, int]]:
        """The cells ``(item, period)`` reads, as the graph evaluates it there: ``_cell_reads``.

        A coordinate the row does not own (``item_owns_cell``: a fixed-cell row owns its anchor
        period only) is no cell and reads nothing. This is the one consumer-cell enumeration:
        ``retire`` and ``invariants.dangling_reads`` walk every period of the axis through it.
        """
        return {cell for _carrier, cell in self._cell_reads(item, period)}

    def _cell_reads(
        self, item: LineItem, period: int
    ) -> List[Tuple[FormulaRef, Tuple[str, int]]]:
        """``(carrier, cell)`` for each cell ``(item, period)`` reads: the spec that period
        selects (an override, else the historical or projected formula, ``formula_periods``
        honoured; a constant reads nothing) through ``_read_refs``, each ref resolved to a cell by
        ``resolve_ref_period`` (a ``SUM_RANGE`` target to every period of its row). ``carrier`` is
        the ref of the spec whose removal removes the read."""
        model = self._loaded_model
        if not item_owns_cell(model, item, int(period)):
            return []
        spec = self._spec_for_period(item, int(period))
        if spec is None:
            return []
        sum_range = self._is_sum_range_spec(spec)
        out: List[Tuple[FormulaRef, Tuple[str, int]]] = []
        for carrier, ref in self._read_refs(spec):
            if sum_range and carrier.role == "target":
                if ref.id in model._index:
                    out.extend(
                        (carrier, (ref.id, int(other)))
                        for other in self._sum_range_target_periods(ref.id)
                    )
                continue
            target_period = self.resolve_ref_period(ref, int(period))
            if target_period is not None:
                out.append((carrier, (ref.id, target_period)))
        return out

    def _read_refs(self, spec: FormulaSpec) -> List[Tuple[FormulaRef, LineItemRef]]:
        """``(carrier, ref)`` for every ref ``spec`` reads, period-free: the one enumeration of an
        operand set, behind the edges ``build`` records and the cells ``cell_operands`` resolves.

        ``FormulaSpec.references()`` is the walk (``carrier`` is the occurrence itself); an
        ``offset_scenario`` also reads the three rows below its anchor, which no param names, so
        those reads are carried by the ``anchor`` occurrence: removing the anchor removes them.
        """
        model = self._loaded_model
        reads = [(formula_ref, formula_ref.ref) for formula_ref in spec.references()]
        if spec.type is FormulaType.valuation and spec.subtype == "offset_scenario":
            carrier = next((r for r, _ref in reads if r.role == "anchor"), None)
            anchor_ref = None if carrier is None else carrier.ref
            sheet = None if anchor_ref is None else self._item_sheet_map.get(anchor_ref.id)
            if carrier is not None and anchor_ref is not None and anchor_ref.id in model._index:
                for offset in (1, 2, 3):
                    row_id = (
                        None
                        if sheet is None
                        else self._row_item_id(sheet, int(model._index[anchor_ref.id].row) + offset)
                    )
                    if row_id is not None:
                        reads.append((carrier, LineItemRef(id=row_id)))
        return reads

    def _is_sum_range_spec(self, spec: Optional[FormulaSpec]) -> bool:
        return (
            spec is not None
            and spec.type == FormulaType.arithmetic
            and (spec.params or {}).get("function") == "SUM_RANGE"
        )

    def _iter_all_formula_specs(self, item: LineItem) -> Iterator[FormulaSpec]:
        for _path, spec in self._iter_all_formula_specs_with_paths(item):
            yield spec

    def _iter_all_formula_specs_with_paths(
        self,
        item: LineItem,
    ) -> Iterator[Tuple[str, FormulaSpec]]:
        if item.historical is not None:
            yield "historical", item.historical
        if item.projected is not None:
            yield "projected", item.projected
        if item.overrides:
            for period, spec in item.overrides.items():
                if spec is not None:
                    yield f"overrides[{int(period)}]", spec

    def resolve_ref_period(self, ref: LineItemRef, period: int) -> Optional[int]:
        """The period of the cell ``ref`` reads when read from ``period``: the one resolver.

        A fixed-cell target reads its anchor period (``_fixed_cell_anchor_period``); a
        ``period_anchor='last'`` ref the target sheet's last projection period shifted by ``t``
        (``_period_for_anchor_last``); any other ref the time-axis period ``t`` along from
        ``period``. ``None`` when the target is not in the model, or the shifted period is off
        the axis (an offset before the axis reads a stored input, no cell of it).
        """
        target_item = self._loaded_model._index.get(ref.id)
        if target_item is None:
            return None
        if target_item.column is not None:
            anchor = self._fixed_cell_anchor_period(target_item)
            return None if anchor is None else int(anchor)
        if ref.period_anchor == "last":
            try:
                return int(self._period_for_anchor_last(ref.id, ref.t))
            except ValueError:
                return None
        order = [int(other) for other in self._time_order()]
        if int(period) not in order:
            return None
        target_index = order.index(int(period)) + int(ref.t)
        return order[target_index] if 0 <= target_index < len(order) else None

    def _consumers_of(self, item_id: str) -> List[str]:
        """Items with a ref to ``item_id`` in any spec, as the last ``build`` recorded them."""
        consumers = set(self.adj.get(item_id, ()))
        consumers.update(
            consumer
            for consumer, refs in self.time_edges.items()
            if any(ref.id == item_id for ref in refs)
        )
        return sorted(consumers)

    def _clear_cell(self, item: LineItem, period: int) -> None:
        """Remove the node ``(item, period)``: its override, formula period and stored value."""
        if item.column is not None:
            # A fixed-cell row is one cell, at its anchor period (``item_owns_cell``): retiring
            # that cell empties the row; any other period is no cell of it and leaves it intact.
            if not item_owns_cell(self._loaded_model, item, int(period)):
                return
            item.historical = None
            item.projected = None
            item.overrides = None
            item.values = None
            item.formula_periods = []
            return
        if item.overrides and period in item.overrides:
            remaining = {key: spec for key, spec in item.overrides.items() if key != period}
            item.overrides = remaining or None
        owned = (
            [int(other) for other in self._time_order()]
            if item.formula_periods is None
            else [int(other) for other in item.formula_periods]
        )
        item.formula_periods = [other for other in owned if other != int(period)]
        if item.values is not None and period in item.values.values:
            item.values = item.values.model_copy(
                update={
                    "values": {
                        key: cell for key, cell in item.values.values.items() if key != period
                    }
                }
            )

    def retire(self, item_id: str, period: int) -> Tuple[Tuple[str, int], ...]:
        """Retire the cell ``(item_id, period)`` and remove it from every cell that reads it.

        The node goes: its override, formula period and stored value are cleared
        (``_clear_cell``; a period a fixed-cell row does not own is no cell of it and leaves the
        row intact). Each consumer cell ``(c, p)``, ``p`` a period ``c`` owns
        (``item_owns_cell``; a fixed-cell reader's only cell is its anchor), whose
        ``cell_operands`` contain it has its effective spec rewritten with ``FormulaSpec.without``
        over the occurrences that read the retired cell (``_cell_reads``; an ``offset_scenario``'s
        implicit scenario-row reads are carried by its ``anchor``) and the rewrite stored as
        ``c``'s override at ``p``; a ``None`` result (a sum's last term, or a retired ref in any
        other role) retires ``(c, p)`` the same way, recursively. A ``SUM_RANGE`` over the retired
        cell's row has no remaining meaning and retires. No formula text is matched.

        Edits cells, not the item-level adjacency: ``build`` must have run on the current model,
        and its ``adj`` / ``time_edges`` stay a superset of the live edges until the next
        ``build``. Returns every retired cell, ``(item_id, period)`` first, in retirement order.
        """
        model = self._loaded_model
        retired: Dict[Tuple[str, int], None] = {}
        pending: List[Tuple[str, int]] = [(item_id, int(period))]
        while pending:
            cell = pending.pop(0)
            if cell in retired:
                continue
            retired[cell] = None
            source_id, source_period = cell
            for consumer_id in self._consumers_of(source_id):
                consumer = model._index[consumer_id]
                for consumer_period in (int(other) for other in self._time_order()):
                    if (consumer_id, consumer_period) in retired:
                        continue
                    carriers = {
                        carrier.path
                        for carrier, read in self._cell_reads(consumer, consumer_period)
                        if read == cell
                    }
                    if not carriers:
                        continue
                    spec = self._spec_for_period(consumer, consumer_period)
                    if spec is None:
                        continue
                    rewritten = spec.without(lambda formula_ref: formula_ref.path in carriers)
                    if rewritten is None:
                        pending.append((consumer_id, consumer_period))
                    elif rewritten is not spec:
                        consumer.overrides = {
                            **(consumer.overrides or {}),
                            consumer_period: rewritten,
                        }
            # After the consumers are read: clearing a fixed-cell row moves its anchor, and a
            # read resolved afterwards would no longer name the cell that was retired.
            self._clear_cell(model._index[source_id], source_period)
        return tuple(retired)

    def get_dependents(self, line_item_id: str) -> List[str]:
        """Return downstream dependents for a line item."""
        return sorted(self.adj.get(line_item_id, set()))

    def get_dependencies(self, line_item_id: str) -> List[str]:
        """Return upstream dependencies for a line item."""
        deps = set()
        for src, dsts in self.adj.items():
            if line_item_id in dsts:
                deps.add(src)
        return sorted(deps)

    def _solve_cycle_block(
        self,
        nodes: List[str],
        period: int,
        results: DependencyGraphResults,
        time_index: Dict[int, int],
        time_order: List[int],
        inputs: Optional[Dict[str, Dict[int, float]]] = None,
    ) -> None:
        """Iteratively solve a cycle block until convergence.

        A block with a frozen node (an explicit input or a constant spec) or a
        cached anchor keeps the fixed point it converges to. An unfrozen block
        is seed-dependent only when a perturbed seed also converges and a value
        moves by more than 1e-4, or becomes None while the first solve did not.
        A perturbed solve that does not converge is not that evidence, so the
        first solution is kept.
        """
        frozen_nodes: Set[str] = set()
        for node in nodes:
            # If the user provided an explicit override for this node+period,
            # keep it and freeze — don't let the model's constant spec overwrite it.
            if inputs and node in inputs and period in inputs[node]:
                frozen_nodes.add(node)
                continue
            item = self._loaded_model.get_item(node)
            spec = self._spec_for_period(item, period)
            if spec and spec.type == FormulaType.constant:
                value = self._eval(node, period, results, time_index, time_order)
                if value is not None:
                    results.setdefault(node, {})[period] = value
                frozen_nodes.add(node)
                continue
            if spec is None and results.get(node, {}).get(period) is not None:
                frozen_nodes.add(node)

        active_nodes = [node for node in nodes if node not in frozen_nodes]
        anchored = bool(frozen_nodes)
        for node in active_nodes:
            if results.get(node, {}).get(period) is not None:
                anchored = True
                continue
            cached = self._cached_value_for_period(node, period, results)
            if cached is not None:
                results.setdefault(node, {})[period] = cached
                anchored = True
            else:
                results.setdefault(node, {})[period] = 0.0

        if not active_nodes:
            return

        converged, residual = self._relax_cycle_block(
            active_nodes, period, results, time_index, time_order
        )
        if converged and not anchored:
            first = {node: results[node][period] for node in active_nodes}
            for node in active_nodes:
                seed = first[node]
                results[node][period] = 0.0 if seed is None else float(seed) + 1.0
            perturbed_converged, _perturbed_residual = self._relax_cycle_block(
                active_nodes, period, results, time_index, time_order
            )
            disagree = False
            if perturbed_converged:
                for node in active_nodes:
                    other = results.get(node, {}).get(period)
                    baseline = first[node]
                    if other is None and baseline is not None:
                        disagree = True
                        break
                    if (
                        other is not None
                        and baseline is not None
                        and abs(float(other) - float(baseline)) > 1e-4
                    ):
                        disagree = True
                        break
            if disagree:
                for node in active_nodes:
                    results[node][period] = None
                return
            for node in active_nodes:
                results[node][period] = first[node]
            return

        pathological = all(results.get(node, {}).get(period) is None for node in active_nodes)
        if (not converged or pathological) and self._cycle_cached_fallback_enabled():
            fallback_count = 0
            for node in active_nodes:
                if (
                    self._cycle_fallback_policy == "auto_propagate"
                    and self._compute_propagate
                    and node in self._compute_propagate
                ):
                    # Keep propagated nodes on their recomputed path; don't snap
                    # them back to cache during sensitivity scenarios.
                    continue
                cached = self._cached_value_for_period(node, period, results)
                if cached is None:
                    continue
                results.setdefault(node, {})[period] = cached
                fallback_count += 1
            if fallback_count:
                logger.debug(
                    "Cycle fallback applied for period=%s nodes=%s converged=%s residual=%s count=%s",
                    period,
                    active_nodes,
                    converged,
                    residual,
                    fallback_count,
                )

    def _relax_cycle_block(
        self,
        active_nodes: List[str],
        period: int,
        results: DependencyGraphResults,
        time_index: Dict[int, int],
        time_order: List[int],
    ) -> Tuple[bool, Optional[float]]:
        """One fixed-point relaxation of ``active_nodes`` at ``period``."""
        converged = False
        residual = None
        for _ in range(100):
            prev = {n: results.get(n, {}).get(period) for n in active_nodes}
            for n in active_nodes:
                results.setdefault(n, {})[period] = self._eval(
                    n, period, results, time_index, time_order
                )
            converged, residual = self._converged(prev, active_nodes, period, results, 1e-6)
            if converged:
                break
        return converged, residual

    def _converged(
        self,
        prev: Dict[str, Optional[float]],
        nodes: List[str],
        period: int,
        results: DependencyGraphResults,
        tol: float,
    ) -> Tuple[bool, Optional[float]]:
        max_residual = 0.0
        for n in nodes:
            current = results.get(n, {}).get(period)
            prior = prev.get(n)
            if prior is None or current is None:
                return False, None
            residual = abs(current - prior)
            if residual > max_residual:
                max_residual = residual
            if residual > tol:
                return False, max_residual
        return True, max_residual

    def _iter_formula_specs(self, item: LineItem) -> Iterable[FormulaSpec]:
        if item.historical:
            yield item.historical
        if item.projected:
            yield item.projected
        if item.overrides:
            for spec in item.overrides.values():
                if spec.type != FormulaType.constant:
                    yield spec

    def _extract_refs(
        self,
        spec: Optional[FormulaSpec],
        *,
        evaluate_condition: Optional[Callable[[Any], Optional[float]]] = None,
    ) -> List[LineItemRef]:
        """Live computation refs of ``spec``, never retained operand lineage: the owner is
        ``FormulaSpec.references``."""
        if spec is None:
            return []
        return [
            formula_ref.ref
            for formula_ref in spec.references(evaluate_condition=evaluate_condition)
        ]
