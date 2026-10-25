"""Analysis tools built on top of the schema dependency graph.

These helpers run scenarios, sweep inputs, and trace sensitivities by
recomputing model outputs with the DependencyGraph. They also hold the
Engine's derived views over one compiled model — ``value``, ``sensitize``,
``compare`` and ``imply`` — and the template DCF arithmetic: every one is a
pure recomputation on read, never a stored input and never an agent's number.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from itertools import product
import math
from typing import TYPE_CHECKING, Dict, Iterable, List, Literal, Optional, Set

from pydantic import BaseModel, ConfigDict

from .collected_claims import Claim, Reference, collected_row_id
from .dependency_graph import DependencyGraph, DependencyGraphResults
from .models import PERIOD_MODE_QUARTERLY5, FinancialModel, ItemType, encode_period
from .ticker_contract import normalize_ticker

if TYPE_CHECKING:
    from .build_model_product_contracts import ProvenanceMap


def run_scenario(
    model: FinancialModel,
    inputs: Dict[str, Dict[int, float]],
    outputs: Optional[List[str]] = None,
) -> DependencyGraphResults:
    """Set assumptions, propagate through the graph, and return outputs.

    If outputs is None, returns all derived line items.
    """
    if not model._index:
        model.build_index()
    graph = DependencyGraph()
    graph.build(model)
    results = graph.compute(inputs or {})

    if outputs is None:
        output_ids = [item_id for item_id, item in model._index.items() if item.item_type == ItemType.derived]
    else:
        output_ids = outputs

    return {output_id: results.get(output_id, {}) for output_id in output_ids}


def run_scenarios(
    model: FinancialModel,
    scenarios: Dict[str, Dict[str, Dict[int, float]]],
    outputs: Optional[List[str]] = None,
) -> Dict[str, DependencyGraphResults]:
    """Run multiple scenarios and return results keyed by scenario name."""
    if not model._index:
        model.build_index()
    results: Dict[str, DependencyGraphResults] = {}
    for name, inputs in scenarios.items():
        results[name] = run_scenario(model, inputs, outputs)
    return results


def sensitivity(
    model: FinancialModel,
    vary: Dict[str, List[float]],
    outputs: List[str],
    base_period: Optional[int] = None,
    base_year: Optional[int] = None,
) -> Dict:
    """Sweep one or more inputs across ranges and return an output grid.

    Returns a list of runs with explicit input values and outputs at the base time key.
    """
    period = base_period if base_period is not None else _resolve_base_year(model, base_year)
    input_ids = list(vary.keys())
    runs = []

    for values in product(*(vary[i] for i in input_ids)):
        inputs = {input_id: {period: value} for input_id, value in zip(input_ids, values)}
        scenario_results = run_scenario(model, inputs, outputs)
        outputs_at_period = {
            output_id: scenario_results.get(output_id, {}).get(period)
            for output_id in outputs
        }
        runs.append({"inputs": dict(zip(input_ids, values)), "outputs": outputs_at_period})

    return {
        "base_year": period,
        "base_period": period,
        "inputs": input_ids,
        "outputs": outputs,
        "runs": runs,
    }


def trace_sensitivity(
    model: FinancialModel,
    input_id: str,
    delta: float,
    output_id: Optional[str] = None,
) -> List[Dict]:
    """Trace the impact of a delta on one input through the dependency chain.

    If output_id is provided, the chain is filtered to nodes on the path
    from input_id to output_id.
    """
    if not model._index:
        model.build_index()
    graph = DependencyGraph()
    graph.build(model)
    period = _default_period(model)

    base_results = graph.compute({})
    base_value = base_results.get(input_id, {}).get(period)
    if base_value is None:
        adjusted_value = delta
    else:
        adjusted_value = base_value + delta

    # Recompute downstream nodes from formulas instead of using cached values
    downstream = _downstream_nodes(graph, input_id)
    after_results = graph.compute({input_id: {period: adjusted_value}}, recompute=downstream)

    impacted = downstream
    if output_id:
        impacted &= _upstream_nodes(graph, output_id)
        impacted.add(output_id)
        impacted.add(input_id)

    ordered = _order_nodes(graph, impacted)

    chain = []
    for node_id in ordered:
        before = base_results.get(node_id, {}).get(period)
        after = after_results.get(node_id, {}).get(period)
        change = None
        pct_change = None
        if before is not None and after is not None:
            change = after - before
            if before != 0:
                pct_change = change / before
        chain.append(
            {
                "id": node_id,
                "label": model.get_item(node_id).label if node_id in model._index else node_id,
                "before": before,
                "after": after,
                "change": change,
                "pct_change": pct_change,
            }
        )

    return chain


# Template DCF arithmetic

DcfMethod = Literal["dcf_terminal_growth", "dcf_exit_multiple"]
# The exit multiple admits only enterprise-value measures. A multiple on one
# of these is an enterprise value, which ``dcf_case_prices`` discounts at WACC
# and converts through the net-debt bridge. ``adjusted_net_income`` is an
# equity-level measure (a multiple on it is an equity value) and is not a
# member, so a policy declaring it cannot reach the pricer.
ExitEarningsMeasure = Literal[
    "terminal_fcf",
    "adjusted_ebitda",
    "ebitda",
    "adjusted_operating_income",
]


EXIT_EARNINGS_ITEMS: dict[ExitEarningsMeasure, str] = {
    "terminal_fcf": "tpl.v.dcf.terminal_year_fcf",
    "adjusted_ebitda": "tpl.fm.adjusted_earnings.adjusted_ebitda",
    "ebitda": "tpl.fm.adjusted_earnings.ebitda",
    "adjusted_operating_income": "tpl.fm.adjusted_earnings.adjusted_operating_income",
}


def dcf_case_prices(
    *,
    method: DcfMethod,
    cases: Mapping[str, Decimal],
    pv_cash_flows: Decimal,
    terminal_fcf: Decimal,
    terminal_discount_period: Decimal,
    wacc: Decimal,
    net_debt: Decimal,
    shares: Decimal,
    exit_earnings: Decimal | None = None,
    exit_earnings_measure: ExitEarningsMeasure = "terminal_fcf",
) -> Dict[str, Decimal]:
    """Per-share equity value for each case's terminal control.

    ``cases`` maps a case name to its terminal growth rate
    (``dcf_terminal_growth``) or exit multiple (``dcf_exit_multiple``). Gordon
    growth always capitalizes terminal free cash flow. The exit multiple
    capitalizes an admitted enterprise-value measure: terminal free cash flow
    unless ``exit_earnings_measure`` names another member of
    ``ExitEarningsMeasure``, passed as ``exit_earnings`` and never relabelled
    as free cash flow.
    """

    # Money: outside this domain the formula still returns a per-share price
    # (a negative or sign-flipped terminal value), so these raises are what keep
    # a meaningless price target from publishing as available; the postcompile
    # valuation door records the reason as an `invalid` method, never a refusal.
    if shares <= 0 or wacc <= 0:
        raise ValueError("DCF shares and WACC must be positive")
    if method == "dcf_exit_multiple" and exit_earnings_measure != "terminal_fcf":
        if exit_earnings is None:
            raise ValueError(
                f"exit-multiple earnings measure {exit_earnings_measure} is not compiled"
            )
        exit_basis = exit_earnings
    else:
        exit_basis = terminal_fcf
    prices: Dict[str, Decimal] = {}
    for case, control in cases.items():
        if method == "dcf_terminal_growth":
            if wacc <= control:
                raise ValueError("terminal growth must be below WACC")
            terminal_value = terminal_fcf * (Decimal("1") + control) / (wacc - control)
        else:
            if control <= 0:
                raise ValueError("exit multiple must be positive")
            terminal_value = exit_basis * control
        present_terminal = terminal_value / (
            (Decimal("1") + wacc) ** terminal_discount_period
        )
        prices[case] = (pv_cash_flows + present_terminal - net_debt) / shares
    return prices


# Derived views over one compiled model: value, sensitize, imply, compare

#: The template's compiled DCF value per share (terminal-growth method).
DCF_PRICE_ITEM_ID = "tpl.v.dcf.equity_value_per_share"

#: ValuationPolicy parameters the template DCF reads, by the item that carries
#: the selected value. A policy row's case cells feed the price through a
#: scenario-table offset the graph does not trace, so the selected item is the
#: one a perturbation moves.
VALUATION_POLICY_DRIVER_ITEMS: Mapping[str, str] = {
    "terminal_growth": "tpl.v.dcf.terminal_growth_rate",
    "exit_multiple": "tpl.v.dcf.exit_multiple",
    "cost_of_equity": "tpl.v.cost_of_equity.cost_of_equity",
}

#: A ValuationPolicy control's lineage id; its parameters are enumerated by
#: :data:`VALUATION_POLICY_DRIVER_ITEMS` instead of by its cells.
_VALUATION_CONTROL_LINEAGE_PREFIX = "valuation_control."

ComparisonBasis = Literal["gaap", "non_gaap", "valuation"]

#: The model line a claimed subject is held against, per accounting basis.
#: Subjects are the consensus vocabulary the capture door mints
#: (``research.model_source_capture.consensus_claims``) plus the price.
COMPARISON_SUBJECT_ITEMS: Mapping[str, tuple[tuple[ComparisonBasis, str], ...]] = {
    "revenue": (("gaap", "tpl.fm.income_statement.total_revenue"),),
    "ebitda": (
        ("gaap", "tpl.fm.adjusted_earnings.ebitda"),
        ("non_gaap", "tpl.fm.adjusted_earnings.adjusted_ebitda"),
    ),
    "ebit": (
        ("gaap", "tpl.fm.income_statement.operating_income"),
        ("non_gaap", "tpl.fm.adjusted_earnings.adjusted_operating_income"),
    ),
    "net_income": (
        ("gaap", "tpl.fm.income_statement.net_income"),
        ("non_gaap", "tpl.fm.adjusted_earnings.adjusted_net_income"),
    ),
    "eps": (
        ("gaap", "tpl.fm.income_statement.eps"),
        ("non_gaap", "tpl.fm.adjusted_earnings.adjusted_eps"),
    ),
    # An issuer bridge's output metric (``HistoricalReconciliationBinding.
    # output_metric``) names the same non-GAAP line as its consensus subject.
    "non_gaap_operating_income": (
        ("non_gaap", "tpl.fm.adjusted_earnings.adjusted_operating_income"),
    ),
    "non_gaap_net_income": (
        ("non_gaap", "tpl.fm.adjusted_earnings.adjusted_net_income"),
    ),
    "share_price": (("valuation", DCF_PRICE_ITEM_ID),),
}

_MODEL_SCALE_FACTORS: Mapping[str, Decimal] = {
    "units": Decimal("1"),
    "millions": Decimal("1000000"),
}


class _View(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class JudgmentDriver(_View):
    """One judgment row readable from the five artifacts and the cells it writes.

    ``subject`` is the row's identity: a ModelSpec forecast assumption id as the
    ProvenanceMap cites it, a ValuationPolicy family authorization's lineage id,
    or ``valuation_policy.<parameter>``. ``item_ids`` are the model items the
    row enters at — the cells citing only this row with no other such cell
    upstream of them.
    """

    subject: str
    driver_key: str
    source: Literal["forecast_assumption_set", "valuation_policy"]
    item_ids: tuple[str, ...]


class Sensitivity(_View):
    """The target's response to a relative bump of one judgment row."""

    subject: str
    driver_key: str
    source: Literal["forecast_assumption_set", "valuation_policy"]
    item_ids: tuple[str, ...]
    target_id: str
    target_period: int
    bump_pct: float
    base_value: Optional[float]
    bumped_value: Optional[float]
    #: Target change per 1% relative change in the row's values.
    delta_per_pct: Optional[float]
    status: Literal["computed", "not_upstream", "unavailable"]
    rank: Optional[int] = None


class ImpliedJudgment(_View):
    """The value of one judgment row at which the model reproduces a Reference.

    ``scale`` multiplies the row's compiled values in every projection period;
    ``values`` are the resulting driver values, the overrides ``value`` maps
    back to the Reference. Every other row holds its compiled value.
    """

    subject: str
    driver_key: str
    source: Literal["forecast_assumption_set", "valuation_policy"]
    item_ids: tuple[str, ...]
    status: Literal["solved", "not_upstream", "unavailable", "no_solution"]
    scale: Optional[float] = None
    values: Dict[str, Dict[int, float]] = {}


class Implied(_View):
    reference_id: str
    reference_kind: str
    reference_value: float
    target_id: str
    target_period: int
    model_value: Optional[float]
    tolerance: float
    judgments: tuple[ImpliedJudgment, ...]


class VarianceRow(_View):
    basis: ComparisonBasis
    model_item_id: str
    model_label: str
    model_scale: str
    model_value: Decimal
    #: The Reference's number in the model item's scale; a range's midpoint.
    reference_value: Decimal
    reference_low: Optional[Decimal] = None
    reference_high: Optional[Decimal] = None
    delta_absolute: Decimal
    delta_pct: Optional[Decimal]


class Variance(_View):
    """The model held against one Reference, computed from both sides."""

    reference_id: str
    reference_kind: str
    claim_id: Optional[str] = None
    author: Optional[str] = None
    subject: str
    fiscal_year: Optional[int]
    period: Optional[str]
    as_of: str
    #: The SourceBundle source and locator of the Reference (and of its Claim).
    source_id: str
    locator: str
    unit: Optional[str]
    status: Literal["compared", "not_comparable"]
    reason: Optional[str] = None
    rows: tuple[VarianceRow, ...] = ()


class _CompiledRun:
    """One DependencyGraph over a compiled model, recomputed per perturbation."""

    def __init__(self, model: FinancialModel) -> None:
        if not model._index:
            model.build_index()
        self.model = model
        self.graph = DependencyGraph()
        self.graph.build(model)
        self.base = self.graph.compute({})
        ts = model.time_structure
        self.projection_periods = tuple(
            int(period) for period in (ts.projection_periods or ts.projection_years)
        )
        self._period_set = set(self.projection_periods)

    def default_period(self) -> int:
        if not self.projection_periods:
            raise ValueError("model has no projection periods")
        return self.projection_periods[0]

    def require_item(self, item_id: str) -> None:
        if item_id not in self.model._index:
            raise KeyError(f"model has no item {item_id!r}")

    def value(
        self,
        target_id: str,
        period: int,
        overrides: Mapping[str, Mapping[int, float]],
    ) -> Optional[float]:
        if not overrides:
            result = self.base.get(target_id, {}).get(period)
        else:
            inputs = {item_id: dict(values) for item_id, values in overrides.items()}
            results = self.graph.compute(
                inputs,
                recompute=self.graph.downstream_for_periods(
                    list(inputs), self._period_set
                ),
                cycle_fallback_policy="auto_propagate",
                periods=self._period_set,
                seed_results=self.base,
                propagate_roots=set(),
            )
            result = results.get(target_id, {}).get(period)
        if result is None or not math.isfinite(float(result)):
            return None
        return float(result)

    def scaled(
        self, item_ids: Sequence[str], factor: float
    ) -> Dict[str, Dict[int, float]]:
        overrides: Dict[str, Dict[int, float]] = {}
        for item_id in item_ids:
            compiled = self.base.get(item_id, {})
            values: Dict[int, float] = {}
            for period in self.projection_periods:
                cell = compiled.get(period)
                if cell is not None:
                    values[period] = float(cell) * factor
            if values:
                overrides[item_id] = values
        return overrides


def judgment_drivers(
    model: FinancialModel,
    provenance: "ProvenanceMap",
    *,
    forecast_assumption_drivers: Mapping[str, str] = {},
) -> tuple[JudgmentDriver, ...]:
    """Every judgment row the compiled cells cite, with the items it enters at.

    ``forecast_assumption_drivers`` maps a ModelSpec forecast assumption id to
    its ``driver`` key, the name the row is reported under; a cited row with no
    entry reports its id. Case-qualified scenario assumptions override a
    forecast row within a scenario case run, not a base sensitivity driver.
    ValuationPolicy controls are read through :data:`VALUATION_POLICY_DRIVER_ITEMS`
    for the parameters the model carries.
    """
    from .dependency_graph_eval import SCENARIO_SNAPSHOT_SUBTYPE
    from .model_queries import ANALYST_NOTE_ARTIFACT, SCENARIO_ASSUMPTION_PREFIX
    if not model._index:
        model.build_index()
    graph = DependencyGraph()
    graph.build(model)
    own_items: Dict[str, Set[str]] = {}
    for cell in provenance.cells:
        if len(cell.assumption_ids) != 1 or cell.semantic_item_id not in model._index:
            continue
        if cell.formula is not None and cell.formula.subtype == SCENARIO_SNAPSHOT_SUBTYPE:
            continue
        subject = cell.assumption_ids[0]
        if subject.startswith((
            _VALUATION_CONTROL_LINEAGE_PREFIX,
            f"{ANALYST_NOTE_ARTIFACT}:",
            SCENARIO_ASSUMPTION_PREFIX,
        )):
            continue
        own_items.setdefault(subject, set()).add(cell.semantic_item_id)
    drivers: List[JudgmentDriver] = []
    for subject in sorted(own_items):
        items = own_items[subject]
        roots = [
            item_id
            for item_id in sorted(items)
            if not ((_upstream_nodes(graph, item_id) - {item_id}) & items)
        ]
        if not roots:
            # The row's cells form a cycle: it enters at its input cells.
            roots = [
                item_id
                for item_id in sorted(items)
                if model.get_item(item_id).item_type == ItemType.input
            ]
        if not roots:
            continue
        source: Literal["forecast_assumption_set", "valuation_policy"] = (
            "valuation_policy"
            if subject.startswith("valuation_")
            else "forecast_assumption_set"
        )
        drivers.append(
            JudgmentDriver(
                subject=subject,
                driver_key=forecast_assumption_drivers.get(subject, subject),
                source=source,
                item_ids=tuple(roots),
            )
        )
    for parameter, item_id in VALUATION_POLICY_DRIVER_ITEMS.items():
        if item_id in model._index:
            drivers.append(
                JudgmentDriver(
                    subject=f"valuation_policy.{parameter}",
                    driver_key=parameter,
                    source="valuation_policy",
                    item_ids=(item_id,),
                )
            )
    return tuple(drivers)


def value(
    model: FinancialModel,
    overrides: Optional[Mapping[str, Mapping[int, float]]] = None,
    *,
    target_id: str = DCF_PRICE_ITEM_ID,
    target_period: Optional[int] = None,
) -> Optional[float]:
    """The target recomputed from the compiled model with ``overrides`` applied."""

    run = _CompiledRun(model)
    run.require_item(target_id)
    period = run.default_period() if target_period is None else target_period
    return run.value(target_id, period, overrides or {})


def sensitize(
    model: FinancialModel,
    drivers: Sequence[JudgmentDriver],
    *,
    target_id: str = DCF_PRICE_ITEM_ID,
    target_period: Optional[int] = None,
    bump_pct: float = 0.01,
) -> tuple[Sensitivity, ...]:
    """Rank judgment rows by the target's response to a relative bump.

    Each row's values are scaled by ``1 + bump_pct`` in every projection period
    while every other row holds; a row whose bump leaves the target unchanged
    is ``not_upstream``. Computed rows rank densely by absolute response.
    """

    if bump_pct == 0:
        raise ValueError("bump_pct must be nonzero")
    run = _CompiledRun(model)
    run.require_item(target_id)
    period = run.default_period() if target_period is None else target_period
    base_value = run.value(target_id, period, {})
    rows: List[Sensitivity] = []
    for driver in drivers:
        overrides = run.scaled(driver.item_ids, 1.0 + bump_pct)
        bumped = run.value(target_id, period, overrides) if overrides else None
        delta = (
            None if bumped is None or base_value is None else bumped - base_value
        )
        status: Literal["computed", "not_upstream", "unavailable"] = (
            "unavailable" if delta is None else "computed" if delta != 0 else "not_upstream"
        )
        rows.append(
            Sensitivity(
                subject=driver.subject,
                driver_key=driver.driver_key,
                source=driver.source,
                item_ids=driver.item_ids,
                target_id=target_id,
                target_period=period,
                bump_pct=bump_pct,
                base_value=base_value,
                bumped_value=bumped,
                delta_per_pct=None if delta is None else delta / (bump_pct * 100),
                status=status,
            )
        )
    computed = sorted(
        (row for row in rows if row.status == "computed"),
        key=lambda row: (-abs(row.delta_per_pct or 0.0), row.subject),
    )
    ranked = tuple(
        row.model_copy(update={"rank": rank}) for rank, row in enumerate(computed, start=1)
    )
    rest = tuple(
        sorted((row for row in rows if row.status != "computed"), key=lambda row: row.subject)
    )
    return ranked + rest


def imply(
    reference: Reference,
    model: FinancialModel,
    drivers: Sequence[JudgmentDriver],
    *,
    target_id: str = DCF_PRICE_ITEM_ID,
    target_period: Optional[int] = None,
    tolerance: float = 1e-6,
    max_iterations: int = 60,
) -> Implied:
    """State a Reference in the model's terms: the row value that reproduces it.

    For each judgment row alone, solve for the scale on its compiled values at
    which the target equals the Reference (the reverse DCF when the target is
    the DCF price and the Reference the pinned price). ``tolerance`` is
    relative to the Reference's magnitude.
    """

    if reference.value is None:
        raise ValueError(
            f"a {reference.kind} reference carries no number of its own to imply"
        )
    goal = float(reference.value)
    run = _CompiledRun(model)
    run.require_item(target_id)
    period = run.default_period() if target_period is None else target_period
    model_value = run.value(target_id, period, {})
    absolute_tolerance = tolerance * max(1.0, abs(goal))
    judgments: List[ImpliedJudgment] = []
    for driver in drivers:
        identity = {
            "subject": driver.subject,
            "driver_key": driver.driver_key,
            "source": driver.source,
            "item_ids": driver.item_ids,
        }
        if model_value is None or not run.scaled(driver.item_ids, 1.0):
            judgments.append(ImpliedJudgment(**identity, status="unavailable"))
            continue

        def miss(scale: float) -> Optional[float]:
            result = run.value(target_id, period, run.scaled(driver.item_ids, scale))
            return None if result is None else result - goal

        low_scale, low_miss = 1.0, model_value - goal
        high_scale, high_miss = 1.05, miss(1.05)
        if high_miss is None:
            judgments.append(ImpliedJudgment(**identity, status="unavailable"))
            continue
        if high_miss == low_miss:
            judgments.append(ImpliedJudgment(**identity, status="not_upstream"))
            continue
        solved: Optional[float] = 1.0 if abs(low_miss) <= absolute_tolerance else None
        for _ in range(max_iterations):
            if solved is not None or high_miss is None or high_miss == low_miss:
                break
            if abs(high_miss) <= absolute_tolerance:
                solved = high_scale
                break
            next_scale = high_scale - high_miss * (high_scale - low_scale) / (
                high_miss - low_miss
            )
            if not math.isfinite(next_scale):
                break
            low_scale, low_miss = high_scale, high_miss
            high_scale, high_miss = next_scale, miss(next_scale)
        if solved is None:
            judgments.append(ImpliedJudgment(**identity, status="no_solution"))
            continue
        judgments.append(
            ImpliedJudgment(
                **identity,
                status="solved",
                scale=solved,
                values=run.scaled(driver.item_ids, solved),
            )
        )
    return Implied(
        reference_id=reference.reference_id,
        reference_kind=reference.kind,
        reference_value=goal,
        target_id=target_id,
        target_period=period,
        model_value=model_value,
        tolerance=tolerance,
        judgments=tuple(judgments),
    )


def consensus_reference(claim: Claim) -> Reference:
    """The consensus or guidance Reference that points at one Claim."""

    kind: Literal["consensus", "guidance"] = (
        "guidance" if claim.author == "management" else "consensus"
    )
    return Reference(
        reference_id=collected_row_id("reference", kind, claim.claim_id),
        kind=kind,
        issuer=claim.issuer,
        subject=claim.subject,
        period=claim.period,
        as_of=claim.as_of,
        source_id=claim.source_id,
        locator=claim.locator,
        claim_id=claim.claim_id,
    )


def _claim_unit_scale(unit: str, item_unit: str, model_scale: str) -> Optional[Decimal]:
    """Divisor from a claimed unit into a model item's scale, or None."""

    if unit == "usd_per_share":
        return Decimal("1") if item_unit == "per_share" else None
    if unit == "usd" and item_unit == "dollars":
        return _MODEL_SCALE_FACTORS.get(model_scale)
    return None


def compare(
    model: FinancialModel,
    reference: Reference,
    *,
    claims: Sequence[Claim] = (),
) -> Variance:
    """Hold the compiled model against one Reference: both sides and the delta.

    A price Reference is held against the compiled DCF value per share in the
    first projection period; a consensus or guidance Reference against the
    model line its Claim's subject names, on every accounting basis the model
    carries for it (which basis is comparable is the interpreter's judgment).
    """

    return _compare(_CompiledRun(model), reference, claims={c.claim_id: c for c in claims})


def _compare(
    run: _CompiledRun,
    reference: Reference,
    *,
    claims: Mapping[str, Claim],
) -> Variance:
    claim = None
    if reference.claim_id is not None:
        claim = claims.get(reference.claim_id)
        if claim is None:
            raise KeyError(f"reference {reference.reference_id} cites an absent claim")
    period = claim.period if claim is not None else reference.period
    fiscal_year = period.fiscal_year if period is not None else None
    unit = claim.unit if claim is not None else reference.unit
    if reference.kind == "price" and fiscal_year is None and run.projection_periods:
        fiscal_year = run.projection_periods[0]
    common = {
        "reference_id": reference.reference_id,
        "reference_kind": reference.kind,
        "claim_id": reference.claim_id,
        "author": None if claim is None else claim.author,
        "subject": reference.subject,
        "fiscal_year": fiscal_year,
        "period": None if fiscal_year is None else f"FY{fiscal_year}",
        "as_of": (claim.as_of if claim is not None else reference.as_of).isoformat(),
        "source_id": str(reference.source_id),
        "locator": reference.locator,
        "unit": unit,
    }

    def not_comparable(reason: str) -> Variance:
        return Variance(**common, status="not_comparable", reason=reason)

    bindings = COMPARISON_SUBJECT_ITEMS.get(reference.subject)
    if bindings is None:
        return not_comparable(f"no model line is bound to subject {reference.subject!r}")
    if period is not None and period.fiscal_quarter is not None:
        return not_comparable("a quarterly claim against an annual model period")
    if fiscal_year is None:
        return not_comparable("the reference names no fiscal period")
    if claim is not None:
        if claim.value is not None:
            claimed, low, high = claim.value, None, None
        else:
            assert claim.low is not None and claim.high is not None
            claimed, low, high = (claim.low + claim.high) / 2, claim.low, claim.high
    else:
        assert reference.value is not None
        claimed, low, high = reference.value, None, None
    model_period = _resolve_base_year(run.model, fiscal_year)
    rows: List[VarianceRow] = []
    for basis, item_id in bindings:
        if item_id not in run.model._index:
            continue
        item = run.model.get_item(item_id)
        modelled = run.base.get(item_id, {}).get(model_period)
        if modelled is None or not math.isfinite(float(modelled)):
            continue
        if basis == "valuation":
            divisor: Optional[Decimal] = Decimal("1")
        else:
            divisor = _claim_unit_scale(
                str(unit), getattr(item.unit, "value", str(item.unit)), item.model_scale
            )
        if divisor is None:
            continue
        model_value = Decimal(str(modelled))
        reference_value = claimed / divisor
        delta = model_value - reference_value
        rows.append(
            VarianceRow(
                basis=basis,
                model_item_id=item_id,
                model_label=item.label,
                model_scale=item.model_scale,
                model_value=model_value,
                reference_value=reference_value,
                reference_low=None if low is None else low / divisor,
                reference_high=None if high is None else high / divisor,
                delta_absolute=delta,
                delta_pct=None if reference_value == 0 else delta / reference_value,
            )
        )
    if not rows:
        return not_comparable(
            f"the model carries no {reference.subject!r} value for FY{fiscal_year} "
            f"in the reference's unit"
        )
    return Variance(**common, status="compared", rows=tuple(rows))


def compare_consensus(
    model: FinancialModel,
    claims: Sequence[Claim],
) -> tuple[Variance, ...]:
    """``compare`` for the focal issuer's consensus Claims, the latest per subject and year.

    A SourceBundle also carries peers' consensus (their ``analyst_estimates``
    for relative valuation); a Claim is held against the model only when its
    issuer is the model's company.
    """

    focal = normalize_ticker(model.company.ticker)
    # An annual statement keys quarter 0 (quarters run 1-4), so it orders first.
    latest: Dict[tuple[str, int, int], Claim] = {}
    for claim in claims:
        if claim.author != "consensus" or normalize_ticker(claim.issuer) != focal:
            continue
        key = (claim.subject, claim.period.fiscal_year, claim.period.fiscal_quarter or 0)
        held = latest.get(key)
        if held is None or (claim.as_of, claim.claim_id) > (held.as_of, held.claim_id):
            latest[key] = claim
    run = _CompiledRun(model)
    return tuple(
        _compare(run, consensus_reference(claim), claims={claim.claim_id: claim})
        for _key, claim in sorted(latest.items(), key=lambda pair: pair[0])
    )


# Helpers


def _resolve_base_year(model: FinancialModel, base_year: Optional[int]) -> int:
    """Map a base_year to the correct period key, handling quarterly mode.

    In quarterly5 mode, base_year=2024 maps to the Annual slot (20245).
    In yearly mode or when base_year is None, falls through to _default_period.
    """
    if base_year is None:
        return _default_period(model)
    mode = model.time_structure.period_mode
    if mode == PERIOD_MODE_QUARTERLY5:
        # If already a period key (e.g. 20245), use as-is
        if base_year > 9999:
            return base_year
        return encode_period(base_year, 5, mode)
    return base_year


def _default_period(model: FinancialModel) -> int:
    """Pick a reasonable default period (last projection, else last historical)."""
    ts = model.time_structure
    periods = list(ts.projection_periods) or list(ts.historical_periods)
    if not periods:
        periods = list(ts.projection_years) or list(ts.historical_years)
    if not periods:
        raise ValueError("Model has no time structure periods")
    return periods[-1]


def _downstream_nodes(graph: DependencyGraph, start: str) -> Set[str]:
    """Return all downstream nodes reachable from the start node."""
    visited: Set[str] = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for dep in graph.adj.get(node, set()):
            if dep not in visited:
                stack.append(dep)
    return visited


def _upstream_nodes(graph: DependencyGraph, target: str) -> Set[str]:
    """Return all upstream nodes that feed the target node."""
    reverse: Dict[str, Set[str]] = {node: set() for node in graph.nodes}
    for src, dsts in graph.adj.items():
        for dst in dsts:
            reverse[dst].add(src)

    visited: Set[str] = set()
    stack = [target]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for dep in reverse.get(node, set()):
            if dep not in visited:
                stack.append(dep)
    return visited


def _order_nodes(graph: DependencyGraph, nodes: Iterable[str]) -> List[str]:
    """Order nodes using the graph's component topological order."""
    node_set = set(nodes)
    ordered: List[str] = []
    if graph.components and graph.component_order:
        for comp_id in graph.component_order:
            for node in graph.components[comp_id].nodes:
                if node in node_set:
                    ordered.append(node)
    else:
        ordered = sorted(node_set)
    return ordered


__all__ = [
    "COMPARISON_SUBJECT_ITEMS",
    "DCF_PRICE_ITEM_ID",
    "VALUATION_POLICY_DRIVER_ITEMS",
    "ComparisonBasis",
    "DcfMethod",
    "ExitEarningsMeasure",
    "EXIT_EARNINGS_ITEMS",
    "Implied",
    "ImpliedJudgment",
    "JudgmentDriver",
    "Sensitivity",
    "Variance",
    "VarianceRow",
    "compare",
    "compare_consensus",
    "consensus_reference",
    "dcf_case_prices",
    "imply",
    "judgment_drivers",
    "run_scenario",
    "run_scenarios",
    "sensitivity",
    "sensitize",
    "trace_sensitivity",
    "value",
]
