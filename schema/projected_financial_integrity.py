"""Deterministic projected-statement integrity checks for annual models."""

from __future__ import annotations
import json
import re

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Literal, NamedTuple, TypeGuard

from .build_diagnostic_types import DiagnosticTolerances
from .dependency_graph import DependencyGraph
from .finding_vocabulary import FindingScalar
from .handle import ComputedValues
from .model_kind_dispatch import DeclaredSemanticUnitPlane
from .model_readiness_common import _historical_periods, _is_present
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ValueProvenance,
    canonical_strategy,
)
from industry_slice_core.industry_types import Unit
from .refs import line_item_ref_from_obj
from .renderer_columns import item_owns_cell
from value_semantics_core import CONVENTIONS_IR_V1


ProjectedFinancialIntegrityIssueCode = Literal[
    "projected_balance_sheet_inputs_missing",
    "projected_balance_sheet_imbalance",
    "filed_balance_sheet_footing_gap",
    "projected_cash_roll_forward_inputs_missing",
    "projected_cash_opening_balance_mismatch",
    "projected_cash_net_change_mismatch",
    "projected_cash_ending_balance_mismatch",
    "required_revenue_component_projection_missing",
    "required_revenue_component_reconciliation_failed",
    "projected_revenue_continuity_break",
    "projected_unit_scale_invariant_failed",
    "projected_sign_invariant_failed",
    "projected_schedule_roll_forward_inputs_missing",
    "projected_schedule_roll_forward_mismatch",
    "semantic_unit_undeclared",
    "historical_balance_sheet_imbalance",
    "historical_cash_net_change_mismatch",
    "historical_cash_ending_balance_mismatch",
    "historical_cash_opening_balance_mismatch",
    "statement_total_operand_dropped",
    "statement_total_operand_offsheet",
    "row_definition_discontinuity",
]

TOTAL_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"

BALANCE_SHEET_ITEM_IDS = (
    "tpl.fm.balance_sheet.total_assets",
    "tpl.fm.balance_sheet.total_liabilities",
    "tpl.fm.balance_sheet.total_equity",
)

CASH_FLOW_ITEM_IDS = (
    "tpl.fm.cash_flow.operating_cash_flow",
    "tpl.fm.cash_flow.investing_cash_flow",
    "tpl.fm.cash_flow.financing_cash_flow",
    "tpl.fm.cash_flow.net_change_in_cash_and_cash_equivalents",
    "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period",
    "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period",
)

CASH_FLOW_FX_ITEM_ID = "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash"

# The template statement each identity anchor's operand closure stays inside.
_STATEMENT_ANCHOR_PREFIXES = (
    (BALANCE_SHEET_ITEM_IDS, "tpl.fm.balance_sheet."),
    (CASH_FLOW_ITEM_IDS, "tpl.fm.cash_flow."),
)

# Half a cent, the exactness bound of a rendered historical balance sheet
# (ruling R4), expressed in dollars before division by the row's scale.
_HISTORICAL_BALANCE_TOLERANCE_DOLLARS = 0.005

_SCALE_MULTIPLIERS = {
    row.numeric_scale: row.multiplier for row in CONVENTIONS_IR_V1.scale_factors
}

# A decline of 50% or more requires review. This is intentionally a conspicuous
# R0 containment threshold rather than a company-specific forecasting policy;
# the frozen-model incident that motivated it fell 88% (the evidence record is
# the reference-model regression captured in `docs/qa/`).
MIN_REVENUE_CONTINUITY_RATIO = 0.50

# The *template's* own control rows and their unit contracts. Model-kind
# semantic rows are not listed here: they are declared by the model kind
# (`ModelKindCapabilities.semantic_units`, plan §3 row S4) and merged in at
# evaluation through `DeclaredSemanticUnitPlane`, so this mechanism table
# carries no issuer's vocabulary.
_TEMPLATE_CONTROL_METADATA = {
    "tpl.a.balance_sheet_wc.days_sales_outstanding_dso": (Unit.days, "units"),
    "tpl.a.balance_sheet_wc.days_payable_outstanding_dpo": (Unit.days, "units"),
    "tpl.a.balance_sheet_wc.accrued_expenses_pct_cogs_and_sga": (
        Unit.percentage,
        "units",
    ),
    "tpl.a.balance_sheet_wc.of_rev": (Unit.percentage, "units"),
    "tpl.a.capital_investments.of_revenue": (Unit.percentage, "units"),
    (
        "tpl.a.depreciation_amortization."
        "amortization_of_beginning_capitalized_internal_use_software"
    ): (Unit.percentage, "units"),
    "tpl.a.tax_net_income.tax_rate": (Unit.percentage, "units"),
}

_ROLL_FORWARD_SPECS = (
    (
        "capitalized software",
        "tpl.a.capital_investments.software_investment_if_applicable",
        None,
        "tpl.a.capital_investments.add_capitalized_software_costs_if_applicable",
        "tpl.a.depreciation_amortization.amortization_of_capitalized_internal_use_software",
        "canonical_capitalized_software_roll_forward",
    ),
    (
        "operating lease ROU asset",
        "tpl.a.other_assets.operating_lease_right_of_use_assets",
        None,
        "tpl.a.other_assets.operating_lease_rou_asset_additions",
        "tpl.a.other_assets.operating_lease_rou_asset_amortization",
        "canonical_operating_lease_rou_asset_roll_forward",
    ),
    (
        "operating lease liability",
        "tpl.a.capital_sources.operating_lease_liability_ending",
        "tpl.a.capital_sources.operating_lease_liability_beginning",
        "tpl.a.capital_sources.operating_lease_liability_additions",
        "tpl.a.capital_sources.operating_lease_liability_payments",
        "canonical_operating_lease_liability_roll_forward",
    ),
    (
        "debt",
        "tpl.a.other_income_interest.debt_end_of_period",
        "tpl.a.other_income_interest.debt_beg_of_period",
        "tpl.a.capital_sources.change",
        None,
        "canonical_debt_balance_roll_forward",
    ),
)


RepairOwner = Literal["binding", "driver", "document", "engine"]


@dataclass(frozen=True)
class ProjectedFinancialIntegrityFinding:
    """One blocking projected-statement finding in canonical model space.

    ``repair_owner`` names the layer whose authored data or code can clear it,
    and is set at each construction site from what the finding's operands are:

    - ``binding``: the operands are filed values, so a mismatch means a filed
      line is unplaced or misplaced (``statement_bindings.v2``), or a missing
      historical face row has a required role or a declared BM balance bridge
      but no pinned value.
    - ``driver``: a projected input a control can revise is missing, or an
      accepted historical driver seed feeds a projected continuity break.
    - ``document``: the formula the working document holds for the row repairs
      it: a total that omits a component its block displays, an operand set
      that changes at the history/forecast seam, or a balance sheet that does
      not foot over inputs that are all present.
    - ``engine``: a missing leaf no control owns, or a check the template's
      own schedule or cash roll cannot close. No authored data or formula
      repairs it.

    ``item_id`` and ``periods`` name the cells the finding is about (the
    renderer's required-unavailable set and declinations read them as cells).
    ``repair_periods`` names the years of the ``related_item_ids`` the repair
    changes when they differ from ``periods``, as for a missing total whose
    root is a historical seed; empty means the repair is in ``periods``.
    ``statement`` identifies the statement an identity closes over;
    ``observed_delta`` carries an already-computed imbalance when available.
    ``observed`` carries scalar diagnostic metadata, including the missing
    leaves and their required roles for an input-gap finding.
    """

    code: ProjectedFinancialIntegrityIssueCode
    detail: str
    repair_owner: RepairOwner
    item_id: str | None = None
    periods: tuple[int, ...] = ()
    related_item_ids: tuple[str, ...] = ()
    bound: float | None = None
    repair_periods: tuple[int, ...] = ()
    statement: str | None = None
    observed_delta: float | None = None
    observed: dict[str, FindingScalar] = field(default_factory=dict)


#: The template namespace whose rows are the statement faces a ``statement_bindings.v2``
#: role fills; every other template namespace (``tpl.a.*`` controls, ``tpl.v.*``
#: valuation) is authored through drivers.
_FACE_ITEM_PREFIX = "tpl.fm."


#: The catalog ``statement`` names an identity closes over (``filed_line_unclaimed``'s
#: ``statement``): the balance-sheet identity and the cash bridge.
BALANCE_SHEET_STATEMENT = "balance_sheet"
CASH_FLOW_STATEMENT = "cash_flow"


def _identity_owner(
    statement: str, unclaimed_statements: Collection[str], otherwise: RepairOwner,
) -> RepairOwner:
    """An identity over a statement that has a filed line no binding row places cannot close
    until the line is placed: the binding owns it. Otherwise the identity's closing owner is the
    call site's (``otherwise``)."""

    return "binding" if statement in unclaimed_statements else otherwise


#: Provenance of a value a role pin wrote. A house ``default`` and an authored
#: ``input`` are not pins.
_PINNED_PROVENANCE = frozenset({
    ValueProvenance.imported_edgar,
    ValueProvenance.imported_fmp,
    ValueProvenance.imported_vendor,
    ValueProvenance.imported_other,
    ValueProvenance.historical_foundation,
})

_OWNER_ORDER: tuple[RepairOwner, ...] = ("binding", "driver", "document", "engine")


def _period_set(model: FinancialModel, *, historical: bool) -> set[int]:
    axis = model.time_structure
    periods = axis.historical_periods if historical else axis.projection_periods
    fallback = axis.historical_years if historical else axis.projection_years
    return {int(period) for period in (periods or fallback)}


def _has_pinned_value(item: LineItem, period: int) -> bool:
    """Whether ``period`` holds a role-bound value, not a template formula."""

    cell = None if item.values is None else item.values.values.get(int(period))
    if (
        cell is not None
        and cell.provenance in _PINNED_PROVENANCE
        and _is_present(cell.value)
    ):
        return True
    override = (item.overrides or {}).get(int(period))
    if override is None or override.type != FormulaType.constant:
        return False
    if override.edgar_provenance is not None or override.vendor_provenance is not None:
        return True
    # Population writes a bare constant beside the fact cell. A house default
    # names its strategy on the note or derivation.
    return (
        override.note is None
        and override.derivation is None
        and _is_present(override.params.get("value"))
    )


@lru_cache(maxsize=1)
def _exposed_control_item_ids() -> frozenset[str]:
    """Template rows a driver mapping or forecast-control profile can revise."""

    from .driver_resolver import registered_driver_mapping

    # Every forecast-profile target binding resolves through this same registry.
    return frozenset(registered_driver_mapping().values())


def _is_projected_input_with_control(item: LineItem) -> bool:
    if item.id in _exposed_control_item_ids():
        return True
    return item.item_type == ItemType.input and item.driver_category is not None


def _root_owner(model: FinancialModel, item_id: str, period: int) -> RepairOwner:
    """The layer that can close one missing leaf, from the cell's own provenance."""

    item = model._index.get(item_id)
    if item is None:
        return "engine"
    if (
        int(period) in _period_set(model, historical=True)
        and item.id.startswith(_FACE_ITEM_PREFIX)
        and (
            item.required_role
            or (
                item.projected is not None
                and item.projected.derivation is not None
                and item.projected.derivation.owner == "build_company_grounded_schedules"
                and canonical_strategy(item.projected.derivation.strategy)
                == "declared_balance_roll"
            )
        )
        and not _has_pinned_value(item, int(period))
    ):
        return "binding"
    if (
        int(period) in _period_set(model, historical=False)
        and _is_projected_input_with_control(item)
    ):
        return "driver"
    return "engine"


def _input_dependency_roots(
    model: FinancialModel,
    graph: DependencyGraph,
    values: ComputedValues,
    cells: dict[str, list[int]],
    *,
    missing_only: bool,
) -> set[tuple[str, int]]:
    """Leaves under each cell, following the graph's selected period operands.

    Missing-input findings follow only missing operands; continuity findings
    follow all operands back to their valued opening seeds. A present gap cell
    is not itself a missing leaf; a missing operand of it still is.
    """

    def present(item_id: str, period: int) -> bool:
        return _is_present(values.get(item_id, {}).get(int(period)))

    done: dict[tuple[str, int], set[tuple[str, int]]] = {}

    def walk(item_id: str, period: int, stack: set[tuple[str, int]]) -> set[tuple[str, int]]:
        key = (item_id, int(period))
        if key in done:
            return done[key]
        if key in stack:
            return set()
        stack.add(key)
        item = model._index.get(item_id)
        operands = graph.cell_operands(item, int(period)) if item is not None else set()
        followed = [
            (operand_id, int(operand_period))
            for operand_id, operand_period in operands
            if not missing_only or not present(operand_id, int(operand_period))
        ]
        found: set[tuple[str, int]] = set()
        for operand_id, operand_period in followed:
            found.update(walk(operand_id, operand_period, stack))
        stack.remove(key)
        if not (missing_only and present(item_id, int(period))):
            found = found or {key}
        done[key] = found
        return found

    roots: set[tuple[str, int]] = set()
    for item_id, periods in cells.items():
        for period in periods:
            roots.update(walk(item_id, period, set()))
    if roots:
        return roots
    return {
        (item_id, int(period))
        for item_id, periods in cells.items()
        for period in periods
    }


def _has_historical_driver_seed(item: LineItem, period: int) -> bool:
    """Recognize the compiler and authored-document HFF seed writers."""

    cell = None if item.values is None else item.values.values.get(int(period))
    if cell is None or not _is_present(cell.value):
        return False
    if cell.provenance == ValueProvenance.historical_foundation:
        return True
    lineage = cell.lineage_reference
    source = lineage.get("source") if isinstance(lineage, Mapping) else None
    row = source.get("row") if isinstance(source, Mapping) else None
    return (
        isinstance(source, Mapping)
        and source.get("kind") == "judgment_row"
        and source.get("artifact") == "historical_foundation"
        and isinstance(row, Mapping)
        and bool(row.get("driver_key"))
    )


def _inputs_missing_findings(
    model: FinancialModel,
    graph: DependencyGraph,
    values: ComputedValues,
    *,
    code: ProjectedFinancialIntegrityIssueCode,
    item_id: str,
    gaps: dict[str, list[int]],
    detail: str,
) -> list[ProjectedFinancialIntegrityFinding]:
    """One finding per owner of the gap's missing leaves.

    ``item_id``, ``periods`` and ``detail`` stay the total's missing cells.
    ``related_item_ids`` and ``repair_periods`` are that owner's root cells:
    the rows and years the repair changes (a binding root's year selects the
    filing to read).
    """

    by_owner: dict[RepairOwner, set[tuple[str, int]]] = {}
    for root_id, period in _input_dependency_roots(
        model, graph, values, gaps, missing_only=True
    ):
        by_owner.setdefault(_root_owner(model, root_id, period), set()).add(
            (root_id, int(period))
        )
    periods = tuple(sorted({period for missing in gaps.values() for period in missing}))
    findings: list[ProjectedFinancialIntegrityFinding] = []
    for owner in _OWNER_ORDER:
        if owner not in by_owner:
            continue
        leaf_ids = sorted({root for root, _period in by_owner[owner]})
        shown_leaf_ids = leaf_ids[:20]
        leaf_roles = {
            leaf_id: item.required_role
            for leaf_id in shown_leaf_ids
            if (item := model._index.get(leaf_id)) is not None and item.required_role
        }
        missing_leaves = ", ".join(
            f"{leaf_id} ({leaf_roles[leaf_id]})" if leaf_id in leaf_roles else leaf_id
            for leaf_id in shown_leaf_ids
        )
        findings.append(ProjectedFinancialIntegrityFinding(
            code=code,
            repair_owner=owner,
            detail=f"{detail}; missing leaves: {missing_leaves}",
            item_id=item_id,
            periods=periods,
            related_item_ids=tuple(leaf_ids),
            repair_periods=tuple(sorted({period for _root, period in by_owner[owner]})),
            observed={
                "missing_leaf_items": json.dumps(shown_leaf_ids),
                "missing_leaf_items_truncated": len(leaf_ids) > len(shown_leaf_ids),
                "missing_leaf_roles": json.dumps([
                    f"{leaf_id}:{role}" for leaf_id, role in leaf_roles.items()
                ]),
                "missing_leaf_count": len(leaf_ids),
            },
        ))
    return findings



def _list_values_are_present(
    values: list[float | None],
) -> TypeGuard[list[float]]:
    return all(_is_present(value) for value in values)


def _dict_values_are_present(
    values: dict[str, float | None],
) -> TypeGuard[dict[str, float]]:
    return all(_is_present(value) for value in values.values())


@dataclass(frozen=True)
class ProjectedFinancialIntegrityResult:
    """Pure evaluation result consumed by model-quality readiness."""

    required_item_ids: tuple[str, ...]
    findings: tuple[ProjectedFinancialIntegrityFinding, ...]


def evaluate_projected_financial_integrity(
    model: FinancialModel,
    *,
    computed_values: ComputedValues,
    projection_periods: list[int],
    tolerances: DiagnosticTolerances | None = None,
    semantic_units: DeclaredSemanticUnitPlane | None = None,
    unclaimed_statements: Collection[str] = (),
) -> ProjectedFinancialIntegrityResult:
    """Evaluate projected accounting identities and revenue continuity.

    Statement families activate from canonical structural anchors. Once a
    family is present, missing members and non-finite period values are
    blockers; a partially materialized statement cannot silently opt out.

    ``semantic_units`` is the model kind's declared semantic-row unit plane
    (plan §3 row S4). Absence is feature-off: the template's own control rows
    are enforced and no model-kind row is, which is exactly what this checker
    did for a model kind that declared nothing.

    ``unclaimed_statements`` are the statements the same validation found a filed line
    on that no binding row claims (``filed_line_unclaimed``); a balance-sheet or cash
    bridge identity over one of them is owned by the binding, not the engine.
    """

    model.build_index()
    periods = sorted({int(period) for period in projection_periods})
    values = {
        str(item_id): {int(period): value for period, value in item_values.items()}
        for item_id, item_values in computed_values.items()
    }
    resolved_tolerances = tolerances or DiagnosticTolerances()
    graph = DependencyGraph()
    graph.build(model)

    required_item_ids: set[str] = set()
    findings: list[ProjectedFinancialIntegrityFinding] = []

    balance_required, balance_findings = _balance_sheet_findings(
        model,
        graph=graph,
        values=values,
        projection_periods=periods,
        tolerances=resolved_tolerances,
        unclaimed_statements=unclaimed_statements,
    )
    required_item_ids.update(balance_required)
    findings.extend(balance_findings)

    cash_required, cash_findings = _cash_roll_forward_findings(
        model,
        graph=graph,
        values=values,
        projection_periods=periods,
        tolerances=resolved_tolerances,
        unclaimed_statements=unclaimed_statements,
    )
    required_item_ids.update(cash_required)
    findings.extend(cash_findings)

    revenue_required, revenue_findings = _revenue_findings(
        model,
        graph=graph,
        values=values,
        projection_periods=periods,
        tolerances=resolved_tolerances,
    )
    required_item_ids.update(revenue_required)
    findings.extend(revenue_findings)

    metadata_required, metadata_findings = _metadata_findings(
        model,
        values=values,
        projection_periods=periods,
        semantic_units=semantic_units or DeclaredSemanticUnitPlane(),
    )
    required_item_ids.update(metadata_required)
    findings.extend(metadata_findings)

    schedule_required, schedule_findings = _schedule_roll_forward_findings(
        model,
        graph=graph,
        values=values,
        projection_periods=periods,
        tolerances=resolved_tolerances,
    )
    required_item_ids.update(schedule_required)
    findings.extend(schedule_findings)

    return ProjectedFinancialIntegrityResult(
        required_item_ids=tuple(sorted(required_item_ids)),
        findings=tuple(findings),
    )


def _balance_sheet_findings(
    model: FinancialModel,
    *,
    graph: DependencyGraph,
    values: ComputedValues,
    projection_periods: list[int],
    tolerances: DiagnosticTolerances,
    unclaimed_statements: Collection[str],
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    if not any(item_id in model._index for item_id in BALANCE_SHEET_ITEM_IDS):
        return set(), []

    required: set[str] = set(BALANCE_SHEET_ITEM_IDS)
    gaps = _required_value_gaps(
        model,
        values=values,
        item_ids=BALANCE_SHEET_ITEM_IDS,
        periods=projection_periods,
    )
    findings: list[ProjectedFinancialIntegrityFinding] = []
    if gaps:
        findings.extend(
            _inputs_missing_findings(
                model,
                graph,
                values,
                code="projected_balance_sheet_inputs_missing",
                item_id=BALANCE_SHEET_ITEM_IDS[0],
                gaps=gaps,
                detail=_missing_detail("projected balance sheet", gaps),
            )
        )

    assets_id, liabilities_id, equity_id = BALANCE_SHEET_ITEM_IDS
    footing = _base_year_footing_gap(
        model,
        values=values,
        projection_periods=projection_periods,
        unclaimed_statements=unclaimed_statements,
    )
    carried_tolerance = _half_cent_in_scale(model._index.get(assets_id))
    unbalanced_periods: list[int] = []
    carried_periods: list[int] = []
    max_delta = 0.0
    for period in projection_periods:
        period_values = [values.get(item_id, {}).get(period) for item_id in BALANCE_SHEET_ITEM_IDS]
        if not _list_values_are_present(period_values):
            continue
        assets, liabilities, equity = (float(value) for value in period_values)
        liabilities_and_equity = liabilities + equity
        delta = assets - liabilities_and_equity
        base = max(abs(assets), abs(liabilities_and_equity))
        # When balance sheet has unclaimed lines, strict half-cent tolerance applies:
        # an unbound line cannot be excused as within tolerance or filer rounding.
        tolerance = carried_tolerance if BALANCE_SHEET_STATEMENT in unclaimed_statements else _max_tolerance(
            base,
            tolerances.bs_balance_abs_m,
            tolerances.bs_balance_pct,
        )
        if (
            footing is not None
            and abs(delta) > carried_tolerance
            and abs(delta - footing.carried_delta) <= carried_tolerance
        ):
            carried_periods.append(period)
        elif abs(delta) > tolerance:
            unbalanced_periods.append(period)
            max_delta = max(max_delta, abs(delta))
    if footing is not None and carried_periods:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="filed_balance_sheet_footing_gap",
                repair_owner="binding",
                detail=(
                    f"the filed FY{footing.base_period} balance sheet does not foot: "
                    f"{footing.rendered}. The model keeps the filed face values and "
                    f"carries the resulting delta={footing.carried_delta:.10g} in periods "
                    f"{carried_periods}; it is the filer's own rounding (the filing's "
                    "rounding disclaimer), recorded as informational, and no plug or "
                    "rounding line is added"
                ),
                item_id=assets_id,
                periods=tuple(carried_periods),
                related_item_ids=(assets_id, liabilities_id, equity_id),
                bound=carried_tolerance,
                statement=BALANCE_SHEET_STATEMENT,
                observed_delta=footing.carried_delta,
            )
        )
    if unbalanced_periods:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="projected_balance_sheet_imbalance",
                repair_owner=_identity_owner(BALANCE_SHEET_STATEMENT, unclaimed_statements, "document"),
                detail=(
                    "projected total assets do not equal total liabilities plus equity "
                    f"within the approved tolerance in periods {unbalanced_periods}; "
                    f"maximum absolute delta={max_delta:g}"
                ),
                item_id=assets_id,
                periods=tuple(unbalanced_periods),
                related_item_ids=(assets_id, liabilities_id, equity_id),
                statement=BALANCE_SHEET_STATEMENT,
                observed_delta=max_delta,
            )
        )
    return required, findings


@dataclass(frozen=True)
class _FootingGap:
    """A filed balance sheet's printed totals against the face lines that roll into them.

    ``carried_delta`` is assets less liabilities plus equity when every total is the sum
    of its face lines; a projection that rolls those lines forward carries it unchanged.
    """

    base_period: int
    carried_delta: float
    rendered: str


DISCLAIMER_PATTERNS = (
    re.compile(r"([^\n]*?(?:Columns\s+and\s+rows\s+may\s+not\s+add|may\s+not\s+add\s+due\s+to\s+rounding|not\s+equal\s+total\s+amounts\s+reported\s+due\s+to\s+rounding)[^\n]*)", re.IGNORECASE),
    re.compile(r"([^\n]*?(?:may\s+not\s+foot\s+due\s+to\s+rounding|differ\s+due\s+to\s+rounding|totals\s+may\s+not\s+add)[^\n]*)", re.IGNORECASE),
)


def _extract_filing_rounding_disclaimer(ticker: str, base_period: int) -> str | None:
    """Locate the filer's explicit rounding disclaimer from primary filing text if present."""
    if not ticker:
        return None
    try:
        from scripts.checkout_paths import sibling
        risk_root = sibling("risk_module")
        candidates = list(risk_root.glob(f"data/generations/*/filings/edgar/{ticker}/*{base_period}*"))
        for cand in candidates:
            if cand.is_file():
                text = cand.read_text(encoding="utf-8", errors="ignore")
                for pat in DISCLAIMER_PATTERNS:
                    m = pat.search(text)
                    if m:
                        cleaned = " ".join(m.group(1).split())
                        if len(cleaned) > 20:
                            return cleaned
    except Exception:
        pass
    return None


def _base_year_footing_gap(
    model: FinancialModel,
    *,
    values: ComputedValues,
    projection_periods: list[int],
    unclaimed_statements: Collection[str] = (),
) -> _FootingGap | None:
    """The last historical column's filed footing gap, or ``None`` when it foots.

    Strict Integrity Guard (Henry 2026-10-13):
    1. Unclaimed face lines: If ANY balance sheet line is unclaimed in statement bindings
       (BALANCE_SHEET_STATEMENT in unclaimed_statements), an imbalance can never be excused
       as filer rounding; it remains an error.
    2. Size bound: The footing gap must be within rounding of the reported precision (no more
       than 2.5 units in the model's reported scale).
    3. Disclaimer citation: The filing itself must contain an explicit rounding disclaimer
       (e.g. Note 1 disclaimer), and the citation carries that actual text.
    """
    if BALANCE_SHEET_STATEMENT in unclaimed_statements:
        return None
    if not projection_periods:
        return None
    base_periods = [period for period in _historical_periods(model) if period < projection_periods[0]]
    if not base_periods:
        return None
    base_period = max(base_periods)
    tolerance = _half_cent_in_scale(model._index.get(BALANCE_SHEET_ITEM_IDS[0]))
    gap = 0.0
    parts: list[str] = []
    components: list[float] = []
    for item_id, sign in zip(BALANCE_SHEET_ITEM_IDS, (1, -1, -1)):
        item = model._index.get(item_id)
        printed = values.get(item_id, {}).get(base_period)
        if item is None:
            return None
        if not _is_present(printed):
            # When a total (e.g. Total Liabilities) is not directly printed as a single line
            # in the base year, its value is computed as the sum of its face lines.
            printed = None
        spec = _rendered_projected_spec(item, projection_periods[0])
        operands = None if spec is None else signed_operand_map(spec)
        if not operands or any(offset != 0 for _, offset in operands):
            return None
        face_sum = sum(
            operand_sign * float(base_value)
            for (operand_id, _), operand_sign in operands.items()
            if _is_present(base_value := values.get(operand_id, {}).get(base_period))
        )
        if printed is not None:
            gap += sign * (float(printed) - face_sum)
            if abs(float(printed) - face_sum) > tolerance:
                parts.append(
                    f"{item_id} printed={float(printed):.10g}, face lines sum={face_sum:.10g}"
                )
        components.append(sign * face_sum)
    # Size bound: must be within rounding of the reported scale (<= 2.5 scale units)
    scale_unit = 1.0  # In model coordinates where scale is millions/thousands, 1 unit = 1.0
    if abs(gap) <= tolerance or abs(gap) > 2.5 * scale_unit:
        return None

    ticker = str(getattr(getattr(model, "company", None), "ticker", "") or "")
    disclaimer = _extract_filing_rounding_disclaimer(ticker, base_period)
    if not disclaimer:
        return None

    return _FootingGap(
        base_period=base_period,
        carried_delta=sum(components),
        rendered=f"{'; '.join(parts)} ({disclaimer})",
    )


def _metadata_findings(
    model: FinancialModel,
    *,
    values: ComputedValues,
    projection_periods: list[int],
    semantic_units: DeclaredSemanticUnitPlane,
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    """Enforce operational unit/scale/sign contracts for compiled controls.

    The template's rows and the model kind's declared semantic rows are held to
    the same contract; only their residence differs. A semantic row this model
    kind declares somewhere but gives no unit row is refused rather than
    skipped — the baked table this replaced would have caught it, so declaring
    the row without its unit may not become a silent exemption (plan §6,
    ``semantic_unit_undeclared``).
    """

    required: set[str] = set()
    findings: list[ProjectedFinancialIntegrityFinding] = []
    for item_id in sorted(semantic_units.required_ids):
        if item_id in semantic_units.units or item_id not in model._index:
            continue
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="semantic_unit_undeclared",
                repair_owner="engine",
                detail=(
                    f"model kind {semantic_units.model_kind!r} declares semantic "
                    f"row {item_id!r} but no unit row for it; declare its unit "
                    "and scale in the model-kind data lane"
                ),
                item_id=item_id,
            )
        )
    control_metadata = {**_TEMPLATE_CONTROL_METADATA, **semantic_units.units}
    for item_id, (expected_unit, expected_scale) in control_metadata.items():
        if item_id not in model._index:
            continue
        required.add(item_id)
        item = model.get_item(item_id)
        if item.unit is not expected_unit or item.model_scale != expected_scale:
            findings.append(
                ProjectedFinancialIntegrityFinding(
                    code="projected_unit_scale_invariant_failed",
                    repair_owner="engine",
                    detail=(
                        f"{item_id!r} requires unit={expected_unit.value!r} and "
                        f"model_scale={expected_scale!r}; observed "
                        f"unit={getattr(item.unit, 'value', item.unit)!r} and "
                        f"model_scale={item.model_scale!r}"
                    ),
                    item_id=item_id,
                )
            )
        negative_periods = tuple(
            period
            for period in projection_periods
            if _is_present(value := values.get(item_id, {}).get(period))
            and float(value) < 0.0
        )
        if negative_periods:
            findings.append(
                ProjectedFinancialIntegrityFinding(
                    code="projected_sign_invariant_failed",
                    repair_owner="driver",
                    detail=(
                        f"{item_id!r} is a non-negative control but is negative "
                        f"in periods {list(negative_periods)}"
                    ),
                    item_id=item_id,
                    periods=negative_periods,
                    bound=0.0,
                )
            )
    return required, findings


def _schedule_roll_forward_findings(
    model: FinancialModel,
    *,
    graph: DependencyGraph,
    values: ComputedValues,
    projection_periods: list[int],
    tolerances: DiagnosticTolerances,
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    """Check the canonical schedule roll-forwards compiled by BM-5."""

    historical_periods = sorted(_historical_periods(model))
    required: set[str] = set()
    findings: list[ProjectedFinancialIntegrityFinding] = []
    for (
        family,
        ending_id,
        beginning_id,
        additions_id,
        subtractions_id,
        required_note,
    ) in (
        _ROLL_FORWARD_SPECS
    ):
        if ending_id not in model._index:
            continue
        ending_item = model.get_item(ending_id)
        if (
            ending_item.projected is None
            or ending_item.projected.note != required_note
        ):
            continue
        item_ids = {
            ending_id,
            additions_id,
            *(tuple([subtractions_id]) if subtractions_id is not None else ()),
            *(tuple([beginning_id]) if beginning_id is not None else ()),
        }
        required.update(item_ids)
        missing: dict[str, list[int]] = {}
        mismatched: list[int] = []
        for index, period in enumerate(projection_periods):
            prior_period = (
                historical_periods[-1]
                if index == 0 and historical_periods
                else projection_periods[index - 1] if index > 0 else None
            )
            beginning = (
                values.get(beginning_id, {}).get(period)
                if beginning_id is not None
                else values.get(ending_id, {}).get(prior_period)
                if prior_period is not None
                else None
            )
            period_values = {
                ending_id: values.get(ending_id, {}).get(period),
                additions_id: values.get(additions_id, {}).get(period),
            }
            if subtractions_id is not None:
                period_values[subtractions_id] = values.get(subtractions_id, {}).get(period)
            if beginning_id is not None:
                period_values[beginning_id] = beginning
            elif not _is_present(beginning):
                missing.setdefault(ending_id, []).append(period)
            for item_id, value in period_values.items():
                if not _is_present(value):
                    missing.setdefault(item_id, []).append(period)
            if (
                beginning is None
                or not _is_present(beginning)
                or not _dict_values_are_present(period_values)
            ):
                continue
            expected = (
                float(beginning)
                + float(period_values[additions_id])
                - (
                    float(period_values[subtractions_id])
                    if subtractions_id is not None
                    else 0.0
                )
            )
            ending = float(period_values[ending_id])
            tolerance = _max_tolerance(
                max(abs(expected), abs(ending)),
                tolerances.bs_balance_abs_m,
                tolerances.bs_balance_pct,
            )
            if abs(ending - expected) > tolerance:
                mismatched.append(period)
        if missing:
            findings.extend(
                _inputs_missing_findings(
                    model,
                    graph,
                    values,
                    code="projected_schedule_roll_forward_inputs_missing",
                    item_id=ending_id,
                    gaps=missing,
                    detail=_missing_detail(f"projected {family} roll-forward", missing),
                )
            )
        if mismatched:
            findings.append(
                ProjectedFinancialIntegrityFinding(
                    code="projected_schedule_roll_forward_mismatch",
                    repair_owner="engine",
                    detail=(
                        f"projected {family} ending balance does not equal "
                        "beginning plus additions less subtractions in periods "
                        f"{mismatched}"
                    ),
                    item_id=ending_id,
                    periods=tuple(mismatched),
                    related_item_ids=tuple(sorted(item_ids - {ending_id})),
                )
            )
    return required, findings


def _cash_roll_forward_findings(
    model: FinancialModel,
    *,
    graph: DependencyGraph,
    values: ComputedValues,
    projection_periods: list[int],
    tolerances: DiagnosticTolerances,
    unclaimed_statements: Collection[str],
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    if not any(item_id in model._index for item_id in CASH_FLOW_ITEM_IDS):
        return set(), []

    required: set[str] = set(CASH_FLOW_ITEM_IDS)
    historical_periods = sorted(_historical_periods(model))
    active_periods = [*historical_periods, *projection_periods]
    if (
        CASH_FLOW_FX_ITEM_ID in model._index
        and any(
            _is_present(values.get(CASH_FLOW_FX_ITEM_ID, {}).get(period))
            for period in active_periods
        )
    ):
        required.add(CASH_FLOW_FX_ITEM_ID)

    gaps = _required_value_gaps(
        model,
        values=values,
        item_ids=tuple(sorted(required)),
        periods=projection_periods,
    )
    end_cash_id = CASH_FLOW_ITEM_IDS[5]
    if projection_periods:
        first_projection = projection_periods[0]
        if not historical_periods or not _is_present(
            values.get(end_cash_id, {}).get(historical_periods[-1] if historical_periods else -1)
        ):
            gaps.setdefault(end_cash_id, []).append(first_projection)
            gaps[end_cash_id] = sorted(set(gaps[end_cash_id]))

    findings: list[ProjectedFinancialIntegrityFinding] = []
    if gaps:
        findings.extend(
            _inputs_missing_findings(
                model,
                graph,
                values,
                code="projected_cash_roll_forward_inputs_missing",
                item_id=end_cash_id,
                gaps=gaps,
                detail=_missing_detail("projected cash roll-forward", gaps),
            )
        )

    (
        operating_id,
        investing_id,
        financing_id,
        net_change_id,
        beginning_cash_id,
        _,
    ) = CASH_FLOW_ITEM_IDS
    opening_mismatch_periods: list[int] = []
    net_change_mismatch_periods: list[int] = []
    ending_mismatch_periods: list[int] = []
    for index, period in enumerate(projection_periods):
        prior_period = historical_periods[-1] if index == 0 and historical_periods else (
            projection_periods[index - 1] if index > 0 else None
        )
        required_period_values = {
            item_id: values.get(item_id, {}).get(period)
            for item_id in required
        }
        prior_ending_cash = (
            values.get(end_cash_id, {}).get(prior_period)
            if prior_period is not None
            else None
        )
        if not _dict_values_are_present(required_period_values):
            continue
        if prior_ending_cash is None or not _is_present(prior_ending_cash):
            continue

        beginning_cash = float(required_period_values[beginning_cash_id])
        ending_cash = float(required_period_values[end_cash_id])
        prior_ending_cash_value = float(prior_ending_cash)
        opening_tolerance = _max_tolerance(
            max(abs(beginning_cash), abs(prior_ending_cash_value)),
            tolerances.cf_reconciliation_abs_m,
            tolerances.cf_reconciliation_pct,
        )
        if abs(beginning_cash - prior_ending_cash_value) > opening_tolerance:
            opening_mismatch_periods.append(period)

        flow_sum = sum(
            float(required_period_values[item_id])
            for item_id in (operating_id, investing_id, financing_id)
        )
        if CASH_FLOW_FX_ITEM_ID in required:
            flow_sum += float(required_period_values[CASH_FLOW_FX_ITEM_ID])
        net_change = float(required_period_values[net_change_id])
        net_change_tolerance = _max_tolerance(
            max(abs(net_change), abs(flow_sum)),
            tolerances.cf_reconciliation_abs_m,
            tolerances.cf_reconciliation_pct,
        )
        if abs(net_change - flow_sum) > net_change_tolerance:
            net_change_mismatch_periods.append(period)

        expected_ending_cash = beginning_cash + net_change
        ending_tolerance = _max_tolerance(
            max(abs(ending_cash), abs(expected_ending_cash)),
            tolerances.cf_reconciliation_abs_m,
            tolerances.cf_reconciliation_pct,
        )
        if abs(ending_cash - expected_ending_cash) > ending_tolerance:
            ending_mismatch_periods.append(period)

    if opening_mismatch_periods:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="projected_cash_opening_balance_mismatch",
                repair_owner=_identity_owner(CASH_FLOW_STATEMENT, unclaimed_statements, "engine"),
                detail=(
                    "projected beginning cash does not equal prior-period ending cash "
                    f"within the approved tolerance in periods {opening_mismatch_periods}"
                ),
                item_id=beginning_cash_id,
                periods=tuple(opening_mismatch_periods),
                related_item_ids=(beginning_cash_id, end_cash_id),
                statement=CASH_FLOW_STATEMENT,
            )
        )
    if net_change_mismatch_periods:
        related_ids = [operating_id, investing_id, financing_id]
        if CASH_FLOW_FX_ITEM_ID in required:
            related_ids.append(CASH_FLOW_FX_ITEM_ID)
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="projected_cash_net_change_mismatch",
                repair_owner=_identity_owner(CASH_FLOW_STATEMENT, unclaimed_statements, "engine"),
                detail=(
                    "projected net change in cash does not equal operating, investing, "
                    "financing, and applicable FX cash flows within the approved tolerance "
                    f"in periods {net_change_mismatch_periods}"
                ),
                item_id=net_change_id,
                periods=tuple(net_change_mismatch_periods),
                related_item_ids=(net_change_id, *related_ids),
                statement=CASH_FLOW_STATEMENT,
            )
        )
    if ending_mismatch_periods:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="projected_cash_ending_balance_mismatch",
                repair_owner=_identity_owner(CASH_FLOW_STATEMENT, unclaimed_statements, "engine"),
                detail=(
                    "projected ending cash does not equal beginning cash plus net change "
                    f"within the approved tolerance in periods {ending_mismatch_periods}"
                ),
                item_id=end_cash_id,
                periods=tuple(ending_mismatch_periods),
                related_item_ids=(end_cash_id, beginning_cash_id, net_change_id),
                statement=CASH_FLOW_STATEMENT,
            )
        )
    return required, findings


def _revenue_findings(
    model: FinancialModel,
    *,
    graph: DependencyGraph,
    values: ComputedValues,
    projection_periods: list[int],
    tolerances: DiagnosticTolerances,
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    if TOTAL_REVENUE_ITEM_ID not in model._index:
        return set(), []

    required = {TOTAL_REVENUE_ITEM_ID}
    findings: list[ProjectedFinancialIntegrityFinding] = []
    total_revenue_item = model.get_item(TOTAL_REVENUE_ITEM_ID)
    active_periods = [*sorted(_historical_periods(model)), *projection_periods]
    if total_revenue_item.projected is not None:
        direct_refs = _additive_formula_ref_item_ids(total_revenue_item.projected)
        required_components = sorted(
            item_id
            for item_id in direct_refs
            if item_id != TOTAL_REVENUE_ITEM_ID
            and _is_required_revenue_component(
                model,
                item_id=item_id,
                values=values,
                active_periods=active_periods,
            )
        )
        required.update(required_components)
        for item_id in required_components:
            missing_periods = tuple(
                period
                for period in projection_periods
                if not _is_present(values.get(item_id, {}).get(period))
            )
            if missing_periods:
                findings.append(
                    ProjectedFinancialIntegrityFinding(
                        code="required_revenue_component_projection_missing",
                        repair_owner="driver" if item_id in model._index else "engine",
                        detail=(
                            f"required direct revenue component {item_id!r} is missing or "
                            f"non-finite in projection periods {list(missing_periods)}"
                        ),
                        item_id=item_id,
                        periods=missing_periods,
                        related_item_ids=(TOTAL_REVENUE_ITEM_ID,),
                    )
                )

        reconciliation_periods: list[int] = []
        for period in projection_periods:
            total = values.get(TOTAL_REVENUE_ITEM_ID, {}).get(period)
            component_values = [values.get(item_id, {}).get(period) for item_id in required_components]
            if (
                not required_components
                or total is None
                or not _is_present(total)
                or not _list_values_are_present(component_values)
            ):
                continue
            component_sum = sum(float(value) for value in component_values)
            total_value = float(total)
            tolerance = _max_tolerance(
                max(abs(total_value), abs(component_sum)),
                tolerances.is_subtotal_abs_m,
                tolerances.is_subtotal_pct,
            )
            if abs(total_value - component_sum) > tolerance:
                reconciliation_periods.append(period)
        if reconciliation_periods:
            findings.append(
                ProjectedFinancialIntegrityFinding(
                    code="required_revenue_component_reconciliation_failed",
                    repair_owner="engine",
                    detail=(
                        "projected total revenue does not reconcile to its required direct "
                        f"components in periods {reconciliation_periods}"
                    ),
                    item_id=TOTAL_REVENUE_ITEM_ID,
                    periods=tuple(reconciliation_periods),
                    related_item_ids=tuple(required_components),
                )
            )

    revenue_values = values.get(TOTAL_REVENUE_ITEM_ID, {})
    historical_periods = sorted(_historical_periods(model))
    continuity_periods = [*historical_periods[-1:], *projection_periods]
    broken_periods: list[int] = []
    observed_ratios: list[float] = []
    for prior_period, period in zip(continuity_periods, continuity_periods[1:]):
        prior_value = revenue_values.get(prior_period)
        current_value = revenue_values.get(period)
        if (
            prior_value is None
            or current_value is None
            or not _is_present(prior_value)
            or not _is_present(current_value)
        ):
            continue
        prior_revenue = float(prior_value)
        if prior_revenue < 1.0:
            continue
        ratio = float(current_value) / prior_revenue
        if ratio <= MIN_REVENUE_CONTINUITY_RATIO:
            broken_periods.append(period)
            observed_ratios.append(ratio)
    if broken_periods:
        seed_roots = {
            (root_id, period)
            for root_id, period in _input_dependency_roots(
                model, graph, values,
                {TOTAL_REVENUE_ITEM_ID: broken_periods},
                missing_only=False,
            )
            if historical_periods
            and period == historical_periods[-1]
            and root_id in model._index
            and _has_historical_driver_seed(model.get_item(root_id), period)
        }
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="projected_revenue_continuity_break",
                repair_owner="driver",
                detail=(
                    "projected total revenue retains 50% or less of the prior-period "
                    f"value in periods {broken_periods}; minimum observed ratio="
                    f"{min(observed_ratios):.6g}"
                ),
                item_id=TOTAL_REVENUE_ITEM_ID,
                periods=tuple(broken_periods),
                related_item_ids=tuple(sorted({root for root, _period in seed_roots})),
                repair_periods=tuple(sorted({period for _root, period in seed_roots})),
                bound=MIN_REVENUE_CONTINUITY_RATIO,
            )
        )
    return required, findings


def _required_value_gaps(
    model: FinancialModel,
    *,
    values: ComputedValues,
    item_ids: tuple[str, ...],
    periods: list[int],
) -> dict[str, list[int]]:
    return {
        item_id: [
            period
            for period in periods
            if item_id not in model._index
            or not _is_present(values.get(item_id, {}).get(period))
        ]
        for item_id in item_ids
        if item_id not in model._index
        or any(not _is_present(values.get(item_id, {}).get(period)) for period in periods)
    }


def _is_required_revenue_component(
    model: FinancialModel,
    *,
    item_id: str,
    values: ComputedValues,
    active_periods: list[int],
) -> bool:
    """Distinguish materialized components from unresolved template slots."""

    item = model._index.get(item_id)
    if item is None:
        # A dangling member of the accepted additive total is structural
        # evidence of requiredness, not permission to erase the denominator.
        return True
    if item.template_token is None:
        # Business-model compilation clears template_token on every accepted
        # segment row, so an all-period-missing compiled component still blocks.
        return True
    return any(
        _is_present(values.get(item_id, {}).get(period))
        for period in active_periods
    )


def _missing_detail(scope: str, gaps: dict[str, list[int]]) -> str:
    rendered = "; ".join(
        f"{item_id}={sorted(set(periods))}"
        for item_id, periods in sorted(gaps.items())
    )
    return f"{scope} has missing or non-finite required inputs: {rendered}"


def _additive_formula_ref_item_ids(spec: FormulaSpec) -> set[str]:
    """Return direct members only for a closed additive/ref rollup shape."""

    raw_members: Any
    if spec.type.value == "ref":
        raw_members = [spec.params.get("source")]
    elif spec.type.value == "arithmetic":
        operands = spec.params.get("operands")
        if isinstance(operands, list) and operands and operands[0] == "+":
            raw_members = operands[1:]
        elif str(spec.params.get("function") or "").upper() == "SUM":
            raw_members = spec.params.get("items")
        else:
            expr = spec.params.get("expr")
            if not isinstance(expr, dict) or expr.get("op") != "+":
                return set()
            raw_members = expr.get("args")
    else:
        return set()

    if not isinstance(raw_members, list) or not raw_members:
        return set()
    refs: list[LineItemRef] = []
    for member in raw_members:
        ref = member if isinstance(member, LineItemRef) else line_item_ref_from_obj(member)
        if ref is None:
            return set()
        refs.append(ref)
    return {ref.id for ref in refs if int(ref.t) == 0}


def _max_tolerance(base: float, absolute: float, relative: float) -> float:
    return max(float(absolute), abs(float(base)) * float(relative))


def cash_roll_tolerance(
    left: float, right: float, tolerances: DiagnosticTolerances | None = None
) -> float:
    """The bound two sides of a historical cash identity agree within: the approved
    ``cf_reconciliation`` tolerance, scaled by the larger level compared."""

    resolved = tolerances or DiagnosticTolerances()
    return _max_tolerance(
        max(abs(left), abs(right)),
        resolved.cf_reconciliation_abs_m,
        resolved.cf_reconciliation_pct,
    )


def cash_roll_identity_holds(
    left: float, right: float, tolerances: DiagnosticTolerances | None = None
) -> bool:
    """Whether two levels of one historical cash identity agree: ending cash against beginning
    plus net change, beginning cash against the prior ending, ending cash against the balance
    sheet's cash. The one comparison ``_historical_cash_roll_findings`` makes; the journey's
    ``cash_roll_ties_bs`` evaluates the same level-shaped identities through it, never a
    flow-shaped restatement with its own bound."""

    return abs(left - right) <= cash_roll_tolerance(left, right, tolerances)


def evaluate_rendered_historical_statement_integrity(
    model: FinancialModel,
    *,
    recalculated_values: ComputedValues,
    template: FinancialModel,
    tolerances: DiagnosticTolerances | None = None,
    printed_unit_dollars: Mapping[int, PrintedUnit] | None = None,
) -> ProjectedFinancialIntegrityResult:
    """Evaluate statement identities on the rendered historical columns.

    ``recalculated_values`` are the recalculated workbook's cell values in
    canonical model space. Families activate from the same anchors as the
    projected check. A period whose identity operands are not all finite is
    skipped, except that missing historical cash-flow ending cash is reported
    as a binding input gap. The balance sheet is exact to half a cent; the cash
    roll uses the projected cash-roll tolerance. ``template`` supplies each
    statement total's operand set, so a compiled historical total that drops a
    rendered operand is reported even when the column happens to balance.
    ``printed_unit_dollars`` maps a historical period to the dollar size of one
    unit the filer printed that column's total assets in and what that size is read
    from (:class:`PrintedUnit`; derived where the bound facts are visible); it bounds
    the filed footing gap. A column without an entry has no filed rounding bound,
    so a delta beyond the half-cent identity tolerance is an imbalance.
    """

    model.build_index()
    template.build_index()
    periods = sorted(_historical_periods(model))
    values = {
        str(item_id): {int(period): value for period, value in item_values.items()}
        for item_id, item_values in recalculated_values.items()
    }
    resolved_tolerances = tolerances or DiagnosticTolerances()

    required_item_ids: set[str] = set()
    findings: list[ProjectedFinancialIntegrityFinding] = []

    balance_required, balance_findings = _historical_balance_sheet_findings(
        model,
        values=values,
        historical_periods=periods,
        printed_unit_dollars=printed_unit_dollars or {},
    )
    required_item_ids.update(balance_required)
    findings.extend(balance_findings)

    cash_required, cash_findings = _historical_cash_roll_findings(
        model,
        values=values,
        historical_periods=periods,
        tolerances=resolved_tolerances,
    )
    required_item_ids.update(cash_required)
    findings.extend(cash_findings)

    findings.extend(
        _dropped_operand_findings(
            model,
            template=template,
            values=values,
            periods=periods,
            lane="historical",
        )
    )

    return ProjectedFinancialIntegrityResult(
        required_item_ids=tuple(sorted(required_item_ids)),
        findings=tuple(findings),
    )


def evaluate_rendered_projected_statement_integrity(
    model: FinancialModel,
    *,
    recalculated_values: ComputedValues,
    template: FinancialModel,
) -> ProjectedFinancialIntegrityResult:
    """Compare each statement total's rendered projected operands to the template.

    The projected counterpart of the historical operand-set comparison in
    :func:`evaluate_rendered_historical_statement_integrity`: every projected
    column's rendered spec of a balance-sheet or cash-flow total is read
    against the template's projected operand set, and a template operand
    whose own recalculated projected cell holds a number but which the
    rendered total omits is ``statement_total_operand_dropped``, the code a
    historical column reports. The totals are the rows with a template
    historical formula; a component row whose compiled projected definition
    relocates an operand to a sibling line is not compared. The projected
    balance and cash identities themselves are
    :func:`evaluate_projected_financial_integrity`'s.
    """

    model.build_index()
    template.build_index()
    periods = sorted(
        int(period) for period in model.time_structure.projection_periods or ()
    )
    values = {
        str(item_id): {int(period): value for period, value in item_values.items()}
        for item_id, item_values in recalculated_values.items()
    }
    return ProjectedFinancialIntegrityResult(
        required_item_ids=(),
        findings=tuple(
            _dropped_operand_findings(
                model,
                template=template,
                values=values,
                periods=periods,
                lane="projected",
            )
        ),
    )


class PrintedUnit(NamedTuple):
    """The dollar size of one printed unit of a column's total assets, and its source.

    ``printed_unit``: the filer's XBRL ``decimals`` (``10**-decimals`` dollars).
    ``display_scale``: one unit of the fact's display scale, for a sealed bundle
    whose fact carries no ``decimals``.
    """

    dollars: float
    basis: Literal["printed_unit", "display_scale"]


def _footing_bound(
    model: FinancialModel,
    *,
    period: int,
    printed_unit_dollars: Mapping[int, PrintedUnit],
) -> tuple[float | None, str]:
    """The filing-derived footing bound of one column, in the delta's model scale.

    ``printed_unit`` uses the filer's ``decimals``; ``display_scale`` uses a legacy
    fact's printed scale when it has no decimals. Without a bound total-assets fact,
    no filed precision is known and no non-zero delta can be attributed to rounding.
    """

    unit = printed_unit_dollars.get(period)
    if unit is None:
        return None, "none"
    return unit.dollars / _scale_multiplier(model._index.get(BALANCE_SHEET_ITEM_IDS[0])), unit.basis


def _filed_subtotal_lines(
    model: FinancialModel,
    *,
    values: ComputedValues,
    period: int,
) -> list[dict[str, Any]]:
    """The filed lines that produce each balance-sheet total in ``period``.

    A total that renders as a filed constant names itself; a total composed
    of bound lines names its signed operands (for a liabilities total, the
    current-liabilities subtotal and the noncurrent lines that sum to it).
    """

    lines: list[dict[str, Any]] = []
    for total_id in BALANCE_SHEET_ITEM_IDS:
        item = model._index.get(total_id)
        spec = None if item is None else _rendered_historical_spec(item, period)
        operands = signed_operand_map(spec) if spec is not None else None
        if (
            spec is not None
            and spec.type is not FormulaType.constant
            and operands
            and all(offset == 0 for _, offset in operands)
        ):
            entries = [
                {"period": int(period), "item_id": operand_id, "value": float(value) * sign}
                for (operand_id, _offset), sign in sorted(operands.items())
                if _is_present(value := values.get(operand_id, {}).get(period))
            ]
            if entries:
                lines.extend(entries)
                continue
        total_value = values.get(total_id, {}).get(period)
        if _is_present(total_value):
            lines.append(
                {"period": int(period), "item_id": total_id, "value": float(total_value)}
            )
    return lines


def _historical_balance_sheet_findings(
    model: FinancialModel,
    *,
    values: ComputedValues,
    historical_periods: list[int],
    printed_unit_dollars: Mapping[int, PrintedUnit],
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    """Historical balance-sheet identity to half a cent, with one informational case.

    A column in which at least one total is a printed (constant) value carries the
    filer's own footing. Within one unit of the bound total-assets fact's printed
    precision (``_footing_bound``), a gap is recorded as
    ``filed_balance_sheet_footing_gap`` (informational), naming the filed subtotal
    lines and the delta per year. Without that fact's precision there is no filed
    rounding to attribute a non-zero delta to. Such a delta, a column whose totals
    are all compositions of bound lines, or a delta beyond the filed bound stays
    ``historical_balance_sheet_imbalance``.
    """

    if not any(item_id in model._index for item_id in BALANCE_SHEET_ITEM_IDS):
        return set(), []

    assets_id, liabilities_id, equity_id = BALANCE_SHEET_ITEM_IDS
    tolerance = _half_cent_in_scale(model._index.get(assets_id))
    unbalanced: list[tuple[int, float, float, float]] = []
    filed: list[tuple[int, float, float, float]] = []
    filed_bounds: dict[int, tuple[float, str]] = {}
    bounds: dict[int, tuple[float | None, str]] = {}
    filed_lines: list[dict[str, Any]] = []
    for period in historical_periods:
        period_values = [values.get(item_id, {}).get(period) for item_id in BALANCE_SHEET_ITEM_IDS]
        if not _list_values_are_present(period_values):
            continue
        assets, liabilities, equity = (float(value) for value in period_values)
        liabilities_and_equity = liabilities + equity
        delta = assets - liabilities_and_equity
        if abs(delta) > tolerance:
            row = (period, assets, liabilities_and_equity, delta)
            printed = _printed_total(model, period)
            bound, basis = _footing_bound(
                model, period=period, printed_unit_dollars=printed_unit_dollars
            )
            bounds[period] = (bound, basis)
            # One printed unit to half a cent: a gap of exactly one unit carries the float noise of
            # subtracting figures of different magnitude (35014.001 - 35014 is 1.0000000000218e-3).
            if printed is not None and bound is not None and abs(delta) <= bound + tolerance:
                filed.append(row)
                filed_bounds[period] = (bound, basis)
                filed_lines.extend(
                    _filed_subtotal_lines(model, values=values, period=period)
                )
            else:
                unbalanced.append(row)
    if not unbalanced and not filed:
        return set(BALANCE_SHEET_ITEM_IDS), []

    def rendered(rows: list[tuple[int, float, float, float]]) -> str:
        return "; ".join(
            f"FY{period}: total assets={assets:.10g}, liabilities plus equity="
            f"{liabilities_and_equity:.10g}, delta={delta:.10g}"
            for period, assets, liabilities_and_equity, delta in rows
        )

    findings: list[ProjectedFinancialIntegrityFinding] = []
    bases = {basis for _bound, basis in filed_bounds.values()}
    if filed:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="filed_balance_sheet_footing_gap",
                repair_owner="binding",
                detail=(
                    "the filed historical balance sheet does not foot: printed total assets "
                    "differ from printed liabilities plus equity by the filer's own rounding "
                    "(the filing's rounding disclaimer); the model keeps the filed face "
                    f"values, records the gap as informational and adds no plug: {rendered(filed)}"
                ),
                item_id=assets_id,
                periods=tuple(period for period, *_ in filed),
                related_item_ids=(assets_id, liabilities_id, equity_id),
                bound=max(bound for bound, _basis in filed_bounds.values()),
                statement=BALANCE_SHEET_STATEMENT,
                observed_delta=max(filed, key=lambda row: abs(row[3]))[3],
                observed={
                    "delta_by_period": json.dumps([
                        {
                            "period": period, "delta": delta,
                            "bound": filed_bounds[period][0], "bound_basis": filed_bounds[period][1],
                        }
                        for period, _assets, _le, delta in filed
                    ]),
                    "filed_subtotal_lines": json.dumps(filed_lines),
                    "bound_basis": next(iter(bases)) if len(bases) == 1 else "mixed",
                },
            )
        )
    if unbalanced:
        unbalanced_bases = {bounds[period][1] for period, *_ in unbalanced}
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="historical_balance_sheet_imbalance",
                repair_owner="binding",
                detail=(
                    "rendered historical total assets do not equal total liabilities "
                    f"plus equity to half a cent: {rendered(unbalanced)}"
                ),
                item_id=assets_id,
                periods=tuple(period for period, *_ in unbalanced),
                related_item_ids=(assets_id, liabilities_id, equity_id),
                bound=tolerance,
                statement=BALANCE_SHEET_STATEMENT,
                observed_delta=max(unbalanced, key=lambda row: abs(row[3]))[3],
                observed={
                    "delta_by_period": json.dumps([
                        {
                            "period": period, "delta": delta,
                            "bound": bounds[period][0], "bound_basis": bounds[period][1],
                        }
                        for period, _assets, _le, delta in unbalanced
                    ]),
                    "bound_basis": (
                        next(iter(unbalanced_bases)) if len(unbalanced_bases) == 1 else "mixed"
                    ),
                },
            )
        )
    return set(BALANCE_SHEET_ITEM_IDS), findings


def _printed_total(model: FinancialModel, period: int) -> LineItem | None:
    """The first balance-sheet total that renders as a filed constant in ``period``."""

    for item_id in BALANCE_SHEET_ITEM_IDS:
        item = model._index.get(item_id)
        spec = None if item is None else _rendered_historical_spec(item, period)
        if item is not None and spec is not None and spec.type is FormulaType.constant:
            return item
    return None


def _scale_multiplier(item: LineItem | None) -> float:
    scale = item.model_scale if item is not None else "units"
    return float(_SCALE_MULTIPLIERS["unit" if scale == "units" else scale])


def _half_cent_in_scale(item: LineItem | None) -> float:
    return _HISTORICAL_BALANCE_TOLERANCE_DOLLARS / _scale_multiplier(item)


def _historical_cash_roll_findings(
    model: FinancialModel,
    *,
    values: ComputedValues,
    historical_periods: list[int],
    tolerances: DiagnosticTolerances,
) -> tuple[set[str], list[ProjectedFinancialIntegrityFinding]]:
    if not any(item_id in model._index for item_id in CASH_FLOW_ITEM_IDS):
        return set(), []

    required: set[str] = set(CASH_FLOW_ITEM_IDS)
    if CASH_FLOW_FX_ITEM_ID in model._index and any(
        _is_present(values.get(CASH_FLOW_FX_ITEM_ID, {}).get(period))
        for period in historical_periods
    ):
        required.add(CASH_FLOW_FX_ITEM_ID)
    (
        operating_id,
        investing_id,
        financing_id,
        net_change_id,
        beginning_cash_id,
        end_cash_id,
    ) = CASH_FLOW_ITEM_IDS
    flow_ids = [operating_id, investing_id, financing_id]
    if CASH_FLOW_FX_ITEM_ID in required:
        flow_ids.append(CASH_FLOW_FX_ITEM_ID)

    def period_values(item_ids: list[str], period: int) -> list[float] | None:
        observed = [values.get(item_id, {}).get(period) for item_id in item_ids]
        if not _list_values_are_present(observed):
            return None
        return [float(value) for value in observed]

    def outside(left: float, right: float) -> bool:
        return not cash_roll_identity_holds(left, right, tolerances)

    net_change_mismatches: list[tuple[int, float, float]] = []
    ending_mismatches: list[tuple[int, float, float]] = []
    opening_mismatches: list[tuple[int, float, float]] = []
    for index, period in enumerate(historical_periods):
        net_inputs = period_values([net_change_id, *flow_ids], period)
        if net_inputs is not None:
            net_change, *flows = net_inputs
            if outside(net_change, sum(flows)):
                net_change_mismatches.append((period, net_change, sum(flows)))
        ending_inputs = period_values([end_cash_id, beginning_cash_id, net_change_id], period)
        if ending_inputs is not None:
            ending_cash, beginning_cash, net_change = ending_inputs
            if outside(ending_cash, beginning_cash + net_change):
                ending_mismatches.append((period, ending_cash, beginning_cash + net_change))
        if index == 0:
            continue
        beginning = period_values([beginning_cash_id], period)
        prior_ending = period_values([end_cash_id], historical_periods[index - 1])
        if beginning is not None and prior_ending is not None:
            if outside(beginning[0], prior_ending[0]):
                opening_mismatches.append((period, beginning[0], prior_ending[0]))

    def rendered(rows: list[tuple[int, float, float]], left: str, right: str) -> str:
        return "; ".join(
            f"FY{period}: {left}={observed:.10g}, {right}={expected:.10g}"
            for period, observed, expected in rows
        )

    findings: list[ProjectedFinancialIntegrityFinding] = []
    missing_end_periods = tuple(
        period
        for period in historical_periods
        if not _is_present(values.get(end_cash_id, {}).get(period))
    )
    if missing_end_periods:
        end_item = model._index.get(end_cash_id)
        end_role = end_item.required_role if end_item is not None else None
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="projected_cash_roll_forward_inputs_missing",
                repair_owner="binding",
                detail=(
                    "historical cash roll-forward cannot be evaluated: "
                    f"{end_role or end_cash_id} has no bound ending-cash value "
                    f"in periods {list(missing_end_periods)}"
                ),
                item_id=end_cash_id,
                periods=missing_end_periods,
                related_item_ids=(end_cash_id,),
                repair_periods=missing_end_periods,
            )
        )
    if net_change_mismatches:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="historical_cash_net_change_mismatch",
                repair_owner="binding",
                detail=(
                    "rendered historical net change in cash does not equal operating, "
                    "investing, financing, and applicable FX cash flows within the "
                    "approved tolerance: "
                    + rendered(net_change_mismatches, "net change", "flow sum")
                ),
                item_id=net_change_id,
                periods=tuple(period for period, *_ in net_change_mismatches),
                related_item_ids=(net_change_id, *flow_ids),
                statement=CASH_FLOW_STATEMENT,
            )
        )
    if ending_mismatches:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="historical_cash_ending_balance_mismatch",
                repair_owner="binding",
                detail=(
                    "rendered historical ending cash does not equal beginning cash plus "
                    "net change within the approved tolerance: "
                    + rendered(ending_mismatches, "ending cash", "beginning plus net change")
                ),
                item_id=end_cash_id,
                periods=tuple(period for period, *_ in ending_mismatches),
                related_item_ids=(beginning_cash_id, net_change_id),
                statement=CASH_FLOW_STATEMENT,
            )
        )
    if opening_mismatches:
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="historical_cash_opening_balance_mismatch",
                repair_owner="binding",
                detail=(
                    "rendered historical beginning cash does not equal prior-period "
                    "ending cash within the approved tolerance: "
                    + rendered(opening_mismatches, "beginning cash", "prior ending cash")
                ),
                item_id=beginning_cash_id,
                periods=tuple(period for period, *_ in opening_mismatches),
                related_item_ids=(beginning_cash_id, end_cash_id),
                statement=CASH_FLOW_STATEMENT,
            )
        )
    return required, findings


def _dropped_operand_findings(
    model: FinancialModel,
    *,
    template: FinancialModel,
    values: ComputedValues,
    periods: list[int],
    lane: Literal["historical", "projected"],
) -> list[ProjectedFinancialIntegrityFinding]:
    """Compare each statement total's rendered operands in one lane to the template.

    ``lane`` names the columns ``periods`` are and the template spec the
    operand set comes from. A template operand missing from the compiled
    rendered spec is a finding only where its own recalculated cell holds a
    finite number; extra compiled operands are compiler additions and are
    allowed.
    """

    rendered_spec = (
        _rendered_historical_spec if lane == "historical" else _rendered_projected_spec
    )
    findings: list[ProjectedFinancialIntegrityFinding] = []
    for total_id, template_operands in _template_statement_closure(
        model, template, lane=lane
    ):
        item = model._index.get(total_id)
        if item is None:
            continue
        from .build_company_grounded_schedules import _nsp_lowered_operand_ids

        nsp_removed = _nsp_lowered_operand_ids(item, model)
        dropped: dict[str, list[tuple[int, float]]] = {}
        for period in periods:
            spec = rendered_spec(item, period)
            if spec is None or spec.type is FormulaType.constant:
                continue
            compiled_operands = _t0_ref_item_ids(spec.params)
            for operand_id in sorted(template_operands - compiled_operands - nsp_removed):
                value = values.get(operand_id, {}).get(period)
                if _is_present(value):
                    dropped.setdefault(operand_id, []).append((period, float(value)))
        if not dropped:
            continue
        rendered = "; ".join(
            f"{operand_id} rendered "
            + ", ".join(f"FY{period}={value:.10g}" for period, value in observed)
            for operand_id, observed in sorted(dropped.items())
        )
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="statement_total_operand_dropped",
                repair_owner="document",
                detail=(
                    f"rendered {lane} {total_id!r} omits template operand(s) whose "
                    f"own cells hold a number: {rendered}"
                ),
                item_id=total_id,
                periods=tuple(
                    sorted({period for observed in dropped.values() for period, _ in observed})
                ),
                related_item_ids=tuple(sorted(dropped)),
            )
        )
    return findings


def _template_statement_closure(
    model: FinancialModel,
    template: FinancialModel,
    *,
    lane: Literal["historical", "projected"],
) -> list[tuple[str, set[str]]]:
    """Return each active anchor's statement totals with their ``lane`` operand sets.

    The statement totals are the template's HISTORICAL graph within the
    statement: a row with a template historical formula. Each total's operand
    set is read from the template spec of ``lane``. A component row (no
    template historical formula) is not a total in either lane, even where its
    projected spec is a formula; its definition at the seam is
    :func:`evaluate_row_definition_continuity`'s.
    """

    closure: list[tuple[str, set[str]]] = []
    visited: set[str] = set()
    for anchor_ids, statement_prefix in _STATEMENT_ANCHOR_PREFIXES:
        if not any(item_id in model._index for item_id in anchor_ids):
            continue
        pending: list[str] = list(anchor_ids)
        while pending:
            item_id = pending.pop(0)
            if item_id in visited:
                continue
            visited.add(item_id)
            template_item = template._index.get(item_id)
            if template_item is None:
                continue
            historical_spec = template_item.historical
            if historical_spec is None or historical_spec.type is FormulaType.constant:
                continue
            historical_operands = _t0_ref_item_ids(historical_spec.params) - {item_id}
            if not historical_operands:
                continue
            pending.extend(
                sorted(
                    operand
                    for operand in historical_operands
                    if operand.startswith(statement_prefix)
                )
            )
            if lane == "historical":
                closure.append((item_id, historical_operands))
                continue
            projected_spec = template_item.projected
            if projected_spec is None or projected_spec.type is FormulaType.constant:
                continue
            projected_operands = _t0_ref_item_ids(projected_spec.params) - {item_id}
            if projected_operands:
                closure.append((item_id, projected_operands))
    return closure


def evaluate_statement_total_operand_placement(
    model: FinancialModel,
    *,
    template: FinancialModel,
) -> ProjectedFinancialIntegrityResult:
    """Report statement totals that sum a row on another sheet.

    Walks the balance-sheet and cash-flow anchor closures over every rendered
    column: a historical column renders ``_rendered_historical_spec``, a
    projected column ``_rendered_projected_spec``. Where a column renders an
    arithmetic aggregation, each same-period operand of the total must be a row
    on the total's own sheet (the model's sheet membership, not the id
    prefix); otherwise the displayed components do not foot to the total.
    One finding per total names its off-sheet operands and columns. Reads
    specs only, never values.
    """

    model.build_index()
    template.build_index()
    sheet_of = {
        item.id: sheet_name
        for sheet_name, sheet in model.sheets.items()
        for section in sheet.sections
        for item in section.line_items
    }
    historical_periods = sorted(int(period) for period in _historical_periods(model))
    projection_periods = sorted(
        int(period) for period in model.time_structure.projection_periods or ()
    )
    columns = [
        *((period, _rendered_historical_spec) for period in historical_periods),
        *((period, _rendered_projected_spec) for period in projection_periods),
    ]
    findings: list[ProjectedFinancialIntegrityFinding] = []
    for total_id, template_operands in _template_statement_closure(
        model, template, lane="historical"
    ):
        item = model._index.get(total_id)
        if item is None:
            continue
        total_sheet = sheet_of.get(total_id)
        template_item = template._index.get(total_id)
        declared_operands = set(template_operands)
        if (
            template_item is not None
            and template_item.projected is not None
            and template_item.projected.type is not FormulaType.constant
        ):
            declared_operands |= _t0_ref_item_ids(template_item.projected.params) - {total_id}
        declared_cross_sheet = {
            op_id for op_id in declared_operands if sheet_of.get(op_id) != total_sheet
        }
        offsheet: dict[str, list[int]] = {}
        for period, rendered_spec in columns:
            spec = rendered_spec(item, period)
            if spec is None or spec.type is not FormulaType.arithmetic:
                continue
            operands = signed_operand_map(spec)
            if operands is None:
                continue
            for operand_id, offset in operands:
                if (
                    offset == 0
                    and sheet_of.get(operand_id) != total_sheet
                    and operand_id not in declared_cross_sheet
                ):
                    offsheet.setdefault(operand_id, []).append(period)
        if not offsheet:
            continue
        rendered = "; ".join(
            f"{operand_id} on sheet {sheet_of.get(operand_id)!r} in "
            + ", ".join(f"FY{period}" for period in sorted(set(periods)))
            for operand_id, periods in sorted(offsheet.items())
        )
        findings.append(
            ProjectedFinancialIntegrityFinding(
                code="statement_total_operand_offsheet",
                repair_owner="engine",
                detail=(
                    f"rendered {total_id!r} on sheet {total_sheet!r} sums row(s) "
                    f"that are not on its sheet, so its displayed components do "
                    f"not foot to it: {rendered}"
                ),
                item_id=total_id,
                periods=tuple(
                    sorted({period for periods in offsheet.values() for period in periods})
                ),
                related_item_ids=tuple(sorted(offsheet)),
            )
        )
    return ProjectedFinancialIntegrityResult(required_item_ids=(), findings=tuple(findings))


def evaluate_row_definition_continuity(
    model: FinancialModel,
) -> ProjectedFinancialIntegrityResult:
    """Report rows whose definition changes between the two seam columns.

    The seam is the last historical column ``h`` and the first projected
    column ``p``; each renders ``_rendered_historical_spec`` and
    ``_rendered_projected_spec`` respectively. Two rules, both over the signed
    operand maps of ``signed_operand_map``:

    * Aggregation (rule A). A row rendered as an arithmetic aggregation in both
      columns has the same signed operands in both, after symmetric expansion:
      an operand is replaced by its own operands only where it too is an
      arithmetic aggregation in both columns.
      Only same-period operands whose rows own cells on both sides participate:
      a forecast-only or retired row is absent by cell ownership, not a change
      to the definition of the total. Unpopulated but owned input cells still
      participate. Fixed-cell references retain their single-cell identity.
      In the historical column only, a host the terminal year's lowering cites
      as including children
      (``removed_roles`` on its rendered spec) stands for those children where the
      forecast lists the children and not the host. Same-period forecast operands
      whose roles that same lowering records in ``optional_absences`` are omitted
      from the comparison too, even when an authored forecast participates. Only
      roots are reported -- a flagged row none of whose direct operands is itself flagged.
    * Round trip (rule B). A row ``X`` projected from a control ``C`` whose
      historical column is seeded from ``X`` returns as exactly itself:
      substituting ``C``'s historical map into ``X``'s projected map gives
      ``{X: +1}``.

    Reads specs only, never values.
    """

    model.build_index()
    historical_periods = _historical_periods(model)
    projection_periods = model.time_structure.projection_periods or ()
    if not historical_periods or not projection_periods:
        return ProjectedFinancialIntegrityResult(required_item_ids=(), findings=())
    h = max(historical_periods)
    p = min(int(period) for period in projection_periods)
    items = [
        item
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    ]
    unshared_cell_items = {
        item.id for item in items
        if item.column is None
        and not (
            item_owns_cell(model, item, h)
            and item_owns_cell(model, item, p)
        )
    }

    def _counterpart_ref_target(item_id: str) -> str | None:
        item = model._index.get(item_id)
        if (
            item is not None
            and item.projected is not None
            and item.projected.type is FormulaType.ref
            and "retained bridge operand" in (item.projected.note or "")
        ):
            source = item.projected.params.get("source")
            if source is not None:
                return getattr(source, "id", None) or source.get("id")
        return None

    def cited_lowering(item_id: str) -> dict[str, Any]:
        item = model._index.get(item_id)
        spec = _rendered_historical_spec(item, h) if item is not None else None
        metadata = None if spec is None else spec.params.get("_not_separately_presented")
        return metadata if isinstance(metadata, dict) else {}

    def cited_operands(item_id: str) -> frozenset[tuple[str, int]]:
        """The ``(operand, offset)`` pairs a cited-operand lowering took out of the row.

        Recorded on the lowered formula's derivation (``omitted_operands``) in either
        seam column: an operand whose resolved cell is cited, lagged or not, is no
        operand of the row's definition, so it leaves the comparison in both columns.
        """

        item = model._index.get(item_id)
        out: set[tuple[str, int]] = set()
        for spec in (
            None if item is None else _rendered_historical_spec(item, h),
            None if item is None else _rendered_projected_spec(item, p),
        ):
            if spec is not None and spec.derivation is not None:
                out.update(spec.derivation.omitted_operands)
        return frozenset(out)

    def aggregation(item_id: str, projected: bool) -> dict[tuple[str, int], int] | None:
        item = model._index.get(item_id)
        if item is None:
            return None
        spec = (
            _rendered_projected_spec(item, p)
            if projected
            else _rendered_historical_spec(item, h)
        )
        if spec is None or spec.type is not FormulaType.arithmetic:
            return None
        operands = signed_operand_map(spec)
        if operands is None:
            return None
        absent_roles = cited_lowering(item_id).get("optional_absences", {}) if projected else {}
        resolved: dict[tuple[str, int], int] = {}
        for (operand_id, offset), sign in operands.items():
            if (operand_id, offset) in cited_operands(item_id):
                continue
            operand = model._index.get(operand_id)
            if offset == 0 and operand is not None and operand.required_role in absent_roles:
                continue
            target = _counterpart_ref_target(operand_id) if offset == 0 else None
            effective_id = target if target and target in model._index else operand_id
            resolved[(effective_id, offset)] = resolved.get((effective_id, offset), 0) + sign
        return {key: sign for key, sign in resolved.items() if sign}

    def cited_inclusions(item_id: str) -> dict[str, tuple[str, ...]]:
        """Host item id -> the child items the terminal year's lowering cites as included in it.

        The lowering's own ``removed_roles`` metadata (child role -> host role) on the
        terminal rendered spec names them; the lowered historical formula counts
        the host alone, so the host stands for its children at the seam.
        """

        removed = cited_lowering(item_id).get("removed_roles")
        if not removed:
            return {}
        by_role: dict[str, list[str]] = {}
        for candidate in items:
            if candidate.required_role is not None:
                by_role.setdefault(candidate.required_role, []).append(candidate.id)
        inclusions: dict[str, tuple[str, ...]] = {}
        for child_role, host_role in removed.items():
            for host_id in by_role.get(host_role, ()):
                inclusions[host_id] = (*inclusions.get(host_id, ()), *by_role.get(child_role, ()))
        return inclusions

    def expanded(item_id: str, projected: bool) -> dict[tuple[str, int], int]:
        signs: dict[tuple[str, int], int] = {}

        def expand(current_id: str, sign: int, path: frozenset[str]) -> None:
            inclusions = {} if projected else cited_inclusions(current_id)
            forecast = aggregation(current_id, True) or {}
            for (operand_id, offset), operand_sign in (
                aggregation(current_id, projected) or {}
            ).items():
                if offset == 0 and operand_id in additive_in_both and operand_id not in path:
                    expand(operand_id, sign * operand_sign, path | {operand_id})
                    continue
                children = inclusions.get(operand_id) if offset == 0 else None
                # The host stands for its children only where the forecast lists them
                # and not the host; a forecast that also lowers to the host is unchanged.
                if children and (operand_id, 0) not in forecast and all(
                    (child_id, 0) in forecast for child_id in children
                ):
                    for child_id in children:
                        signs[(child_id, 0)] = signs.get((child_id, 0), 0) + sign * operand_sign
                    continue
                key = (operand_id, offset)
                signs[key] = signs.get(key, 0) + sign * operand_sign

        expand(item_id, 1, frozenset({item_id}))
        return {
            key: sign for key, sign in signs.items()
            if sign and not (key[1] == 0 and key[0] in unshared_cell_items)
        }

    def seeded_round_trip(
        item: Any,
    ) -> tuple[
        dict[tuple[str, int], int],
        dict[str, dict[tuple[str, int], int]],
        dict[tuple[str, int], int],
    ] | None:
        """The projected definition with each control seeded from this row
        in FY``h`` replaced by that control's historical definition."""

        projected_spec = _rendered_projected_spec(item, p)
        projected_map = signed_operand_map(projected_spec) if projected_spec else None
        if not projected_map:
            return None
        seeded: dict[str, dict[tuple[str, int], int]] = {}
        for control_id, offset in projected_map:
            control = model._index.get(control_id)
            if offset != 0 or control_id == item.id or control is None:
                continue
            control_spec = _rendered_historical_spec(control, h)
            control_map = signed_operand_map(control_spec) if control_spec else None
            if control_map and (item.id, 0) in control_map:
                seeded[control_id] = control_map
        if not seeded:
            return None
        round_trip: dict[tuple[str, int], int] = {}
        for key, sign in projected_map.items():
            substituted = seeded.get(key[0]) if key[1] == 0 else None
            for inner_key, inner_sign in (substituted or {key: 1}).items():
                round_trip[inner_key] = round_trip.get(inner_key, 0) + sign * inner_sign
        return (
            projected_map,
            seeded,
            {key: sign for key, sign in round_trip.items() if sign},
        )

    # A row projected through a bridge seeded from itself in FY``h``
    # (``cash = ending cash - bridge``, ``bridge = ending cash - cash``) is its
    # own historical definition carried by identity: it is the same quantity on
    # both sides of the seam, so it is a leaf of the expansion, never expanded
    # into its two differently-written definitions.
    self_bridged = {
        item.id
        for item in items
        if (bridged := seeded_round_trip(item)) is not None
        and bridged[2] == {(item.id, 0): 1}
    }
    additive_in_both = {
        item.id
        for item in items
        if item.id not in self_bridged
        and aggregation(item.id, False) is not None
        and aggregation(item.id, True) is not None
    }

    findings: list[ProjectedFinancialIntegrityFinding] = []
    discontinuous: dict[
        str, tuple[dict[tuple[str, int], int], dict[tuple[str, int], int]]
    ] = {}
    for item in items:
        if item.id not in additive_in_both:
            continue
        historical_map = expanded(item.id, False)
        projected_map = expanded(item.id, True)
        if historical_map == projected_map:
            continue
        discontinuous[item.id] = (historical_map, projected_map)
    for item_id, (historical_map, projected_map) in discontinuous.items():
        direct = {
            operand_id
            for projected in (False, True)
            for operand_id, offset in aggregation(item_id, projected) or {}
            if offset == 0
        }
        if direct & discontinuous.keys():
            continue
        findings.append(
            _row_definition_finding(
                item_id,
                h,
                p,
                f"rendered {item_id!r} aggregates different operands across the "
                f"forecast seam after symmetric expansion: FY{h} "
                f"{_render_signed_map(historical_map)}; FY{p} "
                f"{_render_signed_map(projected_map)}",
                historical_map,
                projected_map,
            )
        )

    for item in items:
        bridged = seeded_round_trip(item)
        if bridged is None:
            continue
        projected_map, seeded, round_trip = bridged
        identity = {(item.id, 0): 1}
        if round_trip == identity:
            continue
        findings.append(
            _row_definition_finding(
                item.id,
                h,
                p,
                f"rendered {item.id!r} is projected in FY{p} as "
                f"{_render_signed_map(projected_map)} from control(s) seeded in FY{h} "
                + "; ".join(
                    f"{control_id} = {_render_signed_map(control_map)}"
                    for control_id, control_map in sorted(seeded.items())
                )
                + f", so it returns as {_render_signed_map(round_trip)}, not itself",
                identity,
                round_trip,
            )
        )
    return ProjectedFinancialIntegrityResult(required_item_ids=(), findings=tuple(findings))


def _row_definition_finding(
    item_id: str,
    h: int,
    p: int,
    detail: str,
    historical_map: dict[tuple[str, int], int],
    projected_map: dict[tuple[str, int], int],
) -> ProjectedFinancialIntegrityFinding:
    differing = {
        operand_id
        for operand_id, offset in historical_map.keys() | projected_map.keys()
        if historical_map.get((operand_id, offset), 0)
        != projected_map.get((operand_id, offset), 0)
    }
    return ProjectedFinancialIntegrityFinding(
        code="row_definition_discontinuity",
        repair_owner="document",
        detail=detail,
        item_id=item_id,
        periods=(h, p),
        related_item_ids=tuple(sorted(differing)),
    )


def _render_signed_map(signs: dict[tuple[str, int], int]) -> str:
    terms = [
        ("+" if sign > 0 else "-")
        + (f"{abs(sign)}*" if abs(sign) != 1 else "")
        + operand_id
        + (f"[t{offset:+d}]" if offset else "")
        for (operand_id, offset), sign in signs.items()
    ]
    return "{" + ", ".join(terms) + "}" if terms else "{}"


def signed_operand_map(spec: FormulaSpec) -> dict[tuple[str, int], int] | None:
    """Return an additive spec's operands as ``{(item_id, t): net sign}``.

    Additive shapes: a ``ref`` (``negate`` flips the sign); an ``arithmetic``
    ``expr`` tree of ``+``/``SUM``/``-``/``NEG`` nodes over refs; ``SUM`` over
    ``items``; an ``operands`` list headed ``"+"`` or ``"-"`` (the first operand
    less the rest; no operator reads as ``"+"``). Any other node (a literal,
    ``*``, another function, a ref ``adjustment``) and every other formula type
    is not additive and returns ``None``. A ref at another time offset is a
    distinct key; operands whose signs net to zero are dropped.
    """

    signs: dict[tuple[str, int], int] = {}

    def add(node: Any, sign: int) -> bool:
        ref = line_item_ref_from_obj(node)
        if ref is not None:
            key = (ref.id, int(ref.t))
            signs[key] = signs.get(key, 0) + sign
            return True
        if not isinstance(node, dict):
            return False
        op = node.get("op")
        if op in {"+", "SUM"}:
            args = node.get("args")
            return isinstance(args, list) and all(add(arg, sign) for arg in args)
        if op == "-":
            return add(node.get("left"), sign) and add(node.get("right"), -sign)
        if op == "NEG":
            return add(node.get("arg"), -sign)
        return False

    params = spec.params
    if spec.type is FormulaType.ref:
        if params.get("adjustment"):
            return None
        additive = add(params.get("source"), -1 if params.get("negate") else 1)
    elif spec.type is FormulaType.arithmetic:
        operands = params.get("operands")
        if "expr" in params:
            additive = add(params.get("expr"), 1)
        elif "function" in params:
            items = params.get("items")
            additive = (
                str(params.get("function")).upper() == "SUM"
                and isinstance(items, list)
                and all(add(member, 1) for member in items)
            )
        elif isinstance(operands, list) and operands:
            operator = operands[0] if isinstance(operands[0], str) else "+"
            members = operands[1:] if isinstance(operands[0], str) else operands
            additive = operator in {"+", "-"} and all(
                add(member, -1 if operator == "-" and index else 1)
                for index, member in enumerate(members)
            )
        else:
            additive = False
    else:
        additive = False
    if not additive:
        return None
    return {key: sign for key, sign in signs.items() if sign}


def _rendered_historical_spec(item: LineItem, period: int) -> FormulaSpec | None:
    """Return the spec a historical cell renders (an override wins)."""

    return _rendered_lane_spec(item, period, item.historical)


def _rendered_projected_spec(item: LineItem, period: int) -> FormulaSpec | None:
    """Return the spec a projected cell renders (an override wins)."""

    return _rendered_lane_spec(item, period, item.projected)


def _rendered_lane_spec(
    item: LineItem,
    period: int,
    lane_spec: FormulaSpec | None,
) -> FormulaSpec | None:
    override = (item.overrides or {}).get(int(period))
    if override is not None:
        return override
    if item.formula_periods is not None and int(period) not in {
        int(active_period) for active_period in item.formula_periods
    }:
        return None
    return lane_spec


def _t0_ref_item_ids(value: Any) -> set[str]:
    ids: set[str] = set()
    pending: list[Any] = [value]
    while pending:
        node = pending.pop()
        ref = line_item_ref_from_obj(node)
        if ref is not None:
            if int(ref.t) == 0:
                ids.add(ref.id)
            continue
        if isinstance(node, dict):
            pending.extend(node.values())
        elif isinstance(node, (list, tuple)):
            pending.extend(node)
    return ids


__all__ = [
    "BALANCE_SHEET_ITEM_IDS",
    "CASH_FLOW_FX_ITEM_ID",
    "CASH_FLOW_ITEM_IDS",
    "MIN_REVENUE_CONTINUITY_RATIO",
    "ProjectedFinancialIntegrityFinding",
    "ProjectedFinancialIntegrityIssueCode",
    "ProjectedFinancialIntegrityResult",
    "TOTAL_REVENUE_ITEM_ID",
    "evaluate_projected_financial_integrity",
    "evaluate_rendered_historical_statement_integrity",
    "evaluate_rendered_projected_statement_integrity",
    "evaluate_row_definition_continuity",
    "evaluate_statement_total_operand_placement",
    "signed_operand_map",
]
