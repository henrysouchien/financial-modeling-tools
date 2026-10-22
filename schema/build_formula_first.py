"""Formula-first reconciliation helpers for schema build orchestration.

``apply_formula_first`` drops a constant override wherever the item's formula
reproduces it. A pinned EDGAR filing subtotal its children do not reproduce
keeps the filed value and is reported as the
``pinned_filing_subtotal_reconciliation_failed`` Finding; the compile goes on.
Catalog-optional, unreported statement SUM terms do not block reconciliation;
their rows stay missing while the present terms still have to match the pin.
"""

from __future__ import annotations

import logging

from .build_formula_eval import (
    _constant_override_value as _formula_constant_override_value,
    _evaluate_formula_simple as _formula_evaluate_formula_simple,
)
from .build_formula_refs import (
    _all_refs_same_period as _formula_all_refs_same_period,
    _extract_ref_ids as _formula_extract_ref_ids,
    _extract_single_ref as _formula_extract_single_ref,
)
from .build_findings import make_finding
from .build_model_items import _iter_items as _model_iter_items
from .build_real_data import (
    _item_has_direct_real_data as _real_item_has_direct_real_data,
    _item_has_real_data as _real_item_has_real_data,
)
from .finding_vocabulary import Finding
from .models import FinancialModel, FormulaSpec, FormulaType, LineItem
from .required_role_catalog import load_required_role_catalog


_SYNTHETIC_FAST_PATH_TYPES = frozenset({
    FormulaType.arithmetic,
    FormulaType.ref,
    FormulaType.ratio,
})
_REPORTED_TOTAL_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"
_FORMULA_FIRST_EXCLUDED_ITEM_IDS = frozenset({
    # Direct CF ending-cash actuals are statement-basis values. The historical
    # formula is a no-source fallback bridge from BS cash, not an equivalence
    # proof when restricted cash moves separately.
    "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period",
})


def _reconciliation_formula(item: LineItem, period: int) -> FormulaSpec | None:
    """The period's lowered expression, while its filed constant still wins."""
    override = (item.overrides or {}).get(int(period))
    if override is not None:
        saved = override.params.get("_not_separately_presented", {}).get("lowered_formula")
        if saved is not None:
            return FormulaSpec.model_validate_json(saved)
    return item.historical


def _is_pinned_filing_subtotal(item: LineItem, period: int) -> bool:
    if item.required_role is None or not item.required_role.startswith("statement."):
        return False
    if item.overrides is None:
        return False
    override = item.overrides.get(int(period))
    if override is None or override.vendor_provenance is None:
        return False
    return override.vendor_provenance.provider == "edgar"


def _pinned_filing_subtotal_finding(
    item: LineItem,
    period: int,
    computed: float | None,
    children: tuple[str, ...],
) -> Finding:
    """The filed subtotal its template children do not reproduce, as a Finding."""

    assert item.overrides is not None
    exhibited = _formula_constant_override_value(item.overrides[int(period)])
    delta = (
        None
        if computed is None or exhibited is None
        else round(float(exhibited) - float(computed), 6)
    )
    computed_text = "no value" if computed is None else f"{computed:g}"
    return make_finding(
        "pinned_filing_subtotal_reconciliation_failed",
        message=(
            f"{item.id} FY{int(period)}: children {list(children)} sum to "
            f"{computed_text}; the pinned filing value is {exhibited} "
            f"(filed minus children = {delta}). The filed value stays on the "
            "row. Repair the template composition or the statement binding, "
            "or decline."
        ),
        item_id=item.id,
        periods=(int(period),),
        operands=children,
        observed={
            "required_role": item.required_role,
            "computed": None if computed is None else round(float(computed), 6),
            "exhibited": exhibited,
            "delta": delta,
        },
    )


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


def _unreported_optional_sum_terms(
    model: FinancialModel,
    item: LineItem,
    period: int,
    formula: FormulaSpec | None,
) -> frozenset[str]:
    """Missing direct SUM members the statement catalog permits omitting."""

    if (
        formula is None
        or formula.type is not FormulaType.arithmetic
        or formula.params.get("function") != "SUM"
        or "expr" in formula.params
    ):
        return frozenset()
    catalog = load_required_role_catalog()
    # A catalog statement total's optional members, and an income-statement
    # SUM's optional income-statement roles (restructuring rolls into
    # operating expenses with no ``parent_total`` of its own).
    candidate_roles = (
        catalog.roles_rolling_into(item.id)
        if item.id in catalog.statement_totals()
        else ()
    )
    if item.id.startswith("tpl.fm.income_statement."):
        candidate_roles = (
            *candidate_roles,
            *(
                role
                for role in catalog.required_roles
                if role.statement == "income_statement"
            ),
        )
    optional_ids = {
        item_id
        for role in candidate_roles
        if role.optional_if_unreported
        for item_id in role.template_items
    }
    if not optional_ids:
        return frozenset()
    omitted = set()
    for term in formula.params.get("items", []) or []:
        ref = _formula_extract_single_ref({"source": term})
        if ref is None or ref[1] != 0 or ref[0] not in optional_ids:
            continue
        member = model._index.get(ref[0])
        if member is None or not _real_item_has_real_data(member, period, model=model):
            omitted.add(ref[0])
    return frozenset(omitted)


def _formula_first_node_periods(
    model: FinancialModel,
    historical_periods: list[int],
) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """Return removable override periods plus formula-only bridge periods."""

    model.build_index()
    periods = [int(period) for period in historical_periods]

    items_by_id = {item.id: item for item in _model_iter_items(model)}
    has_real_data = {
        item.id: {
            period
            for period in periods
            if _real_item_has_real_data(item, period, model=model)
        }
        for item in items_by_id.values()
    }
    removable_candidates: set[tuple[str, int]] = set()

    for item in items_by_id.values():
        if item.historical is None:
            continue
        if item.id in _FORMULA_FIRST_EXCLUDED_ITEM_IDS:
            continue

        active_periods = _active_formula_first_periods(item, periods)
        if not active_periods:
            continue

        if item.required_role:
            for period in active_periods:
                if item.overrides is None or int(period) not in item.overrides:
                    continue
                override = item.overrides[int(period)]
                if override.type is not FormulaType.constant:
                    continue
                formula = _reconciliation_formula(item, period)
                if formula is None:
                    continue
                if (
                    formula.type in _SYNTHETIC_FAST_PATH_TYPES
                    and not _formula_all_refs_same_period(formula.params)
                ):
                    continue
                is_constant_formula = formula.type is FormulaType.constant
                ref_ids = _formula_extract_ref_ids(formula.params)
                if not ref_ids and not is_constant_formula:
                    continue
                if (
                    item.id == _REPORTED_TOTAL_REVENUE_ITEM_ID
                    and _real_item_has_direct_real_data(item, int(period))
                ):
                    # Reported consolidated revenue is the independent top-line
                    # control. A segment formula can be close while omitting a
                    # small stream that is material downstream. Synthetic
                    # missing-data placeholders may still yield to the formula.
                    continue
                if is_constant_formula:
                    formula_value = _formula_constant_override_value(formula)
                    override_value = _formula_constant_override_value(override)
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
        if item is None or item.required_role or item.historical is None:
            return
        if int(period) not in _active_formula_first_periods(item, periods):
            return
        if (
            item.historical.type not in _SYNTHETIC_FAST_PATH_TYPES
            or not _formula_all_refs_same_period(item.historical.params)
            or _real_item_has_direct_real_data(item, period)
        ):
            return

        bridge_candidates.add(node)
        for ref_id in _formula_extract_ref_ids(item.historical.params):
            add_bridge_dependency(ref_id, int(period))

    for item_id, period in sorted(removable_candidates):
        item = items_by_id[item_id]
        if item.overrides is None or int(period) not in item.overrides:
            continue
        if item.overrides[int(period)].note != "synthetic":
            continue
        for ref_id in _formula_extract_ref_ids(
            item.historical.params if item.historical else None
        ):
            add_bridge_dependency(ref_id, int(period))

    removable: dict[str, set[int]] = {}
    bridges: dict[str, set[int]] = {}
    unresolved = removable_candidates | bridge_candidates

    while unresolved:
        newly_derivable: list[tuple[str, int]] = []
        for item_id, period in sorted(unresolved):
            item = items_by_id[item_id]
            formula = _reconciliation_formula(item, period)
            ref_ids = _formula_extract_ref_ids(
                formula.params if formula is not None else None
            )
            omitted = _unreported_optional_sum_terms(model, item, period, formula)
            if all(
                period in has_real_data.get(ref_id, set()) or ref_id in omitted
                for ref_id in ref_ids
            ):
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

    removable, _bridges = _formula_first_node_periods(model, historical_periods)
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
    if (
        spec.note == "synthetic"
        and item.historical is not None
        and item.historical.type in _SYNTHETIC_FAST_PATH_TYPES
        and _formula_all_refs_same_period(item.historical.params)
    ):
        # Synthetic markers are placeholders, not data. When the historical
        # formula is a real derivation over same-period refs and produced a
        # value, the formula should win. Cross-period refs fail closed because
        # derivability currently validates dependencies at the candidate period.
        return computed_value is not None

    override_value = _formula_constant_override_value(spec)
    if override_value is None:
        return False

    diff = abs(float(computed_value) - override_value)
    if abs(override_value) < 1e-6:
        return diff < 1e-6
    return diff / abs(override_value) <= tolerance


def apply_formula_first(
    model: FinancialModel,
    historical_periods: list[int],
    *,
    findings: list[Finding] | None = None,
) -> dict[str, set[int]]:
    """Remove reconciled constant overrides from derivable formula-backed items.

    A pinned EDGAR filing subtotal whose children do not reproduce it keeps
    its override and appends one ``pinned_filing_subtotal_reconciliation_failed``
    Finding per period to ``findings``.
    """

    model.build_index()
    candidate_periods, bridge_periods = _formula_first_node_periods(
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

    graph_periods: dict[str, set[int]] = {}
    for item_id, periods in candidate_periods.items():
        graph_periods.setdefault(item_id, set()).update({int(period) for period in periods})
    for item_id, periods in bridge_periods.items():
        graph_periods.setdefault(item_id, set()).update({int(period) for period in periods})

    for item_id, periods in graph_periods.items():
        item = model.get_item(item_id)
        # Lowering is period-local; each candidate has its own operand set.
        for period in periods:
            formula = _reconciliation_formula(item, period)
            ref_ids = _formula_extract_ref_ids(
                formula.params if formula is not None else None
            )
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
            logging.warning(
                "Formula-first reconciliation skipped for %s period=%s due to unresolved derivable deps",
                node[0],
                node[1],
            )
            reject_with_downstream(node)
            continue

        item_id, period = node
        item = model.get_item(item_id)
        formula = _reconciliation_formula(item, period)
        omitted = _unreported_optional_sum_terms(model, item, period, formula)
        # These are local SUM identities, not numeric values on the missing
        # rows. Keep any already-computed optional member in the reconciliation.
        evaluation_values = computed_values
        if omitted:
            evaluation_values = {
                **dict.fromkeys(((ref_id, period) for ref_id in omitted), 0.0),
                **computed_values,
            }
        computed_value = _formula_evaluate_formula_simple(
            model,
            item if formula is item.historical else item.model_copy(update={"historical": formula}),
            period,
            evaluation_values,
        )
        if node in bridge_nodes:
            if computed_value is None:
                logging.warning(
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

        if computed_value is None or not _reconcile_override(
            item,
            period,
            computed_value,
        ):
            if findings is not None and _is_pinned_filing_subtotal(item, period):
                findings.append(
                    _pinned_filing_subtotal_finding(
                        item,
                        period,
                        computed_value,
                        tuple(
                            sorted(
                                _formula_extract_ref_ids(
                                    formula.params if formula is not None else None
                                )
                            )
                        ),
                    )
                )
            logging.warning(
                "Formula-first reconciliation failed for %s period=%s: computed=%s override=%s",
                item_id,
                period,
                computed_value,
                override_value,
            )
            reject_with_downstream(node)
            continue

        computed_values[node] = computed_value
        if omitted:
            # The filed total reconciles, but the retained SUM reads each
            # omitted member's empty cell as Missing; the pin stays the value.
            continue
        validated.setdefault(item_id, set()).add(int(period))

    for item_id, periods in validated.items():
        item = model.get_item(item_id)
        if item.overrides is None:
            continue
        for period in periods:
            override = item.overrides[int(period)]
            formula = _reconciliation_formula(item, period)
            metadata = override.params.get("_not_separately_presented", {})
            if metadata.get("lowered_formula") is not None and formula is not None:
                item.overrides[int(period)] = formula.model_copy(
                    update={"params": {
                        **formula.params,
                        "_not_separately_presented": {
                            key: value for key, value in metadata.items()
                            if key != "lowered_formula"
                        },
                    }},
                    deep=True,
                )
            else:
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
