"""Compile statement-window row, population, and formula plans off-product-path.

The plan builder reports instead of refusing. A window whose plan cannot be
built is not applied — none of its mutations reach ``prepared_model`` — and is
one :class:`StatementWindowPlanIssue` on the plan, which reads it as a Finding
(``StatementWindowBuildPlan.findings``).
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
import hashlib
import json
from types import MappingProxyType
from typing import Any, Literal, Mapping, NoReturn, TypeGuard

from .build_findings import make_finding
from .build_semantic_rows import SemanticRowsResult, _materialize_semantic_rows
from .finding_vocabulary import FINDING_SEVERITY_MAP, Finding
from .models import (
    FinancialModel,
    FinancialStatement,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
)
from .overrides import TickerOverrides
from .multi_basis_window import ExistingConceptTarget, InsertTarget
from .statement_windows import (
    CompiledStatementWindows,
    CompiledWindowPlan,
    MultiBasisWindowIncompleteError,
)


PopulationDisposition = Literal[
    "populate",
    "disabled_by_authoritative_basis",
    "diagnostic_only",
]
FormulaGraphKind = Literal["historical", "projected"]


@dataclass(frozen=True)
class StatementWindowPlanIssue:
    """Why one window's build plan was not applied, or one role was dropped."""

    issue_code: str
    detail: str
    window_id: str
    item_id: str | None = None
    period: int | None = None

    def finding(self) -> Finding:
        """The issue as its map row, else ``statement_mapping_build_plan_invalid``."""

        return make_finding(
            (
                self.issue_code
                if self.issue_code in FINDING_SEVERITY_MAP
                else "statement_mapping_build_plan_invalid"
            ),
            message=self.detail,
            item_id=self.item_id or self.window_id,
            periods=() if self.period is None else (self.period,),
            observed={"issue_code": self.issue_code, "window_id": self.window_id},
        )


class _WindowPlanUnwind(Exception):
    """Unwinds one window's planning; caught only at this module's per-window boundary."""

    def __init__(self, issue: StatementWindowPlanIssue) -> None:
        super().__init__(issue.detail)
        self.issue = issue


@dataclass(frozen=True)
class PopulationDispositionEntry:
    window_id: str
    required_role: str
    item_id: str
    disposition: PopulationDisposition
    reason: str


@dataclass(frozen=True)
class FormulaMembershipPath:
    required_role: str
    item_id: str
    item_path: tuple[str, ...]
    cumulative_sign: float = 1.0


@dataclass(frozen=True)
class FormulaMembershipPeriodPlan:
    window_id: str
    graph_kind: FormulaGraphKind
    period: int
    total_required_role: str
    total_item_id: str
    selected_paths: tuple[FormulaMembershipPath, ...]
    active_member_item_ids: tuple[str, ...]
    suppressed_member_item_ids: tuple[str, ...]
    constant_overrides: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class FormulaSuppression:
    window_id: str
    parent_item_id: str
    suppressed_item_id: str
    graph_scope: str


@dataclass(frozen=True)
class _FormulaEdgeDecision:
    spec: FormulaSpec
    parent_item_id: str
    child_item_id: str
    graph_scope: str
    remove: bool


@dataclass(frozen=True)
class StatementWindowBuildPlan:
    prepared_model: FinancialModel
    item_id_by_role: Mapping[str, str]
    population_dispositions: tuple[PopulationDispositionEntry, ...]
    formula_membership: tuple[FormulaMembershipPeriodPlan, ...]
    formula_suppressions: tuple[FormulaSuppression, ...]
    materialization: Mapping[str, Any]
    fingerprint: str
    #: The windows this plan applied, sorted. A window absent here is not
    #: applied (its issue says why) and validation does not read it.
    applied_window_ids: tuple[str, ...]
    issues: tuple[StatementWindowPlanIssue, ...]

    @property
    def findings(self) -> tuple[Finding, ...]:
        """One Finding per issue: an ``error`` that blocks publish."""

        return tuple(issue.finding() for issue in self.issues)

    @property
    def suppressed_member_item_ids(self) -> frozenset[str]:
        """Every row an applied window took out of a formula graph.

        The single owner of the window's suppression set: a consumer that must
        not resurrect a suppressed row (the schedule policies' zero-operand
        reconciler) reads it here, never a hand-written list.
        """

        return frozenset(
            {
                entry.suppressed_item_id for entry in self.formula_suppressions
            }
            | {
                item_id
                for plan in self.formula_membership
                for item_id in plan.suppressed_member_item_ids
            }
        )

    @property
    def population_disposition_by_role(
        self,
    ) -> Mapping[str, PopulationDisposition]:
        """Return the immutable per-role disposition consumed by population."""

        return MappingProxyType(
            {
                entry.required_role: entry.disposition
                for entry in self.population_dispositions
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id_by_role": dict(sorted(self.item_id_by_role.items())),
            "population_dispositions": [
                asdict(entry) for entry in self.population_dispositions
            ],
            "formula_membership": [
                {
                    **asdict(plan),
                    "selected_paths": [
                        asdict(path) for path in plan.selected_paths
                    ],
                }
                for plan in self.formula_membership
            ],
            "formula_suppressions": [
                asdict(entry) for entry in self.formula_suppressions
            ],
            "materialization": copy.deepcopy(dict(self.materialization)),
            "fingerprint": self.fingerprint,
            "applied_window_ids": list(self.applied_window_ids),
            "issues": [asdict(issue) for issue in self.issues],
        }


def _raise_plan_error(
    issue_code: str,
    detail: str,
    *,
    window_id: str,
    item_id: str | None = None,
    period: int | None = None,
) -> NoReturn:
    raise _WindowPlanUnwind(
        StatementWindowPlanIssue(
            issue_code=issue_code,
            detail=detail,
            window_id=window_id,
            item_id=item_id,
            period=period,
        )
    )


def _item_section_roles(
    item: LineItem,
    *,
    statement: FinancialStatement,
    section: str,
) -> set[str]:
    return {
        role.role
        for role in (item.statement_section_roles or [])
        if role.statement == statement and role.section == section
    }


def _section_items(
    model: FinancialModel,
    plan: CompiledWindowPlan,
) -> list[LineItem]:
    return [
        item
        for item in model._index.values()
        if _item_section_roles(
            item,
            statement=plan.statement,
            section=plan.section,
        )
    ]


def _resolve_one_item(
    candidates: list[LineItem],
    *,
    issue_code: str,
    description: str,
    window_id: str,
) -> LineItem:
    if len(candidates) != 1:
        _raise_plan_error(
            issue_code,
            f"{description} resolved {len(candidates)} model rows; expected exactly one",
            window_id=window_id,
        )
    return candidates[0]


def _resolve_mapping_items(
    model: FinancialModel,
    plan: CompiledWindowPlan,
) -> tuple[LineItem, dict[str, LineItem], list[LineItem]]:
    window = plan.window
    section_items = _section_items(model, plan)
    total = _resolve_one_item(
        [
            item
            for item in section_items
            if item.required_role == plan.section_total_required_role
            and "section_total"
            in _item_section_roles(
                item,
                statement=plan.statement,
                section=plan.section,
            )
        ],
        issue_code="section_total_target_ambiguous",
        description=f"section total role {plan.section_total_required_role!r}",
        window_id=plan.window_id,
    )

    selected: dict[str, LineItem] = {}
    for required_role in sorted(window.bound_roles):
        binding = window.binding_for(required_role)
        assert binding is not None
        if isinstance(binding.target, InsertTarget):
            candidates = [
                item
                for item in section_items
                if item.id == binding.target.stable_item_id
                and item.required_role == required_role
            ]
        elif isinstance(binding.target, ExistingConceptTarget):
            candidates = [
                item
                for item in section_items
                if item.required_role == required_role
            ]
        else:  # pragma: no cover - Pydantic discriminator is exhaustive
            raise TypeError(binding.target)
        selected[required_role] = _resolve_one_item(
            candidates,
            issue_code="selected_row_target_ambiguous",
            description=f"selected role {required_role!r}",
            window_id=plan.window_id,
        )
    if len({item.id for item in selected.values()}) != len(selected):
        _raise_plan_error(
            "selected_row_target_ambiguous",
            "multiple selected roles resolve to the same model row",
            window_id=plan.window_id,
        )
    return total, selected, section_items


def _is_line_item_ref_list(value: object) -> TypeGuard[list[LineItemRef]]:
    return isinstance(value, list) and all(
        isinstance(item, LineItemRef) for item in value
    )


def _formula_ref_sequence(
    spec: FormulaSpec,
    *,
    window_id: str,
    item_id: str,
    period: int | None = None,
) -> list[LineItemRef]:
    if spec.type is FormulaType.arithmetic and spec.subtype == "sum_range":
        if set(spec.params) != {"function", "items"}:
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                "sum_range requires exactly params.function and params.items",
                window_id=window_id,
                item_id=item_id,
                period=period,
            )
        if spec.params.get("function") != "SUM":
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                "sum_range requires params.function='SUM'",
                window_id=window_id,
                item_id=item_id,
                period=period,
            )
        refs = spec.params.get("items")
    elif spec.type is FormulaType.arithmetic and spec.subtype == "multi_term":
        if set(spec.params) != {"operands"}:
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                "multi_term requires exactly params.operands",
                window_id=window_id,
                item_id=item_id,
                period=period,
            )
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                "multi_term requires a leading '+' and additive references",
                window_id=window_id,
                item_id=item_id,
                period=period,
            )
        refs = operands[1:]
    elif spec.type is FormulaType.ref:
        source = spec.params.get("source")
        refs = [source]
        if set(spec.params) - {"source"}:
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                "simple ref membership cannot include adjustments or negation",
                window_id=window_id,
                item_id=item_id,
                period=period,
            )
    else:
        _raise_plan_error(
            "unsupported_formula_membership_ast",
            f"formula type={spec.type.value!r} subtype={spec.subtype!r} is not additive v1",
            window_id=window_id,
            item_id=item_id,
            period=period,
        )
    if not isinstance(refs, list) or not refs:
        _raise_plan_error(
            "unsupported_formula_membership_ast",
            "additive formula must contain at least one LineItemRef",
            window_id=window_id,
            item_id=item_id,
            period=period,
        )
    if not _is_line_item_ref_list(refs):
        _raise_plan_error(
            "unsupported_formula_membership_ast",
            "additive formula members must be resolved LineItemRef objects",
            window_id=window_id,
            item_id=item_id,
            period=period,
        )
    for ref in refs:
        if not ref.resolved or int(ref.t) != 0:
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                "formula membership references must be resolved in the active period",
                window_id=window_id,
                item_id=item_id,
                period=period,
            )
    return refs


def _formula_specs_for_mutation(
    item: LineItem,
    *,
    historical_periods: tuple[int, ...],
    projection_periods: tuple[int, ...],
) -> list[tuple[str, FormulaSpec]]:
    specs: list[tuple[str, FormulaSpec]] = []
    if historical_periods and item.historical is not None:
        specs.append(("historical", item.historical))
    if projection_periods and item.projected is not None:
        specs.append(("projected", item.projected))
    active_periods = {*historical_periods, *projection_periods}
    for period, spec in sorted((item.overrides or {}).items()):
        if int(period) in active_periods and spec.type is not FormulaType.constant:
            specs.append((f"override:{int(period)}", spec))
    return specs


def _insert_ref_at(spec: FormulaSpec, member_index: int, ref: LineItemRef) -> None:
    if spec.type is FormulaType.arithmetic and spec.subtype == "sum_range":
        spec.params["items"].insert(member_index, ref)
        return
    if spec.type is FormulaType.arithmetic and spec.subtype == "multi_term":
        spec.params["operands"].insert(member_index + 1, ref)
        return
    raise TypeError("simple-ref formulas do not support additive insertion")


def _remove_ref(spec: FormulaSpec, ref_id: str) -> None:
    if spec.type is FormulaType.arithmetic and spec.subtype == "sum_range":
        members = spec.params["items"]
    elif spec.type is FormulaType.arithmetic and spec.subtype == "multi_term":
        members = spec.params["operands"]
    else:
        raise TypeError("simple-ref formulas do not support additive suppression")
    for index, value in enumerate(members):
        if isinstance(value, LineItemRef) and value.id == ref_id:
            members.pop(index)
            return
    raise KeyError(ref_id)


def _insert_external_formula_member(
    model: FinancialModel,
    *,
    plan: CompiledWindowPlan,
    target: InsertTarget,
    historical_periods: tuple[int, ...],
    projection_periods: tuple[int, ...],
) -> None:
    try:
        parent = model.get_item(target.formula_membership.parent_item_id)
    except KeyError:
        _raise_plan_error(
            "formula_parent_missing",
            f"formula parent {target.formula_membership.parent_item_id!r} is absent",
            window_id=plan.window_id,
        )
    if not _item_section_roles(
        parent,
        statement=plan.statement,
        section=plan.section,
    ):
        _raise_plan_error(
            "formula_parent_outside_section",
            "inserted row formula parent is outside the mapped statement section",
            window_id=plan.window_id,
            item_id=parent.id,
        )

    saw_formula = False
    for scope, spec in _formula_specs_for_mutation(
        parent,
        historical_periods=historical_periods,
        projection_periods=projection_periods,
    ):
        saw_formula = True
        refs = _formula_ref_sequence(
            spec,
            window_id=plan.window_id,
            item_id=parent.id,
        )
        new_matches = [
            index for index, ref in enumerate(refs) if ref.id == target.stable_item_id
        ]
        if len(new_matches) > 1:
            _raise_plan_error(
                "selected_row_duplicated_in_formula",
                f"inserted row appears more than once in {scope} formula",
                window_id=plan.window_id,
                item_id=parent.id,
            )
        if new_matches:
            continue
        if spec.type is FormulaType.ref:
            _raise_plan_error(
                "unsupported_formula_membership_ast",
                f"cannot insert an additive member into the {scope} simple-ref formula",
                window_id=plan.window_id,
                item_id=parent.id,
            )
        anchor_matches = [
            index
            for index, ref in enumerate(refs)
            if ref.id == target.formula_membership.insert_after_item_id
        ]
        if len(anchor_matches) != 1:
            _raise_plan_error(
                "formula_insertion_anchor_ambiguous",
                f"formula anchor resolved {len(anchor_matches)} times in {scope}",
                window_id=plan.window_id,
                item_id=parent.id,
            )
        _insert_ref_at(
            spec,
            anchor_matches[0] + 1,
            LineItemRef(id=target.stable_item_id),
        )
    if not saw_formula:
        _raise_plan_error(
            "formula_parent_missing",
            "inserted row formula parent has no historical or projected formula",
            window_id=plan.window_id,
            item_id=parent.id,
        )


def _suppress_formula_members(
    model: FinancialModel,
    *,
    plan: CompiledWindowPlan,
    total: LineItem,
    selected_item_ids: set[str],
    suppressed_item_ids: set[str],
    historical_periods: tuple[int, ...],
    projection_periods: tuple[int, ...],
) -> list[FormulaSuppression]:
    leaf_roles = {"member", "catch_all", "presentation_only"}

    def merge_decisions(
        target: dict[tuple[int, str], _FormulaEdgeDecision],
        incoming: Mapping[tuple[int, str], _FormulaEdgeDecision],
        *,
        period: int,
    ) -> None:
        for key, decision in incoming.items():
            existing = target.get(key)
            if existing is not None and existing.remove != decision.remove:
                _raise_plan_error(
                    "period_dependent_formula_suppression",
                    f"shared formula edge to {decision.child_item_id!r} requires "
                    "different suppression across active periods",
                    window_id=plan.window_id,
                    item_id=decision.parent_item_id,
                    period=period,
                )
            target.setdefault(key, decision)

    def analyze_branch(
        item: LineItem,
        *,
        graph_kind: FormulaGraphKind,
        period: int,
        active: set[str],
    ) -> tuple[bool, dict[tuple[int, str], _FormulaEdgeDecision]]:
        if item.id in active:
            _raise_plan_error(
                "formula_membership_cycle",
                "formula graph contains a cycle",
                window_id=plan.window_id,
                item_id=item.id,
                period=period,
            )
        spec = _active_formula_spec(
            item,
            graph_kind=graph_kind,
            period=period,
        )
        if spec is None:
            _raise_plan_error(
                "selected_row_not_in_formula",
                "formula path terminates at a non-member subtotal",
                window_id=plan.window_id,
                item_id=item.id,
                period=period,
            )
        refs = _formula_ref_sequence(
            spec,
            window_id=plan.window_id,
            item_id=item.id,
            period=period,
        )
        ref_counts = {
            ref.id: sum(candidate.id == ref.id for candidate in refs)
            for ref in refs
        }
        override = (item.overrides or {}).get(int(period))
        graph_scope = (
            f"override:{int(period)}"
            if override is spec and spec.type is not FormulaType.constant
            else graph_kind
        )
        decisions: dict[tuple[int, str], _FormulaEdgeDecision] = {}
        has_selected_descendant = False

        for ref in refs:
            try:
                child = model.get_item(ref.id)
            except KeyError:
                _raise_plan_error(
                    "unsupported_formula_membership_ast",
                    f"formula reference {ref.id!r} is unresolved",
                    window_id=plan.window_id,
                    item_id=item.id,
                    period=period,
                )
            child_roles = _item_section_roles(
                child,
                statement=plan.statement,
                section=plan.section,
            )
            child_decisions: dict[
                tuple[int, str], _FormulaEdgeDecision
            ] = {}
            if child.id in selected_item_ids:
                keep_child = True
            else:
                if not child_roles:
                    _raise_plan_error(
                        "formula_member_outside_section",
                        f"formula member {child.id!r} is outside the mapped section",
                        window_id=plan.window_id,
                        item_id=item.id,
                        period=period,
                    )
                if child_roles & leaf_roles:
                    if child.id not in suppressed_item_ids:
                        _raise_plan_error(
                            "unlisted_formula_member_active",
                            f"unselected leaf {child.id!r} lacks a suppression disposition",
                            window_id=plan.window_id,
                            item_id=item.id,
                            period=period,
                        )
                    keep_child = False
                else:
                    keep_child, child_decisions = analyze_branch(
                        child,
                        graph_kind=graph_kind,
                        period=period,
                        active=active | {item.id},
                    )

            remove_child = not keep_child
            if remove_child and ref_counts[child.id] != 1:
                _raise_plan_error(
                    "formula_member_duplicated",
                    f"unlisted member {child.id!r} appears multiple times",
                    window_id=plan.window_id,
                    item_id=item.id,
                    period=period,
                )
            edge_key = (id(spec), child.id)
            merge_decisions(
                decisions,
                {
                    edge_key: _FormulaEdgeDecision(
                        spec=spec,
                        parent_item_id=item.id,
                        child_item_id=child.id,
                        graph_scope=graph_scope,
                        remove=remove_child,
                    )
                },
                period=period,
            )
            if keep_child:
                has_selected_descendant = True
                merge_decisions(decisions, child_decisions, period=period)

        return has_selected_descendant, decisions

    all_decisions: dict[tuple[int, str], _FormulaEdgeDecision] = {}
    graph_periods: tuple[
        tuple[FormulaGraphKind, tuple[int, ...]], ...
    ] = (
        ("historical", historical_periods),
        ("projected", projection_periods),
    )
    for graph_kind, periods in graph_periods:
        for period in periods:
            _has_selected, period_decisions = analyze_branch(
                total,
                graph_kind=graph_kind,
                period=period,
                active=set(),
            )
            merge_decisions(all_decisions, period_decisions, period=period)

    suppressions: list[FormulaSuppression] = []
    removals = sorted(
        (decision for decision in all_decisions.values() if decision.remove),
        key=lambda decision: (
            decision.parent_item_id,
            decision.child_item_id,
            decision.graph_scope,
        ),
    )
    for decision in removals:
        if decision.spec.type is FormulaType.ref:
            _raise_plan_error(
                "unsupported_formula_member_suppression",
                "cannot remove the sole source of a simple-ref formula",
                window_id=plan.window_id,
                item_id=decision.parent_item_id,
            )
        _remove_ref(decision.spec, decision.child_item_id)
        suppressions.append(
            FormulaSuppression(
                window_id=plan.window_id,
                parent_item_id=decision.parent_item_id,
                suppressed_item_id=decision.child_item_id,
                graph_scope=decision.graph_scope,
            )
        )
    return suppressions


def _constant_override(item: LineItem, period: int) -> dict[str, Any] | None:
    spec = (item.overrides or {}).get(int(period))
    if spec is None or spec.type is not FormulaType.constant:
        return None
    value = spec.params.get("value")
    return {
        "item_id": item.id,
        "period": int(period),
        "value": value,
    }


def _active_formula_spec(
    item: LineItem,
    *,
    graph_kind: FormulaGraphKind,
    period: int,
) -> FormulaSpec | None:
    override = (item.overrides or {}).get(int(period))
    if override is not None and override.type is not FormulaType.constant:
        return override
    if item.formula_periods is not None and int(period) not in {
        int(active_period) for active_period in item.formula_periods
    }:
        return None
    return item.historical if graph_kind == "historical" else item.projected


def _compile_period_membership(
    model: FinancialModel,
    *,
    plan: CompiledWindowPlan,
    graph_kind: FormulaGraphKind,
    period: int,
    total: LineItem,
    selected: dict[str, LineItem],
    suppressed_item_ids: set[str],
) -> FormulaMembershipPeriodPlan:
    selected_item_ids = {item.id for item in selected.values()}
    paths_by_item: dict[str, list[tuple[str, ...]]] = {}
    active_leaf_ids: set[str] = set()
    constant_overrides: dict[str, dict[str, Any]] = {}

    for item in [total, *selected.values()]:
        constant = _constant_override(item, period)
        if constant is not None:
            constant_overrides[item.id] = constant

    def visit(item: LineItem, path: tuple[str, ...], active: set[str]) -> None:
        if item.id in active:
            _raise_plan_error(
                "formula_membership_cycle",
                "formula graph contains a cycle",
                window_id=plan.window_id,
                item_id=item.id,
                period=period,
            )
        spec = _active_formula_spec(
            item,
            graph_kind=graph_kind,
            period=period,
        )
        if spec is None:
            _raise_plan_error(
                "selected_row_not_in_formula",
                "formula path terminates at a non-member subtotal",
                window_id=plan.window_id,
                item_id=item.id,
                period=period,
            )
        constant = _constant_override(item, period)
        if constant is not None:
            constant_overrides[item.id] = constant
        refs = _formula_ref_sequence(
            spec,
            window_id=plan.window_id,
            item_id=item.id,
            period=period,
        )
        for ref in refs:
            try:
                child = model.get_item(ref.id)
            except KeyError:
                _raise_plan_error(
                    "unsupported_formula_membership_ast",
                    f"formula reference {ref.id!r} is unresolved",
                    window_id=plan.window_id,
                    item_id=item.id,
                    period=period,
                )
            next_path = (*path, child.id)
            child_roles = _item_section_roles(
                child,
                statement=plan.statement,
                section=plan.section,
            )
            is_leaf = (
                child.id in selected_item_ids
                or bool(child_roles & {"member", "catch_all", "presentation_only"})
            )
            if is_leaf:
                if not child_roles and child.id not in selected_item_ids:
                    _raise_plan_error(
                        "formula_member_outside_section",
                        f"formula member {child.id!r} is outside the mapped section",
                        window_id=plan.window_id,
                        item_id=item.id,
                        period=period,
                    )
                active_leaf_ids.add(child.id)
                paths_by_item.setdefault(child.id, []).append(next_path)
                continue
            visit(child, next_path, active | {item.id})

    visit(total, (total.id,), set())

    selected_paths: list[FormulaMembershipPath] = []
    for required_role, item in sorted(selected.items()):
        paths = paths_by_item.get(item.id, [])
        if not paths:
            _raise_plan_error(
                "selected_row_not_in_formula",
                f"selected role {required_role!r} has no active additive path",
                window_id=plan.window_id,
                item_id=item.id,
                period=period,
            )
        if len(paths) != 1:
            _raise_plan_error(
                "selected_row_duplicated_in_formula",
                f"selected role {required_role!r} has {len(paths)} active paths",
                window_id=plan.window_id,
                item_id=item.id,
                period=period,
            )
        selected_paths.append(
            FormulaMembershipPath(
                required_role=required_role,
                item_id=item.id,
                item_path=paths[0],
            )
        )

    leaked = sorted((active_leaf_ids - selected_item_ids) & suppressed_item_ids)
    if leaked:
        _raise_plan_error(
            "suppressed_formula_member_active",
            f"suppressed members remain active: {leaked}",
            window_id=plan.window_id,
            period=period,
        )
    unreviewed = sorted(active_leaf_ids - selected_item_ids)
    if unreviewed:
        _raise_plan_error(
            "unlisted_formula_member_active",
            f"unlisted members remain in authoritative formula graph: {unreviewed}",
            window_id=plan.window_id,
            period=period,
        )

    return FormulaMembershipPeriodPlan(
        window_id=plan.window_id,
        graph_kind=graph_kind,
        period=int(period),
        total_required_role=plan.section_total_required_role,
        total_item_id=total.id,
        selected_paths=tuple(selected_paths),
        active_member_item_ids=tuple(sorted(active_leaf_ids)),
        suppressed_member_item_ids=tuple(sorted(suppressed_item_ids)),
        constant_overrides=tuple(
            constant_overrides[item_id]
            for item_id in sorted(constant_overrides)
        ),
    )


def _materialization_payload(result: SemanticRowsResult) -> dict[str, Any]:
    return {
        "materialized": copy.deepcopy(result.materialized),
        "linkages": copy.deepcopy(result.linkages),
        "collisions": copy.deepcopy(result.collisions),
        "gaps": copy.deepcopy(result.gaps),
    }


def _plan_fingerprint(
    *,
    ticker: str,
    item_id_by_role: Mapping[str, str],
    dispositions: tuple[PopulationDispositionEntry, ...],
    membership: tuple[FormulaMembershipPeriodPlan, ...],
    suppressions: tuple[FormulaSuppression, ...],
    materialization: Mapping[str, Any],
) -> str:
    payload = {
        "version": 1,
        "ticker": str(ticker).strip().upper(),
        "item_id_by_role": dict(sorted(item_id_by_role.items())),
        "population_dispositions": [asdict(entry) for entry in dispositions],
        "formula_membership": [asdict(entry) for entry in membership],
        "formula_suppressions": [asdict(entry) for entry in suppressions],
        "materialization": copy.deepcopy(dict(materialization)),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class _WindowPlan:
    item_id_by_role: dict[str, str]
    dispositions: tuple[PopulationDispositionEntry, ...]
    formula_plans: tuple[FormulaMembershipPeriodPlan, ...]
    suppressions: tuple[FormulaSuppression, ...]


def _plan_window(
    model: FinancialModel,
    section_plan: CompiledWindowPlan,
    *,
    claimed_item_ids: set[str],
    historical_periods: tuple[int, ...],
    projection_periods: tuple[int, ...],
) -> _WindowPlan:
    """Plan one window, mutating ``model``; an issue unwinds as ``_WindowPlanUnwind``."""

    window_id = section_plan.window_id
    window = section_plan.window
    if projection_periods and (
        projection_periods[0]
        != window.projection_applicability.first_projection_fy
    ):
        _raise_plan_error(
            "projection_window_mismatch",
            f"first projection period is {projection_periods[0]}, expected "
            f"{window.projection_applicability.first_projection_fy}",
            window_id=window_id,
        )

    total, selected, section_items = _resolve_mapping_items(model, section_plan)
    if claimed_item_ids & {total.id, *(item.id for item in selected.values())}:
        _raise_plan_error(
            "statement_window_item_collision",
            "a model row is governed by more than one statement mapping",
            window_id=window_id,
        )
    item_id_by_role = {
        section_plan.section_total_required_role: total.id,
        **{required_role: item.id for required_role, item in selected.items()},
    }

    for row_role in sorted(window.bound_roles):
        binding = window.binding_for(row_role)
        assert binding is not None
        if isinstance(binding.target, InsertTarget):
            _insert_external_formula_member(
                model,
                plan=section_plan,
                target=binding.target,
                historical_periods=historical_periods,
                projection_periods=projection_periods,
            )

    selected_item_ids = {item.id for item in selected.values()}
    section_member_items = [
        item
        for item in section_items
        if _item_section_roles(
            item,
            statement=section_plan.statement,
            section=section_plan.section,
        )
        & {"member", "catch_all", "presentation_only"}
    ]
    suppressed_item_ids = {
        item.id for item in section_member_items if item.id not in selected_item_ids
    }
    population_disabled_items = [
        item
        for item in section_items
        if item.id != total.id
        and item.id not in selected_item_ids
        and item.required_role is not None
    ]
    suppressions = _suppress_formula_members(
        model,
        plan=section_plan,
        total=total,
        selected_item_ids=selected_item_ids,
        suppressed_item_ids=suppressed_item_ids,
        historical_periods=historical_periods,
        projection_periods=projection_periods,
    )

    dispositions = [
        PopulationDispositionEntry(
            window_id=window_id,
            required_role=section_plan.section_total_required_role,
            item_id=total.id,
            disposition="populate",
            reason="authoritative_section_total",
        ),
        *(
            PopulationDispositionEntry(
                window_id=window_id,
                required_role=required_role,
                item_id=item.id,
                disposition="populate",
                reason="selected_authoritative_member",
            )
            for required_role, item in sorted(selected.items())
        ),
        *(
            PopulationDispositionEntry(
                window_id=window_id,
                required_role=str(item.required_role),
                item_id=item.id,
                disposition="disabled_by_authoritative_basis",
                reason=(
                    "unlisted_section_member"
                    if item.id in suppressed_item_ids
                    else "unselected_section_intermediate"
                ),
            )
            for item in sorted(
                population_disabled_items,
                key=lambda value: value.id,
            )
        ),
    ]

    graph_periods: tuple[tuple[FormulaGraphKind, tuple[int, ...]], ...] = (
        ("historical", historical_periods),
        ("projected", projection_periods),
    )
    formula_plans = tuple(
        _compile_period_membership(
            model,
            plan=section_plan,
            graph_kind=graph_kind,
            period=period,
            total=total,
            selected=selected,
            suppressed_item_ids=suppressed_item_ids,
        )
        for graph_kind, periods in graph_periods
        for period in periods
    )
    return _WindowPlan(
        item_id_by_role=item_id_by_role,
        dispositions=tuple(dispositions),
        formula_plans=formula_plans,
        suppressions=tuple(suppressions),
    )


def compile_statement_window_build_plan(
    *,
    ticker: str,
    model: FinancialModel,
    compiled_statement_windows: CompiledStatementWindows,
) -> StatementWindowBuildPlan:
    """Compile PR3 artifacts on a deep copy without enabling product runtime.

    Each window plans on its own copy of the model and is applied only when its
    plan completes; a window that cannot be planned is one issue and leaves no
    mutation behind. Materialization that collides or gaps applies no window —
    the inserted rows are shared by every window — so the plan carries the
    caller's model unmaterialized and one issue per window. A role the applied
    windows give more than one row or disposition keeps no disposition; its
    window carries one ``population_disposition_ambiguous`` issue.
    """

    prepared_model = model.model_copy(deep=True)
    prepared_model.build_index()
    inserted_overrides = TickerOverrides(
        ticker=ticker,
        overrides={},
        semantic_rows=copy.deepcopy(compiled_statement_windows.semantic_rows),
    )
    materialization_result = _materialize_semantic_rows(
        prepared_model,
        ticker,
        inserted_overrides,
        compiled_statement_windows=compiled_statement_windows,
    )
    issues: list[StatementWindowPlanIssue] = []
    planned_windows: Mapping[str, CompiledWindowPlan] = (
        compiled_statement_windows.section_plans
    )
    if materialization_result.collisions or materialization_result.gaps:
        prepared_model = model.model_copy(deep=True)
        issues.extend(
            StatementWindowPlanIssue(
                issue_code="statement_window_materialization_failed",
                detail=(
                    f"inserted rows produced {len(materialization_result.collisions)} "
                    f"collision(s) and {len(materialization_result.gaps)} "
                    "materialization gap(s); no statement window is applied"
                ),
                window_id=window_id,
            )
            for window_id in sorted(planned_windows)
        )
        planned_windows = {}
    prepared_model.build_index()

    item_id_by_role: dict[str, str] = {}
    dispositions: list[PopulationDispositionEntry] = []
    formula_plans: list[FormulaMembershipPeriodPlan] = []
    suppressions: list[FormulaSuppression] = []
    claimed_item_ids: set[str] = set()
    applied_window_ids: list[str] = []

    historical_periods = tuple(
        int(period)
        for period in (
            prepared_model.time_structure.historical_periods
            or prepared_model.time_structure.historical_years
        )
    )
    projection_periods = tuple(
        int(period)
        for period in (
            prepared_model.time_structure.projection_periods
            or prepared_model.time_structure.projection_years
        )
    )

    for window_id, section_plan in sorted(planned_windows.items()):
        window = section_plan.window
        uncovered = window.uncovered_years(historical_periods)
        if uncovered:
            raise MultiBasisWindowIncompleteError(
                window_id=window_id,
                uncovered_years=uncovered,
                from_fy=window.from_fy,
                through_fy=window.through_fy,
            )
        candidate = prepared_model.model_copy(deep=True)
        candidate.build_index()
        try:
            window_plan = _plan_window(
                candidate,
                section_plan,
                claimed_item_ids=claimed_item_ids,
                historical_periods=historical_periods,
                projection_periods=projection_periods,
            )
        except _WindowPlanUnwind as unwind:
            issues.append(unwind.issue)
            continue
        prepared_model = candidate
        applied_window_ids.append(window_id)
        claimed_item_ids.update(window_plan.item_id_by_role.values())
        item_id_by_role.update(window_plan.item_id_by_role)
        dispositions.extend(window_plan.dispositions)
        formula_plans.extend(window_plan.formula_plans)
        suppressions.extend(window_plan.suppressions)

    targets_by_role: dict[str, set[tuple[str, PopulationDisposition]]] = {}
    windows_by_role: dict[str, set[str]] = {}
    for entry in dispositions:
        targets_by_role.setdefault(entry.required_role, set()).add(
            (entry.item_id, entry.disposition)
        )
        windows_by_role.setdefault(entry.required_role, set()).add(entry.window_id)
    ambiguous_by_window: dict[str, list[str]] = {}
    for required_role, targets in sorted(targets_by_role.items()):
        if len(targets) > 1:
            ambiguous_by_window.setdefault(
                min(windows_by_role[required_role]), []
            ).append(
                f"role {required_role!r} resolves to multiple mapped "
                f"rows/dispositions {sorted(targets)} across windows "
                f"{sorted(windows_by_role[required_role])}"
            )
    issues.extend(
        StatementWindowPlanIssue(
            issue_code="population_disposition_ambiguous",
            detail="; ".join(details)
            + "; each such role's population dispositions are dropped",
            window_id=window_id,
        )
        for window_id, details in sorted(ambiguous_by_window.items())
    )
    ambiguous_roles = {
        required_role
        for required_role, targets in targets_by_role.items()
        if len(targets) > 1
    }

    disposition_tuple = tuple(
        sorted(
            (
                entry
                for entry in dispositions
                if entry.required_role not in ambiguous_roles
            ),
            key=lambda entry: (entry.window_id, entry.item_id, entry.disposition),
        )
    )
    membership_tuple = tuple(
        sorted(
            formula_plans,
            key=lambda entry: (entry.window_id, entry.graph_kind, entry.period),
        )
    )
    suppression_tuple = tuple(
        sorted(
            suppressions,
            key=lambda entry: (
                entry.window_id,
                entry.parent_item_id,
                entry.suppressed_item_id,
                entry.graph_scope,
            ),
        )
    )
    materialization = _materialization_payload(materialization_result)
    frozen_item_ids = MappingProxyType(dict(sorted(item_id_by_role.items())))
    fingerprint = _plan_fingerprint(
        ticker=ticker,
        item_id_by_role=frozen_item_ids,
        dispositions=disposition_tuple,
        membership=membership_tuple,
        suppressions=suppression_tuple,
        materialization=materialization,
    )
    return StatementWindowBuildPlan(
        prepared_model=prepared_model,
        item_id_by_role=frozen_item_ids,
        population_dispositions=disposition_tuple,
        formula_membership=membership_tuple,
        formula_suppressions=suppression_tuple,
        materialization=MappingProxyType(materialization),
        fingerprint=fingerprint,
        applied_window_ids=tuple(applied_window_ids),
        issues=tuple(issues),
    )


def _inspect_window(
    model: FinancialModel,
    *,
    section_plan: CompiledWindowPlan,
    build_plan: StatementWindowBuildPlan,
) -> list[FormulaMembershipPeriodPlan]:
    total, selected, _section_items_for_mapping = _resolve_mapping_items(
        model,
        section_plan,
    )
    expected_periods = [
        period_plan
        for period_plan in build_plan.formula_membership
        if period_plan.window_id == section_plan.window_id
    ]
    if not expected_periods:
        _raise_plan_error(
            "formula_membership_plan_missing",
            "compiled mapping has no historical/projected membership plan",
            window_id=section_plan.window_id,
        )
    return [
        _compile_period_membership(
            model,
            plan=section_plan,
            graph_kind=expected.graph_kind,
            period=expected.period,
            total=total,
            selected=selected,
            suppressed_item_ids=set(expected.suppressed_member_item_ids),
        )
        for expected in expected_periods
    ]


def inspect_statement_window_formula_membership(
    model: FinancialModel,
    *,
    compiled_statement_windows: CompiledStatementWindows,
    build_plan: StatementWindowBuildPlan,
) -> tuple[
    tuple[FormulaMembershipPeriodPlan, ...],
    tuple[StatementWindowPlanIssue, ...],
]:
    """Re-read the applied windows' formula graphs without mutating the model.

    A window whose graph can no longer be read is one issue, never a raise; its
    periods are absent from the inspected plans.
    """

    inspected: list[FormulaMembershipPeriodPlan] = []
    issues: list[StatementWindowPlanIssue] = []
    for window_id in build_plan.applied_window_ids:
        try:
            inspected.extend(
                _inspect_window(
                    model,
                    section_plan=compiled_statement_windows.section_plans[window_id],
                    build_plan=build_plan,
                )
            )
        except _WindowPlanUnwind as unwind:
            issues.append(unwind.issue)
    return (
        tuple(
            sorted(
                inspected,
                key=lambda entry: (entry.window_id, entry.graph_kind, entry.period),
            )
        ),
        tuple(issues),
    )


__all__ = [
    "FormulaGraphKind",
    "FormulaMembershipPath",
    "FormulaMembershipPeriodPlan",
    "FormulaSuppression",
    "PopulationDisposition",
    "PopulationDispositionEntry",
    "StatementWindowBuildPlan",
    "StatementWindowPlanIssue",
    "compile_statement_window_build_plan",
    "inspect_statement_window_formula_membership",
]
