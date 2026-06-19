"""Scenario bridge locator helpers for model readiness checks."""

from __future__ import annotations

import re
import sys
from typing import Any, Iterable, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .model_readiness_common import _projection_periods
from .dependency_graph import DependencyGraph
from .models import FinancialModel, FormulaType, ItemType, LineItem
from .refs import line_item_ref_from_obj


_PARENT_MODULE = "schema.model_readiness"
_SCENARIO_CASE_PATTERNS: dict[str, set[str]] = {
    "bull": {"bull", "bullcase", "upside", "bullscenario"},
    "base": {"base", "basecase", "central", "basescenario"},
    "bear": {"bear", "bearcase", "downside", "bearscenario"},
}
_SCENARIO_CASES: tuple[str, str, str] = ("bull", "base", "bear")
ScenarioBridgeReadinessStatus = Literal["ready", "incomplete", "blocked", "unknown"]
ScenarioBridgeReadinessScope = Literal["workbook_scenario_bridge"]
ScenarioBridgeReadinessSeverity = Literal["blocking", "warning"]
ScenarioBridgeReadinessIssueCategory = Literal["model_structure", "model_propagation"]
ScenarioBridgeReadinessIssueCode = Literal[
    "scenario_bridge_owner_missing",
    "scenario_bridge_anchor_missing",
    "scenario_bridge_case_row_missing",
    "scenario_bridge_target_missing",
    "inert_scenario_anchor",
]
_DEFAULT_SCENARIO_BRIDGE_TARGET_ID = "tpl.fm.adjusted_earnings.adjusted_eps"


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


class ModelScenarioBridgeOwnerReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    owner_id: str
    label: str | None = None
    anchor_id: str | None = None
    bull_id: str | None = None
    base_id: str | None = None
    bear_id: str | None = None
    missing_cases: list[str] = Field(default_factory=list)
    target_item_id: str | None = None
    upstream_of_target: bool | None = None


class ModelScenarioBridgeReadinessIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: ScenarioBridgeReadinessIssueCode
    severity: ScenarioBridgeReadinessSeverity
    category: ScenarioBridgeReadinessIssueCategory | None = None
    detail: str
    owner_id: str | None = None
    anchor_id: str | None = None
    target_item_id: str | None = None
    missing_cases: list[str] = Field(default_factory=list)
    related_item_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _populate_category(self) -> "ModelScenarioBridgeReadinessIssue":
        if self.category is None:
            self.category = _compat("_scenario_bridge_issue_category", _scenario_bridge_issue_category)(self.code)
        return self


class ModelScenarioBridgeReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: ScenarioBridgeReadinessStatus = "unknown"
    scope: ScenarioBridgeReadinessScope = "workbook_scenario_bridge"
    projection_periods: list[int] = Field(default_factory=list)
    target_item_id: str = "tpl.fm.adjusted_earnings.adjusted_eps"
    owners: list[ModelScenarioBridgeOwnerReadiness] = Field(default_factory=list)
    issues: list[ModelScenarioBridgeReadinessIssue] = Field(default_factory=list)
    summary: str | None = None


def compute_model_scenario_bridge_readiness(
    model: FinancialModel,
    *,
    owner_ids: Iterable[str] | None = None,
    target_item_id: str = _DEFAULT_SCENARIO_BRIDGE_TARGET_ID,
) -> ModelScenarioBridgeReadiness:
    """Return readiness for workbook scenario owners to propagate into EPS."""

    model.build_index()
    projection_periods = _compat("_projection_periods", _projection_periods)(model)
    owner_filter = {str(owner_id) for owner_id in owner_ids or []}
    filtered = bool(owner_filter)
    owners = _compat("_scenario_bridge_owners", _scenario_bridge_owners)(
        model,
        owner_filter=owner_filter if filtered else None,
    )
    issue_model = _compat("ModelScenarioBridgeReadinessIssue", ModelScenarioBridgeReadinessIssue)
    owner_model = _compat("ModelScenarioBridgeOwnerReadiness", ModelScenarioBridgeOwnerReadiness)
    issues: list[ModelScenarioBridgeReadinessIssue] = []
    owner_readiness: list[ModelScenarioBridgeOwnerReadiness] = []

    if target_item_id not in model._index:
        issues.append(
            issue_model(
                code="scenario_bridge_target_missing",
                severity="blocking",
                detail=f"scenario bridge target item {target_item_id!r} was not found",
                target_item_id=target_item_id,
            )
        )

    if filtered:
        found_owner_ids = {owner.id for owner in owners}
        for missing_owner_id in sorted(owner_filter - found_owner_ids):
            issues.append(
                issue_model(
                    code="scenario_bridge_owner_missing",
                    severity="blocking",
                    detail=f"scenario bridge owner {missing_owner_id!r} was not found",
                    owner_id=missing_owner_id,
                    target_item_id=target_item_id,
                )
            )
    elif not owners:
        issues.append(
            issue_model(
                code="scenario_bridge_owner_missing",
                severity="blocking",
                detail="no workbook scenario owner rows were found",
                target_item_id=target_item_id,
            )
        )

    graph_cls = _compat("DependencyGraph", DependencyGraph)
    graph = graph_cls()
    graph.build(model)
    scenario_cases = _compat("_SCENARIO_CASES", _SCENARIO_CASES)

    for owner in owners:
        anchor_id = _compat("_scenario_bridge_anchor_id", _scenario_bridge_anchor_id)(owner)
        case_row_ids = (
            _compat("_scenario_bridge_case_row_ids", _scenario_bridge_case_row_ids)(model, anchor_id)
            if anchor_id is not None
            else {case: None for case in scenario_cases}
        )
        missing_cases = [case for case in scenario_cases if not case_row_ids.get(case)]
        upstream_of_target: bool | None = None
        if target_item_id in model._index:
            downstream = _compat("_graph_downstream_ids", _graph_downstream_ids)(graph, {owner.id})
            upstream_of_target = target_item_id in downstream or owner.id == target_item_id

        owner_readiness.append(
            owner_model(
                owner_id=owner.id,
                label=owner.label,
                anchor_id=anchor_id,
                bull_id=case_row_ids.get("bull"),
                base_id=case_row_ids.get("base"),
                bear_id=case_row_ids.get("bear"),
                missing_cases=missing_cases,
                target_item_id=target_item_id,
                upstream_of_target=upstream_of_target,
            )
        )

        if anchor_id is None:
            issues.append(
                issue_model(
                    code="scenario_bridge_anchor_missing",
                    severity="blocking",
                    detail=f"scenario owner {owner.id!r} is missing an offset-scenario anchor",
                    owner_id=owner.id,
                    target_item_id=target_item_id,
                )
            )
        elif missing_cases:
            issues.append(
                issue_model(
                    code="scenario_bridge_case_row_missing",
                    severity="blocking",
                    detail=f"scenario owner {owner.id!r} is missing scenario case row(s): {', '.join(missing_cases)}",
                    owner_id=owner.id,
                    anchor_id=anchor_id,
                    target_item_id=target_item_id,
                    missing_cases=missing_cases,
                )
            )

        if upstream_of_target is False:
            issues.append(
                issue_model(
                    code="inert_scenario_anchor",
                    severity="blocking",
                    detail=(
                        f"{owner.id} is not upstream of {target_item_id}; scenario row writes would be "
                        "presentation-only for EPS and are not safe as durable scenario assumptions"
                    ),
                    owner_id=owner.id,
                    anchor_id=anchor_id,
                    target_item_id=target_item_id,
                    related_item_ids=[target_item_id],
                )
            )

    if any(issue.severity == "blocking" for issue in issues):
        status: ScenarioBridgeReadinessStatus = "blocked"
    elif issues:
        status = "incomplete"
    else:
        status = "ready"

    if not projection_periods and status == "ready":
        status = "unknown"

    readiness_model = _compat("ModelScenarioBridgeReadiness", ModelScenarioBridgeReadiness)
    return readiness_model(
        status=status,
        projection_periods=list(projection_periods),
        target_item_id=target_item_id,
        owners=owner_readiness,
        issues=issues,
        summary=_compat("_scenario_bridge_summary", _scenario_bridge_summary)(status, issues),
    )


def _scenario_bridge_owners(
    model: FinancialModel,
    *,
    owner_filter: set[str] | None = None,
) -> list[LineItem]:
    owners: list[LineItem] = []
    is_offset_scenario_owner = _compat("_is_offset_scenario_owner", _is_offset_scenario_owner)
    for sheet in model.sheets.values():
        if sheet.name != "Assumptions":
            continue
        for section in sheet.sections:
            for item in section.line_items:
                if owner_filter is not None and item.id not in owner_filter:
                    continue
                if is_offset_scenario_owner(item):
                    owners.append(item)
    owners.sort(key=lambda item: (int(item.row or 0), item.id))
    return owners


def _is_offset_scenario_owner(item: LineItem) -> bool:
    spec = item.projected
    return (
        spec is not None
        and spec.type == FormulaType.valuation
        and spec.subtype == "offset_scenario"
    )


def _scenario_bridge_anchor_id(owner: LineItem) -> str | None:
    spec = owner.projected
    if spec is None:
        return None
    ref_from_obj = _compat("line_item_ref_from_obj", line_item_ref_from_obj)
    anchor_ref = ref_from_obj((spec.params or {}).get("anchor"))
    return anchor_ref.id if anchor_ref is not None else None


def _scenario_bridge_case_row_ids(
    model: FinancialModel,
    anchor_id: str | None,
) -> dict[str, str | None]:
    find_scenario_value_row = _compat("_find_scenario_value_row", _find_scenario_value_row)
    scenario_cases = _compat("_SCENARIO_CASES", _SCENARIO_CASES)
    return {
        case: find_scenario_value_row(model, anchor_id, case) if anchor_id else None
        for case in scenario_cases
    }


def _find_scenario_value_row(model: FinancialModel, anchor_id: str, case: str) -> str | None:
    find_item_location = _compat("_find_item_location", _find_item_location)
    anchor_location = find_item_location(model, anchor_id)
    if anchor_location is None:
        return None
    _sheet_name, section, anchor_index = anchor_location
    anchor_row = int(section.line_items[anchor_index].row)

    offset_anchor_ids = _compat("_offset_anchor_ids", _offset_anchor_ids)
    all_anchor_ids = offset_anchor_ids(model)
    candidates: list[LineItem] = []
    for candidate in section.line_items[anchor_index + 1:]:
        if int(candidate.row) <= anchor_row:
            continue
        if candidate.id in all_anchor_ids:
            break
        if candidate.item_type in {ItemType.header, ItemType.spacer}:
            break
        candidates.append(candidate)

    scenario_case_patterns = _compat("_SCENARIO_CASE_PATTERNS", _SCENARIO_CASE_PATTERNS)
    normalize_scenario_label = _compat("_normalize_scenario_label", _normalize_scenario_label)
    patterns = scenario_case_patterns[case]
    for candidate in candidates:
        if normalize_scenario_label(candidate.label) in patterns:
            return candidate.id

    if len(candidates) != 3:
        return None

    sorted_candidates = sorted(candidates, key=lambda item: int(item.row))
    rows = [int(item.row) for item in sorted_candidates]
    expected_rows = list(range(rows[0], rows[0] + 3))
    if rows != expected_rows:
        return None

    position = {"bull": 0, "base": 1, "bear": 2}[case]
    return sorted_candidates[position].id


def _find_item_location(model: FinancialModel, item_id: str) -> tuple[str, Any, int] | None:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item in enumerate(section.line_items):
                if item.id == item_id:
                    return sheet_name, section, index
    return None


def _offset_anchor_ids(model: FinancialModel) -> set[str]:
    anchors: set[str] = set()
    ref_from_obj = _compat("line_item_ref_from_obj", line_item_ref_from_obj)
    for item in model._index.values():
        for spec in (item.historical, item.projected):
            if spec is None or spec.type != FormulaType.valuation or spec.subtype != "offset_scenario":
                continue
            anchor_ref = ref_from_obj((spec.params or {}).get("anchor"))
            if anchor_ref is not None:
                anchors.add(anchor_ref.id)
    return anchors


def _normalize_scenario_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(label or "").strip().lower())


def _graph_downstream_ids(graph: DependencyGraph, roots: set[str]) -> set[str]:
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


def _scenario_bridge_issue_category(
    code: ScenarioBridgeReadinessIssueCode,
) -> ScenarioBridgeReadinessIssueCategory:
    if code == "inert_scenario_anchor":
        return "model_propagation"
    return "model_structure"


def _scenario_bridge_summary(
    status: ScenarioBridgeReadinessStatus,
    issues: list[ModelScenarioBridgeReadinessIssue],
) -> str:
    if status == "ready":
        return "workbook scenario owners propagate into adjusted EPS"
    if not issues:
        return "scenario bridge readiness could not be determined"
    blocking = sum(1 for issue in issues if issue.severity == "blocking")
    warnings = len(issues) - blocking
    return f"{status}: {blocking} blocking issue(s), {warnings} warning issue(s)"


_ORIGINALS = {
    "DependencyGraph": DependencyGraph,
    "ModelScenarioBridgeOwnerReadiness": ModelScenarioBridgeOwnerReadiness,
    "ModelScenarioBridgeReadiness": ModelScenarioBridgeReadiness,
    "ModelScenarioBridgeReadinessIssue": ModelScenarioBridgeReadinessIssue,
    "ScenarioBridgeReadinessIssueCategory": ScenarioBridgeReadinessIssueCategory,
    "_DEFAULT_SCENARIO_BRIDGE_TARGET_ID": _DEFAULT_SCENARIO_BRIDGE_TARGET_ID,
    "_SCENARIO_CASE_PATTERNS": _SCENARIO_CASE_PATTERNS,
    "_SCENARIO_CASES": _SCENARIO_CASES,
    "_find_item_location": _find_item_location,
    "_find_scenario_value_row": _find_scenario_value_row,
    "_graph_downstream_ids": _graph_downstream_ids,
    "_is_offset_scenario_owner": _is_offset_scenario_owner,
    "_normalize_scenario_label": _normalize_scenario_label,
    "_offset_anchor_ids": _offset_anchor_ids,
    "_projection_periods": _projection_periods,
    "_scenario_bridge_anchor_id": _scenario_bridge_anchor_id,
    "_scenario_bridge_case_row_ids": _scenario_bridge_case_row_ids,
    "_scenario_bridge_issue_category": _scenario_bridge_issue_category,
    "_scenario_bridge_owners": _scenario_bridge_owners,
    "_scenario_bridge_summary": _scenario_bridge_summary,
    "compute_model_scenario_bridge_readiness": compute_model_scenario_bridge_readiness,
    "line_item_ref_from_obj": line_item_ref_from_obj,
}


__all__ = [
    "ModelScenarioBridgeOwnerReadiness",
    "ModelScenarioBridgeReadiness",
    "ModelScenarioBridgeReadinessIssue",
    "ScenarioBridgeReadinessIssueCategory",
    "ScenarioBridgeReadinessIssueCode",
    "ScenarioBridgeReadinessScope",
    "ScenarioBridgeReadinessSeverity",
    "ScenarioBridgeReadinessStatus",
    "_DEFAULT_SCENARIO_BRIDGE_TARGET_ID",
    "_SCENARIO_CASE_PATTERNS",
    "_SCENARIO_CASES",
    "_find_item_location",
    "_find_scenario_value_row",
    "_graph_downstream_ids",
    "_is_offset_scenario_owner",
    "_normalize_scenario_label",
    "_offset_anchor_ids",
    "_scenario_bridge_anchor_id",
    "_scenario_bridge_case_row_ids",
    "_scenario_bridge_issue_category",
    "_scenario_bridge_owners",
    "_scenario_bridge_summary",
    "compute_model_scenario_bridge_readiness",
]
