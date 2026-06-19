"""Scenario-output readiness contracts and helpers."""

from __future__ import annotations

import sys
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .model_readiness_common import _computed_values, _has_any_value, _missing_periods, _projection_periods
from .models import FinancialModel


ScenarioOutputReadinessStatus = Literal["ready", "incomplete", "blocked", "unknown"]
ScenarioOutputReadinessScope = Literal["base_model_outputs", "thesis_snapshot"]
ScenarioOutputReadinessSeverity = Literal["blocking", "warning"]
ScenarioOutputReadinessIssueCategory = Literal["model_structure", "model_formula_gap"]
ScenarioOutputReadinessIssueCode = Literal[
    "scenario_output_item_missing",
    "scenario_output_projection_missing",
    "scenario_output_projection_incomplete",
]
_PARENT_MODULE = "schema.model_readiness"
_SCENARIO_OUTPUT_REQUIREMENTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "revenue_m",
        (
            "tpl.fm.income_statement.total_revenue",
            "tpl.a.revenue_drivers.total_revenue",
        ),
    ),
    (
        "op_margin_pct",
        (
            "tpl.fm.margins.operating_margin",
            "tpl.a.operating_leverage.operating_margin",
        ),
    ),
    (
        "ebitda_margin_pct",
        (
            "tpl.fm.margins.adj_ebtida_margin",
            "tpl.a.adj_ebitda.adjusted_ebitda_margins",
        ),
    ),
    (
        "adj_eps",
        (
            "tpl.fm.adjusted_earnings.adjusted_eps",
            "tpl.a.tax_net_income.adjusted_eps",
        ),
    ),
    (
        "fcf_per_share",
        (
            "tpl.fm.cash_flow.free_cash_flow_per_share",
            "tpl.fm.balance_sheet.free_cash_flow_per_share",
        ),
    ),
)


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


class ModelScenarioOutputFieldReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    field: str
    item_id: str | None = None
    candidate_item_ids: list[str] = Field(default_factory=list)
    projected_periods: list[int] = Field(default_factory=list)
    missing_periods: list[int] = Field(default_factory=list)


class ModelScenarioOutputReadinessIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: ScenarioOutputReadinessIssueCode
    severity: ScenarioOutputReadinessSeverity
    category: ScenarioOutputReadinessIssueCategory | None = None
    detail: str
    field: str
    item_id: str | None = None
    missing_periods: list[int] = Field(default_factory=list)
    related_item_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _populate_category(self) -> "ModelScenarioOutputReadinessIssue":
        if self.category is None:
            self.category = _compat("_scenario_output_issue_category", _scenario_output_issue_category)(self.code)
        return self


class ModelScenarioOutputReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: ScenarioOutputReadinessStatus = "unknown"
    scope: ScenarioOutputReadinessScope = "base_model_outputs"
    projection_periods: list[int] = Field(default_factory=list)
    terminal_period: int | None = None
    fields: dict[str, ModelScenarioOutputFieldReadiness] = Field(default_factory=dict)
    issues: list[ModelScenarioOutputReadinessIssue] = Field(default_factory=list)
    summary: str | None = None


def compute_model_scenario_output_readiness(
    model: FinancialModel,
    *,
    computed_values: dict[str, dict[int, float]] | None = None,
    scope: ScenarioOutputReadinessScope = "base_model_outputs",
) -> ModelScenarioOutputReadiness:
    """Return readiness for outputs needed by scenario and decision workflows."""

    model.build_index()
    projection_periods = _compat("_projection_periods", _projection_periods)(model)
    if not projection_periods:
        readiness_model = _compat("ModelScenarioOutputReadiness", ModelScenarioOutputReadiness)
        return readiness_model(
            status="unknown",
            scope=scope,
            summary="model has no projection periods",
        )

    computed_values_fn = _compat("_computed_values", _computed_values)
    values = computed_values if computed_values is not None else computed_values_fn(model)
    issues: list[ModelScenarioOutputReadinessIssue] = []
    fields: dict[str, ModelScenarioOutputFieldReadiness] = {}
    field_model = _compat("ModelScenarioOutputFieldReadiness", ModelScenarioOutputFieldReadiness)
    issue_model = _compat("ModelScenarioOutputReadinessIssue", ModelScenarioOutputReadinessIssue)
    missing_periods_fn = _compat("_missing_periods", _missing_periods)
    requirements = _compat("_SCENARIO_OUTPUT_REQUIREMENTS", _SCENARIO_OUTPUT_REQUIREMENTS)

    for field_name, candidate_item_ids in requirements:
        present_item_ids = [item_id for item_id in candidate_item_ids if item_id in model._index]
        if not present_item_ids:
            fields[field_name] = field_model(
                field=field_name,
                candidate_item_ids=list(candidate_item_ids),
                missing_periods=list(projection_periods),
            )
            issues.append(
                issue_model(
                    code="scenario_output_item_missing",
                    severity="blocking",
                    detail=f"scenario output field {field_name!r} has no candidate row in the model",
                    field=field_name,
                    missing_periods=list(projection_periods),
                    related_item_ids=list(candidate_item_ids),
                )
            )
            continue

        item_id = _compat("_select_scenario_output_item_id", _select_scenario_output_item_id)(
            values,
            present_item_ids,
            projection_periods,
        )
        item_values = values.get(item_id, {})
        missing = missing_periods_fn(item_values, projection_periods)
        projected = [period for period in projection_periods if period not in missing]
        fields[field_name] = field_model(
            field=field_name,
            item_id=item_id,
            candidate_item_ids=list(candidate_item_ids),
            projected_periods=projected,
            missing_periods=missing,
        )
        if missing == projection_periods:
            issues.append(
                issue_model(
                    code="scenario_output_projection_missing",
                    severity="blocking",
                    detail=f"scenario output field {field_name!r} row {item_id!r} has no projected values",
                    field=field_name,
                    item_id=item_id,
                    missing_periods=missing,
                    related_item_ids=present_item_ids,
                )
            )
        elif missing:
            issues.append(
                issue_model(
                    code="scenario_output_projection_incomplete",
                    severity="warning",
                    detail=f"scenario output field {field_name!r} row {item_id!r} is missing some projected values",
                    field=field_name,
                    item_id=item_id,
                    missing_periods=missing,
                    related_item_ids=present_item_ids,
                )
            )

    if any(issue.severity == "blocking" for issue in issues):
        status: ScenarioOutputReadinessStatus = "blocked"
    elif issues:
        status = "incomplete"
    else:
        status = "ready"

    readiness_model = _compat("ModelScenarioOutputReadiness", ModelScenarioOutputReadiness)
    return readiness_model(
        status=status,
        scope=scope,
        projection_periods=list(projection_periods),
        terminal_period=projection_periods[-1],
        fields=fields,
        issues=issues,
        summary=_compat("_scenario_output_summary", _scenario_output_summary)(status, issues),
    )


def _scenario_output_issue_category(
    code: ScenarioOutputReadinessIssueCode,
) -> ScenarioOutputReadinessIssueCategory:
    if code == "scenario_output_item_missing":
        return "model_structure"
    return "model_formula_gap"


def _select_scenario_output_item_id(
    values: dict[str, dict[int, float]],
    item_ids: list[str],
    projection_periods: list[int],
) -> str:
    has_any_value = _compat("_has_any_value", _has_any_value)
    for item_id in item_ids:
        if has_any_value(values.get(item_id, {}), projection_periods):
            return item_id
    return item_ids[0]


def _scenario_output_summary(
    status: ScenarioOutputReadinessStatus,
    issues: list[ModelScenarioOutputReadinessIssue],
) -> str:
    if status == "ready":
        return "scenario-critical model outputs resolve for the projection horizon"
    if not issues:
        return "scenario output readiness could not be determined"
    blocking = sum(1 for issue in issues if issue.severity == "blocking")
    warnings = len(issues) - blocking
    return f"{status}: {blocking} blocking issue(s), {warnings} warning issue(s)"


_ORIGINALS = {
    "ModelScenarioOutputFieldReadiness": ModelScenarioOutputFieldReadiness,
    "ModelScenarioOutputReadiness": ModelScenarioOutputReadiness,
    "ModelScenarioOutputReadinessIssue": ModelScenarioOutputReadinessIssue,
    "ScenarioOutputReadinessIssueCategory": ScenarioOutputReadinessIssueCategory,
    "_SCENARIO_OUTPUT_REQUIREMENTS": _SCENARIO_OUTPUT_REQUIREMENTS,
    "_computed_values": _computed_values,
    "_has_any_value": _has_any_value,
    "_missing_periods": _missing_periods,
    "_projection_periods": _projection_periods,
    "_scenario_output_issue_category": _scenario_output_issue_category,
    "_scenario_output_summary": _scenario_output_summary,
    "_select_scenario_output_item_id": _select_scenario_output_item_id,
    "compute_model_scenario_output_readiness": compute_model_scenario_output_readiness,
}


__all__ = [
    "ModelScenarioOutputFieldReadiness",
    "ModelScenarioOutputReadiness",
    "ModelScenarioOutputReadinessIssue",
    "ScenarioOutputReadinessIssueCategory",
    "ScenarioOutputReadinessIssueCode",
    "ScenarioOutputReadinessScope",
    "ScenarioOutputReadinessSeverity",
    "ScenarioOutputReadinessStatus",
    "_SCENARIO_OUTPUT_REQUIREMENTS",
    "_scenario_output_issue_category",
    "_scenario_output_summary",
    "_select_scenario_output_item_id",
    "compute_model_scenario_output_readiness",
]
