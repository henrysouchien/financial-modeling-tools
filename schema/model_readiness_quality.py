"""Model-quality readiness contracts and helpers."""

from __future__ import annotations

import sys
from typing import Any, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from .dependency_graph import DependencyGraph
from .model_readiness_common import (
    _computed_values,
    _has_any_value,
    _historical_periods,
    _is_present,
    _missing_periods,
    _normalize_computed_values,
    _projection_periods,
)
from .model_readiness_valuation import ValuationInputReadiness, _valuation_input_status
from .models import FinancialModel

if TYPE_CHECKING:
    from .segments import SegmentProfile


ModelQualityReadinessStatus = Literal["ready", "incomplete", "blocked", "unknown"]
ModelQualityReadinessSeverity = Literal["blocking", "warning"]
ModelQualityReadinessDomain = Literal["share_count", "working_capital", "valuation", "segment_basis"]
ModelQualityDomain = ModelQualityReadinessDomain
_PARENT_MODULE = "schema.model_readiness"

class ModelQualityReadinessIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: str
    severity: ModelQualityReadinessSeverity
    domain: ModelQualityReadinessDomain
    detail: str
    item_id: str | None = None
    missing_periods: list[int] = Field(default_factory=list)
    related_item_ids: list[str] = Field(default_factory=list)

class ModelQualityDomainReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: ModelQualityReadinessStatus = "unknown"
    required_items: list[str] = Field(default_factory=list)
    missing_periods: list[int] = Field(default_factory=list)
    issues: list[ModelQualityReadinessIssue] = Field(default_factory=list)


class ModelQualityReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: ModelQualityReadinessStatus = "unknown"
    scope: str = "model_quality"
    projection_periods: list[int] = Field(default_factory=list)
    domains: dict[ModelQualityReadinessDomain, ModelQualityDomainReadiness] = Field(default_factory=dict)
    issues: list[ModelQualityReadinessIssue] = Field(default_factory=list)
    summary: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


def compute_model_quality_readiness(
    model: FinancialModel,
    *,
    computed_values: dict[str, dict[int, float]] | None = None,
    valuation_input_readiness: ValuationInputReadiness | dict[str, Any] | None = None,
    price_target_skip_reason: str | None = None,
    segment_profile: "SegmentProfile | None" = None,
) -> ModelQualityReadiness:
    """Return FMS-26 economic model-quality readiness across downstream domains."""

    model.build_index()
    projection_periods = _compat("_projection_periods", _projection_periods)(model)
    if not projection_periods:
        return ModelQualityReadiness(
            status="unknown",
            projection_periods=[],
            summary="model has no projection periods",
        )

    computed_values_fn = _compat("_computed_values", _computed_values)
    normalize_computed_values_fn = _compat("_normalize_computed_values", _normalize_computed_values)
    values = normalize_computed_values_fn(
        computed_values if computed_values is not None else computed_values_fn(model)
    )
    domains = {
        "share_count": _compat("_share_count_quality_domain", _share_count_quality_domain)(
            model,
            values,
            projection_periods,
        ),
        "working_capital": _compat("_working_capital_quality_domain", _working_capital_quality_domain)(
            model,
            values,
            projection_periods,
        ),
        "valuation": _compat("_valuation_quality_domain", _valuation_quality_domain)(
            model,
            values,
            projection_periods,
            valuation_input_readiness=valuation_input_readiness,
            price_target_skip_reason=price_target_skip_reason,
        ),
        "segment_basis": _compat("_segment_basis_quality_domain", _segment_basis_quality_domain)(
            model,
            segment_profile=segment_profile,
        ),
    }
    issues = [issue for domain in domains.values() for issue in domain.issues]

    if any(issue.severity == "blocking" for issue in issues):
        status: ModelQualityReadinessStatus = "blocked"
    elif issues:
        status = "incomplete"
    else:
        status = "ready"

    readiness_model = _compat("ModelQualityReadiness", ModelQualityReadiness)
    return readiness_model(
        status=status,
        projection_periods=list(projection_periods),
        domains=domains,
        issues=issues,
        summary=_compat("_model_quality_summary", _model_quality_summary)(status, domains),
    )


def _quality_domain(
    *,
    required_items: list[str],
    issues: list[ModelQualityReadinessIssue],
) -> ModelQualityDomainReadiness:
    if any(issue.severity == "blocking" for issue in issues):
        status: ModelQualityReadinessStatus = "blocked"
    elif issues:
        status = "incomplete"
    else:
        status = "ready"
    missing_periods = sorted({
        int(period)
        for issue in issues
        for period in issue.missing_periods
    })
    domain_model = _compat("ModelQualityDomainReadiness", ModelQualityDomainReadiness)
    return domain_model(
        status=status,
        required_items=required_items,
        missing_periods=missing_periods,
        issues=issues,
    )


def _quality_projection_issue(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    periods: list[int],
    *,
    item_id: str,
    domain: ModelQualityReadinessDomain,
    missing_item_code: str,
    missing_values_code: str,
    label: str,
    all_missing_severity: ModelQualityReadinessSeverity = "blocking",
    partial_missing_severity: ModelQualityReadinessSeverity = "warning",
) -> ModelQualityReadinessIssue | None:
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    if item_id not in model._index:
        return issue_model(
            code=missing_item_code,
            severity="blocking",
            domain=domain,
            detail=f"{label} row {item_id!r} is missing from the model",
            item_id=item_id,
            missing_periods=list(periods),
        )
    missing = _compat("_missing_periods", _missing_periods)(values.get(item_id, {}), periods)
    if not missing:
        return None
    severity = all_missing_severity if missing == periods else partial_missing_severity
    return issue_model(
        code=missing_values_code,
        severity=severity,
        domain=domain,
        detail=f"{label} row {item_id!r} is missing projected values",
        item_id=item_id,
        missing_periods=missing,
    )


def _quality_any_projection_issue(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    periods: list[int],
    *,
    item_id: str,
    domain: ModelQualityReadinessDomain,
    missing_item_code: str,
    missing_values_code: str,
    label: str,
) -> ModelQualityReadinessIssue | None:
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    if item_id not in model._index:
        return issue_model(
            code=missing_item_code,
            severity="warning",
            domain=domain,
            detail=f"{label} row {item_id!r} is missing from the model",
            item_id=item_id,
            missing_periods=list(periods),
        )
    if _compat("_has_any_value", _has_any_value)(values.get(item_id, {}), periods):
        return None
    return issue_model(
        code=missing_values_code,
        severity="warning",
        domain=domain,
        detail=f"{label} row {item_id!r} has no projected value",
        item_id=item_id,
        missing_periods=list(periods),
    )


def _dependency_issue(
    model: FinancialModel,
    *,
    item_id: str,
    expected_upstream_id: str,
    domain: ModelQualityReadinessDomain,
    code: str,
    label: str,
) -> ModelQualityReadinessIssue | None:
    if item_id not in model._index or expected_upstream_id not in model._index:
        return None
    upstream = _compat("_upstream_item_ids", _upstream_item_ids)(model, item_id)
    if expected_upstream_id in upstream:
        return None
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    return issue_model(
        code=code,
        severity="blocking",
        domain=domain,
        detail=f"{label} row {item_id!r} is not downstream of {expected_upstream_id!r}",
        item_id=item_id,
        related_item_ids=[expected_upstream_id],
    )


def _share_count_quality_domain(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
) -> ModelQualityDomainReadiness:
    share_item_id = "tpl.fm.income_statement.diluted_shares_outstanding_m"
    required_items = [
        share_item_id,
        "tpl.fm.adjusted_earnings.adjusted_eps",
        "tpl.fm.cash_flow.free_cash_flow_per_share",
        "tpl.v.current_valuation.shares_outstanding",
        "tpl.v.dcf.shares_outstanding",
        "tpl.v.forward_ev_ebitda.shares_fy2",
    ]
    issues: list[ModelQualityReadinessIssue] = []

    for item_id, label in (
        (share_item_id, "diluted share count"),
        ("tpl.fm.adjusted_earnings.adjusted_eps", "adjusted EPS"),
        ("tpl.fm.cash_flow.free_cash_flow_per_share", "free cash flow per share"),
        ("tpl.v.current_valuation.shares_outstanding", "current valuation shares"),
        ("tpl.v.dcf.shares_outstanding", "DCF shares"),
    ):
        issue = _compat("_quality_projection_issue", _quality_projection_issue)(
            model,
            values,
            projection_periods,
            item_id=item_id,
            domain="share_count",
            missing_item_code="share_count_item_missing",
            missing_values_code="share_count_projection_missing",
            label=label,
        )
        if issue is not None:
            issues.append(issue)

    fy2_issue = _compat("_quality_any_projection_issue", _quality_any_projection_issue)(
        model,
        values,
        projection_periods,
        item_id="tpl.v.forward_ev_ebitda.shares_fy2",
        domain="share_count",
        missing_item_code="share_count_item_missing",
        missing_values_code="share_count_projection_missing",
        label="FY2 valuation shares",
    )
    if fy2_issue is not None:
        issues.append(fy2_issue)

    for item_id, label in (
        ("tpl.fm.adjusted_earnings.adjusted_eps", "adjusted EPS"),
        ("tpl.fm.cash_flow.free_cash_flow_per_share", "free cash flow per share"),
        ("tpl.v.current_valuation.shares_outstanding", "current valuation shares"),
    ):
        issue = _compat("_dependency_issue", _dependency_issue)(
            model,
            item_id=item_id,
            expected_upstream_id=share_item_id,
            domain="share_count",
            code="share_count_dependency_missing",
            label=label,
        )
        if issue is not None:
            issues.append(issue)

    return _compat("_quality_domain", _quality_domain)(required_items=required_items, issues=issues)


def _working_capital_quality_domain(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
) -> ModelQualityDomainReadiness:
    required_items = [
        "tpl.a.balance_sheet_wc.days_sales_outstanding_dso",
        "tpl.a.balance_sheet_wc.days_payable_outstanding_dpo",
        "tpl.fm.balance_sheet.accounts_receivable_net",
        "tpl.fm.balance_sheet.accounts_payable",
        "tpl.fm.balance_sheet.current_asset_2",
        "tpl.fm.cash_flow.current_asset_2",
        "tpl.fm.cash_flow.changes_in_operating_assets_and_liabilities",
        "tpl.fm.cash_flow.free_cash_flow",
    ]
    issues: list[ModelQualityReadinessIssue] = []
    for item_id, label in (
        ("tpl.a.balance_sheet_wc.days_sales_outstanding_dso", "DSO"),
        ("tpl.a.balance_sheet_wc.days_payable_outstanding_dpo", "DPO"),
        ("tpl.fm.balance_sheet.accounts_receivable_net", "accounts receivable"),
        ("tpl.fm.balance_sheet.accounts_payable", "accounts payable"),
        ("tpl.fm.cash_flow.changes_in_operating_assets_and_liabilities", "working-capital cash-flow delta"),
        ("tpl.fm.cash_flow.free_cash_flow", "free cash flow"),
    ):
        issue = _compat("_quality_projection_issue", _quality_projection_issue)(
            model,
            values,
            projection_periods,
            item_id=item_id,
            domain="working_capital",
            missing_item_code="working_capital_item_missing",
            missing_values_code="working_capital_projection_missing",
            label=label,
        )
        if issue is not None:
            issues.append(issue)

    if _compat("_inventory_material", _inventory_material)(model, values):
        for item_id, label in (
            ("tpl.fm.balance_sheet.current_asset_2", "inventory balance"),
            ("tpl.fm.cash_flow.current_asset_2", "change in inventory"),
        ):
            issue = _compat("_quality_projection_issue", _quality_projection_issue)(
                model,
                values,
                projection_periods,
                item_id=item_id,
                domain="working_capital",
                missing_item_code="working_capital_item_missing",
                missing_values_code="inventory_policy_missing",
                label=label,
                all_missing_severity="warning",
                partial_missing_severity="warning",
            )
            if issue is not None:
                issue.code = "inventory_policy_missing"
                issue.detail = (
                    f"{issue.detail}; material inventory requires an explicit inventory policy "
                    "until a real DIO/inventory-days driver exists"
                )
                issues.append(issue)

    return _compat("_quality_domain", _quality_domain)(required_items=required_items, issues=issues)


def _valuation_quality_domain(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
    *,
    valuation_input_readiness: ValuationInputReadiness | dict[str, Any] | None,
    price_target_skip_reason: str | None,
) -> ModelQualityDomainReadiness:
    required_items = [
        "tpl.v.current_valuation.stock_price",
        "tpl.v.current_valuation.shares_outstanding",
        "tpl.v.current_valuation.net_debt",
        "tpl.v.dcf.dcf_price",
    ]
    issues: list[ModelQualityReadinessIssue] = []
    for item_id, label in (
        ("tpl.v.current_valuation.shares_outstanding", "valuation shares"),
        ("tpl.v.current_valuation.net_debt", "valuation net debt"),
        ("tpl.v.dcf.dcf_price", "DCF price"),
    ):
        issue = _compat("_quality_projection_issue", _quality_projection_issue)(
            model,
            values,
            projection_periods,
            item_id=item_id,
            domain="valuation",
            missing_item_code="valuation_item_missing",
            missing_values_code="valuation_projection_missing",
            label=label,
        )
        if issue is not None:
            issues.append(issue)

    valuation_input_status = _compat("_valuation_input_status", _valuation_input_status)
    readiness_status, missing_inputs = valuation_input_status(valuation_input_readiness)
    if readiness_status == "incomplete":
        severity: ModelQualityReadinessSeverity = "blocking" if missing_inputs else "warning"
        detail = (
            f"valuation_input_readiness is incomplete; missing inputs: {', '.join(missing_inputs)}"
            if missing_inputs
            else "valuation_input_readiness is incomplete; only placeholder/staleness flags may be present"
        )
        issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
        issues.append(
            issue_model(
                code="valuation_inputs_incomplete",
                severity=severity,
                domain="valuation",
                detail=detail,
                related_item_ids=list(missing_inputs),
            )
        )

    if price_target_skip_reason:
        issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
        issues.append(
            issue_model(
                code="price_target_skip_reason",
                severity="blocking",
                domain="valuation",
                detail=f"PriceTarget derivation would be skipped: {price_target_skip_reason}",
            )
        )

    return _compat("_quality_domain", _quality_domain)(required_items=required_items, issues=issues)


def _segment_basis_quality_domain(
    model: FinancialModel,
    *,
    segment_profile: "SegmentProfile | None",
) -> ModelQualityDomainReadiness:
    issues: list[ModelQualityReadinessIssue] = []
    segment_profile_basis_issues = _compat("_segment_profile_basis_issues", None)
    if segment_profile is not None and segment_profile_basis_issues is not None:
        issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
        for source_issue in segment_profile_basis_issues(model, segment_profile=segment_profile):
            issues.append(
                issue_model(
                    code=source_issue.code,
                    severity=source_issue.severity,
                    domain="segment_basis",
                    detail=source_issue.detail,
                    item_id=source_issue.item_id,
                    missing_periods=list(source_issue.missing_periods),
                    related_item_ids=list(source_issue.related_item_ids),
                )
            )
    return _compat("_quality_domain", _quality_domain)(required_items=[], issues=issues)


def _inventory_material(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
) -> bool:
    inventory_values = values.get("tpl.fm.balance_sheet.current_asset_2", {})
    if not inventory_values:
        return False
    historical_periods = _compat("_historical_periods", _historical_periods)(model)
    is_present = _compat("_is_present", _is_present)
    inventory = [
        abs(float(value))
        for period, value in inventory_values.items()
        if int(period) in historical_periods and is_present(value)
    ]
    if not inventory or max(inventory) <= 1e-9:
        return False

    revenue_values = values.get("tpl.fm.income_statement.total_revenue", {})
    revenue = [
        abs(float(value))
        for period, value in revenue_values.items()
        if int(period) in historical_periods and is_present(value)
    ]
    if not revenue or max(revenue) <= 1e-9:
        return max(inventory) > 1.0
    return max(inventory) / max(revenue) >= 0.01


def _upstream_item_ids(model: FinancialModel, root_id: str) -> set[str]:
    if root_id not in model._index:
        return set()

    graph = DependencyGraph()
    graph.build(model)
    reverse_adj: dict[str, set[str]] = {node: set() for node in graph.nodes}
    for dependency_id, dependent_ids in graph.adj.items():
        for dependent_id in dependent_ids:
            reverse_adj.setdefault(dependent_id, set()).add(dependency_id)
    for dependent_id, refs in graph.time_edges.items():
        reverse_adj.setdefault(dependent_id, set()).update(ref.id for ref in refs)

    seen: set[str] = set()
    stack = list(reverse_adj.get(root_id, set()))
    while stack:
        item_id = stack.pop()
        if item_id in seen:
            continue
        seen.add(item_id)
        stack.extend(reverse_adj.get(item_id, set()) - seen)
    return seen


def _model_quality_summary(
    status: ModelQualityReadinessStatus,
    domains: dict[ModelQualityReadinessDomain, ModelQualityDomainReadiness],
) -> str:
    if status == "ready":
        return "share count, working capital, valuation, and segment-basis quality checks are ready"
    pieces = [
        f"{domain}={readiness.status}"
        for domain, readiness in domains.items()
        if readiness.status != "ready"
    ]
    return f"{status}: " + ", ".join(pieces)


_ORIGINALS = {
    "ModelQualityDomainReadiness": ModelQualityDomainReadiness,
    "ModelQualityReadiness": ModelQualityReadiness,
    "ModelQualityReadinessIssue": ModelQualityReadinessIssue,
    "_computed_values": _computed_values,
    "_dependency_issue": _dependency_issue,
    "_has_any_value": _has_any_value,
    "_historical_periods": _historical_periods,
    "_inventory_material": _inventory_material,
    "_is_present": _is_present,
    "_missing_periods": _missing_periods,
    "_model_quality_summary": _model_quality_summary,
    "_normalize_computed_values": _normalize_computed_values,
    "_projection_periods": _projection_periods,
    "_quality_any_projection_issue": _quality_any_projection_issue,
    "_quality_domain": _quality_domain,
    "_quality_projection_issue": _quality_projection_issue,
    "_segment_basis_quality_domain": _segment_basis_quality_domain,
    "_segment_profile_basis_issues": None,
    "_share_count_quality_domain": _share_count_quality_domain,
    "_upstream_item_ids": _upstream_item_ids,
    "_valuation_input_status": _valuation_input_status,
    "_valuation_quality_domain": _valuation_quality_domain,
    "_working_capital_quality_domain": _working_capital_quality_domain,
    "compute_model_quality_readiness": compute_model_quality_readiness,
}


__all__ = [
    "ModelQualityDomain",
    "ModelQualityDomainReadiness",
    "ModelQualityReadiness",
    "ModelQualityReadinessDomain",
    "ModelQualityReadinessIssue",
    "ModelQualityReadinessSeverity",
    "ModelQualityReadinessStatus",
    "_dependency_issue",
    "_inventory_material",
    "_model_quality_summary",
    "_quality_any_projection_issue",
    "_quality_domain",
    "_quality_projection_issue",
    "_segment_basis_quality_domain",
    "_share_count_quality_domain",
    "_upstream_item_ids",
    "_valuation_quality_domain",
    "_working_capital_quality_domain",
    "compute_model_quality_readiness",
]
