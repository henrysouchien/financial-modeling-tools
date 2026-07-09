"""Model-quality readiness contracts and helpers."""

from __future__ import annotations

import math
import sys
from typing import Any, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from .build_scenarios import compute_scenario_outputs
from .dependency_graph import DependencyGraph
from .growth_carry_forward_guard import (
    GrowthCarryForwardFinding,
    growth_carry_forward_findings,
)
from .model_readiness_common import (
    _computed_values,
    _has_any_value,
    _historical_periods,
    _is_present,
    _missing_periods,
    _normalize_computed_values,
    _projection_periods,
)
from .model_readiness_scenario_bridge import (
    _scenario_bridge_owners,
    compute_model_scenario_bridge_readiness,
)
from .model_readiness_valuation import ValuationInputReadiness, _valuation_input_status
from .models import FinancialModel

if TYPE_CHECKING:
    from .segments import SegmentProfile


ModelQualityReadinessStatus = Literal["ready", "incomplete", "blocked", "unknown"]
ModelQualityReadinessSeverity = Literal["blocking", "warning"]
ModelQualityReadinessDomain = Literal[
    "share_count",
    "working_capital",
    "valuation",
    "scenario_bridge",
    "segment_basis",
]
ModelQualityDomain = ModelQualityReadinessDomain
_PARENT_MODULE = "schema.model_readiness"
_CAPITAL_STRUCTURE_REQUIRED_ITEMS = [
    "tpl.v.wacc.wacc",
    "tpl.v.wacc.weight_equity",
    "tpl.v.wacc.weight_debt",
    "tpl.v.wacc.total_capital",
    "tpl.v.wacc.debt_value",
    "tpl.v.wacc.after_tax_cost_of_debt",
    "tpl.v.cost_of_equity.cost_of_equity",
    "tpl.fm.balance_sheet.long_term_debt",
]
_CAPITAL_STRUCTURE_LABELS = {
    "tpl.v.wacc.wacc": "WACC",
    "tpl.v.wacc.weight_equity": "equity weight",
    "tpl.v.wacc.weight_debt": "debt weight",
    "tpl.v.wacc.total_capital": "total capital",
    "tpl.v.wacc.debt_value": "debt value",
    "tpl.v.wacc.after_tax_cost_of_debt": "after-tax cost of debt",
    "tpl.v.cost_of_equity.cost_of_equity": "cost of equity",
    "tpl.fm.balance_sheet.long_term_debt": "long-term debt",
}
_CAPITAL_STRUCTURE_TOLERANCE = 1e-6
_CAPITAL_STRUCTURE_HISTORICAL_DEBT_MATERIALITY = 1.0
_CAPITAL_STRUCTURE_MATERIAL_DEBT_CHANGE_RATIO = 10.0
_CAPITAL_STRUCTURE_IMMATERIAL_DEBT_STRESS_RATIO = 0.10
_SCENARIO_DIRECTION_OUTPUTS = {
    "adj_eps": ("tpl.fm.adjusted_earnings.adjusted_eps", "adjusted EPS"),
    "fcf_per_share": ("tpl.fm.cash_flow.free_cash_flow_per_share", "free cash flow per share"),
    "revenue_m": ("tpl.fm.income_statement.total_revenue", "revenue"),
    "op_margin_pct": ("tpl.fm.margins.operating_margin", "operating margin"),
    "adjusted_ebitda_m": ("tpl.fm.adjusted_earnings.adjusted_ebitda", "adjusted EBITDA"),
}
_SCENARIO_DIRECTION_EPS = 1e-9

class ModelQualityReadinessIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: str
    severity: ModelQualityReadinessSeverity
    domain: ModelQualityReadinessDomain
    detail: str
    item_id: str | None = None
    missing_periods: list[int] = Field(default_factory=list)
    related_item_ids: list[str] = Field(default_factory=list)
    unowned_periods: list[int] = Field(default_factory=list)
    carried_rate: float | None = None
    bound: float | None = None
    scenario_row_item_id: str | None = None
    fix: str | None = None

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
    guard_config: Any | None = None,
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
        "scenario_bridge": _compat("_scenario_bridge_quality_domain", _scenario_bridge_quality_domain)(
            model,
            projection_periods,
        ),
        "segment_basis": _compat("_segment_basis_quality_domain", _segment_basis_quality_domain)(
            model,
            segment_profile=segment_profile,
            values=values,
            projection_periods=projection_periods,
            guard_config=guard_config,
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
    has_capital_structure_rows = any(
        item_id in model._index for item_id in _CAPITAL_STRUCTURE_REQUIRED_ITEMS
    )
    required_items = [
        "tpl.v.current_valuation.stock_price",
        "tpl.v.current_valuation.shares_outstanding",
        "tpl.v.current_valuation.net_debt",
        "tpl.v.dcf.dcf_price",
    ]
    if has_capital_structure_rows:
        required_items.extend(_CAPITAL_STRUCTURE_REQUIRED_ITEMS)
    required_items = list(dict.fromkeys(required_items))
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

    if has_capital_structure_rows:
        issues.extend(
            _compat("_capital_structure_quality_issues", _capital_structure_quality_issues)(
                model,
                values,
                projection_periods,
            )
        )

    valuation_input_status = _compat("_valuation_input_status", _valuation_input_status)
    readiness_status, missing_inputs = valuation_input_status(valuation_input_readiness)
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    if readiness_status == "incomplete":
        severity: ModelQualityReadinessSeverity = "blocking" if missing_inputs else "warning"
        detail = (
            f"valuation_input_readiness is incomplete; missing inputs: {', '.join(missing_inputs)}"
            if missing_inputs
            else "valuation_input_readiness is incomplete; only placeholder/staleness flags may be present"
        )
        issues.append(
            issue_model(
                code="valuation_inputs_incomplete",
                severity=severity,
                domain="valuation",
                detail=detail,
                related_item_ids=list(missing_inputs),
            )
        )
    readiness_flags = _valuation_input_flags(valuation_input_readiness)
    if any(flag.get("code") == "terminal_assumptions_defaulted" for flag in readiness_flags):
        issues.append(
            issue_model(
                code="valuation_terminal_assumptions_defaulted",
                severity="warning",
                domain="valuation",
                detail=(
                    "DCF terminal growth / exit multiple assumptions use template defaults; "
                    "review company-specific calibration before treating the DCF as fully clean."
                ),
                related_item_ids=[
                    "tpl.v.dcf.terminal_growth_bull",
                    "tpl.v.dcf.terminal_growth_base",
                    "tpl.v.dcf.terminal_growth_bear",
                    "tpl.v.dcf.exit_multiple_bull",
                    "tpl.v.dcf.exit_multiple_base",
                    "tpl.v.dcf.exit_multiple_bear",
                ],
            )
        )

    if price_target_skip_reason:
        issues.append(
            issue_model(
                code="price_target_skip_reason",
                severity="blocking",
                domain="valuation",
                detail=f"PriceTarget derivation would be skipped: {price_target_skip_reason}",
            )
        )

    return _compat("_quality_domain", _quality_domain)(required_items=required_items, issues=issues)


def _scenario_bridge_quality_domain(
    model: FinancialModel,
    projection_periods: list[int],
) -> ModelQualityDomainReadiness:
    """Validate workbook scenario rows before valuation tools consume them."""

    required_items: list[str] = []
    owners = _compat("_scenario_bridge_owners", _scenario_bridge_owners)(model)
    if not owners:
        return _compat("_quality_domain", _quality_domain)(required_items=required_items, issues=[])

    required_items.extend(owner.id for owner in owners)
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    issues: list[ModelQualityReadinessIssue] = []

    bridge_readiness = _compat(
        "compute_model_scenario_bridge_readiness",
        compute_model_scenario_bridge_readiness,
    )(model)
    for bridge_issue in bridge_readiness.issues:
        related_item_ids = [
            item_id
            for item_id in (
                getattr(bridge_issue, "anchor_id", None),
                *list(getattr(bridge_issue, "related_item_ids", []) or []),
            )
            if item_id
        ]
        issues.append(
            issue_model(
                code=str(getattr(bridge_issue, "code", "scenario_bridge_issue")),
                severity=getattr(bridge_issue, "severity", "blocking"),
                domain="scenario_bridge",
                detail=str(getattr(bridge_issue, "detail", "scenario bridge issue")),
                item_id=getattr(bridge_issue, "owner_id", None),
                related_item_ids=list(dict.fromkeys(related_item_ids)),
            )
        )

    try:
        scenario_outputs = _compat("compute_scenario_outputs", compute_scenario_outputs)(model)
    except Exception as exc:
        issues.append(
            issue_model(
                code="scenario_output_direction_unavailable",
                severity="blocking",
                domain="scenario_bridge",
                detail=f"scenario output direction could not be computed: {exc}",
                related_item_ids=list(required_items),
            )
        )
    else:
        issues.extend(
            _compat("_scenario_output_direction_issues", _scenario_output_direction_issues)(
                scenario_outputs,
                projection_periods,
            )
        )

    return _compat("_quality_domain", _quality_domain)(required_items=required_items, issues=issues)


def _scenario_output_direction_issues(
    scenario_outputs: dict[str, dict[str, dict[int, float]]],
    projection_periods: list[int],
) -> list[ModelQualityReadinessIssue]:
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    issues: list[ModelQualityReadinessIssue] = []
    output_specs = _compat("_SCENARIO_DIRECTION_OUTPUTS", _SCENARIO_DIRECTION_OUTPUTS)
    eps = float(_compat("_SCENARIO_DIRECTION_EPS", _SCENARIO_DIRECTION_EPS))

    for field_name, (item_id, label) in output_specs.items():
        bull_values = scenario_outputs.get("bull", {}).get(field_name, {})
        base_values = scenario_outputs.get("base", {}).get(field_name, {})
        bear_values = scenario_outputs.get("bear", {}).get(field_name, {})
        if not isinstance(bull_values, dict) or not isinstance(base_values, dict) or not isinstance(bear_values, dict):
            continue
        wrong_periods: list[int] = []
        evaluated_periods: list[int] = []
        examples: list[str] = []
        for period in projection_periods:
            bull = bull_values.get(period, bull_values.get(str(period)))
            base = base_values.get(period, base_values.get(str(period)))
            bear = bear_values.get(period, bear_values.get(str(period)))
            if bull is None or base is None or bear is None:
                continue
            try:
                bull_f = float(bull)
                base_f = float(base)
                bear_f = float(bear)
            except (TypeError, ValueError):
                continue
            evaluated_periods.append(int(period))
            if bull_f < base_f - eps or bear_f > base_f + eps or bull_f < bear_f - eps:
                wrong_periods.append(int(period))
                if len(examples) < 3:
                    examples.append(f"{period}:bull={bull_f:g},base={base_f:g},bear={bear_f:g}")
        if not wrong_periods:
            continue
        severity: ModelQualityReadinessSeverity = "blocking"
        detail_suffix = ""
        if field_name == "fcf_per_share":
            # Early-period FCF/share inversions are the expected cash profile
            # of growth investment (capex and working capital scale with the
            # bull case's higher revenue), so FCF ordering is enforced at the
            # terminal period rather than in every period. EPS and the other
            # outputs stay strict in all periods.
            terminal_period = max(evaluated_periods)
            if terminal_period not in wrong_periods:
                severity = "warning"
                detail_suffix = (
                    f"; early-period inversion tolerated: terminal period "
                    f"{terminal_period} orders correctly (growth-investment cash profile)"
                )
        issues.append(
            issue_model(
                code="scenario_output_direction_mismatch",
                severity=severity,
                domain="scenario_bridge",
                detail=(
                    f"{label} scenario readback violates positive-output bull/base/bear ordering: "
                    + "; ".join(examples)
                    + detail_suffix
                ),
                item_id=item_id,
                missing_periods=wrong_periods,
            )
        )

    return issues


def _valuation_input_flags(
    valuation_input_readiness: ValuationInputReadiness | dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if valuation_input_readiness is None:
        return []
    if isinstance(valuation_input_readiness, dict):
        raw_flags = valuation_input_readiness.get("flags")
    else:
        raw_flags = getattr(valuation_input_readiness, "flags", None)
    flags: list[dict[str, Any]] = []
    for raw_flag in raw_flags or []:
        if isinstance(raw_flag, dict):
            flags.append(raw_flag)
            continue
        model_dump = getattr(raw_flag, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="json")
            if isinstance(dumped, dict):
                flags.append(dumped)
    return flags


def _finite_value(
    values: dict[str, dict[int, float]],
    item_id: str,
    period: int,
) -> float | None:
    value = values.get(item_id, {}).get(period)
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _latest_historical_value(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    item_id: str,
) -> float | None:
    historical_periods = sorted(_compat("_historical_periods", _historical_periods)(model))
    for period in reversed(historical_periods):
        value = _finite_value(values, item_id, int(period))
        if value is not None:
            return value
    return None


def _capital_structure_quality_issues(
    model: FinancialModel,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
) -> list[ModelQualityReadinessIssue]:
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    issues: list[ModelQualityReadinessIssue] = []
    for item_id in _CAPITAL_STRUCTURE_REQUIRED_ITEMS:
        issue = _compat("_quality_projection_issue", _quality_projection_issue)(
            model,
            values,
            projection_periods,
            item_id=item_id,
            domain="valuation",
            missing_item_code="capital_structure_item_missing",
            missing_values_code="capital_structure_projection_missing",
            label=_CAPITAL_STRUCTURE_LABELS[item_id],
            all_missing_severity="blocking",
            partial_missing_severity="blocking",
        )
        if issue is not None:
            issues.append(issue)
    if issues:
        return issues

    latest_historical_debt = _compat("_latest_historical_value", _latest_historical_value)(
        model,
        values,
        "tpl.fm.balance_sheet.long_term_debt",
    )
    for period in projection_periods:
        row_values = {
            item_id: _finite_value(values, item_id, int(period))
            for item_id in _CAPITAL_STRUCTURE_REQUIRED_ITEMS
        }
        invalid_ids = [item_id for item_id, value in row_values.items() if value is None]
        if invalid_ids:
            issues.append(
                issue_model(
                    code="capital_structure_value_invalid",
                    severity="blocking",
                    domain="valuation",
                    detail=(
                        f"capital structure rows have non-finite values in {period}: "
                        f"{', '.join(invalid_ids)}"
                    ),
                    missing_periods=[int(period)],
                    related_item_ids=invalid_ids,
                )
            )
            continue

        ltd = row_values["tpl.fm.balance_sheet.long_term_debt"]
        total_capital = row_values["tpl.v.wacc.total_capital"]
        weight_equity = row_values["tpl.v.wacc.weight_equity"]
        weight_debt = row_values["tpl.v.wacc.weight_debt"]
        wacc = row_values["tpl.v.wacc.wacc"]
        kd = row_values["tpl.v.wacc.after_tax_cost_of_debt"]
        ke = row_values["tpl.v.cost_of_equity.cost_of_equity"]
        assert ltd is not None
        assert total_capital is not None
        assert weight_equity is not None
        assert weight_debt is not None
        assert wacc is not None
        assert kd is not None
        assert ke is not None

        blocked_period = False
        if ltd < -_CAPITAL_STRUCTURE_TOLERANCE:
            blocked_period = True
            issues.append(
                issue_model(
                    code="capital_structure_long_term_debt_negative",
                    severity="blocking",
                    domain="valuation",
                    detail=f"projected long-term debt is negative in {period}: {ltd:g}",
                    item_id="tpl.fm.balance_sheet.long_term_debt",
                    missing_periods=[int(period)],
                    related_item_ids=["tpl.v.wacc.debt_value"],
                )
            )
        if total_capital <= _CAPITAL_STRUCTURE_TOLERANCE:
            blocked_period = True
            issues.append(
                issue_model(
                    code="capital_structure_total_capital_invalid",
                    severity="blocking",
                    domain="valuation",
                    detail=f"total capital must be positive in {period}: {total_capital:g}",
                    item_id="tpl.v.wacc.total_capital",
                    missing_periods=[int(period)],
                )
            )

        out_of_bounds_weights = [
            item_id
            for item_id, value in (
                ("tpl.v.wacc.weight_equity", weight_equity),
                ("tpl.v.wacc.weight_debt", weight_debt),
            )
            if value < -_CAPITAL_STRUCTURE_TOLERANCE or value > 1 + _CAPITAL_STRUCTURE_TOLERANCE
        ]
        if out_of_bounds_weights:
            blocked_period = True
            issues.append(
                issue_model(
                    code="capital_structure_weight_out_of_bounds",
                    severity="blocking",
                    domain="valuation",
                    detail=(
                        f"capital structure weights must be within [0, 1] in {period}: "
                        f"equity={weight_equity:g}, debt={weight_debt:g}"
                    ),
                    missing_periods=[int(period)],
                    related_item_ids=out_of_bounds_weights,
                )
            )
        if blocked_period:
            continue

        lower = min(kd, ke) - _CAPITAL_STRUCTURE_TOLERANCE
        upper = max(kd, ke) + _CAPITAL_STRUCTURE_TOLERANCE
        if wacc < lower or wacc > upper:
            issues.append(
                issue_model(
                    code="capital_structure_wacc_out_of_band",
                    severity="blocking",
                    domain="valuation",
                    detail=(
                        f"WACC must sit within the cost-of-capital band in {period}: "
                        f"kd={kd:g}, ke={ke:g}, wacc={wacc:g}"
                    ),
                    item_id="tpl.v.wacc.wacc",
                    missing_periods=[int(period)],
                    related_item_ids=[
                        "tpl.v.wacc.after_tax_cost_of_debt",
                        "tpl.v.cost_of_equity.cost_of_equity",
                    ],
                )
            )

        if latest_historical_debt is None:
            continue
        projected_debt_change = ltd - latest_historical_debt
        if abs(latest_historical_debt) >= _CAPITAL_STRUCTURE_HISTORICAL_DEBT_MATERIALITY:
            change_ratio = abs(projected_debt_change) / abs(latest_historical_debt)
            if change_ratio > _CAPITAL_STRUCTURE_MATERIAL_DEBT_CHANGE_RATIO:
                issues.append(
                    issue_model(
                        code="capital_structure_debt_magnitude_blocking",
                        severity="blocking",
                        domain="valuation",
                        detail=(
                            f"projected long-term debt change is too large versus latest "
                            f"historical debt in {period}: change={projected_debt_change:g}, "
                            f"latest_historical_debt={latest_historical_debt:g}"
                        ),
                        item_id="tpl.fm.balance_sheet.long_term_debt",
                        missing_periods=[int(period)],
                    )
                )
            continue

        market_cap = _finite_value(values, "tpl.v.current_valuation.market_cap", int(period))
        comparison_base = max(
            abs(total_capital),
            abs(market_cap) if market_cap is not None else 0.0,
            1.0,
        )
        stress_ratio = max(abs(ltd), abs(projected_debt_change)) / comparison_base
        if stress_ratio > _CAPITAL_STRUCTURE_IMMATERIAL_DEBT_STRESS_RATIO:
            issues.append(
                issue_model(
                    code="capital_structure_debt_magnitude_warning",
                    severity="warning",
                    domain="valuation",
                    detail=(
                        f"projected debt/change is large versus capital base in {period} "
                        f"with immaterial latest historical debt: debt={ltd:g}, "
                        f"change={projected_debt_change:g}, total_capital={total_capital:g}"
                    ),
                    item_id="tpl.fm.balance_sheet.long_term_debt",
                    missing_periods=[int(period)],
                    related_item_ids=["tpl.v.wacc.total_capital", "tpl.v.current_valuation.market_cap"],
                )
            )

    return issues


def _segment_basis_quality_domain(
    model: FinancialModel,
    *,
    segment_profile: "SegmentProfile | None",
    values: dict[str, dict[int, float]] | None = None,
    projection_periods: list[int] | None = None,
    guard_config: Any | None = None,
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
    if values is not None and projection_periods is not None:
        issues.extend(
            _growth_carry_forward_quality_issue(finding)
            for finding in growth_carry_forward_findings(
                model,
                values=values,
                projection_periods=projection_periods,
                guard_config=guard_config,
            )
        )
    return _compat("_quality_domain", _quality_domain)(required_items=[], issues=issues)


def _growth_carry_forward_quality_issue(
    finding: GrowthCarryForwardFinding,
) -> ModelQualityReadinessIssue:
    issue_model = _compat("ModelQualityReadinessIssue", ModelQualityReadinessIssue)
    if finding.scenario_row_item_id is not None:
        detail = (
            f"segment {finding.segment_id!r} reads scenario row "
            f"{finding.scenario_row_item_id!r} via offset_scenario, but "
            f"{len(finding.unowned_periods)} projection period(s) still carry forward "
            f"{finding.effective_growth_item_id!r} at {finding.carried_rate:g}, above the "
            f"{finding.bound:g} guardrail"
        )
    else:
        detail = (
            f"segment {finding.segment_id!r} has partially authored revenue-growth assumptions, "
            f"but {len(finding.unowned_periods)} projection period(s) still carry forward "
            f"{finding.effective_growth_item_id!r} at {finding.carried_rate:g}, above the "
            f"{finding.bound:g} guardrail"
        )
    return issue_model(
        code="bm_driver_projection_seed_missing",
        severity="blocking",
        domain="segment_basis",
        detail=detail,
        item_id=finding.driver_item_id,
        missing_periods=list(finding.unowned_periods),
        related_item_ids=[finding.effective_growth_item_id],
        unowned_periods=list(finding.unowned_periods),
        carried_rate=finding.carried_rate,
        bound=finding.bound,
        scenario_row_item_id=finding.scenario_row_item_id,
        fix=finding.fix,
    )


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
        return "share count, working capital, valuation, scenario bridge, and segment-basis quality checks are ready"
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
    "_SCENARIO_DIRECTION_EPS": _SCENARIO_DIRECTION_EPS,
    "_SCENARIO_DIRECTION_OUTPUTS": _SCENARIO_DIRECTION_OUTPUTS,
    "_computed_values": _computed_values,
    "_capital_structure_quality_issues": _capital_structure_quality_issues,
    "_dependency_issue": _dependency_issue,
    "_has_any_value": _has_any_value,
    "_historical_periods": _historical_periods,
    "_inventory_material": _inventory_material,
    "_is_present": _is_present,
    "_latest_historical_value": _latest_historical_value,
    "_missing_periods": _missing_periods,
    "_model_quality_summary": _model_quality_summary,
    "_normalize_computed_values": _normalize_computed_values,
    "_projection_periods": _projection_periods,
    "_quality_any_projection_issue": _quality_any_projection_issue,
    "_quality_domain": _quality_domain,
    "_quality_projection_issue": _quality_projection_issue,
    "_segment_basis_quality_domain": _segment_basis_quality_domain,
    "_segment_profile_basis_issues": None,
    "_scenario_bridge_owners": _scenario_bridge_owners,
    "_scenario_bridge_quality_domain": _scenario_bridge_quality_domain,
    "_scenario_output_direction_issues": _scenario_output_direction_issues,
    "_share_count_quality_domain": _share_count_quality_domain,
    "_upstream_item_ids": _upstream_item_ids,
    "_valuation_input_flags": _valuation_input_flags,
    "_valuation_input_status": _valuation_input_status,
    "_valuation_quality_domain": _valuation_quality_domain,
    "_working_capital_quality_domain": _working_capital_quality_domain,
    "compute_model_scenario_bridge_readiness": compute_model_scenario_bridge_readiness,
    "compute_model_quality_readiness": compute_model_quality_readiness,
    "compute_scenario_outputs": compute_scenario_outputs,
}


__all__ = [
    "ModelQualityDomain",
    "ModelQualityDomainReadiness",
    "ModelQualityReadiness",
    "ModelQualityReadinessDomain",
    "ModelQualityReadinessIssue",
    "ModelQualityReadinessSeverity",
    "ModelQualityReadinessStatus",
    "_capital_structure_quality_issues",
    "_dependency_issue",
    "_inventory_material",
    "_model_quality_summary",
    "_quality_any_projection_issue",
    "_quality_domain",
    "_quality_projection_issue",
    "_segment_basis_quality_domain",
    "_scenario_bridge_quality_domain",
    "_scenario_output_direction_issues",
    "_share_count_quality_domain",
    "_upstream_item_ids",
    "_valuation_input_flags",
    "_valuation_quality_domain",
    "_working_capital_quality_domain",
    "compute_model_quality_readiness",
]
