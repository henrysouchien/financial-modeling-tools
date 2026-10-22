"""Model-quality readiness contracts and helpers."""

from __future__ import annotations

from collections.abc import Mapping

import math
from typing import Any, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from .dependency_graph import DependencyGraph
from .renderer_columns import _fixed_cell_anchor_period, item_owns_cell
from .growth_carry_forward_guard import (
    GrowthCarryForwardFinding,
    growth_carry_forward_findings,
)
from .handle import ComputedValues
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
ModelQualityReadinessDomain = Literal[
    "share_count",
    "working_capital",
    "valuation",
    "scenario_bridge",
    "segment_basis",
    "projected_financial_integrity",
]
ModelQualityDomain = ModelQualityReadinessDomain
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
_ZERO_DEBT_DIRECT_WACC_TOLERANCE = 1e-4
_CAPITAL_STRUCTURE_HISTORICAL_DEBT_MATERIALITY = 1.0
_CAPITAL_STRUCTURE_HISTORICAL_DEBT_SHARE_MATERIALITY = 0.01
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




def compute_model_quality_readiness(
    model: FinancialModel,
    *,
    computed_values: ComputedValues | None = None,
    valuation_input_readiness: ValuationInputReadiness | dict[str, Any] | None = None,
    price_target_skip_reason: str | None = None,
    segment_profile: "SegmentProfile | None" = None,
    guard_config: Any | None = None,
) -> ModelQualityReadiness:
    """Return economic model-quality readiness for the remaining owner domains."""

    model.build_index()
    projection_periods = _projection_periods(model)
    if not projection_periods:
        return ModelQualityReadiness(
            status="unknown",
            projection_periods=[],
            summary="model has no projection periods",
        )

    values = _normalize_computed_values(
        computed_values if computed_values is not None else _computed_values(model)
    )
    domains: dict[ModelQualityReadinessDomain, ModelQualityDomainReadiness] = {
        "share_count": _share_count_quality_domain(
            model,
            values,
            projection_periods,
        ),
        "working_capital": _working_capital_quality_domain(
            model,
            values,
            projection_periods,
        ),
        "valuation": _valuation_quality_domain(
            model,
            values,
            projection_periods,
            valuation_input_readiness=valuation_input_readiness,
            price_target_skip_reason=price_target_skip_reason,
        ),
        "segment_basis": _segment_basis_quality_domain(
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

    return ModelQualityReadiness(
        status=status,
        projection_periods=list(projection_periods),
        domains=domains,
        issues=issues,
        summary=_model_quality_summary(status, domains),
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
    domain_model = ModelQualityDomainReadiness
    return domain_model(
        status=status,
        required_items=required_items,
        missing_periods=missing_periods,
        issues=issues,
    )






def _single_anchor_period(model: FinancialModel, item_id: str) -> int | None:
    """The one period a fixed-cell (``column``) row renders, or None for a per-period row."""

    item = model._index.get(item_id)
    if item is None or item.column is None:
        return None
    return _fixed_cell_anchor_period(model, item)


def _rendered_periods(
    model: FinancialModel,
    item_id: str,
    periods: list[int],
) -> list[int]:
    """The ``periods`` a row actually renders a cell in (``item_owns_cell``).

    A period the row does not own has no cell, so it can be neither missing nor ``#N/A``.
    """

    item = model._index.get(item_id)
    if item is None:
        return list(periods)
    return [period for period in periods if item_owns_cell(model, item, period)]


def _quality_projection_issue(
    model: FinancialModel,
    values: ComputedValues,
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
    issue_model = ModelQualityReadinessIssue
    if item_id not in model._index:
        return issue_model(
            code=missing_item_code,
            severity="blocking",
            domain=domain,
            detail=f"{label} row {item_id!r} is missing from the model",
            item_id=item_id,
            missing_periods=list(periods),
        )
    rendered = _rendered_periods(model, item_id, periods)
    missing = _missing_periods(values.get(item_id, {}), rendered)
    if not missing:
        return None
    severity = all_missing_severity if missing == rendered else partial_missing_severity
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
    values: ComputedValues,
    periods: list[int],
    *,
    item_id: str,
    domain: ModelQualityReadinessDomain,
    missing_item_code: str,
    missing_values_code: str,
    label: str,
) -> ModelQualityReadinessIssue | None:
    issue_model = ModelQualityReadinessIssue
    if item_id not in model._index:
        return issue_model(
            code=missing_item_code,
            severity="warning",
            domain=domain,
            detail=f"{label} row {item_id!r} is missing from the model",
            item_id=item_id,
            missing_periods=list(periods),
        )
    rendered = _rendered_periods(model, item_id, periods)
    if _has_any_value(values.get(item_id, {}), rendered):
        return None
    return issue_model(
        code=missing_values_code,
        severity="warning",
        domain=domain,
        detail=f"{label} row {item_id!r} has no projected value",
        item_id=item_id,
        missing_periods=rendered,
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
    upstream = _upstream_item_ids(model, item_id)
    if expected_upstream_id in upstream:
        return None
    issue_model = ModelQualityReadinessIssue
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
    values: ComputedValues,
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
        issue = _quality_projection_issue(
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

    fy2_issue = _quality_any_projection_issue(
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
        issue = _dependency_issue(
            model,
            item_id=item_id,
            expected_upstream_id=share_item_id,
            domain="share_count",
            code="share_count_dependency_missing",
            label=label,
        )
        if issue is not None:
            issues.append(issue)

    return _quality_domain(required_items=required_items, issues=issues)


def _working_capital_quality_domain(
    model: FinancialModel,
    values: ComputedValues,
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
        issue = _quality_projection_issue(
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

    if _inventory_material(model, values):
        for item_id, label in (
            ("tpl.fm.balance_sheet.current_asset_2", "inventory balance"),
            ("tpl.fm.cash_flow.current_asset_2", "change in inventory"),
        ):
            issue = _quality_projection_issue(
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

    return _quality_domain(required_items=required_items, issues=issues)


def _valuation_quality_domain(
    model: FinancialModel,
    values: ComputedValues,
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
        required_items.extend(
            _capital_structure_required_items(model, values, projection_periods)
        )
    required_items = list(dict.fromkeys(required_items))
    issues: list[ModelQualityReadinessIssue] = []
    for item_id, label in (
        ("tpl.v.current_valuation.shares_outstanding", "valuation shares"),
        ("tpl.v.current_valuation.net_debt", "valuation net debt"),
        ("tpl.v.dcf.dcf_price", "DCF price"),
    ):
        issue = _quality_projection_issue(
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
            _capital_structure_quality_issues(model, values, projection_periods)
        )

    valuation_input_status = _valuation_input_status
    readiness_status, missing_inputs = valuation_input_status(valuation_input_readiness)
    issue_model = ModelQualityReadinessIssue
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

    if price_target_skip_reason:
        issues.append(
            issue_model(
                code="price_target_skip_reason",
                severity="blocking",
                domain="valuation",
                detail=f"PriceTarget derivation would be skipped: {price_target_skip_reason}",
            )
        )

    return _quality_domain(required_items=required_items, issues=issues)




_ScenarioPeriodValues = (
    Mapping[int, float]
    | Mapping[str, float]
    | Mapping[int | str, float]
)


def _scenario_period_value(
    values: _ScenarioPeriodValues,
    period: int,
) -> float | None:
    return next(
        (
            value
            for candidate in (period, str(period))
            for key, value in values.items()
            if key == candidate
        ),
        None,
    )


def _scenario_output_direction_issues(
    scenario_outputs: Mapping[str, Mapping[str, _ScenarioPeriodValues]],
    projection_periods: list[int],
) -> list[ModelQualityReadinessIssue]:
    issue_model = ModelQualityReadinessIssue
    issues: list[ModelQualityReadinessIssue] = []
    output_specs = _SCENARIO_DIRECTION_OUTPUTS
    eps = float(_SCENARIO_DIRECTION_EPS)

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
            bull = _scenario_period_value(bull_values, period)
            base = _scenario_period_value(base_values, period)
            bear = _scenario_period_value(bear_values, period)
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


def _finite_value(
    values: ComputedValues,
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
    values: ComputedValues,
    item_id: str,
) -> float | None:
    historical_periods = sorted(_historical_periods(model))
    for period in reversed(historical_periods):
        value = _finite_value(values, item_id, int(period))
        if value is not None:
            return value
    return None


def _projected_debt_is_zero(
    values: ComputedValues,
    projection_periods: list[int],
) -> bool:
    """True when projected long-term debt is zero and every stated debt value is zero.

    ``tpl.v.wacc.debt_value`` is a first-period anchor, not a horizon series. A
    year it does not state is not evidence of debt.
    """

    if not projection_periods:
        return False
    for period in projection_periods:
        long_term = _finite_value(values, "tpl.fm.balance_sheet.long_term_debt", int(period))
        if long_term is None or abs(long_term) > _CAPITAL_STRUCTURE_TOLERANCE:
            return False
    stated = [
        value
        for period in projection_periods
        if (value := _finite_value(values, "tpl.v.wacc.debt_value", int(period))) is not None
    ]
    return bool(stated) and all(abs(value) <= _CAPITAL_STRUCTURE_TOLERANCE for value in stated)


#: The rows that read the debt capital ``retire_unused_cost_of_debt`` retires
#: (``schema/build_valuation_inputs.py``) when the filer carries no debt: with the capital gone
#: the structure is equity-only and these are not required.
_RETIRED_DEBT_READERS = (
    "tpl.v.wacc.weight_debt",
    "tpl.v.wacc.after_tax_cost_of_debt",
)


def _debt_capital_retired(model: FinancialModel, projection_periods: list[int]) -> bool:
    """The model carries the debt-capital row but it holds no cell in any projection period:
    the build retired it because the filer carries no debt. A model without the row at all is
    not that case and is judged as before (``capital_structure_item_missing``)."""

    from .build_valuation_inputs import _DEBT_CAPITAL_ROWS

    rows = [model._index.get(item_id) for item_id in _DEBT_CAPITAL_ROWS]
    return all(row is not None for row in rows) and not any(
        item_owns_cell(model, row, int(period), gaps={})
        for row in rows
        if row is not None
        for period in projection_periods
    )


def _capital_structure_required_items(
    model: FinancialModel,
    values: ComputedValues,
    projection_periods: list[int],
) -> list[str]:
    from .build_valuation_inputs import _DEBT_CAPITAL_ROWS

    required = list(_CAPITAL_STRUCTURE_REQUIRED_ITEMS)
    if _debt_capital_retired(model, projection_periods):
        gone = set(_DEBT_CAPITAL_ROWS) | set(_RETIRED_DEBT_READERS)
        return [item_id for item_id in required if item_id not in gone]
    if _projected_debt_is_zero(values, projection_periods):
        required.remove("tpl.v.wacc.after_tax_cost_of_debt")
    return required


def _capital_structure_quality_issues(
    model: FinancialModel,
    values: ComputedValues,
    projection_periods: list[int],
) -> list[ModelQualityReadinessIssue]:
    issue_model = ModelQualityReadinessIssue
    issues: list[ModelQualityReadinessIssue] = []
    required_items = _capital_structure_required_items(model, values, projection_periods)
    debt_retired = _debt_capital_retired(model, projection_periods)
    for item_id in required_items:
        issue = _quality_projection_issue(
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

    latest_historical_debt = _latest_historical_value(
        model,
        values,
        "tpl.fm.balance_sheet.long_term_debt",
    )
    prior_debt = latest_historical_debt
    for period in projection_periods:
        # A single-anchor row holds one value for the whole horizon: the rows that
        # read it read that cell. A finding about the row is reported only in the
        # period the row renders (``renders``); a period it does not render has no
        # cell to be wrong.
        def read(item_id: str, period: int = period) -> float | None:
            anchor = _single_anchor_period(model, item_id)
            return _finite_value(values, item_id, int(period if anchor is None else anchor))

        def renders(item_id: str, period: int = period) -> bool:
            return period in _rendered_periods(model, item_id, [period])

        row_values = {item_id: read(item_id) for item_id in required_items}
        if debt_retired:
            # Equity-only capital structure: the retired debt capital and its weight are
            # zero. The projected long-term debt row is still the model's own computed
            # value and is judged as such below (a cited absence there is no cell).
            row_values.update({"tpl.v.wacc.debt_value": 0.0, "tpl.v.wacc.weight_debt": 0.0})
            if row_values.get("tpl.fm.balance_sheet.long_term_debt") is None and not renders(
                "tpl.fm.balance_sheet.long_term_debt"
            ):
                row_values.pop("tpl.fm.balance_sheet.long_term_debt", None)
        invalid_ids = [item_id for item_id, value in row_values.items() if value is None]
        if invalid_ids:
            reported_invalid = [item_id for item_id in invalid_ids if renders(item_id)]
            if reported_invalid:
                issues.append(
                    issue_model(
                        code="capital_structure_value_invalid",
                        severity="blocking",
                        domain="valuation",
                        detail=(
                            f"capital structure rows have non-finite values in {period}: "
                            f"{', '.join(reported_invalid)}"
                        ),
                        missing_periods=[int(period)],
                        related_item_ids=reported_invalid,
                    )
                )
            continue

        ltd = row_values.get("tpl.fm.balance_sheet.long_term_debt")
        total_capital = row_values["tpl.v.wacc.total_capital"]
        weight_equity = row_values["tpl.v.wacc.weight_equity"]
        weight_debt = row_values["tpl.v.wacc.weight_debt"]
        wacc = row_values["tpl.v.wacc.wacc"]
        kd = row_values.get("tpl.v.wacc.after_tax_cost_of_debt")
        ke = row_values["tpl.v.cost_of_equity.cost_of_equity"]
        assert total_capital is not None
        assert weight_equity is not None
        assert weight_debt is not None
        assert wacc is not None
        assert ke is not None

        blocked_period = False
        if ltd is not None and ltd < -_CAPITAL_STRUCTURE_TOLERANCE:
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
            if renders("tpl.v.wacc.total_capital"):
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
        reported_weights = [
            item_id for item_id in out_of_bounds_weights if renders(item_id)
        ]
        if reported_weights:
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
                    related_item_ids=reported_weights,
                )
            )
        if blocked_period:
            prior_debt = ltd
            continue

        if kd is None:
            lower = ke - _ZERO_DEBT_DIRECT_WACC_TOLERANCE
            upper = ke + _ZERO_DEBT_DIRECT_WACC_TOLERANCE
        else:
            lower = min(kd, ke) - _CAPITAL_STRUCTURE_TOLERANCE
            upper = max(kd, ke) + _CAPITAL_STRUCTURE_TOLERANCE
        if (wacc < lower or wacc > upper) and renders("tpl.v.wacc.wacc"):
            issues.append(
                issue_model(
                    code="capital_structure_wacc_out_of_band",
                    severity="blocking",
                    domain="valuation",
                    detail=(
                        f"WACC must sit within the cost-of-capital band in {period}: "
                        f"kd={'not applicable' if kd is None else f'{kd:g}'}, "
                        f"ke={ke:g}, wacc={wacc:g}"
                    ),
                    item_id="tpl.v.wacc.wacc",
                    missing_periods=[int(period)],
                    related_item_ids=(
                        ["tpl.v.cost_of_equity.cost_of_equity"]
                        if kd is None
                        else [
                            "tpl.v.wacc.after_tax_cost_of_debt",
                            "tpl.v.cost_of_equity.cost_of_equity",
                        ]
                    ),
                )
            )

        if prior_debt is None or ltd is None:
            prior_debt = ltd
            continue
        projected_debt_change = ltd - prior_debt
        market_cap = read("tpl.v.current_valuation.market_cap")
        comparison_base = max(
            abs(total_capital),
            abs(market_cap) if market_cap is not None else 0.0,
            1.0,
        )
        prior_debt_share = abs(prior_debt) / comparison_base
        prior_debt_is_material = (
            abs(prior_debt) >= _CAPITAL_STRUCTURE_HISTORICAL_DEBT_MATERIALITY
            and prior_debt_share
            >= _CAPITAL_STRUCTURE_HISTORICAL_DEBT_SHARE_MATERIALITY
        )
        if prior_debt_is_material:
            change_ratio = abs(projected_debt_change) / abs(prior_debt)
            if change_ratio > _CAPITAL_STRUCTURE_MATERIAL_DEBT_CHANGE_RATIO:
                issues.append(
                    issue_model(
                        code="capital_structure_debt_magnitude_blocking",
                        severity="blocking",
                        domain="valuation",
                        detail=(
                            f"projected long-term debt change is too large versus prior-period "
                            f"debt in {period}: change={projected_debt_change:g}, "
                            f"prior_debt={prior_debt:g}"
                        ),
                        item_id="tpl.fm.balance_sheet.long_term_debt",
                        missing_periods=[int(period)],
                    )
                )
        else:
            stress_ratio = max(abs(ltd), abs(projected_debt_change)) / comparison_base
            if stress_ratio > _CAPITAL_STRUCTURE_IMMATERIAL_DEBT_STRESS_RATIO:
                issues.append(
                    issue_model(
                        code="capital_structure_debt_magnitude_warning",
                        severity="warning",
                        domain="valuation",
                        detail=(
                            f"projected debt/change is large versus capital base in {period} "
                            f"with immaterial prior-period debt: debt={ltd:g}, "
                            f"change={projected_debt_change:g}, total_capital={total_capital:g}"
                        ),
                        item_id="tpl.fm.balance_sheet.long_term_debt",
                        missing_periods=[int(period)],
                        related_item_ids=[
                            "tpl.v.wacc.total_capital",
                            "tpl.v.current_valuation.market_cap",
                        ],
                    )
                )
        prior_debt = ltd

    return issues


def _segment_basis_quality_domain(
    model: FinancialModel,
    *,
    segment_profile: "SegmentProfile | None",
    values: ComputedValues | None = None,
    projection_periods: list[int] | None = None,
    guard_config: Any | None = None,
) -> ModelQualityDomainReadiness:
    issues: list[ModelQualityReadinessIssue] = []
    if segment_profile is not None:
        from .model_readiness import _segment_profile_basis_issues

        for source_issue in _segment_profile_basis_issues(
            model,
            segment_profile=segment_profile,
        ):
            issues.append(
                ModelQualityReadinessIssue(
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
    return _quality_domain(required_items=[], issues=issues)


def _growth_carry_forward_quality_issue(
    finding: GrowthCarryForwardFinding,
) -> ModelQualityReadinessIssue:
    issue_model = ModelQualityReadinessIssue
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
    values: ComputedValues,
) -> bool:
    inventory_values = values.get("tpl.fm.balance_sheet.current_asset_2", {})
    if not inventory_values:
        return False
    historical_periods = _historical_periods(model)
    is_present = _is_present
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
    "_scenario_output_direction_issues",
    "_share_count_quality_domain",
    "_upstream_item_ids",
    "_valuation_quality_domain",
    "_working_capital_quality_domain",
    "compute_model_quality_readiness",
]
