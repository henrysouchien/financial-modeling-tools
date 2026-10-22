"""Build diagnostics for populated financial models."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Optional

from .build_diagnostic_sections import (
    BS_SECTIONS,
    CF_SECTIONS,
    IS_SECTIONS,
    ParentCandidate as ParentCandidate,
    SectionMember,
)
from .build_diagnostic_values import (
    VALID_SEVERITIES as VALID_SEVERITIES,
    SEVERITY_ORDER as SEVERITY_ORDER,
    _is_synthetic_override as _is_synthetic_override,
    _iter_items as _iter_items,
    _section_lookup as _section_lookup,
    _historical_years as _historical_years,
    _iso_utc_now as _iso_utc_now,
    _collect_severities as _collect_severities,
    _values_are_resolved as _values_are_resolved,
    _max_tolerance as _max_tolerance,
    _percent as _percent,
    _ratio as _ratio,
    _constant_override_value as _constant_override_value,
    _has_real_value as _has_real_value,
    _observed_value as _observed_value,
    _historical_spec as _historical_spec,
    _evaluate_formula_spec as _evaluate_formula_spec,
    _evaluate_expr as _evaluate_expr,
    _extract_ref_ids as _extract_ref_ids,
    _extract_ref_targets as _extract_ref_targets,
    _missing_ref_ids as _missing_ref_ids,
    _build_reverse_dependency_graph as _build_reverse_dependency_graph,
    _transitive_downstream_ids as _transitive_downstream_ids,
    _required_role_item_map as _required_role_item_map,
    _role_has_coverage as _role_has_coverage,
    _role_has_full_coverage as _role_has_full_coverage,
    _item_has_coverage_for_year as _item_has_coverage_for_year,
    _intentional_blank_detail as _intentional_blank_detail,
    _projection_only_historical_blank_detail as _projection_only_historical_blank_detail,
    _item_has_path as _item_has_path,
    _blocking_ref_ids as _blocking_ref_ids,
)
from .build_diagnostic_types import (
    DiagnosticTolerances as DiagnosticTolerances,
    BSBalanceCheck as BSBalanceCheck,
    BSSublineCheck as BSSublineCheck,
    ISSubtotalCheck as ISSubtotalCheck,
    CFReconciliationCheck as CFReconciliationCheck,
    RevenueConsolidationCheck as RevenueConsolidationCheck,
    CoverageSummary as CoverageSummary,
    SyntheticZeroCheck as SyntheticZeroCheck,
    HistoricalPathCoverageCheck as HistoricalPathCoverageCheck,
    CrossSourceValidationCheck as CrossSourceValidationCheck,
    DiagnosticReport as DiagnosticReport,
    require_coverage_provider_id as require_coverage_provider_id,
    validate_diagnostic_provider_keys as validate_diagnostic_provider_keys,
)
from .build_diagnostic_balance_sheet import (
    _check_bs_balance as _check_bs_balance,
    _check_bs_subline_reconciliation as _check_bs_subline_reconciliation,
    _check_bs_subline_reconciliation_legacy as _check_bs_subline_reconciliation_legacy,
    _effective_section_members as _effective_section_members,
    _resolve_section_total as _resolve_section_total,
)
from .build_diagnostic_validation import (
    _check_cross_source_validation as _check_cross_source_validation,
    _validation_buffer_value as _validation_buffer_value,
    _year_lookup as _year_lookup,
    _validation_served_source as _validation_served_source,
)
from .models import (
    ItemType,
    LineItem,
    FinancialModel,
)
from .provider_port.registry import (
    EDGAR_PROVIDER_ID,
    FMP_PROVIDER_ID,
    ProviderId,
)
from .required_role_catalog import RequiredRoleCatalog, load_required_role_catalog
from .validation_input import ValidationInput

if TYPE_CHECKING:
    from .build_types import PopulateStats
    from .provider_port.registry import ProviderRegistry
    from .segment_profile_helpers import SegmentProfile


logger = logging.getLogger(__name__)

DIAGNOSTIC_VERSION = 3
VALID_KINDS = {
    "wrong_tag_suspected",
    "insufficient_inputs",
    "duplicate_rows",
    "synthetic_zero_propagation",
    "covered_by_role",
    "optional_unreported",
    "projection_only",
    "consolidation_mismatch",
    "required_column_absent_from_pin",
    "operating_cash_unexplained",
}


def run_build_diagnostic(
    model: FinancialModel,
    *,
    ticker: str,
    fy: int,
    stats: PopulateStats,
    role_catalog: RequiredRoleCatalog | None = None,
    derivable_items: dict[str, set[int]] | None = None,
    tolerances: DiagnosticTolerances | None = None,
    validation_input: ValidationInput | None = None,
    provider_registry: "ProviderRegistry | None" = None,
    segment_profile: "SegmentProfile | None" = None,
    generated_at: str | None = None,
) -> DiagnosticReport:
    model.build_index()
    historical_years = _historical_years(model)
    tolerances = tolerances or DiagnosticTolerances()
    role_catalog = role_catalog or load_required_role_catalog()
    reverse_graph = _build_reverse_dependency_graph(model)
    synthetic_zero = _check_synthetic_zero_propagation(
        model,
        historical_years=historical_years,
        role_catalog=role_catalog,
        reverse_graph=reverse_graph,
    )

    return DiagnosticReport(
        ticker=str(ticker),
        fiscal_year_end=str(
            model.company.fiscal_year_end or model.time_structure.fiscal_year_end or ""
        ),
        most_recent_fy=int(fy),
        diagnostic_version=DIAGNOSTIC_VERSION,
        generated_at=generated_at or _iso_utc_now(),
        bs_balance=_check_bs_balance(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        bs_subline_reconciliation=_check_bs_subline_reconciliation(
            model,
            historical_years=historical_years,
            role_catalog=role_catalog,
            tolerances=tolerances,
        ),
        is_subtotal_integrity=_check_is_subtotal_integrity(
            model,
            role_catalog=role_catalog,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        cf_reconciliation=_check_cf_reconciliation(
            model,
            role_catalog=role_catalog,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        coverage_summary=_check_coverage_summary(
            model,
            ticker=ticker,
            historical_years=historical_years,
            role_catalog=role_catalog,
            stats=stats,
            derivable_items=derivable_items or {},
            synthetic_zero_check=synthetic_zero,
            provider_registry=provider_registry,
        ),
        synthetic_zero_propagation=synthetic_zero,
        historical_path_coverage=_check_historical_path_coverage(
            model,
            ticker=ticker,
            historical_years=historical_years,
            role_catalog=role_catalog,
            derivable_items=derivable_items or {},
        ),
        cross_source_validation=_check_cross_source_validation(
            validation_input,
            role_catalog=role_catalog,
            tolerances=tolerances,
            provider_registry=provider_registry,
        ),
        revenue_consolidation=_check_revenue_consolidation_reconciliation(
            model,
            historical_years=historical_years,
            segment_profile=segment_profile,
            tolerances=tolerances,
        ),
    )


_TOTAL_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"


def _check_revenue_consolidation_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    segment_profile: "SegmentProfile | None",
    tolerances: DiagnosticTolerances,
) -> RevenueConsolidationCheck:
    """Reconcile the built consolidated top-line revenue against the EDGAR
    consolidated entity revenue (``segment_profile.total_revenue_check``).

    Only runs for segment/BM builds that carry an independent consolidated
    ground truth — the only shape that can double-count. Non-segment / template
    builds (no ``total_revenue_check``), years without a ground-truth entry, and
    an unresolvable total-revenue line all degrade to no-entry / per-year ``gap``,
    never a false ``inconsistency``.
    """

    check = RevenueConsolidationCheck()
    truth_by_year = (
        getattr(segment_profile, "total_revenue_check", None)
        if segment_profile is not None
        else None
    )
    if not truth_by_year:
        return check
    try:
        total_item = model.get_item(_TOTAL_REVENUE_ITEM_ID)
    except KeyError:
        return check

    value_memo: dict[tuple[str, int], Optional[float]] = {}
    for year in historical_years:
        if int(year) not in truth_by_year:
            # No independent consolidated figure for this year → out of scope.
            continue
        reported = truth_by_year.get(int(year))
        built = _observed_value(model, total_item, year, value_memo)
        entry: dict[str, Any] = {
            "year": year,
            "built_total_revenue": (float(built) if built is not None else None),
            "reported_consolidated_revenue": (
                float(reported) if reported is not None else None
            ),
            "delta": None,
            "delta_pct": None,
            "severity": "ok",
            "kind": None,
        }
        if built is None or reported is None:
            entry["severity"] = "gap"
            entry["kind"] = "insufficient_inputs"
        else:
            built_value = float(built)
            reported_value = float(reported)
            delta = built_value - reported_value
            base = max(abs(built_value), abs(reported_value))
            tol = _max_tolerance(
                base,
                tolerances.revenue_consolidation_abs_m,
                tolerances.revenue_consolidation_pct,
            )
            entry["delta"] = delta
            entry["delta_pct"] = _percent(delta, base)
            if abs(delta) > tol:
                entry["severity"] = "inconsistency"
                entry["kind"] = "consolidation_mismatch"
        check.by_year[str(year)] = entry
    return check


def _check_is_subtotal_integrity(
    model: FinancialModel,
    *,
    role_catalog: RequiredRoleCatalog,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
) -> ISSubtotalCheck:
    results: list[dict[str, Any]] = []
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for subtotal_name, definition in IS_SECTIONS.items():
        item = model.get_item(definition["subtotal_item_id"])
        for year in historical_years:
            computed = _evaluate_formula_spec(
                model,
                item.historical,
                year,
                value_memo,
                current_item_id=item.id,
            )
            reported = _observed_value(model, item, year, value_memo)
            entry: dict[str, Any] = {
                "subtotal": subtotal_name,
                "template_item": item.id,
                "year": year,
                "computed": computed,
                "reported": reported,
                "delta": None,
                "delta_pct": None,
                "severity": "ok",
                "kind": None,
            }
            if computed is None or reported is None:
                entry["severity"] = "gap"
                entry["kind"] = "insufficient_inputs"
                entry["missing_inputs"] = _missing_ref_ids(
                    model, item, year, value_memo
                )
            else:
                delta = float(reported) - float(computed)
                base = max(abs(float(reported)), abs(float(computed)))
                tol = _max_tolerance(
                    base, tolerances.is_subtotal_abs_m, tolerances.is_subtotal_pct
                )
                entry["delta"] = delta
                entry["delta_pct"] = _percent(delta, base)
                if abs(delta) > tol:
                    entry["severity"] = "inconsistency"
                    entry["kind"] = "wrong_tag_suspected"
                    entry["inputs"] = _collect_subtotal_input_roles(
                        model,
                        item,
                        year,
                        admitted_roles=frozenset(role_catalog.role_ids()),
                    )
            results.append(entry)

    return ISSubtotalCheck(results=results)


def _collect_subtotal_input_roles(
    model: FinancialModel,
    subtotal_item: LineItem,
    year: int,
    *,
    admitted_roles: frozenset[str],
) -> dict[str, float]:
    inputs: dict[str, float] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}
    visited: set[tuple[str, int]] = set()

    def _walk(item: LineItem, period: int) -> None:
        key = (item.id, int(period))
        if key in visited:
            return
        visited.add(key)

        spec = _historical_spec(item, int(period))
        refs = _extract_ref_targets(
            spec.params if spec is not None else None,
            period=int(period),
            mode=model.time_structure.period_mode,
        )
        if refs:
            before_count = len(inputs)
            for ref_id, ref_period in refs:
                if ref_period is None:
                    continue
                try:
                    ref_item = model.get_item(ref_id)
                except KeyError:
                    continue
                _walk(ref_item, int(ref_period))
            if len(inputs) > before_count:
                return
        required_role = item.required_role
        if required_role in admitted_roles:
            value = _observed_value(model, item, int(period), value_memo)
            if value is not None:
                inputs[required_role] = float(value)

    _walk(subtotal_item, int(year))
    return inputs


def _check_cf_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    role_catalog: RequiredRoleCatalog,
    tolerances: DiagnosticTolerances,
) -> CFReconciliationCheck:
    definition = CF_SECTIONS["net_change_reconciliation"]
    operating_item = model.get_item(definition["operating_item_id"])
    investing_item = model.get_item(definition["investing_item_id"])
    financing_item = model.get_item(definition["financing_item_id"])
    cash_balance_item = model.get_item(definition["cash_balance_item_id"])
    beginning_cash_item = model.get_item(definition["beginning_cash_item_id"])
    forex_item = None
    if definition.get("forex_item_id"):
        try:
            forex_item = model.get_item(definition["forex_item_id"])
        except KeyError:
            forex_item = None
    value_memo: dict[tuple[str, int], Optional[float]] = {}
    admitted_roles = frozenset(role_catalog.role_ids())
    by_year: dict[str, dict[str, Any]] = {}

    for year in historical_years:
        operating = _observed_value(model, operating_item, year, value_memo)
        investing = _observed_value(model, investing_item, year, value_memo)
        financing = _observed_value(model, financing_item, year, value_memo)
        current_cash = _observed_value(model, cash_balance_item, year, value_memo)
        prior_cash = _observed_value(model, cash_balance_item, year - 1, value_memo)
        if prior_cash is None:
            prior_cash = _observed_value(model, beginning_cash_item, year, value_memo)
        forex = (
            _observed_value(model, forex_item, year, value_memo)
            if forex_item is not None
            else None
        )

        payload: dict[str, Any] = {
            "operating": operating,
            "investing": investing,
            "financing": financing,
            "sum": None,
            "reported_net_change": None,
            "delta": None,
            "delta_pct": None,
            "severity": "ok",
            "kind": None,
            "forex_ignored": forex_item is None or forex is None,
        }
        required_values = [
            operating,
            investing,
            financing,
            current_cash,
            prior_cash,
        ]
        missing = [
            name
            for name, value in zip(
                (
                    "operating_cash_flow",
                    "investing_cash_flow",
                    "financing_cash_flow",
                    "cash_and_cash_equivalents",
                    "prior_cash_and_cash_equivalents",
                ),
                required_values,
            )
            if value is None
        ]
        if not _values_are_resolved(required_values):
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
            operating, investing, financing, current_cash, prior_cash = required_values
            sum_value = float(operating) + float(investing) + float(financing)
            if forex is not None:
                sum_value += float(forex)
                payload["forex"] = forex
                payload["forex_ignored"] = False
            reported_net_change = float(current_cash) - float(prior_cash)
            delta = sum_value - reported_net_change
            base = max(abs(sum_value), abs(reported_net_change))
            tol = _max_tolerance(
                base,
                tolerances.cf_reconciliation_abs_m,
                tolerances.cf_reconciliation_pct,
            )
            payload["sum"] = sum_value
            payload["reported_net_change"] = reported_net_change
            payload["delta"] = delta
            payload["delta_pct"] = _percent(delta, base)
            if abs(delta) > tol:
                payload["severity"] = "inconsistency"
                payload["kind"] = "wrong_tag_suspected"
                payload["inputs"] = {
                    "operating_cash_flow": float(operating),
                    "investing_cash_flow": float(investing),
                    "financing_cash_flow": float(financing),
                }
                if forex is not None:
                    payload["inputs"]["effect_of_exchange_rate_on_cash"] = float(forex)
        by_year[str(year)] = payload

    duplicate_rows: list[dict[str, Any]] = []
    role_groups: dict[str, list[LineItem]] = {}
    for item in _iter_items(model):
        if (
            item.required_role not in admitted_roles
            or not item.id.startswith("tpl.fm.cash_flow.")
        ):
            continue
        role_groups.setdefault(item.required_role, []).append(item)

    for required_role, items in sorted(role_groups.items()):
        if len(items) < 2:
            continue
        differing_years: dict[str, dict[str, float]] = {}
        for year in historical_years:
            values: dict[str, float] = {}
            for item in items:
                observed = _observed_value(model, item, year, value_memo)
                if observed is not None:
                    values[item.id] = float(observed)
            if len(values) < 2:
                continue
            if len({round(value, 9) for value in values.values()}) > 1:
                differing_years[str(year)] = values
        if differing_years:
            duplicate_rows.append(
                {
                    "required_role": required_role,
                    "template_items": sorted(item.id for item in items),
                    "values_differ": True,
                    "severity": "inconsistency",
                    "kind": "duplicate_rows",
                    "values_by_year": differing_years,
                }
            )

    return CFReconciliationCheck(
        net_change_reconciliation={"by_year": by_year},
        duplicate_role_rows=duplicate_rows,
        operating_cash_reconciliation=_operating_cash_reconciliation(
            model,
            historical_years=historical_years,
            role_catalog=role_catalog,
            tolerances=tolerances,
            value_memo=value_memo,
        ),
    )


#: The operating-cash components the model carries besides working capital:
#: net income and the non-cash add-backs with their own cash-flow lines.
_OPERATING_CASH_BASE_ROLES = (
    "statement.is.net_income",
    "statement.cf.depreciation_amortization",
    "statement.cf.stock_based_compensation",
    "statement.cf.change_in_deferred_income_taxes",
)
_OPERATING_CASH_FLOW_ROLE = "statement.cf.operating_cash_flow"
_OTHER_WORKING_CAPITAL_ROLE = "statement.cf.change_in_other_working_capital"


def _operating_cash_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    role_catalog: RequiredRoleCatalog,
    tolerances: DiagnosticTolerances,
    value_memo: dict[tuple[str, int], Optional[float]],
) -> dict[str, Any]:
    """Face cash from operations against its modeled components, per year.

    The components are net income, the non-cash add-backs, every working-
    capital line the catalog carries on its own (``change_of``), and other
    working capital at its derived leaf identity. The difference is what the
    face attributes to items the model does not carry (other non-cash
    charges, leaf movements the balances do not show). It is observed and
    reported; it never blocks the build.
    """

    component_roles = (
        *_OPERATING_CASH_BASE_ROLES,
        *(row.required_role for row in role_catalog.required_roles if row.change_of),
        _OTHER_WORKING_CAPITAL_ROLE,
    )

    def _role_item(required_role: str) -> LineItem | None:
        for item_id in role_catalog.role(required_role).template_items:
            if item_id in model._index:
                return model.get_item(item_id)
        return None

    reported_item = _role_item(_OPERATING_CASH_FLOW_ROLE)
    items = {role: _role_item(role) for role in component_roles}
    by_year: dict[str, dict[str, Any]] = {}
    for year in historical_years:
        reported = (
            None
            if reported_item is None
            else _observed_value(model, reported_item, year, value_memo)
        )
        components = {
            role: (None if item is None else _observed_value(model, item, year, value_memo))
            for role, item in items.items()
        }
        payload: dict[str, Any] = {
            "reported_operating_cash_flow": reported,
            "components": components,
            "modeled_sum": None,
            "difference": None,
            "difference_pct": None,
            "severity": "ok",
            "kind": None,
        }
        missing = [role for role, value in components.items() if value is None]
        if reported is None:
            missing.insert(0, _OPERATING_CASH_FLOW_ROLE)
        if missing:
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
            assert reported is not None
            modeled = sum(float(value) for value in components.values() if value is not None)
            difference = float(reported) - modeled
            base = max(abs(float(reported)), abs(modeled))
            payload["modeled_sum"] = modeled
            payload["difference"] = difference
            payload["difference_pct"] = _percent(difference, base)
            if abs(difference) > _max_tolerance(
                base,
                tolerances.cf_reconciliation_abs_m,
                tolerances.cf_reconciliation_pct,
            ):
                payload["severity"] = "gap"
                payload["kind"] = "operating_cash_unexplained"
        by_year[str(year)] = payload
    return {"by_year": by_year}


def _check_coverage_summary(
    model: FinancialModel,
    *,
    ticker: str,
    historical_years: list[int],
    role_catalog: RequiredRoleCatalog,
    stats: PopulateStats,
    derivable_items: dict[str, set[int]],
    synthetic_zero_check: SyntheticZeroCheck,
    provider_registry: "ProviderRegistry | None" = None,
) -> CoverageSummary:
    roles = role_catalog.required_roles
    role_items = _required_role_item_map(
        model,
        role_catalog=role_catalog,
        ticker=ticker,
        required_roles=(role.required_role for role in roles),
    )
    synthetic_by_role = {
        entry["required_role"]: entry
        for entry in synthetic_zero_check.items_with_synthetic_zero
        if entry.get("required_role")
    }
    populated = 0
    missing: list[dict[str, Any]] = []
    intentionally_blank: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    synthetic_zero: list[dict[str, Any]] = []
    missing_set = {str(role) for role in stats.missing_roles}
    error_set = {str(role) for role in (stats.edgar_errors or [])}
    partial_set = {str(role) for role in (stats.edgar_partial_failures or [])}
    path_memo: dict[tuple[str, int], bool] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for role in roles:
        required_role = role.required_role
        items = role_items[required_role]
        if required_role in synthetic_by_role:
            synthetic_entry = synthetic_by_role[required_role]
            synthetic_zero.append(
                {
                    "required_role": required_role,
                    "template_item": synthetic_entry["template_item"],
                    "downstream_impact": synthetic_entry["downstream_impact"],
                }
            )
            continue
        if required_role in error_set or required_role in partial_set:
            error_entry = {
                "required_role": required_role,
                "status": (
                    "edgar_errors"
                    if required_role in error_set
                    else "edgar_partial_failures"
                ),
                "upstream_semantics": "no filing line for this ticker and role",
            }
            served_detail = _served_error_detail_from_stats(stats, required_role)
            if served_detail is not None:
                if served_detail.get("years_served"):
                    populated += 1
                error_entry.update(served_detail)
            errors.append(error_entry)
            continue
        if _role_has_coverage(
            model,
            items,
            historical_years,
            derivable_items,
            path_memo,
            value_memo,
        ):
            populated += 1
            continue

        blank_detail = _intentional_blank_detail(
            model,
            role=role,
            role_items=role_items,
            years=historical_years,
            derivable_items=derivable_items,
            path_memo=path_memo,
            value_memo=value_memo,
        )
        if blank_detail is not None:
            intentionally_blank.append(
                {
                    "required_role": required_role,
                    **blank_detail,
                }
            )
            continue

        if not items:
            missing.append(
                {
                    "required_role": required_role,
                    "status": "role_binds_no_model_item",
                }
            )
        else:
            missing.append(
                {
                    "required_role": required_role,
                    "status": "required_column_absent_from_pin",
                }
            )

    return CoverageSummary(
        total_required_roles=len(roles),
        populated=populated,
        populated_breakdown=_populated_breakdown_from_stats(
            stats,
            provider_registry=provider_registry,
        ),
        missing=missing,
        intentionally_blank=intentionally_blank,
        errors=errors,
        synthetic_zero=synthetic_zero,
        provider_registry=provider_registry,
    )


def _served_error_detail_from_stats(
    stats: PopulateStats,
    required_role: str,
) -> dict[str, Any] | None:
    entry = getattr(stats, "served_by_role", {}).get(required_role)
    if entry is None:
        return None

    producer_by_year = dict(getattr(entry, "producer_by_year", {}) or {})
    years_served = sorted(producer_by_year)
    years_unserved = list(getattr(entry, "years_unserved", []) or [])
    if not years_served and not years_unserved:
        return None

    detail: dict[str, Any] = {}
    if producer_by_year:
        detail["producer_by_year"] = producer_by_year
    if years_served:
        detail["years_served"] = years_served
    if years_unserved:
        detail["years_unserved"] = years_unserved
    return detail


def _populated_breakdown_from_stats(
    stats: PopulateStats,
    *,
    provider_registry: "ProviderRegistry | None" = None,
) -> dict[ProviderId, int]:
    breakdown = {
        EDGAR_PROVIDER_ID: 0,
        FMP_PROVIDER_ID: 0,
    }
    for entry in getattr(stats, "served_by_role", {}).values():
        for producer in dict(
            getattr(entry, "producer_by_year", {}) or {}
        ).values():
            producer = require_coverage_provider_id(
                producer,
                provider_registry=provider_registry,
                entry_point="diagnostic populated-breakdown producer",
            )
            breakdown[producer] = breakdown.get(producer, 0) + 1
    return breakdown


def _check_synthetic_zero_propagation(
    model: FinancialModel,
    *,
    role_catalog: RequiredRoleCatalog,
    historical_years: list[int],
    reverse_graph: dict[str, set[str]],
) -> SyntheticZeroCheck:
    admitted_roles = frozenset(role_catalog.role_ids())
    findings: list[dict[str, Any]] = []

    for item in _iter_items(model):
        years_synthetic = sorted(
            year
            for year in historical_years
            if item.overrides is not None
            and year in item.overrides
            and _is_synthetic_override(item.overrides[year])
        )
        if not years_synthetic:
            continue
        finding: dict[str, Any] = {
            "template_item": item.id,
            "years_synthetic_zeroed": years_synthetic,
            "severity": "material_gap",
            "kind": "synthetic_zero_propagation",
            "downstream_impact": sorted(
                _transitive_downstream_ids(item.id, reverse_graph)
            ),
        }
        if item.required_role in admitted_roles:
            finding["required_role"] = item.required_role
        findings.append(finding)

    findings.sort(key=lambda item: item["template_item"])
    return SyntheticZeroCheck(items_with_synthetic_zero=findings)


def _check_historical_path_coverage(
    model: FinancialModel,
    *,
    ticker: str,
    historical_years: list[int],
    role_catalog: RequiredRoleCatalog,
    derivable_items: dict[str, set[int]],
) -> HistoricalPathCoverageCheck:
    by_section: dict[str, list[dict[str, Any]]] = {}
    section_lookup = _section_lookup(model)
    role_by_id = {role.required_role: role for role in role_catalog.required_roles}
    role_items = _required_role_item_map(
        model,
        role_catalog=role_catalog,
        ticker=ticker,
        required_roles=role_by_id,
    )
    path_memo: dict[tuple[str, int], bool] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for item in _iter_items(model):
        if not item.id.startswith("tpl.fm.") or item.item_type in {
            ItemType.header,
            ItemType.spacer,
        }:
            continue
        has_real_value = any(_has_real_value(item, year) for year in historical_years)
        has_path = any(
            _item_has_path(
                model,
                item,
                year,
                derivable_items,
                path_memo,
                set(),
            )
            for year in historical_years
        )
        if has_real_value or has_path:
            continue

        synthetic_years = sorted(
            year
            for year in historical_years
            if item.overrides is not None
            and year in item.overrides
            and _is_synthetic_override(item.overrides[year])
        )
        required_role = item.required_role
        role = role_by_id.get(required_role) if required_role is not None else None
        entry: dict[str, Any] = {
            "template_item": item.id,
            "years": list(historical_years),
        }
        if role is not None:
            entry["required_role"] = role.required_role

        if synthetic_years:
            entry["severity"] = "material_gap"
            entry["kind"] = "synthetic_zero_propagation"
            entry["years"] = synthetic_years
        elif (
            projection_only_detail := _projection_only_historical_blank_detail(
                model,
                item,
                historical_years,
                derivable_items,
            )
        ) is not None:
            entry.update(projection_only_detail)
        elif role is not None and item.historical is None:
            blank_detail = _intentional_blank_detail(
                model,
                role=role,
                role_items=role_items,
                years=historical_years,
                derivable_items=derivable_items,
                path_memo=path_memo,
                value_memo=value_memo,
            )
            if blank_detail is not None:
                entry.update(blank_detail)
            else:
                entry["severity"] = "material_gap"
                entry["kind"] = "required_column_absent_from_pin"
        else:
            entry["severity"] = "gap"
            entry["kind"] = "insufficient_inputs"
            entry["blocking_refs"] = _blocking_ref_ids(
                model, item, historical_years, value_memo
            )

        section_id = section_lookup.get(item.id, "unassigned")
        by_section.setdefault(section_id, []).append(entry)

    for entries in by_section.values():
        entries.sort(key=lambda entry: entry["template_item"])
    return HistoricalPathCoverageCheck(by_section=by_section)


def _write_diagnostic_log(
    report: DiagnosticReport,
    *,
    log_dir: Path | None = None,
) -> Optional[Path]:
    validate_diagnostic_provider_keys(report)
    try:
        base_dir = log_dir or (
            Path(__file__).resolve().parents[1] / "api" / "logs" / "build_diagnostics"
        )
        base_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        ticker = str(report.ticker)
        filename = base_dir / f"{ticker}_{int(report.most_recent_fy)}_{timestamp}.json"
        filename.write_text(json.dumps(asdict(report), indent=2, sort_keys=True) + "\n")

        latest = base_dir / f"{ticker}_latest.json"
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(filename.name)
        return filename
    except Exception:
        logger.exception(
            "Failed to write build diagnostic log for %s fy=%s",
            report.ticker,
            report.most_recent_fy,
        )
        return None


__all__ = [
    "BS_SECTIONS",
    "CF_SECTIONS",
    "CoverageSummary",
    "BSBalanceCheck",
    "BSSublineCheck",
    "CFReconciliationCheck",
    "RevenueConsolidationCheck",
    "CrossSourceValidationCheck",
    "DiagnosticReport",
    "DiagnosticTolerances",
    "HistoricalPathCoverageCheck",
    "IS_SECTIONS",
    "ISSubtotalCheck",
    "SectionMember",
    "SyntheticZeroCheck",
    "_is_synthetic_override",
    "_write_diagnostic_log",
    "run_build_diagnostic",
]
