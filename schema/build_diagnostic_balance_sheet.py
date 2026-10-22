"""Balance-sheet checks for build diagnostics."""

from __future__ import annotations

from typing import Any, Optional

from .build_diagnostic_sections import SectionMember
from .build_diagnostic_section_roles import resolve_balance_sheet_sections
from .build_diagnostic_types import BSBalanceCheck, BSSublineCheck, DiagnosticTolerances
from .build_diagnostic_values import (
    _max_tolerance,
    _observed_value,
    _percent,
    _ratio,
)
from .models import FinancialModel, FinancialStatement
from .required_role_catalog import RequiredRoleCatalog, load_required_role_catalog



def _check_bs_balance(
    model: FinancialModel,
    *,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
) -> BSBalanceCheck:
    """Total assets against liabilities plus equity on each historical column.

    Each operand is the column's rendered value recomputed by
    ``_observed_value`` (its formula, not a stored total), so a filed total
    whose formula omits a row is unbalanced here as it is in the workbook.
    Projected columns are evaluated on the recalculated workbook
    (``projected_financial_integrity``).
    """
    assets_item = model.get_item("tpl.fm.balance_sheet.total_assets")
    liabilities_item = model.get_item("tpl.fm.balance_sheet.total_liabilities")
    equity_item = model.get_item("tpl.fm.balance_sheet.total_equity")
    result = BSBalanceCheck()
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for year in historical_years:
        assets = _observed_value(model, assets_item, year, value_memo)
        liabilities = _observed_value(model, liabilities_item, year, value_memo)
        equity = _observed_value(model, equity_item, year, value_memo)
        missing = [
            name
            for name, value in (
                ("assets", assets),
                ("liabilities", liabilities),
                ("equity", equity),
            )
            if value is None
        ]
        payload: dict[str, Any] = {
            "assets": assets,
            "liab_plus_equity": None
            if liabilities is None or equity is None
            else liabilities + equity,
            "delta": None,
            "delta_pct": None,
            "severity": "ok",
            "kind": None,
        }
        if assets is None or liabilities is None or equity is None:
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
            liab_plus_equity = liabilities + equity
            delta = assets - liab_plus_equity
            base = max(abs(assets), abs(liab_plus_equity))
            tol = _max_tolerance(
                base, tolerances.bs_balance_abs_m, tolerances.bs_balance_pct
            )
            payload["delta"] = delta
            payload["delta_pct"] = _percent(delta, base)
            payload["liab_plus_equity"] = liab_plus_equity
            if abs(delta) > tol:
                payload["severity"] = "inconsistency"
                payload["kind"] = "wrong_tag_suspected"
                payload["inputs"] = {
                    "total_assets": assets,
                    "total_liabilities": liabilities,
                    "total_equity": equity,
                }
        result.by_year[str(year)] = payload
    return result


def _check_bs_subline_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    role_catalog: RequiredRoleCatalog,
    tolerances: DiagnosticTolerances,
) -> BSSublineCheck:
    section_resolution = resolve_balance_sheet_sections(
        model,
        role_catalog=role_catalog,
    )
    sections = section_resolution.sections
    result = _check_bs_subline_reconciliation_legacy(
        model,
        historical_years=historical_years,
        role_catalog=role_catalog,
        tolerances=tolerances,
        sections=sections,
    )
    result.section_role_resolution = {
        "metadata_source": section_resolution.metadata_source,
        "fallback_reason": section_resolution.fallback_reason,
        "role_derived_sections": section_resolution.role_derived_sections,
        "legacy_fallback_sections": section_resolution.legacy_fallback_sections,
    }
    return result


def _check_bs_subline_reconciliation_legacy(
    model: FinancialModel,
    *,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
    role_catalog: RequiredRoleCatalog,
    sections: dict[str, dict[str, Any]] | None = None,
) -> BSSublineCheck:
    result = BSSublineCheck()
    value_memo: dict[tuple[str, int], Optional[float]] = {}
    admitted_roles = frozenset(role_catalog.role_ids())
    if sections is None:
        sections = resolve_balance_sheet_sections(
            model,
            role_catalog=role_catalog,
        ).sections

    for section_name, definition in sections.items():
        if definition.get("presentation_only"):
            continue
        section_members = _effective_section_members(
            model,
            definition,
            role_catalog=role_catalog,
        )
        section_payload: dict[str, Any] = {"by_year": {}, "coverage_findings": []}

        for year in historical_years:
            findings: list[dict[str, Any]] = []
            sub_lines_sum = 0.0

            for member in section_members:
                item = model.get_item(member.template_item_id)
                value = _observed_value(model, item, year, value_memo)
                if value is not None:
                    sub_lines_sum += float(value)
                    continue

                if item.required_role in admitted_roles:
                    findings.append(
                        {
                            "template_item": member.template_item_id,
                            "required_role": item.required_role,
                            "severity": "gap",
                            "kind": "required_column_absent_from_pin",
                        }
                    )

            section_total = _resolve_section_total(model, definition, year, value_memo)
            missing_inputs: list[str] = []
            if section_total is None:
                total_item = model.get_item(definition["total_item_id"])
                total_reported_raw = _observed_value(
                    model, total_item, year, value_memo
                )
                if total_reported_raw is None:
                    missing_inputs.append(total_item.id)
                included_subtotal_id = definition.get("also_includes_subtotal")
                if included_subtotal_id:
                    included_subtotal = model.get_item(included_subtotal_id)
                    included_value = _observed_value(
                        model, included_subtotal, year, value_memo
                    )
                    if total_reported_raw is None or included_value is None:
                        missing_inputs.append(included_subtotal.id)

            payload: dict[str, Any] = {
                "sub_lines_sum": sub_lines_sum,
                "total_reported": section_total,
                "delta": None,
                "delta_pct": None,
                "severity": "ok",
                "kind": None,
                "findings": findings,
            }

            if section_total is None:
                payload["severity"] = "gap"
                payload["kind"] = "insufficient_inputs"
                payload["missing_inputs"] = sorted(set(missing_inputs))
            else:
                delta = float(section_total) - sub_lines_sum
                delta_pct_ratio = _ratio(abs(delta), float(section_total))
                payload["delta"] = delta
                payload["delta_pct"] = delta_pct_ratio * 100.0
                if delta_pct_ratio >= tolerances.bs_subline_material_pct:
                    payload["severity"] = "material_gap"
                elif delta_pct_ratio >= tolerances.bs_subline_gap_pct:
                    payload["severity"] = "gap"
            section_payload["by_year"][str(year)] = payload
        result.by_section[section_name] = section_payload

    return result


def _effective_section_members(
    model: FinancialModel,
    definition: dict[str, Any],
    *,
    role_catalog: RequiredRoleCatalog | None = None,
) -> list[SectionMember]:
    members = list(definition["sub_lines"])
    if not members:
        return members

    member_ids = {member.template_item_id for member in members}
    structural_ids = {
        str(item_id)
        for item_id in (
            definition.get("pre_subtotal_item_id"),
            definition.get("total_item_id"),
            definition.get("also_includes_subtotal"),
        )
        if item_id
    }
    total_item = model.get_item(definition["total_item_id"])
    first_member_row = min(
        int(model.get_item(member.template_item_id).row) for member in members
    )
    total_row = int(total_item.row)
    section_names = {
        role.section
        for role in total_item.statement_section_roles or ()
        if role.statement == FinancialStatement.balance_sheet
        and role.role == "section_total"
    }
    admitted_roles = frozenset(
        (role_catalog or load_required_role_catalog()).role_ids()
    )

    containing_section = next(
        (
            section
            for sheet in model.sheets.values()
            for section in sheet.sections
            if any(item.id == total_item.id for item in section.line_items)
        ),
        None,
    )
    if containing_section is not None:
        for item in containing_section.line_items:
            if item.id in member_ids or item.id in structural_ids:
                continue
            if not first_member_row <= int(item.row) < total_row:
                continue
            reviewed_window_member = any(
                role.statement == FinancialStatement.balance_sheet
                and role.role == "member"
                and (not section_names or role.section in section_names)
                for role in item.statement_section_roles or ()
            )
            if item.required_role in admitted_roles or reviewed_window_member:
                members.append(SectionMember(item.id))
                member_ids.add(item.id)

    return sorted(
        members,
        key=lambda member: int(model.get_item(member.template_item_id).row),
    )


def _resolve_section_total(
    model: FinancialModel,
    definition: dict[str, Any],
    year: int,
    memo: dict[tuple[str, int], Optional[float]],
) -> float | None:
    total_item = model.get_item(definition["total_item_id"])
    total_reported_raw = _observed_value(model, total_item, year, memo)
    if total_reported_raw is None:
        return None

    included_subtotal_id = definition.get("also_includes_subtotal")
    if not included_subtotal_id:
        return float(total_reported_raw)

    included_subtotal = model.get_item(included_subtotal_id)
    included_value = _observed_value(model, included_subtotal, year, memo)
    if included_value is None:
        return None
    return float(total_reported_raw) - float(included_value)


__all__ = [
    "_check_bs_balance",
    "_check_bs_subline_reconciliation",
    "_check_bs_subline_reconciliation_legacy",
    "_effective_section_members",
    "_resolve_section_total",
]
