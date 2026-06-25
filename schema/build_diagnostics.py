"""Build diagnostics for populated financial models."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

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
    _concept_item_map as _concept_item_map,
    _concept_has_coverage as _concept_has_coverage,
    _concept_has_full_coverage as _concept_has_full_coverage,
    _item_has_coverage_for_year as _item_has_coverage_for_year,
    _intentional_blank_detail as _intentional_blank_detail,
    _projection_only_historical_blank_detail as _projection_only_historical_blank_detail,
    _item_has_path as _item_has_path,
    _blocking_ref_ids as _blocking_ref_ids,
    _is_edgar_sourced as _is_edgar_sourced,
)
from .build_diagnostic_types import (
    DiagnosticTolerances as DiagnosticTolerances,
    BSBalanceCheck as BSBalanceCheck,
    BSSublineCheck as BSSublineCheck,
    ISSubtotalCheck as ISSubtotalCheck,
    CFReconciliationCheck as CFReconciliationCheck,
    CoverageSummary as CoverageSummary,
    FallbackSummary as FallbackSummary,
    SyntheticZeroCheck as SyntheticZeroCheck,
    HistoricalPathCoverageCheck as HistoricalPathCoverageCheck,
    CrossSourceValidationCheck as CrossSourceValidationCheck,
    DiagnosticReport as DiagnosticReport,
)
from .build_diagnostic_balance_sheet import (
    _COVERAGE_FINDING_TOP_N as _COVERAGE_FINDING_TOP_N,
    _CV_CPV_REL_TOL as _CV_CPV_REL_TOL,
    _CV_CPV_ABS_TOL as _CV_CPV_ABS_TOL,
    _KNOWN_CONTRA_BS_FACE_TAGS as _KNOWN_CONTRA_BS_FACE_TAGS,
    _check_bs_balance as _check_bs_balance,
    _check_bs_subline_reconciliation as _check_bs_subline_reconciliation,
    _check_bs_subline_reconciliation_legacy as _check_bs_subline_reconciliation_legacy,
    _check_bs_subline_reconciliation_presentation as _check_bs_subline_reconciliation_presentation,
    _values_match_magnitude as _values_match_magnitude,
    _resolve_signed_contribution as _resolve_signed_contribution,
    _resolve_rollup_weight as _resolve_rollup_weight,
    _select_non_overlapping_presentation_children as _select_non_overlapping_presentation_children,
    _find_rollup_component_slice as _find_rollup_component_slice,
    _prefer_rollup_child as _prefer_rollup_child,
    _presentation_values_equal as _presentation_values_equal,
    _same_xbrl_tag as _same_xbrl_tag,
    _build_taxonomy_tag_index as _build_taxonomy_tag_index,
    _taxonomy_tag_keys as _taxonomy_tag_keys,
    _section_member_for_concept as _section_member_for_concept,
    _concept_sections_by_id as _concept_sections_by_id,
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
    DataSourceMapping,
    ItemType,
    LineItem,
    FinancialModel,
)
from .presentation_tree import PresentationTree
from .validation_input import ValidationInput

if TYPE_CHECKING:
    from .build import PopulateStats


logger = logging.getLogger(__name__)

DIAGNOSTIC_VERSION = 1
VALID_KINDS = {
    "missing_concept",
    "missing_mapping",
    "wrong_tag_suspected",
    "insufficient_inputs",
    "duplicate_rows",
    "synthetic_zero_propagation",
    "tree_missing_parent",
    "tree_missing_year",
    "unmapped_xbrl_concept",
    "excluded_out_of_section_concept",
    "covered_by_concept",
    "optional_unreported",
    "projection_only",
}


def run_build_diagnostic(
    model: FinancialModel,
    *,
    ticker: str,
    fy: int,
    taxonomy: dict[str, DataSourceMapping],
    stats: PopulateStats,
    derivable_items: dict[str, set[int]] | None = None,
    tolerances: DiagnosticTolerances | None = None,
    presentation_tree: PresentationTree | None = None,
    validation_input: ValidationInput | None = None,
) -> DiagnosticReport:
    model.build_index()
    historical_years = _historical_years(model)
    tolerances = tolerances or DiagnosticTolerances()
    reverse_graph = _build_reverse_dependency_graph(model)
    synthetic_zero = _check_synthetic_zero_propagation(
        model,
        historical_years=historical_years,
        reverse_graph=reverse_graph,
    )

    return DiagnosticReport(
        ticker=str(ticker),
        fiscal_year_end=str(
            model.company.fiscal_year_end or model.time_structure.fiscal_year_end or ""
        ),
        most_recent_fy=int(fy),
        diagnostic_version=DIAGNOSTIC_VERSION,
        generated_at=_iso_utc_now(),
        bs_balance=_check_bs_balance(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        bs_subline_reconciliation=_check_bs_subline_reconciliation(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            tolerances=tolerances,
            presentation_tree=presentation_tree,
        ),
        is_subtotal_integrity=_check_is_subtotal_integrity(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        cf_reconciliation=_check_cf_reconciliation(
            model,
            historical_years=historical_years,
            tolerances=tolerances,
        ),
        coverage_summary=_check_coverage_summary(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            stats=stats,
            derivable_items=derivable_items or {},
            synthetic_zero_check=synthetic_zero,
        ),
        fallback_summary=_fallback_summary_from_stats(stats),
        synthetic_zero_propagation=synthetic_zero,
        historical_path_coverage=_check_historical_path_coverage(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            derivable_items=derivable_items or {},
        ),
        cross_source_validation=_check_cross_source_validation(
            validation_input,
            taxonomy=taxonomy,
            tolerances=tolerances,
        ),
    )


def _check_is_subtotal_integrity(
    model: FinancialModel,
    *,
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
                    entry["inputs"] = _collect_subtotal_input_concepts(
                        model, item, year
                    )
            results.append(entry)

    return ISSubtotalCheck(results=results)


def _collect_subtotal_input_concepts(
    model: FinancialModel,
    subtotal_item: LineItem,
    year: int,
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

        concept_id = getattr(item, "data_concept_id", None)
        if concept_id:
            value = _observed_value(model, item, int(period), value_memo)
            if value is not None:
                inputs[str(concept_id)] = float(value)

    _walk(subtotal_item, int(year))
    return inputs


def _check_cf_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
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
        missing = [
            name
            for name, value in (
                ("operating_cash_flow", operating),
                ("investing_cash_flow", investing),
                ("financing_cash_flow", financing),
                ("cash_and_cash_equivalents", current_cash),
                ("prior_cash_and_cash_equivalents", prior_cash),
            )
            if value is None
        ]
        if missing:
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
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
    concept_groups: dict[str, list[LineItem]] = {}
    for item in _iter_items(model):
        if not item.data_concept_id or not item.id.startswith("tpl.fm.cash_flow."):
            continue
        concept_groups.setdefault(item.data_concept_id, []).append(item)

    for concept_id, items in sorted(concept_groups.items()):
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
                    "concept_id": concept_id,
                    "template_items": sorted(item.id for item in items),
                    "values_differ": True,
                    "severity": "inconsistency",
                    "kind": "duplicate_rows",
                    "values_by_year": differing_years,
                }
            )

    return CFReconciliationCheck(
        net_change_reconciliation={"by_year": by_year},
        duplicate_concept_rows=duplicate_rows,
    )


def _check_coverage_summary(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    stats: PopulateStats,
    derivable_items: dict[str, set[int]],
    synthetic_zero_check: SyntheticZeroCheck,
) -> CoverageSummary:
    concept_items = _concept_item_map(model)
    synthetic_by_concept = {
        entry["concept"]: entry
        for entry in synthetic_zero_check.items_with_synthetic_zero
        if entry.get("concept")
    }
    populated = 0
    missing: list[dict[str, Any]] = []
    intentionally_blank: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    synthetic_zero: list[dict[str, Any]] = []
    missing_set = {str(concept) for concept in stats.missing_concepts}
    error_set = {str(concept) for concept in (stats.edgar_errors or [])}
    partial_set = {str(concept) for concept in (stats.edgar_partial_failures or [])}
    path_memo: dict[tuple[str, int], bool] = {}
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    edgar_concepts = [
        concept_id
        for concept_id, mapping in sorted(taxonomy.items())
        if _is_edgar_sourced(mapping)
    ]

    for concept_id in edgar_concepts:
        mapping = taxonomy[concept_id]
        items = concept_items.get(concept_id, [])
        if concept_id in synthetic_by_concept:
            synthetic_entry = synthetic_by_concept[concept_id]
            synthetic_zero.append(
                {
                    "concept": concept_id,
                    "template_item": synthetic_entry["template_item"],
                    "downstream_impact": synthetic_entry["downstream_impact"],
                }
            )
            continue
        if concept_id in error_set or concept_id in partial_set:
            error_entry = {
                "concept": concept_id,
                "status": "edgar_errors"
                if concept_id in error_set
                else "edgar_partial_failures",
                "upstream_semantics": "no data for this (ticker, concept)",
            }
            served_detail = _served_error_detail_from_stats(stats, concept_id)
            if served_detail is not None:
                populated += 1
                error_entry.update(served_detail)
            errors.append(error_entry)
            continue
        if _concept_has_coverage(
            model,
            items,
            historical_years,
            derivable_items,
            path_memo,
            value_memo,
        ):
            populated += 1
            continue

        if concept_id in missing_set or items:
            blank_detail = _intentional_blank_detail(
                model,
                mapping=mapping,
                concept_items=concept_items,
                years=historical_years,
                derivable_items=derivable_items,
                path_memo=path_memo,
                value_memo=value_memo,
            )
            if blank_detail is not None:
                intentionally_blank.append(
                    {
                        "concept": concept_id,
                        **blank_detail,
                    }
                )
                continue

            missing_entry: dict[str, Any] = {
                "concept": concept_id,
                "status": "missing",
                "filer_reports_as": "unknown",
            }
            if mapping.nonadmissible_reason_code is not None:
                missing_entry["nonadmissible_reason_code"] = str(
                    mapping.nonadmissible_reason_code.value
                )
            missing.append(missing_entry)

    return CoverageSummary(
        total_edgar_sourced=len(edgar_concepts),
        populated=populated,
        populated_breakdown=_populated_breakdown_from_stats(stats),
        missing=missing,
        intentionally_blank=intentionally_blank,
        errors=errors,
        synthetic_zero=synthetic_zero,
    )


def _served_error_detail_from_stats(
    stats: PopulateStats, concept_id: str
) -> dict[str, Any] | None:
    entry = getattr(stats, "served_by_breakdown", {}).get(concept_id)
    if entry is None:
        return None

    years_via_primary = list(getattr(entry, "years_via_primary", []) or [])
    years_via_fallback = list(getattr(entry, "years_via_fallback", []) or [])
    if not years_via_primary and not years_via_fallback:
        return None

    detail: dict[str, Any] = {}
    if years_via_primary:
        detail["years_via_primary"] = years_via_primary
    if years_via_fallback:
        detail["years_via_fallback"] = years_via_fallback
        detail["years_recovered"] = years_via_fallback
        primary = getattr(entry, "primary_source", None)
        if primary == "edgar":
            detail["recovered_via"] = "fmp_fallback"
        elif primary == "fmp":
            detail["recovered_via"] = "edgar_fallback"
        else:
            detail["recovered_via"] = "fallback"

    years_unserved = list(getattr(entry, "years_unserved", []) or [])
    if years_unserved:
        detail["years_unserved"] = years_unserved
    return detail


def _populated_breakdown_from_stats(stats: PopulateStats) -> dict[str, int]:
    breakdown = {
        "edgar_primary": 0,
        "edgar_fallback": 0,
        "fmp_primary": 0,
        "fmp_fallback": 0,
    }
    for entry in getattr(stats, "served_by_breakdown", {}).values():
        primary = getattr(entry, "primary_source", None)
        years_via_primary = list(getattr(entry, "years_via_primary", []) or [])
        years_via_fallback = list(getattr(entry, "years_via_fallback", []) or [])
        if primary == "edgar":
            breakdown["edgar_primary"] += len(years_via_primary)
            breakdown["fmp_fallback"] += len(years_via_fallback)
        elif primary == "fmp":
            breakdown["fmp_primary"] += len(years_via_primary)
            breakdown["edgar_fallback"] += len(years_via_fallback)
    return breakdown


def _fallback_summary_from_stats(stats: PopulateStats) -> FallbackSummary:
    concepts: list[dict[str, Any]] = []
    for concept_id, entry in sorted(getattr(stats, "served_by_breakdown", {}).items()):
        years_via_fallback = list(getattr(entry, "years_via_fallback", []) or [])
        if not years_via_fallback:
            continue
        concepts.append(
            {
                "concept_id": concept_id,
                "primary": getattr(entry, "primary_source", None),
                "years_via_primary": list(
                    getattr(entry, "years_via_primary", []) or []
                ),
                "years_via_fallback": years_via_fallback,
                "years_unserved": list(getattr(entry, "years_unserved", []) or []),
            }
        )
    return FallbackSummary(
        fallback_engaged_cells=int(getattr(stats, "fallback_engaged_cells", 0) or 0),
        concepts_with_fallback=concepts,
    )


def _check_synthetic_zero_propagation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    reverse_graph: dict[str, set[str]],
) -> SyntheticZeroCheck:
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
        findings.append(
            {
                "concept": item.data_concept_id,
                "template_item": item.id,
                "years_synthetic_zeroed": years_synthetic,
                "severity": "material_gap",
                "kind": "synthetic_zero_propagation",
                "downstream_impact": sorted(
                    _transitive_downstream_ids(item.id, reverse_graph)
                ),
            }
        )

    findings.sort(key=lambda item: item["template_item"])
    return SyntheticZeroCheck(items_with_synthetic_zero=findings)


def _check_historical_path_coverage(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    derivable_items: dict[str, set[int]],
) -> HistoricalPathCoverageCheck:
    by_section: dict[str, list[dict[str, Any]]] = {}
    section_lookup = _section_lookup(model)
    concept_items = _concept_item_map(model)
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
        entry: dict[str, Any] = {
            "template_item": item.id,
            "data_concept_id": item.data_concept_id,
            "years": list(historical_years),
        }
        if synthetic_years:
            entry["severity"] = "material_gap"
            entry["kind"] = "synthetic_zero_propagation"
            entry["years"] = synthetic_years
        elif (
            projection_only_detail := _projection_only_historical_blank_detail(
                item,
                historical_years,
            )
        ) is not None:
            entry.update(projection_only_detail)
        elif item.data_concept_id is None and item.historical is None:
            entry["severity"] = "material_gap"
            entry["kind"] = "missing_mapping"
        elif item.data_concept_id and item.historical is None:
            mapping = taxonomy.get(item.data_concept_id)
            blank_detail = (
                _intentional_blank_detail(
                    model,
                    mapping=mapping,
                    concept_items=concept_items,
                    years=historical_years,
                    derivable_items=derivable_items,
                    path_memo=path_memo,
                    value_memo=value_memo,
                )
                if mapping is not None
                else None
            )
            if blank_detail is not None:
                entry.update(blank_detail)
            else:
                entry["severity"] = "material_gap"
                entry["kind"] = "missing_concept"
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
    "CrossSourceValidationCheck",
    "DiagnosticReport",
    "DiagnosticTolerances",
    "FallbackSummary",
    "HistoricalPathCoverageCheck",
    "IS_SECTIONS",
    "ISSubtotalCheck",
    "SectionMember",
    "SyntheticZeroCheck",
    "_is_synthetic_override",
    "_write_diagnostic_log",
    "run_build_diagnostic",
]
