"""Balance-sheet and presentation-tree checks for build diagnostics."""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from typing import Any, Optional

from .build_diagnostic_sections import BS_SECTIONS, ParentCandidate, SectionMember
from .build_diagnostic_types import BSBalanceCheck, BSSublineCheck, DiagnosticTolerances
from .build_diagnostic_values import (
    _iter_items,
    _max_tolerance,
    _observed_value,
    _percent,
    _ratio,
)
from .models import DataSourceMapping, FinancialModel
from .presentation_tree import PresentationChild, PresentationTree

logger = logging.getLogger(__name__)

_COVERAGE_FINDING_TOP_N = 5

_CV_CPV_REL_TOL = 1e-9

_CV_CPV_ABS_TOL = 1e-6

_KNOWN_CONTRA_BS_FACE_TAGS: frozenset[str] = frozenset(
    {
        "us-gaap:TreasuryStockCommonValue",
        "us-gaap:TreasuryStockValue",
        "us-gaap:TreasuryStockPreferredValue",
        "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
        "us-gaap:PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAccumulatedDepreciationAndAmortization",
        "us-gaap:AllowanceForDoubtfulAccountsReceivable",
        "us-gaap:AllowanceForDoubtfulAccountsReceivableCurrent",
        "us-gaap:AllowanceForDoubtfulAccountsReceivableNoncurrent",
    }
)

"""BS-face contra concepts used when calc-linkbase sign metadata is unavailable."""


def _check_bs_balance(
    model: FinancialModel,
    *,
    historical_years: list[int],
    tolerances: DiagnosticTolerances,
) -> BSBalanceCheck:
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
        if missing:
            payload["severity"] = "gap"
            payload["kind"] = "insufficient_inputs"
            payload["missing_inputs"] = missing
        else:
            liab_plus_equity = float(liabilities) + float(equity)
            delta = float(assets) - liab_plus_equity
            base = max(abs(float(assets)), abs(liab_plus_equity))
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
                    "total_assets": float(assets),
                    "total_liabilities": float(liabilities),
                    "total_equity": float(equity),
                }
        result.by_year[str(year)] = payload
    return result


def _check_bs_subline_reconciliation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
    presentation_tree: PresentationTree | None = None,
) -> BSSublineCheck:
    if presentation_tree is None:
        return _check_bs_subline_reconciliation_legacy(
            model,
            historical_years=historical_years,
            taxonomy=taxonomy,
            tolerances=tolerances,
        )
    return _check_bs_subline_reconciliation_presentation(
        model,
        historical_years=historical_years,
        taxonomy=taxonomy,
        tolerances=tolerances,
        presentation_tree=presentation_tree,
    )


def _check_bs_subline_reconciliation_legacy(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
) -> BSSublineCheck:
    result = BSSublineCheck()
    value_memo: dict[tuple[str, int], Optional[float]] = {}

    for section_name, definition in BS_SECTIONS.items():
        if definition.get("presentation_only"):
            continue
        section_members = _effective_section_members(model, definition)
        section_payload: dict[str, Any] = {"by_year": {}, "coverage_findings": []}

        for year in historical_years:
            findings: list[dict[str, Any]] = []
            sub_lines_sum = 0.0

            for member in section_members:
                item = model.get_item(member.template_item_id)
                expected_concept_id = member.expected_concept_id or item.data_concept_id
                value = _observed_value(model, item, year, value_memo)
                if value is not None:
                    sub_lines_sum += float(value)
                    continue

                finding: dict[str, Any] = {
                    "template_item": member.template_item_id,
                    "expected_concept_id": expected_concept_id,
                    "severity": "gap",
                }
                if expected_concept_id is None:
                    finding["kind"] = "missing_mapping"
                else:
                    finding["kind"] = "missing_concept"
                    mapping = taxonomy.get(expected_concept_id)
                    detail: dict[str, Any] = {}
                    if (
                        mapping is not None
                        and mapping.nonadmissible_reason_code is not None
                    ):
                        detail["nonadmissible_reason_code"] = str(
                            mapping.nonadmissible_reason_code.value
                        )
                    if detail:
                        finding["detail"] = detail
                findings.append(finding)

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


def _check_bs_subline_reconciliation_presentation(
    model: FinancialModel,
    *,
    historical_years: list[int],
    taxonomy: dict[str, DataSourceMapping],
    tolerances: DiagnosticTolerances,
    presentation_tree: PresentationTree,
) -> BSSublineCheck:
    result = BSSublineCheck()
    tag_to_concept = _build_taxonomy_tag_index(taxonomy)
    concept_sections = _concept_sections_by_id(model)
    coverage_per_section: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for section_name, definition in BS_SECTIONS.items():
        parent_candidates = tuple(definition.get("xbrl_section_parents", ()))
        if not parent_candidates:
            continue
        section_members = _effective_section_members(model, definition)

        section_payload: dict[str, Any] = {"by_year": {}}

        def parent_present(tag: str) -> bool:
            return bool(presentation_tree.immediate_children_of(tag))

        selected_candidate: ParentCandidate | None = None
        children: tuple[PresentationChild, ...] = ()
        for candidate in parent_candidates:
            if candidate.requires_companion and not parent_present(
                candidate.requires_companion
            ):
                continue
            if candidate.requires_no_abstract and any(
                parent_present(tag) for tag in candidate.requires_no_abstract
            ):
                continue
            candidate_children = presentation_tree.immediate_children_of(
                candidate.parent
            )
            if candidate_children:
                selected_candidate = candidate
                children = candidate_children
                break

        if selected_candidate is None:
            if definition.get("emit_missing_parent", True) is False:
                continue
            for year in historical_years:
                section_payload["by_year"][str(year)] = {
                    "severity": "ok",
                    "kind": "tree_missing_parent",
                    "xbrl_section_parents_tried": [
                        candidate.parent for candidate in parent_candidates
                    ],
                    "sub_lines_sum": None,
                    "total_reported": None,
                    "delta": None,
                    "delta_pct": None,
                    "sign_metadata_notes": [],
                    "findings": [],
                }
            section_payload["coverage_findings"] = []
            result.by_section[section_name] = section_payload
            continue

        exclude_tags = selected_candidate.exclude_tags

        for year in historical_years:
            findings: list[dict[str, Any]] = []
            template_value_memo: dict[tuple[str, int], Optional[float]] = {}

            selected_children = _select_non_overlapping_presentation_children(
                children,
                year=year,
                section_total_tags=exclude_tags,
                definition=definition,
                section_members=section_members,
                tag_to_concept=tag_to_concept,
                model=model,
                template_value_memo=template_value_memo,
                parent_tag=selected_candidate.parent,
            )

            sub_lines_sum = 0.0
            children_with_year_value = 0
            sign_metadata_notes: list[dict] = []
            for child in selected_children:
                concept_id = tag_to_concept.get(child.tag)
                member = _section_member_for_concept(
                    model,
                    section_members,
                    concept_id,
                )
                owning_sections = concept_sections.get(concept_id or "", set())
                if (
                    concept_id is not None
                    and member is None
                    and owning_sections
                    and section_name not in owning_sections
                ):
                    findings.append(
                        {
                            "kind": "excluded_out_of_section_concept",
                            "severity": "ok",
                            "xbrl_tag": child.tag,
                            "expected_concept_id": concept_id,
                            "expected_sections": sorted(owning_sections),
                        }
                    )
                    continue

                contribution = _resolve_signed_contribution(
                    child,
                    year=year,
                    parent_tag=selected_candidate.parent,
                    cv_disagreement_log=sign_metadata_notes,
                )
                if contribution is not None:
                    sub_lines_sum += float(contribution)
                    children_with_year_value += 1

                if (
                    concept_id is not None
                    and member is None
                    and contribution is not None
                ):
                    coverage_per_section[section_name].append(
                        {
                            "kind": "unmapped_xbrl_concept",
                            "xbrl_tag": child.tag,
                            "expected_concept_id": concept_id,
                            "year": year,
                            "value_observed": contribution,
                            "severity": "ok",
                        }
                    )
                elif member is not None:
                    item = model.get_item(member.template_item_id)
                    template_value = _observed_value(
                        model, item, year, template_value_memo
                    )
                    if template_value is None and contribution is not None:
                        findings.append(
                            {
                                "template_item": member.template_item_id,
                                "expected_concept_id": concept_id,
                                "kind": "missing_concept",
                                "severity": "gap",
                                "xbrl_tag": child.tag,
                                "value_xbrl": contribution,
                            }
                        )

            if children_with_year_value == 0:
                section_payload["by_year"][str(year)] = {
                    "severity": "ok",
                    "kind": "tree_missing_year",
                    "xbrl_section_parent": selected_candidate.parent,
                    "year": year,
                    "sub_lines_sum": None,
                    "total_reported": None,
                    "delta": None,
                    "delta_pct": None,
                    "sign_metadata_notes": sign_metadata_notes,
                    "findings": findings,
                }
                continue

            total_reported = _resolve_section_total(
                model, definition, year, template_value_memo
            )
            payload: dict[str, Any] = {
                "sub_lines_sum": sub_lines_sum,
                "total_reported": total_reported,
                "delta": None,
                "delta_pct": None,
                "severity": "ok",
                "kind": None,
                "sign_metadata_notes": sign_metadata_notes,
                "findings": findings,
            }

            if total_reported is None:
                payload["severity"] = "gap"
                payload["kind"] = "insufficient_inputs"
            else:
                delta = float(total_reported) - sub_lines_sum
                abs_delta_pct = _ratio(abs(delta), float(total_reported))
                payload["delta"] = delta
                payload["delta_pct"] = abs_delta_pct * 100.0
                if abs_delta_pct >= tolerances.bs_subline_material_pct:
                    payload["severity"] = "material_gap"
                elif abs_delta_pct >= tolerances.bs_subline_gap_pct:
                    payload["severity"] = "gap"

            section_payload["by_year"][str(year)] = payload

        section_payload["coverage_findings"] = sorted(
            coverage_per_section[section_name],
            key=lambda finding: abs(finding.get("value_observed") or 0.0),
            reverse=True,
        )[:_COVERAGE_FINDING_TOP_N]
        result.by_section[section_name] = section_payload

    return result


def _values_match_magnitude(a: float, b: float) -> bool:
    return math.isclose(
        abs(float(a)),
        abs(float(b)),
        rel_tol=_CV_CPV_REL_TOL,
        abs_tol=_CV_CPV_ABS_TOL,
    )


def _resolve_signed_contribution(
    child: PresentationChild,
    *,
    year: int,
    parent_tag: str,
    cv_disagreement_log: list[dict] | None = None,
) -> float | None:
    """Return the child's signed contribution to its parent rollup."""

    obs = child.observation_by_year.get(year)
    legacy = child.value_by_year.get(year)

    if obs is None or (obs.current_value is None and obs.current_period_value is None):
        return float(legacy) if legacy is not None else None

    cv = obs.current_value
    cpv = obs.current_period_value

    if cv is not None and cpv is not None:
        if _values_match_magnitude(cv, cpv):
            cv_sign = math.copysign(1.0, cv) if cv != 0 else 0.0
            cpv_sign = math.copysign(1.0, cpv) if cpv != 0 else 0.0
            opposite_signs = cv_sign != cpv_sign and cv_sign != 0.0 and cpv_sign != 0.0
            if opposite_signs:
                weight = obs.calculation_weight
                if weight is not None:
                    signed = float(weight) * float(cpv)
                    if not math.isclose(
                        float(cv),
                        signed,
                        rel_tol=_CV_CPV_REL_TOL,
                        abs_tol=_CV_CPV_ABS_TOL,
                    ):
                        if cv_disagreement_log is not None:
                            cv_disagreement_log.append(
                                {
                                    "tag": child.tag,
                                    "year": year,
                                    "parent_tag": parent_tag,
                                    "cv": cv,
                                    "cpv": cpv,
                                    "weight": weight,
                                    "weight_x_cpv": signed,
                                    "note": "cv disagrees with weight*cpv; preferring weight",
                                }
                            )
                    return signed
                return float(cv)

            weight = _resolve_rollup_weight(
                child,
                year=year,
                parent_tag=parent_tag,
            )
            return weight * float(cpv)

        if cv_disagreement_log is not None:
            cv_disagreement_log.append(
                {
                    "tag": child.tag,
                    "year": year,
                    "parent_tag": parent_tag,
                    "cv": cv,
                    "cpv": cpv,
                    "note": "cv/cpv magnitude divergence; using cpv with weight resolution",
                }
            )
        weight = _resolve_rollup_weight(child, year=year, parent_tag=parent_tag)
        return weight * float(cpv)

    if cpv is not None and cv is None:
        weight = _resolve_rollup_weight(child, year=year, parent_tag=parent_tag)
        return weight * float(cpv)

    if cv is not None and cpv is None:
        if cv > 0:
            negated_label = (
                obs.preferred_label_role is not None
                and "negated" in obs.preferred_label_role.lower()
            )
            if child.tag in _KNOWN_CONTRA_BS_FACE_TAGS or negated_label:
                return -abs(float(cv))
        return float(cv)

    return None


def _resolve_rollup_weight(
    child: PresentationChild,
    *,
    year: int,
    parent_tag: str,
) -> float:
    """Determine the calculation-linkbase sign for a child under its parent."""

    _ = parent_tag
    obs = child.observation_by_year.get(year)
    if obs is not None:
        if obs.calculation_weight is not None:
            return float(obs.calculation_weight)
        preferred_label_role = (obs.preferred_label_role or "").lower()
        if "negated" in preferred_label_role:
            return -1.0

    if child.tag in _KNOWN_CONTRA_BS_FACE_TAGS:
        return -1.0
    return 1.0


def _select_non_overlapping_presentation_children(
    children: tuple[PresentationChild, ...],
    *,
    year: int,
    section_total_tags: tuple[str, ...],
    definition: dict[str, Any],
    section_members: list[SectionMember],
    tag_to_concept: dict[str, str],
    model: FinancialModel,
    template_value_memo: dict[tuple[str, int], Optional[float]],
    parent_tag: str,
) -> list[PresentationChild]:
    """Select an additive basis from ordered presentation children.

    Some filers present both leaf concepts and intermediate subtotals as siblings
    under the same balance-sheet section abstract. Reconciliation needs exactly
    one basis: either the template-mapped roll-up or its components, not both.
    """

    selected: list[PresentationChild] = []
    for child in children:
        if any(_same_xbrl_tag(child.tag, tag) for tag in section_total_tags):
            continue
        contribution = _resolve_signed_contribution(
            child,
            year=year,
            parent_tag=parent_tag,
        )
        if contribution is None:
            continue

        rollup_slice = _find_rollup_component_slice(
            selected,
            year,
            float(contribution),
            parent_tag=parent_tag,
        )
        if rollup_slice is None:
            selected.append(child)
            continue

        start, end = rollup_slice
        components = selected[start:end]
        if _prefer_rollup_child(
            child,
            components,
            year=year,
            definition=definition,
            section_members=section_members,
            tag_to_concept=tag_to_concept,
            model=model,
            template_value_memo=template_value_memo,
        ):
            selected = selected[:start] + [child] + selected[end:]

    return selected


def _find_rollup_component_slice(
    selected: list[PresentationChild],
    year: int,
    target_value: float,
    *,
    parent_tag: str,
) -> tuple[int, int] | None:
    if len(selected) < 2:
        return None

    running_sum = 0.0
    for start in range(len(selected) - 1, -1, -1):
        contribution = _resolve_signed_contribution(
            selected[start],
            year=year,
            parent_tag=parent_tag,
        )
        if contribution is None:
            return None
        running_sum += float(contribution)
        if len(selected) - start >= 2 and _presentation_values_equal(
            running_sum, target_value
        ):
            return (start, len(selected))
    return None


def _prefer_rollup_child(
    child: PresentationChild,
    components: list[PresentationChild],
    *,
    year: int,
    definition: dict[str, Any],
    section_members: list[SectionMember],
    tag_to_concept: dict[str, str],
    model: FinancialModel,
    template_value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    rollup_member = _section_member_for_concept(
        model,
        section_members,
        tag_to_concept.get(child.tag),
    )
    if rollup_member is None:
        return False

    rollup_item = model.get_item(rollup_member.template_item_id)
    if _observed_value(model, rollup_item, year, template_value_memo) is not None:
        return True

    for component in components:
        component_member = _section_member_for_concept(
            model,
            section_members,
            tag_to_concept.get(component.tag),
        )
        if component_member is None:
            continue
        component_item = model.get_item(component_member.template_item_id)
        if (
            _observed_value(model, component_item, year, template_value_memo)
            is not None
        ):
            return False

    return True


def _presentation_values_equal(left: float, right: float) -> bool:
    tolerance = max(1.0, abs(right) * 0.001)
    return abs(left - right) <= tolerance


def _same_xbrl_tag(left: str | None, right: str | None) -> bool:
    if left is None or right is None:
        return False
    return bool(_taxonomy_tag_keys(left) & _taxonomy_tag_keys(right))


def _build_taxonomy_tag_index(taxonomy: dict[str, DataSourceMapping]) -> dict[str, str]:
    candidates_by_tag: dict[str, list[DataSourceMapping]] = defaultdict(list)
    for concept_id, mapping in taxonomy.items():
        tags = list(mapping.edgar_tags or [])
        if mapping.canonical_tag:
            tags.append(str(mapping.canonical_tag))
        for tag in tags:
            for key in _taxonomy_tag_keys(str(tag)):
                candidates_by_tag[key].append(mapping)

    index: dict[str, str] = {}
    for tag, candidates in candidates_by_tag.items():
        unique_by_concept = {
            candidate.concept_id: candidate for candidate in candidates
        }
        unique_candidates = sorted(
            unique_by_concept.values(), key=lambda candidate: candidate.concept_id
        )
        edgar_preferred = [
            candidate
            for candidate in unique_candidates
            if str(candidate.preferred_source or "").lower() == "edgar"
        ]
        best_candidates = edgar_preferred or unique_candidates
        if len(best_candidates) > 1:
            logger.warning(
                "Taxonomy EDGAR tag collision for %s; choosing %s from %s",
                tag,
                best_candidates[0].concept_id,
                [candidate.concept_id for candidate in best_candidates],
            )
        index[tag] = best_candidates[0].concept_id
    return index


def _taxonomy_tag_keys(tag: str) -> set[str]:
    cleaned = str(tag).strip()
    if not cleaned:
        return set()
    keys = {cleaned}
    if ":" in cleaned:
        keys.add(cleaned.split(":", 1)[-1])
    else:
        keys.add(f"us-gaap:{cleaned}")
    return keys


def _section_member_for_concept(
    model: FinancialModel,
    members: list[SectionMember],
    concept_id: str | None,
) -> SectionMember | None:
    if concept_id is None:
        return None
    for member in members:
        if member.expected_concept_id == concept_id:
            return member
        item = model.get_item(member.template_item_id)
        if item.data_concept_id == concept_id:
            return member
    return None


def _concept_sections_by_id(model: FinancialModel) -> dict[str, set[str]]:
    sections_by_concept: dict[str, set[str]] = defaultdict(set)
    for section_name, definition in BS_SECTIONS.items():
        if definition.get("presentation_only"):
            continue
        for member in _effective_section_members(model, definition):
            concept_id = member.expected_concept_id
            if concept_id is None:
                item = model.get_item(member.template_item_id)
                concept_id = item.data_concept_id
            if concept_id:
                sections_by_concept[str(concept_id)].add(section_name)
    return dict(sections_by_concept)


def _effective_section_members(
    model: FinancialModel,
    definition: dict[str, Any],
) -> list[SectionMember]:
    members = list(definition["sub_lines"])
    member_ids = {member.template_item_id for member in members}
    if not members:
        return members

    try:
        member_rows = [
            int(model.get_item(member.template_item_id).row) for member in members
        ]
        total_row = int(model.get_item(definition["total_item_id"]).row)
    except KeyError:
        return members
    min_member_row = min(member_rows)
    member_concept_ids = {
        concept_id
        for member in members
        for concept_id in (
            member.expected_concept_id,
            model.get_item(member.template_item_id).data_concept_id,
        )
        if concept_id
    }

    for item in _iter_items(model):
        if (
            item.id in member_ids
            or not item.data_concept_id
            or item.data_concept_id in member_concept_ids
        ):
            continue
        if min_member_row <= int(item.row) < total_row:
            members.append(SectionMember(item.id, item.data_concept_id))
            member_ids.add(item.id)
            member_concept_ids.add(item.data_concept_id)
    return sorted(
        members, key=lambda member: int(model.get_item(member.template_item_id).row)
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
    "_COVERAGE_FINDING_TOP_N",
    "_CV_CPV_REL_TOL",
    "_CV_CPV_ABS_TOL",
    "_KNOWN_CONTRA_BS_FACE_TAGS",
    "_check_bs_balance",
    "_check_bs_subline_reconciliation",
    "_check_bs_subline_reconciliation_legacy",
    "_check_bs_subline_reconciliation_presentation",
    "_values_match_magnitude",
    "_resolve_signed_contribution",
    "_resolve_rollup_weight",
    "_select_non_overlapping_presentation_children",
    "_find_rollup_component_slice",
    "_prefer_rollup_child",
    "_presentation_values_equal",
    "_same_xbrl_tag",
    "_build_taxonomy_tag_index",
    "_taxonomy_tag_keys",
    "_section_member_for_concept",
    "_concept_sections_by_id",
    "_effective_section_members",
    "_resolve_section_total",
]
