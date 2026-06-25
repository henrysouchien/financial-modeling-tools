"""Presentation residualization helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import Dict

from .build_diagnostics import (
    BS_SECTIONS as _DIAGNOSTIC_BS_SECTIONS,
    _build_taxonomy_tag_index as _diagnostic_build_taxonomy_tag_index,
    _effective_section_members as _diagnostic_effective_section_members,
    _select_non_overlapping_presentation_children as _diagnostic_select_children,
)
from .build_formula_eval import _lookup_formula_value as _formula_lookup_formula_value
from .models import DataSourceMapping, FinancialModel, LineItem
from .presentation_tree import PresentationTree


_PRESENTATION_RESIDUAL_NOTE = "presentation_residual"
_NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID = "tpl.fm.balance_sheet.long_term_asset_2"


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _apply_presentation_catch_all_residuals(
    model: FinancialModel,
    *,
    taxonomy: Dict[str, DataSourceMapping],
    presentation_tree: PresentationTree,
    historical_periods: list[int],
) -> dict[str, set[int]]:
    """Residualize broad catch-all rows when presentation data proves overlap.

    Some filers populate both a broad catch-all fact and dedicated sub-lines. The
    presentation-tree walker can reconcile the section by choosing a non-
    overlapping basis, but the static model formula would still add every
    populated template row. Residualizing the catch-all before formula-first keeps
    explicit rows visible while preserving section formulas.
    """

    residualize = _parent_attr(
        "_residualize_bs_section_catch_all",
        _residualize_bs_section_catch_all,
    )
    return residualize(
        model,
        taxonomy=taxonomy,
        presentation_tree=presentation_tree,
        historical_periods=historical_periods,
        section_name="non_current_assets",
        catch_all_item_id=_parent_attr(
            "_NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID",
            _NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID,
        ),
    )


def _residualize_bs_section_catch_all(
    model: FinancialModel,
    *,
    taxonomy: Dict[str, DataSourceMapping],
    presentation_tree: PresentationTree,
    historical_periods: list[int],
    section_name: str,
    catch_all_item_id: str,
) -> dict[str, set[int]]:
    definition = _parent_attr("BS_SECTIONS", _DIAGNOSTIC_BS_SECTIONS).get(section_name)
    if not definition:
        return {}

    select_basis = _parent_attr(
        "_select_bs_section_presentation_basis",
        _select_bs_section_presentation_basis,
    )
    selected = select_basis(
        model,
        taxonomy=taxonomy,
        presentation_tree=presentation_tree,
        definition=definition,
        historical_periods=historical_periods,
    )
    if not selected:
        return {}

    catch_all_item = model.get_item(catch_all_item_id)
    tag_to_concept = _parent_attr(
        "_build_taxonomy_tag_index",
        _diagnostic_build_taxonomy_tag_index,
    )(taxonomy)
    lookup_formula_value = _parent_attr(
        "_lookup_formula_value",
        _formula_lookup_formula_value,
    )
    bs_section_total_value = _parent_attr("_bs_section_total_value", _bs_section_total_value)
    bs_section_subline_sum = _parent_attr("_bs_section_subline_sum", _bs_section_subline_sum)
    set_residualized_value = _parent_attr("_set_residualized_value", _set_residualized_value)
    adjusted: dict[str, set[int]] = {}

    for year, children in selected.items():
        selected_concepts = {
            concept_id
            for child in children
            for concept_id in [tag_to_concept.get(child.tag)]
            if concept_id is not None
        }
        if catch_all_item.data_concept_id not in selected_concepts:
            continue

        unselected_explicit_sum = 0.0
        for member in definition["sub_lines"]:
            if member.template_item_id == catch_all_item_id:
                continue
            if member.expected_concept_id is None:
                continue
            if member.expected_concept_id in selected_concepts:
                continue
            value = lookup_formula_value(model, member.template_item_id, int(year), {})
            if value is not None:
                unselected_explicit_sum += float(value)

        if abs(unselected_explicit_sum) < 1e-9:
            continue

        catch_all_value = lookup_formula_value(model, catch_all_item_id, int(year), {})
        section_total = bs_section_total_value(model, definition, int(year))
        section_sum = bs_section_subline_sum(model, definition, int(year))
        if catch_all_value is None or section_total is None or section_sum is None:
            continue

        overage = float(section_sum) - float(section_total)
        tolerance = max(1.0, abs(float(section_total)) * 0.001)
        if abs(overage - unselected_explicit_sum) > tolerance:
            continue

        residual_value = float(catch_all_value) - overage
        if residual_value < -tolerance or residual_value > float(catch_all_value) + tolerance:
            continue

        set_residualized_value(catch_all_item, int(year), residual_value)
        adjusted.setdefault(catch_all_item.id, set()).add(int(year))

    return adjusted


def _select_bs_section_presentation_basis(
    model: FinancialModel,
    *,
    taxonomy: Dict[str, DataSourceMapping],
    presentation_tree: PresentationTree,
    definition: dict,
    historical_periods: list[int],
) -> dict[int, list]:
    parent_candidates = tuple(definition.get("xbrl_section_parents", ()))
    tag_to_concept = _parent_attr(
        "_build_taxonomy_tag_index",
        _diagnostic_build_taxonomy_tag_index,
    )(taxonomy)

    def parent_present(tag: str) -> bool:
        return bool(presentation_tree.immediate_children_of(tag))

    selected_candidate = None
    children = ()
    for candidate in parent_candidates:
        if candidate.requires_companion and not parent_present(candidate.requires_companion):
            continue
        candidate_children = presentation_tree.immediate_children_of(candidate.parent)
        if candidate_children:
            selected_candidate = candidate
            children = candidate_children
            break

    if selected_candidate is None:
        return {}

    select_children = _parent_attr(
        "_select_non_overlapping_presentation_children",
        _diagnostic_select_children,
    )
    effective_section_members = _parent_attr(
        "_effective_section_members",
        _diagnostic_effective_section_members,
    )
    return {
        int(year): select_children(
            children,
            year=int(year),
            section_total_tags=selected_candidate.exclude_tags,
            definition=definition,
            section_members=effective_section_members(model, definition),
            tag_to_concept=tag_to_concept,
            model=model,
            template_value_memo={},
            parent_tag=selected_candidate.parent,
        )
        for year in historical_periods
    }


def _bs_section_total_value(
    model: FinancialModel,
    definition: dict,
    year: int,
) -> float | None:
    lookup_formula_value = _parent_attr(
        "_lookup_formula_value",
        _formula_lookup_formula_value,
    )
    total = lookup_formula_value(model, definition["total_item_id"], int(year), {})
    if total is None:
        return None
    included_subtotal_id = definition.get("also_includes_subtotal")
    if not included_subtotal_id:
        return float(total)
    included = lookup_formula_value(model, included_subtotal_id, int(year), {})
    if included is None:
        return None
    return float(total) - float(included)


def _bs_section_subline_sum(
    model: FinancialModel,
    definition: dict,
    year: int,
) -> float | None:
    lookup_formula_value = _parent_attr(
        "_lookup_formula_value",
        _formula_lookup_formula_value,
    )
    total = 0.0
    saw_value = False
    for member in definition["sub_lines"]:
        value = lookup_formula_value(model, member.template_item_id, int(year), {})
        if value is None:
            continue
        saw_value = True
        total += float(value)
    return total if saw_value else None


def _set_residualized_value(item: LineItem, year: int, value: float) -> None:
    residual_note = _parent_attr("_PRESENTATION_RESIDUAL_NOTE", _PRESENTATION_RESIDUAL_NOTE)
    if item.values is not None and int(year) in item.values.values:
        cell = item.values.values[int(year)]
        item.values.values[int(year)] = cell.model_copy(
            update={
                "value": float(value),
                "note": residual_note,
            }
        )
        return

    if item.overrides is not None and int(year) in item.overrides:
        spec = item.overrides[int(year)]
        item.overrides[int(year)] = spec.model_copy(
            update={
                "params": {**dict(spec.params or {}), "value": float(value)},
                "note": residual_note,
            }
        )


__all__ = [
    "_NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID",
    "_PRESENTATION_RESIDUAL_NOTE",
    "_apply_presentation_catch_all_residuals",
    "_bs_section_subline_sum",
    "_bs_section_total_value",
    "_residualize_bs_section_catch_all",
    "_select_bs_section_presentation_basis",
    "_set_residualized_value",
]
