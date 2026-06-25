"""Cash-flow linkage helpers for semantic row materialization."""

from __future__ import annotations

import sys
from typing import Any

from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    Unit,
)
from .overrides import TickerOverrides


_PARENT_MODULES = ("schema.build", "schema.build_semantic_rows")
_MISSING = object()


def _compat(name: str, fallback: Any = _MISSING) -> Any:
    first_available = fallback
    for module_name in _PARENT_MODULES:
        parent = sys.modules.get(module_name)
        if parent is not None and hasattr(parent, name):
            value = getattr(parent, name)
            if first_available is fallback:
                first_available = value
            if fallback is _MISSING or value is not fallback:
                return value
    if first_available is _MISSING:
        raise AttributeError(name)
    return first_available


def _periods_for_delta_formula(model: FinancialModel) -> list[int]:
    historical = [int(period) for period in model.time_structure.historical_periods]
    projection = [int(period) for period in model.time_structure.projection_periods]
    return historical[1:] + projection


def _bind_or_insert_cf_linkage_row(
    model: FinancialModel,
    *,
    target_policy: dict[str, Any],
    default_id: str,
    label: str,
) -> LineItem:
    preferred_target_id = target_policy.get("preferred_target_item_id")
    if isinstance(preferred_target_id, str) and preferred_target_id.strip():
        item = model.get_item(preferred_target_id.strip())
        if item.data_concept_id not in (None, default_id):
            raise ValueError(
                f"cash-flow linkage target {item.id!r} is occupied by "
                f"{item.data_concept_id!r}"
            )
        item.label = label
        return item

    stable_id = str(target_policy.get("stable_item_id") or default_id)
    try:
        item = model.get_item(stable_id)
        item.label = label
        return item
    except KeyError:
        pass

    insert_after_id = target_policy.get("insert_after_item_id")
    if not isinstance(insert_after_id, str) or not insert_after_id.strip():
        raise ValueError("cash-flow linkage insert target missing insert_after_item_id")
    line_item_cls = _compat("LineItem", LineItem)
    item_type = _compat("ItemType", ItemType)
    unit_cls = _compat("Unit", Unit)
    item = line_item_cls(
        id=stable_id,
        label=label,
        row=0,
        item_type=item_type.derived,
        unit=unit_cls.dollars,
    )
    insert_line_item_after = _compat("_insert_line_item_after")
    return insert_line_item_after(
        model,
        anchor_item_id=insert_after_id.strip(),
        new_item=item,
    )


def _apply_semantic_cash_flow_linkages(
    model: FinancialModel,
    *,
    ticker: str,
    overrides: TickerOverrides,
    materialized_by_concept: dict[str, str],
    result: Any,
) -> None:
    semantic_slug = _compat("_semantic_slug")
    bind_or_insert_cf_linkage_row = _compat(
        "_bind_or_insert_cf_linkage_row",
        _bind_or_insert_cf_linkage_row,
    )
    formula_spec_cls = _compat("FormulaSpec", FormulaSpec)
    formula_type = _compat("FormulaType", FormulaType)
    line_item_ref_cls = _compat("LineItemRef", LineItemRef)
    periods_for_delta_formula = _compat(
        "_periods_for_delta_formula",
        _periods_for_delta_formula,
    )
    semantic_append_note = _compat("_semantic_append_note")
    add_formula_ref_after = _compat("_add_formula_ref_after")
    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        source_item_id = materialized_by_concept.get(concept_id)
        if not source_item_id:
            continue
        linkage = entry.get("cash_flow_linkage")
        if not isinstance(linkage, dict):
            continue
        linkage_type = str(linkage.get("type") or "").strip()
        target_policy = linkage.get("target_row_policy")
        if not isinstance(target_policy, dict):
            target_policy = {}

        if linkage_type == "cash_bridge_adjustment":
            label = str(
                target_policy.get("target_label")
                or linkage.get("target_label")
                or entry.get("target_label")
                or concept_id
            )
            target = bind_or_insert_cf_linkage_row(
                model,
                target_policy=target_policy,
                default_id=(
                    f"fm.semantic.{ticker.lower()}.{semantic_slug(concept_id)}."
                    "cash_bridge"
                ),
                label=label,
            )
            target.historical = formula_spec_cls(
                type=formula_type.ref,
                params={"source": line_item_ref_cls(id=source_item_id, t=0)},
            )
            target.projected = formula_spec_cls(
                type=formula_type.ref,
                params={"source": line_item_ref_cls(id=source_item_id, t=0)},
            )
            target.formula_periods = sorted(
                {
                    int(period)
                    for period in (
                        list(model.time_structure.historical_periods)
                        + list(model.time_structure.projection_periods)
                    )
                }
            )
            semantic_append_note(
                target,
                f"Semantic cash-flow linkage from {source_item_id}.",
            )
            result.linkages.append(
                {
                    "concept_id": concept_id,
                    "type": linkage_type,
                    "item_id": target.id,
                    "source_item_id": source_item_id,
                }
            )
            continue

        if linkage_type == "financing_liability_delta":
            label = str(
                target_policy.get("target_label")
                or linkage.get("target_label")
                or "Net change in liability"
            )
            target = bind_or_insert_cf_linkage_row(
                model,
                target_policy=target_policy,
                default_id=(
                    f"fm.semantic.{ticker.lower()}.net_change_in_"
                    f"{semantic_slug(concept_id)}"
                ),
                label=label,
            )
            target.historical = formula_spec_cls(
                type=formula_type.arithmetic,
                params={
                    "operands": [
                        "-",
                        line_item_ref_cls(id=source_item_id, t=0),
                        line_item_ref_cls(id=source_item_id, t=-1),
                    ]
                },
            )
            target.projected = target.historical.model_copy(deep=True)
            target.formula_periods = periods_for_delta_formula(model)
            semantic_append_note(
                target,
                (
                    "Semantic financing liability delta from "
                    f"{source_item_id}; sign=current_minus_prior."
                ),
            )
            insert_after = target_policy.get("formula_insert_after_item_id") or (
                target_policy.get("insert_after_item_id")
            )
            if not isinstance(insert_after, str) or not insert_after.strip():
                insert_after = "tpl.fm.cash_flow.other_cash_flows_from_financing"
            add_formula_ref_after(
                model,
                formula_item_id="tpl.fm.cash_flow.financing_cash_flow",
                after_id=insert_after.strip(),
                new_id=target.id,
            )
            result.linkages.append(
                {
                    "concept_id": concept_id,
                    "type": linkage_type,
                    "item_id": target.id,
                    "source_item_id": source_item_id,
                }
            )
            continue

        result.gaps.append(
            {
                "concept_id": concept_id,
                "kind": "unsupported_cash_flow_linkage",
                "type": linkage_type,
            }
        )


__all__ = [
    "_apply_semantic_cash_flow_linkages",
    "_bind_or_insert_cf_linkage_row",
    "_periods_for_delta_formula",
]
