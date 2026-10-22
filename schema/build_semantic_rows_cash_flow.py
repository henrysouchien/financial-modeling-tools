"""Cash-flow linkage helpers for semantic row materialization."""

from __future__ import annotations

import logging
from typing import Any

from .build_semantic_rows import (
    _add_formula_ref_after,
    _formula_contains_ref,
    _insert_line_item_after,
    _semantic_append_note,
    _semantic_slug,
)
from .models import (
    FinancialModel,
    FormulaDerivation,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
)
from industry_slice_core.industry_types import Unit
from .overrides import TickerOverrides

logger = logging.getLogger(__name__)


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
    item = LineItem(
        id=stable_id,
        label=label,
        row=0,
        item_type=ItemType.derived,
        unit=Unit.dollars,
    )
    return _insert_line_item_after(
        model,
        anchor_item_id=insert_after_id.strip(),
        new_item=item,
    )


def _apply_semantic_cash_flow_linkages(
    model: FinancialModel,
    *,
    ticker: str,
    overrides: TickerOverrides,
    materialized_by_role: dict[str, str],
    result: Any,
) -> None:
    semantic_slug = _semantic_slug
    bind_or_insert_cf_linkage_row = _bind_or_insert_cf_linkage_row
    formula_spec_cls = FormulaSpec
    formula_derivation_cls = FormulaDerivation
    formula_type = FormulaType
    line_item_ref_cls = LineItemRef
    periods_for_delta_formula = _periods_for_delta_formula
    semantic_append_note = _semantic_append_note
    add_formula_ref_after = _add_formula_ref_after
    for required_role, entry in sorted((overrides.semantic_rows or {}).items()):
        source_item_id = materialized_by_role.get(required_role)
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
                or required_role
            )
            target = bind_or_insert_cf_linkage_row(
                model,
                target_policy=target_policy,
                default_id=(
                    f"fm.semantic.{ticker.lower()}.{semantic_slug(required_role)}."
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
                    "required_role": required_role,
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
                    f"{semantic_slug(required_role)}"
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
                    "required_role": required_role,
                    "type": linkage_type,
                    "item_id": target.id,
                    "source_item_id": source_item_id,
                }
            )
            continue

        if linkage_type == "investing_asset_delta":
            label = str(
                target_policy.get("target_label")
                or linkage.get("target_label")
                or "Net change in asset"
            )
            target = bind_or_insert_cf_linkage_row(
                model,
                target_policy=target_policy,
                default_id=(
                    f"fm.semantic.{ticker.lower()}.net_change_in_"
                    f"{semantic_slug(required_role)}"
                ),
                label=label,
            )
            target.historical = formula_spec_cls(
                type=formula_type.constant,
                params={"value": 0.0},
                note="historical_pass_through_balance_unmodeled",
                derivation=formula_derivation_cls(
                    owner="build_company_grounded_schedules",
                    strategy="pass_through_balance_offset",
                    version=1,
                    dependencies=(source_item_id,),
                ),
            )
            target.projected = formula_spec_cls(
                type=formula_type.arithmetic,
                params={
                    "operands": [
                        "-",
                        line_item_ref_cls(id=source_item_id, t=-1),
                        line_item_ref_cls(id=source_item_id, t=0),
                    ]
                },
            )
            # The historical zero reads no prior balance, so it covers the first
            # historical year too; only the projected delta needs a prior column.
            target.formula_periods = sorted(
                {int(period) for period in model.time_structure.historical_periods}
                | {int(period) for period in model.time_structure.projection_periods}
            )
            semantic_append_note(
                target,
                f"Semantic investing asset delta from {source_item_id}; sign=prior_minus_current.",
            )
            insert_after = target_policy.get("formula_insert_after_item_id") or (
                target_policy.get("insert_after_item_id")
            )
            if not isinstance(insert_after, str) or not insert_after.strip():
                insert_after = "tpl.fm.cash_flow.other_changes_in_investing_activities"
            add_formula_ref_after(
                model,
                formula_item_id="tpl.fm.cash_flow.investing_cash_flow",
                after_id=insert_after.strip(),
                new_id=target.id,
            )
            result.linkages.append(
                {
                    "required_role": required_role,
                    "type": linkage_type,
                    "item_id": target.id,
                    "source_item_id": source_item_id,
                }
            )
            continue

        logger.warning(
            "semantic_row %r cash_flow_linkage skipped as "
            "unsupported_cash_flow_linkage: no handler for linkage type %r "
            "(source item %r); row recorded as gap",
            required_role,
            linkage_type,
            source_item_id,
        )
        result.gaps.append(
            {
                "required_role": required_role,
                "kind": "unsupported_cash_flow_linkage",
                "type": linkage_type,
            }
        )

    # Linkages insert same-period operands into statement subtotal sums after
    # the residual installer ran; the residual identity must subtract them too.
    from .templates.template_builder import _install_residual_histories

    _install_residual_histories(model)


__all__ = [
    "_apply_semantic_cash_flow_linkages",
    "_bind_or_insert_cf_linkage_row",
    "_periods_for_delta_formula",
]
