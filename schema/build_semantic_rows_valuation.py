"""Valuation linkage helpers for semantic row materialization."""

from __future__ import annotations

import sys
from typing import Any

from .models import FinancialModel, FormulaSpec, FormulaType, LineItemRef
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


def _semantic_role(entry: dict[str, Any]) -> str:
    role = entry.get("semantic_role")
    if isinstance(role, str):
        return role.strip().lower()
    return ""


def _apply_semantic_valuation_linkages(
    model: FinancialModel,
    *,
    ticker: str,
    overrides: TickerOverrides,
    materialized_by_concept: dict[str, str],
    result: Any,
) -> None:
    client_fund_obligation_id: str | None = None
    client_funds_asset_id: str | None = None
    semantic_role = _compat("_semantic_role", _semantic_role)
    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        if not isinstance(entry, dict):
            continue
        item_id = materialized_by_concept.get(concept_id)
        if not item_id:
            continue
        role = semantic_role(entry)
        if role == "client_fund_obligation":
            client_fund_obligation_id = item_id
        elif role == "client_funds_asset":
            client_funds_asset_id = item_id

    adjustment_item_id = client_fund_obligation_id or client_funds_asset_id
    if not adjustment_item_id:
        return

    try:
        current_net_debt = model.get_item("tpl.v.current_valuation.net_debt")
        forward_net_cash = model.get_item("tpl.v.forward_ev_ebitda.net_debt_fy2")
    except KeyError as exc:
        result.gaps.append(
            {
                "concept_id": adjustment_item_id,
                "kind": "semantic_valuation_linkage_missing_target",
                "missing_item_id": str(exc),
            }
        )
        return

    formula_spec_cls = _compat("FormulaSpec", FormulaSpec)
    formula_type = _compat("FormulaType", FormulaType)
    line_item_ref_cls = _compat("LineItemRef", LineItemRef)
    semantic_append_note = _compat("_semantic_append_note")
    current_net_debt.projected = formula_spec_cls(
        type=formula_type.arithmetic,
        params={
            "operands": [
                "-",
                line_item_ref_cls(id=adjustment_item_id),
                line_item_ref_cls(id="tpl.fm.balance_sheet.net_cash"),
            ]
        },
    )
    semantic_append_note(
        current_net_debt,
        (
            "Semantic valuation linkage: client-funds cash is not free cash; "
            f"net debt adds back {adjustment_item_id}."
        ),
    )

    forward_net_cash.projected = formula_spec_cls(
        type=formula_type.arithmetic,
        params={
            "operands": [
                "-",
                line_item_ref_cls(id="tpl.fm.balance_sheet.net_cash"),
                line_item_ref_cls(id=adjustment_item_id),
            ]
        },
    )
    semantic_append_note(
        forward_net_cash,
        (
            "Semantic valuation linkage: implied equity bridge uses net cash "
            f"after client-funds adjustment from {adjustment_item_id}."
        ),
    )
    result.linkages.append(
        {
            "concept_id": adjustment_item_id,
            "type": "valuation_client_funds_net_debt_adjustment",
            "item_id": "tpl.v.current_valuation.net_debt",
            "source_item_id": adjustment_item_id,
            "ticker": ticker,
        }
    )
