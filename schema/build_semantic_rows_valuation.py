"""Valuation linkage helpers for semantic row materialization."""

from __future__ import annotations

from typing import Any

from .build_semantic_rows import _semantic_append_note
from .models import FinancialModel, FormulaSpec, FormulaType, LineItemRef
from .overrides import TickerOverrides


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
    materialized_by_role: dict[str, str],
    result: Any,
) -> None:
    client_fund_obligation: tuple[str, str] | None = None
    client_funds_asset: tuple[str, str] | None = None
    for required_role, entry in sorted((overrides.semantic_rows or {}).items()):
        if not isinstance(entry, dict):
            continue
        item_id = materialized_by_role.get(required_role)
        if not item_id:
            continue
        role = _semantic_role(entry)
        if role == "client_fund_obligation":
            client_fund_obligation = (required_role, item_id)
        elif role == "client_funds_asset":
            client_funds_asset = (required_role, item_id)

    adjustment = client_fund_obligation or client_funds_asset
    if adjustment is None:
        return
    adjustment_required_role, adjustment_item_id = adjustment

    try:
        current_net_debt = model.get_item("tpl.v.current_valuation.net_debt")
        forward_net_cash = model.get_item("tpl.v.forward_ev_ebitda.net_debt_fy2")
    except KeyError as exc:
        result.gaps.append(
            {
                "required_role": adjustment_required_role,
                "kind": "semantic_valuation_linkage_missing_target",
                "missing_item_id": str(exc),
            }
        )
        return

    # The client-funds asset and obligation rows are separately presented on
    # the balance sheet.  ``cash_and_marketable_securities`` therefore remains
    # corporate cash, and neither client-funds gross balance belongs in the
    # enterprise-to-equity bridge.  The legacy linkage added the full client
    # obligation to net debt and overstated the reference capture's net debt by
    # several billion (the evidence is in the packaged annual corpus under
    # `api/research/corpus/`).
    current_net_debt.projected = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id="tpl.fm.balance_sheet.net_cash"),
            "negate": True,
        },
    )
    _semantic_append_note(
        current_net_debt,
        (
            "Semantic valuation linkage: separately presented client-funds "
            "assets and obligations are excluded from corporate net debt; "
            f"detected {adjustment_item_id}."
        ),
    )

    forward_net_cash.projected = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id="tpl.fm.balance_sheet.net_cash"),
        },
    )
    _semantic_append_note(
        forward_net_cash,
        (
            "Semantic valuation linkage: implied equity bridge uses corporate "
            "net cash and excludes separately presented client-funds balances; "
            f"detected {adjustment_item_id}."
        ),
    )
    result.linkages.append(
        {
            "required_role": adjustment_required_role,
            "type": "valuation_client_funds_net_debt_exclusion",
            "item_id": "tpl.v.current_valuation.net_debt",
            "source_item_id": adjustment_item_id,
            "ticker": ticker,
        }
    )
