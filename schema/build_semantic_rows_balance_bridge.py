"""Balance bridge materialization for semantic row builds.

For every catalog balance a business-model driver declares (``balance_of``)
the bridge books the balance the template's mapped rows leave out of its
section total into the balance's catalog ``target_item`` and, when the catalog
names a ``counterpart``, the obligation it is held against. The catalog
(`schema/required_role_catalog.py`, ``CatalogBalance``) owns every role, label
and anchor; this module owns the residual arithmetic.
"""

from __future__ import annotations


from typing import Any, TypeGuard

from .build_diagnostic_sections import BS_SECTIONS
from .build_formula_eval import _constant_override_value as _formula_constant_override_value
from .business_model import declared_balance_role
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
    ValueCell,
    ValueProvenance,
    ValueSeries,
    shift_period as _model_shift_period,
)
from .overrides import TickerOverrides
from .required_role_catalog import CatalogBalance, load_required_role_catalog

from .build_semantic_rows import (
    SemanticRowsResult,
    _bind_or_insert_semantic_row,
    _item_has_any_real_historical_data,
    _semantic_target_is_empty,
)
from .build_semantic_rows_cash_flow import _apply_semantic_cash_flow_linkages
from .build_semantic_rows_valuation import _apply_semantic_valuation_linkages


#: How a pass-through balance's period change reaches the cash flow: an asset
#: change is investing, a liability change financing, each with the sign that
#: makes a larger balance a use of cash (asset) or a source (liability), landing
#: after the template's catch-all row of that activity.
_PASS_THROUGH_CASH_FLOW_LINKAGE = {
    "asset": (
        "investing_asset_delta",
        "prior_minus_current",
        "tpl.fm.cash_flow.other_changes_in_investing_activities",
    ),
    "liability": (
        "financing_liability_delta",
        "current_minus_prior",
        "tpl.fm.cash_flow.other_cash_flows_from_financing",
    ),
}


#: The note on every cell the bridge derives; the identity fill overwrites it
#: with the exact pinned observation when one exists (`schema/build.py`).
BALANCE_BRIDGE_NOTE = "derived_balance_subtotal_bridge"


def _section_for_total(total_item_id: str) -> str | None:
    """The balance-sheet section whose total ``total_item_id`` is, with sub-lines to bridge into."""

    return next(
        (
            section
            for section, config in BS_SECTIONS.items()
            if config.get("total_item_id") == total_item_id
            and not config.get("presentation_only")
        ),
        None,
    )


def _values_are_resolved(
    values: list[float | None],
) -> TypeGuard[list[float]]:
    return all(value is not None for value in values)


def _semantic_constant_value(spec: FormulaSpec | None) -> float | None:
    if spec is not None and spec.note == "synthetic":
        return None
    return _formula_constant_override_value(spec)


def _semantic_shift_period(model: FinancialModel, period: int, t: int) -> int | None:
    shifted = _model_shift_period(int(period), int(t), model.time_structure.period_mode)
    return int(shifted) if shifted is not None else None


def _semantic_observed_value(
    model: FinancialModel,
    item_id: str,
    period: int,
    memo: dict[tuple[str, int], float | None] | None = None,
    stack: set[tuple[str, int]] | None = None,
    t: int = 0,
) -> float | None:
    if int(t) != 0:
        shifted = _semantic_shift_period(model, int(period), int(t))
        if shifted is None:
            return None
        period = shifted
    memo = memo if memo is not None else {}
    key = (str(item_id), int(period))
    if key in memo:
        return memo[key]
    try:
        item = model.get_item(str(item_id))
    except KeyError:
        memo[key] = None
        return None

    if item.overrides is not None and int(period) in item.overrides:
        value = _semantic_constant_value(item.overrides[int(period)])
        if value is not None:
            memo[key] = value
            return value

    if item.values is not None and int(period) in item.values.values:
        value_cell = item.values.values[int(period)]
        if value_cell.value is not None:
            memo[key] = float(value_cell.value)
            return memo[key]

    if item.historical is None:
        memo[key] = None
        return None

    active_stack = stack if stack is not None else set()
    if key in active_stack:
        memo[key] = None
        return None
    active_stack.add(key)
    value = _semantic_evaluate_formula(model, item.historical, int(period), memo, active_stack)
    active_stack.discard(key)
    memo[key] = value
    return value


def _semantic_evaluate_expr(
    model: FinancialModel,
    expr: Any,
    period: int,
    memo: dict[tuple[str, int], float | None],
    stack: set[tuple[str, int]],
) -> float | None:
    if expr is None:
        return None
    if isinstance(expr, bool):
        return float(int(expr))
    if isinstance(expr, (int, float)):
        return float(expr)
    if isinstance(expr, LineItemRef):
        return _semantic_observed_value(
            model,
            expr.id,
            int(period),
            memo,
            stack,
            t=int(getattr(expr, "t", 0) or 0),
        )
    if isinstance(expr, dict):
        if "id" in expr and isinstance(expr.get("id"), str):
            try:
                ref_t = int(expr.get("t", 0) or 0)
            except (TypeError, ValueError):
                ref_t = 0
            return _semantic_observed_value(
                model,
                expr["id"],
                int(period),
                memo,
                stack,
                t=ref_t,
            )
        op = expr.get("op")
        args = list(expr.get("args", []) or [])
        if op in {"+", "SUM"}:
            values = [
                _semantic_evaluate_expr(model, arg, int(period), memo, stack)
                for arg in args
            ]
            if not _values_are_resolved(values):
                return None
            if not values:
                return None
            return sum(values)
        if op == "-":
            if "left" in expr or "right" in expr:
                left = _semantic_evaluate_expr(
                    model,
                    expr.get("left"),
                    int(period),
                    memo,
                    stack,
                )
                right = _semantic_evaluate_expr(
                    model,
                    expr.get("right"),
                    int(period),
                    memo,
                    stack,
                )
                if left is None or right is None:
                    return None
                return left - right
            values = [
                _semantic_evaluate_expr(model, arg, int(period), memo, stack)
                for arg in args
            ]
            if not _values_are_resolved(values):
                return None
            if not values:
                return None
            result = values[0]
            for value in values[1:]:
                result -= value
            return result
    return None


def _semantic_evaluate_formula(
    model: FinancialModel,
    spec: FormulaSpec,
    period: int,
    memo: dict[tuple[str, int], float | None],
    stack: set[tuple[str, int]],
) -> float | None:
    if spec.type is FormulaType.constant:
        return _semantic_constant_value(spec)
    params = spec.params or {}
    if spec.type is FormulaType.ref:
        value = _semantic_evaluate_expr(model, params.get("source"), int(period), memo, stack)
        if value is None:
            return None
        adjustment = params.get("adjustment")
        if adjustment is not None:
            try:
                value += float(adjustment)
            except (TypeError, ValueError):
                pass
        if params.get("negate"):
            value = -value
        return value
    if spec.type is not FormulaType.arithmetic:
        return None
    if "expr" in params:
        return _semantic_evaluate_expr(model, params.get("expr"), int(period), memo, stack)
    values: list[float | None]
    if isinstance(params.get("operands"), list):
        operands = list(params.get("operands") or [])
        operator = "+"
        if operands and isinstance(operands[0], str):
            operator = operands.pop(0)
        values = [
            _semantic_evaluate_expr(model, operand, int(period), memo, stack)
            for operand in operands
        ]
        if not _values_are_resolved(values):
            return None
        if not values:
            return None
        if operator == "-":
            result = values[0]
            for value in values[1:]:
                result -= value
            return result
        if operator == "+":
            return sum(values)
        return None
    values = [
        _semantic_evaluate_expr(model, item, int(period), memo, stack)
        for item in list(params.get("items", []) or [])
    ]
    if not _values_are_resolved(values):
        return None
    if not values:
        return None
    return sum(values)


def _balance_bridge_entry(
    ticker: str,
    balance: CatalogBalance,
) -> tuple[str, dict[str, Any]] | None:
    """``(section, semantic row entry)`` for one balance, or ``None`` when it has no anchor or section."""

    section = _section_for_total(balance.parent_total)
    if section is None or balance.insert_after_item is None:
        return None
    row_policy: dict[str, Any] = {
        "mode": (
            "bind_if_empty_or_insert"
            if balance.target_item
            else "insert_or_bind_same_semantic"
        ),
        "insert_after_item_id": balance.insert_after_item,
        "stable_item_id": balance.semantic_item(ticker),
    }
    if balance.target_item:
        row_policy["preferred_target_item_id"] = balance.target_item
    row_policy["formula_insert_after_item_id"] = balance.insert_after_item
    entry: dict[str, Any] = {
        "statement": "balance_sheet",
        "section": section,
        "semantic_role": balance.semantic_role,
        "target_label": balance.display_name,
        "unit": "dollars",
        "row_policy": row_policy,
        "forecast_policy": {
            "type": "carry_forward",
            "rationale": "derived from reported total less mapped section rows",
        },
        "notes": (
            "Derived bridge row for companies that report section totals inclusive "
            f"of {balance.display_name.lower()} beyond the mapped baseline section rows."
        ),
    }
    if balance.pass_through:
        # A template-owned balance moves through its own working-capital or
        # schedule rows; only a pass-through balance's change is a cash-flow
        # line of its own.
        linkage_type, sign_convention, flow_anchor = _PASS_THROUGH_CASH_FLOW_LINKAGE[
            balance.side
        ]
        entry["cash_flow_linkage"] = {
            "type": linkage_type,
            "sign_convention": sign_convention,
            "target_row_policy": {
                "mode": "insert_or_bind_same_semantic",
                "insert_after_item_id": flow_anchor,
                "stable_item_id": balance.change_item(ticker),
                "target_label": f"Net change in {balance.display_name.lower()}",
            },
        }
    return section, entry


def _declared_balance_roles(business_model: Any | None) -> tuple[str, ...]:
    """The catalog balances the authored revenue driver trees declare, in role order."""

    declared: set[str] = set()

    def collect(node: Any) -> None:
        role = declared_balance_role(node)
        if role is not None:
            declared.add(role)
        for child in getattr(node, "children", None) or ():
            collect(child)

    for segment in getattr(business_model, "segments", None) or ():
        for node in (
            getattr(getattr(segment, "revenue_model", None), "decomposition", None)
            or ()
        ):
            collect(node)
    return tuple(sorted(declared))


def _bridged_balances(business_model: Any | None) -> tuple[CatalogBalance, ...]:
    """Each declared balance, then the counterpart it is held against, once."""

    catalog = load_required_role_catalog()
    ordered: dict[str, CatalogBalance] = {}
    for role in _declared_balance_roles(business_model):
        balance = catalog.balance(role)
        ordered.setdefault(role, balance)
        if balance.counterpart is not None:
            ordered.setdefault(balance.counterpart, catalog.balance(balance.counterpart))
    return tuple(ordered.values())


def _balance_bridge_target_is_available(
    model: FinancialModel,
    *,
    entry: dict[str, Any],
) -> bool:
    row_policy = entry.get("row_policy")
    if not isinstance(row_policy, dict):
        return False
    stable_item_id = row_policy.get("stable_item_id")
    if isinstance(stable_item_id, str) and stable_item_id.strip():
        try:
            stable_item = model.get_item(stable_item_id.strip())
        except KeyError:
            pass
        else:
            if _item_has_any_real_historical_data(model, stable_item):
                return False
    preferred_target_id = row_policy.get("preferred_target_item_id")
    if not isinstance(preferred_target_id, str) or not preferred_target_id.strip():
        return True
    try:
        preferred_item = model.get_item(preferred_target_id.strip())
    except KeyError:
        return True
    return _semantic_target_is_empty(model, preferred_item)


def _balance_bridge_residuals(
    model: FinancialModel,
    *,
    section: str,
    target_item_id: str | None,
    historical_periods: list[int],
) -> dict[int, float]:
    section_config = BS_SECTIONS.get(section)
    if not isinstance(section_config, dict):
        return {}
    total_item_id = section_config.get("total_item_id")
    members = list(section_config.get("sub_lines") or [])
    if not isinstance(total_item_id, str):
        return {}

    residuals: dict[int, float] = {}
    for period in [int(period) for period in historical_periods]:
        memo: dict[tuple[str, int], float | None] = {}
        total_value = _semantic_observed_value(model, total_item_id, period, memo)
        if total_value is None:
            continue

        subline_sum = 0.0
        unresolved = False
        for member in members:
            member_item_id = getattr(member, "template_item_id", None)
            if not isinstance(member_item_id, str):
                continue
            if target_item_id is not None and member_item_id == target_item_id:
                continue
            member_value = _semantic_observed_value(model, member_item_id, period, memo)
            if member_value is not None:
                subline_sum += float(member_value)
                continue
            member_item = model._index.get(member_item_id)
            if (
                member_item is not None
                and member_item.required_role
                and member_item.historical is not None
            ):
                # A bound row defined by a formula over rows the build has not
                # filled yet is pending, not absent: summing it as zero would
                # book its balance (cash, before the cash-flow history lands)
                # as the residual.
                unresolved = True
                break
        if unresolved:
            continue
        residual = float(total_value) - subline_sum
        if residual <= max(1e-6, abs(float(total_value)) * 1e-9):
            continue
        residuals[period] = residual
    return residuals


def _set_semantic_derived_values(
    item: LineItem,
    values: dict[int, float],
    *,
    note: str,
) -> None:
    if item.values is None:
        item.values = ValueSeries()
    for period, value in sorted(values.items()):
        item.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=ValueProvenance.derived,
            note=note,
        )


def _materialize_balance_bridges(
    model: FinancialModel,
    ticker: str,
    historical_periods: list[int],
    result: SemanticRowsResult | None = None,
    *,
    business_model: Any | None = None,
) -> SemanticRowsResult:
    result = result or SemanticRowsResult()
    if not historical_periods:
        return result
    balances = _bridged_balances(business_model)
    if not balances:
        return result

    entries: dict[str, dict[str, Any]] = {}
    materialized_by_role: dict[str, str] = {}


    for balance in balances:
        required_role = balance.required_role
        bridge = _balance_bridge_entry(ticker, balance)
        if bridge is None:
            result.gaps.append(
                {"required_role": required_role, "kind": "balance_bridge_unanchored"}
            )
            continue
        section, entry = bridge
        entries[required_role] = entry
        row_policy = entry.get("row_policy") if isinstance(entry, dict) else {}
        target_item_id = None
        existing_item_ids: tuple[Any, ...] = ()
        if isinstance(row_policy, dict):
            preferred = row_policy.get("preferred_target_item_id")
            stable = row_policy.get("stable_item_id")
            target_item_id = preferred if isinstance(preferred, str) else stable
            existing_item_ids = (preferred, stable)
        for existing_id in existing_item_ids:
            if not isinstance(existing_id, str):
                continue
            existing_item = model._index.get(existing_id)
            if existing_item is not None and existing_item.values is not None and any(
                cell.value is not None for cell in existing_item.values.values.values()
            ):
                materialized_by_role[required_role] = existing_id
                break
        if required_role in materialized_by_role:
            continue
        if not _balance_bridge_target_is_available(
            model,
            entry=entry,
        ):
            result.gaps.append(
                {
                    "required_role": required_role,
                    "kind": "balance_bridge_target_occupied",
                    "item_id": target_item_id,
                }
            )
            continue
        residuals = _balance_bridge_residuals(
            model,
            section=section,
            target_item_id=target_item_id,
            historical_periods=historical_periods,
        )
        if max(int(period) for period in historical_periods) not in residuals:
            # The bridge row projects from the latest reported year; a residual
            # that leaves that year open is a line with no opening to roll.
            continue
        item = _bind_or_insert_semantic_row(
            model,
            ticker=ticker,
            required_role=required_role,
            entry=entry,
            result=result,
        )
        if item is None:
            continue
        _set_semantic_derived_values(
            item,
            residuals,
            note=BALANCE_BRIDGE_NOTE,
        )
        materialized_by_role[required_role] = item.id
        result.materialized.append(
            {
                "required_role": required_role,
                "action": "derived_subtotal_bridge_values",
                "item_id": item.id,
                "periods": sorted(residuals),
            }
        )

    if not materialized_by_role:
        return result

    overrides = TickerOverrides(
        ticker=ticker,
        overrides={},
        semantic_rows=entries,
        file_meta={"ticker": ticker, "schema_version": "3"},
    )
    _apply_semantic_cash_flow_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_role=materialized_by_role,
        result=result,
    )
    _apply_semantic_valuation_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_role=materialized_by_role,
        result=result,
    )
    return result
