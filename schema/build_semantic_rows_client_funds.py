"""Client-funds bridge materialization for semantic row builds."""

from __future__ import annotations

import sys
from typing import Any

from .build_diagnostic_sections import BS_SECTIONS
from .build_formula_eval import _constant_override_value as _formula_constant_override_value
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


_CLIENT_FUNDS_BRIDGE_SECTIONS = {
    "current_assets": {
        "concept_suffix": "funds_held_for_clients",
        "semantic_role": "client_funds_asset",
        "target_label": "Funds held for clients",
        "preferred_target_item_id": "tpl.fm.balance_sheet.current_asset_4",
        "insert_after_item_id": (
            "tpl.fm.balance_sheet.total_current_assets_before_funds_held_for_clients"
        ),
        "stable_item_id": "fm.semantic.{ticker}.funds_held_for_clients",
        "cash_flow_linkage": {
            "type": "cash_bridge_adjustment",
            "target_row_policy": {
                "mode": "bind_if_empty_or_insert",
                "preferred_target_item_id": (
                    "tpl.fm.cash_flow.funds_held_for_client_s_cash_and_cash_equivalents"
                ),
                "target_label": "Funds held for clients cash bridge",
            },
        },
    },
    "current_liabilities": {
        "concept_suffix": "client_fund_obligations_current",
        "semantic_role": "client_fund_obligation",
        "target_label": "Client fund obligations",
        "insert_after_item_id": "tpl.fm.balance_sheet.current_liability_3",
        "formula_insert_after_item_id": "tpl.fm.balance_sheet.current_liability_3",
        "stable_item_id": "fm.semantic.{ticker}.client_fund_obligations",
        "cash_flow_linkage": {
            "type": "financing_liability_delta",
            "sign_convention": "current_minus_prior",
            "target_row_policy": {
                "mode": "insert_or_bind_same_semantic",
                "insert_after_item_id": "tpl.fm.cash_flow.other_cash_flows_from_financing",
                "stable_item_id": (
                    "fm.semantic.{ticker}.net_change_in_client_fund_obligations"
                ),
                "target_label": "Net change in client fund obligations",
            },
        },
    },
}
_CLIENT_FUNDS_BUSINESS_MODEL_TERMS = (
    "funds held for clients",
    "client fund",
    "client payroll",
    "payroll funds",
    "tax funds",
)


def _parent_attr(name: str, fallback: Any) -> Any:
    package_name = __name__.rsplit(".", 1)[0]
    build_module = sys.modules.get(f"{package_name}.build")
    if build_module is not None and hasattr(build_module, name):
        return getattr(build_module, name)
    semantic_rows_module = sys.modules.get(f"{package_name}.build_semantic_rows")
    if semantic_rows_module is not None and hasattr(semantic_rows_module, name):
        return getattr(semantic_rows_module, name)
    return fallback


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build semantic rows helper {name!r} is unavailable")
    return helper


def _semantic_constant_value(spec: FormulaSpec | None) -> float | None:
    if spec is not None and spec.note == "synthetic":
        return None
    constant_override_value = _parent_attr(
        "_constant_override_value",
        _formula_constant_override_value,
    )
    return constant_override_value(spec)


def _semantic_shift_period(model: FinancialModel, period: int, t: int) -> int | None:
    shift_period = _parent_attr("shift_period", _model_shift_period)
    shifted = shift_period(int(period), int(t), model.time_structure.period_mode)
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

    semantic_constant_value = _parent_attr(
        "_semantic_constant_value",
        _semantic_constant_value,
    )
    if item.overrides is not None and int(period) in item.overrides:
        value = semantic_constant_value(item.overrides[int(period)])
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
    line_item_ref_cls = _parent_attr("LineItemRef", LineItemRef)
    if expr is None:
        return None
    if isinstance(expr, bool):
        return float(int(expr))
    if isinstance(expr, (int, float)):
        return float(expr)
    if isinstance(expr, line_item_ref_cls):
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
            if any(value is None for value in values):
                return None
            return sum(value for value in values if value is not None)
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
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            result = values[0] or 0.0
            for value in values[1:]:
                result -= value or 0.0
            return result
    return None


def _semantic_evaluate_formula(
    model: FinancialModel,
    spec: FormulaSpec,
    period: int,
    memo: dict[tuple[str, int], float | None],
    stack: set[tuple[str, int]],
) -> float | None:
    formula_type = _parent_attr("FormulaType", FormulaType)
    semantic_constant_value = _parent_attr(
        "_semantic_constant_value",
        _semantic_constant_value,
    )
    if spec.type is formula_type.constant:
        return semantic_constant_value(spec)
    params = spec.params or {}
    if spec.type is formula_type.ref:
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
    if spec.type is not formula_type.arithmetic:
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
        if any(value is None for value in values):
            return None
        if not values:
            return 0.0
        if operator == "-":
            result = values[0] or 0.0
            for value in values[1:]:
                result -= value or 0.0
            return result
        if operator == "+":
            return sum(value for value in values if value is not None)
        return None
    values = [
        _semantic_evaluate_expr(model, item, int(period), memo, stack)
        for item in list(params.get("items", []) or [])
    ]
    if any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None)


def _client_funds_bridge_entry(
    ticker: str,
    section: str,
    config: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    ticker_slug = str(ticker).strip().lower()
    concept_id = f"{ticker_slug}__{config['concept_suffix']}"

    def format_value(value: Any) -> Any:
        if isinstance(value, str):
            return value.format(ticker=ticker_slug)
        if isinstance(value, dict):
            return {key: format_value(nested) for key, nested in value.items()}
        if isinstance(value, list):
            return [format_value(nested) for nested in value]
        return value

    row_policy: dict[str, Any] = {
        "mode": (
            "bind_if_empty_or_insert"
            if config.get("preferred_target_item_id")
            else "insert_or_bind_same_semantic"
        ),
        "insert_after_item_id": config["insert_after_item_id"],
        "stable_item_id": format_value(config["stable_item_id"]),
    }
    if config.get("preferred_target_item_id"):
        row_policy["preferred_target_item_id"] = config["preferred_target_item_id"]
    if config.get("formula_insert_after_item_id"):
        row_policy["formula_insert_after_item_id"] = config[
            "formula_insert_after_item_id"
        ]

    return concept_id, {
        "statement": "balance_sheet",
        "section": section,
        "semantic_role": config["semantic_role"],
        "target_label": config["target_label"],
        "unit": "dollars",
        "row_policy": row_policy,
        "forecast_policy": {
            "type": "carry_forward",
            "rationale": "derived from reported total less mapped subtotal rows",
        },
        "cash_flow_linkage": format_value(config.get("cash_flow_linkage") or {}),
        "notes": (
            "Derived bridge row for companies that report current totals inclusive "
            "of client funds while the template separately models the pre-client-funds subtotal."
        ),
    }


def _business_model_text_values(value: Any, *, depth: int = 0) -> list[str]:
    if depth > 6 or value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float, bool)):
        return []
    if isinstance(value, dict):
        texts: list[str] = []
        for key, nested in value.items():
            if isinstance(key, str):
                texts.append(key)
            texts.extend(_business_model_text_values(nested, depth=depth + 1))
        return texts
    if isinstance(value, (list, tuple, set)):
        texts: list[str] = []
        for nested in value:
            texts.extend(_business_model_text_values(nested, depth=depth + 1))
        return texts
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _business_model_text_values(model_dump(mode="python"), depth=depth + 1)
        except TypeError:
            return _business_model_text_values(model_dump(), depth=depth + 1)
    if hasattr(value, "__dict__"):
        return _business_model_text_values(vars(value), depth=depth + 1)
    return []


def _business_model_has_client_funds_topology(business_model: Any | None) -> bool:
    if business_model is None:
        return False
    haystack = " ".join(_business_model_text_values(business_model)).lower()
    return any(term in haystack for term in _CLIENT_FUNDS_BUSINESS_MODEL_TERMS)


def _client_funds_bridge_target_is_available(
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
            item_has_any_real_historical_data = _required_parent_attr(
                "_item_has_any_real_historical_data"
            )
            if item_has_any_real_historical_data(model, stable_item):
                return False
    preferred_target_id = row_policy.get("preferred_target_item_id")
    if not isinstance(preferred_target_id, str) or not preferred_target_id.strip():
        return True
    try:
        preferred_item = model.get_item(preferred_target_id.strip())
    except KeyError:
        return True
    semantic_target_is_empty = _required_parent_attr("_semantic_target_is_empty")
    return semantic_target_is_empty(model, preferred_item)


def _client_funds_bridge_residuals(
    model: FinancialModel,
    *,
    section: str,
    target_item_id: str | None,
    historical_periods: list[int],
) -> dict[int, float]:
    section_config = BS_SECTIONS.get(section)
    if not isinstance(section_config, dict):
        return {}
    if not section_config.get("pre_subtotal_item_id"):
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
        for member in members:
            member_item_id = getattr(member, "template_item_id", None)
            if not isinstance(member_item_id, str):
                continue
            if target_item_id is not None and member_item_id == target_item_id:
                continue
            member_value = _semantic_observed_value(model, member_item_id, period, memo)
            if member_value is not None:
                subline_sum += float(member_value)
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
    value_cell_cls = _parent_attr("ValueCell", ValueCell)
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    for period, value in sorted(values.items()):
        item.values.values[int(period)] = value_cell_cls(
            period=int(period),
            value=float(value),
            provenance=value_provenance.derived,
            note=note,
        )


def _materialize_client_funds_subtotal_bridges(
    model: FinancialModel,
    ticker: str,
    historical_periods: list[int],
    result: Any | None = None,
    *,
    business_model: Any | None = None,
) -> Any:
    semantic_rows_result_cls = _required_parent_attr("SemanticRowsResult")
    result = result or semantic_rows_result_cls()
    if not historical_periods:
        return result
    business_model_has_client_funds_topology = _parent_attr(
        "_business_model_has_client_funds_topology",
        _business_model_has_client_funds_topology,
    )
    if not business_model_has_client_funds_topology(business_model):
        return result

    entries: dict[str, dict[str, Any]] = {}
    materialized_by_concept: dict[str, str] = {}
    bind_or_insert_semantic_row = _required_parent_attr("_bind_or_insert_semantic_row")
    client_funds_bridge_entry = _parent_attr(
        "_client_funds_bridge_entry",
        _client_funds_bridge_entry,
    )
    client_funds_bridge_target_is_available = _parent_attr(
        "_client_funds_bridge_target_is_available",
        _client_funds_bridge_target_is_available,
    )
    client_funds_bridge_residuals = _parent_attr(
        "_client_funds_bridge_residuals",
        _client_funds_bridge_residuals,
    )
    set_semantic_derived_values = _parent_attr(
        "_set_semantic_derived_values",
        _set_semantic_derived_values,
    )

    for section, config in sorted(_CLIENT_FUNDS_BRIDGE_SECTIONS.items()):
        concept_id, entry = client_funds_bridge_entry(ticker, section, config)
        entries[concept_id] = entry
        row_policy = entry.get("row_policy") if isinstance(entry, dict) else {}
        target_item_id = None
        if isinstance(row_policy, dict):
            preferred = row_policy.get("preferred_target_item_id")
            stable = row_policy.get("stable_item_id")
            target_item_id = preferred if isinstance(preferred, str) else stable
        if not client_funds_bridge_target_is_available(
            model,
            entry=entry,
        ):
            result.gaps.append(
                {
                    "concept_id": concept_id,
                    "kind": "client_funds_bridge_target_occupied",
                    "item_id": target_item_id,
                }
            )
            continue
        residuals = client_funds_bridge_residuals(
            model,
            section=section,
            target_item_id=target_item_id,
            historical_periods=historical_periods,
        )
        if not residuals:
            continue
        item = bind_or_insert_semantic_row(
            model,
            ticker=ticker,
            concept_id=concept_id,
            entry=entry,
            result=result,
        )
        if item is None:
            continue
        set_semantic_derived_values(
            item,
            residuals,
            note="derived_client_funds_subtotal_bridge",
        )
        materialized_by_concept[concept_id] = item.id
        result.materialized.append(
            {
                "concept_id": concept_id,
                "action": "derived_subtotal_bridge_values",
                "item_id": item.id,
                "periods": sorted(residuals),
            }
        )

    if not materialized_by_concept:
        return result

    overrides = TickerOverrides(
        ticker=ticker,
        overrides={},
        custom_concepts={},
        semantic_rows=entries,
        file_meta={"ticker": ticker, "schema_version": "3"},
    )
    apply_semantic_cash_flow_linkages = _required_parent_attr(
        "_apply_semantic_cash_flow_linkages"
    )
    apply_semantic_valuation_linkages = _required_parent_attr(
        "_apply_semantic_valuation_linkages"
    )
    apply_semantic_cash_flow_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_concept=materialized_by_concept,
        result=result,
    )
    apply_semantic_valuation_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_concept=materialized_by_concept,
        result=result,
    )
    return result
