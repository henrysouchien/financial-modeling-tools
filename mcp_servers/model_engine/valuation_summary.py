from __future__ import annotations

import math
from typing import Any

from schema.build import (
  _BLOOMBERG_STYLE_BETA_WEIGHT,
  _MARKET_BETA_ANCHOR,
  _VALUATION_DEFAULTS,
  _VALUATION_ECONOMIC_INPUTS,
  _VALUATION_PLACEHOLDER_VALUES,
  _VALUATION_REQUIRED_INPUTS,
)


VALUATION_READBACK_ITEMS: list[dict[str, str]] = [
  {
    "key": "stock_price",
    "group": "current_valuation",
    "item_id": "tpl.v.current_valuation.stock_price",
    "description": "Current stock price used by valuation return calculations.",
  },
  {
    "key": "shares_outstanding",
    "group": "current_valuation",
    "item_id": "tpl.v.current_valuation.shares_outstanding",
    "description": "Shares outstanding used for per-share valuation outputs.",
  },
  {
    "key": "net_debt",
    "group": "current_valuation",
    "item_id": "tpl.v.current_valuation.net_debt",
    "description": "Net debt bridge from enterprise value to equity value.",
  },
  {
    "key": "enterprise_value",
    "group": "current_valuation",
    "item_id": "tpl.v.current_valuation.enterprise_value",
    "description": "Current enterprise value used for relative valuation context.",
  },
  {
    "key": "dcf_price",
    "group": "dcf",
    "item_id": "tpl.v.dcf.dcf_price",
    "description": "DCF-derived share price.",
  },
  {
    "key": "dcf_price_summary",
    "group": "dcf",
    "item_id": "tpl.v.dcf.dcf_price_summary",
    "description": "DCF price summary row shown in the valuation summary block.",
  },
  {
    "key": "pv_cash_flows",
    "group": "dcf",
    "item_id": "tpl.v.dcf.pv_cash_flows",
    "description": "Present value of projected cash flows.",
  },
  {
    "key": "pv_terminal_value",
    "group": "dcf",
    "item_id": "tpl.v.dcf.pv_terminal_value",
    "description": "Present value of terminal value.",
  },
  {
    "key": "total_ev",
    "group": "dcf",
    "item_id": "tpl.v.dcf.total_ev",
    "description": "DCF total enterprise value.",
  },
  {
    "key": "equity_value_per_share",
    "group": "dcf",
    "item_id": "tpl.v.dcf.equity_value_per_share",
    "description": "DCF equity value per share before final price adjustment.",
  },
  {
    "key": "terminal_growth_rate",
    "group": "dcf_terminal",
    "item_id": "tpl.v.dcf.terminal_growth_rate",
    "description": "Perpetuity-growth terminal assumption.",
  },
  {
    "key": "terminal_value_growth",
    "group": "dcf_terminal",
    "item_id": "tpl.v.dcf.terminal_value_growth",
    "description": "Terminal value using constant-growth method.",
  },
  {
    "key": "exit_multiple",
    "group": "dcf_terminal",
    "item_id": "tpl.v.dcf.exit_multiple",
    "description": "Exit multiple assumption used for the terminal-value cross-check.",
  },
  {
    "key": "terminal_value_multiple",
    "group": "dcf_terminal",
    "item_id": "tpl.v.dcf.terminal_value_multiple",
    "description": "Terminal value using exit-multiple cross-check.",
  },
  {
    "key": "forward_pe",
    "group": "relative_pe",
    "item_id": "tpl.v.forward_pe.forward_pe",
    "description": "Selected forward P/E multiple.",
  },
  {
    "key": "forward_pe_price",
    "group": "relative_pe",
    "item_id": "tpl.v.forward_pe.forward_pe_price",
    "description": "Forward P/E implied share price.",
  },
  {
    "key": "ev_ebitda",
    "group": "relative_ev_ebitda",
    "item_id": "tpl.v.forward_ev_ebitda.forward_ev_ebitda",
    "description": "Selected forward EV/EBITDA multiple.",
  },
  {
    "key": "ev_ebitda_price",
    "group": "relative_ev_ebitda",
    "item_id": "tpl.v.forward_ev_ebitda.ev_ebitda_price",
    "description": "Forward EV/EBITDA implied share price.",
  },
  {
    "key": "risk_free_rate",
    "group": "wacc",
    "item_id": "tpl.v.cost_of_equity.risk_free_rate",
    "description": "Risk-free rate used in CAPM.",
  },
  {
    "key": "equity_risk_premium",
    "group": "wacc",
    "item_id": "tpl.v.cost_of_equity.equity_risk_premium",
    "description": "Equity risk premium used in CAPM.",
  },
  {
    "key": "raw_beta",
    "group": "wacc",
    "item_id": "tpl.v.cost_of_equity.raw_beta",
    "description": "Raw equity beta input.",
  },
  {
    "key": "beta_floor",
    "group": "wacc",
    "item_id": "tpl.v.cost_of_equity.beta_floor",
    "description": "Analyst floor applied to adjusted beta.",
  },
  {
    "key": "beta",
    "group": "wacc",
    "item_id": "tpl.v.cost_of_equity.beta",
    "description": "Adjusted beta used in CAPM.",
  },
  {
    "key": "cost_of_equity",
    "group": "wacc",
    "item_id": "tpl.v.cost_of_equity.cost_of_equity",
    "description": "CAPM cost of equity.",
  },
  {
    "key": "sofr_rate",
    "group": "wacc",
    "item_id": "tpl.v.wacc.sofr_rate",
    "description": "SOFR input for cost of debt.",
  },
  {
    "key": "credit_spread",
    "group": "wacc",
    "item_id": "tpl.v.wacc.credit_spread",
    "description": "Credit spread input for cost of debt.",
  },
  {
    "key": "wacc",
    "group": "wacc",
    "item_id": "tpl.v.wacc.wacc",
    "description": "Weighted average cost of capital.",
  },
  {
    "key": "blended_price",
    "group": "target",
    "item_id": "tpl.v.blended_target.blended_price",
    "description": "Blended valuation target price.",
  },
  {
    "key": "expected_return",
    "group": "target",
    "item_id": "tpl.v.blended_target.expected_return",
    "description": "Expected return from current price to blended target.",
  },
]
VALUATION_READBACK_ITEM_IDS = [entry["item_id"] for entry in VALUATION_READBACK_ITEMS]
VALUATION_READBACK_BY_ID = {entry["item_id"]: entry for entry in VALUATION_READBACK_ITEMS}
VALUATION_READBACK_FIELD_KEYS = {
  **{field: field for field in _VALUATION_ECONOMIC_INPUTS},
  "adjusted_beta": "beta",
}
VALUATION_READBACK_ITEM_TO_FIELD = {
  item_id: field for field, item_id in _VALUATION_ECONOMIC_INPUTS.items()
}


def sort_period_key(period: Any) -> tuple[int, str]:
  try:
    return int(period), str(period)
  except (TypeError, ValueError):
    return -1, str(period)


def latest_numeric_value(row: dict[str, Any] | None) -> float | None:
  if not row:
    return None
  values = row.get("values")
  if not isinstance(values, dict):
    return None
  for period in sorted(values, key=sort_period_key, reverse=True):
    value = values.get(period)
    if isinstance(value, bool):
      continue
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
      return float(value)
  return None


def ratio_metric(
  key: str,
  label: str,
  numerator_row: dict[str, Any] | None,
  denominator_row: dict[str, Any] | None,
  *,
  formula: str,
  subtract_one: bool = False,
) -> dict[str, Any]:
  numerator = latest_numeric_value(numerator_row)
  denominator = latest_numeric_value(denominator_row)
  value = None
  if numerator is not None and denominator not in (None, 0):
    value = numerator / denominator
    if subtract_one:
      value -= 1
  return {
    "key": key,
    "label": label,
    "value": value,
    "formula": formula,
    "inputs": [
      {
        "key": numerator_row.get("key") if numerator_row else None,
        "item_id": numerator_row.get("id") if numerator_row else None,
        "value": numerator,
      },
      {
        "key": denominator_row.get("key") if denominator_row else None,
        "item_id": denominator_row.get("id") if denominator_row else None,
        "value": denominator,
      },
    ],
  }


def valuation_derived_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
  by_key = {row.get("key"): row for row in rows if row.get("key")}
  return [
    ratio_metric(
      "terminal_value_share_of_total_ev",
      "PV terminal value as share of DCF enterprise value",
      by_key.get("pv_terminal_value"),
      by_key.get("total_ev"),
      formula="pv_terminal_value / total_ev",
    ),
    ratio_metric(
      "terminal_value_growth_vs_multiple_pct",
      "Constant-growth terminal value vs exit-multiple terminal value",
      by_key.get("terminal_value_growth"),
      by_key.get("terminal_value_multiple"),
      formula="terminal_value_growth / terminal_value_multiple - 1",
      subtract_one=True,
    ),
    ratio_metric(
      "dcf_vs_forward_pe_price_pct",
      "DCF price vs forward P/E implied price",
      by_key.get("dcf_price"),
      by_key.get("forward_pe_price"),
      formula="dcf_price / forward_pe_price - 1",
      subtract_one=True,
    ),
    ratio_metric(
      "dcf_vs_ev_ebitda_price_pct",
      "DCF price vs forward EV/EBITDA implied price",
      by_key.get("dcf_price"),
      by_key.get("ev_ebitda_price"),
      formula="dcf_price / ev_ebitda_price - 1",
      subtract_one=True,
    ),
  ]


def row_latest_value(by_key: dict[str, dict[str, Any]], key: str) -> float | None:
  return latest_numeric_value(by_key.get(key))


def valuation_input_readiness(rows: list[dict[str, Any]]) -> dict[str, Any]:
  by_key = {row.get("key"): row for row in rows if row.get("key")}
  populated: list[str] = []
  missing: list[str] = []
  sources: dict[str, str] = {}
  legacy_placeholder_like: list[dict[str, Any]] = []

  for field in _VALUATION_REQUIRED_INPUTS:
    key = VALUATION_READBACK_FIELD_KEYS.get(field, field)
    value = row_latest_value(by_key, key)
    if value is None:
      missing.append(field)
    else:
      populated.append(field)
      sources[field] = "workbook_readback"

  for item_id, placeholder_value in _VALUATION_PLACEHOLDER_VALUES.items():
    field = VALUATION_READBACK_ITEM_TO_FIELD.get(item_id)
    if field is None:
      continue
    key = VALUATION_READBACK_FIELD_KEYS.get(field, field)
    value = row_latest_value(by_key, key)
    if nearly_equal(value, placeholder_value, tolerance=1e-12):
      legacy_placeholder_like.append(
        {
          "field": field,
          "item_id": item_id,
          "value": value,
          "legacy_placeholder_value": placeholder_value,
        }
      )

  raw_beta = row_latest_value(by_key, "raw_beta")
  adjusted_beta = row_latest_value(by_key, "beta")
  if (
    nearly_equal(raw_beta, _VALUATION_PLACEHOLDER_VALUES.get("tpl.v.cost_of_equity.raw_beta"), tolerance=1e-12)
    and nearly_equal(adjusted_beta, _MARKET_BETA_ANCHOR, tolerance=1e-12)
  ):
    legacy_placeholder_like.append(
      {
        "field": "adjusted_beta",
        "item_id": "tpl.v.cost_of_equity.beta",
        "value": adjusted_beta,
        "legacy_placeholder_value": _MARKET_BETA_ANCHOR,
      }
    )

  flags: list[dict[str, Any]] = []
  if missing:
    flags.append(
      {
        "code": "valuation_inputs_missing",
        "severity": "warning",
        "message": "One or more required valuation input rows are blank; do not treat DCF/WACC outputs as clean until the inputs are sourced.",
      }
    )
  if legacy_placeholder_like:
    flags.append(
      {
        "code": "legacy_valuation_placeholders_present",
        "severity": "warning",
        "message": "One or more valuation input rows match legacy template placeholder values; treat them as stale or ambiguous unless independently sourced.",
      }
    )

  return {
    "status": "complete" if not missing and not legacy_placeholder_like else "incomplete",
    "populated": populated,
    "missing": missing,
    "sources": sources,
    "flags": flags,
    "legacy_placeholder_like": legacy_placeholder_like,
  }


def nearly_equal(left: float | None, right: float | None, *, tolerance: float = 1e-9) -> bool | None:
  if left is None or right is None:
    return None
  return abs(float(left) - float(right)) <= tolerance


def policy_number(value: float) -> str:
  text = f"{value:.12g}"
  if "." not in text and "e" not in text.lower():
    return f"{text}.0"
  return text


def valuation_policy_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
  by_key = {row.get("key"): row for row in rows if row.get("key")}
  risk_free_rate = row_latest_value(by_key, "risk_free_rate")
  equity_risk_premium = row_latest_value(by_key, "equity_risk_premium")
  raw_beta = row_latest_value(by_key, "raw_beta")
  beta_floor = row_latest_value(by_key, "beta_floor")
  adjusted_beta = row_latest_value(by_key, "beta")
  workbook_cost_of_equity = row_latest_value(by_key, "cost_of_equity")

  unfloored_adjusted_beta = None
  if raw_beta is not None:
    unfloored_adjusted_beta = (
      (_BLOOMBERG_STYLE_BETA_WEIGHT * raw_beta)
      + ((1.0 - _BLOOMBERG_STYLE_BETA_WEIGHT) * _MARKET_BETA_ANCHOR)
    )

  computed_adjusted_beta = None
  if unfloored_adjusted_beta is not None:
    computed_adjusted_beta = (
      max(unfloored_adjusted_beta, beta_floor)
      if beta_floor is not None
      else unfloored_adjusted_beta
    )

  recomputed_cost_of_equity = None
  if (
    risk_free_rate is not None
    and adjusted_beta is not None
    and equity_risk_premium is not None
  ):
    recomputed_cost_of_equity = risk_free_rate + (adjusted_beta * equity_risk_premium)

  raw_beta_cost_of_equity = None
  if (
    risk_free_rate is not None
    and raw_beta is not None
    and equity_risk_premium is not None
  ):
    raw_beta_cost_of_equity = risk_free_rate + (raw_beta * equity_risk_premium)

  adjusted_vs_raw_delta = None
  if recomputed_cost_of_equity is not None and raw_beta_cost_of_equity is not None:
    adjusted_vs_raw_delta = recomputed_cost_of_equity - raw_beta_cost_of_equity

  floor_applied = bool(
    unfloored_adjusted_beta is not None
    and beta_floor is not None
    and nearly_equal(adjusted_beta, beta_floor)
    and unfloored_adjusted_beta < beta_floor - 1e-9
  )
  beta_adjustment_formula = (
    f"{policy_number(_BLOOMBERG_STYLE_BETA_WEIGHT)} * raw_beta + "
    f"{policy_number(1.0 - _BLOOMBERG_STYLE_BETA_WEIGHT)} * "
    f"{policy_number(_MARKET_BETA_ANCHOR)}"
  )
  if beta_floor is not None:
    beta_adjustment_formula = f"max({beta_adjustment_formula}, beta_floor)"

  erp_placeholder = _VALUATION_DEFAULTS["tpl.v.cost_of_equity.equity_risk_premium"]
  notices = []
  if nearly_equal(equity_risk_premium, erp_placeholder, tolerance=1e-12):
    notices.append(
      {
        "code": "legacy_placeholder_equity_risk_premium",
        "severity": "warning",
        "message": "Equity risk premium matches a legacy template placeholder; treat it as stale or ambiguous unless the build context explicitly supplied it.",
      }
    )
  if floor_applied:
    notices.append(
      {
        "code": "beta_floor_applied",
        "severity": "info",
        "message": "Adjusted beta is at the analyst floor; cost of equity is using the floor rather than the raw imported beta directly.",
      }
    )
  if nearly_equal(recomputed_cost_of_equity, workbook_cost_of_equity) is False:
    notices.append(
      {
        "code": "cost_of_equity_formula_mismatch",
        "severity": "warning",
        "message": "Workbook cost of equity does not match risk_free_rate + adjusted_beta * equity_risk_premium.",
      }
    )

  return {
    "method": "CAPM cost of equity feeding WACC",
    "cost_of_equity": {
      "item_id": "tpl.v.cost_of_equity.cost_of_equity",
      "formula": "risk_free_rate + adjusted_beta * equity_risk_premium",
      "workbook_value": workbook_cost_of_equity,
      "recomputed_value": recomputed_cost_of_equity,
      "matches_workbook": nearly_equal(recomputed_cost_of_equity, workbook_cost_of_equity),
    },
    "equity_risk_premium": {
      "item_id": "tpl.v.cost_of_equity.equity_risk_premium",
      "value": equity_risk_premium,
      "legacy_placeholder_value": erp_placeholder,
      "is_legacy_placeholder_value": nearly_equal(equity_risk_premium, erp_placeholder, tolerance=1e-12),
      "source_policy": (
        "ModelBuildContext.valuation.inputs.equity_risk_premium is the explicit "
        "build-time value; it may be analyst-provided or a sourced house-policy "
        "MBC ERP. Use model_semantics(..., sections=[\"valuation\"]) and the "
        "valuation-inputs artifact for row-level ERP source/rationale once "
        "valuation-inputs has durably persisted the row. When ERP is absent, "
        "schema.build leaves it blank instead of inferring a default."
      ),
    },
    "beta": {
      "raw_beta_item_id": "tpl.v.cost_of_equity.raw_beta",
      "adjusted_beta_item_id": "tpl.v.cost_of_equity.beta",
      "beta_floor_item_id": "tpl.v.cost_of_equity.beta_floor",
      "raw_beta": raw_beta,
      "adjusted_beta": adjusted_beta,
      "beta_floor": beta_floor,
      "unfloored_adjusted_beta": unfloored_adjusted_beta,
      "computed_adjusted_beta": computed_adjusted_beta,
      "adjustment_formula": beta_adjustment_formula,
      "blume_weight": _BLOOMBERG_STYLE_BETA_WEIGHT,
      "market_beta_anchor": _MARKET_BETA_ANCHOR,
      "floor_applied": floor_applied,
      "raw_beta_cost_of_equity": raw_beta_cost_of_equity,
      "adjusted_vs_raw_cost_of_equity_delta": adjusted_vs_raw_delta,
      "source_policy": "Raw beta is imported from market/profile data when available; adjusted beta applies the visible Blume-style formula, with beta_floor applied only when that workbook input is populated.",
    },
    "notices": notices,
  }


def enrich_valuation_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
  rows: list[dict[str, Any]] = []
  for item in items:
    row = dict(item)
    meta = VALUATION_READBACK_BY_ID.get(str(row.get("id") or ""))
    if meta:
      row["key"] = meta["key"]
      row["group"] = meta["group"]
      row["description"] = meta["description"]
      row["latest_value"] = latest_numeric_value(row)
    rows.append(row)
  return rows


def group_valuation_rows(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
  groups: dict[str, list[str]] = {}
  for row in rows:
    group = str(row.get("group") or "unknown")
    key = str(row.get("key") or row.get("id") or "")
    groups.setdefault(group, []).append(key)
  return groups


__all__ = [
  "VALUATION_READBACK_BY_ID",
  "VALUATION_READBACK_FIELD_KEYS",
  "VALUATION_READBACK_ITEM_IDS",
  "VALUATION_READBACK_ITEM_TO_FIELD",
  "VALUATION_READBACK_ITEMS",
  "enrich_valuation_rows",
  "group_valuation_rows",
  "latest_numeric_value",
  "nearly_equal",
  "policy_number",
  "ratio_metric",
  "row_latest_value",
  "sort_period_key",
  "valuation_derived_metrics",
  "valuation_input_readiness",
  "valuation_policy_summary",
]
