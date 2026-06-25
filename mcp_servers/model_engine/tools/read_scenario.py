from __future__ import annotations

from typing import Any


SCENARIO_TABLE_PREFIX = "tpl.a.scenario_tables."
SCENARIO_TABLE_SCALAR_KEYS = {
  "all",
  "model_value",
  "override",
  "projection",
  "scenario_value",
  "value",
}
MISSING = object()


def is_scenario_table_value_item(item_id: str) -> bool:
  if not item_id.startswith(SCENARIO_TABLE_PREFIX):
    return False
  return not item_id.rsplit(".", 1)[-1].startswith("scenario_")


def scenario_table_scalar_value(period_values: Any) -> Any:
  if not isinstance(period_values, dict):
    return period_values
  if len(period_values) != 1:
    return MISSING
  raw_key, raw_value = next(iter(period_values.items()))
  key = str(raw_key).strip().lower()
  if key in SCENARIO_TABLE_SCALAR_KEYS:
    return raw_value
  return MISSING


def projection_periods_for_scenario_table_item(
  *,
  deps: Any,
  file_path: str,
  item_id: str,
  historical_cutoff_year: int | None,
) -> list[int]:
  result = deps.values(
    file_path,
    [item_id],
    periods="projection",
    historical_cutoff_year=historical_cutoff_year,
  )
  rows = result.get("items") if isinstance(result, dict) else None
  first_row = rows[0] if isinstance(rows, list) and rows else None
  if isinstance(first_row, dict) and first_row.get("error_code"):
    raise KeyError(first_row.get("error") or f"Unknown item_id: {item_id}")
  values = first_row.get("values") if isinstance(first_row, dict) else None
  if not isinstance(values, dict) or not values:
    return []
  return [int(period) for period in values]


def is_number(value: Any) -> bool:
  return isinstance(value, (int, float)) and not isinstance(value, bool)


def scenario_no_effect_warning(
  *,
  result: dict[str, Any],
  overrides: dict[str, dict[int, float]],
  is_number_fn: Any | None = None,
) -> dict[str, Any] | None:
  number_check = is_number_fn or is_number
  comparisons = result.get("comparisons")
  if not overrides or not isinstance(comparisons, list) or not comparisons:
    return None
  numeric_comparisons = [
    comparison
    for comparison in comparisons
    if isinstance(comparison, dict)
    and number_check(comparison.get("base"))
    and number_check(comparison.get("scenario"))
  ]
  if not numeric_comparisons:
    return None
  if any(
    abs(float(comparison["scenario"]) - float(comparison["base"])) > 1e-9
    for comparison in numeric_comparisons
  ):
    return None
  return {
    "code": "scenario_no_effect",
    "kind": "scenario_no_effect",
    "message": (
      "Scenario override completed but every numeric comparison was unchanged; "
      "the override rows may not be decision-usable scenario anchors."
    ),
    "override_item_ids": list(overrides.keys())[:20],
    "compare_item_ids": [
      str(comparison.get("id") or comparison.get("item_id") or "")
      for comparison in numeric_comparisons[:20]
      if comparison.get("id") or comparison.get("item_id")
    ],
  }


def attach_scenario_no_effect_guidance(
  *,
  result: dict[str, Any],
  overrides: dict[str, dict[int, float]],
  scenario_no_effect_warning_fn: Any | None = None,
) -> dict[str, Any]:
  warning_fn = scenario_no_effect_warning_fn or scenario_no_effect_warning
  warning = warning_fn(result=result, overrides=overrides)
  if warning is None:
    return result
  guided = dict(result)
  warnings = guided.get("warnings")
  warning_rows = list(warnings) if isinstance(warnings, list) else []
  warning_rows.append(warning)
  guided["warnings"] = warning_rows
  next_actions = guided.get("next_actions")
  action_rows = list(next_actions) if isinstance(next_actions, list) else []
  action_rows.extend(
    [
      "Call model_scenario_topology before the next model_scenario attempt and override returned bull/base/bear case row IDs for one scenario case at a time; model_scenario maps each case row to its owning economic row internally.",
      "If topology is unresolved or auto-selected case-row overrides still produce zero output deltas, persist INSUFFICIENT_DATA through FMS instead of continuing exploratory reads.",
    ]
  )
  guided["next_actions"] = action_rows
  return guided


__all__ = [
  "MISSING",
  "SCENARIO_TABLE_PREFIX",
  "SCENARIO_TABLE_SCALAR_KEYS",
  "attach_scenario_no_effect_guidance",
  "is_number",
  "is_scenario_table_value_item",
  "projection_periods_for_scenario_table_item",
  "scenario_no_effect_warning",
  "scenario_table_scalar_value",
]
