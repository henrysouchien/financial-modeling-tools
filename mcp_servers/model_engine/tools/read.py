from __future__ import annotations

from collections.abc import Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any


_SCENARIO_TABLE_PREFIX = "tpl.a.scenario_tables."
_SCENARIO_TABLE_SCALAR_KEYS = {
  "all",
  "model_value",
  "override",
  "projection",
  "scenario_value",
  "value",
}
_MISSING = object()


@dataclass(frozen=True)
class ModelReadDeps:
  validate_file_path: Callable[[str], str]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  summarize: Callable[..., dict[str, Any]]
  values: Callable[..., dict[str, Any]]
  enrich_valuation_rows: Callable[[list[Any]], list[dict[str, Any]]]
  group_valuation_rows: Callable[[list[dict[str, Any]]], dict[str, Any]]
  valuation_derived_metrics: Callable[[list[dict[str, Any]]], dict[str, Any]]
  valuation_policy_summary: Callable[[list[dict[str, Any]]], dict[str, Any]]
  valuation_input_readiness: Callable[[list[dict[str, Any]]], dict[str, Any]]
  valuation_readback_items: Sequence[Any]
  valuation_readback_item_ids: Sequence[str]
  workbook_presentation_fingerprint: Callable[..., dict[str, Any]]
  workbook_presentation_gap: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
  find: Callable[..., list[dict[str, Any]]]
  coerce_json_list_arg: Callable[..., list[Any]]
  drivers: Callable[..., dict[str, Any]]
  sensitivity: Callable[..., dict[str, Any]]
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  invalid_override_period_error: Callable[..., Exception]
  invalid_override_value_error: Callable[..., Exception]
  scenario: Callable[..., dict[str, Any]]


def _is_scenario_table_value_item(item_id: str) -> bool:
  if not item_id.startswith(_SCENARIO_TABLE_PREFIX):
    return False
  return not item_id.rsplit(".", 1)[-1].startswith("scenario_")


def _scenario_table_scalar_value(period_values: Any) -> Any:
  if not isinstance(period_values, dict):
    return period_values
  if len(period_values) != 1:
    return _MISSING
  raw_key, raw_value = next(iter(period_values.items()))
  key = str(raw_key).strip().lower()
  if key in _SCENARIO_TABLE_SCALAR_KEYS:
    return raw_value
  return _MISSING


def _projection_periods_for_scenario_table_item(
  *,
  deps: ModelReadDeps,
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


def _model_quality_readiness_for_valuation_summary(
  *,
  deps: ModelReadDeps,
  file_path: str,
  historical_cutoff_year: int | None,
  valuation_input_readiness: dict[str, Any],
) -> dict[str, Any]:
  try:
    summary_result = deps.summarize(
      file_path,
      historical_cutoff_year=historical_cutoff_year,
      include_items=False,
    )
    readiness = summary_result.get("model_quality_readiness")
  except Exception as exc:
    readiness = {
      "status": "incomplete",
      "scope": "model_quality",
      "projection_periods": [],
      "domains": {},
      "issues": [
        {
          "code": "model_quality_readiness_unavailable",
          "severity": "warning",
          "domain": "valuation",
          "detail": (
            "model_summarize could not compute model_quality_readiness "
            f"for this valuation summary: {exc}"
          ),
        }
      ],
      "summary": "incomplete: model_quality_readiness unavailable",
    }
  if not isinstance(readiness, dict):
    readiness = {
      "status": "incomplete",
      "scope": "model_quality",
      "projection_periods": [],
      "domains": {},
      "issues": [
        {
          "code": "model_quality_readiness_unavailable",
          "severity": "warning",
          "domain": "valuation",
          "detail": "model_summarize did not return model_quality_readiness",
        }
      ],
      "summary": "incomplete: model_quality_readiness unavailable",
    }
  return _merge_valuation_input_readiness(readiness, valuation_input_readiness)


def _merge_valuation_input_readiness(
  readiness: dict[str, Any],
  valuation_input_readiness: dict[str, Any],
) -> dict[str, Any]:
  result = deepcopy(readiness)
  result.setdefault("scope", "model_quality")
  result.setdefault("projection_periods", [])
  domains = result.setdefault("domains", {})
  if not isinstance(domains, dict):
    domains = {}
    result["domains"] = domains
  issues = result.setdefault("issues", [])
  if not isinstance(issues, list):
    issues = []
    result["issues"] = issues

  input_issue = _valuation_input_quality_issue(valuation_input_readiness)
  if input_issue is not None:
    valuation_domain = domains.get("valuation")
    if not isinstance(valuation_domain, dict):
      valuation_domain = {
        "status": "ready",
        "required_items": [
          "tpl.v.current_valuation.stock_price",
          "tpl.v.current_valuation.shares_outstanding",
          "tpl.v.current_valuation.net_debt",
          "tpl.v.dcf.dcf_price",
        ],
        "missing_periods": [],
        "issues": [],
      }
      domains["valuation"] = valuation_domain
    domain_issues = valuation_domain.get("issues")
    if not isinstance(domain_issues, list):
      domain_issues = []
      valuation_domain["issues"] = domain_issues
    _upsert_quality_issue(domain_issues, input_issue)
    _upsert_quality_issue(issues, input_issue)
    valuation_domain["status"] = _quality_status(domain_issues)

    result["status"] = _quality_status(
      issues,
      fallback=str(result.get("status") or "unknown"),
    )
    result["summary"] = _model_quality_summary(result["status"], domains)
    return result

  status = str(result.get("status") or "unknown")
  if status not in {"ready", "incomplete", "blocked", "unknown"}:
    status = _quality_status(issues, fallback="unknown")
    result["status"] = status
  if not isinstance(result.get("summary"), str) or not result.get("summary"):
    result["summary"] = _model_quality_summary(status, domains)
  return result


def _valuation_input_quality_issue(
  valuation_input_readiness: dict[str, Any],
) -> dict[str, Any] | None:
  if valuation_input_readiness.get("status") != "incomplete":
    return None
  missing = [str(item) for item in valuation_input_readiness.get("missing") or []]
  severity = "blocking" if missing else "warning"
  detail = (
    f"valuation_input_readiness is incomplete; missing inputs: {', '.join(missing)}"
    if missing
    else "valuation_input_readiness is incomplete; only placeholder/staleness flags may be present"
  )
  return {
    "code": "valuation_inputs_incomplete",
    "severity": severity,
    "domain": "valuation",
    "detail": detail,
    "item_id": None,
    "missing_periods": [],
    "related_item_ids": missing,
  }


def _upsert_quality_issue(issues: list[Any], issue: dict[str, Any]) -> None:
  key = (issue.get("domain"), issue.get("code"))
  for index, existing in enumerate(issues):
    if (
      isinstance(existing, dict)
      and (existing.get("domain"), existing.get("code")) == key
    ):
      issues[index] = issue
      return
  issues.append(issue)


def _quality_status(issues: list[Any], *, fallback: str = "ready") -> str:
  fallback = fallback if fallback in {"ready", "incomplete", "blocked", "unknown"} else "unknown"
  if any(
    isinstance(issue, dict) and issue.get("severity") == "blocking"
    for issue in issues
  ) or fallback == "blocked":
    return "blocked"
  if issues or fallback == "incomplete":
    return "incomplete"
  return fallback


def _model_quality_summary(status: str, domains: dict[str, Any]) -> str:
  if status == "ready":
    return "share count, working capital, valuation, and segment-basis quality checks are ready"
  pieces = [
    f"{domain}={readiness.get('status')}"
    for domain, readiness in domains.items()
    if isinstance(readiness, dict) and readiness.get("status") != "ready"
  ]
  return f"{status}: " + ", ".join(pieces) if pieces else f"{status}: model quality readiness unavailable"


def model_summarize_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  historical_cutoff_year: int | None = None,
  include_items: bool = False,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    result = deps.summarize(
      file_path,
      historical_cutoff_year=historical_cutoff_year,
      include_items=include_items,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_valuation_summary_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  periods: str | list[int | str] = "all",
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    values_result = deps.values(
      file_path,
      deps.valuation_readback_item_ids,
      periods=periods,
      historical_cutoff_year=historical_cutoff_year,
    )
    rows = deps.enrich_valuation_rows(list(values_result.get("items") or []))
    valuation_input_readiness = deps.valuation_input_readiness(rows)
    model_quality_readiness = _model_quality_readiness_for_valuation_summary(
      deps=deps,
      file_path=file_path,
      historical_cutoff_year=historical_cutoff_year,
      valuation_input_readiness=valuation_input_readiness,
    )
    return {
      "status": "ok",
      "periods_returned": values_result.get("periods_returned"),
      "period_count": values_result.get("period_count"),
      "canonical_item_ids": {
        entry["key"]: entry["item_id"] for entry in deps.valuation_readback_items
      },
      "sections": deps.group_valuation_rows(rows),
      "items": rows,
      "derived_metrics": deps.valuation_derived_metrics(rows),
      "valuation_policy": deps.valuation_policy_summary(rows),
      "valuation_input_readiness": valuation_input_readiness,
      "model_quality_readiness": model_quality_readiness,
      "next_actions": [
        "Use returned canonical_item_ids with model_values only for additional period detail.",
        "Use model_find only for non-standard custom valuation rows missing from this summary.",
        "Check valuation_input_readiness before clean DCF acceptance; missing rows or legacy placeholder-like values are input gaps, not valuation conclusions.",
        "Check model_quality_readiness before treating valuation output as client-ready; blocked or incomplete model domains are model-quality gates, not valuation conclusions.",
        "Use valuation_policy to explain ERP, raw beta, adjusted beta, beta floor, and cost-of-equity basis after input readiness is complete.",
        "For WACC rationale in Thesis, update an existing registered assumption or add a data gap if no stable assumption id exists.",
      ],
    }
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_presentation_fingerprint_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  max_cells_per_sheet: int = 50_000,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    fingerprint = deps.workbook_presentation_fingerprint(
      file_path,
      max_cells_per_sheet=max_cells_per_sheet,
    )
    return {"status": "ok", "fingerprint": fingerprint}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


def model_presentation_compare_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  reference_path: str,
  max_cells_per_sheet: int = 50_000,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    reference_path = deps.validate_file_path(reference_path)
    generated = deps.workbook_presentation_fingerprint(
      file_path,
      max_cells_per_sheet=max_cells_per_sheet,
    )
    reference = deps.workbook_presentation_fingerprint(
      reference_path,
      max_cells_per_sheet=max_cells_per_sheet,
    )
    gap = deps.workbook_presentation_gap(generated, reference)
    return {
      "status": "ok",
      "generated": generated,
      "reference": reference,
      "gap": gap,
    }
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


def model_find_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  query: str,
  limit: int = 20,
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    results = deps.find(
      file_path,
      query,
      limit=limit,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", "items": results, "count": len(results)}
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_values_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  item_ids: list[str] | str,
  periods: str | list[int | str] = "all",
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    parsed_item_ids = deps.coerce_json_list_arg(item_ids, name="item_ids")
    result = deps.values(
      file_path,
      [str(item_id) for item_id in parsed_item_ids],
      periods=periods,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_drivers_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  item_id: str,
  depth: int = 3,
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    result = deps.drivers(
      file_path,
      item_id,
      depth=depth,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_sensitivity_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  target_id: str,
  n: int = 15,
  bump_pct: float = 0.10,
  candidate_filter: str = "drivers",
  max_candidates: int | None = None,
  candidate_ids: list[str] | None = None,
  sensitivity_mode: str | None = None,
  recompute_policy: str = "projection_safe",
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    result = deps.sensitivity(
      file_path,
      target_id,
      n=n,
      bump_pct=bump_pct,
      candidate_filter=candidate_filter,
      max_candidates=max_candidates,
      candidate_ids=candidate_ids,
      sensitivity_mode=sensitivity_mode,
      recompute_policy=recompute_policy,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_scenario_handler(
  *,
  deps: ModelReadDeps,
  file_path: str,
  overrides: dict[str, dict[str, float]] | str,
  compare_items: list[str] | str | None = None,
  recompute_policy: str = "projection_safe",
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    file_path = deps.validate_file_path(file_path)
    overrides = deps.coerce_json_dict_arg(overrides, name="overrides")
    if compare_items is not None:
      compare_items = [
        str(item_id)
        for item_id in deps.coerce_json_list_arg(
          compare_items,
          name="compare_items",
        )
      ]
    normalized: dict[str, dict[int, float]] = {}
    if not isinstance(overrides, dict):
      return {
        "status": "error",
        "error": "overrides must be a dict mapping item_id -> {period -> value}",
      }
    for item_id, period_values in overrides.items():
      item_id_str = str(item_id)
      if _is_scenario_table_value_item(item_id_str):
        scalar_value = _scenario_table_scalar_value(period_values)
        if scalar_value is not _MISSING:
          try:
            value = float(scalar_value)
          except (TypeError, ValueError) as exc:
            raise deps.invalid_override_value_error(
              item_id_str,
              "projection",
              scalar_value,
              cause=str(exc),
            ) from exc
          normalized[item_id_str] = {
            period: value
            for period in _projection_periods_for_scenario_table_item(
              deps=deps,
              file_path=file_path,
              item_id=item_id_str,
              historical_cutoff_year=historical_cutoff_year,
            )
          }
          continue
      if not isinstance(period_values, dict):
        return {
          "status": "error",
          "error": f"overrides['{item_id}'] must be a dict mapping period -> value",
        }
      normalized_values: dict[int, float] = {}
      for raw_period, raw_value in period_values.items():
        try:
          period = int(raw_period)
        except (TypeError, ValueError) as exc:
          raise deps.invalid_override_period_error(
            raw_period,
            file_path=file_path,
            historical_cutoff_year=historical_cutoff_year,
            cause=str(exc),
          ) from exc
        try:
          normalized_values[period] = float(raw_value)
        except (TypeError, ValueError) as exc:
          raise deps.invalid_override_value_error(
            str(item_id),
            raw_period,
            raw_value,
            cause=str(exc),
          ) from exc
      normalized[str(item_id)] = normalized_values

    result = deps.scenario(
      file_path,
      normalized,
      compare_items=compare_items,
      recompute_policy=recompute_policy,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelReadToolFunctions:
  model_summarize: Callable[..., dict[str, Any]]
  model_valuation_summary: Callable[..., dict[str, Any]]
  model_presentation_fingerprint: Callable[..., dict[str, Any]]
  model_presentation_compare: Callable[..., dict[str, Any]]
  model_find: Callable[..., dict[str, Any]]
  model_values: Callable[..., dict[str, Any]]
  model_drivers: Callable[..., dict[str, Any]]
  model_sensitivity: Callable[..., dict[str, Any]]
  model_scenario: Callable[..., dict[str, Any]]


def _parent_read_deps(parent_namespace: ParentNamespaceProvider) -> ModelReadDeps:
  return parent_namespace()["_model_read_deps"]()


def _parent_handler(
  parent_namespace: ParentNamespaceProvider,
  name: str,
) -> Callable[..., dict[str, Any]]:
  return parent_namespace()[name]


def _bind_parent_module(
  function: Callable[..., dict[str, Any]],
  parent_namespace: ParentNamespaceProvider,
) -> Callable[..., dict[str, Any]]:
  parent_module = parent_namespace().get("__name__")
  if isinstance(parent_module, str):
    function.__module__ = parent_module
    function.__qualname__ = function.__name__
  return function


def _register_tool(
  mcp: Any,
  function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
  registered = mcp.tool()(function)
  return registered or function


def register_model_read_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ModelReadToolFunctions | None = None,
) -> ModelReadToolFunctions:
  functions = functions or build_model_read_tool_functions(
    parent_namespace=parent_namespace,
  )
  selected_tool_names = tool_names or tuple(functions.__dict__)
  for name in selected_tool_names:
    functions = replace(
      functions,
      **{
        name: _register_tool(
          mcp,
          _bind_parent_module(getattr(functions, name), parent_namespace),
        )
      },
    )
  return functions


def build_model_read_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ModelReadToolFunctions:
  def model_summarize(
    file_path: str,
    historical_cutoff_year: int | None = None,
    include_items: bool = False,
  ) -> dict[str, Any]:
    """Summarize model structure: sheets, sections, item counts, key metrics.

    Discovery: use model_build output_path or a prior model_summarize/model_find
    result to obtain file_path. Set include_items=True when choosing item_ids for
    follow-up model_values, model_drivers, or model_sensitivity calls.
    """
    return _parent_handler(parent_namespace, "_model_summarize_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      historical_cutoff_year=historical_cutoff_year,
      include_items=include_items,
    )

  def model_valuation_summary(
    file_path: str,
    periods: str | list[int | str] = "all",
    historical_cutoff_year: int | None = None,
  ) -> dict[str, Any]:
    """Read canonical valuation, DCF, WACC, P/E, and EV/EBITDA rows.

    Discovery: use model_build output_path or a current-model file_path from
    portfolio-mcp.get_current_model. Use model_summarize(include_items=True) only
    when this canonical valuation reader reports an unknown-item gap.

    Use this before dcf-relative-valuation or valuation QA. It returns the stable
    SIA valuation item IDs and values for common outputs so agents do not need
    repeated model_find probes for DCF price, terminal value, WACC, P/E, or
    EV/EBITDA rows. If an older/custom workbook lacks a canonical row, that row
    appears with an unknown-item error and suggested recovery rather than hiding
    the gap.
    """
    return _parent_handler(parent_namespace, "_model_valuation_summary_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      periods=periods,
      historical_cutoff_year=historical_cutoff_year,
    )

  def model_presentation_fingerprint(
    file_path: str,
    max_cells_per_sheet: int = 50_000,
  ) -> dict[str, Any]:
    """Inventory workbook presentation/style primitives for rendering review.

    Discovery: pass file_path from model_build output or a reference workbook.
    Use this single-workbook inventory to inspect visible sheets, fills, borders,
    merged ranges, widths, and freeze panes. Use model_presentation_compare when
    you also have a reference workbook and need a side-by-side formatting diff.
    This is not a substitute for visual benchmark review before claiming a model
    is client-ready.
    """
    return _parent_handler(
      parent_namespace,
      "_model_presentation_fingerprint_handler",
    )(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      max_cells_per_sheet=max_cells_per_sheet,
    )

  def model_presentation_compare(
    file_path: str,
    reference_path: str,
    max_cells_per_sheet: int = 50_000,
  ) -> dict[str, Any]:
    """Compare observed workbook presentation primitives against a reference workbook.

    Discovery: file_path is usually a fresh model_build output; reference_path is
    a hand-built or client-ready workbook such as Henry's MSCI/PCTY references.
    Use model_presentation_fingerprint instead when you only need to inventory one
    workbook. This comparison is intentionally coarse so stale forecast values in
    the reference do not pollute the formatting review. Treat it as an inventory
    diff, not as a client-ready formatting verdict.
    """
    return _parent_handler(parent_namespace, "_model_presentation_compare_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      reference_path=reference_path,
      max_cells_per_sheet=max_cells_per_sheet,
    )

  def model_find(
    file_path: str,
    query: str,
    limit: int = 20,
    historical_cutoff_year: int | None = None,
  ) -> dict[str, Any]:
    """Search for line items by name or ID substring match.

    Discovery: pass file_path from model_build output or model_summarize, then use
    query terms from the visible financial statement label or expected concept id.
    Returned item ids feed model_values, model_drivers, and model_sensitivity.
    For thesis-factor scenario anchors, use model_find_scenario_anchor instead of
    generic line-item search.
    """
    return _parent_handler(parent_namespace, "_model_find_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      query=query,
      limit=limit,
      historical_cutoff_year=historical_cutoff_year,
    )

  def model_values(
    file_path: str,
    item_ids: list[str] | str,
    periods: str | list[int | str] = "all",
    historical_cutoff_year: int | None = None,
  ) -> dict[str, Any]:
    """Get full time series values for one or more line items.

    Discovery: use model_find or model_summarize(include_items=True) to obtain
    item_ids for this file_path. Pass a JSON list or native list of item ids.

    Periods: use "all", "historical", "projection", "YYYY:YYYY", a comma list
    such as "2024,2025,2026", or a native list such as [2024, 2025, "FY2026"].

    Batching: model_values accepts normal analytical batches with more than 10
    item_ids. If item_ids x periods would produce an oversized response, it
    returns a structured model_values_request_too_large error with suggested
    item_id batches instead of a raw validation failure.
    """
    return _parent_handler(parent_namespace, "_model_values_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      item_ids=item_ids,
      periods=periods,
      historical_cutoff_year=historical_cutoff_year,
    )

  def model_drivers(
    file_path: str,
    item_id: str,
    depth: int = 3,
    historical_cutoff_year: int | None = None,
  ) -> dict[str, Any]:
    """Trace the upstream driver tree for a line item.

    Discovery: use model_find or model_summarize(include_items=True) to obtain
    item_id for this file_path. Increase depth only when the first trace does not
    reach the relevant drivers.
    """
    return _parent_handler(parent_namespace, "_model_drivers_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      item_id=item_id,
      depth=depth,
      historical_cutoff_year=historical_cutoff_year,
    )

  def model_sensitivity(
    file_path: str,
    target_id: str,
    n: int = 15,
    bump_pct: float = 0.10,
    candidate_filter: str = "drivers",
    max_candidates: int | None = None,
    candidate_ids: list[str] | None = None,
    sensitivity_mode: str | None = None,
    recompute_policy: str = "projection_safe",
    historical_cutoff_year: int | None = None,
  ) -> dict[str, Any]:
    """Rank upstream inputs by impact on a target metric. Can be slow.

    Discovery: use model_find or model_summarize(include_items=True) to obtain
    target_id for this file_path. Use candidate_filter='drivers' for focused
    upstream sensitivity before broadening the candidate set. Pass candidate_ids
    to evaluate a specific upstream candidate row set instead of the global top-N
    preselection. By default this uses projection-safe semantics, which respect
    workbook period overrides; pass recompute_policy='legacy_global' only for
    compatibility diagnostics.
    """
    return _parent_handler(parent_namespace, "_model_sensitivity_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      target_id=target_id,
      n=n,
      bump_pct=bump_pct,
      candidate_filter=candidate_filter,
      max_candidates=max_candidates,
      candidate_ids=candidate_ids,
      sensitivity_mode=sensitivity_mode,
      recompute_policy=recompute_policy,
      historical_cutoff_year=historical_cutoff_year,
    )

  def model_scenario(
    file_path: str,
    overrides: dict[str, dict[str, float]] | str,
    compare_items: list[str] | str | None = None,
    recompute_policy: str = "projection_safe",
    historical_cutoff_year: int | None = None,
  ) -> dict[str, Any]:
    """Apply input overrides and compare resulting metrics against the base case.

    Discovery: file_path comes from model_build output. Use model_find or
    model_summarize(include_items=True) to choose override item ids and
    compare_items before running the scenario.
    """
    return _parent_handler(parent_namespace, "_model_scenario_handler")(
      deps=_parent_read_deps(parent_namespace),
      file_path=file_path,
      overrides=overrides,
      compare_items=compare_items,
      recompute_policy=recompute_policy,
      historical_cutoff_year=historical_cutoff_year,
    )

  return ModelReadToolFunctions(
    model_summarize=model_summarize,
    model_valuation_summary=model_valuation_summary,
    model_presentation_fingerprint=model_presentation_fingerprint,
    model_presentation_compare=model_presentation_compare,
    model_find=model_find,
    model_values=model_values,
    model_drivers=model_drivers,
    model_sensitivity=model_sensitivity,
    model_scenario=model_scenario,
  )


__all__ = [
  "ModelReadDeps",
  "ModelReadToolFunctions",
  "build_model_read_tool_functions",
  "model_drivers_handler",
  "model_find_handler",
  "model_presentation_compare_handler",
  "model_presentation_fingerprint_handler",
  "model_scenario_handler",
  "model_sensitivity_handler",
  "model_summarize_handler",
  "model_valuation_summary_handler",
  "model_values_handler",
  "register_model_read_tools",
]
