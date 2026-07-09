from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import inspect
import json
import os
from pathlib import Path
import statistics
from typing import Any
import urllib.parse
import urllib.request

from schema.overrides import TickerOverrides
from api.research.source_html import provider_symbol
from mcp_servers.model_engine.reconcile_candidates import (
  RECONCILE_CANDIDATE_CONCEPTS as RECONCILE_CANDIDATE_CONCEPTS,
  best_value_for_target as _best_value_for_target_impl,
  current_values_from_diagnostic_inputs as current_values_from_diagnostic_inputs,
  delta_by_year as delta_by_year,
  evaluate_reconcile_candidates as _evaluate_reconcile_candidates_impl,
  metric_value_for_concept as metric_value_for_concept,
  metrics_by_tag_for_concept as _metrics_by_tag_for_concept_impl,
  normalize_metric_entries as normalize_metric_entries,
  relative_distance as relative_distance,
  target_value_for_candidate as target_value_for_candidate,
)

RECONCILE_DIAGNOSTICS_CHECKED = [
  "is_subtotal_integrity",
  "bs_balance",
  "cf_reconciliation",
]

RECONCILE_UNSUPPORTED_SUBTOTALS = {
  ("is_subtotal_integrity", "pretax_income"),
  ("is_subtotal_integrity", "net_income"),
}

RECONCILE_SEVERITY_ORDER = {
  "ok": 0,
  "gap": 1,
  "material_gap": 2,
  "inconsistency": 3,
  "unknown": 4,
}


def reconcile_error(error_type: str, message: str, *, recoverable: bool) -> dict:
  return {
    "status": "error",
    "error_type": error_type,
    "message": message,
    "recoverable": bool(recoverable),
  }


def validate_reconcile_mode(mode: str, reconcile_error_fn=None) -> str | dict:
  reconcile_error_fn = reconcile_error if reconcile_error_fn is None else reconcile_error_fn
  normalized = str(mode or "").strip().lower()
  if normalized not in {"dry_run", "apply"}:
    return reconcile_error_fn(
      "invalid_mode",
      "mode must be one of {'dry_run', 'apply'}",
      recoverable=False,
    )
  return normalized


def validate_reconcile_build_kwargs(
  build_kwargs: dict,
  *,
  coerce_json_dict_arg_fn,
  signature,
  reconcile_error_fn=None,
  inspect_module=inspect,
) -> dict | dict[str, Any]:
  reconcile_error_fn = reconcile_error if reconcile_error_fn is None else reconcile_error_fn
  try:
    kwargs = coerce_json_dict_arg_fn(build_kwargs, name="build_kwargs")
  except Exception as exc:
    return reconcile_error_fn("build_kwargs_invalid", str(exc), recoverable=False)

  allowed_keys = set(signature.parameters)
  unknown_keys = sorted(set(kwargs) - allowed_keys)
  if unknown_keys:
    return reconcile_error_fn(
      "build_kwargs_invalid",
      f"Unsupported model_build kwargs: {unknown_keys}",
      recoverable=False,
    )

  required_keys = [
    name
    for name, parameter in signature.parameters.items()
    if parameter.default is inspect_module._empty
    and parameter.kind in {
      inspect_module.Parameter.POSITIONAL_OR_KEYWORD,
      inspect_module.Parameter.KEYWORD_ONLY,
    }
  ]
  required_keys.append("n_historical")
  missing = [key for key in dict.fromkeys(required_keys) if key not in kwargs]
  if missing:
    return reconcile_error_fn(
      "build_kwargs_invalid",
      f"Missing required model_build kwargs: {missing}",
      recoverable=False,
    )

  try:
    kwargs["most_recent_fy"] = int(kwargs["most_recent_fy"])
    kwargs["n_historical"] = int(kwargs["n_historical"])
  except Exception as exc:
    return reconcile_error_fn(
      "build_kwargs_invalid",
      f"most_recent_fy and n_historical must be integers: {exc}",
      recoverable=False,
    )
  if kwargs["n_historical"] <= 0:
    return reconcile_error_fn(
      "build_kwargs_invalid",
      "n_historical must be positive",
      recoverable=False,
    )
  return kwargs


def reconcile_historical_years(build_kwargs: dict) -> list[int]:
  most_recent_fy = int(build_kwargs["most_recent_fy"])
  n_historical = int(build_kwargs["n_historical"])
  return list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))


def normalized_reconcile_path(path: Any) -> str:
  return str(Path(os.path.expanduser(str(path))).resolve(strict=False))


def validate_reconcile_file_path_match(
  file_path: str,
  build_kwargs: dict,
  *,
  reconcile_error_fn=None,
  normalized_reconcile_path_fn=None,
) -> dict | None:
  reconcile_error_fn = reconcile_error if reconcile_error_fn is None else reconcile_error_fn
  normalized_reconcile_path_fn = (
    normalized_reconcile_path
    if normalized_reconcile_path_fn is None
    else normalized_reconcile_path_fn
  )
  output_path = build_kwargs.get("output_path")
  if not output_path:
    return reconcile_error_fn(
      "build_kwargs_invalid",
      "build_kwargs.output_path is required",
      recoverable=False,
    )
  if normalized_reconcile_path_fn(file_path) != normalized_reconcile_path_fn(output_path):
    return reconcile_error_fn(
      "file_path_mismatch",
      (
        "file_path must match build_kwargs.output_path: "
        f"{file_path!r} != {output_path!r}"
      ),
      recoverable=False,
    )
  return None


def validate_inputs_field(entry: dict, where: str, reconcile_error_fn=None) -> dict | None:
  reconcile_error_fn = reconcile_error if reconcile_error_fn is None else reconcile_error_fn
  inputs = entry.get("inputs")
  if inputs is None:
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      f"{where} missing required field 'inputs' on wrong_tag_suspected entry",
      recoverable=False,
    )
  if not isinstance(inputs, dict) or not inputs:
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      f"{where} 'inputs' must be a non-empty dict",
      recoverable=False,
    )
  for key, value in inputs.items():
    if not isinstance(key, str) or not isinstance(value, (int, float)):
      return reconcile_error_fn(
        "invalid_diagnostic_report",
        (
          f"{where} 'inputs' must map str to numeric; "
          f"got {key!r}: {type(value).__name__}"
        ),
        recoverable=False,
      )
  return None


def validate_reconcile_diagnostic_report(
  diagnostic_report: dict,
  *,
  diagnostics_checked=None,
  reconcile_error_fn=None,
) -> dict | dict[str, Any]:
  diagnostics_checked = (
    RECONCILE_DIAGNOSTICS_CHECKED
    if diagnostics_checked is None
    else diagnostics_checked
  )
  reconcile_error_fn = reconcile_error if reconcile_error_fn is None else reconcile_error_fn
  if not isinstance(diagnostic_report, dict):
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      (
        "diagnostic_report must be a dict, "
        f"got {type(diagnostic_report).__name__}"
      ),
      recoverable=False,
    )

  required_keys = set(diagnostics_checked)
  missing = required_keys - set(diagnostic_report.keys())
  if missing:
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      f"Missing required keys: {sorted(missing)}",
      recoverable=False,
    )

  is_check = diagnostic_report["is_subtotal_integrity"]
  bs_check = diagnostic_report["bs_balance"]
  cf_check = diagnostic_report["cf_reconciliation"]
  if not isinstance(is_check, dict) or not isinstance(is_check.get("results"), list):
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      "is_subtotal_integrity.results must be a list",
      recoverable=False,
    )
  if not isinstance(bs_check, dict) or not isinstance(bs_check.get("by_year"), dict):
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      "bs_balance.by_year must be a dict",
      recoverable=False,
    )
  if (
    not isinstance(cf_check, dict)
    or not isinstance(cf_check.get("net_change_reconciliation"), dict)
    or not isinstance(cf_check["net_change_reconciliation"].get("by_year"), dict)
  ):
    return reconcile_error_fn(
      "invalid_diagnostic_report",
      "cf_reconciliation.net_change_reconciliation.by_year must be a dict",
      recoverable=False,
    )
  return diagnostic_report


def flagged_reconcile_entries(
  diagnostics: dict[str, Any],
  validate_inputs_field_fn=None,
) -> list[dict[str, Any]] | dict:
  validate_inputs_field_fn = (
    validate_inputs_field
    if validate_inputs_field_fn is None
    else validate_inputs_field_fn
  )
  flagged: list[dict[str, Any]] = []

  is_check = diagnostics["is_subtotal_integrity"]
  for entry in list(is_check.get("results", []) or []):
    if isinstance(entry, dict) and entry.get("kind") == "wrong_tag_suspected":
      where = (
        "is_subtotal_integrity.results"
        f"[subtotal={entry.get('subtotal')}, year={entry.get('year')}]"
      )
      err = validate_inputs_field_fn(entry, where)
      if err is not None:
        return err
      flagged.append(
        {
          "diagnostic_type": "is_subtotal_integrity",
          "subtotal_name": str(entry.get("subtotal") or ""),
          "year": int(entry["year"]),
          "entry": entry,
        }
      )

  bs_check = diagnostics["bs_balance"]
  for year_str, payload in dict(bs_check.get("by_year", {}) or {}).items():
    if isinstance(payload, dict) and payload.get("kind") == "wrong_tag_suspected":
      err = validate_inputs_field_fn(payload, f"bs_balance.by_year[{year_str}]")
      if err is not None:
        return err
      flagged.append(
        {
          "diagnostic_type": "bs_balance",
          "subtotal_name": "balance_sheet",
          "year": int(year_str),
          "entry": payload,
        }
      )

  cf_check = diagnostics["cf_reconciliation"]
  by_year = (cf_check.get("net_change_reconciliation") or {}).get("by_year", {})
  for year_str, payload in dict(by_year or {}).items():
    if isinstance(payload, dict) and payload.get("kind") == "wrong_tag_suspected":
      err = validate_inputs_field_fn(
        payload,
        f"cf_reconciliation.net_change_reconciliation.by_year[{year_str}]",
      )
      if err is not None:
        return err
      flagged.append(
        {
          "diagnostic_type": "cf_reconciliation",
          "subtotal_name": "net_change",
          "year": int(year_str),
          "entry": payload,
        }
      )

  return flagged


def group_reconcile_entries(flagged: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
  groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
  for entry in flagged:
    groups[(entry["diagnostic_type"], entry["subtotal_name"])].append(entry)
  return dict(groups)


def group_payload(
  ticker: str,
  key: tuple[str, str],
  entries: list[dict[str, Any]],
  *,
  delta_by_year_fn=None,
  filing_pointer_fn=None,
) -> dict:
  delta_by_year_fn = delta_by_year if delta_by_year_fn is None else delta_by_year_fn
  filing_pointer_fn = filing_pointer if filing_pointer_fn is None else filing_pointer_fn
  return {
    "diagnostic_type": key[0],
    "subtotal_name": key[1],
    "delta_by_year": delta_by_year_fn(entries),
    "filing_pointer": filing_pointer_fn(ticker, entries),
  }


def worst_severity(
  entries: list[dict[str, Any]],
  severity_order=None,
) -> str:
  severity_order = RECONCILE_SEVERITY_ORDER if severity_order is None else severity_order
  severities = [
    str(entry.get("severity") or "unknown")
    for entry in entries
    if isinstance(entry, dict)
  ]
  if not severities:
    return "unknown"
  return max(severities, key=lambda severity: severity_order.get(severity, 4))


def before_summary(
  entries: list[dict[str, Any]],
  *,
  worst_severity_fn=None,
  delta_by_year_fn=None,
) -> dict:
  worst_severity_fn = worst_severity if worst_severity_fn is None else worst_severity_fn
  delta_by_year_fn = delta_by_year if delta_by_year_fn is None else delta_by_year_fn
  raw_entries = [entry["entry"] for entry in entries]
  return {
    "severity": worst_severity_fn(raw_entries),
    "kind": "wrong_tag_suspected",
    "delta_by_year": delta_by_year_fn(entries),
  }


def entries_from_serialized_report(
  report: dict,
  diagnostic_type: str,
  subtotal_name: str,
) -> list[dict[str, Any]]:
  if diagnostic_type == "is_subtotal_integrity":
    results = ((report.get("is_subtotal_integrity") or {}).get("results") or [])
    return [
      {
        "year": int(entry.get("year")),
        "entry": entry,
      }
      for entry in results
      if isinstance(entry, dict) and str(entry.get("subtotal") or "") == subtotal_name
    ]
  if diagnostic_type == "bs_balance":
    by_year = ((report.get("bs_balance") or {}).get("by_year") or {})
    return [
      {
        "year": int(year),
        "entry": payload,
      }
      for year, payload in dict(by_year).items()
      if isinstance(payload, dict)
    ]
  if diagnostic_type == "cf_reconciliation":
    by_year = (
      ((report.get("cf_reconciliation") or {}).get("net_change_reconciliation") or {})
      .get("by_year")
      or {}
    )
    return [
      {
        "year": int(year),
        "entry": payload,
      }
      for year, payload in dict(by_year).items()
      if isinstance(payload, dict)
    ]
  return []


def after_summary(
  report: dict | None,
  diagnostic_type: str,
  subtotal_name: str,
  *,
  serialize_diagnostic_report_fn,
  entries_from_serialized_report_fn=None,
  worst_severity_fn=None,
  delta_by_year_fn=None,
) -> dict:
  entries_from_serialized_report_fn = (
    entries_from_serialized_report
    if entries_from_serialized_report_fn is None
    else entries_from_serialized_report_fn
  )
  worst_severity_fn = worst_severity if worst_severity_fn is None else worst_severity_fn
  delta_by_year_fn = delta_by_year if delta_by_year_fn is None else delta_by_year_fn
  serialized = serialize_diagnostic_report_fn(report)
  if not isinstance(serialized, dict):
    return {"severity": "unknown", "kind": None, "delta_by_year": {}}
  entries = entries_from_serialized_report_fn(serialized, diagnostic_type, subtotal_name)
  raw_entries = [entry["entry"] for entry in entries]
  return {
    "severity": worst_severity_fn(raw_entries),
    "kind": "wrong_tag_suspected"
    if any(entry.get("kind") == "wrong_tag_suspected" for entry in raw_entries)
    else None,
    "delta_by_year": delta_by_year_fn(
      [
        {
          "year": entry["year"],
          "entry": entry["entry"],
        }
        for entry in entries
        if entry["entry"].get("delta") is not None
      ]
    ),
  }


def diagnostic_group_cleared(
  report: dict | None,
  diagnostic_type: str,
  subtotal_name: str,
  *,
  serialize_diagnostic_report_fn,
  entries_from_serialized_report_fn=None,
) -> bool:
  entries_from_serialized_report_fn = (
    entries_from_serialized_report
    if entries_from_serialized_report_fn is None
    else entries_from_serialized_report_fn
  )
  serialized = serialize_diagnostic_report_fn(report)
  if not isinstance(serialized, dict):
    return False
  entries = entries_from_serialized_report_fn(serialized, diagnostic_type, subtotal_name)
  return not any(
    entry["entry"].get("kind") == "wrong_tag_suspected"
    for entry in entries
  )


def filing_pointer(ticker: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
  years = [int(entry["year"]) for entry in entries]
  return {
    "ticker": str(ticker or "").strip().upper(),
    "year": max(years) if years else None,
    "quarter": 4,
  }


def tag_local_part(tag: str) -> str:
  return str(tag).split(":", 1)[-1]


def existing_override_preserved(existing: TickerOverrides | None, affected_input_concept: str) -> list[str]:
  if existing is None:
    return []
  preserved = [
    concept_id
    for concept_id in sorted(existing.overrides or {})
    if concept_id != affected_input_concept
  ]
  preserved.extend(
    f"custom_concepts.{concept_id}"
    for concept_id in sorted(existing.custom_concepts or {})
  )
  return preserved


def override_conflict(
  existing: TickerOverrides | None,
  affected_input_concept: str,
  proposed_field: dict,
) -> dict | None:
  if existing is None:
    return None
  existing_field = (existing.overrides or {}).get(affected_input_concept)
  if existing_field is None:
    return None
  proposed_identity = (
    proposed_field.get("canonical_tag"),
    frozenset(str(tag) for tag in (proposed_field.get("edgar_tags") or [])),
    proposed_field.get("registry_group_id"),
  )
  existing_identity = (
    existing_field.get("canonical_tag"),
    frozenset(str(tag) for tag in (existing_field.get("edgar_tags") or [])),
    existing_field.get("registry_group_id"),
  )
  if proposed_identity == existing_identity:
    return None
  return dict(existing_field)


def new_override_field(
  *,
  matched_tag: str,
  diagnostic_type: str,
  subtotal_name: str,
  entries: list[dict[str, Any]],
  registry_revision: str,
  delta_by_year_fn=None,
  tag_local_part_fn=None,
) -> dict:
  delta_by_year_fn = delta_by_year if delta_by_year_fn is None else delta_by_year_fn
  tag_local_part_fn = tag_local_part if tag_local_part_fn is None else tag_local_part_fn
  deltas = [abs(float(value)) for value in delta_by_year_fn(entries).values()]
  median_delta = statistics.median(deltas) if deltas else 0.0
  years = sorted(str(entry["year"]) for entry in entries)
  return {
    "canonical_tag": matched_tag,
    "edgar_tags": [tag_local_part_fn(matched_tag)],
    "registry_group_id": None,
    "notes": (
      "Auto-generated by build-model Phase 1.5 — "
      f"{diagnostic_type}.{subtotal_name} delta ${median_delta:.1f}m "
      f"matched {matched_tag} across {years}. "
      "See schema/overrides/README.md."
    ),
    "registry_revision": registry_revision,
  }


def merged_ticker_overrides(
  ticker_upper: str,
  existing: TickerOverrides | None,
  affected_input_concept: str,
  override_field: dict,
) -> TickerOverrides:
  file_meta = dict(existing.file_meta or {}) if existing is not None else {}
  file_meta.update(
    {
      "last_updated": datetime.now(timezone.utc).isoformat(),
      "generated_by": "reconcile_subtotal_integrity",
    }
  )
  overrides = dict(existing.overrides or {}) if existing is not None else {}
  overrides[affected_input_concept] = dict(override_field)
  custom_concepts = dict(existing.custom_concepts or {}) if existing is not None else {}
  return TickerOverrides(
    ticker=ticker_upper,
    overrides=overrides,
    custom_concepts=custom_concepts,
    file_meta=file_meta,
    projections=dict(existing.projections or {}) if existing is not None else {},
    semantic_rows=dict(existing.semantic_rows or {}) if existing is not None else {},
    guards=existing.guards if existing is not None else None,
  )


def override_conflict_response(
  ticker: str,
  entries: list[dict[str, Any]],
  affected_input_concept: str,
  existing_override: dict,
  proposed_override: dict,
  filing_pointer_fn=None,
) -> dict:
  filing_pointer_fn = filing_pointer if filing_pointer_fn is None else filing_pointer_fn
  return {
    "status": "escalate_override_conflict",
    "affected_input_concept": affected_input_concept,
    "existing_override": existing_override,
    "proposed_override": proposed_override,
    "filing_pointer": filing_pointer_fn(ticker, entries),
    "recommendation": "manual_review",
  }


def list_metrics(
  ticker: str,
  year: int,
  quarter: int = 4,
  *,
  date_type: str = "FY",
  include_values: bool = True,
  limit: int = 1000,
  os_module=os,
  urllib_parse_module=urllib.parse,
  urllib_request_module=urllib.request,
  json_module=json,
) -> list[dict]:
  api_key = os_module.getenv("EDGAR_API_KEY", "")
  base_url = os_module.getenv("EDGAR_API_URL", "https://www.edgarparser.com").rstrip("/")
  endpoint = f"{base_url}/api/financials/list_metrics"
  ticker_upper = str(ticker).upper()
  params = {
    "ticker": provider_symbol(ticker_upper),
    "year": str(int(year)),
    "quarter": str(int(quarter)),
    "date_type": str(date_type),
    "include_values": str(bool(include_values)).lower(),
    "limit": str(int(limit)),
    "key": api_key,
  }
  request = urllib_request_module.Request(
    f"{endpoint}?{urllib_parse_module.urlencode(params)}",
    headers={"User-Agent": "model-engine-mcp"},
  )
  with urllib_request_module.urlopen(request, timeout=120) as response:
    payload = json_module.loads(response.read().decode("utf-8"))
  if isinstance(payload, list):
    metrics = payload
  elif isinstance(payload, dict):
    if str(payload.get("status") or "").lower() == "error":
      raise RuntimeError(str(payload.get("message") or payload.get("error") or "list_metrics failed"))
    metrics = payload.get("metrics") or payload.get("data") or payload.get("results")
  else:
    metrics = None
  if not isinstance(metrics, list):
    raise RuntimeError("list_metrics response did not include a metrics list")
  return [dict(entry) for entry in metrics if isinstance(entry, dict)]


def metrics_by_tag_for_concept(
  entries: list[dict],
  concept_id: str,
  year: int,
  metric_value_for_concept_fn=None,
) -> dict[str, list[float]]:
  metric_value_for_concept_fn = (
    metric_value_for_concept
    if metric_value_for_concept_fn is None
    else metric_value_for_concept_fn
  )
  return _metrics_by_tag_for_concept_impl(
    entries,
    concept_id,
    year,
    metric_value_for_concept_fn=metric_value_for_concept_fn,
  )


def best_value_for_target(
  values: list[float],
  target: float,
  floor_abs_m: float,
  relative_distance_fn=None,
) -> tuple[float, float]:
  relative_distance_fn = relative_distance if relative_distance_fn is None else relative_distance_fn
  return _best_value_for_target_impl(
    values,
    target,
    floor_abs_m,
    relative_distance_fn=relative_distance_fn,
  )


def evaluate_reconcile_candidates(
  *,
  diagnostic_type: str,
  subtotal_name: str,
  entries: list[dict[str, Any]],
  tags_by_year: dict[int, list[dict]],
  match_tolerance_pct: float,
  match_floor_abs_m: float,
  candidate_concepts_by_key=None,
  delta_by_year_fn=None,
  current_values_from_diagnostic_inputs_fn=None,
  target_value_for_candidate_fn=None,
  metrics_by_tag_for_concept_fn=None,
  best_value_for_target_fn=None,
) -> dict:
  candidate_concepts_by_key = (
    RECONCILE_CANDIDATE_CONCEPTS
    if candidate_concepts_by_key is None
    else candidate_concepts_by_key
  )
  delta_by_year_fn = delta_by_year if delta_by_year_fn is None else delta_by_year_fn
  current_values_from_diagnostic_inputs_fn = (
    current_values_from_diagnostic_inputs
    if current_values_from_diagnostic_inputs_fn is None
    else current_values_from_diagnostic_inputs_fn
  )
  target_value_for_candidate_fn = (
    target_value_for_candidate
    if target_value_for_candidate_fn is None
    else target_value_for_candidate_fn
  )
  metrics_by_tag_for_concept_fn = (
    metrics_by_tag_for_concept
    if metrics_by_tag_for_concept_fn is None
    else metrics_by_tag_for_concept_fn
  )
  best_value_for_target_fn = (
    best_value_for_target
    if best_value_for_target_fn is None
    else best_value_for_target_fn
  )
  return _evaluate_reconcile_candidates_impl(
    diagnostic_type=diagnostic_type,
    subtotal_name=subtotal_name,
    entries=entries,
    tags_by_year=tags_by_year,
    match_tolerance_pct=match_tolerance_pct,
    match_floor_abs_m=match_floor_abs_m,
    candidate_concepts_by_key=candidate_concepts_by_key,
    delta_by_year_fn=delta_by_year_fn,
    current_values_from_diagnostic_inputs_fn=current_values_from_diagnostic_inputs_fn,
    target_value_for_candidate_fn=target_value_for_candidate_fn,
    metrics_by_tag_for_concept_fn=metrics_by_tag_for_concept_fn,
    best_value_for_target_fn=best_value_for_target_fn,
  )


__all__ = [
  "RECONCILE_DIAGNOSTICS_CHECKED",
  "RECONCILE_SEVERITY_ORDER",
  "RECONCILE_UNSUPPORTED_SUBTOTALS",
  "RECONCILE_CANDIDATE_CONCEPTS",
  "after_summary",
  "best_value_for_target",
  "before_summary",
  "current_values_from_diagnostic_inputs",
  "delta_by_year",
  "diagnostic_group_cleared",
  "entries_from_serialized_report",
  "evaluate_reconcile_candidates",
  "existing_override_preserved",
  "filing_pointer",
  "flagged_reconcile_entries",
  "group_payload",
  "group_reconcile_entries",
  "list_metrics",
  "merged_ticker_overrides",
  "metric_value_for_concept",
  "metrics_by_tag_for_concept",
  "new_override_field",
  "normalized_reconcile_path",
  "normalize_metric_entries",
  "override_conflict",
  "override_conflict_response",
  "reconcile_error",
  "reconcile_historical_years",
  "relative_distance",
  "tag_local_part",
  "target_value_for_candidate",
  "validate_inputs_field",
  "validate_reconcile_build_kwargs",
  "validate_reconcile_diagnostic_report",
  "validate_reconcile_file_path_match",
  "validate_reconcile_mode",
  "worst_severity",
]
