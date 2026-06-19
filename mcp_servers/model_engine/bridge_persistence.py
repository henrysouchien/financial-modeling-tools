from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Callable
import uuid

from schema.scenario_bridge import BridgeWarning, factor_values_by_period

from .bridge import empty_bridge_projection_persistence


def bridge_factor_values_by_period(
  factor_payload: dict,
  case: str,
  projection_periods: list[int],
) -> dict[str, float] | None:
  values = factor_values_by_period(
    factor_payload,
    case,
    projection_periods,
    allow_scalar=False,
  )
  if values is None:
    return None
  if any(int(period) not in values for period in projection_periods):
    return None
  return {str(int(period)): float(value) for period, value in values.items()}


def bridge_source_id_slug(value: str) -> str:
  slug = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip()).strip("_").lower()
  return slug or "factor"


def bridge_source_text(factor_payload: dict, case: str, factor_name: str) -> str:
  for key in (
    f"{case}_source",
    f"{case}_source_text",
    f"{case}_evidence",
    f"{case}_basis",
  ):
    value = factor_payload.get(key)
    if isinstance(value, str) and value.strip():
      return value.strip()
  return f"{case.title()}-case flex per critical-factor '{factor_name}'."


def bridge_source_provider(
  source_payload: dict,
  source_type: str,
  source_id: str,
  text: str,
) -> str | None:
  provider = source_payload.get("provider")
  if isinstance(provider, str) and provider.strip():
    return provider.strip()
  haystack = f"{source_payload.get('type', '')} {source_id} {text}".lower()
  if "fmp" in haystack or "market_data" in haystack:
    return "fmp"
  if source_type == "other":
    return "earnings-scenarios"
  return None


def bridge_normalize_sources(
  factor_payload: dict,
  case: str,
  factor_name: str,
) -> tuple[list[dict], dict[str, str]]:
  raw_sources = factor_payload.get("sources")
  if isinstance(raw_sources, dict):
    source_payloads = [value for value in raw_sources.values() if isinstance(value, dict)]
  elif isinstance(raw_sources, list):
    source_payloads = [value for value in raw_sources if isinstance(value, dict)]
  else:
    source_payloads = []

  if not source_payloads:
    text = bridge_source_text(factor_payload, case, factor_name)
    factor_slug = bridge_source_id_slug(str(factor_payload.get("critical_factor_id") or factor_name))
    source_payloads = [
      {
        "id": "src_1",
        "type": "other",
        "source_id": f"earnings-scenarios:{case}_flex:{factor_slug}",
        "text": text,
        "provider": "earnings-scenarios",
      }
    ]

  sources: list[dict] = []
  id_map: dict[str, str] = {}
  used_ids: set[str] = set()
  next_id = 1
  for raw in source_payloads:
    raw_id = str(raw.get("id") or "").strip()
    source_id_for_refs = raw_id if raw_id else None
    if re.fullmatch(r"src_[1-9]\d*", raw_id) and raw_id not in used_ids:
      source_ref_id = raw_id
    else:
      while f"src_{next_id}" in used_ids:
        next_id += 1
      source_ref_id = f"src_{next_id}"
    used_ids.add(source_ref_id)
    next_id += 1

    raw_type = str(raw.get("type") or "other").strip()
    source_type = raw_type if raw_type in {"filing", "transcript", "investor_deck", "other"} else "other"
    source_id = str(raw.get("source_id") or "").strip()
    if not source_id:
      factor_slug = bridge_source_id_slug(str(factor_payload.get("critical_factor_id") or factor_name))
      source_id = f"earnings-scenarios:{case}_flex:{factor_slug}:{source_ref_id}"
    text = str(raw.get("text") or bridge_source_text(factor_payload, case, factor_name)).strip()
    normalized = {
      "id": source_ref_id,
      "type": source_type,
      "source_id": source_id,
      "text": text,
    }
    provider = bridge_source_provider(raw, source_type, source_id, text)
    if provider:
      normalized["provider"] = provider
    for optional_key in (
      "section_header",
      "char_start",
      "char_end",
      "annotation_id",
      "endpoint_or_filing_id",
      "key_fields",
      "retrieved_at",
      "identity_hash",
    ):
      if raw.get(optional_key) is not None:
        normalized[optional_key] = raw[optional_key]
    sources.append(normalized)
    if source_id_for_refs:
      id_map[source_id_for_refs] = source_ref_id

  return sources, id_map


def bridge_source_refs_by_period(
  factor_payload: dict,
  case: str,
  values: dict[str, float],
  sources: list[dict],
  id_map: dict[str, str],
) -> dict[str, list[str]]:
  source_ids = [str(source["id"]) for source in sources]
  source_id_set = set(source_ids)
  raw_refs_by_period = None
  for key in (f"{case}_source_refs_by_period", "source_refs_by_period"):
    value = factor_payload.get(key)
    if isinstance(value, dict):
      raw_refs_by_period = value
      break

  refs_by_period: dict[str, list[str]] = {}
  if raw_refs_by_period:
    for period_key in values:
      raw_refs = raw_refs_by_period.get(period_key) or raw_refs_by_period.get(int(period_key))
      if not isinstance(raw_refs, list):
        refs_by_period[period_key] = list(source_ids)
        continue
      normalized_refs = [
        id_map.get(str(ref), str(ref))
        for ref in raw_refs
        if id_map.get(str(ref), str(ref)) in source_id_set
      ]
      refs_by_period[period_key] = normalized_refs or list(source_ids)
    return refs_by_period

  return {period_key: list(source_ids) for period_key in values}


def bridge_confidence(
  factor_payload: dict,
  scenario_payload: dict,
  case: str,
) -> str:
  candidates = [
    factor_payload.get(f"{case}_confidence"),
    factor_payload.get("confidence"),
  ]
  case_payload = scenario_payload.get(case)
  if isinstance(case_payload, dict):
    candidates.append(case_payload.get("confidence"))
  candidates.append(scenario_payload.get("confidence"))
  for candidate in candidates:
    normalized = str(candidate or "").strip().lower()
    if normalized in {"low", "medium", "high"}:
      return normalized
  return "medium"


def bridge_scenario_entry(
  *,
  factor_payload: dict,
  scenario_payload: dict,
  case: str,
  values: dict[str, float],
  skill_run_id: str,
  written_at: str,
  model_build_id: str | None,
) -> dict:
  factor_name = str(factor_payload.get("factor") or "").strip()
  critical_factor_id = str(
    factor_payload.get("critical_factor_id")
    or factor_payload.get("factor_id")
    or factor_payload.get("assumption_id")
    or factor_name
  )
  sources, id_map = bridge_normalize_sources(factor_payload, case, factor_name)
  return {
    "values": values,
    "rationale": f"{case.title()}-case flex per critical-factor '{critical_factor_id}'.",
    "confidence": bridge_confidence(factor_payload, scenario_payload, case),
    "held_at_base": False,
    "source_refs_by_period": bridge_source_refs_by_period(
      factor_payload,
      case,
      values,
      sources,
      id_map,
    ),
    "sources": sources,
    "provenance": {
      "source_skill": "earnings-scenarios",
      "skill_run_id": skill_run_id,
      "written_at": written_at,
      "model_build_id": model_build_id,
    },
  }


def bridge_projection_persistence(
  *,
  ticker: str,
  assumptions: list[dict],
  scenario_payload: dict,
  projection_periods: list[int],
  resolutions: list,
  warnings: list[BridgeWarning],
  skill_run_id: str | None,
  model_build_id: str | None,
  model_override_fn: Callable[..., dict],
) -> dict:
  persistence = empty_bridge_projection_persistence()
  failure_details: list[dict] = []
  blocked_factors = {
    warning.factor
    for warning in warnings
    if warning.kind in {"unit_shape_mismatch", "scenario_ordering_violation", "inert_scenario_anchor"}
    and warning.factor is not None
  }
  run_id = str(skill_run_id or "").strip() or f"run_{uuid.uuid4().hex}"
  written_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
  ticker_upper = str(ticker or "").strip().upper()

  for factor_payload, resolution in zip(assumptions, resolutions):
    factor_name = str(factor_payload.get("factor") or "").strip()
    if not getattr(resolution, "anchor_id", None):
      continue
    if factor_name in blocked_factors:
      continue
    bull_values = bridge_factor_values_by_period(factor_payload, "bull", projection_periods)
    bear_values = bridge_factor_values_by_period(factor_payload, "bear", projection_periods)
    if not bull_values or not bear_values:
      continue

    persistence["attempts"] += 1
    if not ticker_upper:
      persistence["failures"] += 1
      failure_details.append(
        {
          "factor": factor_name,
          "rate_key": resolution.anchor_id,
          "error": "ticker unavailable for projection persistence",
        }
      )
      continue

    entry = {
      "scenarios": {
        "bull": bridge_scenario_entry(
          factor_payload=factor_payload,
          scenario_payload=scenario_payload,
          case="bull",
          values=bull_values,
          skill_run_id=run_id,
          written_at=written_at,
          model_build_id=model_build_id,
        ),
        "bear": bridge_scenario_entry(
          factor_payload=factor_payload,
          scenario_payload=scenario_payload,
          case="bear",
          values=bear_values,
          skill_run_id=run_id,
          written_at=written_at,
          model_build_id=model_build_id,
        ),
      }
    }
    result = model_override_fn(
      ticker=ticker_upper,
      action="set_projection",
      rate_key=resolution.anchor_id,
      entry=entry,
      expected_scenarios=["bull", "bear"],
    )
    if result.get("status") != "success":
      persistence["failures"] += 1
      failure_details.append(
        {
          "factor": factor_name,
          "rate_key": resolution.anchor_id,
          "error": result.get("error") or result,
        }
      )
      continue
    persistence["successes"] += 1
    persistence["schema_version_bumped"] = (
      persistence["schema_version_bumped"] or bool(result.get("schema_version_bumped"))
    )
    persistence["cross_skill_notes"].extend(result.get("cross_skill_notes") or [])

  if failure_details:
    persistence["failure_details"] = failure_details
  return persistence


__all__ = [
  "bridge_confidence",
  "bridge_factor_values_by_period",
  "bridge_normalize_sources",
  "bridge_projection_persistence",
  "bridge_scenario_entry",
  "bridge_source_id_slug",
  "bridge_source_provider",
  "bridge_source_refs_by_period",
  "bridge_source_text",
]
