from __future__ import annotations

from typing import Any

from schema.overrides import derive_overrides_file_meta, overrides_schema_version_rank


PROJECTION_SCENARIOS = {"base", "bull", "bear"}
PROJECTION_SCENARIO_DISPLAY_METADATA_KEYS = {
  "scenario",
  "unit",
  "units",
  "expected_scenarios",
  "rate_key",
}


def scenario_source_skill(scenario: Any) -> str | None:
  if hasattr(scenario, "provenance"):
    provenance = getattr(scenario, "provenance", None)
    return getattr(provenance, "source_skill", None)
  if isinstance(scenario, dict):
    provenance = scenario.get("provenance")
    if isinstance(provenance, dict):
      source_skill = provenance.get("source_skill")
      return str(source_skill) if source_skill is not None else None
  return None


def scenario_written_at(scenario: Any) -> str | None:
  if hasattr(scenario, "provenance"):
    provenance = getattr(scenario, "provenance", None)
    written_at = getattr(provenance, "written_at", None)
    return str(written_at) if written_at is not None else None
  if isinstance(scenario, dict):
    provenance = scenario.get("provenance")
    if isinstance(provenance, dict):
      written_at = provenance.get("written_at")
      return str(written_at) if written_at is not None else None
  return None


def projection_entry_to_dict(projection_entry: Any) -> dict:
  if projection_entry is None:
    return {}
  if hasattr(projection_entry, "model_dump"):
    return projection_entry.model_dump(mode="json")
  if isinstance(projection_entry, dict):
    return dict(projection_entry)
  return {}


def normalized_projection_scenario_entry(entry: dict[str, Any]) -> dict[str, Any]:
  normalized = dict(entry)
  if "values" not in normalized and "values_by_period" in normalized:
    normalized["values"] = normalized.pop("values_by_period")
  if "source_refs_by_period" not in normalized and "source_ids_by_period" in normalized:
    normalized["source_refs_by_period"] = normalized.pop("source_ids_by_period")
  for key in PROJECTION_SCENARIO_DISPLAY_METADATA_KEYS:
    normalized.pop(key, None)
  if "source_skill" in normalized and isinstance(normalized.get("provenance"), dict):
    normalized.pop("source_skill", None)
  return normalized


def normalized_projection_scenario_value(value: Any) -> Any:
  if isinstance(value, dict):
    return normalized_projection_scenario_entry(value)
  return value


def normalize_projection_entry_payload(
  entry_payload: dict[str, Any],
  *,
  default_scenario: str | None = None,
) -> dict[str, Any]:
  scenario_name = str(default_scenario or "base").strip() or "base"
  if scenario_name not in PROJECTION_SCENARIOS:
    raise ValueError("scenario must be one of base, bull, bear")

  if isinstance(entry_payload.get("scenarios"), dict):
    return {
      "scenarios": {
        str(name): normalized_projection_scenario_value(value)
        for name, value in entry_payload["scenarios"].items()
      }
    }

  if entry_payload and set(entry_payload).issubset(PROJECTION_SCENARIOS):
    return {
      "scenarios": {
        str(name): normalized_projection_scenario_value(value)
        for name, value in entry_payload.items()
      }
    }

  if "values" in entry_payload or "values_by_period" in entry_payload:
    return {
      "scenarios": {
        scenario_name: normalized_projection_scenario_entry(entry_payload),
      }
    }

  return entry_payload


def projection_scenarios(projection_entry: Any) -> dict:
  projection_dict = projection_entry_to_dict(projection_entry)
  scenarios = projection_dict.get("scenarios", {})
  return dict(scenarios) if isinstance(scenarios, dict) else {}


def projection_scenario_dump(scenario_entry: Any) -> dict:
  if hasattr(scenario_entry, "model_dump"):
    return scenario_entry.model_dump(mode="json")
  if isinstance(scenario_entry, dict):
    return dict(scenario_entry)
  return {}


def get_projection_data(
  projections: dict | None,
  *,
  rate_key: str | None = None,
) -> Any:
  projection_map = projections or {}
  if rate_key is None:
    return dict(projection_map)
  return projection_map.get(rate_key)


def delete_projection_entry(
  projections: dict | None,
  *,
  rate_key: str,
) -> dict[str, Any]:
  projection_map = projections or {}
  if rate_key not in projection_map:
    return {
      "updated_projections": dict(projection_map),
      "deleted": False,
    }

  updated_projections = dict(projection_map)
  del updated_projections[rate_key]
  return {
    "updated_projections": updated_projections,
    "deleted": True,
  }


def merge_projection_scenarios(
  *,
  existing_projection: Any,
  new_scenarios: dict[str, Any],
  file_meta: dict[str, Any],
) -> dict[str, Any]:
  existing_scenarios = projection_scenarios(existing_projection)
  merged_scenarios = dict(existing_scenarios)
  cross_skill_notes: list[str] = []

  for scenario_name, scenario_entry in new_scenarios.items():
    existing_scenario = existing_scenarios.get(scenario_name)
    if existing_scenario:
      existing_skill = scenario_source_skill(existing_scenario)
      new_skill = scenario_source_skill(scenario_entry)
      if existing_skill and existing_skill != new_skill:
        cross_skill_notes.append(
          "scenario "
          f"{scenario_name!r} previously written by {existing_skill!r}, "
          f"now overwritten by {new_skill!r}"
        )
    merged_scenarios[scenario_name] = projection_scenario_dump(scenario_entry)

  old_schema_version = str(file_meta.get("schema_version", "1") or "1")
  updated_file_meta = derive_overrides_file_meta(
    file_meta,
    projections={"_projection_merge": {"scenarios": merged_scenarios}},
  )
  schema_version_bumped = overrides_schema_version_rank(
    updated_file_meta.get("schema_version")
  ) > overrides_schema_version_rank(old_schema_version)

  return {
    "projection": {"scenarios": merged_scenarios},
    "file_meta": updated_file_meta,
    "scenarios_written": sorted(new_scenarios.keys()),
    "schema_version_bumped": schema_version_bumped,
    "cross_skill_notes": cross_skill_notes,
  }


def projection_summaries(projections: dict | None) -> list[dict[str, Any]]:
  summaries: list[dict[str, Any]] = []
  for projection_rate_key, projection_entry in sorted((projections or {}).items()):
    scenarios = projection_scenarios(projection_entry)
    last_written_at = None
    last_written_by = None
    for scenario_entry in scenarios.values():
      written_at = scenario_written_at(scenario_entry)
      if written_at is not None and (
        last_written_at is None or written_at > last_written_at
      ):
        last_written_at = written_at
        last_written_by = scenario_source_skill(scenario_entry)
    summaries.append(
      {
        "rate_key": projection_rate_key,
        "scenarios": list(scenarios.keys()),
        "last_written_by": last_written_by,
        "last_written_at": last_written_at,
      }
    )
  return summaries


def prune_projection_scenarios(
  projections: dict[str, dict],
  *,
  source_skill: str,
  keep_rate_keys: set[str],
  stale_rate_keys: set[str] | None = None,
  scenario: str | None = None,
) -> dict[str, Any]:
  updated_projections: dict[str, dict] = {}
  deleted_scenarios: list[dict[str, str]] = []
  deleted_rate_keys: list[str] = []
  changed = False

  for projection_rate_key, projection_entry in sorted(projections.items()):
    scenarios = projection_scenarios(projection_entry)
    if stale_rate_keys is not None and projection_rate_key not in stale_rate_keys:
      updated_projections[projection_rate_key] = {"scenarios": scenarios}
      continue
    if stale_rate_keys is None and projection_rate_key in keep_rate_keys:
      updated_projections[projection_rate_key] = {"scenarios": scenarios}
      continue

    remaining_scenarios = dict(scenarios)
    removed_for_rate_key = False
    for scenario_name, scenario_entry in list(scenarios.items()):
      if scenario is not None and scenario_name != scenario:
        continue
      if scenario_source_skill(scenario_entry) != source_skill:
        continue
      del remaining_scenarios[scenario_name]
      deleted_scenarios.append(
        {
          "rate_key": projection_rate_key,
          "scenario": scenario_name,
        }
      )
      changed = True
      removed_for_rate_key = True

    if remaining_scenarios or not removed_for_rate_key:
      updated_projections[projection_rate_key] = {"scenarios": remaining_scenarios}
    else:
      deleted_rate_keys.append(projection_rate_key)
      changed = True

  return {
    "updated_projections": updated_projections,
    "changed": changed,
    "deleted_scenarios": deleted_scenarios,
    "deleted_rate_keys": deleted_rate_keys,
  }


__all__ = [
  "PROJECTION_SCENARIO_DISPLAY_METADATA_KEYS",
  "PROJECTION_SCENARIOS",
  "delete_projection_entry",
  "get_projection_data",
  "merge_projection_scenarios",
  "normalize_projection_entry_payload",
  "normalized_projection_scenario_entry",
  "normalized_projection_scenario_value",
  "prune_projection_scenarios",
  "projection_entry_to_dict",
  "projection_scenario_dump",
  "projection_scenarios",
  "projection_summaries",
  "scenario_source_skill",
  "scenario_written_at",
]
