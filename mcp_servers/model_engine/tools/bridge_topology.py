from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from mcp_servers.model_engine.scenario_readiness import (
  READINESS_OWNER_MIN_GAP,
  READINESS_OWNER_MIN_SCORE,
  READINESS_OWNER_STRONG_SCORE,
  readiness_owner_tokens,
)


def _jsonable(value: Any) -> dict[str, Any]:
  if hasattr(value, "model_dump"):
    return value.model_dump(mode="json")
  if is_dataclass(value):
    return asdict(value)
  if isinstance(value, dict):
    return value
  return {"value": value}


def _parse_optional_factor_list(
  deps: Any,
  factors: list[str] | str | None,
) -> list[str]:
  if factors is None:
    return []
  if isinstance(factors, str):
    raw = factors.strip()
    if not raw:
      return []
    if raw.startswith("["):
      values = deps.coerce_json_list_arg(raw, name="factors")
    else:
      values = [raw]
  else:
    values = deps.coerce_json_list_arg(factors, name="factors")
  return [str(value).strip() for value in values if str(value).strip()]


def _topology_next_actions() -> list[str]:
  return [
    "Use topology.owners[].owner_id as the factor_anchor_hints value for model_bridge_scenarios after confirming the economic factor.",
    "Use topology.owners[].bull_id/base_id/bear_id with model_values to inspect seeded scenario rows.",
    "For one-off model_scenario sensitivity, override returned bull/base/bear case row IDs for one scenario case at a time; model_scenario maps each case row to its owning economic row internally.",
    "Use model_find_scenario_anchor(file_path=..., factor=..., hint=owner_id) only when you need to bind one thesis factor or disambiguate a candidate owner.",
    "Use model_find only for non-scenario workbook rows; scenario owner topology is authoritative here.",
  ]


def _object_value(value: Any, key: str) -> Any:
  if isinstance(value, dict):
    return value.get(key)
  return getattr(value, key, None)


def _readiness_owners(readiness: Any) -> list[Any]:
  owners = getattr(readiness, "owners", None)
  if owners is None and hasattr(readiness, "model_dump"):
    dumped = readiness.model_dump(mode="json")
    owners = dumped.get("owners") if isinstance(dumped, dict) else None
  if owners is None and isinstance(readiness, dict):
    owners = readiness.get("owners")
  return list(owners or [])


def _topology_owner_complete(owner: Any) -> bool:
  return bool(
    _object_value(owner, "owner_id")
    and _object_value(owner, "anchor_id")
    and _object_value(owner, "bull_id")
    and _object_value(owner, "base_id")
    and _object_value(owner, "bear_id")
    and _object_value(owner, "upstream_of_target") is True
    and not (_object_value(owner, "missing_cases") or [])
  )


def _topology_owner_score(factor_tokens: set[str], owner: Any) -> float:
  owner_tokens = (
    readiness_owner_tokens(_object_value(owner, "owner_id"))
    | readiness_owner_tokens(_object_value(owner, "anchor_id"))
  )
  if not factor_tokens or not owner_tokens:
    return 0.0
  return len(factor_tokens & owner_tokens) / len(factor_tokens | owner_tokens)


def _candidate_owner_scores(scored: list[tuple[float, Any]]) -> list[dict[str, Any]]:
  return [
    {
      "owner_id": _object_value(owner, "owner_id"),
      "score": score,
    }
    for score, owner in scored[:5]
  ]


def _unresolved_topology_factor_payload(
  *,
  deps: Any,
  factor: str,
  readiness: Any,
  scored: list[tuple[float, Any]],
  match_reason: str = "unresolved",
  score: float | None = None,
) -> dict[str, Any]:
  if scored:
    candidates = [
      str(_object_value(owner, "owner_id"))
      for _score, owner in scored[:5]
      if _object_value(owner, "owner_id")
    ]
  else:
    candidates = [
      str(_object_value(owner, "owner_id"))
      for owner in _readiness_owners(readiness)[:5]
      if _object_value(owner, "owner_id")
    ]
  payload = {
    "status": "ok",
    "factor": factor,
    "owner_id": None,
    "anchor_id": None,
    "bull_id": None,
    "base_id": None,
    "bear_id": None,
    "match_reason": match_reason,
    "score": score,
    "candidates": candidates,
    "readiness_match": {
      "source": "scenario_bridge_readiness.owners",
      "target_item_id": getattr(readiness, "target_item_id", None)
      or (readiness.get("target_item_id") if isinstance(readiness, dict) else None),
      "readiness_status": getattr(readiness, "status", None)
      or (readiness.get("status") if isinstance(readiness, dict) else None),
      "candidate_owner_scores": _candidate_owner_scores(scored),
    },
  }
  recovery = deps.anchor_recovery(payload)
  if recovery is not None:
    payload["recovery"] = recovery
  return payload


def _resolve_topology_factor(
  *,
  deps: Any,
  model: Any,
  factor: str,
  readiness: Any,
) -> dict[str, Any]:
  factor_tokens = readiness_owner_tokens(factor)
  scored = [
    (_topology_owner_score(factor_tokens, owner), owner)
    for owner in _readiness_owners(readiness)
    if _topology_owner_complete(owner)
  ]
  scored.sort(key=lambda entry: (-entry[0], str(_object_value(entry[1], "owner_id") or "")))
  if not scored:
    return _unresolved_topology_factor_payload(
      deps=deps,
      factor=factor,
      readiness=readiness,
      scored=scored,
    )

  best_score, best_owner = scored[0]
  next_score = scored[1][0] if len(scored) > 1 else 0.0
  if best_score < READINESS_OWNER_MIN_SCORE:
    return _unresolved_topology_factor_payload(
      deps=deps,
      factor=factor,
      readiness=readiness,
      scored=scored,
      score=best_score,
    )
  if best_score < READINESS_OWNER_STRONG_SCORE and (
    best_score - next_score
  ) < READINESS_OWNER_MIN_GAP:
    return _unresolved_topology_factor_payload(
      deps=deps,
      factor=factor,
      readiness=readiness,
      scored=scored,
      match_reason="label_match_low_confidence",
      score=best_score,
    )

  resolution = deps.find_scenario_anchor(
    model,
    factor,
    hint=str(_object_value(best_owner, "owner_id")),
  )
  if getattr(resolution, "match_reason", None) != "explicit_hint":
    return _unresolved_topology_factor_payload(
      deps=deps,
      factor=factor,
      readiness=readiness,
      scored=scored,
      match_reason=getattr(resolution, "match_reason", None) or "unresolved",
      score=best_score,
    )
  payload = {"status": "ok", **asdict(resolution)}
  payload["match_reason"] = "readiness_owner"
  payload["score"] = best_score
  payload["readiness_match"] = {
    "source": "scenario_bridge_readiness.owners",
    "readiness_status": getattr(readiness, "status", None)
    or (readiness.get("status") if isinstance(readiness, dict) else None),
    "owner_id": _object_value(best_owner, "owner_id"),
    "owner_label": _object_value(best_owner, "label"),
    "anchor_id": _object_value(best_owner, "anchor_id"),
    "bull_id": _object_value(best_owner, "bull_id"),
    "base_id": _object_value(best_owner, "base_id"),
    "bear_id": _object_value(best_owner, "bear_id"),
    "target_item_id": _object_value(best_owner, "target_item_id"),
    "upstream_of_target": _object_value(best_owner, "upstream_of_target"),
    "score": best_score,
    "next_best_score": next_score,
    "candidate_owner_scores": _candidate_owner_scores(scored),
  }
  recovery = deps.anchor_recovery(payload)
  if recovery is not None:
    payload["recovery"] = recovery
  return payload
