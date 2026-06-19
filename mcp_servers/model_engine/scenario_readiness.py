from __future__ import annotations

from dataclasses import asdict
import re
from typing import Any, Callable

from schema.model_readiness import compute_model_scenario_bridge_readiness
from schema.scenario_bridge import BridgeWarning, find_scenario_anchor


READINESS_OWNER_TOKEN_NOISE = {
  "tpl",
  "a",
  "s",
  "fm",
  "scenario",
  "scenarios",
  "table",
  "tables",
  "label",
  "driver",
  "drivers",
  "unit",
  "economics",
  "pct",
  "percent",
  "yoy",
  "chg",
  "change",
}
READINESS_OWNER_MIN_SCORE = 0.30
READINESS_OWNER_MIN_GAP = 0.10
READINESS_OWNER_STRONG_SCORE = 0.70


def readiness_owner_tokens(value: str | None) -> set[str]:
  raw_tokens = re.split(r"[^a-z]+|\d+", str(value or "").lower().replace("%", " percent "))
  return {
    token
    for token in raw_tokens
    if token and token not in READINESS_OWNER_TOKEN_NOISE
  }


def readiness_owner_score(factor_tokens: set[str], owner: Any) -> float:
  owner_tokens = (
    readiness_owner_tokens(getattr(owner, "owner_id", None))
    | readiness_owner_tokens(getattr(owner, "anchor_id", None))
  )
  if not factor_tokens or not owner_tokens:
    return 0.0
  return len(factor_tokens & owner_tokens) / len(factor_tokens | owner_tokens)


def readiness_owner_complete(owner: Any) -> bool:
  return bool(
    getattr(owner, "owner_id", None)
    and getattr(owner, "anchor_id", None)
    and getattr(owner, "bull_id", None)
    and getattr(owner, "base_id", None)
    and getattr(owner, "bear_id", None)
    and getattr(owner, "upstream_of_target", None) is True
    and not (getattr(owner, "missing_cases", None) or [])
  )


def bridge_inert_anchor_warnings(
  model: Any,
  resolutions: list,
  *,
  compute_readiness_fn: Callable[..., Any] = compute_model_scenario_bridge_readiness,
) -> list[BridgeWarning]:
  owner_ids = [
    str(getattr(resolution, "owner_id", ""))
    for resolution in resolutions
    if getattr(resolution, "owner_id", None)
  ]
  if not owner_ids:
    return []
  factor_by_owner = {
    str(getattr(resolution, "owner_id")): getattr(resolution, "factor", None)
    for resolution in resolutions
    if getattr(resolution, "owner_id", None)
  }
  readiness = compute_readiness_fn(model, owner_ids=owner_ids)
  warnings: list[BridgeWarning] = []
  for issue in readiness.issues:
    if issue.code != "inert_scenario_anchor" or not issue.owner_id:
      continue
    warnings.append(
      BridgeWarning(
        kind="inert_scenario_anchor",
        factor=factor_by_owner.get(issue.owner_id),
        field=issue.owner_id,
        detail=issue.detail,
      )
    )
  return warnings


def readiness_owner_anchor_payload(
  model: Any,
  factor: str,
  *,
  compute_readiness_fn: Callable[..., Any] = compute_model_scenario_bridge_readiness,
  find_anchor_fn: Callable[..., Any] = find_scenario_anchor,
) -> dict | None:
  readiness = compute_readiness_fn(model)
  factor_tokens = readiness_owner_tokens(factor)
  scored = [
    (readiness_owner_score(factor_tokens, owner), owner)
    for owner in readiness.owners
    if readiness_owner_complete(owner)
  ]
  scored.sort(key=lambda entry: (-entry[0], getattr(entry[1], "owner_id", "")))
  if not scored:
    return None

  best_score, best_owner = scored[0]
  next_score = scored[1][0] if len(scored) > 1 else 0.0
  if best_score < READINESS_OWNER_MIN_SCORE:
    return None
  if best_score < READINESS_OWNER_STRONG_SCORE and (best_score - next_score) < READINESS_OWNER_MIN_GAP:
    return None

  resolution = find_anchor_fn(model, factor, hint=getattr(best_owner, "owner_id", None))
  if resolution.match_reason != "explicit_hint":
    return None

  payload = {"status": "ok", **asdict(resolution)}
  payload["match_reason"] = "readiness_owner"
  payload["score"] = best_score
  payload["readiness_match"] = {
    "source": "scenario_bridge_readiness.owners",
    "readiness_status": readiness.status,
    "owner_id": best_owner.owner_id,
    "owner_label": best_owner.label,
    "anchor_id": best_owner.anchor_id,
    "bull_id": best_owner.bull_id,
    "base_id": best_owner.base_id,
    "bear_id": best_owner.bear_id,
    "upstream_of_target": best_owner.upstream_of_target,
    "score": best_score,
    "next_best_score": next_score,
    "candidate_owner_scores": [
      {"owner_id": owner.owner_id, "score": score}
      for score, owner in scored[:5]
    ],
  }
  return payload


def readiness_owner_anchor_hints(
  model: Any,
  assumptions: list[dict],
  hints: dict | None,
  *,
  compute_readiness_fn: Callable[..., Any] = compute_model_scenario_bridge_readiness,
  find_anchor_fn: Callable[..., Any] = find_scenario_anchor,
) -> dict | None:
  merged: dict[str, str] = {
    str(factor): str(owner_id)
    for factor, owner_id in (hints or {}).items()
    if str(factor).strip() and str(owner_id).strip()
  }
  for assumption in assumptions:
    factor = str((assumption or {}).get("factor") or "").strip()
    if not factor or factor in merged:
      continue
    payload = readiness_owner_anchor_payload(
      model,
      factor,
      compute_readiness_fn=compute_readiness_fn,
      find_anchor_fn=find_anchor_fn,
    )
    owner_id = str((payload or {}).get("owner_id") or "").strip()
    if owner_id:
      merged[factor] = owner_id
  return merged or None


__all__ = [
  "READINESS_OWNER_MIN_GAP",
  "READINESS_OWNER_MIN_SCORE",
  "READINESS_OWNER_STRONG_SCORE",
  "READINESS_OWNER_TOKEN_NOISE",
  "bridge_inert_anchor_warnings",
  "readiness_owner_anchor_hints",
  "readiness_owner_anchor_payload",
  "readiness_owner_complete",
  "readiness_owner_score",
  "readiness_owner_tokens",
]
