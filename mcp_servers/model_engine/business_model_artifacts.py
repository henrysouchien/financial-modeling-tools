from __future__ import annotations

import json
import os
from pathlib import Path

from schema.business_model import BusinessModel, normalize_legacy_profitability_targets
from schema.business_model_markdown import parse as parse_business_model_markdown

from .args import valid_workspace_user_id


def business_model_workspace_candidates(path_value: str, user_id: str | None = None) -> list[Path]:
  path = Path(path_value).expanduser()
  candidates: list[Path] = [path]
  if path.is_absolute():
    return candidates

  user_ids: list[str] = []
  resolved_user_id = valid_workspace_user_id(user_id)
  if resolved_user_id and resolved_user_id not in user_ids:
    user_ids.append(resolved_user_id)
  try:
    from memory import get_current_user_id

    current_user_id = valid_workspace_user_id(get_current_user_id())
    if current_user_id and current_user_id not in user_ids:
      user_ids.append(current_user_id)
  except Exception:
    pass

  configured_data_dir = os.getenv("USER_DATA_DIR", "").strip()
  if configured_data_dir:
    data_dir = Path(configured_data_dir).expanduser()
    for uid in user_ids:
      candidates.append(data_dir / "users" / uid / "workspace" / path)

  try:
    from memory import get_workspace_dir

    for uid in user_ids:
      candidates.append(get_workspace_dir(uid) / path)
  except Exception:
    pass

  deduped: list[Path] = []
  seen: set[str] = set()
  for candidate in candidates:
    key = str(candidate)
    if key in seen:
      continue
    seen.add(key)
    deduped.append(candidate)
  return deduped


def resolve_business_model_path(path_value: str, user_id: str | None = None) -> tuple[Path, list[str]]:
  candidates = business_model_workspace_candidates(path_value, user_id=user_id)
  searched = [str(candidate) for candidate in candidates]
  for candidate in candidates:
    if candidate.is_file():
      return candidate, searched
  return candidates[0], searched


def load_business_model_from_path(
  path_value: str,
  user_id: str | None = None,
) -> tuple[BusinessModel, str, Path, list[str]]:
  path, searched_paths = resolve_business_model_path(path_value, user_id=user_id)
  if not path.is_file():
    raise FileNotFoundError(
      f"business model artifact not found: {path_value!r}; searched: {searched_paths}"
    )
  raw = path.read_text(encoding="utf-8")
  if path.suffix.lower() in {".md", ".markdown"} or raw.lstrip().startswith("---"):
    return parse_business_model_markdown(raw), "markdown", path, searched_paths
  payload = normalize_legacy_profitability_targets(json.loads(raw))
  return BusinessModel.model_validate(payload), "json", path, searched_paths


__all__ = [
  "business_model_workspace_candidates",
  "load_business_model_from_path",
  "resolve_business_model_path",
]
