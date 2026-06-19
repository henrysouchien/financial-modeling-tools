from __future__ import annotations

import os
from pathlib import Path

from schema.business_model import BusinessModel
from schema.business_model_markdown import parse as parse_business_model_markdown

from .args import valid_workspace_user_id


def business_model_workspace_candidates(path_value: str) -> list[Path]:
  path = Path(path_value).expanduser()
  candidates: list[Path] = [path]
  if path.is_absolute():
    return candidates

  user_ids: list[str] = []
  for env_var in ("AUTONOMOUS_USER_ID", "MEMORY_STARTUP_USER_ID"):
    user_id = valid_workspace_user_id(os.getenv(env_var))
    if user_id and user_id not in user_ids:
      user_ids.append(user_id)

  configured_data_dir = os.getenv("USER_DATA_DIR", "").strip()
  if configured_data_dir:
    data_dir = Path(configured_data_dir).expanduser()
    for user_id in user_ids:
      candidates.append(data_dir / "users" / user_id / "workspace" / path)

  try:
    from memory import get_current_user_id, get_workspace_dir

    current_user_id = valid_workspace_user_id(get_current_user_id())
    if current_user_id and current_user_id not in user_ids:
      user_ids.append(current_user_id)
    for user_id in user_ids:
      candidates.append(get_workspace_dir(user_id) / path)
    if not user_ids:
      candidates.append(get_workspace_dir() / path)
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


def resolve_business_model_path(path_value: str) -> tuple[Path, list[str]]:
  candidates = business_model_workspace_candidates(path_value)
  searched = [str(candidate) for candidate in candidates]
  for candidate in candidates:
    if candidate.is_file():
      return candidate, searched
  return candidates[0], searched


def load_business_model_from_path(path_value: str) -> tuple[BusinessModel, str, Path, list[str]]:
  path, searched_paths = resolve_business_model_path(path_value)
  if not path.is_file():
    raise FileNotFoundError(
      f"business model artifact not found: {path_value!r}; searched: {searched_paths}"
    )
  raw = path.read_text(encoding="utf-8")
  if path.suffix.lower() in {".md", ".markdown"} or raw.lstrip().startswith("---"):
    return parse_business_model_markdown(raw), "markdown", path, searched_paths
  return BusinessModel.model_validate_json(raw), "json", path, searched_paths


__all__ = [
  "business_model_workspace_candidates",
  "load_business_model_from_path",
  "resolve_business_model_path",
]
