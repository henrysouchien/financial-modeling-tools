from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError


def coerce_json_arg(value: Any, *, name: str) -> Any:
  """Accept structured MCP args passed either natively or as JSON strings."""
  if not isinstance(value, str):
    return value
  raw = value.strip()
  try:
    return json.loads(raw)
  except json.JSONDecodeError as exc:
    raise ValueError(f"{name} must be valid JSON when passed as a string: {exc.msg}") from exc


def coerce_json_list_arg(value: Any, *, name: str) -> list[Any]:
  decoded = coerce_json_arg(value, name=name)
  if not isinstance(decoded, list):
    raise ValueError(f"{name} must be a list")
  return decoded


def coerce_json_dict_arg(value: Any, *, name: str) -> dict[str, Any]:
  decoded = coerce_json_arg(value, name=name)
  if not isinstance(decoded, dict):
    raise ValueError(f"{name} must be a dict")
  return decoded


def safe_pydantic_errors(exc: ValidationError) -> list[dict[str, Any]]:
  try:
    decoded = json.loads(exc.json())
    return decoded if isinstance(decoded, list) else []
  except Exception:
    return [{"msg": str(exc)}]


def valid_workspace_user_id(value: object | None) -> str | None:
  raw = str(value or "").strip()
  if not raw:
    return None
  candidate = Path(raw)
  if candidate.is_absolute() or any(part in {"", ".", ".."} for part in candidate.parts):
    return None
  return raw


__all__ = [
  "coerce_json_arg",
  "coerce_json_dict_arg",
  "coerce_json_list_arg",
  "safe_pydantic_errors",
  "valid_workspace_user_id",
]
