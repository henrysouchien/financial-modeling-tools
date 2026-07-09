from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from schema import serialization
from schema.handle import ModelHandle
from schema.handle_token import (
  ModelHandleTokenError,
  canonical_workbook_path,
  issue_model_handle_token,
)


LoadHandle = Callable[..., ModelHandle]


def model_handle_token_payload(
  *,
  file_path: str,
  historical_cutoff_year: int | None,
  issued_by: str,
  load_handle: LoadHandle,
) -> dict[str, Any]:
  cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
  workbook_path = canonical_workbook_path(file_path)
  handle = load_handle(workbook_path, historical_cutoff_year=cutoff)
  try:
    token = issue_model_handle_token(
      workbook_path,
      handle,
      historical_cutoff_year=cutoff,
      issued_by=issued_by,
    )
  except ModelHandleTokenError as exc:
    if exc.error_code != "non_durable_model_handle_token":
      raise
    if Path(serialization.sidecar_path(workbook_path)).exists():
      raise
    handle = load_handle(
      workbook_path,
      model=handle.model,
      historical_cutoff_year=cutoff,
      persist=True,
    )
    token = issue_model_handle_token(
      workbook_path,
      handle,
      historical_cutoff_year=cutoff,
      issued_by=issued_by,
    )
  return token.model_dump(mode="json")


__all__ = [
  "model_handle_token_payload",
]
