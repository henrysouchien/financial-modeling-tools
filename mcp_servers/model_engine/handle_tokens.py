from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from schema.handle import ModelHandle
from schema.handle_token import (
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
  handle = load_handle(
    workbook_path,
    historical_cutoff_year=cutoff,
    persist=False,
    refresh_stale_sidecar=False,
    use_cache=False,
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
