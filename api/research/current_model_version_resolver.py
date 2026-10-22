from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from schema.model_ref import CurrentModelPointer, ModelRecord


class CurrentModelResolutionError(RuntimeError):
  #: The operator move that clears this refusal, when there is exactly one.
  #: Callers that re-wrap this error for a product surface must carry it
  #: through: a typed refusal whose repair is dropped on the way out reads to
  #: the operator as corruption (Stage D plan D-DP5).
  fix: str | None

  def __init__(
    self,
    code: str,
    message: str,
    *,
    retryable: bool = False,
    fix: str | None = None,
  ) -> None:
    super().__init__(message)
    self.code = code
    self.retryable = retryable
    self.fix = fix


class CurrentModelNotFound(CurrentModelResolutionError):
  def __init__(self, research_file_id: int) -> None:
    super().__init__(
      "current_model_not_found",
      f"no current ModelVersion is selected for research_file_id={research_file_id}",
    )


class CurrentModelSelectionChanged(CurrentModelResolutionError):
  def __init__(self) -> None:
    super().__init__(
      "current_model_selection_changed",
      "current model selection changed while its ModelVersion was resolved",
      retryable=True,
    )


@dataclass(frozen=True, slots=True)
class ResolvedCurrentModel:
  pointer: CurrentModelPointer
  model_version: ModelRecord


def _require_same_pointer(repository: Any, current_model: ResolvedCurrentModel) -> None:
  version = current_model.model_version
  refreshed = repository.get_current_model_pointer(
    user_id=version.user_id,
    research_file_id=version.research_file_id,
    ticker=version.ticker,
  )
  if refreshed != current_model.pointer:
    raise CurrentModelSelectionChanged()


def resolve_current_model_version(
  repository: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
) -> ResolvedCurrentModel:
  """Resolve one authenticated pointer/version pair without exposing a path."""

  from research.repository import ModelVersionRepositoryError

  try:
    pointer = repository.get_current_model_pointer(
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
    )
    if pointer is None or pointer.selected is None:
      raise CurrentModelNotFound(research_file_id)
    model_version = repository.get_model_record(
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
      model_ref=pointer.selected,
    )
    if model_version is None:
      raise CurrentModelResolutionError(
        "current_model_version_missing",
        "current model pointer does not resolve to an immutable ModelVersion",
      )
    resolved = ResolvedCurrentModel(
      pointer=pointer,
      model_version=model_version,
    )
    if pointer.selected != model_version.ref:
      raise CurrentModelResolutionError(
        "current_model_identity_invalid",
        "current model pointer and immutable ModelVersion identities differ",
      )
    _require_same_pointer(repository, resolved)
    return resolved
  except CurrentModelResolutionError:
    raise
  except ModelVersionRepositoryError as exc:
    raise CurrentModelResolutionError(exc.code, str(exc)) from exc




__all__ = [
  "CurrentModelNotFound",
  "CurrentModelResolutionError",
  "CurrentModelSelectionChanged",
  "ResolvedCurrentModel",
  "resolve_current_model_version",
]
