from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


def current_registry_revision(
  *,
  get_registry_cache_fn: Callable[[], Any],
  reconcile_error_fn: Callable[..., dict],
) -> str | dict:
  try:
    cache = get_registry_cache_fn()
    cache.ensure_loaded()
    revision = cache.registry_revision
  except Exception as exc:
    return reconcile_error_fn(
      "registry_cache_unavailable",
      str(exc),
      recoverable=True,
    )
  if not revision:
    return reconcile_error_fn(
      "registry_cache_unavailable",
      "registry_revision was unavailable after cache initialization",
      recoverable=True,
    )
  return str(revision)


def override_directory(*, schema_overrides_module: Any) -> Path:
  return schema_overrides_module.resolve_overrides_dir()


def merge_override_fields(
  overrides: dict | None,
  *,
  concept_key: str,
  override_fields: dict,
) -> dict[str, Any]:
  merged_entry = dict((overrides or {}).get(concept_key, {}))
  merged_entry.update(dict(override_fields))
  return merged_entry


def custom_concept_entry(custom_concept: dict) -> dict[str, Any]:
  return dict(custom_concept)


def remove_override_concept(
  overrides: dict | None,
  custom_concepts: dict | None,
  *,
  concept_key: str,
) -> dict[str, Any]:
  updated_overrides = dict(overrides or {})
  updated_custom_concepts = dict(custom_concepts or {})
  removed = False

  if concept_key in updated_overrides:
    del updated_overrides[concept_key]
    removed = True
  if concept_key in updated_custom_concepts:
    del updated_custom_concepts[concept_key]
    removed = True

  return {
    "overrides": updated_overrides,
    "custom_concepts": updated_custom_concepts,
    "removed": removed,
  }


def resolve_reconcile_override_path(
  ticker_upper: str,
  *,
  schema_overrides_module: Any,
  override_directory_fn: Callable[[], Path],
) -> Path:
  directory = override_directory_fn()
  resolved = schema_overrides_module._resolve_override_path(ticker_upper, directory)
  return resolved if resolved is not None else directory / f"{ticker_upper}.json"


@contextmanager
def ticker_override_lock(
  ticker_upper: str,
  *,
  override_directory_fn: Callable[[], Path],
  fcntl_module: Any,
) -> Iterator[None]:
  directory = override_directory_fn()
  directory.mkdir(parents=True, exist_ok=True)
  lock_path = directory / f"{ticker_upper}.json.lock"
  with lock_path.open("a+", encoding="utf-8") as lock_handle:
    fcntl_module.flock(lock_handle.fileno(), fcntl_module.LOCK_EX)
    try:
      yield
    finally:
      fcntl_module.flock(lock_handle.fileno(), fcntl_module.LOCK_UN)


__all__ = [
  "current_registry_revision",
  "custom_concept_entry",
  "merge_override_fields",
  "override_directory",
  "remove_override_concept",
  "resolve_reconcile_override_path",
  "ticker_override_lock",
]
