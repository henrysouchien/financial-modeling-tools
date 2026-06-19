from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, List, Optional, get_type_hints


_MODEL_SEMANTICS_SECTIONS = frozenset({"forecast", "scenarios", "valuation"})


@dataclass(frozen=True)
class ModelSemanticsDeps:
  load_ticker_overrides: Callable[[str], Any | None]
  ticker_overrides_cls: Any
  model_semantics_cls: Any
  derive_ticker_overrides_schema_version: Callable[[Any], str]
  get_repository_factory: Callable[[], Any]
  resolve_current_model_ref: Callable[[Any, int], Any | None]
  current_model_ref_cls: Any
  valid_workspace_user_id: Callable[[object | None], str | None]


def model_semantics_handler(
  *,
  deps: ModelSemanticsDeps,
  research_file_id: int | None = None,
  current_model_ref: dict[str, Any] | Any | None = None,
  ticker: str | None = None,
  sections: list[str] | str | None = None,
  user_id: str | None = None,
) -> dict[str, Any]:
  try:
    requested_sections = _normalize_sections(sections)
  except ValueError as exc:
    return _error_payload("invalid_sections", str(exc), field="sections")

  resolved_user_id = _resolve_user_id(deps, user_id)
  try:
    identity_ref, ticker_upper = _resolve_identity(
      deps,
      research_file_id=research_file_id,
      current_model_ref=current_model_ref,
      ticker=ticker,
      user_id=resolved_user_id,
    )
  except ValueError as exc:
    return _error_payload(
      getattr(exc, "code", "model_semantics_identity_error"),
      str(exc),
      details=getattr(exc, "details", None),
    )
  except Exception as exc:
    return _error_payload("model_semantics_identity_error", str(exc))

  if not ticker_upper:
    return _error_payload(
      "ticker_required",
      "Provide research_file_id, current_model_ref, or ticker so model_semantics can resolve a ticker.",
      field="ticker",
    )

  try:
    overrides = deps.load_ticker_overrides(ticker_upper)
    if overrides is None:
      overrides = deps.ticker_overrides_cls(
        ticker=ticker_upper,
        overrides={},
        custom_concepts={},
        file_meta={"ticker": ticker_upper, "schema_version": "1"},
      )
    semantics = deps.model_semantics_cls.from_ticker_overrides(overrides)
  except Exception as exc:
    return _error_payload("model_semantics_unavailable", str(exc), ticker=ticker_upper)

  has_semantics = bool(
    semantics.forecast.entries
    or semantics.scenarios.entries
    or semantics.valuation is not None
  )
  payload = {
    "status": "success",
    "ticker": ticker_upper,
    "identity": _identity_payload(
      deps,
      identity_ref=identity_ref,
      overrides=overrides,
      semantics=semantics,
      has_semantics=has_semantics,
    ),
    "sections": _sections_payload(semantics, requested_sections),
  }
  if not has_semantics:
    payload["warnings"] = [
      {
        "code": "model_semantics_missing",
        "message": "No forecast, scenario, or valuation intent is present in ticker overrides.",
      }
    ]
  return payload


def _normalize_sections(sections: list[str] | str | None) -> list[str]:
  if sections is None:
    return ["forecast", "scenarios", "valuation"]
  raw_values: list[Any]
  if isinstance(sections, str):
    text = sections.strip()
    raw_values = [] if not text else [part.strip() for part in text.split(",")]
  elif isinstance(sections, list):
    raw_values = sections
  else:
    raise ValueError("sections must be a list or comma-separated string")
  normalized: list[str] = []
  for value in raw_values:
    section = str(value or "").strip().lower()
    if not section:
      continue
    if section not in _MODEL_SEMANTICS_SECTIONS:
      raise ValueError(f"unsupported model_semantics section: {section!r}")
    if section not in normalized:
      normalized.append(section)
  return normalized or ["forecast", "scenarios", "valuation"]


def _resolve_user_id(deps: ModelSemanticsDeps, explicit_user_id: str | None) -> str | None:
  normalized = deps.valid_workspace_user_id(explicit_user_id)
  if normalized is not None:
    return normalized
  for env_var in ("AUTONOMOUS_USER_ID", "MEMORY_STARTUP_USER_ID", "RESEARCH_USER_ID"):
    normalized = deps.valid_workspace_user_id(os.getenv(env_var))
    if normalized is not None:
      return normalized
  try:
    from memory import get_current_user_id

    return deps.valid_workspace_user_id(get_current_user_id())
  except Exception:
    return None


def _resolve_identity(
  deps: ModelSemanticsDeps,
  *,
  research_file_id: int | None,
  current_model_ref: dict[str, Any] | Any | None,
  ticker: str | None,
  user_id: str | None,
) -> tuple[Any | None, str | None]:
  if current_model_ref is not None:
    ref = deps.current_model_ref_cls.model_validate(
      current_model_ref.model_dump(mode="json") if hasattr(current_model_ref, "model_dump") else current_model_ref
    )
    return ref, _normalize_ticker(getattr(ref, "ticker", None))

  if research_file_id is not None:
    if user_id is None:
      raise _IdentityError("user_id_required", "user_id is required when resolving model_semantics by research_file_id")
    repo = deps.get_repository_factory().get(user_id)
    ref = deps.resolve_current_model_ref(repo, int(research_file_id))
    if ref is None:
      return None, _ticker_from_file(repo, int(research_file_id))
    return ref, _normalize_ticker(getattr(ref, "ticker", None))

  ticker_upper = _normalize_ticker(ticker)
  if ticker_upper and user_id is not None:
    return _resolve_ticker_identity(deps, user_id=user_id, ticker=ticker_upper)
  return None, ticker_upper


def _resolve_ticker_identity(
  deps: ModelSemanticsDeps,
  *,
  user_id: str,
  ticker: str,
) -> tuple[Any | None, str]:
  repo = deps.get_repository_factory().get(user_id)
  matches = [
    row for row in repo.list_files(visibility="default", origin_kind="all")
    if _normalize_ticker(row.get("ticker")) == ticker
  ]
  live_refs = []
  for row in matches:
    try:
      ref = deps.resolve_current_model_ref(repo, int(row["id"]))
    except Exception:
      continue
    if ref is not None:
      live_refs.append(ref)
  identities = {
    (
      getattr(ref, "research_file_id", None),
      getattr(ref, "model_id", None),
      getattr(ref, "version", None),
      getattr(ref, "model_build_context_id", None),
      getattr(ref, "model_build_context_version", None),
    )
    for ref in live_refs
  }
  if len(identities) > 1:
    details = [
      {
        "research_file_id": getattr(ref, "research_file_id", None),
        "model_id": getattr(ref, "model_id", None),
        "version": getattr(ref, "version", None),
        "model_build_context_id": getattr(ref, "model_build_context_id", None),
        "model_build_context_version": getattr(ref, "model_build_context_version", None),
      }
      for ref in live_refs
    ]
    raise _IdentityError(
      "ambiguous_ticker",
      f"ticker {ticker} maps to more than one live current-model identity; pass research_file_id or current_model_ref.",
      details=details,
    )
  return (live_refs[0], ticker) if live_refs else (None, ticker)


def _ticker_from_file(repo: Any, research_file_id: int) -> str | None:
  row = repo.get_file(int(research_file_id))
  if not isinstance(row, dict):
    return None
  return _normalize_ticker(row.get("ticker"))


def _normalize_ticker(value: Any) -> str | None:
  text = str(value or "").strip().upper()
  return text or None


def _identity_payload(
  deps: ModelSemanticsDeps,
  *,
  identity_ref: Any | None,
  overrides: Any,
  semantics: Any,
  has_semantics: bool,
) -> dict[str, Any]:
  identity = {
    "research_file_id": getattr(identity_ref, "research_file_id", None),
    "handoff_id": getattr(identity_ref, "handoff_id", None),
    "handoff_version": getattr(identity_ref, "handoff_version", None),
    "model_id": getattr(identity_ref, "model_id", None),
    "model_version": getattr(identity_ref, "version", None),
    "model_build_context_id": getattr(identity_ref, "model_build_context_id", None),
    "model_build_context_version": getattr(identity_ref, "model_build_context_version", None),
    "workbook_path": getattr(identity_ref, "workbook_path", None),
    "file_path": getattr(identity_ref, "file_path", None),
    "overrides_schema_version": deps.derive_ticker_overrides_schema_version(overrides),
    "model_semantics_status": "success" if has_semantics else "missing",
    "model_semantics_hash": semantics.model_semantics_hash() if has_semantics else None,
  }
  return identity


def _sections_payload(semantics: Any, sections: list[str]) -> dict[str, Any]:
  payload: dict[str, Any] = {}
  if "forecast" in sections:
    payload["forecast"] = semantics.forecast.model_dump(mode="json")
  if "scenarios" in sections:
    payload["scenarios"] = semantics.scenarios.model_dump(mode="json")
  if "valuation" in sections:
    payload["valuation"] = (
      semantics.valuation.model_dump(mode="json")
      if semantics.valuation is not None
      else None
    )
  return payload


class _IdentityError(ValueError):
  def __init__(self, code: str, message: str, *, details: Any | None = None):
    super().__init__(message)
    self.code = code
    self.details = details


def _error_payload(
  code: str,
  message: str,
  *,
  field: str | None = None,
  ticker: str | None = None,
  details: Any | None = None,
) -> dict[str, Any]:
  error: dict[str, Any] = {"code": code, "message": message}
  if field is not None:
    error["field"] = field
  if details is not None:
    error["details"] = details
  payload: dict[str, Any] = {"status": "error", "error": error}
  if ticker is not None:
    payload["ticker"] = ticker
  return payload


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelSemanticsFunctions:
  model_semantics: Callable[..., dict[str, Any]]


def _parent_model_semantics_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelSemanticsDeps:
  return parent_namespace()["_model_semantics_deps"]()


def _parent_handler(
  parent_namespace: ParentNamespaceProvider,
  name: str,
) -> Callable[..., dict[str, Any]]:
  return parent_namespace()[name]


def _bind_parent_module(
  function: Callable[..., dict[str, Any]],
  parent_namespace: ParentNamespaceProvider,
) -> Callable[..., dict[str, Any]]:
  parent_module = parent_namespace().get("__name__")
  if isinstance(parent_module, str):
    function.__module__ = parent_module
    function.__qualname__ = function.__name__
    function.__annotations__ = {**get_type_hints(function), "return": dict}
  return function


def _register_tool(
  mcp: Any,
  function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
  registered = mcp.tool()(function)
  return registered or function


def register_model_semantics_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ModelSemanticsFunctions | None = None,
) -> ModelSemanticsFunctions:
  functions = functions or build_model_semantics_tool_functions(
    parent_namespace=parent_namespace,
  )
  selected_tool_names = tool_names or tuple(functions.__dict__)
  for name in selected_tool_names:
    functions = replace(
      functions,
      **{
        name: _register_tool(
          mcp,
          _bind_parent_module(getattr(functions, name), parent_namespace),
        )
      },
    )
  return functions


def build_model_semantics_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ModelSemanticsFunctions:
  def model_semantics(
    research_file_id: Optional[int] = None,
    current_model_ref: Optional[dict] = None,
    ticker: Optional[str] = None,
    sections: Optional[List[str] | str] = None,
    user_id: Optional[str] = None,
  ) -> dict:
    """Read typed model semantics for forecast, scenario, and valuation intent.

    Prefer research_file_id or a CurrentModelRef payload so the response carries
    model identity. Use ticker only when there is exactly one live current-model
    identity for that ticker, or when identity context is intentionally absent.
    """
    return _parent_handler(parent_namespace, "_model_semantics_handler")(
      deps=_parent_model_semantics_deps(parent_namespace),
      research_file_id=research_file_id,
      current_model_ref=current_model_ref,
      ticker=ticker,
      sections=sections,
      user_id=user_id,
    )

  return ModelSemanticsFunctions(model_semantics=model_semantics)


__all__ = [
  "ModelSemanticsFunctions",
  "ModelSemanticsDeps",
  "build_model_semantics_tool_functions",
  "model_semantics_handler",
  "register_model_semantics_tools",
]
