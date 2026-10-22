from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from model_build.reads import ModelSemanticsRead
from schema.model_ref import CurrentModelPointer, ModelRecord
from industry_slice_core.thesis_shared_slice_helpers import _normalize_ticker as _canonicalize_ticker


_MODEL_SEMANTICS_SECTION_ORDER = ("forecast", "scenarios", "valuation")
_MODEL_SEMANTICS_SECTIONS = frozenset(_MODEL_SEMANTICS_SECTION_ORDER)
_MODEL_SEMANTICS_SOURCE_DETAILS = frozenset({"none", "metadata", "full"})
_MODEL_SEMANTICS_SCENARIO_NAMES = frozenset({"base", "bull", "bear"})
_MODEL_SEMANTICS_CONTRACT_VERSION = "4.0"
_MODEL_SEMANTICS_DEFAULT_LIMIT = 25
_MODEL_SEMANTICS_MAX_LIMIT = 100
_MODEL_SEMANTICS_MAX_RESPONSE_CHARS = 40_000
_CURRENT_MODEL_REF_IDENTITY_FIELDS = frozenset({
  "ticker",
  "research_file_id",
})
_CURRENT_MODEL_REF_INTEGER_FIELDS = frozenset({
  "research_file_id",
})



ResolutionErrorT = TypeVar("ResolutionErrorT", bound=Exception)


@dataclass(frozen=True)
class ModelResolutionFailure:
  code: str
  retryable: bool
  fix: str | None


@dataclass(frozen=True)
class ModelSemanticsDeps(Generic[ResolutionErrorT]):
  load_current_model_semantics: Callable[
    [str, int, str],
    ModelSemanticsRead,
  ]
  get_repository_factory: Callable[[], Any]
  resolve_current_model_version: Callable[..., Any]
  valid_workspace_user_id: Callable[[object | None], str | None]
  current_model_resolution_error: type[ResolutionErrorT]
  resolution_failure: Callable[[ResolutionErrorT], ModelResolutionFailure]
  resolved_current_model_type: type


def model_semantics_handler(
  *,
  deps: ModelSemanticsDeps[ResolutionErrorT],
  research_file_id: int | None = None,
  current_model_ref: dict[str, Any] | Any | None = None,
  ticker: str | None = None,
  sections: list[str] | str | None = None,
  item_ids: list[str] | None = None,
  driver_keys: list[str] | None = None,
  scenario_names: list[str] | None = None,
  years: list[int] | None = None,
  source_detail: str = "metadata",
  limit: int = _MODEL_SEMANTICS_DEFAULT_LIMIT,
  cursor: str | None = None,
  user_id: str | None = None,
) -> dict[str, Any]:
  try:
    requested_sections = _normalize_sections(sections)
    requested_item_ids = _normalize_string_filter(item_ids, field="item_ids")
    requested_driver_keys = _normalize_string_filter(driver_keys, field="driver_keys")
    requested_scenarios = _normalize_scenario_names(scenario_names)
    requested_years = _normalize_years(years)
    requested_source_detail = _normalize_source_detail(source_detail)
    page_limit = _normalize_limit(limit)
  except _RequestError as exc:
    return _error_payload(
      exc.code,
      str(exc),
      field=exc.field,
      details=exc.details,
      recoverable=True,
      next_actions=exc.next_actions,
    )

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
      recoverable=getattr(exc, "recoverable", None),
      next_actions=getattr(exc, "next_actions", None),
    )
  except Exception as exc:
    return _error_payload("model_semantics_identity_error", str(exc))

  if identity_ref is None or not ticker_upper:
    return _error_payload(
      "canonical_model_identity_required",
      (
        "Provide an authenticated research_file_id or the exact current "
        "pointer/version payload; ticker cannot select a current model."
      ),
      field="research_file_id",
      recoverable=True,
      next_actions=[
        "Pass research_file_id for the exact current model.",
      ],
    )

  if resolved_user_id is None:
    return _error_payload(
      "user_id_required",
      "Canonical model semantics require an authenticated workspace user.",
      field="user_id",
      ticker=ticker_upper,
      recoverable=True,
      next_actions=[
        "Pass user_id together with research_file_id.",
      ],
    )
  resolved_research_file_id = _resolved_research_file_id(identity_ref)
  if (
    type(resolved_research_file_id) is not int
    or resolved_research_file_id < 1
  ):
    return _error_payload(
      "canonical_model_identity_required",
      (
        "model_semantics requires a live exact CurrentModel authority carrying "
        "research_file_id."
      ),
      ticker=ticker_upper,
      recoverable=True,
      next_actions=[
        "Pass research_file_id for a live current model.",
      ],
    )

  try:
    control_state = deps.load_current_model_semantics(
      resolved_user_id,
      resolved_research_file_id,
      ticker_upper,
    )
  except deps.current_model_resolution_error as exc:
    failure = deps.resolution_failure(exc)
    return _error_payload(
      failure.code,
      str(exc),
      ticker=ticker_upper,
      details={"error_type": type(exc).__name__},
      recoverable=failure.retryable,
      next_actions=[failure.fix] if failure.fix else None,
    )
  except Exception as exc:
    return _error_payload(
      "current_model_semantics_unavailable",
      str(exc),
      ticker=ticker_upper,
      details={"error_type": type(exc).__name__},
      recoverable=False,
    )

  semantics = control_state.sections
  identity = dict(control_state.identity)
  full_semantics_hash = identity["model_semantics_hash"]
  has_semantics = identity["model_semantics_status"] == "success"
  filters = {
    "sections": requested_sections,
    "item_ids": requested_item_ids,
    "driver_keys": requested_driver_keys,
    "scenario_names": requested_scenarios,
    "years": requested_years,
  }
  query_hash = _query_hash(filters, requested_source_detail)
  try:
    offset = _decode_cursor(
      cursor,
      full_semantics_hash=full_semantics_hash,
      query_hash=query_hash,
    )
  except _RequestError as exc:
    return _error_payload(
      exc.code,
      str(exc),
      field=exc.field,
      details=exc.details,
      recoverable=True,
      next_actions=exc.next_actions,
    )

  filtered = _filtered_sections(
    semantics,
    sections=requested_sections,
    item_ids=requested_item_ids,
    driver_keys=requested_driver_keys,
    scenario_names=requested_scenarios,
    years=requested_years,
  )
  request_warnings = _scenario_filter_warnings(
    sections=requested_sections,
    scenario_names=requested_scenarios,
  )
  matched_total = filtered["counts"]["matched"]["total"]
  if offset > matched_total:
    return _error_payload(
      "invalid_cursor",
      "cursor points beyond the matching model_semantics entries.",
      field="cursor",
      details={"offset": offset, "matched_entries": matched_total},
      recoverable=True,
      next_actions=["Retry the same model_semantics request without cursor."],
    )

  requested_page_size = min(page_limit, max(0, matched_total - offset))
  page_size = requested_page_size
  budget_limited = False
  while True:
    payload = _success_payload(
      ticker=ticker_upper,
      identity=identity,
      filtered=filtered,
      filters=filters,
      source_detail=requested_source_detail,
      requested_limit=limit,
      page_limit=page_limit,
      cursor=cursor,
      offset=offset,
      page_size=page_size,
      query_hash=query_hash,
      full_semantics_hash=full_semantics_hash,
      budget_limited=budget_limited,
      has_semantics=has_semantics,
      request_warnings=request_warnings,
    )
    response_chars = _serialized_chars(payload)
    if response_chars <= _MODEL_SEMANTICS_MAX_RESPONSE_CHARS or page_size <= 1:
      payload["view"]["response_chars"] = response_chars
      return payload
    page_size = max(1, page_size // 2)
    budget_limited = True


def _normalize_sections(sections: list[str] | str | None) -> list[str]:
  if sections is None:
    return list(_MODEL_SEMANTICS_SECTION_ORDER)
  raw_values: list[Any]
  if isinstance(sections, str):
    text = sections.strip()
    if not text:
      raw_values = []
    elif text.startswith("["):
      try:
        decoded = json.loads(text)
      except json.JSONDecodeError:
        raw_values = [part.strip() for part in text.split(",")]
      else:
        if not isinstance(decoded, list):
          raise _RequestError(
            "invalid_sections",
            "JSON-encoded sections must decode to a list of section names.",
            field="sections",
          )
        raw_values = decoded
    else:
      raw_values = [part.strip() for part in text.split(",")]
  elif isinstance(sections, list):
    raw_values = sections
  else:
    raise _RequestError(
      "invalid_sections",
      "sections must be a list or comma-separated string.",
      field="sections",
    )
  if not raw_values:
    raise _RequestError(
      "invalid_sections",
      "sections must not be empty; omit sections to read all sections.",
      field="sections",
      next_actions=[
        "Omit sections for forecast, scenarios, and valuation, or pass one or more named sections."
      ],
    )
  selected: set[str] = set()
  for value in raw_values:
    section = str(value or "").strip().lower()
    if not section:
      raise _RequestError(
        "invalid_sections",
        "sections must not contain blank values.",
        field="sections",
      )
    if section not in _MODEL_SEMANTICS_SECTIONS:
      raise _RequestError(
        "invalid_sections",
        f"unsupported model_semantics section: {section!r}.",
        field="sections",
        details={"supported_sections": list(_MODEL_SEMANTICS_SECTION_ORDER)},
      )
    selected.add(section)
  return [section for section in _MODEL_SEMANTICS_SECTION_ORDER if section in selected]


def _normalize_string_filter(values: list[str] | None, *, field: str) -> list[str] | None:
  if values is None:
    return None
  if not isinstance(values, list):
    raise _RequestError(
      "invalid_filters",
      f"{field} must be a list of non-empty strings.",
      field=field,
    )
  if not values:
    raise _RequestError(
      "invalid_filters",
      f"{field} must not be empty; omit it to match all entries.",
      field=field,
    )
  normalized: list[str] = []
  for value in values:
    if not isinstance(value, str) or not value.strip():
      raise _RequestError(
        "invalid_filters",
        f"{field} must contain only non-empty strings.",
        field=field,
      )
    text = value.strip()
    if text not in normalized:
      normalized.append(text)
  return sorted(normalized)


def _normalize_scenario_names(values: list[str] | None) -> list[str] | None:
  normalized = _normalize_string_filter(values, field="scenario_names")
  if normalized is None:
    return None
  unsupported = sorted(set(normalized) - _MODEL_SEMANTICS_SCENARIO_NAMES)
  if unsupported:
    raise _RequestError(
      "invalid_filters",
      f"unsupported scenario_names: {unsupported}.",
      field="scenario_names",
      details={"supported_scenario_names": ["base", "bull", "bear"]},
    )
  return [name for name in ("base", "bull", "bear") if name in normalized]


def _scenario_filter_warnings(
  *,
  sections: list[str],
  scenario_names: list[str] | None,
) -> list[dict[str, str]]:
  if scenario_names is None:
    return []
  selected = set(scenario_names)
  warnings: list[dict[str, str]] = []
  if "forecast" in sections and "base" not in selected:
    warnings.append({
      "code": "model_semantics_scenario_filter_excludes_forecast",
      "message": (
        "scenario_names excludes base, so the requested forecast section has no entries."
      ),
    })
  return warnings


def _normalize_years(values: list[int] | None) -> list[int] | None:
  if values is None:
    return None
  if not isinstance(values, list) or not values:
    raise _RequestError(
      "invalid_filters",
      "years must be a non-empty list of four-digit integers.",
      field="years",
    )
  normalized: set[int] = set()
  for value in values:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1000 or value > 9999:
      raise _RequestError(
        "invalid_filters",
        "years must contain only four-digit integers, for example [2027, 2028].",
        field="years",
      )
    normalized.add(value)
  return sorted(normalized)


def _normalize_source_detail(value: str) -> str:
  normalized = str(value or "").strip().lower()
  if normalized not in _MODEL_SEMANTICS_SOURCE_DETAILS:
    raise _RequestError(
      "invalid_source_detail",
      "source_detail must be one of: none, metadata, full.",
      field="source_detail",
      details={"supported_source_details": ["none", "metadata", "full"]},
    )
  return normalized


def _normalize_limit(value: int) -> int:
  if isinstance(value, bool) or not isinstance(value, int):
    raise _RequestError(
      "invalid_limit",
      "limit must be an integer.",
      field="limit",
    )
  return min(max(value, 1), _MODEL_SEMANTICS_MAX_LIMIT)


def _resolve_user_id(deps: ModelSemanticsDeps[ResolutionErrorT], explicit_user_id: str | None) -> str | None:
  normalized = deps.valid_workspace_user_id(explicit_user_id)
  if normalized is not None:
    return normalized
  try:
    from memory import get_current_user_id

    return deps.valid_workspace_user_id(get_current_user_id())
  except Exception:
    return None


def _resolve_identity(
  deps: ModelSemanticsDeps[ResolutionErrorT],
  *,
  research_file_id: int | None,
  current_model_ref: dict[str, Any] | Any | None,
  ticker: str | None,
  user_id: str | None,
) -> tuple[Any | None, str | None]:
  if current_model_ref is not None:
    payload = _current_model_ref_payload(current_model_ref)
    validation_input = payload if payload is not None else current_model_ref
    try:
      supplied_ref = _validate_current_model_resolution(deps, validation_input)
    except (TypeError, ValueError) as exc:
      if payload is None:
        raise _IdentityError(
          "invalid_current_model_ref",
          (
            "current_model_ref must be the exact current pointer/version "
            "payload or an identity selector containing research_file_id."
          ),
          details={
            "accepted_selectors": ["research_file_id"],
            "received_type": type(current_model_ref).__name__,
          },
          next_actions=[
            "Pass research_file_id as a top-level argument or inside current_model_ref.",
            "Alternatively pass the exact current pointer/version payload returned by the research workflow.",
          ],
        ) from exc
    else:
      constraints = _top_level_identity_constraints(
        research_file_id=research_file_id,
        ticker=ticker,
      )
      _validate_resolved_identity(supplied_ref, constraints)
      resolved_ref, resolved_ticker = _resolve_research_file_identity(
        deps,
        research_file_id=_resolved_research_file_id(supplied_ref),
        user_id=user_id,
      )
      _validate_exact_current_model_ref(
        supplied_ref=supplied_ref,
        resolved_ref=resolved_ref,
      )
      return resolved_ref, resolved_ticker

    return _resolve_identity_selector(
      deps,
      selector=payload,
      research_file_id=research_file_id,
      ticker=ticker,
      user_id=user_id,
    )

  return _resolve_identity_selector(
    deps,
    selector={},
    research_file_id=research_file_id,
    ticker=ticker,
    user_id=user_id,
  )


def _resolve_identity_selector(
  deps: ModelSemanticsDeps[ResolutionErrorT],
  *,
  selector: dict[str, Any],
  research_file_id: int | None,
  ticker: str | None,
  user_id: str | None,
) -> tuple[Any | None, str | None]:
  unexpected_fields = sorted(set(selector) - _CURRENT_MODEL_REF_IDENTITY_FIELDS)
  if unexpected_fields:
    raise _IdentityError(
      "invalid_current_model_ref",
      "A partial current_model_ref may contain only current-model identity fields.",
      details={
        "unexpected_fields": unexpected_fields,
        "accepted_lookup_fields": ["research_file_id"],
      },
      next_actions=[
        "Pass the exact current pointer/version payload, or reduce current_model_ref to a research_file_id selector.",
      ],
    )

  top_level_file_id = _optional_positive_int(research_file_id, field="research_file_id")
  nested_file_id = _optional_positive_int(
    selector.get("research_file_id"),
    field="current_model_ref.research_file_id",
  )
  if top_level_file_id is not None and nested_file_id is not None and top_level_file_id != nested_file_id:
    raise _identity_mismatch(
      "research_file_id",
      top_level_file_id,
      nested_file_id,
      resolved_source="top-level and current_model_ref selectors",
    )
  selected_file_id = top_level_file_id if top_level_file_id is not None else nested_file_id

  top_level_ticker = _normalize_ticker(ticker)
  nested_ticker = _normalize_ticker(selector.get("ticker"))
  if top_level_ticker and nested_ticker and top_level_ticker != nested_ticker:
    raise _identity_mismatch(
      "ticker",
      top_level_ticker,
      nested_ticker,
      resolved_source="top-level and current_model_ref selectors",
    )
  selected_ticker = top_level_ticker or nested_ticker

  if selected_file_id is not None:
    ref, resolved_ticker = _resolve_research_file_identity(
      deps,
      research_file_id=selected_file_id,
      user_id=user_id,
    )
    _validate_resolved_identity(ref, selector)
    if selected_ticker and resolved_ticker and selected_ticker != resolved_ticker:
      raise _identity_mismatch(
        "ticker",
        selected_ticker,
        resolved_ticker,
        resolved_source=f"research_file_id={selected_file_id}",
      )
    return ref, resolved_ticker

  supplied_fields = sorted(
    key for key, value in selector.items() if value is not None
  )
  if selected_ticker and "ticker" not in supplied_fields:
    supplied_fields.append("ticker")
    supplied_fields.sort()
  raise _IdentityError(
    "canonical_model_identity_required",
    (
      "model_semantics requires research_file_id or the exact current "
      "pointer/version payload; ticker and other identity hints cannot select a "
      "current model."
    ),
    details={
      "supplied_fields": supplied_fields,
      "accepted_lookup_fields": ["research_file_id"],
    },
    next_actions=[
      "Pass research_file_id so the server can resolve and authenticate the exact CurrentModel authority.",
      "Or pass the exact current pointer/version payload returned by the research workflow.",
    ],
  )


def _resolve_research_file_identity(
  deps: ModelSemanticsDeps[ResolutionErrorT],
  *,
  research_file_id: int,
  user_id: str | None,
) -> tuple[Any | None, str | None]:
  if user_id is None:
    raise _IdentityError(
      "user_id_required",
      "user_id is required when resolving model_semantics by research_file_id.",
      next_actions=["Pass the workspace user_id together with research_file_id."],
    )
  repo = deps.get_repository_factory().get_live_read_only(user_id)
  file_row = repo.get_file(research_file_id)
  if file_row is None:
    raise _IdentityError(
      "current_model_ref_not_found",
      f"research_file_id={research_file_id} was not found in the authenticated workspace.",
      details={"research_file_id": research_file_id},
      next_actions=["Use a research_file_id in the authenticated workspace."],
    )
  ticker = _normalize_ticker(file_row.get("ticker"))
  if ticker is None:
    raise _IdentityError(
      "current_model_ref_invalid",
      "The research file does not have a canonical ticker.",
      details={"research_file_id": research_file_id},
      recoverable=False,
    )
  try:
    ref = deps.resolve_current_model_version(
      repo,
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
    )
  except deps.current_model_resolution_error as exc:
    failure = deps.resolution_failure(exc)
    raise _IdentityError(
      failure.code,
      str(exc),
      details={"research_file_id": research_file_id},
      recoverable=failure.retryable,
      next_actions=(
        [failure.fix]
        if failure.fix
        else ["Select a valid current ModelVersion and retry."]
      ),
    ) from exc
  except Exception as exc:
    raise _IdentityError(
      getattr(exc, "code", "current_model_ref_not_found"),
      str(exc),
      details={"research_file_id": research_file_id},
      next_actions=["Select a valid current ModelVersion and retry."],
    ) from exc
  if not isinstance(ref, deps.resolved_current_model_type):
    raise _IdentityError(
      "current_model_ref_invalid",
      "The CurrentModel resolver did not return a ResolvedCurrentModel.",
      details={
        "research_file_id": research_file_id,
        "resolved_type": type(ref).__name__,
      },
      recoverable=False,
    )
  return ref, _normalize_ticker(ref.model_version.ticker)


def _validate_current_model_resolution(
  deps: ModelSemanticsDeps[ResolutionErrorT],
  value: Any,
) -> Any:
  if isinstance(value, deps.resolved_current_model_type):
    return value
  if not isinstance(value, Mapping):
    raise TypeError("current_model_ref must be a mapping")
  if set(value) != {"current_model_pointer", "model_version"}:
    raise ValueError("current_model_ref must contain the exact pointer/version payload")
  pointer = CurrentModelPointer.model_validate(
    value["current_model_pointer"]
  )
  model_version = ModelRecord.model_validate(value["model_version"])
  return deps.resolved_current_model_type(
    pointer=pointer,
    model_version=model_version,
  )


def _resolved_research_file_id(ref: Any) -> int:
  value = getattr(getattr(ref, "model_version", None), "research_file_id", None)
  if type(value) is not int or value < 1:
    raise _IdentityError(
      "current_model_ref_invalid",
      "The pointer/version resolution has no valid research_file_id.",
      recoverable=False,
    )
  return value


def _current_model_ref_payload(current_model_ref: Any) -> dict[str, Any] | None:
  pointer = getattr(current_model_ref, "pointer", None)
  model_version = getattr(current_model_ref, "model_version", None)
  if pointer is not None and model_version is not None:
    return {
      "current_model_pointer": pointer.model_dump(mode="json"),
      "model_version": model_version.model_dump(mode="json"),
    }
  if hasattr(current_model_ref, "model_dump"):
    dumped = current_model_ref.model_dump(mode="json")
    return dict(dumped) if isinstance(dumped, Mapping) else None
  if isinstance(current_model_ref, Mapping):
    return dict(current_model_ref)
  return None


def _top_level_identity_constraints(
  *,
  research_file_id: int | None,
  ticker: str | None,
) -> dict[str, Any]:
  constraints: dict[str, Any] = {}
  normalized_file_id = _optional_positive_int(research_file_id, field="research_file_id")
  if normalized_file_id is not None:
    constraints["research_file_id"] = normalized_file_id
  normalized_ticker = _normalize_ticker(ticker)
  if normalized_ticker:
    constraints["ticker"] = normalized_ticker
  return constraints


def _optional_positive_int(value: Any, *, field: str) -> int | None:
  if value is None:
    return None
  if isinstance(value, bool):
    raise _IdentityError(
      "invalid_research_file_id",
      f"{field} must be a positive integer.",
      details={"field": field, "value": value},
    )
  try:
    normalized = int(value)
  except (TypeError, ValueError) as exc:
    raise _IdentityError(
      "invalid_research_file_id",
      f"{field} must be a positive integer.",
      details={"field": field, "value": value},
    ) from exc
  if normalized < 1:
    raise _IdentityError(
      "invalid_research_file_id",
      f"{field} must be a positive integer.",
      details={"field": field, "value": value},
    )
  return normalized


def _validate_resolved_identity(ref: Any, constraints: Mapping[str, Any]) -> None:
  mismatches = []
  for field in sorted(_CURRENT_MODEL_REF_IDENTITY_FIELDS.intersection(constraints)):
    supplied = constraints[field]
    if supplied is None:
      continue
    resolved = getattr(getattr(ref, "model_version", None), field, None)
    if _normalized_identity_value(field, supplied) == _normalized_identity_value(field, resolved):
      continue
    mismatches.append({
      "field": field,
      "supplied": supplied,
      "resolved": resolved,
    })
  if mismatches:
    raise _IdentityError(
      "current_model_ref_mismatch",
      "The supplied identity fields do not match the canonical current model.",
      details={"mismatches": mismatches},
      next_actions=[
        "Use the canonical current model returned for this research_file_id, or remove stale identity hints.",
      ],
    )


def _validate_exact_current_model_ref(
  *,
  supplied_ref: Any,
  resolved_ref: Any,
) -> None:
  supplied_payload = _current_model_ref_payload(supplied_ref)
  resolved_payload = _current_model_ref_payload(resolved_ref)
  if (
    type(supplied_ref) is type(resolved_ref)
    and supplied_payload is not None
    and supplied_payload == resolved_payload
  ):
    return
  raise _IdentityError(
    "current_model_ref_mismatch",
    (
      "The supplied current pointer/version resolution is not byte-identical "
      "to the authenticated current selection."
    ),
    details={
      "research_file_id": _resolved_research_file_id(supplied_ref),
      "supplied_pointer_epoch": getattr(supplied_ref.pointer, "epoch", None),
      "resolved_pointer_epoch": getattr(resolved_ref.pointer, "epoch", None),
    },
    next_actions=[
      "Re-read the exact current pointer/version for research_file_id and retry.",
    ],
  )


def _normalized_identity_value(field: str, value: Any) -> Any:
  if field == "ticker":
    return _normalize_ticker(value)
  if field in _CURRENT_MODEL_REF_INTEGER_FIELDS:
    try:
      return int(value)
    except (TypeError, ValueError):
      return value
  return str(value).strip() if value is not None else None


def _identity_mismatch(
  field: str,
  supplied: Any,
  resolved: Any,
  *,
  resolved_source: str,
) -> _IdentityError:
  return _IdentityError(
    "current_model_ref_mismatch",
    f"Conflicting {field} values were supplied for model_semantics identity resolution.",
    details={
      "mismatches": [{"field": field, "supplied": supplied, "resolved": resolved}],
      "resolved_source": resolved_source,
    },
    next_actions=["Pass one canonical identity selector, preferably research_file_id."],
  )


def _normalize_ticker(value: Any) -> str | None:
  return _canonicalize_ticker(value) if str(value or "").strip() else None


def _filtered_sections(
  semantics: Mapping[str, Any],
  *,
  sections: list[str],
  item_ids: list[str] | None,
  driver_keys: list[str] | None,
  scenario_names: list[str] | None,
  years: list[int] | None,
) -> dict[str, Any]:
  forecast_dump = copy.deepcopy(dict(semantics["forecast"]))
  scenarios_dump = copy.deepcopy(dict(semantics["scenarios"]))
  valuation_dump = copy.deepcopy(semantics["valuation"])
  available = {
    "forecast_entries": len(forecast_dump.get("entries", [])),
    "scenario_entries": len(scenarios_dump.get("entries", [])),
    "scenario_variants": sum(
      len(entry.get("scenarios", {})) for entry in scenarios_dump.get("entries", [])
    ),
    "valuation_present": bool(valuation_dump.get("entries")),
  }

  stream: list[tuple[str, dict[str, Any]]] = []
  matched_forecast = 0
  matched_scenarios = 0
  matched_variants = 0
  if "forecast" in sections and (scenario_names is None or "base" in scenario_names):
    for entry in forecast_dump.get("entries", []):
      if not _entry_matches(
        entry,
        item_ids=item_ids,
        driver_keys=driver_keys,
      ):
        continue
      base = _filter_scenario_years(entry.get("base"), years)
      if base is None:
        continue
      filtered_entry = {**entry, "base": base}
      stream.append(("forecast", filtered_entry))
      matched_forecast += 1

  if "scenarios" in sections:
    requested_scenarios = (
      {"base", "bull", "bear"}
      if scenario_names is None
      else set(scenario_names).intersection({"base", "bull", "bear"})
    )
    for entry in scenarios_dump.get("entries", []):
      if not _entry_matches(
        entry,
        item_ids=item_ids,
        driver_keys=driver_keys,
      ):
        continue
      filtered_scenarios: dict[str, Any] = {}
      for name in ("base", "bull", "bear"):
        if name not in requested_scenarios:
          continue
        scenario = _filter_scenario_years(entry.get("scenarios", {}).get(name), years)
        if scenario is not None:
          filtered_scenarios[name] = scenario
      if not filtered_scenarios:
        continue
      filtered_entry = {**entry, "scenarios": filtered_scenarios}
      stream.append(("scenarios", filtered_entry))
      matched_scenarios += 1
      matched_variants += len(filtered_scenarios)

  section_rank = {"forecast": 0, "scenarios": 1}
  stream.sort(
    key=lambda row: (
      section_rank[row[0]],
      str(row[1].get("driver_id") or ""),
      str(row[1].get("control_id") or ""),
      str(row[1].get("item_id") or ""),
    )
  )
  templates: dict[str, Any] = {}
  if "forecast" in sections:
    templates["forecast"] = {
      key: copy.deepcopy(value)
      for key, value in forecast_dump.items()
      if key != "entries"
    }
  if "scenarios" in sections:
    templates["scenarios"] = {
      key: copy.deepcopy(value)
      for key, value in scenarios_dump.items()
      if key != "entries"
    }
  if "valuation" in sections:
    templates["valuation"] = copy.deepcopy(valuation_dump)

  return {
    "templates": templates,
    "stream": stream,
    "counts": {
      "available": available,
      "matched": {
        "forecast_entries": matched_forecast,
        "scenario_entries": matched_scenarios,
        "scenario_variants": matched_variants,
        "valuation_present": (
          "valuation" in sections and bool(valuation_dump.get("entries"))
        ),
        "total": len(stream),
      },
    },
  }


def _entry_matches(
  entry: Mapping[str, Any],
  *,
  item_ids: list[str] | None,
  driver_keys: list[str] | None,
) -> bool:
  if item_ids is not None and entry.get("item_id") not in item_ids:
    return False
  if driver_keys is not None and entry.get("driver_id") not in driver_keys:
    return False
  return True


def _filter_scenario_years(
  scenario: Mapping[str, Any] | None,
  years: list[int] | None,
) -> dict[str, Any] | None:
  if not isinstance(scenario, Mapping):
    return None
  filtered = copy.deepcopy(dict(scenario))
  if years is None:
    return filtered
  requested = {str(year) for year in years}
  values = {
    year: value
    for year, value in (filtered.get("values") or {}).items()
    if year in requested
  }
  if not values:
    return None
  filtered["values"] = values
  for field in ("extension_basis_by_period", "source_refs_by_period"):
    by_period = filtered.get(field)
    if isinstance(by_period, Mapping):
      filtered[field] = {
        year: value for year, value in by_period.items() if year in requested
      }
  refs_by_period = filtered.get("source_refs_by_period")
  if isinstance(refs_by_period, Mapping):
    referenced_ids = {
      str(source_id)
      for refs in refs_by_period.values()
      if isinstance(refs, list)
      for source_id in refs
    }
    filtered["sources"] = [
      source
      for source in filtered.get("sources", [])
      if str(source.get("id")) in referenced_ids
    ]
  return filtered


def _success_payload(
  *,
  ticker: str,
  identity: dict[str, Any],
  filtered: dict[str, Any],
  filters: dict[str, Any],
  source_detail: str,
  requested_limit: int,
  page_limit: int,
  cursor: str | None,
  offset: int,
  page_size: int,
  query_hash: str,
  full_semantics_hash: str | None,
  budget_limited: bool,
  has_semantics: bool,
  request_warnings: list[dict[str, str]],
) -> dict[str, Any]:
  sections = copy.deepcopy(filtered["templates"])
  if "forecast" in sections:
    sections["forecast"]["entries"] = []
  if "scenarios" in sections:
    sections["scenarios"]["entries"] = []

  page = filtered["stream"][offset:offset + page_size]
  returned_forecast = 0
  returned_scenarios = 0
  returned_variants = 0
  for section, entry in page:
    sections[section]["entries"].append(copy.deepcopy(entry))
    if section == "forecast":
      returned_forecast += 1
    else:
      returned_scenarios += 1
      returned_variants += len(entry.get("scenarios", {}))

  source_registry_plan = _source_registry_plan(filtered["stream"])
  source_registry, returned_source_count = _project_sources(
    sections,
    source_detail=source_detail,
    registry_plan=source_registry_plan,
  )
  matched_source_count = len(source_registry_plan)
  matched_total = filtered["counts"]["matched"]["total"]
  next_offset = offset + len(page)
  has_more = next_offset < matched_total
  next_cursor = (
    _encode_cursor(
      offset=next_offset,
      full_semantics_hash=full_semantics_hash,
      query_hash=query_hash,
    )
    if has_more
    else None
  )

  payload: dict[str, Any] = {
    "status": "success",
    "ticker": ticker,
    "identity": identity,
    "contract": {
      "name": "model_semantics.response",
      "version": _MODEL_SEMANTICS_CONTRACT_VERSION,
      "canonical_full_model_semantics_hash": full_semantics_hash,
      "entry_order": ["forecast", "scenarios"],
    },
    "view": {
      "mode": "bounded",
      "source_detail": source_detail,
      "source_layout": {
        "none": "references_only",
        "metadata": "deduplicated_source_registry",
        "full": "inline_historical_source_metadata",
      }[source_detail],
      "max_response_chars": _MODEL_SEMANTICS_MAX_RESPONSE_CHARS,
    },
    "filters": filters,
    "counts": {
      **filtered["counts"],
      "returned": {
        "forecast_entries": returned_forecast,
        "scenario_entries": returned_scenarios,
        "scenario_variants": returned_variants,
        "valuation_present": bool(
          (sections.get("valuation") or {}).get("entries")
        ),
        "total": len(page),
      },
      "sources": {
        "matched_unique": matched_source_count,
        "returned_unique": returned_source_count,
      },
    },
    "pagination": {
      "requested_limit": requested_limit,
      "limit": page_limit,
      "returned": len(page),
      "cursor": cursor,
      "next_cursor": next_cursor,
      "has_more": has_more,
      "budget_limited": budget_limited,
    },
    "sections": sections,
  }
  if source_detail == "metadata":
    payload["source_registry"] = source_registry
  payload["citation_contract"] = {
    "model_semantics_evidence": "historical_provenance_only",
    "run_issued_handle_ids": [],
    "historical_source_count": returned_source_count,
    "instruction": (
      "model_semantics never mints current-run citation handles. Do not use any stored "
      "excerpt_id or handle-shaped historical identifier in an FMS source_evidence_by_ref "
      "field. Before FMS, re-read the exact filing or transcript with "
      "filings_source_excerpt or transcripts_source_excerpt during this skill run and use "
      "only the returned handle_id."
    ),
  }
  warnings: list[dict[str, Any]] = []
  if returned_source_count:
    warnings.append({
      "code": "historical_sources_not_current_run_evidence",
      "message": (
        f"Returned {returned_source_count} historical source record(s). Their identifiers "
        "and provenance are context only and cannot satisfy an FMS current-run evidence door."
      ),
    })
  warnings.extend(request_warnings)
  if not has_semantics:
    warnings.append({
      "code": "model_semantics_missing",
      "message": (
        "No canonical forecast/scenario controls or valuation inputs are present."
      ),
    })
  if warnings:
    payload["warnings"] = warnings
  return payload


def _project_sources(
  sections: dict[str, Any],
  *,
  source_detail: str,
  registry_plan: Mapping[tuple[str, str], str],
) -> tuple[list[dict[str, Any]], int]:
  registry: dict[str, dict[str, Any]] = {}
  returned_registry_ids: set[str] = set()
  for scenario in _section_scenarios(sections):
    sources = scenario.get("sources")
    if not isinstance(sources, list):
      sources = []
    local_ids: set[str] = set()
    registry_ids: set[str] = set()
    registry_ids_by_local: dict[str, set[str]] = {}
    for source in sources:
      if not isinstance(source, Mapping) or not source.get("id"):
        continue
      local_id = str(source["id"])
      registry_id = registry_plan[(local_id, _canonical_source_hash(source))]
      local_ids.add(local_id)
      registry_ids.add(registry_id)
      returned_registry_ids.add(registry_id)
      registry_ids_by_local.setdefault(local_id, set()).add(registry_id)
      if source_detail == "metadata" and registry_id not in registry:
        metadata = _source_metadata(source)
        if registry_id != local_id:
          metadata["scenario_local_id"] = local_id
          metadata["id"] = registry_id
        registry[registry_id] = metadata
    if source_detail == "full":
      scenario["sources"] = [
        _historical_source_full(source)
        for source in sources
        if isinstance(source, Mapping)
      ]
      continue
    scenario.pop("sources", None)
    scenario["source_ids"] = sorted(
      registry_ids if source_detail == "metadata" else local_ids
    )
    if source_detail == "metadata":
      _rewrite_source_refs_by_period(
        scenario,
        registry_ids_by_local=registry_ids_by_local,
      )
  return [registry[source_id] for source_id in sorted(registry)], len(returned_registry_ids)


def _source_registry_plan(
  stream: list[tuple[str, dict[str, Any]]],
) -> dict[tuple[str, str], str]:
  """Assign page-stable registry IDs without assuming source IDs are global.

  ScenarioEntry validates source references only against its own sources. Reusing
  a local ID such as ``src_1`` in another entry is therefore schema-valid. Keep
  the readable local ID when it identifies one record across the filtered
  stream; namespace it deterministically when multiple records share it.
  """

  hashes_by_local_id: dict[str, set[str]] = {}
  keys: set[tuple[str, str]] = set()
  for source in _stream_sources(stream):
    local_id = str(source.get("id") or "")
    if not local_id:
      continue
    record_hash = _canonical_source_hash(source)
    keys.add((local_id, record_hash))
    hashes_by_local_id.setdefault(local_id, set()).add(record_hash)

  plan: dict[tuple[str, str], str] = {}
  reserved_local_ids = set(hashes_by_local_id)
  assigned_registry_ids: set[str] = set()
  for local_id, record_hash in sorted(keys):
    if len(hashes_by_local_id[local_id]) == 1:
      registry_id = local_id
    else:
      scoped_hash = _sha256_text(f"{local_id}\0{record_hash}")
      registry_id = f"registry:sha256:{scoped_hash}"
      while registry_id in reserved_local_ids or registry_id in assigned_registry_ids:
        registry_id = f"registry:sha256:{_sha256_text(registry_id)}"
    plan[(local_id, record_hash)] = registry_id
    assigned_registry_ids.add(registry_id)
  return plan


def _rewrite_source_refs_by_period(
  scenario: dict[str, Any],
  *,
  registry_ids_by_local: Mapping[str, set[str]],
) -> None:
  refs_by_period = scenario.get("source_refs_by_period")
  if not isinstance(refs_by_period, Mapping):
    return
  rewritten: dict[str, Any] = {}
  for period, refs in refs_by_period.items():
    if not isinstance(refs, list):
      rewritten[str(period)] = copy.deepcopy(refs)
      continue
    registry_refs: set[str] = set()
    for ref in refs:
      local_id = str(ref)
      registry_refs.update(registry_ids_by_local.get(local_id, {local_id}))
    rewritten[str(period)] = sorted(registry_refs)
  scenario["source_refs_by_period"] = rewritten


def _canonical_source_hash(source: Mapping[str, Any]) -> str:
  encoded = json.dumps(
    source,
    sort_keys=True,
    separators=(",", ":"),
    default=str,
  ).encode("utf-8")
  return hashlib.sha256(encoded).hexdigest()


def _sha256_text(value: str) -> str:
  return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _stream_sources(
  stream: list[tuple[str, dict[str, Any]]],
) -> Iterator[Mapping[str, Any]]:
  for section, entry in stream:
    scenario_values = [entry.get("base")] if section == "forecast" else list(
      (entry.get("scenarios") or {}).values()
    )
    for scenario in scenario_values:
      if not isinstance(scenario, Mapping):
        continue
      for source in scenario.get("sources", []):
        if isinstance(source, Mapping):
          yield source


def _section_scenarios(sections: Mapping[str, Any]) -> list[dict[str, Any]]:
  scenarios: list[dict[str, Any]] = []
  forecast = sections.get("forecast")
  if isinstance(forecast, Mapping):
    for entry in forecast.get("entries", []):
      base = entry.get("base")
      if isinstance(base, dict):
        scenarios.append(base)
  scenario_set = sections.get("scenarios")
  if isinstance(scenario_set, Mapping):
    for entry in scenario_set.get("entries", []):
      variants = entry.get("scenarios")
      if not isinstance(variants, Mapping):
        continue
      for name in ("base", "bull", "bear"):
        scenario = variants.get(name)
        if isinstance(scenario, dict):
          scenarios.append(scenario)
  return scenarios


def _source_metadata(source: Mapping[str, Any]) -> dict[str, Any]:
  fields = (
    "id",
    "type",
    "source_id",
    "identity_hash",
    "provider",
    "document_id",
    "endpoint_or_filing_id",
    "retrieved_at",
    "section_header",
    "char_start",
    "char_end",
    "annotation_id",
    "table_citation_record_id",
    "table_id",
    "table_value_source",
    "parsed_value_text",
    "parsed_numeric_value",
    "unit",
    "period",
    "concept",
    "skill_name",
    "artifact_path",
    "artifact_id",
    "skill_run_id",
    "source_path",
    "page_number",
    "spine_hash",
    "extraction_version",
  )
  metadata = {
    field: copy.deepcopy(source[field])
    for field in fields
    if source.get(field) is not None
  }
  source_id = metadata.get("source_id")
  if isinstance(source_id, str) and _is_local_path_shaped(source_id):
    metadata["source_id"] = f"sha256:{_sha256_text(source_id)}"
    metadata["source_id_local_path_redacted"] = True
  text = source.get("text")
  excerpts = source.get("excerpts")
  metadata["text_chars"] = len(text) if isinstance(text, str) else 0
  metadata["excerpt_count"] = len(excerpts) if isinstance(excerpts, list) else 0
  metadata["excerpt_chars"] = sum(
    len(str(excerpt.get("text") or ""))
    for excerpt in excerpts or []
    if isinstance(excerpt, Mapping)
  )
  metadata["historical_excerpt_count"] = metadata["excerpt_count"]
  metadata["citation_status"] = "historical_provenance_only"
  metadata["fresh_handle_required_for_fms"] = True
  key_fields = source.get("key_fields")
  if isinstance(key_fields, Mapping):
    metadata["key_field_names"] = sorted(str(key) for key in key_fields)
  return metadata


def _historical_source_full(source: Mapping[str, Any]) -> dict[str, Any]:
  """Return the inline source shape without stale evidence text or handles."""

  projected = {
    key: copy.deepcopy(value)
    for key, value in source.items()
    if key not in {"text", "excerpts"}
  }
  text = source.get("text")
  excerpts = source.get("excerpts")
  projected["text_chars"] = len(text) if isinstance(text, str) else 0
  projected["historical_excerpt_count"] = len(excerpts) if isinstance(excerpts, list) else 0
  projected["citation_status"] = "historical_provenance_only"
  projected["fresh_handle_required_for_fms"] = True
  return projected


def _is_local_path_shaped(value: str) -> bool:
  text = value.strip()
  lowered = text.lower()
  return bool(
    lowered.startswith("file://")
    or text.startswith(("/", "~/", "\\\\", "//"))
    or re.match(r"^[A-Za-z]:[\\/]", text)
  )


def _query_hash(filters: Mapping[str, Any], source_detail: str) -> str:
  encoded = json.dumps(
    {"filters": filters, "source_detail": source_detail},
    sort_keys=True,
    separators=(",", ":"),
  ).encode("utf-8")
  return hashlib.sha256(encoded).hexdigest()


def _encode_cursor(
  *,
  offset: int,
  full_semantics_hash: str | None,
  query_hash: str,
) -> str:
  payload = {
    "v": 1,
    "h": full_semantics_hash,
    "q": query_hash,
    "o": offset,
  }
  encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
  return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def _decode_cursor(
  cursor: str | None,
  *,
  full_semantics_hash: str | None,
  query_hash: str,
) -> int:
  if cursor is None:
    return 0
  if not isinstance(cursor, str) or not cursor.strip():
    raise _RequestError(
      "invalid_cursor",
      "cursor must be the non-empty cursor returned by a previous model_semantics page.",
      field="cursor",
      next_actions=["Retry the same model_semantics request without cursor."],
    )
  try:
    padding = "=" * (-len(cursor) % 4)
    decoded = base64.urlsafe_b64decode((cursor + padding).encode("ascii"))
    payload = json.loads(decoded.decode("utf-8"))
  except (ValueError, TypeError, UnicodeError, json.JSONDecodeError, binascii.Error) as exc:
    raise _RequestError(
      "invalid_cursor",
      "cursor is not a valid model_semantics pagination cursor.",
      field="cursor",
      next_actions=["Retry the same model_semantics request without cursor."],
    ) from exc
  if not isinstance(payload, dict) or payload.get("v") != 1:
    raise _RequestError(
      "invalid_cursor",
      "cursor uses an unsupported model_semantics cursor version.",
      field="cursor",
      next_actions=["Retry the same model_semantics request without cursor."],
    )
  if payload.get("h") != full_semantics_hash:
    raise _RequestError(
      "model_semantics_cursor_stale",
      "model semantics changed after this cursor was issued.",
      field="cursor",
      next_actions=["Restart pagination without cursor to read the current model semantics."],
    )
  if payload.get("q") != query_hash:
    raise _RequestError(
      "invalid_cursor",
      "cursor does not match the requested sections, filters, or source_detail.",
      field="cursor",
      next_actions=[
        "Use the cursor with exactly the same sections, filters, and source_detail, or restart without cursor."
      ],
    )
  offset = payload.get("o")
  if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
    raise _RequestError(
      "invalid_cursor",
      "cursor contains an invalid model_semantics entry offset.",
      field="cursor",
      next_actions=["Retry the same model_semantics request without cursor."],
    )
  return offset


def _serialized_chars(payload: Mapping[str, Any]) -> int:
  # Match the ordinary JSON representation used by runner/tool-result paths.
  # Compact separators undercount meaningful whitespace overhead, while
  # ensure_ascii=False undercounts escaped non-ASCII content.
  return len(json.dumps(payload, sort_keys=True, default=str))


class _RequestError(ValueError):
  def __init__(
    self,
    code: str,
    message: str,
    *,
    field: str,
    details: Any | None = None,
    next_actions: list[str] | None = None,
  ):
    super().__init__(message)
    self.code = code
    self.field = field
    self.details = details
    self.next_actions = next_actions


class _IdentityError(ValueError):
  def __init__(
    self,
    code: str,
    message: str,
    *,
    details: Any | None = None,
    recoverable: bool = True,
    next_actions: list[str] | None = None,
  ):
    super().__init__(message)
    self.code = code
    self.details = details
    self.recoverable = recoverable
    self.next_actions = next_actions


def _error_payload(
  code: str,
  message: str,
  *,
  field: str | None = None,
  ticker: str | None = None,
  details: Any | None = None,
  recoverable: bool | None = None,
  next_actions: list[str] | None = None,
  _enforce_budget: bool = True,
) -> dict[str, Any]:
  error: dict[str, Any] = {"code": code, "message": message}
  if field is not None:
    error["field"] = field
  if details is not None:
    error["details"] = details
  if recoverable is not None:
    error["recoverable"] = recoverable
  payload: dict[str, Any] = {"status": "error", "error": error}
  if ticker is not None:
    payload["ticker"] = ticker
  if next_actions:
    payload["next_actions"] = next_actions
  if not _enforce_budget:
    return payload
  return _bounded_error_payload(payload)


def _bounded_error_payload(payload: dict[str, Any]) -> dict[str, Any]:
  response_chars = _serialized_chars(payload)
  if response_chars <= _MODEL_SEMANTICS_MAX_RESPONSE_CHARS:
    return payload

  original_error = payload.get("error")
  error = original_error if isinstance(original_error, Mapping) else {}
  compact_error: dict[str, Any] = {
    "code": str(error.get("code") or "model_semantics_error"),
    "message": (
      "The error response exceeded the model_semantics producer budget; "
      "oversized input values and details were omitted."
    ),
    "details": {
      "response_compacted": True,
      "original_response_chars": response_chars,
      "max_response_chars": _MODEL_SEMANTICS_MAX_RESPONSE_CHARS,
    },
  }
  if error.get("field") is not None:
    compact_error["field"] = str(error["field"])
  if error.get("recoverable") is not None:
    compact_error["recoverable"] = bool(error["recoverable"])
  return {
    "status": "error",
    "error": compact_error,
    "next_actions": [
      "Retry model_semantics with shorter selector and filter values that match the documented input schema."
    ],
  }




__all__ = ["ModelResolutionFailure", "ModelSemanticsDeps", "model_semantics_handler"]
