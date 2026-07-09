from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from typing import Any, Optional, get_type_hints

from schema.model_writer_lock import model_writer_lock
from schema.overrides import overrides_lock_target


MODEL_OVERRIDE_ACTIONS = {
  "get",
  "set",
  "add_custom",
  "remove",
  "list",
  "set_projection",
  "get_projection",
  "delete_projection",
  "list_projections",
  "prune_projections",
}


@dataclass(frozen=True)
class ModelOverrideDeps:
  list_ticker_overrides: Callable[[], Iterable[str]]
  load_ticker_overrides: Callable[[str], Any]
  save_ticker_overrides: Callable[[Any], None]
  serialize_ticker_overrides: Callable[[Any], dict[str, Any] | None]
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  coerce_json_list_arg: Callable[..., list[Any]]
  normalize_projection_entry_payload: Callable[..., dict[str, Any]]
  projection_entry_cls: Any
  projection_entry_validation_payload: Callable[..., dict[str, Any]]
  validation_error_type: type[Exception]
  ticker_override_lock: Callable[[str], Any]
  ticker_overrides_cls: Any
  merge_projection_scenarios: Callable[..., dict[str, Any]]
  get_projection_data: Callable[..., Any]
  delete_projection_entry: Callable[..., dict[str, Any]]
  projection_summaries: Callable[[dict | None], list[dict[str, Any]]]
  prune_projection_scenarios: Callable[..., dict[str, Any]]
  merge_override_fields: Callable[..., dict[str, Any]]
  custom_concept_entry: Callable[[dict], dict[str, Any]]
  remove_override_concept: Callable[..., dict[str, Any]]


def normalize_model_override_action(action: Any) -> str:
  return str(action or "").strip().lower()


def normalize_model_override_ticker(ticker: Any) -> str:
  return str(ticker or "").strip().upper()


def unsupported_model_override_action_payload(
  *,
  action: Any,
  ticker: Any,
) -> dict[str, Any]:
  return {
    "status": "error",
    "ticker": str(ticker or "").upper(),
    "error": f"Unsupported action: {action!r}",
  }


def model_override_list_payload(
  *,
  ticker: Any,
  tickers: Iterable[str],
) -> dict[str, Any]:
  return {
    "status": "success",
    "ticker": str(ticker or "").upper(),
    "tickers": list(tickers),
  }


def model_override_star_action_error_payload() -> dict[str, Any]:
  return {
    "status": "error",
    "ticker": "*",
    "error": "ticker='*' is only supported for action='get'",
  }


def model_override_star_get_payload(
  *,
  tickers: Iterable[str],
  load_ticker_overrides_fn: Callable[[str], Any],
  serialize_ticker_overrides_fn: Callable[[Any], dict[str, Any] | None],
) -> dict[str, Any]:
  return {
    "status": "success",
    "ticker": "*",
    "data": {
      ticker: serialize_ticker_overrides_fn(load_ticker_overrides_fn(ticker))
      for ticker in tickers
    },
  }


def ticker_required_payload(ticker_upper: str) -> dict[str, Any]:
  return {"status": "error", "ticker": ticker_upper, "error": "ticker is required"}


def model_override_get_payload(
  *,
  ticker_upper: str,
  ticker_overrides: Any,
  serialize_ticker_overrides_fn: Callable[[Any], dict[str, Any] | None],
) -> dict[str, Any]:
  return {
    "status": "success",
    "ticker": ticker_upper,
    "data": serialize_ticker_overrides_fn(ticker_overrides),
  }


def concept_required_payload(ticker_upper: str) -> dict[str, Any]:
  return {
    "status": "error",
    "ticker": ticker_upper,
    "error": "concept_id is required",
  }


def concept_result_payload(
  *,
  ticker_upper: str,
  concept_key: str,
  status: str = "success",
) -> dict[str, Any]:
  return {
    "status": status,
    "ticker": ticker_upper,
    "concept_id": concept_key,
  }


def model_override_handler(
  *,
  deps: ModelOverrideDeps,
  ticker: str,
  action: str = "get",
  concept_id: str | None = None,
  override_fields: dict | None = None,
  custom_concept: dict | None = None,
  rate_key: str | None = None,
  entry: dict | None = None,
  expected_scenarios: list[str] | None = None,
  source_skill: str | None = None,
  scenario: str | None = None,
  keep_rate_keys: list[str] | None = None,
  stale_rate_keys: list[str] | None = None,
) -> dict[str, Any]:
  try:
    normalized_action = normalize_model_override_action(action)
    if normalized_action not in MODEL_OVERRIDE_ACTIONS:
      return unsupported_model_override_action_payload(
        action=action,
        ticker=ticker,
      )

    if normalized_action == "list":
      return model_override_list_payload(
        ticker=ticker,
        tickers=deps.list_ticker_overrides(),
      )

    if ticker == "*":
      if normalized_action != "get":
        return model_override_star_action_error_payload()
      return model_override_star_get_payload(
        tickers=deps.list_ticker_overrides(),
        load_ticker_overrides_fn=deps.load_ticker_overrides,
        serialize_ticker_overrides_fn=deps.serialize_ticker_overrides,
      )

    ticker_upper = normalize_model_override_ticker(ticker)
    if not ticker_upper:
      return ticker_required_payload(ticker_upper)

    if normalized_action == "get":
      return model_override_get_payload(
        ticker_upper=ticker_upper,
        ticker_overrides=deps.load_ticker_overrides(ticker_upper),
        serialize_ticker_overrides_fn=deps.serialize_ticker_overrides,
      )

    if normalized_action == "set_projection":
      if not isinstance(rate_key, str) or not rate_key.strip():
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "rate_key required for action='set_projection'",
        }
      projection_rate_key = rate_key.strip()
      if entry is None:
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "entry (dict) required for action='set_projection'",
        }
      try:
        entry_payload = deps.coerce_json_dict_arg(entry, name="entry")
      except ValueError as exc:
        return {"status": "error", "ticker": ticker_upper, "error": str(exc)}
      try:
        entry_payload = deps.normalize_projection_entry_payload(
          entry_payload,
          default_scenario=scenario,
        )
        validated_entry = deps.projection_entry_cls.model_validate(entry_payload)
      except deps.validation_error_type as exc:
        return deps.projection_entry_validation_payload(
          ticker=ticker_upper,
          rate_key=projection_rate_key,
          exc=exc,
        )
      except ValueError as exc:
        return {"status": "error", "ticker": ticker_upper, "error": str(exc)}

      expected_scenarios_payload = None
      if expected_scenarios is not None:
        try:
          expected_scenarios_payload = deps.coerce_json_list_arg(
            expected_scenarios,
            name="expected_scenarios",
          )
        except ValueError as exc:
          return {"status": "error", "ticker": ticker_upper, "error": str(exc)}
        provided = set(validated_entry.scenarios.keys())
        expected = {str(scenario) for scenario in expected_scenarios_payload}
        if provided != expected:
          return {
            "status": "error",
            "ticker": ticker_upper,
            "error": (
              f"scenario mismatch: entry has {sorted(provided)}, "
              f"expected {sorted(expected)}"
            ),
          }

      with model_writer_lock(overrides_lock_target(ticker_upper), ticker=ticker_upper):
        with deps.ticker_override_lock(ticker_upper):
          current = deps.load_ticker_overrides(ticker_upper) or deps.ticker_overrides_cls(
            ticker=ticker_upper,
            overrides={},
            custom_concepts={},
            file_meta={"ticker": ticker_upper, "schema_version": "1"},
          )
          if current.projections is None:
            current.projections = {}

          merge_result = deps.merge_projection_scenarios(
            existing_projection=current.projections.get(
              projection_rate_key,
              {"scenarios": {}},
            ),
            new_scenarios=validated_entry.scenarios,
            file_meta=current.file_meta,
          )
          current.projections[projection_rate_key] = merge_result["projection"]
          current.file_meta = merge_result["file_meta"]

          deps.save_ticker_overrides(current)

      return {
        "status": "success",
        "ticker": ticker_upper,
        "rate_key": projection_rate_key,
        "scenarios_written": merge_result["scenarios_written"],
        "schema_version_bumped": merge_result["schema_version_bumped"],
        "cross_skill_notes": merge_result["cross_skill_notes"],
      }

    if normalized_action == "get_projection":
      with deps.ticker_override_lock(ticker_upper):
        current = deps.load_ticker_overrides(ticker_upper)
        if rate_key is None:
          return {
            "status": "success",
            "ticker": ticker_upper,
            "data": deps.get_projection_data(
              current.projections if current is not None else None
            ),
          }
        projection_rate_key = str(rate_key)
        return {
          "status": "success",
          "ticker": ticker_upper,
          "rate_key": projection_rate_key,
          "data": deps.get_projection_data(
            current.projections if current is not None else None,
            rate_key=projection_rate_key,
          ),
        }

    if normalized_action == "delete_projection":
      if not isinstance(rate_key, str) or not rate_key.strip():
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "rate_key required for action='delete_projection'",
        }
      projection_rate_key = rate_key.strip()
      with model_writer_lock(overrides_lock_target(ticker_upper), ticker=ticker_upper):
        with deps.ticker_override_lock(ticker_upper):
          current = deps.load_ticker_overrides(ticker_upper)
          delete_result = deps.delete_projection_entry(
            current.projections if current is not None else None,
            rate_key=projection_rate_key,
          )
          if current is not None and delete_result["deleted"]:
            current.projections = delete_result["updated_projections"]
            deps.save_ticker_overrides(current)
      return {
        "status": "success",
        "ticker": ticker_upper,
        "rate_key": projection_rate_key,
        "deleted": delete_result["deleted"],
      }

    if normalized_action == "list_projections":
      with deps.ticker_override_lock(ticker_upper):
        current = deps.load_ticker_overrides(ticker_upper)
        if current is None:
          return {"status": "success", "ticker": ticker_upper, "projections": []}
        summaries = deps.projection_summaries(current.projections)
        return {
          "status": "success",
          "ticker": ticker_upper,
          "projections": summaries,
        }

    if normalized_action == "prune_projections":
      normalized_source_skill = str(source_skill or "").strip()
      if not normalized_source_skill:
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "source_skill required for action='prune_projections'",
        }
      if keep_rate_keys is None and stale_rate_keys is None:
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "keep_rate_keys or stale_rate_keys required for action='prune_projections'",
        }
      if keep_rate_keys is not None and stale_rate_keys is not None:
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "provide either keep_rate_keys or stale_rate_keys for action='prune_projections', not both",
        }
      if keep_rate_keys is not None:
        try:
          keep_rate_keys_payload = deps.coerce_json_list_arg(
            keep_rate_keys,
            name="keep_rate_keys",
          )
        except ValueError as exc:
          return {"status": "error", "ticker": ticker_upper, "error": str(exc)}
      else:
        keep_rate_keys_payload = []
      keep_set = {str(key).strip() for key in keep_rate_keys_payload if str(key).strip()}
      if stale_rate_keys is not None:
        try:
          stale_rate_keys_payload = deps.coerce_json_list_arg(
            stale_rate_keys,
            name="stale_rate_keys",
          )
        except ValueError as exc:
          return {"status": "error", "ticker": ticker_upper, "error": str(exc)}
        stale_set = {str(key).strip() for key in stale_rate_keys_payload if str(key).strip()}
      else:
        stale_set = None
      normalized_scenario = str(scenario).strip() if scenario is not None else None
      if normalized_scenario == "":
        normalized_scenario = None
      if normalized_scenario is not None and normalized_scenario not in {"base", "bull", "bear"}:
        return {
          "status": "error",
          "ticker": ticker_upper,
          "error": "scenario must be one of base, bull, bear",
        }

      deleted_scenarios: list[dict[str, str]] = []
      deleted_rate_keys: list[str] = []
      with model_writer_lock(overrides_lock_target(ticker_upper), ticker=ticker_upper):
        with deps.ticker_override_lock(ticker_upper):
          current = deps.load_ticker_overrides(ticker_upper)
          if current is None or not current.projections:
            return {
              "status": "success",
              "ticker": ticker_upper,
              "source_skill": normalized_source_skill,
              "scenario": normalized_scenario,
              "keep_rate_keys": sorted(keep_set),
              **({"stale_rate_keys": sorted(stale_set)} if stale_set is not None else {}),
              **({"cleanup_mode": "explicit_stale_keys"} if stale_set is not None else {}),
              "deleted_scenarios": [],
              "deleted_rate_keys": [],
            }

          prune_result = deps.prune_projection_scenarios(
            current.projections,
            source_skill=normalized_source_skill,
            keep_rate_keys=keep_set,
            stale_rate_keys=stale_set,
            scenario=normalized_scenario,
          )
          deleted_scenarios = prune_result["deleted_scenarios"]
          deleted_rate_keys = prune_result["deleted_rate_keys"]
          if prune_result["changed"]:
            current.projections = prune_result["updated_projections"]
            deps.save_ticker_overrides(current)

      return {
        "status": "success",
        "ticker": ticker_upper,
        "source_skill": normalized_source_skill,
        "scenario": normalized_scenario,
        "keep_rate_keys": sorted(keep_set),
        **({"stale_rate_keys": sorted(stale_set)} if stale_set is not None else {}),
        **({"cleanup_mode": "explicit_stale_keys"} if stale_set is not None else {}),
        "deleted_scenarios": deleted_scenarios,
        "deleted_rate_keys": deleted_rate_keys,
      }

    if not concept_id:
      return concept_required_payload(ticker_upper)

    with model_writer_lock(overrides_lock_target(ticker_upper), ticker=ticker_upper):
      current = deps.load_ticker_overrides(ticker_upper) or deps.ticker_overrides_cls(
        ticker=ticker_upper,
        overrides={},
        custom_concepts={},
        file_meta={"ticker": ticker_upper, "schema_version": "1"},
      )
      concept_key = str(concept_id)

      if normalized_action == "set":
        if override_fields is None:
          return {
            "status": "error",
            "ticker": ticker_upper,
            "error": "override_fields is required for action='set'",
          }
        try:
          override_fields_payload = deps.coerce_json_dict_arg(
            override_fields,
            name="override_fields",
          )
        except ValueError as exc:
          return {"status": "error", "ticker": ticker_upper, "error": str(exc)}
        current.overrides[concept_key] = deps.merge_override_fields(
          current.overrides,
          concept_key=concept_key,
          override_fields=override_fields_payload,
        )
        deps.save_ticker_overrides(current)
        return concept_result_payload(
          ticker_upper=ticker_upper,
          concept_key=concept_key,
        )

      if normalized_action == "add_custom":
        if custom_concept is None:
          return {
            "status": "error",
            "ticker": ticker_upper,
            "error": "custom_concept is required for action='add_custom'",
          }
        try:
          custom_concept_payload = deps.coerce_json_dict_arg(
            custom_concept,
            name="custom_concept",
          )
        except ValueError as exc:
          return {"status": "error", "ticker": ticker_upper, "error": str(exc)}
        current.custom_concepts[concept_key] = deps.custom_concept_entry(
          custom_concept_payload
        )
        deps.save_ticker_overrides(current)
        return concept_result_payload(
          ticker_upper=ticker_upper,
          concept_key=concept_key,
        )

      if normalized_action == "remove":
        remove_result = deps.remove_override_concept(
          current.overrides,
          current.custom_concepts,
          concept_key=concept_key,
        )
        if not remove_result["removed"]:
          return concept_result_payload(
            ticker_upper=ticker_upper,
            concept_key=concept_key,
            status="no_changes",
          )
        current.overrides = remove_result["overrides"]
        current.custom_concepts = remove_result["custom_concepts"]
        deps.save_ticker_overrides(current)
        return concept_result_payload(
          ticker_upper=ticker_upper,
          concept_key=concept_key,
        )

    return unsupported_model_override_action_payload(
      action=action,
      ticker=ticker_upper,
    )
  except Exception as exc:
    return {"status": "error", "ticker": str(ticker or "").upper(), "error": str(exc)}


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelOverrideToolFunctions:
  model_override: Callable[..., dict[str, Any]]


def _parent_model_override_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelOverrideDeps:
  return parent_namespace()["_model_override_deps"]()


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


def register_model_override_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ModelOverrideToolFunctions | None = None,
) -> ModelOverrideToolFunctions:
  functions = functions or build_model_override_tool_functions(
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


def build_model_override_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ModelOverrideToolFunctions:
  def model_override(
    ticker: str,
    action: str = "get",
    concept_id: Optional[str] = None,
    override_fields: Optional[dict] = None,
    custom_concept: Optional[dict] = None,
    rate_key: Optional[str] = None,
    entry: Optional[dict] = None,
    expected_scenarios: Optional[list[str]] = None,
    source_skill: Optional[str] = None,
    scenario: Optional[str] = None,
    keep_rate_keys: Optional[list[str]] = None,
    stale_rate_keys: Optional[list[str]] = None,
  ) -> dict:
    """Manage per-ticker model override entries.

    Discovery: use action="list" to discover tickers with overrides, action="get"
    for a specific ticker or ticker="*" for all overrides, then pass concept_id
    from the returned data or model_find.

    v1 actions (unchanged): get, list, set, add_custom, remove.
    v2 actions:
      - set_projection: requires rate_key + entry. Optionally expected_scenarios.
      - get_projection: rate_key optional (returns one or all projections).
      - delete_projection: requires rate_key.
      - list_projections: returns all rate_keys with provenance summary.
      - prune_projections: requires source_skill plus either keep_rate_keys
        (complete-run cleanup) or stale_rate_keys (explicit scoped cleanup).
        Optionally scenario. Removes stale scenarios from that source while
        preserving other source/scenario entries under the same rate_key.

    Projection ops are wrapped in _ticker_override_lock. Writes use
    save_ticker_overrides, whose temp+rename+fsync atomicity lives in the schema
    layer.
    """
    return _parent_handler(parent_namespace, "_model_override_handler")(
      deps=_parent_model_override_deps(parent_namespace),
      ticker=ticker,
      action=action,
      concept_id=concept_id,
      override_fields=override_fields,
      custom_concept=custom_concept,
      rate_key=rate_key,
      entry=entry,
      expected_scenarios=expected_scenarios,
      source_skill=source_skill,
      scenario=scenario,
      keep_rate_keys=keep_rate_keys,
      stale_rate_keys=stale_rate_keys,
    )

  return ModelOverrideToolFunctions(model_override=model_override)


__all__ = [
  "MODEL_OVERRIDE_ACTIONS",
  "ModelOverrideDeps",
  "ModelOverrideToolFunctions",
  "build_model_override_tool_functions",
  "concept_required_payload",
  "concept_result_payload",
  "model_override_handler",
  "model_override_get_payload",
  "model_override_list_payload",
  "model_override_star_action_error_payload",
  "model_override_star_get_payload",
  "normalize_model_override_action",
  "normalize_model_override_ticker",
  "register_model_override_tools",
  "ticker_required_payload",
  "unsupported_model_override_action_payload",
]
