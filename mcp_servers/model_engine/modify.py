from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ModelModifyDeps:
  coerce_json_list_arg: Callable[..., list[Any]]
  current_year: Callable[[], int]
  model_cache: Any
  normalize_modify_operation_payload: Callable[[Any, Any], Any]
  operation_cls: Any
  apply_modify_request: Callable[..., Any]
  modify_error_type: type[Exception]


def normalize_modify_operation_payload(operation: Any, model: Any = None) -> Any:
  if not isinstance(operation, dict):
    return operation
  normalized = dict(operation)
  if "type" not in normalized and "op" in normalized:
    normalized["type"] = normalized["op"]
  if normalized.get("type") == "set_value" and "values" not in normalized and "periods" in normalized:
    normalized["values"] = normalized["periods"]
  if normalized.get("type") == "set_value" and "values" not in normalized and "period_values" in normalized:
    normalized["values"] = normalized["period_values"]
  if (
    normalized.get("type") == "set_value"
    and normalized.get("item_id")
    and model is not None
    and "values" not in normalized
    and "period" in normalized
  ):
    try:
      item = model.get_item(normalized["item_id"])
    except (KeyError, AttributeError):
      item = None
    if item is not None and getattr(item, "column", None) is not None:
      # Fixed-cell row: schema will use op.value with the cell's anchor period.
      # Stray period is ignored by Pydantic's default extra-field behavior.
      return normalized
  if (
    normalized.get("type") == "set_value"
    and "values" not in normalized
    and "period" in normalized
    and "value" in normalized
  ):
    normalized["values"] = {str(normalized["period"]): normalized["value"]}
    normalized.pop("value", None)
  return normalized


def model_modify_handler(
  *,
  deps: ModelModifyDeps,
  file_path: str,
  operations: list[dict] | str,
  target: str = "file",
  conflict_strategy: str = "fail_on_collision",
  force_overwrite: bool = False,
  best_effort: bool = False,
  historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
  try:
    parsed_operations = deps.coerce_json_list_arg(operations, name="operations")
    cutoff = (
      historical_cutoff_year
      if historical_cutoff_year is not None
      else deps.current_year()
    )
    bundle = deps.model_cache.get((file_path, cutoff))
    model_for_normalize = bundle.model if bundle is not None else None
    parsed_ops = [
      deps.operation_cls.model_validate(
        deps.normalize_modify_operation_payload(op, model_for_normalize)
      )
      for op in parsed_operations
    ]
  except Exception as exc:
    return {
      "status": "error",
      "error": f"invalid operation: {exc}",
      "error_type": "validation",
    }

  try:
    result = deps.apply_modify_request(
      file_path=file_path,
      operations=parsed_ops,
      target=target,
      conflict_strategy=conflict_strategy,
      force_overwrite=force_overwrite,
      best_effort=best_effort,
      historical_cutoff_year=historical_cutoff_year,
    )
  except deps.modify_error_type as exc:
    return {"status": "error", "error": str(exc), "error_type": "cache_miss"}
  except Exception as exc:
    return {"status": "error", "error": str(exc), "error_type": type(exc).__name__}

  return {"status": "ok", **result.model_dump()}


__all__ = [
  "ModelModifyDeps",
  "model_modify_handler",
  "normalize_modify_operation_payload",
]
