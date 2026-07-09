from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any

import yaml

from .model_build_context_errors import InvalidDriverKey, UnsupportedInSegmentMode
from .models import FinancialModel, ItemType, LineItem, SheetLayout
from .renderer import AbsoluteColumnMapper
from .segments import SegmentInfo, SegmentProfile, expand_segments
from .templates.template_builder import load_sia_generic_template

if TYPE_CHECKING:
  from .business_model_compiler import CompiledDriverRegistry


DRIVER_MAPPING_PATH = Path(__file__).resolve().parent / "templates" / "driver_mapping.yaml"
RAW_PREFIX = "raw:"

_CATEGORY_A_ROLES = frozenset({"volume_growth", "price_growth"})
_SEGMENT_DRIVER_KEY_RE = re.compile(r"^revenue\.segment_(\d+)\.([a-z_]+)$")
_CANONICAL_SEGMENT_ITEM_RE = re.compile(r"^tpl\.a\.revenue_drivers\.business_segment_(\d+)_([a-z0-9_]+)$")
_DEFAULT_TEMPLATE_CACHE_KEY = "__default__"


def _projection_periods(model: FinancialModel) -> list[int]:
  periods = model.time_structure.projection_periods or model.time_structure.projection_years
  return [int(period) for period in periods]


def _iter_items(model: FinancialModel):
  for sheet in model.sheets.values():
    for section in sheet.sections:
      for item in section.line_items:
        yield sheet.name, item


def _find_item(model: FinancialModel, item_id: str) -> tuple[str, LineItem]:
  if not model._index:
    model.build_index()
  item = model.get_item(item_id)
  for sheet_name, candidate in _iter_items(model):
    if candidate.id == item.id:
      return sheet_name, candidate
  raise KeyError(f"Item not found in rendered model: {item_id}")


def _template_cache_key(template_path: str | Path | None = None) -> str:
  if template_path is None:
    return _DEFAULT_TEMPLATE_CACHE_KEY
  return str(Path(template_path).resolve())


@lru_cache(maxsize=1)
def _load_driver_mapping_payload() -> dict[str, Any]:
  payload = yaml.safe_load(DRIVER_MAPPING_PATH.read_text(encoding="utf-8")) or {}
  if not isinstance(payload, dict):
    raise ValueError("driver_mapping.yaml must define a top-level mapping object")
  return payload


@lru_cache(maxsize=8)
def _load_base_template_model(template_key: str = _DEFAULT_TEMPLATE_CACHE_KEY) -> FinancialModel:
  if template_key == _DEFAULT_TEMPLATE_CACHE_KEY:
    model = load_sia_generic_template()
  else:
    model = FinancialModel.model_validate_json(Path(template_key).read_text(encoding="utf-8"))
  model.build_index()
  return model


def _synthetic_segment_profile(segment_count: int) -> SegmentProfile:
  if segment_count < 1:
    raise ValueError("segment_count must be >= 1")
  return SegmentProfile(
    ticker="TEMPLATE",
    segments=[SegmentInfo(name=f"Segment {index}") for index in range(1, segment_count + 1)],
    source="caller_override",
  )


@lru_cache(maxsize=32)
def _load_segment_expanded_template(
  segment_count: int,
  template_key: str = _DEFAULT_TEMPLATE_CACHE_KEY,
) -> FinancialModel:
  model = _load_base_template_model(template_key).model_copy(deep=True)
  expand_segments(model, _synthetic_segment_profile(segment_count))
  model.build_index()
  return model


def _validation_model_for_item_id(
  item_id: str,
  template_path: str | Path | None = None,
) -> FinancialModel:
  match = _CANONICAL_SEGMENT_ITEM_RE.match(item_id)
  template_key = _template_cache_key(template_path)
  if not match:
    return _load_base_template_model(template_key)
  return _load_segment_expanded_template(int(match.group(1)), template_key)


def _find_validation_item(
  item_id: str,
  template_path: str | Path | None = None,
) -> tuple[str, LineItem]:
  return _find_item(_validation_model_for_item_id(item_id, template_path), item_id)


@lru_cache(maxsize=1)
def load_driver_mapping() -> dict[str, str]:
  payload = _load_driver_mapping_payload()
  mappings = payload.get("mappings") if isinstance(payload, dict) else None
  if not isinstance(mappings, dict):
    raise ValueError("driver_mapping.yaml must define a top-level 'mappings' object")
  normalized = {
    str(key).strip(): str(value).strip()
    for key, value in mappings.items()
    if str(key).strip() and str(value).strip()
  }
  _validate_mapping(normalized)
  return normalized


@lru_cache(maxsize=1)
def load_unsupported_in_segment_mode() -> tuple[tuple[str, str], ...]:
  payload = _load_driver_mapping_payload()
  entries = payload.get("unsupported_in_segment_mode", []) if isinstance(payload, dict) else []
  if not isinstance(entries, list):
    raise ValueError("driver_mapping.yaml unsupported_in_segment_mode must be a list")

  normalized: list[tuple[str, str]] = []
  for entry in entries:
    if not isinstance(entry, dict):
      raise ValueError("unsupported_in_segment_mode entries must be objects with key and reason")
    key = str(entry.get("key") or "").strip()
    reason = str(entry.get("reason") or "").strip()
    if not key or not reason:
      raise ValueError("unsupported_in_segment_mode entries must define non-empty key and reason")
    normalized.append((key, reason))
  return tuple(normalized)


@lru_cache(maxsize=1)
def load_scale_correction_rules() -> dict[str, dict[str, str]]:
  payload = _load_driver_mapping_payload()
  entries = payload.get("scale_correction_rules", []) if isinstance(payload, dict) else []
  if not isinstance(entries, list):
    raise ValueError("driver_mapping.yaml scale_correction_rules must be a list")

  rules: dict[str, dict[str, str]] = {}
  mapping = load_driver_mapping()
  for entry in entries:
    if not isinstance(entry, dict):
      raise ValueError("scale_correction_rules entries must be objects")
    key = str(entry.get("key") or "").strip()
    target_item_id = str(entry.get("target_item_id") or "").strip()
    comparator_item_id = str(entry.get("comparator_item_id") or "").strip()
    reason = str(entry.get("reason") or "").strip()
    if not key or not target_item_id or not comparator_item_id:
      raise ValueError("scale_correction_rules entries must define key, target_item_id, and comparator_item_id")
    if mapping.get(key) != target_item_id:
      raise ValueError(f"scale_correction_rules target mismatch for {key}: {target_item_id}")
    try:
      _find_validation_item(target_item_id)
      _find_validation_item(comparator_item_id)
    except KeyError as exc:
      raise ValueError(f"scale_correction_rules row missing for {key}: {exc}") from exc
    rules[key] = {
      "key": key,
      "target_item_id": target_item_id,
      "comparator_item_id": comparator_item_id,
      "reason": reason,
    }
  return rules


def _validate_mapping(mapping: dict[str, str], template_path: str | Path | None = None) -> None:
  violations: list[str] = []
  for driver_key, item_id in mapping.items():
    try:
      _, item = _find_validation_item(item_id, template_path)
    except KeyError:
      violations.append(f"{driver_key} -> {item_id} (missing)")
      continue
    if item.item_type != ItemType.input:
      violations.append(f"{driver_key} -> {item_id} ({item.item_type.value})")

  if violations:
    joined = ", ".join(violations)
    raise ValueError(f"Driver mapping targets must resolve to input items: {joined}")


def _unsupported_category(pattern_key: str) -> str:
  if pattern_key.endswith(".operating_metric"):
    return "B1"
  if pattern_key.endswith(".revenue"):
    return "B2"
  raise ValueError(
    "unsupported_in_segment_mode keys must end with '.operating_metric' or '.revenue'"
  )


@lru_cache(maxsize=16)
def _compile_unsupported_pattern(pattern_key: str) -> re.Pattern[str]:
  escaped = re.escape(pattern_key)
  marker = re.escape("segment_N")
  if marker not in escaped:
    raise ValueError("unsupported_in_segment_mode keys must include literal segment_N marker")
  return re.compile("^" + escaped.replace(marker, r"segment_(\d+)") + "$")


@lru_cache(maxsize=1)
def _compiled_unsupported_in_segment_mode() -> tuple[tuple[re.Pattern[str], str, str], ...]:
  compiled: list[tuple[re.Pattern[str], str, str]] = []
  for pattern_key, reason in load_unsupported_in_segment_mode():
    compiled.append((_compile_unsupported_pattern(pattern_key), reason, _unsupported_category(pattern_key)))
  return tuple(compiled)


def _resolve_dynamic_segment_key(driver_key: str) -> str | None:
  match = _SEGMENT_DRIVER_KEY_RE.match(driver_key)
  if not match:
    return None

  segment_index = int(match.group(1))
  role = match.group(2)
  if role not in _CATEGORY_A_ROLES:
    return None

  item_id = f"tpl.a.revenue_drivers.business_segment_{segment_index}_{role}"
  try:
    _, item = _find_validation_item(item_id)
  except KeyError as exc:
    raise InvalidDriverKey(driver_key, f"resolved target missing from template ({item_id})") from exc
  if item.item_type != ItemType.input:
    raise InvalidDriverKey(
      driver_key,
      f"resolved target is not ItemType.input ({item.item_type.value})",
    )
  return item_id


def resolve_driver_key(
  driver_key: str,
  compiled_registry: "CompiledDriverRegistry | None" = None,
) -> str:
  normalized = str(driver_key or "").strip()
  if not normalized:
    raise KeyError("driver_key is required")

  for pattern, reason, category in _compiled_unsupported_in_segment_mode():
    if pattern.fullmatch(normalized):
      raise UnsupportedInSegmentMode(normalized, category=category, reason=reason)

  if compiled_registry and normalized in compiled_registry.driver_keys:
    return compiled_registry.driver_keys[normalized]

  mapping = load_driver_mapping()
  if normalized in mapping:
    return mapping[normalized]

  dynamic_item_id = _resolve_dynamic_segment_key(normalized)
  if dynamic_item_id is not None:
    return dynamic_item_id

  if normalized.startswith(RAW_PREFIX):
    literal = normalized[len(RAW_PREFIX):].strip()
    if not literal:
      raise KeyError("raw: driver keys must include a literal item_id")
    return literal

  raise InvalidDriverKey(normalized, "not in driver_mapping.yaml")


def resolve_driver_cells(
  model: FinancialModel,
  driver_key: str,
  periods: list[int] | None = None,
  compiled_registry: "CompiledDriverRegistry | None" = None,
) -> list[tuple[str, str, int]]:
  item_id = resolve_driver_key(driver_key, compiled_registry=compiled_registry)
  sheet_name, item = _find_item(model, item_id)
  if item.item_type != ItemType.input:
    raise ValueError(f"Driver target must resolve to an input item: {item_id}")

  sheet = model.sheets[sheet_name]
  layout = sheet.layout or SheetLayout(label_column="A", first_data_column="B")
  mapper = AbsoluteColumnMapper(
    model.time_structure,
    (layout.first_data_column or "B").upper(),
    period_scope=layout.period_scope,
  )
  target_periods = [int(period) for period in (periods or _projection_periods(model))]

  resolved: list[tuple[str, str, int]] = []
  for period in target_periods:
    column = item.column.upper() if item.column is not None else mapper.col_for_period(int(period))
    resolved.append((sheet_name, f"{column}{item.row}", int(period)))
  return resolved


__all__ = [
  "DRIVER_MAPPING_PATH",
  "RAW_PREFIX",
  "load_driver_mapping",
  "load_scale_correction_rules",
  "load_unsupported_in_segment_mode",
  "resolve_driver_cells",
  "resolve_driver_key",
  "_validate_mapping",
]
