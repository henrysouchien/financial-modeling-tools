from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from pydantic import ValidationError

from schema.model_build_context import HistoricalSources
from schema.overrides import TickerOverrides
from schema.segments import SEGMENT_AXES_PRIORITY
from schema.tools import model_tool_error_payload

from .args import safe_pydantic_errors


def serialize_year_values(values: dict[int, float] | None) -> dict[str, float]:
  return {
    str(int(year)): float(value)
    for year, value in sorted((values or {}).items())
  }


def model_build_error_payload(
  exc: Exception,
  *,
  model_build_id: str,
  model_tool_error_payload_fn=model_tool_error_payload,
  safe_pydantic_errors_fn=safe_pydantic_errors,
) -> dict:
  payload = model_tool_error_payload_fn(exc)
  payload["model_build_id"] = model_build_id
  if isinstance(exc, ValidationError):
    payload["error_code"] = "validation_error"
    details = dict(payload.get("details") or {})
    details["pydantic_errors"] = safe_pydantic_errors_fn(exc)
    details["source_contract"] = {
      "source": "fmp | edgar",
      "historical_sources": {
        "default_source": "fmp | edgar",
        "default_fallback_enabled": False,
        "overrides": [
          {
            "concept_id": "revenue",
            "preferred": "edgar",
            "fallback_order": ["edgar", "fmp"],
          }
        ],
      },
      "invariant": "For each override, fallback_order[0] must equal preferred.",
    }
    payload["details"] = details
    payload["recovery"] = {
      "next_actions": [
        "Fix the payload to match the source_contract shape and allowed source values.",
        "If routing one concept to EDGAR first, use fallback_order=['edgar', 'fmp']; if routing to FMP first, use ['fmp', 'edgar'].",
        "Retry model_build with the corrected historical_sources payload; do not drop routing silently.",
      ]
    }
  return payload


def axis_priority(
  axis: str | None,
  *,
  axes_priority: tuple[str, ...] | list[str] = SEGMENT_AXES_PRIORITY,
) -> int | None:
  if axis not in axes_priority:
    return None
  return list(axes_priority).index(axis) + 1


def serialize_diagnostic_report(report: Any) -> dict | None:
  if report is None:
    return None
  if is_dataclass(report):
    return asdict(report)
  if isinstance(report, dict):
    return report
  return None


def serialize_ticker_overrides(overrides: TickerOverrides | None) -> dict:
  if overrides is None:
    return {
      "_meta": {},
      "overrides": {},
      "custom_concepts": {},
    }
  return {
    "_meta": dict(overrides.file_meta or {}),
    "overrides": dict(overrides.overrides or {}),
    "custom_concepts": dict(overrides.custom_concepts or {}),
  }


def serialize_overrides_write_report(report: Any) -> dict:
  payload = asdict(report)
  if payload.get("output_path") is not None:
    payload["output_path"] = str(payload["output_path"])
  payload["nodes_skipped_invalid_source"] = [
    list(item) for item in payload.get("nodes_skipped_invalid_source", [])
  ]
  return payload


def rebuild_summary(rebuild_result: dict) -> dict:
  return {
    "items_populated": rebuild_result.get("items_populated"),
    "edgar_errors": rebuild_result.get("edgar_errors"),
  }


def historical_sources_touch_edgar(historical_sources: HistoricalSources | None) -> bool:
  if historical_sources is None:
    return False
  if historical_sources.default_source == "edgar":
    return True
  if getattr(historical_sources, "default_fallback_enabled", False):
    return True
  return any(
    override.preferred == "edgar" or "edgar" in override.fallback_order
    for override in historical_sources.overrides
  )


def historical_sources_touch_fmp(historical_sources: HistoricalSources | None) -> bool:
  if historical_sources is None:
    return False
  if historical_sources.default_source == "fmp":
    return True
  if getattr(historical_sources, "default_fallback_enabled", False):
    return True
  return any(
    override.preferred == "fmp" or "fmp" in override.fallback_order
    for override in historical_sources.overrides
  )


__all__ = [
  "axis_priority",
  "historical_sources_touch_edgar",
  "historical_sources_touch_fmp",
  "model_build_error_payload",
  "rebuild_summary",
  "serialize_diagnostic_report",
  "serialize_overrides_write_report",
  "serialize_ticker_overrides",
  "serialize_year_values",
]
