from __future__ import annotations

from pydantic import ValidationError

from schema.scenario_bridge import BridgeWarning

from .args import safe_pydantic_errors


def projection_entry_validation_payload(
  *,
  ticker: str,
  rate_key: str,
  exc: ValidationError,
) -> dict:
  return {
    "status": "error",
    "ticker": ticker,
    "rate_key": rate_key,
    "error": f"entry validation failed: {exc}",
    "error_code": "projection_entry_validation_error",
    "details": {
      "pydantic_errors": safe_pydantic_errors(exc),
      "entry_contract": {
        "scenarios": {
          "base|bull|bear": {
            "values": {"2026": 12.0},
            "value_scale": "display | model (default display; use model when values are already in internal workbook units such as 0.12 for 12%)",
            "rationale": "source-backed reason for this projection curve",
            "confidence": "low | medium | high",
            "held_at_base": False,
            "source_refs_by_period": {"2026": ["src_1"]},
            "sources": [
              {
                "id": "src_1",
                "type": "filing | transcript | investor_deck | other",
                "source_id": "stable external/source identifier",
                "text": "source descriptor or excerpt summary",
              }
            ],
            "provenance": {
              "source_skill": "forecast-assumptions",
              "skill_run_id": "run id from the writing skill",
              "written_at": "ISO timestamp",
              "model_build_id": "optional current model_build_id",
            },
          }
        }
      },
      "source_ref_invariant": (
        "source_refs_by_period values must be canonical src_N ids present in the "
        "same scenario's sources[] block; source records require id, type, "
        "source_id, and text."
      ),
    },
    "recovery": {
      "next_actions": [
        "Use entry.scenarios.<case>.provenance, not evidence, to identify the writing skill/run/model build.",
        "Use 4-digit fiscal-year keys in values, e.g. {'2026': 12.0}; do not use FY2026/FY1 labels.",
        "For percentage rows, either pass display values such as 12.0 with value_scale='display' or internal model values such as 0.12 with value_scale='model'; do not leave the scale ambiguous.",
        "If source_refs_by_period is present, include matching SourceRecord objects in sources[] with ids like src_1.",
        "Retry model_override(action='set_projection') with the repaired entry; do not drop provenance or source refs silently.",
      ]
    },
  }


def anchor_recovery(payload: dict) -> dict | None:
  match_reason = payload.get("match_reason")
  if match_reason not in {"invalid_hint", "unresolved", "label_match_low_confidence"}:
    return None
  candidates = payload.get("candidates") or []
  return {
    "candidate_hints": candidates,
    "next_actions": [
      "Call model_find_scenario_anchor again with hint set to one of candidate_hints after confirming it is the intended factor owner.",
      "If candidate_hints is empty, call model_find(file_path=..., query='<driver label>') or model_summarize(include_items=True) to inspect scenario owner rows.",
      "Pass factor_anchor_hints={factor: owner_id} into model_bridge_scenarios once the owner id is confirmed.",
    ],
  }


def bridge_recovery(reason: str | None) -> dict | None:
  if reason == "model_not_in_cache":
    return {
      "next_actions": [
        "Call model_summarize(file_path=...) or model_find(file_path=..., query='revenue') to verify the workbook and sidecar can be loaded.",
        "If the file was rebuilt or moved, use the latest model_build output_path and rerun the bridge.",
        "If no sidecar exists, rerun the orchestrated build path before calling model_bridge_scenarios.",
      ]
    }
  if reason and reason.startswith("invalid_input:"):
    return {
      "next_actions": [
        "Pass assumptions_by_factor as a JSON list or native list of dicts.",
        "Pass scenarios as a JSON object/native dict with bull, base, and bear keys.",
        "Pass factor_anchor_hints as an object mapping factor name to owner item_id.",
      ]
    }
  if reason == "no_operations_to_apply":
    return {
      "next_actions": [
        "Inspect unresolved_factors, low_confidence_factors, and warnings.",
        "Resolve anchors with model_find_scenario_anchor and rerun with factor_anchor_hints.",
        "Ensure each flexed factor has explicit bull_values and bear_values curves for projection years.",
      ]
    }
  if reason == "scenario_not_decision_usable":
    return {
      "next_actions": [
        "Inspect bridge_validation.reason, missing_cases, eps_ordering_issues, and eps_delta_issues.",
        "Use model_find_scenario_anchor to bind factors to rows that actually feed adjusted EPS.",
        "If the model has no EPS propagation path, rebuild or repair the model before writing scenario assumptions.",
      ]
    }
  if reason == "live_workbook_unavailable":
    return {
      "next_actions": [
        "Use durable_file_mode, output_path, projection_persistence, and workbook_outputs as the authoritative file-mode bridge result.",
        "Reconnect or register the Excel taskpane, then rerun model_bridge_scenarios only if live workbook writeback is required.",
        "Do not treat this as scenario_not_decision_usable unless bridge_validation or operations_applied also show a scenario propagation failure.",
      ]
    }
  return None


def bridge_period_coverage_recovery(warnings: list[BridgeWarning]) -> dict | None:
  period_warnings = [
    warning
    for warning in warnings
    if warning.kind == "period_coverage_gap" and warning.detail.startswith("curve_missing_periods:")
  ]
  if not period_warnings:
    return None

  missing_periods: set[str] = set()
  factors: set[str] = set()
  for warning in period_warnings:
    if warning.factor:
      factors.add(str(warning.factor))
    _, _, period_text = warning.detail.partition(":")
    for token in period_text.split(","):
      period = token.strip()
      if period:
        missing_periods.add(period)

  def period_sort_key(value: str) -> tuple[int, int | str]:
    try:
      return (0, int(value))
    except ValueError:
      return (1, value)

  factor_text = ", ".join(sorted(factors)) or "each flexed factor"
  period_text = ", ".join(sorted(missing_periods, key=period_sort_key))
  return {
    "next_actions": [
      f"Extend bull_values and bear_values for {factor_text} to cover missing projection periods: {period_text}.",
      "Use explicit fiscal-year keys from the workbook projection horizon; scalar bull/base/bear values are not enough for scenario bridge writes.",
      "Rerun model_bridge_scenarios after the curves cover every projection year; do not debug anchors first unless period coverage is already complete.",
    ]
  }


__all__ = [
  "anchor_recovery",
  "bridge_period_coverage_recovery",
  "bridge_recovery",
  "projection_entry_validation_payload",
]
