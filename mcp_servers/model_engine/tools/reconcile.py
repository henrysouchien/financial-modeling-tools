from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any, Callable, get_type_hints


@dataclass(frozen=True)
class ReconcileSubtotalIntegrityDeps:
  validate_reconcile_mode: Callable[[str], str | dict]
  validate_reconcile_build_kwargs: Callable[[dict], dict | dict[str, Any]]
  validate_reconcile_file_path_match: Callable[[str, dict], dict | None]
  validate_reconcile_diagnostic_report: Callable[[dict], dict | dict[str, Any]]
  reconcile_error: Callable[[str, str], dict]
  flagged_reconcile_entries: Callable[[dict[str, Any]], list[dict[str, Any]] | dict]
  diagnostics_checked: list[str]
  group_reconcile_entries: Callable[[list[dict[str, Any]]], dict[tuple[str, str], list[dict[str, Any]]]]
  group_payload: Callable[[str, tuple[str, str], list[dict[str, Any]]], dict]
  delta_by_year: Callable[[list[dict[str, Any]]], dict]
  filing_pointer: Callable[[str, list[dict[str, Any]]], dict]
  unsupported_subtotals: set[tuple[str, str]]
  candidate_concepts_by_key: dict[tuple[str, str], list[str]]
  normalize_metric_entries: Callable[[list[dict], int], list[dict]]
  list_metrics: Callable[..., list[dict]]
  evaluate_reconcile_candidates: Callable[..., dict]
  current_registry_revision: Callable[[], str | dict]
  new_override_field: Callable[..., dict]
  resolve_reconcile_override_path: Callable[[str], Any]
  load_ticker_overrides: Callable[[str], Any]
  override_conflict: Callable[[Any, str, dict], dict | None]
  merged_ticker_overrides: Callable[[str, Any, str, dict], Any]
  override_conflict_response: Callable[[str, list[dict[str, Any]], str, dict, dict], dict]
  serialize_ticker_overrides: Callable[[Any], dict]
  existing_override_preserved: Callable[[Any, str], bool]
  ticker_override_lock: Callable[[str], Any]
  save_ticker_overrides: Callable[[Any], Any]
  model_build: Callable[..., dict]
  before_summary: Callable[[list[dict[str, Any]]], dict]
  after_summary: Callable[[dict | None, str, str], dict]
  diagnostic_group_cleared: Callable[[dict | None, str, str], bool]
  rebuild_summary: Callable[[dict], dict]


def reconcile_subtotal_integrity_handler(
  *,
  deps: ReconcileSubtotalIntegrityDeps,
  ticker: str,
  file_path: str,
  diagnostic_report: dict,
  build_kwargs: dict,
  mode: str = "dry_run",
  match_tolerance_pct: float = 0.01,
  match_floor_abs_m: float = 0.1,
) -> dict:
  mode_result = deps.validate_reconcile_mode(mode)
  if isinstance(mode_result, dict):
    return mode_result
  normalized_mode = mode_result

  kwargs_result = deps.validate_reconcile_build_kwargs(build_kwargs)
  if kwargs_result.get("status") == "error":
    return kwargs_result
  validated_build_kwargs = dict(kwargs_result)

  path_mismatch = deps.validate_reconcile_file_path_match(file_path, validated_build_kwargs)
  if path_mismatch is not None:
    return path_mismatch

  diagnostics_result = deps.validate_reconcile_diagnostic_report(diagnostic_report)
  if diagnostics_result.get("status") == "error":
    return diagnostics_result
  diagnostics = diagnostics_result

  ticker_upper = str(ticker or "").strip().upper()
  if not ticker_upper:
    return deps.reconcile_error("build_kwargs_invalid", "ticker is required", recoverable=False)

  flagged_result = deps.flagged_reconcile_entries(diagnostics)
  if isinstance(flagged_result, dict) and flagged_result.get("status") == "error":
    return flagged_result
  flagged = flagged_result
  if not flagged:
    return {
      "status": "no_action_needed",
      "flagged_count": 0,
      "diagnostics_checked": list(deps.diagnostics_checked),
    }

  grouped = deps.group_reconcile_entries(flagged)
  if len(grouped) > 1:
    return {
      "status": "escalate_multiple_groups",
      "groups": [
        deps.group_payload(ticker_upper, key, entries)
        for key, entries in sorted(grouped.items())
      ],
      "recommendation": "manual_resolve_each_group_then_rebuild",
    }

  (diagnostic_type, subtotal_name), group_entries = next(iter(grouped.items()))
  delta_by_year = deps.delta_by_year(group_entries)
  filing_pointer = deps.filing_pointer(ticker_upper, group_entries)

  if (diagnostic_type, subtotal_name) in deps.unsupported_subtotals:
    return {
      "status": "escalate_unsupported_subtotal",
      "affected_diagnostic": diagnostic_type,
      "affected_subtotal": subtotal_name,
      "delta_by_year": delta_by_year,
      "filing_pointer": filing_pointer,
      "recommendation": "manual_investigation",
    }

  if (diagnostic_type, subtotal_name) not in deps.candidate_concepts_by_key:
    return {
      "status": "escalate_unsupported_subtotal",
      "affected_diagnostic": diagnostic_type,
      "affected_subtotal": subtotal_name,
      "delta_by_year": delta_by_year,
      "filing_pointer": filing_pointer,
      "recommendation": "manual_investigation",
    }

  tags_by_year: dict[int, list[dict]] = {}
  try:
    for year in sorted(int(entry["year"]) for entry in group_entries):
      tags_by_year[year] = deps.normalize_metric_entries(
        deps.list_metrics(
          ticker_upper,
          year,
          4,
          date_type="FY",
          include_values=True,
          limit=1000,
        ),
        year=year,
      )
  except Exception as exc:
    return deps.reconcile_error("list_metrics_failed", str(exc), recoverable=True)

  candidate_result = deps.evaluate_reconcile_candidates(
    diagnostic_type=diagnostic_type,
    subtotal_name=subtotal_name,
    entries=group_entries,
    tags_by_year=tags_by_year,
    match_tolerance_pct=float(match_tolerance_pct),
    match_floor_abs_m=float(match_floor_abs_m),
  )
  matches = candidate_result["matches"]
  if not matches:
    partials = candidate_result["partials"]
    if partials:
      partial = partials[0]
      return {
        "status": "escalate_year_disagreement",
        "affected_diagnostic": diagnostic_type,
        "affected_subtotal": subtotal_name,
        "delta_by_year": delta_by_year,
        "candidate_per_year": partial["candidate_per_year"],
        "filing_pointer": filing_pointer,
        "recommendation": "manual_investigation",
      }
    return {
      "status": "escalate_no_candidate",
      "affected_diagnostic": diagnostic_type,
      "affected_subtotal": subtotal_name,
      "delta_by_year": delta_by_year,
      "searched_tag_count": candidate_result["searched_tag_count"],
      "top_5_nearest_by_year": candidate_result["top_5_nearest_by_year"],
      "filing_pointer": filing_pointer,
      "skip_reasons": candidate_result["skip_reasons"],
    }

  if len(matches) > 1:
    return {
      "status": "escalate_multiple_candidates",
      "affected_diagnostic": diagnostic_type,
      "affected_subtotal": subtotal_name,
      "delta_by_year": delta_by_year,
      "candidates": [
        {
          "candidate_concept": match["candidate_concept"],
          "tag": match["tag"],
          "value_by_year": {
            str(year): value
            for year, value in sorted(match["value_by_year"].items())
          },
          "distance_max_across_years": match["distance_max_across_years"],
          "suggested_scope_hint": (
            f"Review filing presentation for {match['candidate_concept']}"
          ),
        }
        for match in matches
      ],
      "filing_pointer": filing_pointer,
    }

  match = matches[0]
  affected_input_concept = str(match["candidate_concept"])
  candidate_tag = str(match["tag"])
  registry_revision = deps.current_registry_revision()
  if isinstance(registry_revision, dict):
    return registry_revision

  proposed_field = deps.new_override_field(
    matched_tag=candidate_tag,
    diagnostic_type=diagnostic_type,
    subtotal_name=subtotal_name,
    entries=group_entries,
    registry_revision=registry_revision,
  )

  try:
    override_path = deps.resolve_reconcile_override_path(ticker_upper)
    existing = deps.load_ticker_overrides(ticker_upper)
  except ValueError as exc:
    if "Invalid override file casing" in str(exc):
      return deps.reconcile_error(
        "existing_override_casing_conflict",
        str(exc),
        recoverable=True,
      )
    return deps.reconcile_error("override_write_failed", str(exc), recoverable=True)
  except Exception as exc:
    return deps.reconcile_error("override_write_failed", str(exc), recoverable=True)

  conflict = deps.override_conflict(existing, affected_input_concept, proposed_field)
  if conflict is not None:
    proposed = deps.merged_ticker_overrides(
      ticker_upper,
      existing,
      affected_input_concept,
      proposed_field,
    )
    return deps.override_conflict_response(
      ticker_upper,
      group_entries,
      affected_input_concept,
      conflict,
      deps.serialize_ticker_overrides(proposed),
    )

  proposed_overrides = deps.merged_ticker_overrides(
    ticker_upper,
    existing,
    affected_input_concept,
    proposed_field,
  )
  existing_override_preserved = deps.existing_override_preserved(
    existing,
    affected_input_concept,
  )

  if normalized_mode == "dry_run":
    return {
      "status": "proposed",
      "affected_diagnostic": diagnostic_type,
      "affected_subtotal": subtotal_name,
      "affected_input_concept": affected_input_concept,
      "delta_by_year": delta_by_year,
      "proposed_override": deps.serialize_ticker_overrides(proposed_overrides),
      "candidate_tag": candidate_tag,
      "candidate_values_by_year": {
        str(year): value
        for year, value in sorted(match["value_by_year"].items())
      },
      "override_path": str(override_path),
      "existing_override_preserved": existing_override_preserved,
    }

  try:
    with deps.ticker_override_lock(ticker_upper):
      override_path = deps.resolve_reconcile_override_path(ticker_upper)
      existing = deps.load_ticker_overrides(ticker_upper)
      conflict = deps.override_conflict(existing, affected_input_concept, proposed_field)
      if conflict is not None:
        proposed = deps.merged_ticker_overrides(
          ticker_upper,
          existing,
          affected_input_concept,
          proposed_field,
        )
        return deps.override_conflict_response(
          ticker_upper,
          group_entries,
          affected_input_concept,
          conflict,
          deps.serialize_ticker_overrides(proposed),
        )
      existing_override_preserved = deps.existing_override_preserved(
        existing,
        affected_input_concept,
      )
      merged_overrides = deps.merged_ticker_overrides(
        ticker_upper,
        existing,
        affected_input_concept,
        proposed_field,
      )
      written_path = deps.save_ticker_overrides(merged_overrides)
  except ValueError as exc:
    if "Invalid override file casing" in str(exc):
      return deps.reconcile_error(
        "existing_override_casing_conflict",
        str(exc),
        recoverable=True,
      )
    return deps.reconcile_error("override_write_failed", str(exc), recoverable=True)
  except Exception as exc:
    return deps.reconcile_error("override_write_failed", str(exc), recoverable=True)

  try:
    rebuild_result = deps.model_build(**validated_build_kwargs)
  except Exception as exc:
    return deps.reconcile_error("model_build_replay_failed", str(exc), recoverable=True)
  if not isinstance(rebuild_result, dict) or rebuild_result.get("status") == "error":
    return deps.reconcile_error(
      "model_build_replay_failed",
      str(rebuild_result.get("error") if isinstance(rebuild_result, dict) else rebuild_result),
      recoverable=True,
    )

  diagnostic_report = rebuild_result.get("diagnostic_report")
  before = deps.before_summary(group_entries)
  after = deps.after_summary(diagnostic_report, diagnostic_type, subtotal_name)
  if deps.diagnostic_group_cleared(diagnostic_report, diagnostic_type, subtotal_name):
    return {
      "status": "applied",
      "affected_diagnostic": diagnostic_type,
      "affected_subtotal": subtotal_name,
      "affected_input_concept": affected_input_concept,
      "candidate_tag": candidate_tag,
      "candidate_values_by_year": {
        str(year): value
        for year, value in sorted(match["value_by_year"].items())
      },
      "proposed_override": deps.serialize_ticker_overrides(merged_overrides),
      "override_path": str(written_path),
      "rebuild_summary": deps.rebuild_summary(rebuild_result),
      "before_after": {
        diagnostic_type: {
          "before": before,
          "after": after,
        }
      },
      "existing_override_preserved": existing_override_preserved,
    }

  return {
    "status": "escalate_rebuild_still_failing",
    "attempted_override": deps.serialize_ticker_overrides(merged_overrides),
    "before": before,
    "after": after,
    "recommendation": "manual_investigation",
    "filing_pointer": filing_pointer,
  }


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ReconcileToolFunctions:
  reconcile_subtotal_integrity: Callable[..., dict[str, Any]]


def _parent_reconcile_subtotal_integrity_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ReconcileSubtotalIntegrityDeps:
  return parent_namespace()["_reconcile_subtotal_integrity_deps"]()


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


def register_reconcile_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ReconcileToolFunctions | None = None,
) -> ReconcileToolFunctions:
  functions = functions or build_reconcile_tool_functions(
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


def build_reconcile_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ReconcileToolFunctions:
  def reconcile_subtotal_integrity(
    ticker: str,
    file_path: str,
    diagnostic_report: dict,
    build_kwargs: dict,
    mode: str = "dry_run",
    match_tolerance_pct: float = 0.01,
    match_floor_abs_m: float = 0.1,
  ) -> dict:
    """Detect and auto-resolve subtotal-integrity diagnostic gaps caused by
    wrong XBRL tag selection (registry-equivalence collapse failures).

    Consumes the build-time diagnostic_report, identifies the input concept
    whose tag selection is wrong via cross-year list_metrics value-matching,
    proposes a canonical_tag override, and (when mode="apply") writes the
    override + rebuilds + reverifies. Value lookups come directly from
    wrong_tag_suspected entry["inputs"]; no model is loaded for derivation.

    Default mode="dry_run" returns the proposed override without side effects.
    The calling skill explicitly passes mode="apply" for the auto-apply path.

    On ambiguity (zero matches, multiple matches, or rebuild still failing),
    returns a typed escalation envelope with candidates and a filing pointer
    for agent-led qualitative review.

    Discovery: file_path, diagnostic_report, and build_kwargs come from the
    immediately preceding model_build response. Pass diagnostic_report unchanged;
    build_kwargs should be the same model_build kwargs that produced file_path.
    """
    return _parent_handler(
      parent_namespace,
      "_reconcile_subtotal_integrity_handler",
    )(
      deps=_parent_reconcile_subtotal_integrity_deps(parent_namespace),
      ticker=ticker,
      file_path=file_path,
      diagnostic_report=diagnostic_report,
      build_kwargs=build_kwargs,
      mode=mode,
      match_tolerance_pct=match_tolerance_pct,
      match_floor_abs_m=match_floor_abs_m,
    )

  return ReconcileToolFunctions(
    reconcile_subtotal_integrity=reconcile_subtotal_integrity
  )


__all__ = [
  "ReconcileSubtotalIntegrityDeps",
  "ReconcileToolFunctions",
  "build_reconcile_tool_functions",
  "register_reconcile_tools",
  "reconcile_subtotal_integrity_handler",
]
