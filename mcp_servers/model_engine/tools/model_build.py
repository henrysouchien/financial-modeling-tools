from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, Optional, get_type_hints


@dataclass(frozen=True)
class ModelBuildDeps:
  new_model_build_id: Callable[[], str]
  expanduser: Callable[[str], str]
  historical_sources_cls: Any
  business_model_cls: Any
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  load_business_model_from_path: Callable[..., tuple[Any, str, Any, list[str]]]
  logger: Any
  historical_sources_touch_fmp: Callable[[Any], bool]
  historical_sources_touch_edgar: Callable[[Any], bool]
  fmp_zero_missing_edgar_fallback_needed: Callable[..., bool]
  asyncio_run: Callable[[Any], Any]
  fetch_fmp_financials: Callable[[str], Any]
  build_valuation_comps_fallback: Callable[..., Any]
  make_edgar_fetcher: Callable[[], Any]
  make_edgar_financials_fetcher: Callable[[], Any]
  warm_edgar_cache: Callable[..., dict[int, Any]]
  accumulate_tree: Callable[[dict[int, Any]], Any]
  build_model: Callable[..., Any]
  load_model_bundle: Callable[..., Any]
  render_plan_to_addin_payload: Callable[..., dict[str, Any]]
  dispatch_to_addin: Callable[[str, dict[str, Any]], Any]
  addin_dispatch_error_status: Callable[[Exception], dict[str, Any]]
  seed_projections_result_cls: Any
  serialize_diagnostic_report: Callable[[Any], Any]
  asdict: Callable[[Any], dict[str, Any]]
  is_dataclass: Callable[[Any], bool]
  model_build_error_payload: Callable[..., dict[str, Any]]
  model_handle_token_payload: Callable[..., dict[str, Any]]


def model_build_handler(
  *,
  deps: ModelBuildDeps,
  ticker: str,
  output_path: str | None = None,
  company_name: str,
  fiscal_year_end: str,
  most_recent_fy: int,
  source: str = "fmp",
  financials: dict | None = None,
  sector: str | None = None,
  n_historical: int = 5,
  n_projection: int = 12,
  discover_segments: bool = False,
  segment_mapping: list | None = None,
  axis: str | None = None,
  historical_sources: dict | None = None,
  business_model: dict | None = None,
  business_model_path: str | None = None,
  equity_risk_premium: float | None = None,
  valuation_comps: dict | None = None,
  validation_mode: bool = False,
  source_arbitration_mode: str = "off",
  target: str = "file",
  conflict_strategy: str = "fail_on_collision",
) -> dict[str, Any]:
  model_build_id = deps.new_model_build_id()
  if business_model is not None and business_model_path is not None:
    raise ValueError("business_model and business_model_path are mutually exclusive")

  try:
    if target not in {"file", "workbook"}:
      return {
        "status": "error",
        "error": f"Unsupported target: {target!r}",
        "model_build_id": model_build_id,
      }
    if target == "workbook" and conflict_strategy not in ("fail_on_collision", "overwrite"):
      return {
        "status": "error",
        "error": (
          f"Invalid conflict_strategy {conflict_strategy!r}; "
          "supported: 'fail_on_collision' (default) or 'overwrite'."
        ),
        "model_build_id": model_build_id,
      }
    resolved_output_path = deps.expanduser(output_path) if output_path is not None else None
    if target == "file" and resolved_output_path is None:
      return {
        "status": "error",
        "error": "output_path is required when target='file'",
        "model_build_id": model_build_id,
      }

    parsed_historical_sources = (
      deps.historical_sources_cls.model_validate(historical_sources)
      if historical_sources is not None
      else None
    )
    parsed_valuation_comps = (
      deps.coerce_json_dict_arg(valuation_comps, name="valuation_comps")
      if valuation_comps is not None
      else None
    )
    parsed_business_model = None
    if business_model is not None:
      parsed_business_model = deps.business_model_cls.model_validate(business_model)
    elif business_model_path is not None:
      parsed_business_model, _input_format, _path, _searched_paths = (
        deps.load_business_model_from_path(business_model_path)
      )
    if parsed_historical_sources is not None and str(source).lower() != "fmp":
      deps.logger.warning(
        "model_build received source=%s with historical_sources; historical_sources routing wins",
        source,
      )

    segment_mode = bool(discover_segments) or segment_mapping is not None
    if axis is not None and not segment_mode:
      raise ValueError("axis requires segment discovery to be enabled")
    normalized_source = str(source).lower()
    if segment_mode and normalized_source != "edgar":
      deps.logger.warning("Segment mode requires EDGAR source; forcing source='edgar'")
      normalized_source = "edgar"

    if (
      normalized_source == "edgar"
      and parsed_historical_sources is None
    ):
      parsed_historical_sources = deps.historical_sources_cls(
        default_source="edgar",
        default_fallback_enabled=True,
      )
    source_arbitration_enabled = str(source_arbitration_mode).lower() != "off"
    fmp_required_by_populate = (
      (normalized_source == "fmp" and parsed_historical_sources is None)
      or deps.historical_sources_touch_fmp(parsed_historical_sources)
    )
    auto_edgar_fallback = (
      normalized_source == "edgar"
      and parsed_historical_sources is not None
      and parsed_historical_sources.default_source == "edgar"
      and parsed_historical_sources.default_fallback_enabled
      and not historical_sources
    )
    if financials is None and (
      auto_edgar_fallback or validation_mode or source_arbitration_enabled
    ):
      try:
        financials = deps.asyncio_run(deps.fetch_fmp_financials(ticker))
      except Exception as exc:
        if auto_edgar_fallback:
          raise RuntimeError(
            f"FMP fetch failed for source=edgar fallback build (ticker={ticker}): {exc}. "
            "Either supply 'financials' explicitly or disable default-fallback by passing historical_sources."
          ) from exc
        if fmp_required_by_populate:
          raise RuntimeError(
            f"FMP fetch failed for source={normalized_source} build (ticker={ticker}): {exc}. "
            "Supply 'financials' explicitly or choose an EDGAR-only historical_sources route."
          ) from exc
        deps.logger.warning(
          "FMP fetch failed for diagnostic-only model_build (ticker=%s): %s",
          ticker,
          exc,
        )
        financials = None
    if parsed_valuation_comps is None:
      parsed_valuation_comps = deps.build_valuation_comps_fallback(ticker, financials)

    routing_touches_edgar = deps.historical_sources_touch_edgar(parsed_historical_sources)
    zero_missing_edgar_fallback = deps.fmp_zero_missing_edgar_fallback_needed(
      source=normalized_source,
      financials=financials,
      historical_sources=parsed_historical_sources,
    )
    needs_edgar_fetcher = (
      normalized_source == "edgar"
      or routing_touches_edgar
      or zero_missing_edgar_fallback
      or validation_mode
      or source_arbitration_enabled
    )
    edgar_fetcher = deps.make_edgar_fetcher() if needs_edgar_fetcher else None
    edgar_financials_fetcher = deps.make_edgar_financials_fetcher() if segment_mode else None
    warm_fetcher = (
      deps.make_edgar_financials_fetcher()
      if needs_edgar_fetcher and not segment_mode
      else None
    )
    cache_warm_results: dict[int, str] = {}
    presentation_tree = None
    if warm_fetcher is not None:
      historical_years = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))
      warm_results = deps.warm_edgar_cache(
        ticker=ticker,
        historical_years=historical_years,
        financials_fetcher=warm_fetcher,
      )
      payloads_by_year = {year: result.payload for year, result in warm_results.items() if result.payload}
      presentation_tree = deps.accumulate_tree(payloads_by_year)
      cache_warm_results = {year: result.status for year, result in warm_results.items()}
      cache_warm_messages = {
        year: result.message for year, result in warm_results.items() if result.message
      }
    else:
      cache_warm_messages = {}
    result = deps.build_model(
      ticker=ticker,
      company_name=company_name,
      fiscal_year_end=fiscal_year_end,
      most_recent_fy=most_recent_fy,
      output_path=resolved_output_path,
      source=normalized_source,
      fmp_data=financials,
      sector=sector,
      n_historical=n_historical,
      n_projection=n_projection,
      edgar_fetcher=edgar_fetcher,
      segment_mapping=segment_mapping,
      edgar_financials_fetcher=edgar_financials_fetcher,
      axis=axis,
      historical_sources=parsed_historical_sources,
      business_model=parsed_business_model,
      presentation_tree=presentation_tree,
      validation_mode=validation_mode,
      source_arbitration_mode=source_arbitration_mode,
      equity_risk_premium=equity_risk_premium,
      valuation_comps=parsed_valuation_comps,
      enforce_model_quality_status_block=True,
    )
    persisted_model_bundle = None
    if resolved_output_path and getattr(result, "model", None) is not None:
      persisted_model_bundle = deps.load_model_bundle(
        resolved_output_path,
        model=result.model,
        persist=True,
      )

    workbook_result = None
    workbook_dispatch_failed = False
    if target == "workbook":
      payload = deps.render_plan_to_addin_payload(
        result.render_plan,
        conflict_strategy=conflict_strategy,
      )
      try:
        workbook_result = deps.dispatch_to_addin("apply_render_plan", payload)
      except Exception as exc:
        workbook_dispatch_failed = True
        workbook_result = deps.addin_dispatch_error_status(exc)

    segment_profile = getattr(result, "segment_profile", None)
    segments = []
    if segment_profile is not None:
      for index, segment in enumerate(segment_profile.segments, start=1):
        segments.append(
          {
            "index": index,
            "name": segment.name,
            "edgar_member": segment.edgar_member,
            "item_ids": dict(segment.item_ids or {}),
          }
        )

    response_status = "ok"
    if workbook_dispatch_failed:
      response_status = "partial" if resolved_output_path else "error"
    response = {
      "status": response_status,
      "model_build_id": model_build_id,
      "output_path": result.output_path,
      "source": result.stats.source,
      "items_populated": result.stats.items_populated,
      "periods_populated": result.stats.periods_populated,
      "items_skipped": result.stats.items_skipped,
      "missing_concepts": result.stats.missing_concepts,
      "edgar_api_calls": result.stats.edgar_api_calls,
      "edgar_errors": result.stats.edgar_errors,
      "edgar_partial_failures": result.stats.edgar_partial_failures,
      "cache_warm_results": {str(y): s for y, s in cache_warm_results.items()},
      "diagnostic_report": deps.serialize_diagnostic_report(getattr(result, "diagnostic", None)),
      "segments_discovered": len(segments),
      "segments": segments,
      "overrides_applied": getattr(result, "overrides_applied", 0),
      "custom_concepts_applied": getattr(result, "custom_concepts_applied", 0),
      "bm_compiled": getattr(result, "compiled_registry", None) is not None,
    }
    if source_arbitration_enabled:
      response["source_arbitration_mode"] = source_arbitration_mode
      response["source_arbitration_final_source_by_concept_year"] = {
        concept_id: {str(year): source for year, source in by_year.items()}
        for concept_id, by_year in getattr(
          result,
          "source_arbitration_final_source_by_concept_year",
          {},
        ).items()
      }
    if workbook_result is not None:
      response["workbook_result"] = workbook_result
    if workbook_dispatch_failed:
      response["reason"] = "live_workbook_unavailable"
    seed_projection_result = getattr(result, "seed_projections", deps.seed_projections_result_cls())
    if hasattr(seed_projection_result, "to_dict"):
      response["seed_projections"] = seed_projection_result.to_dict()
    else:
      response["seed_projections"] = seed_projection_result
    semantic_rows_result = getattr(result, "semantic_rows", None)
    if semantic_rows_result is not None:
      response["semantic_rows"] = (
        semantic_rows_result.to_dict()
        if hasattr(semantic_rows_result, "to_dict")
        else semantic_rows_result
      )
    valuation_input_readiness = getattr(result, "valuation_input_readiness", None)
    if valuation_input_readiness is not None:
      response["valuation_input_readiness"] = (
        valuation_input_readiness.to_dict()
        if hasattr(valuation_input_readiness, "to_dict")
        else deps.asdict(valuation_input_readiness)
        if deps.is_dataclass(valuation_input_readiness)
        else valuation_input_readiness
      )
    if cache_warm_messages:
      response["cache_warm_messages"] = {str(y): s for y, s in cache_warm_messages.items()}
    fmp_quality_warnings = list(getattr(result.stats, "fmp_quality_warnings", []) or [])
    if fmp_quality_warnings:
      response["fmp_quality_warnings"] = fmp_quality_warnings
    if persisted_model_bundle is not None:
      try:
        response["model_handle_token"] = deps.model_handle_token_payload(
          file_path=resolved_output_path,
          historical_cutoff_year=most_recent_fy,
          issued_by="model_build",
        )
      except Exception as exc:
        response["model_handle_token_error"] = {
          "error": str(exc),
          "error_code": getattr(exc, "error_code", "model_handle_token_unavailable"),
        }
    return response
  except Exception as exc:
    return deps.model_build_error_payload(exc, model_build_id=model_build_id)


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelBuildToolFunctions:
  model_build: Callable[..., dict[str, Any]]


def _parent_model_build_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelBuildDeps:
  return parent_namespace()["_model_build_deps"]()


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


def register_model_build_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ModelBuildToolFunctions | None = None,
) -> ModelBuildToolFunctions:
  functions = functions or build_model_build_tool_functions(
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


def build_model_build_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ModelBuildToolFunctions:
  def model_build(
    ticker: str,
    output_path: Optional[str] = None,
    *,
    company_name: str,
    fiscal_year_end: str,
    most_recent_fy: int,
    source: str = "fmp",
    financials: Optional[dict] = None,
    sector: Optional[str] = None,
    n_historical: int = 5,
    n_projection: int = 12,
    discover_segments: bool = False,
    segment_mapping: Optional[list] = None,
    axis: Optional[str] = None,
    historical_sources: Optional[dict] = None,
    business_model: Optional[dict] = None,
    business_model_path: Optional[str] = None,
    equity_risk_premium: float | None = None,
    valuation_comps: Optional[dict] = None,
    validation_mode: bool = False,
    source_arbitration_mode: str = "off",
    target: str = "file",
    conflict_strategy: str = "fail_on_collision",
  ) -> dict:
    """Build a populated financial model from SIA template + financial data.

    Discovery: company_name, fiscal_year_end, and most_recent_fy come from the
    company profile or filing context. For source="fmp", pre-fetch financials and
    pass them explicitly; for source="edgar", provide the ticker and routing
    config and let the local EDGAR path populate historicals.

    source="fmp" (default): pass pre-fetched FMP financials as `financials`.
    source="edgar": fetch historicals from the local EDGAR API.
    historical_sources: optional routing config, e.g.
      {"default_source": "fmp", "overrides": [
        {"concept_id": "revenue", "preferred": "edgar", "fallback_order": ["edgar", "fmp"]}
      ]}.
      When supplied, per-concept routing wins over the global source for historical population.
    valuation_comps: optional build-time comp-table payload:
      {"source": "peer_comparison", "basis": "forward_ntm_fy1",
       "target": {"ticker": "TICK", "forward_pe": 20.0, "peg": 1.5,
                  "ev_ebitda": 12.0, "trailing_low": 15.0,
                  "trailing_median": 22.0, "trailing_high": 30.0},
       "peers": [{"ticker": "PEER", "forward_pe": 18.0, "peg": 1.2,
                  "ev_ebitda": 10.0, "trailing_low": 12.0,
                  "trailing_median": 19.0, "trailing_high": 25.0}]}.
      Primary production callers should bridge this from Thesis peer_comparison;
      if omitted, model-engine tries a small FMP build_fallback.
    source_arbitration_mode: "off" (default), "shadow", or "apply". This
      controls the explicit FMP-vs-EDGAR source-arbitration diagnostic/apply
      pass; validation_mode by itself does not enable arbitration.

    target="file" (default): write a .xlsx via openpyxl to output_path.
    target="workbook": dispatch the render plan to the active Excel add-in
      via apply_render_plan. output_path becomes optional. conflict_strategy
      controls existing-sheets handling: "fail_on_collision" (default) returns
      an error if Assumptions/Financial_model already exist; "overwrite"
      clears existing sheets before writing (sheet names preserved → cross-
      sheet formulas continue to resolve).
    Pass returned model_handle_token unchanged to later model modification or
    bridge tools when available.
    """
    return _parent_handler(parent_namespace, "_model_build_handler")(
      deps=_parent_model_build_deps(parent_namespace),
      ticker=ticker,
      output_path=output_path,
      company_name=company_name,
      fiscal_year_end=fiscal_year_end,
      most_recent_fy=most_recent_fy,
      source=source,
      financials=financials,
      sector=sector,
      n_historical=n_historical,
      n_projection=n_projection,
      discover_segments=discover_segments,
      segment_mapping=segment_mapping,
      axis=axis,
      historical_sources=historical_sources,
      business_model=business_model,
      business_model_path=business_model_path,
      equity_risk_premium=equity_risk_premium,
      valuation_comps=valuation_comps,
      validation_mode=validation_mode,
      source_arbitration_mode=source_arbitration_mode,
      target=target,
      conflict_strategy=conflict_strategy,
    )

  return ModelBuildToolFunctions(model_build=model_build)


__all__ = [
  "ModelBuildDeps",
  "ModelBuildToolFunctions",
  "build_model_build_tool_functions",
  "model_build_handler",
  "register_model_build_tools",
]
