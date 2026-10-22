#!/usr/bin/env python3
"""Compose the read-only financial-model inspection MCP server.

Stdio enters through ``mcp``/``main``; this entrypoint injects dependencies and
registers handlers from ``model_build`` and ``mcp_servers/model_engine/tools/``
on the server built by ``mcp_servers/model_engine/server.py:create_mcp``.
``schema/`` owns model contracts and parsing, while hosted-current-model
acquisition stays in ``api/research/`` and projection in ``model_build``.
Durable model mutation is outside this server. Preserve the trusted-local versus authenticated-hosted read split
and keep import-time output off stdout. See ``mcp_servers/README.md``.
"""

# ruff: noqa: E402, F401 -- bootstrap adjusts import paths; handlers are
# resolved by the registered tools through this module's parent namespace.

from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, List, Literal, Optional, get_type_hints

if TYPE_CHECKING:
    from research.current_model_version_resolver import CurrentModelResolutionError


def _model_engine_root() -> Path:
    path = Path(__file__).resolve()
    for candidate in (path.parent, *path.parents):
        if (candidate / "schema").is_dir() and (
            candidate / "mcp_servers" / "model_engine"
        ).is_dir():
            return candidate
    return path.parents[1]


ROOT = _model_engine_root()
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "api"))

_real_stdout = sys.stdout
sys.stdout = sys.stderr

import bootstrap_env

from pydantic import ValidationError
from fastmcp.server.context import Context

import schema.business_model_validate as business_model_validate_schema
from schema.business_model import BusinessModel
from schema.handle_token import (
    ModelHandleToken,
    ModelHandleTokenError,
    validate_model_handle_token,
)
from schema.model_readiness import compute_model_scenario_bridge_readiness
from schema.model_state_identity import CurrentModelIdentity
import schema.overrides as schema_overrides
from schema.scenario_bridge import find_scenario_anchor
from schema.segments import (
    SEGMENT_AXES_PRIORITY as _SEGMENT_AXES_PRIORITY,
    discover_all_axes as _discover_all_axes,
    segment_revenue_observation_list as _segment_revenue_observation_list,
)
from schema.statement_window_preview import (
    preview_statement_windows as _preview_statement_windows,
)
from schema.statement_windows import (
    parse_statement_windows as _parse_statement_windows,
    serialize_statement_windows as _serialize_statement_windows,
)
from schema.tools import (
    JudgmentBasis as _JudgmentBasis,
    invalid_override_value_error as _invalid_override_value_error,
    judgment_analysis as _judgment_analysis,
    model_tool_error_payload as _model_tool_error_payload,
)
from schema.workbook_presentation import (
    workbook_presentation_fingerprint,
    workbook_presentation_gap,
)
from mcp_servers.model_engine.args import (
    coerce_json_dict_arg as _coerce_json_dict_arg,
    coerce_json_list_arg as _coerce_json_list_arg,
    safe_pydantic_errors as _safe_pydantic_errors,
    valid_workspace_user_id as _valid_workspace_user_id,
)
from mcp_servers.model_engine.business_model_artifacts import (
    load_business_model_from_path as _load_business_model_from_path,
    resolve_business_model_path as _resolve_business_model_path,
)
from model_build.build_helpers import (
    axis_priority as _axis_priority_impl,
    serialize_year_values as _serialize_year_values,
)
from mcp_servers.model_engine.handle_tokens import (
    model_handle_token_payload as _model_handle_token_payload_impl,
)
from mcp_servers.model_engine.runtime import (
    apply_override_directory_env as _apply_override_directory_env_impl,
    validate_file_path as _validate_file_path_impl,
)
from mcp_servers.model_engine.scenario_readiness import (
    readiness_owner_anchor_payload as _readiness_owner_anchor_payload,
)
from mcp_servers.model_engine.server import (
    FastMCP,
    create_mcp as _create_mcp,
    main as _run_mcp,
)
from mcp_servers.model_engine import standalone_readonly as _standalone_readonly
from mcp_servers.model_engine.tools.bridge import (
    ModelFindScenarioAnchorDeps as _ModelFindScenarioAnchorDeps,
    ModelScenarioTopologyDeps as _ModelScenarioTopologyDeps,
    model_find_scenario_anchor_handler as _model_find_scenario_anchor_handler,
    model_scenario_topology_handler as _model_scenario_topology_handler,
    register_bridge_tools as _register_bridge_tools,
)
from mcp_servers.model_engine.tools.business_model import (
    BusinessModelToolDeps as _BusinessModelToolDeps,
    business_model_validate_handler as _business_model_validate_handler,
    register_business_model_tools as _register_business_model_tools,
)
from mcp_servers.model_engine.tools.discovery import (
    ModelDiscoveryDeps as _ModelDiscoveryDeps,
    model_discover_segments_handler as _model_discover_segments_handler,
    register_model_discovery_tools as _register_model_discovery_tools,
)
from model_build.model_semantics import (
    ModelResolutionFailure as _ModelResolutionFailure,
    ModelSemanticsDeps as _ModelSemanticsDeps,
    model_semantics_handler as _model_semantics_handler,
)
from mcp_servers.model_engine.tools.read import (
    ModelReadDeps as _ModelReadDeps,
    ModelReadSource as _ModelReadSource,
    model_drivers_handler as _model_drivers_handler,
    model_find_handler as _model_find_handler,
    model_presentation_compare_handler as _model_presentation_compare_handler,
    model_presentation_fingerprint_handler as _model_presentation_fingerprint_handler,
    model_scenario_handler as _model_scenario_handler,
    model_sensitivity_handler as _model_sensitivity_handler,
    model_summarize_handler as _model_summarize_handler,
    model_valuation_summary_handler as _model_valuation_summary_handler,
    model_values_handler as _model_values_handler,
    register_model_read_tools as _register_model_read_tools,
)
from mcp_servers.model_engine.tools.statement_window import (
    StatementWindowToolDeps as _StatementWindowToolDeps,
    register_statement_mapping_tools as _register_statement_mapping_tools,
)
from model_build.valuation_summary import (
    VALUATION_READBACK_ITEMS as _VALUATION_READBACK_ITEMS,
    VALUATION_READBACK_ITEM_IDS as _VALUATION_READBACK_ITEM_IDS,
    enrich_valuation_rows as _enrich_valuation_rows,
    group_valuation_rows as _group_valuation_rows,
    valuation_derived_metrics as _valuation_derived_metrics,
    valuation_input_readiness as _valuation_input_readiness,
    valuation_policy_summary as _valuation_policy_summary,
)


_MODEL_ENGINE_OVERRIDES_DIR_ENV = schema_overrides.MODEL_ENGINE_OVERRIDES_DIR_ENV


def _make_edgar_financials_fetcher() -> Any:
    """Load the EDGAR adapter only when a fetch-backed read tool executes."""

    from research.financials_fetch import make_edgar_financials_fetcher

    return make_edgar_financials_fetcher()


def _apply_override_directory_env() -> Path | None:
    return _apply_override_directory_env_impl(
        overrides_dir_env=_MODEL_ENGINE_OVERRIDES_DIR_ENV,
        schema_overrides_module=schema_overrides,
        os_module=os,
        path_cls=Path,
    )


sys.stdout = _real_stdout

mcp = _create_mcp()


def _validate_file_path(file_path: str) -> str:
    """Expand ``~`` and verify that the model file exists."""
    return _validate_file_path_impl(file_path, os_path=os.path)


def _request_meta(ctx: Context | None) -> dict[str, Any]:
  request_context = ctx.request_context if ctx is not None else None
  if request_context is None or not request_context.meta:
    return {}
  # FastMCP exposes raw wire metadata; progress alone is not hosted authority.
  return {
    key: value
    for key, value in request_context.meta.items()
    if key != "progressToken"
  }


def _authenticated_model_reader_user(ctx: Context | None) -> str:
    raw_user_id = _request_meta(ctx).get("user_id")
    normalized = str(raw_user_id or "").strip()
    if not normalized.isascii() or not normalized.isdecimal() or int(normalized) <= 0:
        raise ValueError(
            "hosted current-model reads require authenticated MCP user metadata"
        )
    return str(int(normalized))


def _model_semantics_user(
    *,
    ctx: Context | None,
    supplied_user_id: str | None,
) -> str | None:
    meta = _request_meta(ctx)
    if not meta:
        return supplied_user_id
    if supplied_user_id is not None:
        raise ValueError("hosted model_semantics does not accept caller user_id")
    return _authenticated_model_reader_user(ctx)
def _business_model_user(
    *,
    ctx: Context | None,
    supplied_user_id: str | None,
) -> str | None:
    meta = _request_meta(ctx)
    if not meta:
        return supplied_user_id
    if supplied_user_id is not None:
        raise ValueError("hosted business_model_validate does not accept caller user_id")
    return _authenticated_model_reader_user(ctx)

@contextmanager
def _resolved_model_read_source(
    *,
    file_path: str | None,
    current_model: dict[str, Any] | None,
    ctx: Context | None,
):
    """Yield a trusted-local path or an authenticated private hosted snapshot."""

    if (file_path is None) == (current_model is None):
        raise ValueError("provide exactly one of file_path or current_model")
    request_meta = _request_meta(ctx)
    if current_model is None:
        if request_meta:
            raise ValueError(
                "authenticated hosted model reads require current_model identity"
            )
        if not isinstance(file_path, str) or not file_path.strip():
            raise ValueError("file_path is required for trusted-local model reads")
        yield _ModelReadSource(file_path=file_path)
        return

    user_id = _authenticated_model_reader_user(ctx)
    asserted = CurrentModelIdentity.model_validate(current_model)
    from research.build_model_reads import read_current_model
    from model_build.report_projection import control_snapshot_projection
    from research.current_model_version_resolver import (
        resolve_current_model_version,
    )
    from research.repository import get_repository_factory
    from schema.published_model_readback import (
        verified_published_model_from_artifacts,
    )

    repository = get_repository_factory().get_live_read_only(user_id)
    file_row = repository.get_file(asserted.research_file_id)
    if not isinstance(file_row, dict):
        raise ValueError("authenticated research file does not exist")
    ticker = str(file_row.get("ticker") or "").strip().upper()
    resolved = resolve_current_model_version(
        repository,
        user_id=user_id,
        research_file_id=asserted.research_file_id,
        ticker=ticker,
    )
    artifacts = read_current_model(repository, resolved)
    if artifacts.identity != asserted:
        raise ValueError("asserted current_model identity is no longer current")

    verified_model = verified_published_model_from_artifacts(
        artifacts.workbook,
        artifacts.semantic_document,
        artifacts.validation_report,
    )
    yield _ModelReadSource(
        file_path="authenticated-current-model.xlsx",
        model=verified_model,
        validation_status=artifacts.semantic_document.validation_status,
        validation_findings=artifacts.validation_report.findings,
        control_snapshot_projection=control_snapshot_projection(
            artifacts.semantic_document.render_state
        ).model_dump(mode="json"),
        judgment_basis=_JudgmentBasis(
            model=artifacts.semantic_document.model,
            provenance=artifacts.provenance,
            forecast_assumptions=artifacts.forecast_assumptions,
            claims=artifacts.claims,
            references=artifacts.references,
        ),
    )


def _axis_priority(axis: Optional[str]) -> Optional[int]:
    return _axis_priority_impl(axis, axes_priority=_SEGMENT_AXES_PRIORITY)


def _model_handle_token_payload(
    *,
    file_path: str,
    historical_cutoff_year: int | None,
    issued_by: str,
) -> dict[str, Any]:
    return _model_handle_token_payload_impl(
        file_path=file_path,
        historical_cutoff_year=historical_cutoff_year,
        issued_by=issued_by,
        load_handle=_standalone_readonly.load_handle,
    )


def _model_read_deps() -> _ModelReadDeps:
    return _ModelReadDeps(
        validate_file_path=_validate_file_path,
        model_tool_error_payload=_model_tool_error_payload,
        summarize=_standalone_readonly.summarize,
        values=_standalone_readonly.values,
        enrich_valuation_rows=_enrich_valuation_rows,
        group_valuation_rows=_group_valuation_rows,
        valuation_derived_metrics=_valuation_derived_metrics,
        valuation_policy_summary=_valuation_policy_summary,
        valuation_input_readiness=_valuation_input_readiness,
        valuation_readback_items=_VALUATION_READBACK_ITEMS,
        valuation_readback_item_ids=_VALUATION_READBACK_ITEM_IDS,
        workbook_presentation_fingerprint=workbook_presentation_fingerprint,
        workbook_presentation_gap=workbook_presentation_gap,
        find=_standalone_readonly.find,
        coerce_json_list_arg=_coerce_json_list_arg,
        drivers=_standalone_readonly.drivers,
        sensitivity=_standalone_readonly.sensitivity,
        coerce_json_dict_arg=_coerce_json_dict_arg,
        invalid_override_period_error=_standalone_readonly.invalid_override_period_error,
        invalid_override_value_error=_invalid_override_value_error,
        scenario=_standalone_readonly.scenario,
        model_handle_token_payload=_model_handle_token_payload,
        published_values=_standalone_readonly.published_values,
        judgment_analysis=_judgment_analysis,
    )


def _model_discovery_deps() -> _ModelDiscoveryDeps:
    return _ModelDiscoveryDeps(
        discover_all_axes=_discover_all_axes,
        make_edgar_financials_fetcher=_make_edgar_financials_fetcher,
        axis_priority=_axis_priority,
        segment_revenue_observation_list=_segment_revenue_observation_list,
        serialize_year_values=_serialize_year_values,
    )


def _load_model_semantics_control_state(
    authenticated_user_id: str,
    research_file_id: int,
    ticker: str,
) -> Any:
    """Load the exact current model's root-projected compiler semantics."""

    from research.build_model_reads import read_model_semantics
    from research.repository import get_repository_factory

    repository_factory = get_repository_factory()
    repository = repository_factory.get_live_read_only(authenticated_user_id)
    return read_model_semantics(
        repository,
        user_id=authenticated_user_id,
        research_file_id=research_file_id,
        ticker=ticker,
    )


def _model_semantics_deps() -> "_ModelSemanticsDeps[CurrentModelResolutionError]":
    from research.current_model_version_resolver import (
        CurrentModelResolutionError,
        ResolvedCurrentModel,
        resolve_current_model_version,
    )
    from research.repository import get_repository_factory

    def resolution_failure(exc: CurrentModelResolutionError) -> _ModelResolutionFailure:
        return _ModelResolutionFailure(exc.code, exc.retryable, exc.fix)

    return _ModelSemanticsDeps(
        load_current_model_semantics=_load_model_semantics_control_state,
        get_repository_factory=get_repository_factory,
        resolve_current_model_version=resolve_current_model_version,
        valid_workspace_user_id=_valid_workspace_user_id,
        current_model_resolution_error=CurrentModelResolutionError,
        resolution_failure=resolution_failure,
        resolved_current_model_type=ResolvedCurrentModel,
    )


def _business_model_tool_deps() -> _BusinessModelToolDeps:
    return _BusinessModelToolDeps(
        business_model_cls=BusinessModel,
        validation_error_type=ValidationError,
        validate_schema=business_model_validate_schema,
        load_business_model_from_path=_load_business_model_from_path,
        resolve_business_model_path=_resolve_business_model_path,
        coerce_json_dict_arg=_coerce_json_dict_arg,
        coerce_json_list_arg=_coerce_json_list_arg,
        make_edgar_financials_fetcher=_make_edgar_financials_fetcher,
        discover_all_axes=_discover_all_axes,
        safe_pydantic_errors=_safe_pydantic_errors,
        model_tool_error_payload=_model_tool_error_payload,
    )


def _statement_mapping_tool_deps() -> _StatementWindowToolDeps:
    return _StatementWindowToolDeps(
        coerce_json_dict_arg=_coerce_json_dict_arg,
        make_edgar_financials_fetcher=_make_edgar_financials_fetcher,
        load_snapshot=schema_overrides.load_ticker_overrides_snapshot,
        preview_statement_windows=_preview_statement_windows,
        parse_statement_windows=_parse_statement_windows,
        serialize_statement_windows=_serialize_statement_windows,
        model_tool_error_payload=_model_tool_error_payload,
    )


def _bridge_model_bundle(file_path: str, cutoff: int) -> Any:
    return _standalone_readonly.load_bundle(
        file_path,
        historical_cutoff_year=cutoff,
    )


def _bridge_model_bundle_from_model(
    file_path: str,
    cutoff: int,
    model: Any,
) -> Any:
    return _standalone_readonly.load_bundle(
        file_path,
        model=model,
        historical_cutoff_year=cutoff,
    )


def _anchor_recovery(payload: dict[str, Any]) -> dict[str, Any] | None:
    if payload.get("match_reason") not in {
        "invalid_hint",
        "unresolved",
        "label_match_low_confidence",
    }:
        return None
    return {
        "candidate_hints": payload.get("candidates") or [],
        "next_actions": [
            "Retry model_find_scenario_anchor with a confirmed candidate owner_id.",
            "Use model_find or model_summarize to inspect related scenario rows.",
        ],
    }


def _model_find_scenario_anchor_deps() -> _ModelFindScenarioAnchorDeps:
    return _ModelFindScenarioAnchorDeps(
        current_year=lambda: datetime.now(timezone.utc).year,
        bridge_model_bundle=_bridge_model_bundle,
        readiness_owner_anchor_payload=_readiness_owner_anchor_payload,
        find_scenario_anchor=find_scenario_anchor,
        anchor_recovery=_anchor_recovery,
        model_tool_error_payload=_model_tool_error_payload,
        validate_model_handle_token=validate_model_handle_token,
        model_handle_token_cls=ModelHandleToken,
        model_handle_token_error_type=ModelHandleTokenError,
    )


def _model_scenario_topology_deps() -> _ModelScenarioTopologyDeps:
    return _ModelScenarioTopologyDeps(
        current_year=lambda: datetime.now(timezone.utc).year,
        bridge_model_bundle=_bridge_model_bundle,
        coerce_json_list_arg=_coerce_json_list_arg,
        compute_scenario_bridge_readiness=compute_model_scenario_bridge_readiness,
        readiness_owner_anchor_payload=_readiness_owner_anchor_payload,
        find_scenario_anchor=find_scenario_anchor,
        anchor_recovery=_anchor_recovery,
        model_tool_error_payload=_model_tool_error_payload,
        validate_model_handle_token=validate_model_handle_token,
        model_handle_token_cls=ModelHandleToken,
        model_handle_token_error_type=ModelHandleTokenError,
    )


_read_tool_functions = _register_model_read_tools(
    mcp,
    parent_namespace=lambda: globals(),
)
model_summarize = _read_tool_functions.model_summarize
model_valuation_summary = _read_tool_functions.model_valuation_summary
model_presentation_fingerprint = _read_tool_functions.model_presentation_fingerprint
model_presentation_compare = _read_tool_functions.model_presentation_compare
model_find = _read_tool_functions.model_find
model_values = _read_tool_functions.model_values
model_drivers = _read_tool_functions.model_drivers
model_sensitivity = _read_tool_functions.model_sensitivity
model_scenario = _read_tool_functions.model_scenario

ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelSemanticsFunctions:
  model_semantics: Callable[..., dict[str, Any]]


def _parent_model_semantics_deps(
  parent_namespace: ParentNamespaceProvider,
) -> "_ModelSemanticsDeps[CurrentModelResolutionError]":
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
  registered = mcp.tool(meta={
    "hank/code-disposition": "read_composable",
    "hank/code-reason": "Reads typed forecast, scenario, valuation, and source intent from the authenticated current ModelVersion selected by an explicit research-file identity without modifying the model.",
  })(function)
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
    *,
    item_ids: Optional[List[str]] = None,
    driver_keys: Optional[List[str]] = None,
    scenario_names: Optional[List[Literal["base", "bull", "bear"]]] = None,
    years: Optional[List[int]] = None,
    source_detail: Literal["none", "metadata", "full"] = "metadata",
    limit: int = 25,
    cursor: Optional[str] = None,
    ctx: Optional[Context] = None,
  ) -> dict:
    """Read a bounded, typed view of forecast, scenario, and valuation intent.

    Values, rationale, confidence, and source_refs_by_period are projected
    directly from the selected ModelVersion's canonical ModelSpec. Use
    source_detail="none" for the smallest reference-only view.
    A page whose serialization exceeds max_response_chars halves until it fits
    or holds one entry; pagination.budget_limited marks the shrink and
    view.response_chars records the returned size. Continue with next_cursor.

    What a rendered number rests on (its trace to facts and judgment rows,
    each with its operation kind), every judgment and house default a version
    binds, and a judgment's revision history come from the research door
    query_model_version in one call, not from pages of this tool.

    Citation boundary: this tool never mints current-run citation handles.
    Re-read the exact source with filings_source_excerpt or
    transcripts_source_excerpt during the current skill run before using it at
    an FMS evidence door.

    Forecast assumptions, scenarios, and valuation controls come only from the
    authenticated current ModelVersion's validated ModelSpec. Missing, stale,
    or invalid current-model compiler inputs fail closed.

    Filter forecast/scenario entries with typed item_ids and stable ModelSpec
    assumption keys in driver_keys,
    select base/bull/bear with scenario_names, and slice fiscal years with
    four-digit integer years. Pagination is deterministic across forecast then
    scenario entries. Reuse next_cursor only with the same sections, filters,
    and source_detail. limit defaults to 25 and is clamped to 1..100;
    pagination.requested_limit echoes the request and pagination.limit the page size used.
    A JSON-encoded list string is accepted for clients that stringify array
    arguments. Explicitly empty sections are rejected; omit sections to request
    all three.

    Identity selection requires an authenticated research_file_id. The server
    resolves that stable selector through the current-model pointer; it never
    scans research files by ticker or infers a latest model. current_model_ref
    accepts either the complete pointer/version payload or a partial selector
    containing research_file_id. A complete payload is re-resolved and must
    be byte-identical to the current selection. ticker is a consistency check
    only and cannot perform a lookup. If top-level and
    nested selectors are both supplied, they must agree.

    Examples: model_semantics(research_file_id=2, user_id="1") for the safe
    default; model_semantics(research_file_id=2, sections=["scenarios"],
    scenario_names=["bull"], years=[2027, 2028], source_detail="none"); or
    model_semantics(research_file_id=2, ticker="<TICKER>", sections=["forecast"],
    item_ids=["bm.arr"], source_detail="full", limit=1).
    """
    authenticated_user_id = parent_namespace()["_model_semantics_user"](
      ctx=ctx,
      supplied_user_id=user_id,
    )
    return _parent_handler(parent_namespace, "_model_semantics_handler")(
      deps=_parent_model_semantics_deps(parent_namespace),
      research_file_id=research_file_id,
      current_model_ref=current_model_ref,
      ticker=ticker,
      sections=sections,
      item_ids=item_ids,
      driver_keys=driver_keys,
      scenario_names=scenario_names,
      years=years,
      source_detail=source_detail,
      limit=limit,
      cursor=cursor,
      user_id=authenticated_user_id,
    )

  return ModelSemanticsFunctions(model_semantics=model_semantics)


_model_semantics_tool_functions = register_model_semantics_tools(
    mcp,
    parent_namespace=lambda: globals(),
)
model_semantics = _model_semantics_tool_functions.model_semantics

_model_discovery_tool_functions = _register_model_discovery_tools(
    mcp,
    parent_namespace=lambda: globals(),
)
model_discover_segments = _model_discovery_tool_functions.model_discover_segments

_business_model_tool_functions = _register_business_model_tools(
    mcp,
    parent_namespace=lambda: globals(),
)
business_model_validate = _business_model_tool_functions.business_model_validate

_statement_mapping_tool_functions = _register_statement_mapping_tools(
    mcp,
    parent_namespace=lambda: globals(),
)
statement_mapping_validate = (
    _statement_mapping_tool_functions.statement_mapping_validate
)

_bridge_tool_functions = _register_bridge_tools(
    mcp,
    parent_namespace=lambda: globals(),
)
model_scenario_topology = _bridge_tool_functions.model_scenario_topology
model_find_scenario_anchor = _bridge_tool_functions.model_find_scenario_anchor


def main() -> None:
    """Validate serving configuration, apply runtime overrides, and run stdio."""
    bootstrap_env.bootstrap(required=["EDGAR_API_KEY"])
    _apply_override_directory_env()
    _run_mcp(mcp)


if __name__ == "__main__":
    main()
