from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
import logging
from typing import Any

from fastmcp.server.context import Context

from mcp_servers.model_engine.tools import read_scenario as _read_scenario
from mcp_servers.model_engine.tools import read_valuation as _read_valuation
from research.current_model_version_resolver import CurrentModelResolutionError


_MISSING = _read_scenario.MISSING
_REASON_LIMIT = 300
_VALUES_MAX_RESPONSE_CELLS = 600

log = logging.getLogger(__name__)


def _empty_model_handle_token_payload(**_kwargs: Any) -> dict[str, Any]:
    return {}


@dataclass(frozen=True)
class ModelReadDeps:
    validate_file_path: Callable[[str], str]
    model_tool_error_payload: Callable[[Exception], dict[str, Any]]
    summarize: Callable[..., dict[str, Any]]
    values: Callable[..., dict[str, Any]]
    enrich_valuation_rows: Callable[[list[Any]], list[dict[str, Any]]]
    group_valuation_rows: Callable[[list[dict[str, Any]]], dict[str, Any]]
    valuation_derived_metrics: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    valuation_policy_summary: Callable[[list[dict[str, Any]]], dict[str, Any]]
    valuation_input_readiness: Callable[[list[dict[str, Any]]], dict[str, Any]]
    valuation_readback_items: Sequence[Any]
    valuation_readback_item_ids: Sequence[str]
    workbook_presentation_fingerprint: Callable[..., dict[str, Any]]
    workbook_presentation_gap: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ]
    find: Callable[..., list[dict[str, Any]]]
    coerce_json_list_arg: Callable[..., list[Any]]
    drivers: Callable[..., dict[str, Any]]
    sensitivity: Callable[..., dict[str, Any]]
    coerce_json_dict_arg: Callable[..., dict[str, Any]]
    invalid_override_period_error: Callable[..., Exception]
    invalid_override_value_error: Callable[..., Exception]
    scenario: Callable[..., dict[str, Any]]
    model_handle_token_payload: Callable[..., dict[str, Any]] = (
        _empty_model_handle_token_payload
    )
    published_values: Callable[..., dict[str, Any]] | None = None
    #: ``schema.tools.judgment_analysis``; bound to the hosted source's
    #: JudgmentBasis by ``_execute_model_read``.
    judgment_analysis: Callable[..., dict[str, Any]] | None = None


@dataclass(frozen=True)
class ModelReadSource:
    """Path-free hosted source or explicit trusted-local workbook path.

    A hosted source carries its version's stored ``ValidationReport``
    verdict and findings and the canonical control projection its render
    state records; a trusted-local path carries none of them, and hosted
    reads alone return them.
    """

    file_path: str
    model: Any | None = None
    validation_status: str | None = None
    validation_findings: tuple[Any, ...] = ()
    control_snapshot_projection: dict[str, Any] | None = None
    #: The hosted ModelVersion's ``schema.tools.JudgmentBasis`` (model, cell
    #: lineage, judgment rows, References); trusted-local paths carry none.
    judgment_basis: Any | None = None


def _validation_finding_row(finding: Any) -> dict[str, Any]:
    """Project one stored build ``Finding`` to its reader row."""

    declination = finding.declination
    return {
        "code": finding.code,
        "severity": finding.severity,
        "item_id": finding.item_id,
        "periods": list(finding.periods),
        "message": finding.message,
        "declination": (
            None
            if declination is None
            else {
                "declination_id": declination.declination_id,
                "reason": declination.reason,
            }
        ),
    }


def _attach_model_handle_token(
    response: dict[str, Any],
    *,
    deps: ModelReadDeps,
    file_path: str,
    historical_cutoff_year: int | None,
    issued_by: str,
) -> dict[str, Any]:
    try:
        response["model_handle_token"] = deps.model_handle_token_payload(
            file_path=file_path,
            historical_cutoff_year=historical_cutoff_year,
            issued_by=issued_by,
        )
    except Exception as exc:
        response["model_handle_token_error"] = {
            "error": str(exc),
            "error_code": getattr(exc, "error_code", "model_handle_token_unavailable"),
        }
    return response


def _is_scenario_table_value_item(item_id: str) -> bool:
    return _read_scenario.is_scenario_table_value_item(item_id)


def _scenario_table_scalar_value(period_values: Any) -> Any:
    return _read_scenario.scenario_table_scalar_value(period_values)


def _projection_periods_for_scenario_table_item(
    *,
    deps: ModelReadDeps,
    file_path: str,
    item_id: str,
    historical_cutoff_year: int | None,
) -> list[int]:
    return _read_scenario.projection_periods_for_scenario_table_item(
        deps=deps,
        file_path=file_path,
        item_id=item_id,
        historical_cutoff_year=historical_cutoff_year,
    )


def _is_number(value: Any) -> bool:
    return _read_scenario.is_number(value)


def _scenario_no_effect_warning(
    *,
    result: dict[str, Any],
    overrides: dict[str, dict[int, float]],
) -> dict[str, Any] | None:
    return _read_scenario.scenario_no_effect_warning(
        result=result,
        overrides=overrides,
        is_number_fn=_is_number,
    )


def _attach_scenario_no_effect_guidance(
    *,
    result: dict[str, Any],
    overrides: dict[str, dict[int, float]],
) -> dict[str, Any]:
    return _read_scenario.attach_scenario_no_effect_guidance(
        result=result,
        overrides=overrides,
        scenario_no_effect_warning_fn=_scenario_no_effect_warning,
    )


def _model_quality_readiness_for_valuation_summary(
    *,
    deps: ModelReadDeps,
    file_path: str,
    historical_cutoff_year: int | None,
    valuation_input_readiness: dict[str, Any],
) -> dict[str, Any]:
    return _read_valuation.model_quality_readiness_for_valuation_summary(
        deps=deps,
        file_path=file_path,
        historical_cutoff_year=historical_cutoff_year,
        valuation_input_readiness=valuation_input_readiness,
        merge_valuation_input_readiness_fn=_merge_valuation_input_readiness,
    )


def _merge_valuation_input_readiness(
    readiness: dict[str, Any],
    valuation_input_readiness: dict[str, Any],
) -> dict[str, Any]:
    return _read_valuation.merge_valuation_input_readiness(
        readiness,
        valuation_input_readiness,
        valuation_input_quality_issue_fn=_valuation_input_quality_issue,
        upsert_quality_issue_fn=_upsert_quality_issue,
        quality_status_fn=_quality_status,
        model_quality_summary_fn=_model_quality_summary,
    )


def _valuation_input_quality_issue(
    valuation_input_readiness: dict[str, Any],
) -> dict[str, Any] | None:
    return _read_valuation.valuation_input_quality_issue(valuation_input_readiness)


def _upsert_quality_issue(issues: list[Any], issue: dict[str, Any]) -> None:
    return _read_valuation.upsert_quality_issue(issues, issue)


def _quality_status(issues: list[Any], *, fallback: str = "ready") -> str:
    return _read_valuation.quality_status(issues, fallback=fallback)


def _model_quality_summary(status: str, domains: dict[str, Any]) -> str:
    return _read_valuation.model_quality_summary(status, domains)


def model_summarize_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    historical_cutoff_year: int | None = None,
    include_items: bool = False,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        result = deps.summarize(
            file_path,
            historical_cutoff_year=historical_cutoff_year,
            include_items=include_items,
        )
        return _attach_model_handle_token(
            {"status": "ok", **result},
            deps=deps,
            file_path=file_path,
            historical_cutoff_year=historical_cutoff_year,
            issued_by="model_summarize",
        )
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


def model_valuation_summary_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    periods: str | list[int | str] = "all",
    historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        values_result = deps.values(
            file_path,
            deps.valuation_readback_item_ids,
            periods=periods,
            historical_cutoff_year=historical_cutoff_year,
        )
        if deps.published_values is not None:
            values_result = deps.published_values(
                file_path,
                list(deps.valuation_readback_item_ids),
                periods=periods,
                historical_cutoff_year=historical_cutoff_year,
                fallback=values_result,
            )
        rows = deps.enrich_valuation_rows(list(values_result.get("items") or []))
        valuation_input_readiness = deps.valuation_input_readiness(rows)
        model_quality_readiness = _model_quality_readiness_for_valuation_summary(
            deps=deps,
            file_path=file_path,
            historical_cutoff_year=historical_cutoff_year,
            valuation_input_readiness=valuation_input_readiness,
        )
        result = {
            "status": "ok",
            "periods_returned": values_result.get("periods_returned"),
            "period_count": values_result.get("period_count"),
            "canonical_item_ids": {
                entry["key"]: entry["item_id"]
                for entry in deps.valuation_readback_items
            },
            "sections": deps.group_valuation_rows(rows),
            "items": rows,
            "derived_metrics": deps.valuation_derived_metrics(rows),
            "valuation_policy": deps.valuation_policy_summary(rows),
            "valuation_input_readiness": valuation_input_readiness,
            "model_quality_readiness": model_quality_readiness,
            "next_actions": [
                "Use returned canonical_item_ids with model_values only for additional period detail.",
                "Use model_find only for non-standard custom valuation rows missing from this summary.",
                "Check valuation_input_readiness before clean DCF acceptance; missing rows or legacy placeholder-like values are input gaps, not valuation conclusions.",
                "Check model_quality_readiness before treating valuation output as client-ready; blocked or incomplete model domains are model-quality gates, not valuation conclusions.",
                "Use valuation_policy to explain ERP, raw beta, adjusted beta, beta floor, and cost-of-equity basis after input readiness is complete.",
                "For WACC rationale in Thesis, update an existing registered assumption or add a data gap if no stable assumption id exists.",
            ],
        }
        published_readback = values_result.get("published_model_readback")
        if isinstance(published_readback, dict):
            result["published_model_readback"] = published_readback
            result["valuation_evidence_status"] = published_readback.get(
                "valuation_evidence_status"
            )
        return result
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


def model_presentation_fingerprint_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    max_cells_per_sheet: int = 50_000,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        fingerprint = deps.workbook_presentation_fingerprint(
            file_path,
            max_cells_per_sheet=max_cells_per_sheet,
        )
        return {"status": "ok", "fingerprint": fingerprint}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def model_presentation_compare_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    reference_path: str,
    max_cells_per_sheet: int = 50_000,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        reference_path = deps.validate_file_path(reference_path)
        generated = deps.workbook_presentation_fingerprint(
            file_path,
            max_cells_per_sheet=max_cells_per_sheet,
        )
        reference = deps.workbook_presentation_fingerprint(
            reference_path,
            max_cells_per_sheet=max_cells_per_sheet,
        )
        gap = deps.workbook_presentation_gap(generated, reference)
        return {
            "status": "ok",
            "generated": generated,
            "reference": reference,
            "gap": gap,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def model_find_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    query: str,
    limit: int = 20,
    historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        results = deps.find(
            file_path,
            query,
            limit=limit,
            historical_cutoff_year=historical_cutoff_year,
        )
        return {"status": "ok", "items": results, "count": len(results)}
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


def model_values_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    item_ids: list[str] | str,
    periods: str | list[int | str] = "all",
    historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        parsed_item_ids = deps.coerce_json_list_arg(item_ids, name="item_ids")
        requested_item_ids = list(dict.fromkeys(str(item_id) for item_id in parsed_item_ids))
        result = deps.values(
            file_path,
            requested_item_ids,
            periods=periods,
            historical_cutoff_year=historical_cutoff_year,
        )
        period_count = int(result["period_count"])
        # Agent response budget: return the first item batch that fits the cell
        # budget and hand the rest back as the item_ids of the next calls.
        batch_size = max(1, _VALUES_MAX_RESPONSE_CELLS // max(period_count, 1))
        item_id_batches = [
            requested_item_ids[index : index + batch_size]
            for index in range(0, len(requested_item_ids), batch_size)
        ]
        returned_item_ids = item_id_batches[0] if item_id_batches else []
        result = {**result, "items": result["items"][: len(returned_item_ids)]}
        if deps.published_values is not None:
            result = deps.published_values(
                file_path,
                returned_item_ids,
                periods=periods,
                historical_cutoff_year=historical_cutoff_year,
                fallback=result,
            )
        result = {
            **result,
            "requested_cells": len(requested_item_ids) * period_count,
            "max_response_cells": _VALUES_MAX_RESPONSE_CELLS,
            "remaining_item_id_batches": item_id_batches[1:],
        }
        return _attach_model_handle_token(
            {"status": "ok", **result},
            deps=deps,
            file_path=file_path,
            historical_cutoff_year=historical_cutoff_year,
            issued_by="model_values",
        )
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


def model_drivers_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    item_id: str,
    depth: int = 3,
    historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        result = deps.drivers(
            file_path,
            item_id,
            depth=depth,
            historical_cutoff_year=historical_cutoff_year,
        )
        return {"status": "ok", **result}
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


def model_sensitivity_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    target_id: str,
    n: int = 15,
    bump_pct: float = 0.10,
    candidate_filter: str = "drivers",
    max_candidates: int | None = None,
    candidate_ids: list[str] | None = None,
    sensitivity_mode: str | None = None,
    recompute_policy: str = "projection_safe",
    historical_cutoff_year: int | None = None,
    view: str = "items",
) -> dict[str, Any]:
    try:
        if view != "items":
            if deps.judgment_analysis is None:
                raise ValueError(
                    f"view={view!r} reads the hosted ModelVersion's judgment rows and "
                    "References; pass current_model instead of file_path"
                )
            result = deps.judgment_analysis(view, target_id=target_id or None)
            return {"status": "ok", **result}
        file_path = deps.validate_file_path(file_path)
        result = deps.sensitivity(
            file_path,
            target_id,
            n=n,
            bump_pct=bump_pct,
            candidate_filter=candidate_filter,
            max_candidates=max_candidates,
            candidate_ids=candidate_ids,
            sensitivity_mode=sensitivity_mode,
            recompute_policy=recompute_policy,
            historical_cutoff_year=historical_cutoff_year,
        )
        return {"status": "ok", **result}
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


def model_scenario_handler(
    *,
    deps: ModelReadDeps,
    file_path: str,
    overrides: dict[str, dict[str, float]] | str,
    compare_items: list[str] | str | None = None,
    recompute_policy: str = "projection_safe",
    historical_cutoff_year: int | None = None,
) -> dict[str, Any]:
    try:
        file_path = deps.validate_file_path(file_path)
        overrides = deps.coerce_json_dict_arg(overrides, name="overrides")
        if compare_items is not None:
            compare_items = [
                str(item_id)
                for item_id in deps.coerce_json_list_arg(
                    compare_items,
                    name="compare_items",
                )
            ]
        normalized: dict[str, dict[int, float]] = {}
        if not isinstance(overrides, dict):
            return {
                "status": "error",
                "error": "overrides must be a dict mapping item_id -> {period -> value}",
            }
        for item_id, period_values in overrides.items():
            item_id_str = str(item_id)
            if _is_scenario_table_value_item(item_id_str):
                scalar_value = _scenario_table_scalar_value(period_values)
                if scalar_value is not _MISSING:
                    try:
                        value = float(scalar_value)
                    except (TypeError, ValueError) as exc:
                        raise deps.invalid_override_value_error(
                            item_id_str,
                            "projection",
                            scalar_value,
                            cause=str(exc),
                        ) from exc
                    normalized[item_id_str] = {
                        period: value
                        for period in _projection_periods_for_scenario_table_item(
                            deps=deps,
                            file_path=file_path,
                            item_id=item_id_str,
                            historical_cutoff_year=historical_cutoff_year,
                        )
                    }
                    continue
            if not isinstance(period_values, dict):
                return {
                    "status": "error",
                    "error": f"overrides['{item_id}'] must be a dict mapping period -> value",
                }
            normalized_values: dict[int, float] = {}
            for raw_period, raw_value in period_values.items():
                try:
                    period = int(raw_period)
                except (TypeError, ValueError) as exc:
                    raise deps.invalid_override_period_error(
                        raw_period,
                        file_path=file_path,
                        historical_cutoff_year=historical_cutoff_year,
                        cause=str(exc),
                    ) from exc
                try:
                    normalized_values[period] = float(raw_value)
                except (TypeError, ValueError) as exc:
                    raise deps.invalid_override_value_error(
                        str(item_id),
                        raw_period,
                        raw_value,
                        cause=str(exc),
                    ) from exc
            normalized[str(item_id)] = normalized_values

        result = deps.scenario(
            file_path,
            normalized,
            compare_items=compare_items,
            recompute_policy=recompute_policy,
            historical_cutoff_year=historical_cutoff_year,
        )
        result = _attach_scenario_no_effect_guidance(
            result=result,
            overrides=normalized,
        )
        return _attach_model_handle_token(
            {"status": "ok", **result},
            deps=deps,
            file_path=file_path,
            historical_cutoff_year=historical_cutoff_year,
            issued_by="model_scenario",
        )
    except Exception as exc:
        return deps.model_tool_error_payload(exc)


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelReadToolFunctions:
    model_summarize: Callable[..., dict[str, Any]]
    model_valuation_summary: Callable[..., dict[str, Any]]
    model_presentation_fingerprint: Callable[..., dict[str, Any]]
    model_presentation_compare: Callable[..., dict[str, Any]]
    model_find: Callable[..., dict[str, Any]]
    model_values: Callable[..., dict[str, Any]]
    model_drivers: Callable[..., dict[str, Any]]
    model_sensitivity: Callable[..., dict[str, Any]]
    model_scenario: Callable[..., dict[str, Any]]


def _parent_read_deps(parent_namespace: ParentNamespaceProvider) -> ModelReadDeps:
    return parent_namespace()["_model_read_deps"]()


def _parent_handler(
    parent_namespace: ParentNamespaceProvider,
    name: str,
) -> Callable[..., dict[str, Any]]:
    return parent_namespace()[name]


def _hosted_model_read_result(value: dict[str, Any]) -> dict[str, Any]:
    """Remove the exact legacy top-level coordinates from a hosted response."""

    forbidden_keys = frozenset({
        "file_path",
        "model_file_path",
        "model_handle_token",
        "model_handle_token_error",
        "user_id",
        "workbook_path",
    })
    return {key: item for key, item in value.items() if key not in forbidden_keys}


def _hosted_model_read_error(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, CurrentModelResolutionError):
        return {
            "status": "error",
            "error": str(exc),
            "error_code": exc.code,
            "retryable": exc.retryable,
            **({"fix": exc.fix} if exc.fix else {}),
        }
    # The read failed outside the resolver's typed errors. Name the cause in
    # the log and, bounded, in the payload: a bare generic error hid a store
    # condition from every reader of these runs.
    log.warning(
        "hosted current-model read failed: %s: %s",
        type(exc).__name__,
        exc,
        exc_info=exc,
    )
    return {
        "status": "error",
        "error": "The authenticated current model could not be read.",
        "error_code": "current_model_read_failed",
        "reason": f"{type(exc).__name__}: {exc}"[:_REASON_LIMIT],
    }


def _execute_model_read(
    parent_namespace: ParentNamespaceProvider,
    *,
    file_path: str | None,
    current_model: dict[str, Any] | None,
    ctx: Context | None,
    execute: Callable[[ModelReadDeps, str], dict[str, Any]],
    include_version_report: bool = False,
) -> dict[str, Any]:
    deps = _parent_read_deps(parent_namespace)
    if current_model is not None:
        deps = replace(deps, model_tool_error_payload=_hosted_model_read_error)
    try:
        with parent_namespace()["_resolved_model_read_source"](
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
        ) as source:
            if not isinstance(source, ModelReadSource):
                raise TypeError("model read source must be exact ModelReadSource")
            if source.model is not None:
                def _with_model(function: Callable[..., Any]) -> Callable[..., Any]:
                    def _bound(file_path: str, *args: Any, **kwargs: Any) -> Any:
                        kwargs["model"] = source.model
                        return function(file_path, *args, **kwargs)

                    return _bound

                deps = replace(
                    deps,
                    validate_file_path=lambda value: value,
                    summarize=_with_model(deps.summarize),
                    values=_with_model(deps.values),
                    find=_with_model(deps.find),
                    drivers=_with_model(deps.drivers),
                    sensitivity=_with_model(deps.sensitivity),
                    scenario=_with_model(deps.scenario),
                    model_handle_token_payload=_empty_model_handle_token_payload,
                    published_values=None,
                )
            analysis = deps.judgment_analysis
            basis = source.judgment_basis
            if analysis is None or basis is None:
                deps = replace(deps, judgment_analysis=None)
            else:
                def _with_basis(view: str, **kwargs: Any) -> dict[str, Any]:
                    return analysis(view, basis=basis, **kwargs)

                deps = replace(deps, judgment_analysis=_with_basis)
            result = execute(deps, source.file_path)
            if current_model is not None:
                result["validation_status"] = source.validation_status
                if include_version_report:
                    result["validation_findings"] = [
                        _validation_finding_row(finding)
                        for finding in source.validation_findings
                    ]
                    result["control_snapshot_projection"] = (
                        source.control_snapshot_projection
                    )
                result = _hosted_model_read_result(result)
            return result
    except Exception as exc:
        return (
            _hosted_model_read_error(exc)
            if current_model is not None
            else deps.model_tool_error_payload(exc)
        )


def _bind_parent_module(
    function: Callable[..., dict[str, Any]],
    parent_namespace: ParentNamespaceProvider,
) -> Callable[..., dict[str, Any]]:
    parent_module = parent_namespace().get("__name__")
    if isinstance(parent_module, str):
        function.__module__ = parent_module
        function.__qualname__ = function.__name__
    return function


def _register_tool(
    mcp: Any,
    function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
    registered = mcp.tool(meta={
        "hank/code-disposition": "read_composable",
        "hank/code-reason": "Reads an explicit trusted-local workbook or authenticated current model and derives structure, values, dependencies, presentation, or in-memory analyses without persisting model changes.",
    })(function)
    return registered or function


def register_model_read_tools(
    mcp: Any,
    *,
    parent_namespace: ParentNamespaceProvider,
    tool_names: Sequence[str] | None = None,
    functions: ModelReadToolFunctions | None = None,
) -> ModelReadToolFunctions:
    functions = functions or build_model_read_tool_functions(
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


def build_model_read_tool_functions(
    *,
    parent_namespace: ParentNamespaceProvider,
) -> ModelReadToolFunctions:
    def model_summarize(
        file_path: str | None = None,
        historical_cutoff_year: int | None = None,
        include_items: bool = False,
        current_model: dict[str, Any] | None = None,
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Summarize model structure: sheets, sections, item counts, key metrics.

        Hosted: copy only research_file_id, ticker, model_id, version,
        pointer_epoch, workbook_xlsx_sha256, and model_spec_sha256 from
        get_current_model into current_model, and omit file_path. Set
        include_items=True when choosing item_ids for follow-up reads. Trusted-local
        operator calls may instead omit current_model and pass an explicit file_path;
        only those calls reuse a returned model_handle_token.

        A hosted call also returns that version's own build findings as
        validation_findings (code, severity, item_id, periods, message and the
        clearing declination), read from the version's stored validation report,
        and its control_snapshot_projection (source_authority, curve counts and
        one coverage row per seeded curve: control_id, binding_id, scenario,
        resolved_item_id, periods), derived from the version's render state.
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_summarize_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                historical_cutoff_year=historical_cutoff_year,
                include_items=include_items,
            ),
            include_version_report=True,
        )

    def model_valuation_summary(
        file_path: str | None = None,
        periods: str | list[int | str] = "all",
        historical_cutoff_year: int | None = None,
        current_model: dict[str, Any] | None = None,
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Read canonical valuation, DCF, WACC, P/E, and EV/EBITDA rows.

        Hosted: copy only research_file_id, ticker, model_id, version,
        pointer_epoch, workbook_xlsx_sha256, and model_spec_sha256 from
        get_current_model into current_model, and omit file_path. Trusted-local
        operator calls may instead omit current_model and pass an explicit file_path.
        Use model_summarize(include_items=True) only when this reader reports an
        unknown-item gap.

        Use this before dcf-relative-valuation or valuation QA. It returns the stable
        SIA valuation item IDs and values for common outputs so agents do not need
        repeated model_find probes for DCF price, terminal value, WACC, P/E, or
        EV/EBITDA rows. If an older/custom workbook lacks a canonical row, that row
        appears with an unknown-item error and suggested recovery rather than hiding
        the gap.
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_valuation_summary_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                periods=periods,
                historical_cutoff_year=historical_cutoff_year,
            ),
        )

    def model_presentation_fingerprint(
        file_path: str,
        max_cells_per_sheet: int = 50_000,
    ) -> dict[str, Any]:
        """Inventory workbook presentation/style primitives for rendering review.

        Trusted-local/operator only: pass an explicit file_path for a workbook the
        operator has selected.
        Use this single-workbook inventory to inspect visible sheets, fills, borders,
        merged ranges, widths, and freeze panes. Use model_presentation_compare when
        you also have a reference workbook and need a side-by-side formatting diff.
        This is not a substitute for visual benchmark review before claiming a model
        is client-ready.
        """
        return _parent_handler(
            parent_namespace,
            "_model_presentation_fingerprint_handler",
        )(
            deps=_parent_read_deps(parent_namespace),
            file_path=file_path,
            max_cells_per_sheet=max_cells_per_sheet,
        )

    def model_presentation_compare(
        file_path: str,
        reference_path: str,
        max_cells_per_sheet: int = 50_000,
    ) -> dict[str, Any]:
        """Compare observed workbook presentation primitives against a reference workbook.

        Trusted-local/operator only: file_path and reference_path are explicit
        operator-selected workbooks; reference_path may be a hand-built or client-ready
        benchmark.
        Use model_presentation_fingerprint instead when you only need to inventory one
        workbook. This comparison is intentionally coarse so stale forecast values in
        the reference do not pollute the formatting review. Treat it as an inventory
        diff, not as a client-ready formatting verdict.
        """
        return _parent_handler(parent_namespace, "_model_presentation_compare_handler")(
            deps=_parent_read_deps(parent_namespace),
            file_path=file_path,
            reference_path=reference_path,
            max_cells_per_sheet=max_cells_per_sheet,
        )

    def model_find(
        file_path: str | None = None,
        query: str = "",
        limit: int = 20,
        historical_cutoff_year: int | None = None,
        current_model: dict[str, Any] | None = None,
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Search for line items by name or ID substring match.

        Hosted: copy the seven identity fields from get_current_model into
        current_model and omit file_path. Trusted-local operator calls may instead pass an explicit
        file_path. Use query terms from the visible statement label or expected concept id.
        Returned item ids feed model_values, model_drivers, and model_sensitivity.
        For thesis-factor scenario anchors, use model_find_scenario_anchor instead of
        generic line-item search.
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_find_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                query=query,
                limit=limit,
                historical_cutoff_year=historical_cutoff_year,
            ),
        )

    def model_values(
        file_path: str | None = None,
        item_ids: list[str] | str | None = None,
        periods: str | list[int | str] = "all",
        historical_cutoff_year: int | None = None,
        current_model: dict[str, Any] | None = None,
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Get full time series values for one or more line items.

        Hosted: copy the seven identity fields from get_current_model into
        current_model and omit file_path. Trusted-local operator calls may instead pass an explicit
        file_path. Use model_find or model_summarize(include_items=True) to obtain
        item_ids, then pass a JSON list or native list of item ids.

        Periods: use "all", "historical", "projection", "YYYY:YYYY", a comma list
        such as "2024,2025,2026", or a native list such as [2024, 2025, "FY2026"].

        Batching: model_values accepts normal analytical batches with more than 10
        item_ids. A response carries at most 600 cells (item_ids x periods): a larger
        request returns the first item batch, requested_cells, and
        remaining_item_id_batches — call model_values again with each batch as
        item_ids and the same periods, then merge the returned items by id.
        Only trusted-local path calls reuse a returned model_handle_token; hosted calls
        repeat the exact current_model identity.
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_values_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                item_ids=item_ids,
                periods=periods,
                historical_cutoff_year=historical_cutoff_year,
            ),
        )

    def model_drivers(
        file_path: str | None = None,
        item_id: str = "",
        depth: int = 3,
        historical_cutoff_year: int | None = None,
        current_model: dict[str, Any] | None = None,
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Trace the upstream driver tree for a line item.

        Hosted: copy the seven identity fields from get_current_model into
        current_model and omit file_path. Trusted-local operator calls may instead pass an explicit
        file_path. Use model_find or model_summarize(include_items=True) to obtain
        item_id. Increase depth only when the first trace misses relevant drivers.
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_drivers_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                item_id=item_id,
                depth=depth,
                historical_cutoff_year=historical_cutoff_year,
            ),
        )

    def model_sensitivity(
        file_path: str | None = None,
        target_id: str = "",
        n: int = 15,
        bump_pct: float = 0.10,
        candidate_filter: str = "drivers",
        max_candidates: int | None = None,
        candidate_ids: list[str] | None = None,
        sensitivity_mode: str | None = None,
        recompute_policy: str = "projection_safe",
        historical_cutoff_year: int | None = None,
        current_model: dict[str, Any] | None = None,
        view: str = "items",
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Rank upstream inputs by impact on a target metric. Can be slow.

        Hosted: copy the seven identity fields from get_current_model into
        current_model and omit file_path. Trusted-local operator calls may instead pass an explicit
        file_path. Use model_find or model_summarize(include_items=True) to obtain
        target_id. Use candidate_filter='drivers' for focused
        upstream sensitivity before broadening the candidate set. Pass candidate_ids
        to evaluate a specific upstream candidate row set instead of the global top-N
        preselection. By default this uses projection-safe semantics, which respect
        workbook period overrides; pass recompute_policy='legacy_global' only for
        compatibility diagnostics.

        view (hosted current_model only) reads the exact ModelVersion's judgment
        rows and References instead of workbook items, recomputed on read:
        view='judgments' ranks every forecast-assumption and valuation-policy row
        the compiled cells cite by the target's change per 1% of the row (target_id
        defaults to the DCF value per share); view='compare' returns the Variance
        of the model against the pinned price and each consensus Claim (both sides
        and the delta, every accounting basis the model carries); view='imply'
        returns, for each row alone, the value at which the model reproduces the
        pinned price (reverse DCF).
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_sensitivity_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                target_id=target_id,
                n=n,
                bump_pct=bump_pct,
                candidate_filter=candidate_filter,
                max_candidates=max_candidates,
                candidate_ids=candidate_ids,
                sensitivity_mode=sensitivity_mode,
                recompute_policy=recompute_policy,
                historical_cutoff_year=historical_cutoff_year,
                view=view,
            ),
        )

    def model_scenario(
        file_path: str | None = None,
        overrides: dict[str, dict[str, float]] | str | None = None,
        compare_items: list[str] | str | None = None,
        recompute_policy: str = "projection_safe",
        historical_cutoff_year: int | None = None,
        current_model: dict[str, Any] | None = None,
        *,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Apply input overrides and compare resulting metrics against the base case.

        Hosted: copy the seven identity fields from get_current_model into
        current_model and omit file_path. Trusted-local operator calls may instead pass an explicit
        file_path. Use model_find or model_summarize(include_items=True) to choose override item ids and
        compare_items before running the scenario. Each comparison includes
        base_values/scenario_values by period; use those instead of a follow-up
        model_values call after a successful scenario readback.
        Only trusted-local path calls reuse a returned model_handle_token; hosted calls
        repeat the exact current_model identity.
        """
        return _execute_model_read(
            parent_namespace,
            file_path=file_path,
            current_model=current_model,
            ctx=ctx,
            execute=lambda deps, resolved_file_path: _parent_handler(
                parent_namespace,
                "_model_scenario_handler",
            )(
                deps=deps,
                file_path=resolved_file_path,
                overrides=overrides,
                compare_items=compare_items,
                recompute_policy=recompute_policy,
                historical_cutoff_year=historical_cutoff_year,
            ),
        )

    return ModelReadToolFunctions(
        model_summarize=model_summarize,
        model_valuation_summary=model_valuation_summary,
        model_presentation_fingerprint=model_presentation_fingerprint,
        model_presentation_compare=model_presentation_compare,
        model_find=model_find,
        model_values=model_values,
        model_drivers=model_drivers,
        model_sensitivity=model_sensitivity,
        model_scenario=model_scenario,
    )


__all__ = [
    "ModelReadDeps",
    "ModelReadSource",
    "ModelReadToolFunctions",
    "build_model_read_tool_functions",
    "_attach_scenario_no_effect_guidance",
    "_is_number",
    "_is_scenario_table_value_item",
    "_merge_valuation_input_readiness",
    "_model_quality_readiness_for_valuation_summary",
    "_model_quality_summary",
    "_projection_periods_for_scenario_table_item",
    "_scenario_no_effect_warning",
    "_scenario_table_scalar_value",
    "_quality_status",
    "_upsert_quality_issue",
    "_valuation_input_quality_issue",
    "model_drivers_handler",
    "model_find_handler",
    "model_presentation_compare_handler",
    "model_presentation_fingerprint_handler",
    "model_scenario_handler",
    "model_sensitivity_handler",
    "model_summarize_handler",
    "model_valuation_summary_handler",
    "model_values_handler",
    "register_model_read_tools",
]
