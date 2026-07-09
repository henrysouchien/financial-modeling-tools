"""BusinessModel validation helpers shared by MCP and FMS writer paths."""

from __future__ import annotations

from typing import Any
import json
import re

from pydantic import ValidationError

from schema.business_model import BusinessModel, DriverAssumptionPlan, derive_driver_assumption_plan
from schema.business_model_compiler import compile_business_model
from schema.driver_resolver import load_driver_mapping, resolve_driver_key
from schema.kpi_overrides_writer import validate_kpi_source_reference
from schema.segments import SEGMENT_AXES_PRIORITY, discover_all_axes
from schema.templates import load_sia_generic_template
from schema.tools import model_tool_error_payload


def _safe_pydantic_errors(exc: ValidationError) -> list[dict[str, Any]]:
    try:
        decoded = json.loads(exc.json())
        return decoded if isinstance(decoded, list) else []
    except Exception:
        return [{"msg": str(exc)}]


def validation_error(
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "error_code": "business_model_validation_error",
        "message": message,
        "details": details or {},
    }


def _walk_business_model_nodes(parsed: BusinessModel):
    def _walk(segment, node, path: tuple[str, ...]):
        yield segment, node, path
        for child in node.children or []:
            yield from _walk(segment, child, (*path, child.id))

    for segment in parsed.segments:
        for node in segment.revenue_model.decomposition:
            yield from _walk(segment, node, (node.id,))


_REVENUE_KPI_SOURCE_LOCAL_TOKENS = frozenset({
    "netsales",
    "revenue",
    "revenuefromcontractwithcustomerexcludingassessedtax",
    "revenues",
    "revenuenotfromcontractwithcustomer",
    "sales",
    "salesrevenuegoodsnet",
    "salesrevenuenet",
})
_NON_DIRECT_REVENUE_SOURCE_WORDS = frozenset({
    "arpu",
    "average",
    "avg",
    "change",
    "count",
    "counts",
    "growth",
    "margin",
    "per",
    "percent",
    "percentage",
    "price",
    "quantity",
    "rate",
    "unit",
    "units",
    "volume",
    "yield",
})
_REVENUE_SOURCE_FORBIDDEN_NODE_TOKENS = frozenset({
    "account_count",
    "arpu",
    "asp",
    "average_revenue_per",
    "balance",
    "client_count",
    "count",
    "customer_count",
    "employee_count",
    "per_account",
    "per_client",
    "per_customer",
    "per_employee",
    "per_user",
    "price",
    "subscriber_count",
    "take_rate",
    "user_count",
    "yield",
})
_CAMEL_WORD_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _node_unit_value(node: Any) -> str:
    return str(getattr(getattr(node, "unit", None), "value", getattr(node, "unit", "")) or "").strip().lower()


def _node_factor_values(node: Any) -> list[str]:
    return [
        str(getattr(factor, "value", factor) or "").strip().lower()
        for factor in getattr(node, "factors", []) or []
    ]


def _is_external_price_leaf(node: Any) -> bool:
    driver = getattr(node, "driver", None)
    return (
        getattr(driver, "type", None) == "external"
        and _node_factor_values(node) == ["price"]
        and not getattr(node, "children", None)
    )


def _dollar_model_scale_warning(segment: Any, node: Any, path: tuple[str, ...]) -> dict[str, Any] | None:
    if _node_unit_value(node) != "dollars":
        return None
    if getattr(node, "model_scale", None) is not None:
        return None
    compile_to = getattr(node, "compile_to", None)
    target_type = getattr(compile_to, "target_type", None)
    if str(getattr(target_type, "value", target_type) or "") not in {"assumption_row", "derived_row"}:
        return None
    if _is_external_price_leaf(node):
        return None
    return {
        "code": "missing_dollar_model_scale",
        "reason": "materialized_dollar_node_without_model_scale",
        "segment_id": segment.id,
        "node_id": node.id,
        "qualified_id": f"{segment.id}.{'.'.join(path)}",
        "unit": "dollars",
        "compile_to": str(getattr(target_type, "value", target_type) or ""),
        "inferred_model_scale": "millions",
        "loc": [
            "segments",
            segment.id,
            "revenue_model",
            "decomposition",
            *path,
            "model_scale",
        ],
        "message": (
            "Dollar BusinessModel nodes that materialize model rows must declare "
            "model_scale so downstream seeds do not rely on compiler fallback. Use "
            "'millions' for statement/aggregate dollar rows, or 'units' for raw "
            "per-unit price rows."
        ),
    }


def _is_revenue_kpi_source(value: object | None) -> bool:
    local = _tag_local_name(value)
    if local in _REVENUE_KPI_SOURCE_LOCAL_TOKENS:
        return True
    words = set(_tag_local_words(value))
    if words & _NON_DIRECT_REVENUE_SOURCE_WORDS:
        return False
    return bool(words & {"revenue", "revenues", "sales"})


def _node_identity_tokens(node: Any, path: tuple[str, ...]) -> set[str]:
    values = [getattr(node, "id", ""), getattr(node, "label", ""), *path]
    tokens: set[str] = set()
    for value in values:
        text = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
        if not text:
            continue
        tokens.add(text)
        parts = [part for part in text.split("_") if part]
        tokens.update(parts)
        for size in (2, 3):
            tokens.update("_".join(parts[index : index + size]) for index in range(len(parts) - size + 1))
    return tokens


def _node_has_non_direct_revenue_metric_identity(identity: set[str]) -> bool:
    if identity & _REVENUE_SOURCE_FORBIDDEN_NODE_TOKENS:
        return True
    if "per" in identity:
        return True
    return "average" in identity and "revenue" in identity


def _revenue_kpi_source_metric_mismatch(segment: Any, node: Any, path: tuple[str, ...]) -> dict[str, Any] | None:
    if not _is_revenue_kpi_source(getattr(node, "kpi_source", None)):
        return None
    unit = _node_unit_value(node)
    identity = _node_identity_tokens(node, path)
    if unit == "dollars" and not _node_has_non_direct_revenue_metric_identity(identity):
        return None
    return {
        "code": "kpi_source_metric_mismatch",
        "reason": "revenue_source_for_non_revenue_node",
        "segment_id": segment.id,
        "node_id": node.id,
        "qualified_id": f"{segment.id}.{'.'.join(path)}",
        "unit": unit or None,
        "kpi_source": node.kpi_source,
        "loc": [
            "segments",
            segment.id,
            "revenue_model",
            "decomposition",
            *path,
            "kpi_source",
        ],
        "message": (
            "Revenue XBRL concepts can source direct revenue nodes only; use a KPI "
            "source for this node's own metric, set kpi=false, or collapse to a "
            "source-backed direct segment_revenue node."
        ),
    }


def contract_checks(
    parsed: BusinessModel,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for segment, node, path in _walk_business_model_nodes(parsed):
        scale_issue = _dollar_model_scale_warning(segment, node, path)
        if scale_issue is not None:
            warnings.append(scale_issue)
        if not node.kpi:
            continue
        ok, reason = validate_kpi_source_reference(node.kpi_source)
        if ok:
            semantic_issue = _revenue_kpi_source_metric_mismatch(segment, node, path)
            if semantic_issue is not None:
                errors.append(semantic_issue)
            continue
        issue = {
            "code": "missing_kpi_source" if reason == "missing" else "invalid_kpi_source",
            "reason": reason or "invalid",
            "segment_id": segment.id,
            "node_id": node.id,
            "qualified_id": f"{segment.id}.{'.'.join(path)}",
            "kpi_source": node.kpi_source,
            "loc": [
                "segments",
                segment.id,
                "revenue_model",
                "decomposition",
                *path,
                "kpi_source",
            ],
            "message": "KPI nodes should use a valid tag-like metric reference in kpi_source",
        }
        if reason == "missing":
            warnings.append(issue)
            continue
        errors.append(issue)
    return errors, warnings


_SEGMENT_NAME_TOKEN_RE = re.compile(r"[^a-z0-9]+")


def _normalize_segment_name(value: object | None) -> str:
    return _SEGMENT_NAME_TOKEN_RE.sub("", str(value or "").lower())


def _tag_local_name(value: object | None) -> str:
    return str(value or "").strip().split(":", 1)[-1].lower()


def _tag_local_words(value: object | None) -> tuple[str, ...]:
    local = str(value or "").strip().split(":", 1)[-1]
    local = _CAMEL_WORD_BOUNDARY_RE.sub("_", local)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", local).strip("_").lower()
    return tuple(part for part in normalized.split("_") if part)


def _tag_namespace(value: object | None) -> str | None:
    raw = str(value or "").strip()
    if ":" not in raw:
        return None
    return raw.split(":", 1)[0].lower() or None


def _tags_equivalent(requested: object | None, returned: object | None) -> bool:
    requested_local = _tag_local_name(requested)
    returned_local = _tag_local_name(returned)
    return bool(requested_local) and requested_local == returned_local


def _axis_equivalent(requested: object | None, returned: object | None) -> bool:
    return _tags_equivalent(requested, returned)


def _member_exact_or_unqualified_discovery(requested: object | None, returned: object | None) -> bool:
    if not _tags_equivalent(requested, returned):
        return False
    returned_namespace = _tag_namespace(returned)
    if returned_namespace is None:
        return True
    requested_raw = str(requested or "").strip()
    returned_raw = str(returned or "").strip()
    return requested_raw == returned_raw


def _suggested_axis_qname(axis: object | None) -> str | None:
    raw = str(axis or "").strip()
    if not raw:
        return None
    if ":" in raw:
        return raw
    return f"srt:{raw}"


def _segment_axis_payloads(segment_discovery: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(segment_discovery, dict):
        return []
    axes = segment_discovery.get("axes")
    if not isinstance(axes, list):
        return []
    payloads: list[dict[str, Any]] = []
    for axis in axes:
        if isinstance(axis, dict):
            payloads.append(axis)
    return payloads


def _axis_priority(axis: str | None) -> int | None:
    if axis not in SEGMENT_AXES_PRIORITY:
        return None
    return SEGMENT_AXES_PRIORITY.index(axis) + 1


def discover_segments(
    parsed: BusinessModel,
    *,
    fetcher: Any,
    most_recent_fy: int | None,
    n_historical: int,
) -> dict[str, Any] | None:
    if most_recent_fy is None:
        return None
    if fetcher is None:
        return None
    ticker = str(parsed.company.ticker or "").strip()
    if not ticker:
        return None
    result = discover_all_axes(
        ticker=ticker,
        fetcher=fetcher,
        most_recent_fy=int(most_recent_fy),
        n_historical=int(n_historical),
    )
    axes: list[dict[str, Any]] = []
    for profile in result.profiles:
        axes.append(
            {
                "axis": profile.axis_used,
                "priority": _axis_priority(profile.axis_used),
                "segment_count": len(profile.segments),
                "segments": [
                    {
                        "name": segment.name,
                        "edgar_member": segment.edgar_member,
                    }
                    for segment in profile.segments
                ],
            }
        )
    return {
        "status": "ok",
        "ticker": result.ticker,
        "axes_found": len(axes),
        "axes": axes,
    }


def _segment_suggestions(
    segment: object,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    label_keys = {
        _normalize_segment_name(getattr(segment, "label", None)),
        _normalize_segment_name(getattr(segment, "match_name", None)),
        _normalize_segment_name(getattr(segment, "id", None)),
    }
    label_keys.discard("")
    label_matches = [
        candidate
        for candidate in candidates
        if _normalize_segment_name(candidate.get("name")) in label_keys
        or _normalize_segment_name(candidate.get("edgar_member")) in label_keys
    ]
    selected = label_matches or candidates
    return [
        {
            "axis": candidate.get("axis"),
            "suggested_axis": _suggested_axis_qname(candidate.get("axis")),
            "name": candidate.get("name"),
            "edgar_member": candidate.get("edgar_member"),
        }
        for candidate in selected[:8]
    ]


def segment_context_checks(
    parsed: BusinessModel,
    *,
    segment_discovery: dict[str, Any] | None,
    validation_requested: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
    axes = _segment_axis_payloads(segment_discovery)
    if not axes:
        if not validation_requested:
            return [], [], None
        reason = "no_discovered_segment_axes"
        if isinstance(segment_discovery, dict) and segment_discovery.get("status") == "error":
            reason = "segment_discovery_error"
        context = {
            "status": "skipped",
            "reason": reason,
            "axes_checked": 0,
            "segments_checked": 0,
        }
        warnings = [
            {
                "code": "segment_validation_unavailable",
                "reason": reason,
                "message": "Segment context validation was requested but no discovered segment axes were available.",
            }
        ]
        if isinstance(segment_discovery, dict) and segment_discovery.get("error"):
            warnings[0]["error"] = segment_discovery.get("error")
        return [], warnings, context

    discovered: list[dict[str, Any]] = []
    for axis_payload in axes:
        axis = axis_payload.get("axis")
        for segment_payload in axis_payload.get("segments") or []:
            if not isinstance(segment_payload, dict):
                continue
            discovered.append(
                {
                    "axis": axis,
                    "name": segment_payload.get("name"),
                    "edgar_member": segment_payload.get("edgar_member"),
                }
            )

    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    matched: list[dict[str, Any]] = []
    for segment in parsed.segments:
        declared_member = str(segment.edgar_member or "").strip()
        if declared_member:
            declared_axis = str(segment.edgar_axis or "").strip()
            axis_candidates = [
                candidate
                for candidate in discovered
                if not declared_axis or _axis_equivalent(declared_axis, candidate.get("axis"))
            ]
            exact = next(
                (
                    candidate
                    for candidate in axis_candidates
                    if _member_exact_or_unqualified_discovery(declared_member, candidate.get("edgar_member"))
                ),
                None,
            )
            if exact is not None:
                matched.append(
                    {
                        "segment_id": segment.id,
                        "declared_axis": declared_axis or None,
                        "declared_member": declared_member,
                        "matched_axis": exact.get("axis"),
                        "matched_member": exact.get("edgar_member"),
                    }
                )
                if not declared_axis:
                    errors.append(
                        {
                            "code": "segment_axis_missing",
                            "segment_id": segment.id,
                            "segment_label": segment.label,
                            "declared_member": declared_member,
                            "discovered_axis": exact.get("axis"),
                            "suggested_axis": _suggested_axis_qname(exact.get("axis")),
                            "suggested_member": exact.get("edgar_member"),
                            "message": "Segment.edgar_member matched live discovery but edgar_axis is missing; set both axis and member before build.",
                        }
                    )
                continue

            suggestions = _segment_suggestions(segment, axis_candidates or discovered)
            errors.append(
                {
                    "code": "segment_member_mismatch",
                    "segment_id": segment.id,
                    "segment_label": segment.label,
                    "declared_axis": declared_axis or None,
                    "declared_member": declared_member,
                    "suggested_segments": suggestions,
                    "message": "Segment.edgar_member does not exactly match live segment discovery for this ticker/year.",
                }
            )
        for claim_index, claim in enumerate(segment.absorbs or []):
            claim_member = str(getattr(claim, "member", "") or "").strip()
            claim_name = str(getattr(claim, "name", "") or "").strip()
            exact = next(
                (
                    candidate
                    for candidate in discovered
                    if claim_member
                    and _member_exact_or_unqualified_discovery(claim_member, candidate.get("edgar_member"))
                ),
                None,
            )
            if exact is not None:
                matched.append(
                    {
                        "segment_id": segment.id,
                        "claim_index": claim_index,
                        "claim_name": claim_name or None,
                        "declared_member": claim_member,
                        "matched_axis": exact.get("axis"),
                        "matched_member": exact.get("edgar_member"),
                    }
                )
                continue
            name_match = next(
                (
                    candidate
                    for candidate in discovered
                    if claim_name and _normalize_segment_name(candidate.get("name")) == _normalize_segment_name(claim_name)
                ),
                None,
            )
            if name_match is not None:
                warnings.append(
                    {
                        "code": "absorbed_claim_member_suggestion",
                        "segment_id": segment.id,
                        "segment_label": segment.label,
                        "claim_index": claim_index,
                        "claim_name": claim_name or None,
                        "declared_member": claim_member or None,
                        "discovered_axis": name_match.get("axis"),
                        "suggested_axis": _suggested_axis_qname(name_match.get("axis")),
                        "suggested_member": name_match.get("edgar_member"),
                        "message": "Segment.absorbs claim matched live discovery by name; add or update the member qname.",
                    }
                )
                matched.append(
                    {
                        "segment_id": segment.id,
                        "claim_index": claim_index,
                        "claim_name": claim_name or None,
                        "matched_axis": name_match.get("axis"),
                        "matched_member": name_match.get("edgar_member"),
                    }
                )

    coverage_warnings = _unclaimed_memberful_discovery_warnings(parsed, discovered)
    warnings.extend(coverage_warnings)

    context = {
        "status": "ok",
        "axes_checked": len(axes),
        "segments_checked": len([segment for segment in parsed.segments if segment.edgar_member]),
        "matches": matched,
        "coverage_scope": "memberful_discovery_candidates",
    }
    return errors, warnings, context


def _unclaimed_memberful_discovery_warnings(
    parsed: BusinessModel,
    discovered: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for candidate in discovered:
        candidate_member = str(candidate.get("edgar_member") or "").strip()
        if not candidate_member:
            continue
        if _discovery_candidate_claimed(parsed, candidate):
            continue
        warnings.append(
            {
                "code": "unclaimed_discovery_member",
                "axis": candidate.get("axis"),
                "name": candidate.get("name"),
                "edgar_member": candidate.get("edgar_member"),
                "message": (
                    "Discovered memberful EDGAR segment is not claimed by any BusinessModel "
                    "segment; add a segment edgar_member or include it in Segment.absorbs."
                ),
            }
        )
    return warnings


def _discovery_candidate_claimed(parsed: BusinessModel, candidate: dict[str, Any]) -> bool:
    candidate_name = _normalize_segment_name(candidate.get("name"))
    candidate_member = candidate.get("edgar_member")
    for segment in parsed.segments:
        if candidate_name and _normalize_segment_name(segment.match_name) == candidate_name:
            return True
        declared_member = str(segment.edgar_member or "").strip()
        if declared_member and _member_exact_or_unqualified_discovery(declared_member, candidate_member):
            return True
        for claim in segment.absorbs or []:
            claim_member = str(getattr(claim, "member", "") or "").strip()
            if claim_member and _member_exact_or_unqualified_discovery(claim_member, candidate_member):
                return True
            claim_name = _normalize_segment_name(getattr(claim, "name", None))
            if claim_name and claim_name == candidate_name:
                return True
    return False


def driver_key_checks(
    parsed: BusinessModel,
    *,
    assumption_driver_keys: list[str] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not assumption_driver_keys:
        return [], None

    registry = compile_business_model(load_sia_generic_template(), parsed)
    driver_plan_error: dict[str, Any] | None = None
    try:
        driver_plan = derive_driver_assumption_plan(parsed)
    except Exception as exc:
        driver_plan = None
        driver_plan_error = {
            "code": "driver_assumption_plan_unavailable",
            "error": type(exc).__name__,
            "message": str(exc),
        }
    accepted_driver_keys = sorted(
        set(load_driver_mapping())
        | set(registry.driver_keys)
        | (set(driver_plan.accepted_driver_keys()) if driver_plan is not None else set())
    )
    invalid: list[dict[str, Any]] = []
    valid: list[dict[str, Any]] = []
    for raw_key in assumption_driver_keys:
        driver_key = str(raw_key or "").strip()
        if not driver_key:
            invalid.append(
                {
                    "code": "invalid_driver_key",
                    "driver_key": driver_key,
                    "reason": "driver_key is required",
                    "accepted_driver_key_examples": accepted_driver_keys[:20],
                }
            )
            continue
        if driver_key not in accepted_driver_keys:
            invalid.append(
                {
                    "code": "invalid_driver_key",
                    "driver_key": driver_key,
                    "reason": "not in accepted BusinessModel or canonical driver keys",
                    "accepted_driver_key_examples": accepted_driver_keys[:20],
                    "driver_assumption_plan_key_examples": driver_plan.driver_keys[:20] if driver_plan is not None else [],
                    "business_model_driver_key_examples": sorted(registry.driver_keys)[:20],
                    "canonical_driver_key_examples": sorted(load_driver_mapping())[:20],
                }
            )
            continue
        resolved = (
            _resolve_assumption_driver_key(
                driver_key,
                compiled_registry=registry,
                driver_plan=driver_plan,
            )
            if driver_plan is not None
            else _resolve_assumption_driver_key_without_plan(
                driver_key,
                compiled_registry=registry,
            )
        )
        if isinstance(resolved, Exception):
            invalid.append(
                {
                    "code": "invalid_driver_key",
                    "driver_key": driver_key,
                    "reason": getattr(resolved, "reason", str(resolved)),
                    "accepted_driver_key_examples": accepted_driver_keys[:20],
                    "driver_assumption_plan_key_examples": driver_plan.driver_keys[:20] if driver_plan is not None else [],
                    "business_model_driver_key_examples": sorted(registry.driver_keys)[:20],
                    "canonical_driver_key_examples": sorted(load_driver_mapping())[:20],
                }
            )
            continue
        resolved_item_id, canonical_driver_key = resolved
        valid_payload = {"driver_key": driver_key, "resolved_item_id": resolved_item_id}
        if canonical_driver_key != driver_key:
            valid_payload["canonical_driver_key"] = canonical_driver_key
        valid.append(valid_payload)

    context = {
        "checked": len(assumption_driver_keys),
        "valid": valid,
        "accepted_driver_key_count": len(accepted_driver_keys),
        "driver_assumption_plan_keys": driver_plan.driver_keys if driver_plan is not None else [],
        "driver_assumption_plan_aliases": dict(driver_plan.alias_map) if driver_plan is not None else {},
        "driver_assumption_plan_error": driver_plan_error,
        "business_model_driver_keys": sorted(registry.driver_keys),
        "canonical_driver_keys": sorted(load_driver_mapping()),
    }
    return invalid, context


def _resolve_assumption_driver_key(
    driver_key: str,
    *,
    compiled_registry: Any,
    driver_plan: DriverAssumptionPlan,
) -> tuple[str, str] | Exception:
    canonical_driver_key = driver_plan.resolve_driver_key(driver_key)
    first_error: Exception | None = None
    try:
        return resolve_driver_key(driver_key, compiled_registry=compiled_registry), canonical_driver_key or driver_key
    except Exception as exc:
        first_error = exc
        if canonical_driver_key is None:
            return exc

    if canonical_driver_key is not None:
        entry = driver_plan.entry_for_driver_key(canonical_driver_key)
        if entry is not None:
            for candidate in entry.aliases:
                try:
                    return resolve_driver_key(candidate, compiled_registry=compiled_registry), canonical_driver_key
                except Exception:
                    continue
    return first_error or KeyError(driver_key)


def _resolve_assumption_driver_key_without_plan(
    driver_key: str,
    *,
    compiled_registry: Any,
) -> tuple[str, str] | Exception:
    try:
        return resolve_driver_key(driver_key, compiled_registry=compiled_registry), driver_key
    except Exception as first_exc:
        return first_exc


def summary(
    parsed: BusinessModel,
    *,
    input_format: str,
    contract_warnings: list[dict[str, Any]] | None = None,
    segment_validation: dict[str, Any] | None = None,
    driver_key_validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    segments: list[dict[str, Any]] = []
    projection_driver_keys: list[str] = []
    source_backed_kpi_nodes: list[str] = []

    for segment in parsed.segments:
        nodes: list[dict[str, Any]] = []
        for _node_segment, node, path in _walk_business_model_nodes(parsed):
            if _node_segment.id != segment.id:
                continue
            driver_type = getattr(node.driver, "type", None)
            rate_key = None
            if driver_type == "growth":
                rate_key = getattr(getattr(node.driver, "params", None), "rate_key", None)
            qualified_key = f"{segment.id}.{node.id}"
            if node.compile_to.target_type == "assumption_row":
                projection_driver_keys.append(qualified_key)
                if rate_key:
                    projection_driver_keys.append(f"{qualified_key}.{rate_key}")
            if node.kpi and node.kpi_source:
                source_backed_kpi_nodes.append(qualified_key)
            nodes.append(
                {
                    "id": node.id,
                    "unit": getattr(node.unit, "value", node.unit),
                    "model_scale": node.model_scale,
                    "kpi": bool(node.kpi),
                    "kpi_source": node.kpi_source,
                    "driver_type": driver_type,
                    "rate_key": rate_key,
                    "compile_to": node.compile_to.target_type,
                }
            )
        segments.append(
            {
                "id": segment.id,
                "label": segment.label,
                "edgar_axis": segment.edgar_axis,
                "edgar_member": segment.edgar_member,
                "absorbs": [
                    {
                        "name": claim.name,
                        "member": claim.member,
                    }
                    for claim in (segment.absorbs or [])
                ] or None,
                "revenue_model_type": segment.revenue_model.type,
                "revenue_share": segment.revenue_share,
                "nodes": nodes,
            }
        )

    return {
        "status": "ok",
        "input_format": input_format,
        "ticker": parsed.company.ticker,
        "company_name": parsed.company.name,
        "revision": parsed.metadata.revision,
        "schema_version": parsed.schema_version,
        "recommended_depth": parsed.recommended_depth,
        "segment_count": len(parsed.segments),
        "segments": segments,
        "source_backed_kpi_nodes": source_backed_kpi_nodes,
        "projection_driver_keys": projection_driver_keys,
        "validation_warnings": contract_warnings or [],
        "segment_validation": segment_validation,
        "driver_key_validation": driver_key_validation,
    }


def validate(
    parsed: BusinessModel,
    *,
    segment_discovery: dict[str, Any] | None = None,
    most_recent_fy: int | None = None,
    n_historical: int = 5,
    assumption_driver_keys: list[str] | None = None,
    fetcher: Any = None,
) -> dict[str, Any]:
    try:
        contract_errors, contract_warnings = contract_checks(parsed)
        segment_validation_requested = segment_discovery is not None or most_recent_fy is not None
        if segment_discovery is None:
            segment_discovery = discover_segments(
                parsed,
                fetcher=fetcher,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
            )
        segment_errors, segment_warnings, segment_validation = segment_context_checks(
            parsed,
            segment_discovery=segment_discovery,
            validation_requested=segment_validation_requested,
        )
        contract_errors.extend(segment_errors)
        contract_warnings.extend(segment_warnings)

        driver_errors, driver_key_validation = driver_key_checks(
            parsed,
            assumption_driver_keys=assumption_driver_keys,
        )
        contract_errors.extend(driver_errors)

        if contract_errors:
            return validation_error(
                "BusinessModel contract validation failed",
                details={
                    "contract_errors": contract_errors,
                    "segment_validation": segment_validation,
                    "driver_key_validation": driver_key_validation,
                },
            )

        return summary(
            parsed,
            input_format="object",
            contract_warnings=contract_warnings,
            segment_validation=segment_validation,
            driver_key_validation=driver_key_validation,
        )
    except ValidationError as exc:
        return validation_error(
            "BusinessModel validation failed",
            details={"pydantic_errors": _safe_pydantic_errors(exc)},
        )
    except Exception as exc:
        payload = model_tool_error_payload(exc)
        payload.setdefault("status", "error")
        payload.setdefault("error_code", "business_model_validation_error")
        return payload


__all__ = [
    "contract_checks",
    "discover_segments",
    "driver_key_checks",
    "segment_context_checks",
    "summary",
    "validate",
    "validation_error",
]
