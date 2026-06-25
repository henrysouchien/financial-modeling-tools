from __future__ import annotations

import sys
from typing import Any

from .thesis import (
    BusinessModelRef,
    DecisionsLogEntry,
    GaapNonGaapBridge,
    PositionMetadata,
    QuantitativeFraming,
    ThesisFromIdea,
    ThesisLink,
    ThesisModelRef,
)
from .thesis_markdown_parse_support import (
    _LABELED_LINE_RE,
    _ParseState,
    _collect_labeled_lines,
    _normalize_label_key,
    _safe_validate,
    _split_nested_subsections,
    _split_subsections,
    _trim_blank_edges,
)
from .thesis_markdown_utils import (
    _nullable_cell,
    _parse_jsonish_value,
    _parse_source_token_list,
    _parse_table,
)


_PARENT_MODULES = ("schema.thesis_markdown", "schema.thesis_markdown_parse")


def _compat(name: str, fallback: Any) -> Any:
    first_available = fallback
    for module_name in _PARENT_MODULES:
        parent = sys.modules.get(module_name)
        if parent is not None and hasattr(parent, name):
            value = getattr(parent, name)
            if first_available is fallback:
                first_available = value
            if value is not fallback:
                return value
    return first_available


def _parse_quantitative_framing(
    body_lines: list[str],
    state: _ParseState,
) -> dict[str, Any]:
    split_subsections = _compat("_split_subsections", _split_subsections)
    collect_labeled_lines = _compat("_collect_labeled_lines", _collect_labeled_lines)
    parse_jsonish_value = _compat("_parse_jsonish_value", _parse_jsonish_value)
    nullable_cell = _compat("_nullable_cell", _nullable_cell)
    split_nested_subsections = _compat(
        "_split_nested_subsections",
        _split_nested_subsections,
    )
    normalize_label_key = _compat("_normalize_label_key", _normalize_label_key)
    parse_gaap_non_gaap_bridge = _compat(
        "_parse_gaap_non_gaap_bridge",
        _parse_gaap_non_gaap_bridge,
    )
    safe_validate = _compat("_safe_validate", _safe_validate)
    quantitative_framing_model = _compat("QuantitativeFraming", QuantitativeFraming)

    sections = split_subsections(body_lines)
    payload: dict[str, Any] = {}
    if "Revenue" in sections:
        labeled = collect_labeled_lines(sections["Revenue"])
        payload["revenue"] = {
            "base": parse_jsonish_value(labeled.get("base", "")),
            "bull": parse_jsonish_value(labeled.get("bull", "")),
            "bear": parse_jsonish_value(labeled.get("bear", "")),
            "rationale": labeled.get("rationale"),
        }
    if "Margins" in sections:
        labeled = collect_labeled_lines(sections["Margins"])
        payload["margins"] = {
            "trajectory": labeled.get("trajectory"),
            "key_drivers": [
                part.strip()
                for part in labeled.get("key_drivers", "").split(";")
                if part.strip()
            ],
        }
    if "EPS / FCF" in sections:
        labeled = collect_labeled_lines(sections["EPS / FCF"])
        payload["eps_fcf"] = {
            "projection": parse_jsonish_value(labeled.get("projection", "")),
            "delta_vs_consensus": parse_jsonish_value(
                labeled.get("delta_vs_consensus", "")
            ),
            "terminal_year": parse_jsonish_value(labeled.get("terminal_year", "")),
            "basis": nullable_cell(labeled.get("basis")),
        }
        for title, bridge_lines in split_nested_subsections(
            sections["EPS / FCF"]
        ).items():
            if normalize_label_key(title) != "gaap_non_gaap_bridge":
                continue
            bridge = parse_gaap_non_gaap_bridge(bridge_lines, state)
            if bridge is not None:
                payload["eps_fcf"]["bridge"] = bridge
            break
    if "Scenarios" in sections:
        payload["scenarios"] = {}
        for title, scenario_lines in split_nested_subsections(
            sections["Scenarios"]
        ).items():
            labeled = collect_labeled_lines(scenario_lines)
            payload["scenarios"][normalize_label_key(title)] = {
                "target_price": labeled.get("target_price"),
                "return_pct": labeled.get("return_pct"),
                "revenue_m": parse_jsonish_value(labeled.get("revenue_m", "")),
                "probability": parse_jsonish_value(labeled.get("probability", "")),
                "op_margin_pct": parse_jsonish_value(
                    labeled.get("op_margin_pct", "")
                ),
                "ebitda_margin_pct": parse_jsonish_value(
                    labeled.get("ebitda_margin_pct", "")
                ),
                "eps": parse_jsonish_value(labeled.get("eps", "")),
                "eps_gaap": parse_jsonish_value(labeled.get("eps_gaap", "")),
                "eps_non_gaap": parse_jsonish_value(
                    labeled.get("eps_non_gaap", "")
                ),
                "adj_eps": parse_jsonish_value(labeled.get("adj_eps", "")),
                "fcf_per_share": parse_jsonish_value(
                    labeled.get("fcf_per_share", "")
                ),
                "what_has_to_happen": labeled.get("what_has_to_happen"),
                "methodology": labeled.get("methodology"),
                "what_goes_wrong": labeled.get("what_goes_wrong"),
            }
    validated = safe_validate(
        quantitative_framing_model,
        payload,
        state,
        "quantitative_framing",
    )
    return {"quantitative_framing": validated} if validated is not None else {}


def _parse_gaap_non_gaap_bridge(
    body_lines: list[str],
    state: _ParseState,
) -> GaapNonGaapBridge | None:
    collect_labeled_lines = _compat("_collect_labeled_lines", _collect_labeled_lines)
    parse_jsonish_value = _compat("_parse_jsonish_value", _parse_jsonish_value)
    parse_source_token_list = _compat(
        "_parse_source_token_list",
        _parse_source_token_list,
    )
    safe_validate = _compat("_safe_validate", _safe_validate)
    bridge_model = _compat("GaapNonGaapBridge", GaapNonGaapBridge)

    labeled = collect_labeled_lines(body_lines)
    components_value = (
        labeled.get("bridge_components_json", "")
        or labeled.get("bridge_components", "")
    )
    components = parse_jsonish_value(components_value)
    if components is None:
        bridge_components: list[str] = []
    elif isinstance(components, list):
        bridge_components = [str(item) for item in components]
    else:
        bridge_components = [
            part.strip() for part in str(components).split(";") if part.strip()
        ]
    return safe_validate(
        bridge_model,
        {
            "metric": labeled.get("metric"),
            "period": labeled.get("period"),
            "gaap_value": parse_jsonish_value(labeled.get("gaap_value", "")),
            "non_gaap_value": parse_jsonish_value(labeled.get("non_gaap_value", "")),
            "bridge_value": parse_jsonish_value(labeled.get("bridge_value", "")),
            "bridge_components": bridge_components,
            "rationale": labeled.get("rationale"),
            "approximation_quality": labeled.get("approximation_quality"),
            "source_refs": parse_source_token_list(
                labeled.get("sources", "") or labeled.get("source_refs", ""),
                state,
            ),
        },
        state,
        "quantitative_framing",
    )


def _parse_position_metadata(
    body_lines: list[str],
    state: _ParseState,
) -> dict[str, Any]:
    split_subsections = _compat("_split_subsections", _split_subsections)
    collect_labeled_lines = _compat("_collect_labeled_lines", _collect_labeled_lines)
    parse_jsonish_value = _compat("_parse_jsonish_value", _parse_jsonish_value)
    safe_validate = _compat("_safe_validate", _safe_validate)
    position_metadata_model = _compat("PositionMetadata", PositionMetadata)

    sections = split_subsections(body_lines)
    labeled = collect_labeled_lines(sections.pop("", []))
    payload: dict[str, Any] = {"date_initiated": labeled.get("date_initiated")}
    if "Position Size" in sections:
        size = collect_labeled_lines(sections["Position Size"])
        payload["position_size"] = {
            "target_pct": size.get("target_pct"),
            "current_pct": size.get("current_pct"),
        }
    if "Portfolio Fit" in sections:
        fit = collect_labeled_lines(sections["Portfolio Fit"])
        payload["portfolio_fit"] = {
            "sector_exposure": fit.get("sector_exposure"),
            "factor_exposure": fit.get("factor_exposure"),
            "correlation_cluster": fit.get("correlation_cluster"),
            "dominant_factor_stability": parse_jsonish_value(
                fit.get("dominant_factor_stability", "")
            ),
            "idiosyncratic_volatility_annualized_pct": parse_jsonish_value(
                fit.get("idiosyncratic_volatility_annualized_pct", "")
            ),
            "market_only_decomposition": parse_jsonish_value(
                fit.get("market_only_decomposition_json", "")
            ),
        }
    validated = safe_validate(
        position_metadata_model,
        payload,
        state,
        "position_metadata",
    )
    return {"position_metadata": validated} if validated is not None else {}


def _parse_from_idea(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    collect_labeled_lines = _compat("_collect_labeled_lines", _collect_labeled_lines)
    labeled_line_re = _compat("_LABELED_LINE_RE", _LABELED_LINE_RE)
    trim_blank_edges = _compat("_trim_blank_edges", _trim_blank_edges)
    safe_validate = _compat("_safe_validate", _safe_validate)
    thesis_from_idea_model = _compat("ThesisFromIdea", ThesisFromIdea)

    labeled = collect_labeled_lines(body_lines)
    remaining_lines = list(body_lines)
    hypothesis_lines: list[str] = []
    for line in remaining_lines:
        if labeled_line_re.match(line.strip()):
            continue
        hypothesis_lines.append(line)
    validated = safe_validate(
        thesis_from_idea_model,
        {
            "idea_id": labeled.get("idea_id"),
            "seeded_at": labeled.get("seeded_at"),
            "schema_version": labeled.get("schema_version") or "1.0",
            "thesis_hypothesis": "\n".join(trim_blank_edges(hypothesis_lines)),
        },
        state,
        "from_idea",
    )
    return {"from_idea": validated} if validated is not None else {}


def _parse_model_linkage(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    split_subsections = _compat("_split_subsections", _split_subsections)
    collect_labeled_lines = _compat("_collect_labeled_lines", _collect_labeled_lines)
    parse_table = _compat("_parse_table", _parse_table)
    parse_jsonish_value = _compat("_parse_jsonish_value", _parse_jsonish_value)
    safe_validate = _compat("_safe_validate", _safe_validate)
    thesis_model_ref_model = _compat("ThesisModelRef", ThesisModelRef)
    business_model_ref_model = _compat("BusinessModelRef", BusinessModelRef)
    thesis_link_model = _compat("ThesisLink", ThesisLink)

    sections = split_subsections(body_lines)
    payload: dict[str, Any] = {}
    if "Model Reference" in sections:
        labeled = collect_labeled_lines(sections["Model Reference"])
        model_ref = safe_validate(
            thesis_model_ref_model,
            {
                "model_id": labeled.get("model_id"),
                "version": labeled.get("version"),
                "file_path": labeled.get("file_path"),
                "last_updated": labeled.get("last_updated"),
                "drivers_locked": [
                    part.strip()
                    for part in labeled.get("drivers_locked", "").split(",")
                    if part.strip()
                ],
            },
            state,
            "model_links",
        )
        if model_ref is not None:
            payload["model_ref"] = model_ref
    if "Business Model Reference" in sections:
        labeled = collect_labeled_lines(sections["Business Model Reference"])
        business_model_ref = safe_validate(
            business_model_ref_model,
            {
                "business_model_id": labeled.get("id"),
                "schema_version": labeled.get("schema_version") or "1.0",
                "revision": labeled.get("revision"),
                "last_updated": labeled.get("last_updated"),
            },
            state,
            "model_links",
        )
        if business_model_ref is not None:
            payload["business_model_ref"] = business_model_ref
    if "Links" in sections:
        links: list[ThesisLink] = []
        for row in parse_table(sections["Links"]):
            validated = safe_validate(
                thesis_link_model,
                {
                    "thesis_link_id": row.get("Link ID") or None,
                    "thesis_point_id": row.get("Point ID"),
                    "category": row.get("Category"),
                    "thesis_direction": row.get("Thesis Direction"),
                    "driver_key": row.get("Driver Key") or None,
                    "data_concept_id": row.get("Data Concept ID") or None,
                    "model_item_id": row.get("Model Item ID") or None,
                    "business_model_node_id": row.get("BM Node") or None,
                    "template_version": row.get("Template Version") or None,
                    "model_id": row.get("Model ID") or None,
                    "periods": [
                        int(part)
                        for part in row.get("Periods", "").split(",")
                        if part.strip()
                    ],
                    "thesis_value": row.get("Thesis Value") or None,
                    "consensus_value": row.get("Consensus Value") or None,
                    "structural_fingerprint": parse_jsonish_value(
                        row.get("Structural Fingerprint JSON", "")
                    ),
                    "thesis_text": row.get("Thesis Text"),
                },
                state,
                "model_links",
            )
            if validated is not None:
                links.append(validated)
        payload["model_links"] = links
    return payload


def _parse_decisions_log(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    parse_table = _compat("_parse_table", _parse_table)
    parse_jsonish_value = _compat("_parse_jsonish_value", _parse_jsonish_value)
    safe_validate = _compat("_safe_validate", _safe_validate)
    decisions_log_entry_model = _compat("DecisionsLogEntry", DecisionsLogEntry)

    entries = []
    for row in parse_table(body_lines):
        validated = safe_validate(
            decisions_log_entry_model,
            {
                "entry_id": row.get("Entry ID") or None,
                "date": row.get("Date") or None,
                "skill": row.get("Skill") or None,
                "verdict": row.get("Verdict") or None,
                "decision": row.get("Decision") or None,
                "rationale": row.get("Rationale") or None,
                "previous_value": parse_jsonish_value(row.get("Previous Value", "")),
                "new_value": parse_jsonish_value(row.get("New Value", "")),
                "patch_ops_applied": parse_jsonish_value(
                    row.get("Patch Ops JSON", "")
                )
                or [],
                "run_id": row.get("Run ID") or None,
                "artifact_refs": parse_jsonish_value(row.get("Artifact Refs JSON", ""))
                or [],
            },
            state,
            "decisions_log",
        )
        if validated is not None:
            entries.append(validated)
    return {"decisions_log": entries}


__all__ = [
    "_parse_decisions_log",
    "_parse_from_idea",
    "_parse_gaap_non_gaap_bridge",
    "_parse_model_linkage",
    "_parse_position_metadata",
    "_parse_quantitative_framing",
]
