from __future__ import annotations

from typing import Any

from .model_ref import ModelVersionRef
from .thesis import (
    BusinessModelRef,
    DecisionsLogEntry,
    GaapNonGaapBridge,
    PositionMetadata,
    QuantitativeFraming,
    ThesisFromIdea,
    ThesisLink,
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


def _parse_quantitative_framing(
    body_lines: list[str],
    state: _ParseState,
) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    payload: dict[str, Any] = {}
    top_level_lines: list[str] = []
    for line in body_lines:
        if line.startswith("### "):
            break
        top_level_lines.append(line)
    top_labeled = _collect_labeled_lines(top_level_lines)
    model_ref = _parse_jsonish_value(top_labeled.get("model_ref_json", ""))
    if model_ref is not None:
        payload["model_ref"] = model_ref
    if top_labeled.get("as_of"):
        payload["as_of"] = top_labeled["as_of"]
    if "Revenue" in sections:
        labeled = _collect_labeled_lines(sections["Revenue"])
        payload["revenue"] = {
            "claim_id": labeled.get("claim_id"),
            "source_refs": _parse_source_token_list(labeled.get("sources", ""), state),
            "base": _parse_jsonish_value(labeled.get("base", "")),
            "bull": _parse_jsonish_value(labeled.get("bull", "")),
            "bear": _parse_jsonish_value(labeled.get("bear", "")),
            "rationale": labeled.get("rationale"),
        }
    if "Margins" in sections:
        labeled = _collect_labeled_lines(sections["Margins"])
        payload["margins"] = {
            "claim_id": labeled.get("claim_id"),
            "source_refs": _parse_source_token_list(labeled.get("sources", ""), state),
            "trajectory": labeled.get("trajectory"),
            "key_drivers": [
                part.strip()
                for part in labeled.get("key_drivers", "").split(";")
                if part.strip()
            ],
        }
    if "EPS / FCF" in sections:
        eps_lines = []
        for line in sections["EPS / FCF"]:
            if line.startswith("#### "):
                break
            eps_lines.append(line)
        labeled = _collect_labeled_lines(eps_lines)
        payload["eps_fcf"] = {
            "claim_id": labeled.get("claim_id"),
            "source_refs": _parse_source_token_list(labeled.get("sources", ""), state),
            "projection": _parse_jsonish_value(labeled.get("projection", "")),
            "delta_vs_consensus": _parse_jsonish_value(
                labeled.get("delta_vs_consensus", "")
            ),
            "terminal_year": _parse_jsonish_value(labeled.get("terminal_year", "")),
            "basis": _nullable_cell(labeled.get("basis")),
        }
        for title, bridge_lines in _split_nested_subsections(
            sections["EPS / FCF"]
        ).items():
            if _normalize_label_key(title) != "gaap_non_gaap_bridge":
                continue
            bridge = _parse_gaap_non_gaap_bridge(bridge_lines, state)
            if bridge is not None:
                payload["eps_fcf"]["bridge"] = bridge
            break
    if "Scenarios" in sections:
        payload["scenarios"] = {}
        for title, scenario_lines in _split_nested_subsections(
            sections["Scenarios"]
        ).items():
            labeled = _collect_labeled_lines(scenario_lines)
            payload["scenarios"][_normalize_label_key(title)] = {
                "target_price": labeled.get("target_price"),
                "return_pct": labeled.get("return_pct"),
                "scenario_multiple": _parse_jsonish_value(
                    labeled.get("scenario_multiple_json", "")
                ),
                "revenue_m": _parse_jsonish_value(labeled.get("revenue_m", "")),
                "probability": _parse_jsonish_value(labeled.get("probability", "")),
                "op_margin_pct": _parse_jsonish_value(
                    labeled.get("op_margin_pct", "")
                ),
                "ebitda_margin_pct": _parse_jsonish_value(
                    labeled.get("ebitda_margin_pct", "")
                ),
                "eps": _parse_jsonish_value(labeled.get("eps", "")),
                "eps_gaap": _parse_jsonish_value(labeled.get("eps_gaap", "")),
                "eps_non_gaap": _parse_jsonish_value(
                    labeled.get("eps_non_gaap", "")
                ),
                "adj_eps": _parse_jsonish_value(labeled.get("adj_eps", "")),
                "fcf_per_share": _parse_jsonish_value(
                    labeled.get("fcf_per_share", "")
                ),
                "what_has_to_happen": labeled.get("what_has_to_happen"),
                "methodology": labeled.get("methodology"),
                "what_goes_wrong": labeled.get("what_goes_wrong"),
            }
    validated = _safe_validate(
        QuantitativeFraming,
        payload,
        state,
        "quantitative_framing",
    )
    return {"quantitative_framing": validated} if validated is not None else {}


def _parse_gaap_non_gaap_bridge(
    body_lines: list[str],
    state: _ParseState,
) -> GaapNonGaapBridge | None:
    labeled = _collect_labeled_lines(body_lines)
    components_value = (
        labeled.get("bridge_components_json", "")
        or labeled.get("bridge_components", "")
    )
    components = _parse_jsonish_value(components_value)
    if components is None:
        bridge_components: list[str] = []
    elif isinstance(components, list):
        bridge_components = [str(item) for item in components]
    else:
        bridge_components = [
            part.strip() for part in str(components).split(";") if part.strip()
        ]
    return _safe_validate(
        GaapNonGaapBridge,
        {
            "bridge_id": labeled.get("bridge_id"),
            "metric": labeled.get("metric"),
            "period": labeled.get("period"),
            "gaap_value": _parse_jsonish_value(labeled.get("gaap_value", "")),
            "non_gaap_value": _parse_jsonish_value(labeled.get("non_gaap_value", "")),
            "bridge_value": _parse_jsonish_value(labeled.get("bridge_value", "")),
            "bridge_components": bridge_components,
            "rationale": labeled.get("rationale"),
            "approximation_quality": labeled.get("approximation_quality"),
            "source_refs": _parse_source_token_list(
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
    sections = _split_subsections(body_lines)
    labeled = _collect_labeled_lines(sections.pop("", []))
    payload: dict[str, Any] = {"date_initiated": labeled.get("date_initiated")}
    if "Position Size" in sections:
        size = _collect_labeled_lines(sections["Position Size"])
        payload["position_size"] = {
            "target_pct": size.get("target_pct"),
            "current_pct": size.get("current_pct"),
        }
    if "Portfolio Fit" in sections:
        fit = _collect_labeled_lines(sections["Portfolio Fit"])
        payload["portfolio_fit"] = {
            "sector_exposure": fit.get("sector_exposure"),
            "factor_exposure": fit.get("factor_exposure"),
            "correlation_cluster": fit.get("correlation_cluster"),
            "dominant_factor_stability": _parse_jsonish_value(
                fit.get("dominant_factor_stability", "")
            ),
            "idiosyncratic_volatility_annualized_pct": _parse_jsonish_value(
                fit.get("idiosyncratic_volatility_annualized_pct", "")
            ),
            "market_only_decomposition": _parse_jsonish_value(
                fit.get("market_only_decomposition_json", "")
            ),
            "derived_from": _parse_jsonish_value(fit.get("derived_from_json", "")),
        }
    validated = _safe_validate(
        PositionMetadata,
        payload,
        state,
        "position_metadata",
    )
    return {"position_metadata": validated} if validated is not None else {}


def _parse_from_idea(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    labeled = _collect_labeled_lines(body_lines)
    remaining_lines = list(body_lines)
    hypothesis_lines: list[str] = []
    for line in remaining_lines:
        if _LABELED_LINE_RE.match(line.strip()):
            continue
        hypothesis_lines.append(line)
    validated = _safe_validate(
        ThesisFromIdea,
        {
            "idea_id": labeled.get("idea_id"),
            "seeded_at": labeled.get("seeded_at"),
            "schema_version": labeled.get("schema_version") or "1.0",
            "thesis_hypothesis": "\n".join(_trim_blank_edges(hypothesis_lines)),
        },
        state,
        "from_idea",
    )
    return {"from_idea": validated} if validated is not None else {}


def _parse_model_linkage(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    payload: dict[str, Any] = {}
    if "Model Reference" in sections:
        labeled = _collect_labeled_lines(sections["Model Reference"])
        model_ref = _safe_validate(
            ModelVersionRef,
            {
                "model_id": labeled.get("model_id"),
                "version": labeled.get("version"),
            },
            state,
            "model_links",
        )
        if model_ref is not None:
            payload["model_ref"] = model_ref
    if "Business Model Reference" in sections:
        labeled = _collect_labeled_lines(sections["Business Model Reference"])
        business_model_ref = _safe_validate(
            BusinessModelRef,
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
        for row in _parse_table(sections["Links"]):
            validated = _safe_validate(
                ThesisLink,
                {
                    "thesis_link_id": row.get("Link ID") or None,
                    "thesis_point_id": row.get("Point ID"),
                    "category": row.get("Category"),
                    "thesis_direction": row.get("Thesis Direction"),
                    "driver_key": row.get("Driver Key") or None,
                    "required_role": row.get("Required Role") or None,
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
                    "structural_fingerprint": _parse_jsonish_value(
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
    entries = []
    for row in _parse_table(body_lines):
        validated = _safe_validate(
            DecisionsLogEntry,
            {
                "entry_id": row.get("Entry ID") or None,
                "date": row.get("Date") or None,
                "skill": row.get("Skill") or None,
                "verdict": row.get("Verdict") or None,
                "decision": row.get("Decision") or None,
                "rationale": row.get("Rationale") or None,
                "previous_value": _parse_jsonish_value(row.get("Previous Value", "")),
                "new_value": _parse_jsonish_value(row.get("New Value", "")),
                "patch_ops_applied": _parse_jsonish_value(
                    row.get("Patch Ops JSON", "")
                )
                or [],
                "run_id": row.get("Run ID") or None,
                "artifact_refs": _parse_jsonish_value(row.get("Artifact Refs JSON", ""))
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
