from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from .research_labels import canonicalize_research_label
from .thesis import (
    BusinessModelRef,
    DecisionsLogEntry,
    GaapNonGaapBridge,
    PositionMetadata,
    QuantitativeFraming,
    ThesisFromIdea,
    ThesisLink,
    ThesisModelRef,
    UnknownSection,
)
from .thesis_shared_slice import (
    Assumption,
    BusinessOverview,
    Catalyst,
    CompanyField,
    CompsNarrative,
    ConsensusView,
    DataGap,
    DifferentiatedViewClaim,
    Excerpt,
    HistoricalCoincidence,
    IndustryAnalysis,
    InvalidationTrigger,
    MaterialityThreshold,
    Monitoring,
    Ownership,
    Peer,
    QualitativeFactor,
    Risk,
    SourceRecord,
    ThesisField,
    Valuation,
    WatchItem,
    _normalize_ticker,
)
from .thesis_markdown_utils import (
    _extract_source_tokens,
    _normalize_label_key,
    _nullable_cell,
    _parse_boolish,
    _parse_jsonish_value,
    _parse_key_value_segments,
    _parse_source_refs_cell,
    _parse_source_token_list,
    _parse_table,
)
from .thesis_markdown_parse_support import (  # noqa: F401
    ParseWarning,
    ParsedThesis,
    PriorNormalizedEntry,
    PriorThesisIndex,
    _ANCHOR_TO_HEADER,
    _CANONICAL_ANCHORS,
    _HEADER_TO_ANCHOR,
    _LABELED_LINE_RE,
    _MarkdownSection,
    _NUMBERED_ITEM_RE,
    _ParseState,
    _ROLE_TO_PRIOR_FIELD,
    _SECTION_HEADER_RE,
    _SECTION_SPECS,
    _SOURCE_EXCERPT_RE,
    _SUBSECTION_ID_RE,
    _SYNC_HEADER_PREFIX,
    _SYNC_VERSION_RE,
    _TITLE_PREFIX,
    _TITLE_RE,
    _TRIGGER_BULLET_RE,
    _build_prior_entry,
    _collect_labeled_lines,
    _extract_explicit_id,
    _extract_version_marker,
    _jaccard,
    _match_prior_id,
    _merge_parsed_payload,
    _normalize_match_value,
    _resolve_item_id,
    _safe_validate,
    _split_bullet_with_metadata,
    _split_nested_subsections,
    _split_sections,
    _split_subsections,
    _trim_blank_edges,
)


def parse_thesis_markdown(content: str, prior: PriorThesisIndex | None = None) -> ParsedThesis:
    normalized = content.replace("\r\n", "\n")
    lines = normalized.split("\n")
    title_index = next((index for index, line in enumerate(lines) if line.startswith(_TITLE_PREFIX)), -1)
    if title_index < 0:
        raise ValueError("markdown is missing the thesis title header")
    header_match = _TITLE_RE.match(lines[title_index].strip())
    if header_match is None:
        raise ValueError("markdown thesis title header is malformed")
    ticker = _normalize_ticker(header_match.group("ticker"))
    label = canonicalize_research_label(header_match.group("label")) or None
    version_marker, marker_present = _extract_version_marker(lines[: title_index + 1])

    sections = _split_sections(lines[title_index + 1 :])
    state = _ParseState(
        prior=prior,
        warnings=[],
        claimed_prior_ids={role: set() for role in _ROLE_TO_PRIOR_FIELD},
        referenced_source_ids=set(),
    )
    parsed_data: dict[str, Any] = {
        "ticker": ticker,
        "label": label,
        "version_marker": version_marker,
        "marker_present": marker_present,
        "company": {"ticker": ticker},
        "raw_markdown_extras": [],
    }
    last_known_anchor: str | None = None

    for section in sections:
        anchor = _HEADER_TO_ANCHOR.get(section.header)
        if anchor is None:
            parsed_data["raw_markdown_extras"].append(
                UnknownSection(anchor_after=last_known_anchor, content=section.raw_text)
            )
            continue
        try:
            payload = _parse_section(anchor, section.body_lines, state)
        except Exception as exc:  # pragma: no cover
            state.warnings.append(
                ParseWarning(
                    code="section_parse_error",
                    section=anchor,
                    message=f"failed to parse section {section.header}",
                    context={"error": str(exc)},
                )
            )
            payload = {}
        _merge_parsed_payload(parsed_data, payload)
        last_known_anchor = anchor

    source_ids = {record.id for record in parsed_data.get("sources") or [] if isinstance(record, SourceRecord)}
    for source_id in sorted(state.referenced_source_ids - source_ids):
        state.warnings.append(
            ParseWarning(
                code="unknown_source_token",
                section="sources",
                message=f"citation token references unknown source id {source_id}",
                context={"source_id": source_id},
            )
        )

    parsed_data["company"] = CompanyField.model_validate(parsed_data.get("company") or {"ticker": ticker})
    if "thesis" in parsed_data and not isinstance(parsed_data["thesis"], ThesisField):
        parsed_data["thesis"] = ThesisField.model_validate(parsed_data["thesis"])
    parsed_data["parse_warnings"] = state.warnings
    return ParsedThesis.model_validate(parsed_data)


def _parse_section(anchor: str, body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    if anchor == "thesis_statement":
        text = "\n".join(body_lines).strip()
        return {"thesis": {"statement": text or None}} if text or body_lines else {}
    if anchor == "thesis_metadata":
        return _parse_thesis_metadata(body_lines, state)
    if anchor == "from_idea":
        return _parse_from_idea(body_lines, state)
    if anchor == "consensus_view":
        return _parse_consensus_view(body_lines, state)
    if anchor == "materiality":
        return _parse_materiality(body_lines, state)
    if anchor == "differentiated_view":
        return _parse_differentiated_view(body_lines, state)
    if anchor == "invalidation_triggers":
        return _parse_invalidation_triggers(body_lines, state)
    if anchor == "business_overview":
        return _parse_business_overview(body_lines, state)
    if anchor == "catalysts":
        return _parse_catalysts(body_lines, state)
    if anchor == "risks":
        return _parse_risks(body_lines, state)
    if anchor == "valuation":
        return _parse_valuation(body_lines, state)
    if anchor == "peers":
        return _parse_peers(body_lines, state)
    if anchor == "assumptions":
        return _parse_assumptions(body_lines, state)
    if anchor == "historical_coincidences":
        return _parse_historical_coincidences(body_lines, state)
    if anchor == "data_gaps":
        return _parse_data_gaps(body_lines, state)
    if anchor == "qualitative_factors":
        return _parse_qualitative_factors(body_lines, state)
    if anchor == "ownership":
        return _parse_ownership(body_lines, state)
    if anchor == "monitoring":
        return _parse_monitoring(body_lines, state)
    if anchor == "industry_analysis":
        return _parse_industry_analysis(body_lines, state)
    if anchor == "quantitative_framing":
        return _parse_quantitative_framing(body_lines, state)
    if anchor == "position_metadata":
        return _parse_position_metadata(body_lines, state)
    if anchor == "sources":
        return _parse_sources(body_lines, state)
    if anchor == "model_links":
        return _parse_model_linkage(body_lines, state)
    if anchor == "decisions_log":
        return _parse_decisions_log(body_lines, state)
    return {}


def _parse_thesis_metadata(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    thesis_payload: dict[str, Any] = {}
    company_payload: dict[str, Any] = {}
    for line in body_lines:
        match = _LABELED_LINE_RE.match(line)
        if match is None:
            continue
        label = _normalize_label_key(match.group("label"))
        value = match.group("value").strip()
        if label == "sources":
            thesis_payload["source_refs"] = _parse_source_token_list(value, state)
        elif label in {"direction", "strategy", "timeframe"}:
            thesis_payload[label] = value
        elif label == "conviction":
            thesis_payload[label] = int(value) if value else None
        elif label == "company_name":
            company_payload["name"] = value
        elif label == "sector":
            company_payload["sector"] = value
        elif label == "industry":
            company_payload["industry"] = value
        elif label == "fiscal_year_end":
            company_payload["fiscal_year_end"] = value
        elif label == "most_recent_fy":
            company_payload["most_recent_fy"] = int(value) if value else None
        elif label == "exchange":
            company_payload["exchange"] = value
    payload: dict[str, Any] = {}
    if thesis_payload:
        payload["thesis"] = thesis_payload
    if company_payload:
        payload["company"] = company_payload
    return payload


def _parse_consensus_view(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    basis: str | None = None
    narrative_lines: list[str] = []
    for line in body_lines:
        match = _LABELED_LINE_RE.match(line)
        if match is not None and _normalize_label_key(match.group("label")) == "basis":
            basis = match.group("value").strip() or None
            continue
        narrative_lines.append(line)
    text = "\n".join(narrative_lines).strip()
    narrative, citations = _extract_source_tokens(text)
    for source_id in citations:
        state.referenced_source_ids.add(source_id)
    if not narrative and not citations and basis is None:
        return {}
    return {"consensus_view": ConsensusView.model_validate({"narrative": narrative, "basis": basis, "citations": citations})}


def _parse_materiality(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    labeled = _collect_labeled_lines(body_lines)
    payload = {
        "basis": labeled.get("basis"),
        "threshold_pct": labeled.get("threshold_pct"),
        "metric": labeled.get("metric"),
        "horizon": labeled.get("horizon"),
        "rationale": labeled.get("rationale"),
        "source_refs": _parse_source_token_list(labeled.get("sources", "") or labeled.get("source_refs", ""), state),
    }
    validated = _safe_validate(MaterialityThreshold, payload, state, "materiality")
    return {"materiality": validated} if validated is not None else {}


def _parse_differentiated_view(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    claims: list[DifferentiatedViewClaim] = []
    for title, subsection_lines in _split_subsections(body_lines).items():
        if not title:
            continue
        labeled = _collect_labeled_lines(subsection_lines)
        payload = {
            "claim": labeled.get("claim"),
            "rationale": labeled.get("rationale"),
            "evidence": _parse_source_token_list(labeled.get("evidence", ""), state),
            "upside_if_right": labeled.get("upside_if_right"),
            "downside_if_wrong": labeled.get("downside_if_wrong"),
        }
        payload["claim_id"] = _resolve_item_id("claims", title, payload, state)
        validated = _safe_validate(DifferentiatedViewClaim, payload, state, "differentiated_view")
        if validated is not None:
            claims.append(validated)
    return {"differentiated_view": claims}


def _parse_invalidation_triggers(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    triggers: list[InvalidationTrigger] = []
    for line in body_lines:
        if not line.strip():
            continue
        if not line.startswith("- "):
            state.warnings.append(ParseWarning(code="section_parse_error", section="invalidation_triggers", message="unexpected invalidation trigger line", context={"line": line}))
            continue
        if "{" in line and "}" not in line:
            state.warnings.append(ParseWarning(code="section_parse_error", section="invalidation_triggers", message="unclosed trigger id brace", context={"line": line}))
            continue
        match = _TRIGGER_BULLET_RE.match(line)
        if match is None:
            state.warnings.append(ParseWarning(code="section_parse_error", section="invalidation_triggers", message="failed to parse invalidation trigger bullet", context={"line": line}))
            continue
        metadata = _parse_key_value_segments(match.group("meta") or "")
        threshold = metadata.get("threshold")
        if threshold is not None:
            threshold = _parse_jsonish_value(threshold)
        payload = {
            "description": match.group("description").strip(),
            "metric": metadata.get("metric"),
            "threshold": threshold,
            "threshold_direction": metadata.get("threshold_direction"),
            "direction": metadata.get("direction"),
            "source_refs": _parse_source_token_list(
                metadata.get("sources", "") or metadata.get("source_refs", ""),
                state,
            ),
        }
        payload["trigger_id"] = (match.group("identifier") or "").strip() or _resolve_item_id("triggers", None, payload, state)
        validated = _safe_validate(InvalidationTrigger, payload, state, "invalidation_triggers")
        if validated is not None:
            triggers.append(validated)
    return {"invalidation_triggers": triggers}


def _parse_business_overview(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    description_lines = []
    source_refs: list[str] = []
    for line in sections.pop("", []):
        match = _LABELED_LINE_RE.match(line)
        if match and _normalize_label_key(match.group("label")) == "sources":
            source_refs = _parse_source_token_list(match.group("value"), state)
            continue
        description_lines.append(line)
    payload: dict[str, Any] = {"description": "\n".join(_trim_blank_edges(description_lines)).strip() or None, "source_refs": source_refs}
    if "Segments" in sections:
        segment_lines = sections["Segments"]
        payload["segments"] = [
            {"name": row.get("Name"), "rev_pct": row.get("Rev %") or None}
            for row in _parse_table(segment_lines)
        ]
        for line in segment_lines:
            match = _LABELED_LINE_RE.match(line)
            if match and _normalize_label_key(match.group("label")) == "sources":
                payload["source_refs"] = _parse_source_token_list(match.group("value"), state)
    validated = _safe_validate(BusinessOverview, payload, state, "business_overview")
    return {"business_overview": validated} if validated is not None else {}


def _parse_catalysts(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    catalysts: list[Catalyst] = []
    for title, subsection_lines in _split_subsections(body_lines).items():
        if not title:
            continue
        labeled = _collect_labeled_lines(subsection_lines)
        refs = _parse_source_token_list(labeled.get("source", ""), state)
        payload = {"description": labeled.get("description"), "expected_date": labeled.get("expected_date"), "severity": labeled.get("severity"), "source_ref": refs[0] if refs else None}
        payload["catalyst_id"] = _resolve_item_id("catalysts", title, payload, state)
        validated = _safe_validate(Catalyst, payload, state, "catalysts")
        if validated is not None:
            catalysts.append(validated)
    return {"catalysts": catalysts}


def _parse_risks(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    risks: list[Risk] = []
    for title, subsection_lines in _split_subsections(body_lines).items():
        if not title:
            continue
        labeled = _collect_labeled_lines(subsection_lines)
        refs = _parse_source_token_list(labeled.get("source", ""), state)
        payload = {"description": labeled.get("description"), "severity": labeled.get("severity"), "type": labeled.get("type"), "source_ref": refs[0] if refs else None}
        payload["risk_id"] = _resolve_item_id("risks", title, payload, state)
        validated = _safe_validate(Risk, payload, state, "risks")
        if validated is not None:
            risks.append(validated)
    return {"risks": risks}


def _parse_valuation(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    labeled = _collect_labeled_lines(sections.pop("", []))
    payload = {
        "method": labeled.get("method"),
        "low": labeled.get("low"),
        "mid": labeled.get("mid"),
        "high": labeled.get("high"),
        "current_multiple": labeled.get("current_multiple"),
        "wacc": labeled.get("wacc"),
        "risk_free_rate": labeled.get("risk_free_rate"),
        "equity_risk_premium": labeled.get("equity_risk_premium"),
        "cost_of_equity": labeled.get("cost_of_equity"),
        "raw_beta": labeled.get("raw_beta"),
        "adjusted_beta": labeled.get("adjusted_beta"),
        "beta_floor": labeled.get("beta_floor"),
        "terminal_growth_rate": labeled.get("terminal_growth_rate"),
        "terminal_multiple": labeled.get("terminal_multiple"),
        "source_refs": _parse_source_token_list(labeled.get("sources", ""), state),
        "rationale": "\n".join(_trim_blank_edges(sections.get("Rationale", []))).strip()
        or None,
    }
    validated = _safe_validate(Valuation, payload, state, "valuation")
    return {"valuation": validated} if validated is not None else {}


def _parse_peers(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    peers: list[Peer] = []
    for row in _parse_table(body_lines):
        validated = _safe_validate(Peer, {"ticker": row.get("Ticker"), "name": row.get("Name"), "source_refs": _parse_source_token_list(row.get("Sources", ""), state)}, state, "peers")
        if validated is not None:
            peers.append(validated)
    return {"peers": peers}


def _parse_assumptions(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    assumptions: list[Assumption] = []
    for title, subsection_lines in _split_subsections(body_lines).items():
        if not title:
            continue
        labeled = _collect_labeled_lines(subsection_lines)
        payload = {"driver": labeled.get("driver"), "value": _parse_jsonish_value(labeled.get("value", "")), "unit": labeled.get("unit"), "rationale": labeled.get("rationale"), "driver_category": labeled.get("driver_category"), "confidence": labeled.get("confidence"), "held_at_base": _parse_boolish(labeled.get("held_at_base")) if "held_at_base" in labeled else False, "source_refs": _parse_source_token_list(labeled.get("sources", ""), state)}
        payload["assumption_id"] = _resolve_item_id("assumptions", title, payload, state)
        validated = _safe_validate(Assumption, payload, state, "assumptions")
        if validated is not None:
            assumptions.append(validated)
    return {"assumptions": assumptions}


def _parse_historical_coincidences(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    coincidences: list[HistoricalCoincidence] = []
    for row in _parse_table(body_lines):
        payload = {
            "coincidence_id": _nullable_cell(row.get("coincidence_id")),
            "period": _nullable_cell(row.get("period")),
            "factor": _nullable_cell(row.get("factor")),
            "assumption_id": _nullable_cell(row.get("assumption_id")),
            "market_reaction": _nullable_cell(row.get("market_reaction")),
            "factor_direction": _nullable_cell(row.get("factor_direction")),
            "stock_outcome": _nullable_cell(row.get("stock_outcome")),
            "driver": _nullable_cell(row.get("driver")),
            "source_refs": _parse_source_refs_cell(row.get("source_refs"), state),
        }
        validated = _safe_validate(HistoricalCoincidence, payload, state, "historical_coincidences")
        if validated is not None:
            coincidences.append(validated)
    return {"historical_coincidences": coincidences}


def _parse_data_gaps(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    data_gaps: list[DataGap] = []
    for row in _parse_table(body_lines):
        payload = {
            "gap_id": _nullable_cell(row.get("gap_id")),
            "target_handle": _nullable_cell(row.get("target_handle")),
            "description": _nullable_cell(row.get("description")),
            "workaround": _nullable_cell(row.get("workaround")),
            "severity": _nullable_cell(row.get("severity")),
            "status": _nullable_cell(row.get("status")) or "open",
            "resolution_note": _nullable_cell(row.get("resolution_note")),
            "resolution_source_refs": _parse_source_refs_cell(
                row.get("resolution_source_refs"),
                state,
            ),
            "resolved_at": _nullable_cell(row.get("resolved_at")),
            "superseded_by_gap_id": _nullable_cell(row.get("superseded_by_gap_id")),
        }
        validated = _safe_validate(DataGap, payload, state, "data_gaps")
        if validated is not None:
            data_gaps.append(validated)
    return {"data_gaps": data_gaps}


def _parse_qualitative_factors(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    factors: list[QualitativeFactor] = []
    for title, subsection_lines in _split_subsections(body_lines).items():
        if not title:
            continue
        labeled = _collect_labeled_lines(subsection_lines)
        payload = {"category": labeled.get("category"), "label": labeled.get("label"), "assessment": labeled.get("assessment"), "rating": labeled.get("rating"), "data": _parse_jsonish_value(labeled.get("data_json", "")), "source_refs": _parse_source_token_list(labeled.get("sources", ""), state)}
        payload["id"] = _resolve_item_id("factors", title, payload, state)
        validated = _safe_validate(QualitativeFactor, payload, state, "qualitative_factors")
        if validated is not None:
            factors.append(validated)
    return {"qualitative_factors": factors}


def _parse_ownership(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    labeled = _collect_labeled_lines(sections.pop("", []))
    payload = {"institutional_pct": labeled.get("institutional_pct"), "insider_pct": labeled.get("insider_pct"), "source_refs": _parse_source_token_list(labeled.get("sources", ""), state), "recent_activity": "\n".join(_trim_blank_edges(sections.get("Recent Activity", []))).strip() or None}
    validated = _safe_validate(Ownership, payload, state, "ownership")
    return {"ownership": validated} if validated is not None else {}


def _parse_monitoring(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    payload: dict[str, Any] = {"watch_list": [], "source_refs": []}
    for line in body_lines:
        labeled = _LABELED_LINE_RE.match(line)
        if labeled and _normalize_label_key(labeled.group("label")) == "sources":
            payload["source_refs"] = _parse_source_token_list(labeled.group("value"), state)
            continue
        if not line.startswith("- "):
            continue
        content = line[2:].strip()
        segments = _parse_key_value_segments(content)
        if not segments:
            segments = {"description": content}
        if "sources" in segments and "source_refs" not in segments:
            segments["source_refs"] = _parse_source_token_list(segments["sources"], state)
            segments.pop("sources", None)
        if "source_refs" in segments and isinstance(segments["source_refs"], str):
            segments["source_refs"] = _parse_source_refs_cell(segments["source_refs"], state)
        if "threshold" in segments:
            segments["threshold"] = _parse_jsonish_value(segments["threshold"])
        watch_item = _safe_validate(WatchItem, segments, state, "monitoring")
        if watch_item is not None:
            payload["watch_list"].append(watch_item)
    validated = _safe_validate(Monitoring, payload, state, "monitoring")
    return {"monitoring": validated} if validated is not None else {}


def _parse_industry_analysis(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    payload: dict[str, Any] = {}
    if "Landscape" in sections:
        text = "\n".join(_trim_blank_edges(sections["Landscape"])).strip()
        narrative, citations = _extract_source_tokens(text)
        for source_id in citations:
            state.referenced_source_ids.add(source_id)
        payload["landscape"] = {"narrative": narrative, "citations": citations}
    if "Comps Narrative" in sections:
        text = "\n".join(_trim_blank_edges(sections["Comps Narrative"])).strip()
        narrative, citations = _extract_source_tokens(text)
        for source_id in citations:
            state.referenced_source_ids.add(source_id)
        comps_narrative = _safe_validate(
            CompsNarrative,
            {"narrative": narrative, "citations": citations},
            state,
            "industry_analysis.comps_narrative",
        )
        if comps_narrative is not None:
            payload["comps_narrative"] = comps_narrative
    if "Peer Comparison" in sections:
        payload["peer_comparison"] = {
            "peers": [
                {"ticker": row.get("Ticker"), "name": row.get("Name"), "relative_position": row.get("Relative Position") or None, "key_metrics": _parse_jsonish_value(row.get("Key Metrics JSON", "")), "source_refs": _parse_source_token_list(row.get("Sources", ""), state)}
                for row in _parse_table(sections["Peer Comparison"])
            ]
        }
    if "Macro Overlay" in sections:
        drivers = []
        for line in sections["Macro Overlay"]:
            if not line.startswith("- "):
                continue
            description, meta = _split_bullet_with_metadata(line[2:])
            metadata = _parse_key_value_segments(meta)
            drivers.append({"description": description, "sensitivity": metadata.get("sensitivity"), "source_refs": _parse_source_token_list(metadata.get("sources", ""), state)})
        payload["macro_overlay"] = {"drivers": drivers}
    if "Structural Trends" in sections:
        payload["structural_trends"] = []
        for line in sections["Structural Trends"]:
            if not line.startswith("- "):
                continue
            description, meta = _split_bullet_with_metadata(line[2:])
            metadata = _parse_key_value_segments(meta)
            payload["structural_trends"].append({"description": description, "time_horizon": metadata.get("time_horizon"), "source_refs": _parse_source_token_list(metadata.get("sources", ""), state)})
    validated = _safe_validate(IndustryAnalysis, payload, state, "industry_analysis")
    return {"industry_analysis": validated} if validated is not None else {}


def _parse_quantitative_framing(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    payload: dict[str, Any] = {}
    if "Revenue" in sections:
        labeled = _collect_labeled_lines(sections["Revenue"])
        payload["revenue"] = {"base": _parse_jsonish_value(labeled.get("base", "")), "bull": _parse_jsonish_value(labeled.get("bull", "")), "bear": _parse_jsonish_value(labeled.get("bear", "")), "rationale": labeled.get("rationale")}
    if "Margins" in sections:
        labeled = _collect_labeled_lines(sections["Margins"])
        payload["margins"] = {"trajectory": labeled.get("trajectory"), "key_drivers": [part.strip() for part in labeled.get("key_drivers", "").split(";") if part.strip()]}
    if "EPS / FCF" in sections:
        labeled = _collect_labeled_lines(sections["EPS / FCF"])
        payload["eps_fcf"] = {
            "projection": _parse_jsonish_value(labeled.get("projection", "")),
            "delta_vs_consensus": _parse_jsonish_value(labeled.get("delta_vs_consensus", "")),
            "terminal_year": _parse_jsonish_value(labeled.get("terminal_year", "")),
            "basis": _nullable_cell(labeled.get("basis")),
        }
        for title, bridge_lines in _split_nested_subsections(sections["EPS / FCF"]).items():
            if _normalize_label_key(title) != "gaap_non_gaap_bridge":
                continue
            bridge = _parse_gaap_non_gaap_bridge(bridge_lines, state)
            if bridge is not None:
                payload["eps_fcf"]["bridge"] = bridge
            break
    if "Scenarios" in sections:
        payload["scenarios"] = {}
        for title, scenario_lines in _split_nested_subsections(sections["Scenarios"]).items():
            labeled = _collect_labeled_lines(scenario_lines)
            payload["scenarios"][_normalize_label_key(title)] = {
                "target_price": labeled.get("target_price"),
                "return_pct": labeled.get("return_pct"),
                "revenue_m": _parse_jsonish_value(labeled.get("revenue_m", "")),
                "probability": _parse_jsonish_value(labeled.get("probability", "")),
                "op_margin_pct": _parse_jsonish_value(labeled.get("op_margin_pct", "")),
                "ebitda_margin_pct": _parse_jsonish_value(labeled.get("ebitda_margin_pct", "")),
                "eps": _parse_jsonish_value(labeled.get("eps", "")),
                "eps_gaap": _parse_jsonish_value(labeled.get("eps_gaap", "")),
                "eps_non_gaap": _parse_jsonish_value(labeled.get("eps_non_gaap", "")),
                "adj_eps": _parse_jsonish_value(labeled.get("adj_eps", "")),
                "fcf_per_share": _parse_jsonish_value(labeled.get("fcf_per_share", "")),
                "what_has_to_happen": labeled.get("what_has_to_happen"),
                "methodology": labeled.get("methodology"),
                "what_goes_wrong": labeled.get("what_goes_wrong"),
            }
    validated = _safe_validate(QuantitativeFraming, payload, state, "quantitative_framing")
    return {"quantitative_framing": validated} if validated is not None else {}


def _parse_gaap_non_gaap_bridge(body_lines: list[str], state: _ParseState) -> GaapNonGaapBridge | None:
    labeled = _collect_labeled_lines(body_lines)
    components_value = labeled.get("bridge_components_json", "") or labeled.get("bridge_components", "")
    components = _parse_jsonish_value(components_value)
    if components is None:
        bridge_components: list[str] = []
    elif isinstance(components, list):
        bridge_components = [str(item) for item in components]
    else:
        bridge_components = [part.strip() for part in str(components).split(";") if part.strip()]
    return _safe_validate(
        GaapNonGaapBridge,
        {
            "metric": labeled.get("metric"),
            "period": labeled.get("period"),
            "gaap_value": _parse_jsonish_value(labeled.get("gaap_value", "")),
            "non_gaap_value": _parse_jsonish_value(labeled.get("non_gaap_value", "")),
            "bridge_value": _parse_jsonish_value(labeled.get("bridge_value", "")),
            "bridge_components": bridge_components,
            "rationale": labeled.get("rationale"),
            "approximation_quality": labeled.get("approximation_quality"),
            "source_refs": _parse_source_token_list(labeled.get("sources", "") or labeled.get("source_refs", ""), state),
        },
        state,
        "quantitative_framing",
    )


def _parse_position_metadata(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    sections = _split_subsections(body_lines)
    labeled = _collect_labeled_lines(sections.pop("", []))
    payload: dict[str, Any] = {"date_initiated": labeled.get("date_initiated")}
    if "Position Size" in sections:
        size = _collect_labeled_lines(sections["Position Size"])
        payload["position_size"] = {"target_pct": size.get("target_pct"), "current_pct": size.get("current_pct")}
    if "Portfolio Fit" in sections:
        fit = _collect_labeled_lines(sections["Portfolio Fit"])
        payload["portfolio_fit"] = {
            "sector_exposure": fit.get("sector_exposure"),
            "factor_exposure": fit.get("factor_exposure"),
            "correlation_cluster": fit.get("correlation_cluster"),
            "dominant_factor_stability": _parse_jsonish_value(fit.get("dominant_factor_stability", "")),
            "idiosyncratic_volatility_annualized_pct": _parse_jsonish_value(fit.get("idiosyncratic_volatility_annualized_pct", "")),
            "market_only_decomposition": _parse_jsonish_value(fit.get("market_only_decomposition_json", "")),
        }
    validated = _safe_validate(PositionMetadata, payload, state, "position_metadata")
    return {"position_metadata": validated} if validated is not None else {}


def _parse_sources(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    records: list[SourceRecord] = []
    index = 0
    while index < len(body_lines):
        line = body_lines[index]
        match = _NUMBERED_ITEM_RE.match(line.strip())
        if match is None:
            state.warnings.append(ParseWarning(code="section_parse_error", section="sources", message="failed to parse source line", context={"line": line}))
            index += 1
            continue
        index += 1
        excerpts: list[Excerpt] = []
        while index < len(body_lines):
            excerpt_match = _SOURCE_EXCERPT_RE.match(body_lines[index])
            if excerpt_match is None:
                break
            excerpt = _parse_source_excerpt(excerpt_match.group("payload"), state)
            if excerpt is not None:
                excerpts.append(excerpt)
            index += 1
        metadata = _parse_key_value_segments(match.group("meta") or "")
        validated = _safe_validate(
            SourceRecord,
            {
                "id": match.group("source_id"),
                "type": metadata.get("type"),
                "source_id": metadata.get("source_id"),
                "identity_hash": metadata.get("identity_hash"),
                "section_header": metadata.get("section_header"),
                "char_start": metadata.get("char_start"),
                "char_end": metadata.get("char_end"),
                "text": metadata.get("text"),
                "annotation_id": metadata.get("annotation_id"),
                "provider": metadata.get("provider"),
                "endpoint_or_filing_id": metadata.get("endpoint_or_filing_id"),
                "retrieved_at": metadata.get("retrieved_at"),
                "skill_name": metadata.get("skill_name"),
                "artifact_path": metadata.get("artifact_path"),
                "artifact_id": metadata.get("artifact_id"),
                "skill_run_id": metadata.get("skill_run_id"),
                "source_path": metadata.get("source_path"),
                "excerpts": excerpts,
            },
            state,
            "sources",
        )
        if validated is not None:
            records.append(validated)
    return {"sources": records}


def _parse_source_excerpt(raw_payload: str, state: _ParseState) -> Excerpt | None:
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        state.warnings.append(
            ParseWarning(
                code="section_parse_error",
                section="sources",
                message="failed to parse source excerpt JSON",
                context={"error": str(exc), "payload": raw_payload},
            )
        )
        return None
    try:
        return Excerpt.model_validate(payload)
    except ValidationError as exc:
        state.warnings.append(
            ParseWarning(
                code="section_parse_error",
                section="sources",
                message="failed to validate source excerpt",
                context={"errors": exc.errors(include_url=False)},
            )
        )
        return None


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
        model_ref = _safe_validate(ThesisModelRef, {"model_id": labeled.get("model_id"), "version": labeled.get("version"), "file_path": labeled.get("file_path"), "last_updated": labeled.get("last_updated"), "drivers_locked": [part.strip() for part in labeled.get("drivers_locked", "").split(",") if part.strip()]}, state, "model_links")
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
            validated = _safe_validate(ThesisLink, {"thesis_link_id": row.get("Link ID") or None, "thesis_point_id": row.get("Point ID"), "category": row.get("Category"), "thesis_direction": row.get("Thesis Direction"), "driver_key": row.get("Driver Key") or None, "data_concept_id": row.get("Data Concept ID") or None, "model_item_id": row.get("Model Item ID") or None, "business_model_node_id": row.get("BM Node") or None, "template_version": row.get("Template Version") or None, "model_id": row.get("Model ID") or None, "periods": [int(part) for part in row.get("Periods", "").split(",") if part.strip()], "thesis_value": row.get("Thesis Value") or None, "consensus_value": row.get("Consensus Value") or None, "structural_fingerprint": _parse_jsonish_value(row.get("Structural Fingerprint JSON", "")), "thesis_text": row.get("Thesis Text")}, state, "model_links")
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
                "patch_ops_applied": _parse_jsonish_value(row.get("Patch Ops JSON", "")) or [],
                "run_id": row.get("Run ID") or None,
                "artifact_refs": _parse_jsonish_value(row.get("Artifact Refs JSON", "")) or [],
            },
            state,
            "decisions_log",
        )
        if validated is not None:
            entries.append(validated)
    return {"decisions_log": entries}


__all__ = [
    "ParseWarning",
    "ParsedThesis",
    "PriorNormalizedEntry",
    "PriorThesisIndex",
    "parse_thesis_markdown",
]
