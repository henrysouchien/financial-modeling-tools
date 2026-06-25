from __future__ import annotations

from collections import defaultdict
from typing import Any
import uuid

from .thesis import (  # noqa: F401
    BusinessModelRef,
    DecisionsLogEntry,
    GaapNonGaapBridge,
    PositionMetadata,
    QuantitativeFraming,
    Thesis,
    ThesisFromIdea,
    ThesisLink,
    ThesisModelRef,
    UnknownSection,
)
from .research_labels import canonicalize_research_label
from .thesis_shared_slice import (  # noqa: F401
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
    _ContractModel,
    _normalize_ticker,
)
from .thesis_markdown_utils import (  # noqa: F401
    _SOURCE_TOKEN_RE,  # noqa: F401 - compatibility alias for schema.thesis_markdown imports
    _append_block,
    _escape_table_cell,  # noqa: F401 - compatibility alias for schema.thesis_markdown imports
    _extract_source_tokens,
    _format_source_refs_cell,
    _format_source_tokens,
    _format_table,
    _has_meaningful_value,
    _normalize_label_key,
    _nullable_cell,
    _parse_boolish,
    _parse_jsonish_value,
    _parse_key_value_segments,
    _parse_source_refs_cell,
    _parse_source_token_list,
    _parse_table,
    _render_bullet,
    _render_labeled_line,
    _serialize_json_value,
    _slugify_label,
    _split_lines,
    _split_table_row,  # noqa: F401 - compatibility alias for schema.thesis_markdown imports
    _unescape_table_cell,  # noqa: F401 - compatibility alias for schema.thesis_markdown imports
)

from .thesis_markdown_parse import (  # noqa: F401
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
    _parse_assumptions,
    _parse_business_overview,
    _parse_catalysts,
    _parse_consensus_view,
    _parse_data_gaps,
    _parse_decisions_log,
    _parse_differentiated_view,
    _parse_from_idea,
    _parse_gaap_non_gaap_bridge,
    _parse_historical_coincidences,
    _parse_industry_analysis,
    _parse_invalidation_triggers,
    _parse_materiality,
    _parse_model_linkage,
    _parse_monitoring,
    _parse_ownership,
    _parse_peers,
    _parse_position_metadata,
    _parse_quantitative_framing,
    _parse_qualitative_factors,
    _parse_risks,
    _parse_section,
    _parse_source_excerpt,
    _parse_sources,
    _parse_thesis_metadata,
    _parse_valuation,
    _resolve_item_id,
    _safe_validate,
    _split_bullet_with_metadata,
    _split_nested_subsections,
    _split_sections,
    _split_subsections,
    _trim_blank_edges,
    parse_thesis_markdown,
)
from .thesis_markdown_serialize import (  # noqa: F401
    _serialize_decisions_log as _serialize_decisions_log,
    _serialize_from_idea as _serialize_from_idea,
    _serialize_model_linkage as _serialize_model_linkage,
    _serialize_source_excerpt as _serialize_source_excerpt,
    _serialize_sources as _serialize_sources,
)




def thesis_markdown_path(ticker: str, label: str | None = None) -> str:
    normalized_ticker = _normalize_ticker(ticker)
    normalized_label = canonicalize_research_label(label)
    if not normalized_label:
        return f"theses/{normalized_ticker}.md"
    return f"theses/{normalized_ticker}__{_slugify_label(normalized_label)}.md"


def serialize_thesis(thesis: Thesis) -> str:
    thesis = Thesis.model_validate(thesis)
    lines: list[str] = [
        f"{_SYNC_HEADER_PREFIX} {_sync_key(thesis.ticker, thesis.label)} version={thesis.version} -->",
        f"{_TITLE_PREFIX}{thesis.ticker}{f': {thesis.label}' if thesis.label else ''}",
    ]
    extras_by_anchor: dict[str | None, list[str]] = defaultdict(list)
    for extra in thesis.raw_markdown_extras:
        extras_by_anchor[extra.anchor_after].append(extra.content.rstrip("\n"))

    for content in extras_by_anchor.get(None, []):
        _append_block(lines, [content])

    section_blocks = {
        "thesis_statement": _serialize_thesis_statement(thesis),
        "thesis_metadata": _serialize_thesis_metadata(thesis),
        "from_idea": _serialize_from_idea(thesis.from_idea),
        "consensus_view": _serialize_consensus_view(thesis.consensus_view),
        "materiality": _serialize_materiality(thesis.materiality),
        "differentiated_view": _serialize_differentiated_view(thesis.differentiated_view),
        "invalidation_triggers": _serialize_invalidation_triggers(thesis.invalidation_triggers),
        "business_overview": _serialize_business_overview(thesis.business_overview),
        "catalysts": _serialize_catalysts(thesis.catalysts),
        "risks": _serialize_risks(thesis.risks),
        "valuation": _serialize_valuation(thesis.valuation),
        "peers": _serialize_peers(thesis.peers),
        "assumptions": _serialize_assumptions(thesis.assumptions),
        "historical_coincidences": _serialize_historical_coincidences(thesis.historical_coincidences),
        "data_gaps": _serialize_data_gaps(thesis.data_gaps),
        "qualitative_factors": _serialize_qualitative_factors(thesis.qualitative_factors),
        "ownership": _serialize_ownership(thesis.ownership),
        "monitoring": _serialize_monitoring(thesis.monitoring),
        "industry_analysis": _serialize_industry_analysis(thesis.industry_analysis),
        "quantitative_framing": _serialize_quantitative_framing(thesis.quantitative_framing),
        "position_metadata": _serialize_position_metadata(thesis.position_metadata),
        "sources": _serialize_sources(thesis.sources),
        "model_links": _serialize_model_linkage(
            thesis.model_ref,
            thesis.business_model_ref,
            thesis.model_links,
        ),
        "decisions_log": _serialize_decisions_log(thesis.decisions_log),
    }

    for anchor in _CANONICAL_ANCHORS:
        block = section_blocks[anchor]
        if block:
            _append_block(lines, [f"## {_ANCHOR_TO_HEADER[anchor]}", *block])
        for content in extras_by_anchor.get(anchor, []):
            _append_block(lines, [content])

    return "\n".join(lines).rstrip() + "\n"




def _sync_key(ticker: str, label: str | None) -> str:
    normalized_ticker = _normalize_ticker(ticker)
    normalized_label = canonicalize_research_label(label)
    if not normalized_label:
        return f"thesis:{normalized_ticker}"
    return f"thesis:{normalized_ticker}:{_slugify_label(normalized_label)}"




def _serialize_thesis_statement(thesis: Thesis) -> list[str]:
    return _split_lines(thesis.thesis.statement)


def _serialize_thesis_metadata(thesis: Thesis) -> list[str]:
    lines: list[str] = []
    for label, value in (
        ("Direction", thesis.thesis.direction),
        ("Strategy", thesis.thesis.strategy),
        ("Conviction", thesis.thesis.conviction),
        ("Timeframe", thesis.thesis.timeframe),
        ("Sources", _format_source_tokens(thesis.thesis.source_refs) if thesis.thesis.source_refs else None),
        ("Company Name", thesis.company.name),
        ("Sector", thesis.company.sector),
        ("Industry", thesis.company.industry),
        ("Fiscal Year End", thesis.company.fiscal_year_end),
        ("Most Recent FY", thesis.company.most_recent_fy),
        ("Exchange", thesis.company.exchange),
    ):
        line = _render_bullet(label, value)
        if line:
            lines.append(line)
    return lines


def _serialize_consensus_view(view: ConsensusView | None) -> list[str]:
    if view is None:
        return []
    lines: list[str] = []
    basis_line = _render_labeled_line("Basis", view.basis)
    if basis_line:
        lines.append(basis_line)
    text = view.narrative.strip()
    if view.citations:
        text = f"{text} {_format_source_tokens(view.citations)}".strip()
    lines.extend(_split_lines(text))
    return lines


def _serialize_materiality(materiality: MaterialityThreshold | None) -> list[str]:
    if materiality is None:
        return []
    return [
        line
        for line in (
            _render_bullet("Basis", materiality.basis),
            _render_bullet("Threshold Pct", materiality.threshold_pct),
            _render_bullet("Metric", materiality.metric),
            _render_bullet("Horizon", materiality.horizon),
            _render_bullet("Rationale", materiality.rationale),
            _render_bullet("Sources", _format_source_tokens(materiality.source_refs) if materiality.source_refs else None),
        )
        if line
    ]


def _serialize_differentiated_view(claims: list[DifferentiatedViewClaim]) -> list[str]:
    lines: list[str] = []
    for claim in claims:
        lines.append(f"### claim_id:{claim.claim_id or str(uuid.uuid4())}")
        lines.append(_render_labeled_line("Claim", claim.claim) or "")
        lines.append(_render_labeled_line("Rationale", claim.rationale) or "")
        evidence_line = _render_labeled_line("Evidence", _format_source_tokens(claim.evidence) if claim.evidence else None)
        if evidence_line:
            lines.append(evidence_line)
        for label, value in (("Upside If Right", claim.upside_if_right), ("Downside If Wrong", claim.downside_if_wrong)):
            line = _render_labeled_line(label, value)
            if line:
                lines.append(line)
        lines.append("")
    return _trim_blank_edges(lines)


def _serialize_invalidation_triggers(triggers: list[InvalidationTrigger]) -> list[str]:
    lines: list[str] = []
    for trigger in triggers:
        prefix = f"{{{trigger.trigger_id}}} " if trigger.trigger_id else ""
        metadata = []
        for key, value in (
            ("metric", trigger.metric),
            ("threshold", trigger.threshold),
            ("threshold_direction", trigger.threshold_direction),
            ("direction", trigger.direction),
            (
                "sources",
                _format_source_tokens(trigger.source_refs)
                if trigger.source_refs
                else None,
            ),
        ):
            if _has_meaningful_value(value):
                metadata.append(f"{key}={value}")
        suffix = f" | {' | '.join(metadata)}" if metadata else ""
        lines.append(f"- {prefix}{trigger.description}{suffix}")
    return lines


def _serialize_business_overview(overview: BusinessOverview | None) -> list[str]:
    if overview is None:
        return []
    lines = _split_lines(overview.description)
    if overview.segments:
        if lines:
            lines.append("")
        lines.append("### Segments")
        lines.extend(_format_table(["Name", "Rev %"], [[segment.name, segment.rev_pct or ""] for segment in overview.segments]))
    source_line = _render_labeled_line("Sources", _format_source_tokens(overview.source_refs) if overview.source_refs else None)
    if source_line:
        if lines:
            lines.append("")
        lines.append(source_line)
    return lines


def _serialize_catalysts(catalysts: list[Catalyst]) -> list[str]:
    return _serialize_id_subsections(
        catalysts,
        "catalyst_id",
        lambda item: [
            _render_labeled_line("Description", item.description),
            _render_labeled_line("Expected Date", item.expected_date),
            _render_labeled_line("Severity", item.severity),
            _render_labeled_line("Source", _format_source_tokens([item.source_ref]) if item.source_ref else None),
        ],
    )


def _serialize_risks(risks: list[Risk]) -> list[str]:
    return _serialize_id_subsections(
        risks,
        "risk_id",
        lambda item: [
            _render_labeled_line("Description", item.description),
            _render_labeled_line("Severity", item.severity),
            _render_labeled_line("Type", item.type),
            _render_labeled_line("Source", _format_source_tokens([item.source_ref]) if item.source_ref else None),
        ],
    )


def _serialize_valuation(valuation: Valuation | None) -> list[str]:
    if valuation is None:
        return []
    lines = [
        line
        for line in (
            _render_bullet("Method", valuation.method),
            _render_bullet("Low", valuation.low),
            _render_bullet("Mid", valuation.mid),
            _render_bullet("High", valuation.high),
            _render_bullet("Current Multiple", valuation.current_multiple),
            _render_bullet("WACC", valuation.wacc),
            _render_bullet("Risk Free Rate", valuation.risk_free_rate),
            _render_bullet("Equity Risk Premium", valuation.equity_risk_premium),
            _render_bullet("Cost of Equity", valuation.cost_of_equity),
            _render_bullet("Raw Beta", valuation.raw_beta),
            _render_bullet("Adjusted Beta", valuation.adjusted_beta),
            _render_bullet("Beta Floor", valuation.beta_floor),
            _render_bullet("Terminal Growth Rate", valuation.terminal_growth_rate),
            _render_bullet("Terminal Multiple", valuation.terminal_multiple),
            _render_bullet("Sources", _format_source_tokens(valuation.source_refs) if valuation.source_refs else None),
        )
        if line
    ]
    if valuation.rationale:
        if lines:
            lines.append("")
        lines.append("### Rationale")
        lines.extend(_split_lines(valuation.rationale))
    return lines


def _serialize_peers(peers: list[Peer]) -> list[str]:
    if not peers:
        return []
    return _format_table(["Ticker", "Name", "Sources"], [[peer.ticker, peer.name, _format_source_tokens(peer.source_refs)] for peer in peers])


def _serialize_assumptions(assumptions: list[Assumption]) -> list[str]:
    return _serialize_id_subsections(
        assumptions,
        "assumption_id",
        lambda item: [
            _render_labeled_line("Driver", item.driver),
            _render_labeled_line("Value", _serialize_json_value(item.value)),
            _render_labeled_line("Unit", item.unit),
            _render_labeled_line("Rationale", item.rationale),
            _render_labeled_line("Driver Category", item.driver_category.value if item.driver_category is not None else None),
            _render_labeled_line("Confidence", item.confidence),
            _render_labeled_line("Held At Base", "true" if item.held_at_base else None),
            _render_labeled_line("Sources", _format_source_tokens(item.source_refs) if item.source_refs else None),
        ],
    )


def _serialize_historical_coincidences(coincidences: list[HistoricalCoincidence]) -> list[str]:
    if not coincidences:
        return []
    return _format_table(
        [
            "coincidence_id",
            "period",
            "factor",
            "assumption_id",
            "market_reaction",
            "factor_direction",
            "stock_outcome",
            "driver",
            "source_refs",
        ],
        [
            [
                item.coincidence_id,
                item.period,
                item.factor,
                item.assumption_id or "",
                item.market_reaction,
                item.factor_direction or "",
                item.stock_outcome or "",
                item.driver,
                _format_source_refs_cell(item.source_refs),
            ]
            for item in coincidences
        ],
    )


def _serialize_data_gaps(data_gaps: list[DataGap]) -> list[str]:
    if not data_gaps:
        return []
    return _format_table(
        [
            "gap_id",
            "target_handle",
            "description",
            "workaround",
            "severity",
            "status",
            "resolution_note",
            "resolution_source_refs",
            "resolved_at",
            "superseded_by_gap_id",
        ],
        [
            [
                item.gap_id or "",
                item.target_handle or "",
                item.description,
                item.workaround or "",
                item.severity or "",
                item.status,
                item.resolution_note or "",
                _format_source_tokens(item.resolution_source_refs)
                if item.resolution_source_refs
                else "",
                item.resolved_at or "",
                item.superseded_by_gap_id or "",
            ]
            for item in data_gaps
        ],
    )


def _serialize_qualitative_factors(factors: list[QualitativeFactor]) -> list[str]:
    return _serialize_id_subsections(
        factors,
        "factor_id",
        lambda item: [
            _render_labeled_line("Category", item.category),
            _render_labeled_line("Label", item.label),
            _render_labeled_line("Assessment", item.assessment),
            _render_labeled_line("Rating", item.rating),
            _render_labeled_line("Data JSON", _serialize_json_value(item.data) if item.data is not None else None),
            _render_labeled_line("Sources", _format_source_tokens(item.source_refs) if item.source_refs else None),
        ],
    )


def _serialize_ownership(ownership: Ownership | None) -> list[str]:
    if ownership is None:
        return []
    lines = [
        line
        for line in (
            _render_bullet("Institutional Pct", ownership.institutional_pct),
            _render_bullet("Insider Pct", ownership.insider_pct),
            _render_bullet("Sources", _format_source_tokens(ownership.source_refs) if ownership.source_refs else None),
        )
        if line
    ]
    if ownership.recent_activity:
        if lines:
            lines.append("")
        lines.append("### Recent Activity")
        lines.extend(_split_lines(ownership.recent_activity))
    return lines


def _serialize_monitoring(monitoring: Monitoring | None) -> list[str]:
    if monitoring is None:
        return []
    lines: list[str] = []
    sources_line = _render_bullet("Sources", _format_source_tokens(monitoring.source_refs) if monitoring.source_refs else None)
    if sources_line:
        lines.append(sources_line)
    for item in monitoring.watch_list:
        metadata = []
        for key, value in (
            ("watch_item_id", item.watch_item_id),
            ("description", item.description),
            ("metric", item.metric),
            ("threshold", item.threshold),
            ("threshold_direction", item.threshold_direction),
            ("last_checked", item.last_checked),
            ("derived_from_handle", item.derived_from_handle),
            ("derived_from_kind", item.derived_from_kind),
        ):
            if _has_meaningful_value(value):
                metadata.append(f"{key}={value}")
        metadata.append(f"sources={_format_source_tokens(item.source_refs)}")
        lines.append(f"- {' | '.join(metadata)}")
    return lines


def _serialize_industry_analysis(analysis: IndustryAnalysis | None) -> list[str]:
    if analysis is None:
        return []
    lines: list[str] = []
    if analysis.landscape:
        lines.append("### Landscape")
        narrative = analysis.landscape.narrative
        if analysis.landscape.citations:
            narrative = f"{narrative} {_format_source_tokens(analysis.landscape.citations)}".strip()
        lines.extend(_split_lines(narrative))
        lines.append("")
    if analysis.comps_narrative:
        lines.append("### Comps Narrative")
        narrative = analysis.comps_narrative.narrative
        if analysis.comps_narrative.citations:
            narrative = f"{narrative} {_format_source_tokens(analysis.comps_narrative.citations)}".strip()
        lines.extend(_split_lines(narrative))
        lines.append("")
    if analysis.peer_comparison and analysis.peer_comparison.peers:
        lines.append("### Peer Comparison")
        lines.extend(
            _format_table(
                ["Ticker", "Name", "Relative Position", "Key Metrics JSON", "Sources"],
                [
                    [peer.ticker, peer.name, peer.relative_position or "", _serialize_json_value(peer.key_metrics) if peer.key_metrics else "", _format_source_tokens(peer.source_refs)]
                    for peer in analysis.peer_comparison.peers
                ],
            )
        )
        lines.append("")
    if analysis.macro_overlay and analysis.macro_overlay.drivers:
        lines.append("### Macro Overlay")
        for driver in analysis.macro_overlay.drivers:
            metadata = []
            if driver.sensitivity:
                metadata.append(f"sensitivity={driver.sensitivity}")
            if driver.source_refs:
                metadata.append(f"sources={_format_source_tokens(driver.source_refs)}")
            suffix = f" | {' | '.join(metadata)}" if metadata else ""
            lines.append(f"- {driver.description}{suffix}")
        lines.append("")
    if analysis.structural_trends:
        lines.append("### Structural Trends")
        for trend in analysis.structural_trends:
            metadata = []
            if trend.time_horizon:
                metadata.append(f"time_horizon={trend.time_horizon}")
            if trend.source_refs:
                metadata.append(f"sources={_format_source_tokens(trend.source_refs)}")
            suffix = f" | {' | '.join(metadata)}" if metadata else ""
            lines.append(f"- {trend.description}{suffix}")
    return _trim_blank_edges(lines)


def _serialize_quantitative_framing(framing: QuantitativeFraming | None) -> list[str]:
    if framing is None:
        return []
    lines: list[str] = []
    if framing.revenue:
        lines.append("### Revenue")
        for label, value in (("Base", framing.revenue.base), ("Bull", framing.revenue.bull), ("Bear", framing.revenue.bear), ("Rationale", framing.revenue.rationale)):
            line = _render_bullet(label, _serialize_json_value(value) if label != "Rationale" else value)
            if line:
                lines.append(line)
        lines.append("")
    if framing.margins:
        lines.append("### Margins")
        for label, value in (("Trajectory", framing.margins.trajectory), ("Key Drivers", "; ".join(framing.margins.key_drivers) if framing.margins.key_drivers else None)):
            line = _render_bullet(label, value)
            if line:
                lines.append(line)
        lines.append("")
    if framing.eps_fcf:
        lines.append("### EPS / FCF")
        for label, value in (
            ("Projection", _serialize_json_value(framing.eps_fcf.projection)),
            ("Delta Vs Consensus", _serialize_json_value(framing.eps_fcf.delta_vs_consensus)),
            ("Terminal Year", framing.eps_fcf.terminal_year),
            ("Basis", framing.eps_fcf.basis),
        ):
            line = _render_bullet(label, value)
            if line:
                lines.append(line)
        if framing.eps_fcf.bridge is not None:
            if len(lines) > 1 and lines[-1].strip():
                lines.append("")
            lines.append("#### GAAP / Non-GAAP Bridge")
            lines.extend(_serialize_gaap_non_gaap_bridge(framing.eps_fcf.bridge))
        lines.append("")
    if framing.scenarios:
        lines.append("### Scenarios")
        for name, scenario in (("Bull", framing.scenarios.bull), ("Base", framing.scenarios.base), ("Bear", framing.scenarios.bear)):
            if scenario is None:
                continue
            lines.append(f"#### {name}")
            for label, value in (
                ("Target Price", scenario.target_price),
                ("Return Pct", scenario.return_pct),
                ("Revenue M", _serialize_json_value(scenario.revenue_m)),
                (
                    "Probability",
                    _serialize_json_value(
                        scenario.probability.model_dump(mode="json")
                        if scenario.probability
                        else None
                    ),
                ),
                ("Op Margin Pct", _serialize_json_value(scenario.op_margin_pct)),
                ("EBITDA Margin Pct", _serialize_json_value(scenario.ebitda_margin_pct)),
                ("EPS", _serialize_json_value(scenario.eps)),
                ("EPS GAAP", _serialize_json_value(scenario.eps_gaap)),
                ("EPS Non-GAAP", _serialize_json_value(scenario.eps_non_gaap)),
                ("Adj EPS", _serialize_json_value(scenario.adj_eps)),
                ("FCF Per Share", _serialize_json_value(scenario.fcf_per_share)),
                ("What Has To Happen", scenario.what_has_to_happen),
                ("Methodology", scenario.methodology),
                ("What Goes Wrong", scenario.what_goes_wrong),
            ):
                line = _render_bullet(label, value)
                if line:
                    lines.append(line)
            lines.append("")
    return _trim_blank_edges(lines)


def _serialize_gaap_non_gaap_bridge(bridge: GaapNonGaapBridge) -> list[str]:
    return [
        line
        for line in (
            _render_bullet("Metric", bridge.metric),
            _render_bullet("Period", bridge.period),
            _render_bullet("GAAP Value", _serialize_json_value(bridge.gaap_value)),
            _render_bullet("Non-GAAP Value", _serialize_json_value(bridge.non_gaap_value)),
            _render_bullet("Bridge Value", _serialize_json_value(bridge.bridge_value)),
            _render_bullet("Bridge Components JSON", _serialize_json_value(bridge.bridge_components) if bridge.bridge_components else None),
            _render_bullet("Rationale", bridge.rationale),
            _render_bullet("Approximation Quality", bridge.approximation_quality),
            _render_bullet("Sources", _format_source_tokens(bridge.source_refs) if bridge.source_refs else None),
        )
        if line
    ]


def _serialize_position_metadata(metadata: PositionMetadata | None) -> list[str]:
    if metadata is None:
        return []
    lines: list[str] = []
    line = _render_bullet("Date Initiated", metadata.date_initiated)
    if line:
        lines.append(line)
    if metadata.position_size:
        if lines:
            lines.append("")
        lines.append("### Position Size")
        for label, value in (("Target Pct", metadata.position_size.target_pct), ("Current Pct", metadata.position_size.current_pct)):
            inner_line = _render_bullet(label, value)
            if inner_line:
                lines.append(inner_line)
    if metadata.portfolio_fit:
        if lines:
            lines.append("")
        lines.append("### Portfolio Fit")
        market_only_decomposition = None
        if metadata.portfolio_fit.market_only_decomposition is not None:
            market_only_decomposition = metadata.portfolio_fit.market_only_decomposition.model_dump(mode="json", exclude_none=True)
        for label, value in (
            ("Sector Exposure", metadata.portfolio_fit.sector_exposure),
            ("Factor Exposure", metadata.portfolio_fit.factor_exposure),
            ("Correlation Cluster", metadata.portfolio_fit.correlation_cluster),
            ("Dominant Factor Stability", metadata.portfolio_fit.dominant_factor_stability),
            ("Idiosyncratic Volatility Annualized Pct", metadata.portfolio_fit.idiosyncratic_volatility_annualized_pct),
            ("Market Only Decomposition JSON", _serialize_json_value(market_only_decomposition)),
        ):
            inner_line = _render_bullet(label, value)
            if inner_line:
                lines.append(inner_line)
    return lines


def _serialize_id_subsections(items: list[Any], role_label: str, renderer: Any) -> list[str]:
    lines: list[str] = []
    id_attr = "id" if role_label == "factor_id" else role_label
    for item in items:
        lines.append(f"### {role_label}:{getattr(item, id_attr, None) or str(uuid.uuid4())}")
        for line in renderer(item):
            if line:
                lines.append(line)
        lines.append("")
    return _trim_blank_edges(lines)






__all__ = [
    "ParsedThesis",
    "ParseWarning",
    "PriorNormalizedEntry",
    "PriorThesisIndex",
    "parse_thesis_markdown",
    "serialize_thesis",
    "thesis_markdown_path",
]
