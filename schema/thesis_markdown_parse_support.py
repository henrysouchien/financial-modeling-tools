from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any
import uuid

from pydantic import Field, ValidationError

from .thesis import (
    BusinessModelRef,
    DecisionsLogEntry,
    PositionMetadata,
    QuantitativeFraming,
    Thesis,
    ThesisFromIdea,
    ThesisLink,
    ThesisModelRef,
    UnknownSection,
)
from .thesis_markdown_utils import _has_meaningful_value, _normalize_label_key
from .thesis_shared_slice import (
    Assumption,
    BusinessOverview,
    Catalyst,
    CompanyField,
    ConsensusView,
    DataGap,
    DifferentiatedViewClaim,
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
    _ContractModel,
)

_SYNC_HEADER_PREFIX = "<!-- memory-sync:"
_SYNC_VERSION_RE = re.compile(r"\b(?:version|thesis_version)=(?P<version>[1-9]\d*)\b")
_TITLE_PREFIX = "# Thesis — "
_TITLE_RE = re.compile(r"^# Thesis — (?P<ticker>[A-Za-z0-9.]+)(?:: (?P<label>.+))?$")
_SECTION_HEADER_RE = re.compile(r"^## (?P<header>.+)$")
_SUBSECTION_ID_RE = re.compile(r"^(?P<role>[a-z_]+_id):(?P<identifier>.+)$")
_LABELED_LINE_RE = re.compile(r"^(?:- )?\*\*(?P<label>.+?)\*\*:\s*(?P<value>.*)$")
_NUMBERED_ITEM_RE = re.compile(r"^\d+\.\s+\[(?P<source_id>src_[1-9]\d*)\](?:\s*\|\s*(?P<meta>.*))?$")
_SOURCE_EXCERPT_RE = re.compile(r"^\s{3,}-\s+excerpt:\s*(?P<payload>.+?)\s*$")
_TRIGGER_BULLET_RE = re.compile(r"^- (?:\{(?P<identifier>[^}]+)\}\s*)?(?P<description>[^|]+?)(?:\s+\|\s+(?P<meta>.*))?$")

_SECTION_SPECS: tuple[tuple[str, str], ...] = (
    ("thesis_statement", "Thesis Statement"),
    ("thesis_metadata", "Thesis Metadata"),
    ("from_idea", "From Idea"),
    ("consensus_view", "Consensus View"),
    ("materiality", "Materiality Threshold"),
    ("differentiated_view", "Differentiated View"),
    ("invalidation_triggers", "Invalidation Triggers"),
    ("business_overview", "Business Overview"),
    ("catalysts", "Catalysts"),
    ("risks", "Risks"),
    ("valuation", "Valuation"),
    ("peers", "Peers"),
    ("assumptions", "Assumptions"),
    ("historical_coincidences", "Historical Coincidences"),
    ("data_gaps", "Data Gaps"),
    ("qualitative_factors", "Qualitative Factors"),
    ("ownership", "Ownership"),
    ("monitoring", "Monitoring"),
    ("industry_analysis", "Industry Analysis"),
    ("quantitative_framing", "Quantitative Framing"),
    ("position_metadata", "Position Metadata"),
    ("sources", "Sources"),
    ("model_links", "Model Linkage"),
    ("decisions_log", "Decisions Log"),
)
_ANCHOR_TO_HEADER = dict(_SECTION_SPECS)
_HEADER_TO_ANCHOR = {header: anchor for anchor, header in _SECTION_SPECS}
_CANONICAL_ANCHORS = tuple(anchor for anchor, _ in _SECTION_SPECS)
_ROLE_TO_PRIOR_FIELD = {
    "claims": "claims_by_id",
    "risks": "risks_by_id",
    "catalysts": "catalysts_by_id",
    "assumptions": "assumptions_by_id",
    "triggers": "triggers_by_id",
    "factors": "factors_by_id",
}


class ParseWarning(_ContractModel):
    code: str
    section: str | None = None
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class PriorNormalizedEntry(_ContractModel):
    primary_text: str
    structured_fields: dict[str, Any] = Field(default_factory=dict)


class PriorThesisIndex(_ContractModel):
    claims_by_id: dict[str, PriorNormalizedEntry] = Field(default_factory=dict)
    risks_by_id: dict[str, PriorNormalizedEntry] = Field(default_factory=dict)
    catalysts_by_id: dict[str, PriorNormalizedEntry] = Field(default_factory=dict)
    assumptions_by_id: dict[str, PriorNormalizedEntry] = Field(default_factory=dict)
    triggers_by_id: dict[str, PriorNormalizedEntry] = Field(default_factory=dict)
    factors_by_id: dict[int, PriorNormalizedEntry] = Field(default_factory=dict)

    @classmethod
    def from_thesis(cls, thesis: Thesis) -> "PriorThesisIndex":
        data: dict[str, dict[Any, PriorNormalizedEntry]] = {field_name: {} for field_name in _ROLE_TO_PRIOR_FIELD.values()}
        for item in thesis.differentiated_view:
            if item.claim_id:
                data["claims_by_id"][item.claim_id] = _build_prior_entry("claims", item.model_dump(mode="json"))
        for item in thesis.risks:
            if item.risk_id:
                data["risks_by_id"][item.risk_id] = _build_prior_entry("risks", item.model_dump(mode="json"))
        for item in thesis.catalysts:
            if item.catalyst_id:
                data["catalysts_by_id"][item.catalyst_id] = _build_prior_entry("catalysts", item.model_dump(mode="json"))
        for item in thesis.assumptions:
            if item.assumption_id:
                data["assumptions_by_id"][item.assumption_id] = _build_prior_entry("assumptions", item.model_dump(mode="json"))
        for item in thesis.invalidation_triggers:
            if item.trigger_id:
                data["triggers_by_id"][item.trigger_id] = _build_prior_entry("triggers", item.model_dump(mode="json"))
        for item in thesis.qualitative_factors:
            if item.id:
                data["factors_by_id"][item.id] = _build_prior_entry("factors", item.model_dump(mode="json"))
        return cls.model_validate(data)


class ParsedThesis(_ContractModel):
    ticker: str
    label: str | None = None
    version_marker: int | None = None
    marker_present: bool = False
    company: CompanyField | None = None
    thesis: ThesisField | None = None
    from_idea: ThesisFromIdea | None = None
    consensus_view: ConsensusView | None = None
    materiality: MaterialityThreshold | None = None
    differentiated_view: list[DifferentiatedViewClaim] | None = None
    invalidation_triggers: list[InvalidationTrigger] | None = None
    business_overview: BusinessOverview | None = None
    catalysts: list[Catalyst] | None = None
    risks: list[Risk] | None = None
    valuation: Valuation | None = None
    peers: list[Peer] | None = None
    assumptions: list[Assumption] | None = None
    historical_coincidences: list[HistoricalCoincidence] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)
    qualitative_factors: list[QualitativeFactor] | None = None
    ownership: Ownership | None = None
    monitoring: Monitoring | None = None
    industry_analysis: IndustryAnalysis | None = None
    quantitative_framing: QuantitativeFraming | None = None
    position_metadata: PositionMetadata | None = None
    sources: list[SourceRecord] | None = None
    model_ref: ThesisModelRef | None = None
    business_model_ref: BusinessModelRef | None = None
    model_links: list[ThesisLink] | None = None
    decisions_log: list[DecisionsLogEntry] | None = None
    raw_markdown_extras: list[UnknownSection] = Field(default_factory=list)
    parse_warnings: list[ParseWarning] = Field(default_factory=list)


@dataclass(frozen=True)
class _MarkdownSection:
    header: str
    body_lines: list[str]
    raw_text: str


@dataclass
class _ParseState:
    prior: PriorThesisIndex | None
    warnings: list[ParseWarning]
    claimed_prior_ids: dict[str, set[str | int]]
    referenced_source_ids: set[str]


def _extract_version_marker(lines: list[str]) -> tuple[int | None, bool]:
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith(_SYNC_HEADER_PREFIX):
            continue
        match = _SYNC_VERSION_RE.search(stripped)
        if match is None:
            return None, False
        return int(match.group("version")), True
    return None, False


def _trim_blank_edges(lines: list[str]) -> list[str]:
    trimmed = list(lines)
    while trimmed and not trimmed[0].strip():
        trimmed.pop(0)
    while trimmed and not trimmed[-1].strip():
        trimmed.pop()
    return trimmed


def _split_sections(lines: list[str]) -> list[_MarkdownSection]:
    sections: list[_MarkdownSection] = []
    current_header: str | None = None
    current_lines: list[str] = []
    for line in lines:
        match = _SECTION_HEADER_RE.match(line)
        if match:
            if current_header is not None:
                sections.append(_MarkdownSection(current_header, _trim_blank_edges(current_lines), "\n".join([f"## {current_header}", *current_lines]).strip("\n")))
            current_header = match.group("header").strip()
            current_lines = []
            continue
        if current_header is None:
            if line.strip().startswith("<!--") or not line.strip():
                continue
            continue
        current_lines.append(line)
    if current_header is not None:
        sections.append(_MarkdownSection(current_header, _trim_blank_edges(current_lines), "\n".join([f"## {current_header}", *current_lines]).strip("\n")))
    return sections


def _merge_parsed_payload(target: dict[str, Any], payload: dict[str, Any]) -> None:
    for key, value in payload.items():
        if key in {"thesis", "company"} and value is not None:
            current = target.setdefault(key, {})
            current.update(value)
        else:
            target[key] = value


def _split_subsections(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {"": []}
    current = ""
    for line in lines:
        if line.startswith("### ") and not line.startswith("#### "):
            current = line[4:].strip()
            sections.setdefault(current, [])
            continue
        sections.setdefault(current, []).append(line)
    return {key: _trim_blank_edges(value) for key, value in sections.items()}


def _split_nested_subsections(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current = ""
    for line in lines:
        if line.startswith("#### "):
            current = line[5:].strip()
            sections.setdefault(current, [])
            continue
        sections.setdefault(current, []).append(line)
    return {key: _trim_blank_edges(value) for key, value in sections.items() if key}


def _collect_labeled_lines(lines: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        match = _LABELED_LINE_RE.match(line)
        if match is not None:
            values[_normalize_label_key(match.group("label"))] = match.group("value").strip()
    return values


def _split_bullet_with_metadata(text: str) -> tuple[str, str]:
    parts = [part.strip() for part in text.split("|")]
    return (parts[0] if parts else "", " | ".join(parts[1:]) if len(parts) > 1 else "")


def _safe_validate(model_type: Any, payload: dict[str, Any], state: _ParseState, section: str) -> Any | None:
    try:
        return model_type.model_validate(payload)
    except ValidationError as exc:
        state.warnings.append(ParseWarning(code="section_parse_error", section=section, message=f"failed to validate parsed {section} payload", context={"errors": exc.errors(include_url=False)}))
        return None


def _resolve_item_id(role: str, subsection_title: str | None, payload: dict[str, Any], state: _ParseState) -> str | int:
    explicit_id = _extract_explicit_id(role, subsection_title)
    if explicit_id is not None:
        return explicit_id
    if state.prior is None:
        return str(uuid.uuid4())
    matched = _match_prior_id(role, payload, state)
    if matched is not None:
        state.claimed_prior_ids[role].add(matched)
        return matched
    new_id = str(uuid.uuid4())
    state.warnings.append(ParseWarning(code="id_reassigned", section=role, message=f"assigned new id for {role} item because no safe prior match was found", context={"role": role, "new_id": new_id}))
    return new_id


def _extract_explicit_id(role: str, subsection_title: str | None) -> str | None:
    if not subsection_title:
        return None
    match = _SUBSECTION_ID_RE.match(subsection_title.strip())
    if match is None:
        return None
    expected_role = {"claims": "claim_id", "risks": "risk_id", "catalysts": "catalyst_id", "assumptions": "assumption_id", "factors": "factor_id"}.get(role)
    if match.group("role") != expected_role:
        return None
    return match.group("identifier").strip() or None


def _match_prior_id(role: str, payload: dict[str, Any], state: _ParseState) -> str | int | None:
    if state.prior is None:
        return None
    entry = _build_prior_entry(role, payload)
    candidates: dict[str | int, PriorNormalizedEntry] = getattr(state.prior, _ROLE_TO_PRIOR_FIELD[role])
    scored: list[tuple[float, str]] = []
    for identifier, candidate in candidates.items():
        if identifier in state.claimed_prior_ids[role]:
            continue
        keys = [key for key, value in entry.structured_fields.items() if _has_meaningful_value(value)]
        structured_score = 1.0 if not keys else sum(1 for key in keys if _normalize_match_value(entry.structured_fields.get(key)) == _normalize_match_value(candidate.structured_fields.get(key))) / len(keys)
        score = 0.7 * _jaccard(entry.primary_text, candidate.primary_text) + 0.3 * structured_score
        scored.append((score, identifier))
    if not scored:
        return None
    scored.sort(reverse=True)
    best_score, best_identifier = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    if best_score >= 0.90 and (best_score - second_score) >= 0.10:
        return best_identifier
    return None


def _normalize_match_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_match_value(val) for key, val in sorted(value.items())}
    if isinstance(value, list):
        return [_normalize_match_value(item) for item in value]
    if value is None:
        return None
    return str(value).strip().lower()


def _jaccard(left: str, right: str) -> float:
    left_tokens = set(re.findall(r"[a-z0-9]+", left.lower()))
    right_tokens = set(re.findall(r"[a-z0-9]+", right.lower()))
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _build_prior_entry(role: str, payload: dict[str, Any]) -> PriorNormalizedEntry:
    if role == "claims":
        primary = payload.get("claim") or ""
        structured = {"rationale": payload.get("rationale"), "evidence": payload.get("evidence") or [], "upside_if_right": payload.get("upside_if_right"), "downside_if_wrong": payload.get("downside_if_wrong")}
    elif role == "risks":
        primary = payload.get("description") or ""
        structured = {"severity": payload.get("severity"), "type": payload.get("type"), "source_ref": payload.get("source_ref")}
    elif role == "catalysts":
        primary = payload.get("description") or ""
        structured = {"expected_date": payload.get("expected_date"), "severity": payload.get("severity"), "source_ref": payload.get("source_ref")}
    elif role == "assumptions":
        primary = payload.get("driver") or ""
        structured = {"value": payload.get("value"), "unit": payload.get("unit"), "rationale": payload.get("rationale"), "driver_category": payload.get("driver_category"), "confidence": payload.get("confidence"), "held_at_base": payload.get("held_at_base", False), "source_refs": payload.get("source_refs") or []}
    elif role == "triggers":
        primary = payload.get("description") or ""
        structured = {"metric": payload.get("metric"), "threshold": payload.get("threshold"), "direction": payload.get("direction")}
    else:
        primary = f"{payload.get('label') or ''} {payload.get('assessment') or ''}".strip()
        structured = {"category": payload.get("category"), "rating": payload.get("rating"), "data": payload.get("data"), "source_refs": payload.get("source_refs") or []}
    return PriorNormalizedEntry(primary_text=primary, structured_fields=structured)


__all__ = [
    "ParseWarning",
    "ParsedThesis",
    "PriorNormalizedEntry",
    "PriorThesisIndex",
    "_ANCHOR_TO_HEADER",
    "_CANONICAL_ANCHORS",
    "_HEADER_TO_ANCHOR",
    "_LABELED_LINE_RE",
    "_MarkdownSection",
    "_NUMBERED_ITEM_RE",
    "_ParseState",
    "_ROLE_TO_PRIOR_FIELD",
    "_SECTION_HEADER_RE",
    "_SECTION_SPECS",
    "_SOURCE_EXCERPT_RE",
    "_SUBSECTION_ID_RE",
    "_SYNC_HEADER_PREFIX",
    "_SYNC_VERSION_RE",
    "_TITLE_PREFIX",
    "_TITLE_RE",
    "_TRIGGER_BULLET_RE",
    "_build_prior_entry",
    "_collect_labeled_lines",
    "_extract_explicit_id",
    "_extract_version_marker",
    "_jaccard",
    "_match_prior_id",
    "_merge_parsed_payload",
    "_normalize_match_value",
    "_resolve_item_id",
    "_safe_validate",
    "_split_bullet_with_metadata",
    "_split_nested_subsections",
    "_split_sections",
    "_split_subsections",
    "_trim_blank_edges",
]
