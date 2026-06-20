from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from .enum_canonicalizers import (
    canonicalize_optional_direction,
    canonicalize_optional_strategy,
    canonicalize_optional_time_horizon,
    canonicalize_optional_timeframe,
)
from .models import DriverCategory
from .process_template import StrategyCategoryId
from .thesis_shared_slice_helpers import (
    _EXCHANGE_SUFFIXES as _EXCHANGE_SUFFIXES,
    _LEVEL_VALUES as _LEVEL_VALUES,
    _SKILL_NAME_RE as _SKILL_NAME_RE,
    _SHARE_CLASS_SUFFIXES as _SHARE_CLASS_SUFFIXES,
    _TICKER_RE as _TICKER_RE,
    _VALUATION_METHODS as _VALUATION_METHODS,
    _VALUATION_METHOD_ALIASES as _VALUATION_METHOD_ALIASES,
    _VALUATION_NUMERIC_FIELDS as _VALUATION_NUMERIC_FIELDS,
    _VALUATION_RATE_FIELDS as _VALUATION_RATE_FIELDS,
    _normalize_level as _normalize_level,
    _normalize_optional_identifier as _normalize_optional_identifier,
    _normalize_ticker as _normalize_ticker,
    _normalize_valuation_method as _normalize_valuation_method,
    _normalize_workspace_relative_path as _normalize_workspace_relative_path,
)


DirectionValue = Literal["long", "short", "hedge", "pair"]
TimeframeValue = Literal["near_term", "medium", "long_term"]
ConfidenceLevel = Literal["low", "medium", "high"]
SourceType = Literal["filing", "transcript", "investor_deck", "other", "skill_artifact"]
DerivedFromKind = Literal["catalyst", "trigger"]
DataGapStatus = Literal["open", "resolved", "superseded", "retained"]
SourceId = Annotated[
    str, StringConstraints(strip_whitespace=True, pattern=r"^src_[1-9]\d*$")
]
ExcerptLocatorKind = Literal[
    "text_range", "section", "page", "external_anchor", "reader_anchor", "unknown"
]
ReaderTableValueSource = Literal["edgar_financials_table", "edgar_statement", "xbrl_fact"]
ScalarValue: TypeAlias = str | int | float | bool
ThresholdValue: TypeAlias = str | int | float

class _ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )


class CompanyField(_ContractModel):
    ticker: str
    name: str | None = None
    sector: str | None = None
    industry: str | None = None
    fiscal_year_end: str | None = None
    most_recent_fy: int | None = None
    exchange: str | None = None
    cik: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class ThesisField(_ContractModel):
    statement: str | None = None
    direction: DirectionValue | None = None
    strategy: StrategyCategoryId | None = None
    conviction: int | None = None
    timeframe: TimeframeValue | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("direction", mode="before")
    @classmethod
    def _canonicalize_direction(cls, value: object | None) -> str | None:
        return canonicalize_optional_direction(value)

    @field_validator("strategy", mode="before")
    @classmethod
    def _canonicalize_strategy(cls, value: object | None) -> str | None:
        return canonicalize_optional_strategy(value)

    @field_validator("timeframe", mode="before")
    @classmethod
    def _canonicalize_timeframe(cls, value: object | None) -> str | None:
        return canonicalize_optional_timeframe(value)

    @field_validator("conviction", mode="before")
    @classmethod
    def _validate_conviction(cls, value: object | None) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("conviction must be an int between 1 and 5")
        if value < 1 or value > 5:
            raise ValueError("conviction must be between 1 and 5")
        return value


class ConsensusView(_ContractModel):
    narrative: str
    basis: Literal["gaap", "non_gaap"] | None = None
    citations: list[SourceId] = Field(default_factory=list)


class MaterialityThreshold(_ContractModel):
    """Threshold used to filter critical-factor candidates and judge thesis violations."""

    basis: str
    threshold_pct: float = Field(gt=0, le=100)
    metric: str | None = None
    horizon: str | None = None
    rationale: str
    source_refs: list[SourceId] = Field(default_factory=list)


class HistoricalCoincidence(_ContractModel):
    """Episode where a critical factor's movement coincided with a stock outcome."""

    coincidence_id: str = Field(min_length=1)
    period: str
    factor: str
    assumption_id: str | None = None
    market_reaction: str
    factor_direction: str | None = None
    stock_outcome: Literal[
        "outperformance", "underperformance", "neutral", "mixed"
    ] | None = None
    driver: str
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("coincidence_id", "assumption_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class DataGap(_ContractModel):
    """A known data unavailability that affected the analysis."""

    gap_id: str | None = None
    target_handle: str | None = None
    description: str
    workaround: str | None = None
    severity: Literal["blocking", "approximate", "minor"] | None = None
    status: DataGapStatus = "open"
    remediation: dict[str, Any] | None = None
    resolution_note: str | None = None
    resolution_source_refs: list[SourceId] = Field(default_factory=list)
    resolved_at: str | None = None
    superseded_by_gap_id: str | None = None

    @field_validator("gap_id", "target_handle", "superseded_by_gap_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @model_validator(mode="before")
    @classmethod
    def _coerce_string(cls, value: object) -> object:
        if isinstance(value, str):
            return {"description": value}
        return value

    @model_validator(mode="after")
    def _validate_lifecycle(self) -> "DataGap":
        if self.status == "open":
            if self.superseded_by_gap_id is not None:
                raise ValueError("open data gaps cannot set superseded_by_gap_id")
            return self
        if not self.resolution_note and not self.resolution_source_refs:
            raise ValueError(
                "non-open data gaps require resolution_note or resolution_source_refs"
            )
        if self.status == "superseded" and not self.superseded_by_gap_id:
            raise ValueError("superseded data gaps require superseded_by_gap_id")
        if self.status != "superseded" and self.superseded_by_gap_id is not None:
            raise ValueError(
                "superseded_by_gap_id is only valid when status is superseded"
            )
        return self


class DifferentiatedViewClaim(_ContractModel):
    claim_id: str | None = None
    claim: str
    rationale: str
    evidence: list[SourceId] = Field(default_factory=list)
    upside_if_right: str | None = None
    downside_if_wrong: str | None = None

    @field_validator("claim_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class InvalidationTrigger(_ContractModel):
    trigger_id: str | None = None
    description: str
    metric: str | None = None
    threshold: ThresholdValue | None = None
    threshold_direction: Literal["above", "below"] | None = None
    direction: DirectionValue | None = None
    source_refs: list[SourceId] = Field(min_length=1)

    @field_validator("trigger_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("direction", mode="before")
    @classmethod
    def _canonicalize_direction(cls, value: object | None) -> str | None:
        return canonicalize_optional_direction(value)


class BusinessSegment(_ContractModel):
    name: str
    rev_pct: float | None = None


class BusinessOverview(_ContractModel):
    description: str | None = None
    segments: list[BusinessSegment] = Field(default_factory=list)
    source_refs: list[SourceId] = Field(default_factory=list)


class Catalyst(_ContractModel):
    catalyst_id: str | None = None
    description: str
    expected_date: str | None = None
    severity: str
    source_ref: SourceId | None = None

    @field_validator("catalyst_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Risk(_ContractModel):
    risk_id: str | None = None
    description: str
    severity: str
    type: str | None = None
    source_ref: SourceId | None = None

    @field_validator("risk_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Valuation(_ContractModel):
    method: str | None = None
    low: float | None = None
    mid: float | None = None
    high: float | None = None
    current_multiple: float | None = None
    wacc: float | None = None
    risk_free_rate: float | None = None
    equity_risk_premium: float | None = None
    cost_of_equity: float | None = None
    raw_beta: float | None = None
    adjusted_beta: float | None = None
    beta_floor: float | None = None
    terminal_growth_rate: float | None = None
    terminal_multiple: float | None = None
    rationale: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("method", mode="before")
    @classmethod
    def _normalize_method(cls, value: object | None) -> str | None:
        return _normalize_valuation_method(value)

    @field_validator(*_VALUATION_NUMERIC_FIELDS, mode="before")
    @classmethod
    def _reject_bool_numeric_inputs(cls, value: object | None) -> object | None:
        if isinstance(value, bool):
            raise ValueError("valuation numeric fields cannot be boolean")
        return value

    @model_validator(mode="after")
    def _rate_fields_use_decimal_scale(self) -> "Valuation":
        for field_name in _VALUATION_RATE_FIELDS:
            value = getattr(self, field_name)
            if value is None:
                continue
            if not -1.0 <= float(value) <= 1.0:
                raise ValueError(
                    f"valuation.{field_name} must be a decimal rate between -1 and 1"
                )
        return self


class Peer(_ContractModel):
    ticker: str
    name: str
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class Assumption(_ContractModel):
    assumption_id: str | None = None
    driver: str
    value: ScalarValue | None = None
    unit: str | None = None
    rationale: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)
    driver_category: DriverCategory | None = None
    confidence: ConfidenceLevel | None = None
    held_at_base: bool = False
    # True means scenario engine intentionally did NOT flex this assumption:
    # held at recent average / management guidance / task input. Distinct from
    # "the base-case value of a flexed assumption" (that's just `value`).

    @field_validator("assumption_id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("confidence", mode="before")
    @classmethod
    def _canonicalize_confidence(cls, value: object | None) -> str | None:
        return _normalize_level(value)


class QualitativeFactor(_ContractModel):
    id: int | None = None
    category: str
    label: str
    assessment: str = Field(min_length=1)
    rating: ConfidenceLevel | None = None
    data: dict[str, Any] | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("rating", mode="before")
    @classmethod
    def _canonicalize_rating(cls, value: object | None) -> str | None:
        return _normalize_level(value)


class Ownership(_ContractModel):
    institutional_pct: float | None = None
    insider_pct: float | None = None
    recent_activity: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("institutional_pct", "insider_pct")
    @classmethod
    def _normalize_percentage_scale(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if value > 1.0:
            return value / 100.0
        return value

    @model_validator(mode="after")
    def _validate_evidence(self) -> "Ownership":
        has_content = (
            self.institutional_pct is not None
            or self.insider_pct is not None
            or (self.recent_activity is not None and self.recent_activity.strip())
        )
        if has_content and not self.source_refs:
            raise ValueError(
                "Ownership with populated fields requires non-empty source_refs (R1)"
            )
        return self


class WatchItem(BaseModel):
    watch_item_id: str | None = None
    description: str = Field(min_length=1)
    metric: str | None = None
    threshold: ThresholdValue | None = None
    threshold_direction: Literal["above", "below"] | None = None
    last_checked: str | None = None
    source_refs: list[SourceId] = Field(min_length=1)
    derived_from_handle: str | None = None
    derived_from_kind: DerivedFromKind | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    @field_validator("watch_item_id", "derived_from_handle", mode="before")
    @classmethod
    def _normalize_watch_item_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


class Monitoring(_ContractModel):
    watch_list: list[WatchItem] = Field(default_factory=list)
    source_refs: list[SourceId] = Field(default_factory=list)


class ReaderTableFilingIdentity(_ContractModel):
    document_id: str | None = None
    accession: str
    primary_document_url: str
    cik: str | None = None
    form_type: str | None = None
    source: str | None = None
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None


class ReaderTableContext(_ContractModel):
    table_citation_record_id: str | None = None
    table_index: int | None = None
    row_index: int | None = None
    column_index: int | None = None
    row_header: str | None = None
    column_header: str | None = None
    table_value_source: ReaderTableValueSource
    value_filing_identity: ReaderTableFilingIdentity
    table_id: str
    table_section_key: str | None = None
    table_section_header: str | None = None
    raw_cell_text: str
    parsed_value_text: str
    parsed_numeric_value: str
    unit: str | None = None
    scale: str | None = None
    period: str | None = None
    concept: str | None = None
    fact_id: str | None = None
    reader_source_html_hash: str
    resolver_version: str
    resolved_at: str
    mismatch_diagnostics: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_table_context(self) -> "ReaderTableContext":
        if self.row_index is None and not self.row_header:
            raise ValueError("table_context row identity is required")
        if self.column_index is None and not self.column_header:
            raise ValueError("table_context column identity is required")
        return self


class ExcerptLocator(_ContractModel):
    kind: ExcerptLocatorKind
    section_header: str | None = None
    speaker: str | None = None
    char_start: int | None = None
    char_end: int | None = None
    page: int | None = None
    anchor: str | None = None
    reader_anchor_version: Literal["v2"] | None = None
    anchor_kind: Literal["filing_quote", "filing_mapped", "filing_table_cell"] | None = None
    confidence: Literal["quote", "section_only", "none", "exact", "high"] | None = None
    source_id: str | None = None
    document_id: str | None = None
    source_html_hash: str | None = None
    corpus_content_hash: str | None = None
    visible_text_anchor: dict[str, Any] | None = None
    anchor_hash: str | None = None
    mapping_record_id: str | None = None
    mapping_algorithm_version: str | None = None
    offset_frame: Literal["corpus_doc"] | None = None
    table_citation_record_id: str | None = None
    table_context: ReaderTableContext | None = None

    @model_validator(mode="after")
    def _validate_locator(self) -> "ExcerptLocator":
        if (
            self.char_start is not None
            and self.char_end is not None
            and self.char_end < self.char_start
        ):
            raise ValueError("char_end must be greater than or equal to char_start")
        if self.kind == "text_range" and (
            self.char_start is None or self.char_end is None
        ):
            raise ValueError("text_range locator requires char_start and char_end")
        if self.kind == "page" and (self.page is None or self.page < 1):
            raise ValueError("page locator requires page >= 1")
        if self.kind == "section" and not self.section_header:
            raise ValueError("section locator requires section_header")
        if self.kind == "external_anchor" and not self.anchor:
            raise ValueError("external_anchor locator requires anchor")
        if self.kind == "reader_anchor":
            required = {
                "reader_anchor_version": self.reader_anchor_version,
                "anchor_kind": self.anchor_kind,
                "confidence": self.confidence,
                "source_id": self.source_id,
                "document_id": self.document_id,
                "source_html_hash": self.source_html_hash,
                "corpus_content_hash": self.corpus_content_hash,
                "visible_text_anchor": self.visible_text_anchor,
                "anchor_hash": self.anchor_hash,
            }
            missing = sorted(name for name, value in required.items() if value is None)
            if missing:
                raise ValueError(
                    "reader_anchor locator missing required fields: "
                    + ", ".join(missing)
                )
            visible_anchor = self.visible_text_anchor or {}
            if visible_anchor.get("visible_text_offset_frame") != "source_html_visible_text_v1":
                raise ValueError(
                    "reader_anchor locator requires visible_text_anchor.visible_text_offset_frame "
                    "source_html_visible_text_v1"
                )
            if not str(visible_anchor.get("text_quote") or "").strip():
                raise ValueError("reader_anchor locator requires visible_text_anchor.text_quote")
            if self.anchor_kind == "filing_mapped":
                if (
                    self.char_start is None
                    or self.char_end is None
                    or self.offset_frame != "corpus_doc"
                    or not self.mapping_record_id
                    or not self.mapping_algorithm_version
                ):
                    raise ValueError(
                        "mapped reader_anchor locator requires char_start, char_end, "
                        "offset_frame corpus_doc, mapping_record_id, and mapping_algorithm_version"
                    )
                if self.confidence not in {"exact", "high"}:
                    raise ValueError("mapped reader_anchor locator requires exact or high confidence")
                if self.table_citation_record_id is not None or self.table_context is not None:
                    raise ValueError("mapped reader_anchor locator cannot carry table citation fields")
            elif self.anchor_kind == "filing_quote":
                if (
                    self.char_start is not None
                    or self.char_end is not None
                    or self.offset_frame is not None
                    or self.mapping_record_id is not None
                    or self.mapping_algorithm_version is not None
                    or self.table_citation_record_id is not None
                    or self.table_context is not None
                ):
                    raise ValueError("quote reader_anchor locator cannot carry mapping or table citation fields")
                if self.confidence not in {"quote", "section_only", "none"}:
                    raise ValueError(
                        "quote reader_anchor locator requires quote, section_only, or none confidence"
                    )
            elif self.anchor_kind == "filing_table_cell":
                if (
                    self.char_start is not None
                    or self.char_end is not None
                    or self.offset_frame is not None
                    or self.mapping_record_id is not None
                    or self.mapping_algorithm_version is not None
                ):
                    raise ValueError("table-cell reader_anchor locator cannot carry corpus mapping fields")
                if self.confidence not in {"exact", "high"}:
                    raise ValueError("table-cell reader_anchor locator requires exact or high confidence")
                if not self.table_citation_record_id:
                    raise ValueError("table-cell reader_anchor locator requires table_citation_record_id")
                if self.table_context is None:
                    raise ValueError("table-cell reader_anchor locator requires table_context")
                context_record_id = self.table_context.table_citation_record_id
                if context_record_id is not None and context_record_id != self.table_citation_record_id:
                    raise ValueError("table-cell reader_anchor locator table_context id mismatch")
                if self.table_context.reader_source_html_hash != self.source_html_hash:
                    raise ValueError("table-cell reader_anchor locator source_html_hash does not match table_context")
        return self


class Excerpt(_ContractModel):
    excerpt_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    locator: ExcerptLocator
    hash: str | None = None
    claim_ids: list[str] = Field(default_factory=list)
    created_by: str = "agent"
    created_at: str

    @field_validator("excerpt_id", "hash", mode="before")
    @classmethod
    def _normalize_identifier(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("claim_ids")
    @classmethod
    def _normalize_claim_ids(cls, value: list[str]) -> list[str]:
        normalized = sorted({str(item).strip() for item in value if str(item).strip()})
        return normalized


class SourceRecord(_ContractModel):
    id: SourceId
    type: SourceType
    source_id: str
    section_header: str | None = None
    char_start: int | None = None
    char_end: int | None = None
    text: str
    annotation_id: str | None = None
    provider: str | None = None
    endpoint_or_filing_id: str | None = None
    key_fields: dict[str, ScalarValue] | None = None
    retrieved_at: str | None = None
    excerpts: list[Excerpt] = Field(default_factory=list)
    identity_hash: str | None = None
    table_citation_record_id: str | None = None
    table_id: str | None = None
    table_value_source: ReaderTableValueSource | None = None
    parsed_value_text: str | None = None
    parsed_numeric_value: str | None = None
    unit: str | None = None
    period: str | None = None
    concept: str | None = None
    skill_name: str | None = None
    artifact_path: str | None = None
    artifact_id: str | None = None
    skill_run_id: str | None = None
    source_path: str | None = None

    @model_validator(mode="after")
    def _validate_source_record(self) -> "SourceRecord":
        if (
            self.char_start is not None
            and self.char_end is not None
            and self.char_end < self.char_start
        ):
            raise ValueError("char_end must be greater than or equal to char_start")
        skill_fields = {
            "skill_name": self.skill_name,
            "artifact_path": self.artifact_path,
            "artifact_id": self.artifact_id,
            "skill_run_id": self.skill_run_id,
            "source_path": self.source_path,
        }
        if self.type == "skill_artifact":
            if not self.skill_name:
                raise ValueError("skill_artifact sources require skill_name")
            if not _SKILL_NAME_RE.fullmatch(self.skill_name):
                raise ValueError("skill_name must use lowercase kebab-case")
            if not self.artifact_path:
                raise ValueError("skill_artifact sources require artifact_path")
            if not self.artifact_path.startswith("artifacts/"):
                raise ValueError("skill_artifact artifact_path must live under artifacts/")
            if self.source_path is not None and not self.source_path.startswith("notes/skills/"):
                raise ValueError("skill_artifact source_path must live under notes/skills/")
        elif any(value is not None for value in skill_fields.values()):
            raise ValueError("skill artifact metadata is only valid for type='skill_artifact'")
        table_fields = {
            "table_citation_record_id": self.table_citation_record_id,
            "table_id": self.table_id,
            "table_value_source": self.table_value_source,
            "parsed_value_text": self.parsed_value_text,
            "parsed_numeric_value": self.parsed_numeric_value,
            "unit": self.unit,
            "period": self.period,
            "concept": self.concept,
        }
        table_locators = [
            excerpt.locator
            for excerpt in self.excerpts
            if excerpt.locator.kind == "reader_anchor" and excerpt.locator.anchor_kind == "filing_table_cell"
        ]
        if any(value is not None for value in table_fields.values()):
            if self.type != "filing":
                raise ValueError("table evidence metadata is only valid for filing sources")
            if not table_locators:
                raise ValueError("table evidence metadata requires a filing_table_cell reader_anchor excerpt")
            required = {
                key: value
                for key, value in table_fields.items()
                if key in {
                    "table_citation_record_id",
                    "table_id",
                    "table_value_source",
                    "parsed_value_text",
                    "parsed_numeric_value",
                }
            }
            missing = sorted(key for key, value in required.items() if value is None)
            if missing:
                raise ValueError("table evidence metadata missing required fields: " + ", ".join(missing))
        for locator in table_locators:
            context = locator.table_context
            if context is None:
                continue
            if self.table_citation_record_id is not None and locator.table_citation_record_id != self.table_citation_record_id:
                raise ValueError("table evidence metadata does not match locator table_citation_record_id")
            if self.table_id is not None and context.table_id != self.table_id:
                raise ValueError("table evidence metadata does not match locator table_id")
            if self.table_value_source is not None and context.table_value_source != self.table_value_source:
                raise ValueError("table evidence metadata does not match locator table_value_source")
            if self.parsed_value_text is not None and context.parsed_value_text != self.parsed_value_text:
                raise ValueError("table evidence metadata does not match locator parsed_value_text")
            if self.parsed_numeric_value is not None and context.parsed_numeric_value != self.parsed_numeric_value:
                raise ValueError("table evidence metadata does not match locator parsed_numeric_value")
        return self

    @field_validator("skill_name", "artifact_id", "skill_run_id", mode="before")
    @classmethod
    def _normalize_optional_skill_identifier(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)

    @field_validator("artifact_path", "source_path", mode="before")
    @classmethod
    def _normalize_workspace_relative_path(cls, value: object | None) -> str | None:
        return _normalize_workspace_relative_path(value)


class IndustryLandscape(_ContractModel):
    narrative: str
    citations: list[SourceId] = Field(default_factory=list)


class CompsNarrative(_ContractModel):
    narrative: str
    citations: list[SourceId] = Field(default_factory=list)


class IndustryPeerComparisonPeer(_ContractModel):
    ticker: str
    name: str
    key_metrics: dict[str, ScalarValue] | None = None
    relative_position: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class EditorialPeer(_ContractModel):
    ticker: str
    name: str
    source: Literal["editorial"] = "editorial"
    added_by: str | None = None
    added_at: str | None = None
    rationale: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class CompMetricCell(_ContractModel):
    value: ScalarValue | None = None
    source_refs: list[SourceId] = Field(default_factory=list)
    derived: bool = False


class SnapshotMetric(_ContractModel):
    key: str = Field(min_length=1)
    label: str
    units: str | None = None
    values: dict[str, CompMetricCell] = Field(default_factory=dict)
    median: CompMetricCell | None = None


class SnapshotSection(_ContractModel):
    name: str
    metrics: list[SnapshotMetric] = Field(default_factory=list)


class TimeseriesMetric(_ContractModel):
    key: str = Field(min_length=1)
    label: str
    units: str | None = None
    series: dict[str, dict[int, CompMetricCell]] = Field(default_factory=dict)
    median_series: dict[int, CompMetricCell] = Field(default_factory=dict)


class TimeseriesGroup(_ContractModel):
    name: str
    metrics: list[TimeseriesMetric] = Field(default_factory=list)


class OperatingComparison(_ContractModel):
    industry_key: str
    template_manifest_id: str
    years: list[int] = Field(default_factory=list)
    metric_groups: list[TimeseriesGroup] = Field(default_factory=list)


class IndustryPeerComparison(_ContractModel):
    peers: list[IndustryPeerComparisonPeer] = Field(default_factory=list)
    industry_key: str | None = None
    template_manifest_id: str | None = None
    as_of: str | None = None
    sections: list[SnapshotSection] = Field(default_factory=list)


class MacroOverlayDriver(_ContractModel):
    description: str
    sensitivity: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)


class MacroOverlay(_ContractModel):
    drivers: list[MacroOverlayDriver] = Field(default_factory=list)


class StructuralTrend(_ContractModel):
    description: str
    time_horizon: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("time_horizon", mode="before")
    @classmethod
    def _canonicalize_time_horizon(cls, value: object | None) -> str | None:
        return canonicalize_optional_time_horizon(value)


class IndustryAnalysis(_ContractModel):
    landscape: IndustryLandscape | None = None
    comps_narrative: CompsNarrative | None = None
    peer_comparison: IndustryPeerComparison | None = None
    macro_overlay: MacroOverlay | None = None
    structural_trends: list[StructuralTrend] = Field(default_factory=list)
    editorial_peer_set: list[EditorialPeer] = Field(default_factory=list)
    operating_comparison: OperatingComparison | None = None

    @field_validator("editorial_peer_set")
    @classmethod
    def _reject_duplicate_editorial_peer_tickers(
        cls, value: list[EditorialPeer]
    ) -> list[EditorialPeer]:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for peer in value:
            if peer.ticker in seen:
                duplicates.add(peer.ticker)
            seen.add(peer.ticker)
        if duplicates:
            raise ValueError(
                "editorial_peer_set contains duplicate tickers: "
                f"{sorted(duplicates)}"
            )
        return value


__all__ = [
    "Assumption",
    "BusinessOverview",
    "BusinessSegment",
    "Catalyst",
    "CompanyField",
    "ConfidenceLevel",
    "CompMetricCell",
    "CompsNarrative",
    "ConsensusView",
    "DataGap",
    "DataGapStatus",
    "DerivedFromKind",
    "DifferentiatedViewClaim",
    "DirectionValue",
    "EditorialPeer",
    "Excerpt",
    "ExcerptLocator",
    "ExcerptLocatorKind",
    "HistoricalCoincidence",
    "IndustryAnalysis",
    "IndustryLandscape",
    "IndustryPeerComparison",
    "IndustryPeerComparisonPeer",
    "InvalidationTrigger",
    "MacroOverlay",
    "MacroOverlayDriver",
    "MaterialityThreshold",
    "Monitoring",
    "OperatingComparison",
    "Ownership",
    "Peer",
    "QualitativeFactor",
    "ReaderTableContext",
    "ReaderTableFilingIdentity",
    "ReaderTableValueSource",
    "Risk",
    "ScalarValue",
    "SnapshotMetric",
    "SnapshotSection",
    "SourceId",
    "SourceRecord",
    "SourceType",
    "StructuralTrend",
    "ThesisField",
    "ThresholdValue",
    "TimeseriesGroup",
    "TimeseriesMetric",
    "TimeframeValue",
    "Valuation",
    "WatchItem",
]
