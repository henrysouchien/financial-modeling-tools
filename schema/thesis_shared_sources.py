from __future__ import annotations

import re
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator

from .thesis_shared_slice_helpers import (
    _SKILL_NAME_RE,
    _normalize_optional_identifier,
    _normalize_workspace_relative_path,
)


SourceType = Literal["filing", "transcript", "document", "investor_deck", "other", "skill_artifact"]
SourceId = Annotated[
    str, StringConstraints(strip_whitespace=True, pattern=r"^src_[1-9]\d*$")
]
_CANONICAL_DOCUMENT_SOURCE_ID_RE = re.compile(r"^doc:[0-9a-f]{32}$")
_DOCUMENT_SPINE_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
ExcerptLocatorKind = Literal[
    "text_range", "section", "page", "external_anchor", "reader_anchor", "document_quote", "unknown"
]
ReaderTableValueSource = Literal["edgar_financials_table", "edgar_statement", "xbrl_fact"]
ScalarValue: TypeAlias = str | int | float | bool


class _ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )


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
    anchor_kind: Literal["filing_quote", "filing_mapped", "filing_table_cell", "document_quote"] | None = None
    confidence: Literal["quote", "section_only", "none", "exact", "high"] | None = None
    source_id: str | None = None
    document_id: str | None = None
    page_number: int | None = None
    spine_hash: str | None = None
    extraction_version: int | None = None
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
        if self.kind == "document_quote":
            if self.page_number is not None and self.page_number < 1:
                raise ValueError("document_quote locator page_number must be >= 1")
            if self.spine_hash is not None and not _DOCUMENT_SPINE_HASH_RE.fullmatch(self.spine_hash):
                raise ValueError("document_quote locator spine_hash must be 64 lowercase hex")
            if self.extraction_version is not None and self.extraction_version < 1:
                raise ValueError("document_quote locator extraction_version must be >= 1")
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
            elif self.anchor_kind in {"filing_quote", "document_quote"}:
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
    document_id: str | None = None
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
    page_number: int | None = None
    spine_hash: str | None = None
    extraction_version: int | None = None

    @model_validator(mode="after")
    def _validate_source_record(self) -> "SourceRecord":
        if (
            self.char_start is not None
            and self.char_end is not None
            and self.char_end < self.char_start
        ):
            raise ValueError("char_end must be greater than or equal to char_start")
        document_identity_fields = {
            "document_id": self.document_id,
            "page_number": self.page_number,
            "spine_hash": self.spine_hash,
            "extraction_version": self.extraction_version,
        }
        doc_like_ids = [
            value
            for value in (self.source_id, self.endpoint_or_filing_id, self.document_id)
            if isinstance(value, str) and value.strip().lower().startswith("doc:")
        ]
        if self.type != "document" and (doc_like_ids or any(value is not None for value in document_identity_fields.values())):
            raise ValueError("doc:<hash> source identity requires type='document'")
        if self.type == "document":
            if not _CANONICAL_DOCUMENT_SOURCE_ID_RE.fullmatch(self.source_id):
                raise ValueError("document source_id must be canonical doc:<32 lowercase hex>")
            if self.document_id is not None and self.document_id != self.source_id:
                raise ValueError("document document_id must match source_id")
            if self.endpoint_or_filing_id is not None and not _CANONICAL_DOCUMENT_SOURCE_ID_RE.fullmatch(
                self.endpoint_or_filing_id
            ):
                raise ValueError("document endpoint_or_filing_id must be canonical doc:<32 lowercase hex>")
            if self.page_number is not None and self.page_number < 1:
                raise ValueError("document page_number must be >= 1")
            if self.spine_hash is not None and not _DOCUMENT_SPINE_HASH_RE.fullmatch(self.spine_hash):
                raise ValueError("document spine_hash must be 64 lowercase hex")
            if self.extraction_version is not None and self.extraction_version < 1:
                raise ValueError("document extraction_version must be >= 1")
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


__all__ = [
    "Excerpt",
    "ExcerptLocator",
    "ExcerptLocatorKind",
    "ReaderTableContext",
    "ReaderTableFilingIdentity",
    "ReaderTableValueSource",
    "ScalarValue",
    "SourceId",
    "SourceRecord",
    "SourceType",
]
