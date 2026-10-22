from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from .thesis_markdown_parse_support import (
    ParseWarning,
    _NUMBERED_ITEM_RE,
    _ParseState,
    _SOURCE_EXCERPT_RE,
    _safe_validate,
)
from .thesis_markdown_utils import _parse_key_value_segments
from industry_slice_core.thesis_shared_sources import Excerpt, SourceRecord


_SOURCE_METADATA_FIELDS = frozenset(SourceRecord.model_fields) - {"id", "excerpts"}
_DEFAULT_PARSE_KEY_VALUE_SEGMENTS = _parse_key_value_segments


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
        metadata_lines = [match.group("meta") or ""]
        while index < len(body_lines):
            candidate = body_lines[index]
            if _SOURCE_EXCERPT_RE.match(candidate) is not None:
                break
            if _NUMBERED_ITEM_RE.match(candidate.strip()) is not None:
                break
            # Older canonical renders embedded raw multi-line source text in the
            # numbered record. Recover those physical continuation lines so a
            # read/re-render converts them to the escaped one-line contract.
            metadata_lines.append(candidate)
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
        metadata = _parse_source_metadata(
            "\n".join(metadata_lines),
            parse_key_value_segments=_parse_key_value_segments,
        )
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
                "document_id": metadata.get("document_id"),
                "endpoint_or_filing_id": metadata.get("endpoint_or_filing_id"),
                "key_fields": metadata.get("key_fields"),
                "retrieved_at": metadata.get("retrieved_at"),
                "table_citation_record_id": metadata.get("table_citation_record_id"),
                "table_id": metadata.get("table_id"),
                "table_value_source": metadata.get("table_value_source"),
                "parsed_value_text": metadata.get("parsed_value_text"),
                "parsed_numeric_value": metadata.get("parsed_numeric_value"),
                "unit": metadata.get("unit"),
                "period": metadata.get("period"),
                "concept": metadata.get("concept"),
                "skill_name": metadata.get("skill_name"),
                "artifact_path": metadata.get("artifact_path"),
                "artifact_id": metadata.get("artifact_id"),
                "skill_run_id": metadata.get("skill_run_id"),
                "source_path": metadata.get("source_path"),
                "page_number": metadata.get("page_number"),
                "spine_hash": metadata.get("spine_hash"),
                "extraction_version": metadata.get("extraction_version"),
                "excerpts": excerpts,
            },
            state,
            "sources",
        )
        if validated is not None:
            records.append(validated)
    return {"sources": records}


def _parse_source_metadata(
    raw_metadata: str,
    *,
    parse_key_value_segments: Any = _DEFAULT_PARSE_KEY_VALUE_SEGMENTS,
) -> dict[str, Any]:
    # Split only on delimiters that introduce actual SourceRecord fields. This
    # recovers legacy source text containing Markdown table pipes while keeping
    # the ordinary key=value format human-readable.
    if parse_key_value_segments is not _DEFAULT_PARSE_KEY_VALUE_SEGMENTS:
        raw_values = parse_key_value_segments(raw_metadata)
        return {
            key: _decode_source_metadata_value(value)
            for key, value in raw_values.items()
        }

    parsed: dict[str, Any] = {}
    for segment in _split_source_metadata_segments(raw_metadata):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        normalized_key = key.strip().lower()
        if normalized_key in _SOURCE_METADATA_FIELDS:
            parsed[normalized_key] = _decode_source_metadata_value(value.strip())
    return parsed


def _split_source_metadata_segments(raw_metadata: str) -> list[str]:
    segments: list[str] = []
    buffer: list[str] = []
    index = 0
    decoder = json.JSONDecoder()
    while index < len(raw_metadata):
        if raw_metadata.startswith("@json:", index):
            try:
                _, consumed = decoder.raw_decode(raw_metadata[index + len("@json:") :])
            except json.JSONDecodeError:
                pass
            else:
                end = index + len("@json:") + consumed
                buffer.append(raw_metadata[index:end])
                index = end
                continue
        if raw_metadata[index] == "|" and index > 0 and raw_metadata[index - 1].isspace():
            remainder = raw_metadata[index + 1 :]
            stripped = remainder.lstrip()
            possible_key = stripped.split("=", 1)[0].strip().lower()
            if "=" in stripped and possible_key in _SOURCE_METADATA_FIELDS:
                segments.append("".join(buffer).strip())
                buffer = []
                index += 1 + (len(remainder) - len(stripped))
                continue
        buffer.append(raw_metadata[index])
        index += 1
    segments.append("".join(buffer).strip())
    return segments


def _decode_source_metadata_value(value: Any) -> Any:
    if not isinstance(value, str) or not value.startswith("@json:"):
        return value
    try:
        return json.loads(value.removeprefix("@json:"))
    except json.JSONDecodeError:
        return value


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


__all__ = [
    "_parse_source_excerpt",
    "_parse_sources",
]
