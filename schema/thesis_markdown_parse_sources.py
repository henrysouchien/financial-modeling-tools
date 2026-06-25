from __future__ import annotations

import json
import sys
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
from .thesis_shared_slice import Excerpt, SourceRecord


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


def _parse_sources(body_lines: list[str], state: _ParseState) -> dict[str, Any]:
    records: list[SourceRecord] = []
    parse_warning_model = _compat("ParseWarning", ParseWarning)
    numbered_item_re = _compat("_NUMBERED_ITEM_RE", _NUMBERED_ITEM_RE)
    source_excerpt_re = _compat("_SOURCE_EXCERPT_RE", _SOURCE_EXCERPT_RE)
    parse_key_value_segments = _compat(
        "_parse_key_value_segments",
        _parse_key_value_segments,
    )
    parse_source_excerpt = _compat("_parse_source_excerpt", _parse_source_excerpt)
    safe_validate = _compat("_safe_validate", _safe_validate)
    source_record_model = _compat("SourceRecord", SourceRecord)
    index = 0
    while index < len(body_lines):
        line = body_lines[index]
        match = numbered_item_re.match(line.strip())
        if match is None:
            state.warnings.append(parse_warning_model(code="section_parse_error", section="sources", message="failed to parse source line", context={"line": line}))
            index += 1
            continue
        index += 1
        excerpts: list[Excerpt] = []
        while index < len(body_lines):
            excerpt_match = source_excerpt_re.match(body_lines[index])
            if excerpt_match is None:
                break
            excerpt = parse_source_excerpt(excerpt_match.group("payload"), state)
            if excerpt is not None:
                excerpts.append(excerpt)
            index += 1
        metadata = parse_key_value_segments(match.group("meta") or "")
        validated = safe_validate(
            source_record_model,
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
    parse_warning_model = _compat("ParseWarning", ParseWarning)
    excerpt_model = _compat("Excerpt", Excerpt)
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        state.warnings.append(
            parse_warning_model(
                code="section_parse_error",
                section="sources",
                message="failed to parse source excerpt JSON",
                context={"error": str(exc), "payload": raw_payload},
            )
        )
        return None
    try:
        return excerpt_model.model_validate(payload)
    except ValidationError as exc:
        state.warnings.append(
            parse_warning_model(
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
