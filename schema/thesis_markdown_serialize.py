from __future__ import annotations

import json
from typing import Any

from .model_ref import ModelVersionRef
from .thesis import (
    BusinessModelRef,
    DecisionsLogEntry,
    ThesisFromIdea,
    ThesisLink,
)
from industry_slice_core.thesis_shared_sources import Excerpt, SourceRecord
from .thesis_markdown_utils import (
    _format_table,
    _has_meaningful_value,
    _render_bullet,
    _serialize_json_value,
    _split_lines,
)


def _serialize_sources(sources: list[SourceRecord]) -> list[str]:
    lines: list[str] = []
    for index, source in enumerate(sources, start=1):
        metadata_values = (
            ("type", source.type),
            ("source_id", source.source_id),
            ("identity_hash", source.identity_hash),
            ("text", source.text),
            ("section_header", source.section_header),
            ("char_start", source.char_start),
            ("char_end", source.char_end),
            ("annotation_id", source.annotation_id),
            ("provider", source.provider),
            ("document_id", source.document_id),
            ("endpoint_or_filing_id", source.endpoint_or_filing_id),
            ("key_fields", source.key_fields),
            ("retrieved_at", source.retrieved_at),
            ("table_citation_record_id", source.table_citation_record_id),
            ("table_id", source.table_id),
            ("table_value_source", source.table_value_source),
            ("parsed_value_text", source.parsed_value_text),
            ("parsed_numeric_value", source.parsed_numeric_value),
            ("unit", source.unit),
            ("period", source.period),
            ("concept", source.concept),
            ("skill_name", source.skill_name),
            ("artifact_path", source.artifact_path),
            ("artifact_id", source.artifact_id),
            ("skill_run_id", source.skill_run_id),
            ("source_path", source.source_path),
            ("page_number", source.page_number),
            ("spine_hash", source.spine_hash),
            ("extraction_version", source.extraction_version),
        )
        metadata = [
            f"{key}={_serialize_source_metadata_value(value)}"
            for key, value in metadata_values
            if value is not None
        ]
        lines.append(f"{index}. [{source.id}] | " + " | ".join(metadata))
        for excerpt in sorted(source.excerpts, key=lambda item: (item.excerpt_id, item.text)):
            lines.append(
                "   - excerpt: "
                + json.dumps(_serialize_source_excerpt(excerpt), sort_keys=True)
            )
    return lines


def _serialize_source_metadata_value(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, str) and not any(
        marker in value for marker in ("|", "\n", "\r")
    ) and not value.startswith("@json:"):
        return value
    return "@json:" + json.dumps(value, ensure_ascii=False, sort_keys=True)


def _serialize_source_excerpt(excerpt: Excerpt) -> dict[str, Any]:
    payload = excerpt.model_dump(mode="json", exclude_none=True)
    if payload.get("created_by") == "agent":
        payload.pop("created_by", None)
    return payload


def _serialize_from_idea(from_idea: ThesisFromIdea | None) -> list[str]:
    if from_idea is None:
        return []
    lines = [
        line
        for line in (
            _render_bullet("Idea ID", from_idea.idea_id),
            _render_bullet("Seeded At", from_idea.seeded_at),
            _render_bullet("Schema Version", from_idea.schema_version),
        )
        if line
    ]
    if _has_meaningful_value(from_idea.thesis_hypothesis):
        if lines:
            lines.append("")
        lines.extend(_split_lines(from_idea.thesis_hypothesis))
    return lines


def _serialize_model_linkage(
    model_ref: ModelVersionRef | None,
    business_model_ref: BusinessModelRef | None,
    model_links: list[ThesisLink],
) -> list[str]:
    if model_ref is None and business_model_ref is None and not model_links:
        return []
    lines: list[str] = []
    if model_ref is not None:
        lines.append("### Model Reference")
        for label, value in (
            ("Model ID", model_ref.model_id),
            ("Version", model_ref.version),
        ):
            line = _render_bullet(label, value)
            if line:
                lines.append(line)
    if business_model_ref is not None:
        if lines:
            lines.append("")
        lines.append("### Business Model Reference")
        for label, value in (
            ("ID", business_model_ref.business_model_id),
            ("Schema Version", business_model_ref.schema_version),
            ("Revision", business_model_ref.revision),
            ("Last Updated", business_model_ref.last_updated),
        ):
            line = _render_bullet(label, value)
            if line:
                lines.append(line)
    if model_links:
        if lines:
            lines.append("")
        lines.append("### Links")
        lines.extend(
            _format_table(
                ["Link ID", "Point ID", "Category", "Thesis Direction", "Driver Key", "Required Role", "Model Item ID", "BM Node", "Template Version", "Model ID", "Periods", "Thesis Value", "Consensus Value", "Structural Fingerprint JSON", "Thesis Text"],
                [
                    [
                        link.thesis_link_id or "",
                        link.thesis_point_id,
                        link.category,
                        link.thesis_direction,
                        link.driver_key or "",
                        link.required_role or "",
                        link.model_item_id or "",
                        link.business_model_node_id or "",
                        link.template_version or "",
                        link.model_id or "",
                        ",".join(str(period) for period in link.periods),
                        link.thesis_value if link.thesis_value is not None else "",
                        link.consensus_value if link.consensus_value is not None else "",
                        _serialize_json_value(link.structural_fingerprint.model_dump(mode="json")) if link.structural_fingerprint else "",
                        link.thesis_text,
                    ]
                    for link in model_links
                ],
            )
        )
    return lines


def _serialize_decisions_log(entries: list[DecisionsLogEntry]) -> list[str]:
    if not entries:
        return []
    include_run_fields = any(
        entry.verdict or entry.run_id or entry.artifact_refs for entry in entries
    )
    if not include_run_fields:
        return _format_table(
            ["Entry ID", "Date", "Skill", "Decision", "Rationale", "Previous Value", "New Value", "Patch Ops JSON"],
            [
                [entry.entry_id or "", entry.date, entry.skill, entry.decision, entry.rationale, _serialize_json_value(entry.previous_value), _serialize_json_value(entry.new_value), _serialize_json_value(entry.patch_ops_applied)]
                for entry in entries
            ],
        )
    return _format_table(
        ["Entry ID", "Date", "Skill", "Verdict", "Decision", "Rationale", "Previous Value", "New Value", "Patch Ops JSON", "Run ID", "Artifact Refs JSON"],
        [
            [
                entry.entry_id or "",
                entry.date,
                entry.skill,
                entry.verdict or "",
                entry.decision,
                entry.rationale,
                _serialize_json_value(entry.previous_value),
                _serialize_json_value(entry.new_value),
                _serialize_json_value(entry.patch_ops_applied),
                entry.run_id or "",
                _serialize_json_value(
                    [ref.model_dump(mode="json") for ref in entry.artifact_refs]
                ),
            ]
            for entry in entries
        ],
    )


__all__ = [
    "_serialize_decisions_log",
    "_serialize_from_idea",
    "_serialize_model_linkage",
    "_serialize_source_excerpt",
    "_serialize_sources",
]
