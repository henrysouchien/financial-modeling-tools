from __future__ import annotations

import json
import sys
from typing import Any

from .thesis import (
    BusinessModelRef,
    DecisionsLogEntry,
    ThesisFromIdea,
    ThesisLink,
    ThesisModelRef,
)
from .thesis_shared_slice import Excerpt, SourceRecord
from .thesis_markdown_utils import (
    _format_table,
    _has_meaningful_value,
    _render_bullet,
    _serialize_json_value,
    _split_lines,
)


_PARENT_MODULE = "schema.thesis_markdown"


def _compat(name: str, fallback: Any) -> Any:
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is not None and hasattr(parent, name):
        return getattr(parent, name)
    return fallback


def _serialize_sources(sources: list[SourceRecord]) -> list[str]:
    lines: list[str] = []
    serialize_source_excerpt = _compat(
        "_serialize_source_excerpt",
        _serialize_source_excerpt,
    )
    for index, source in enumerate(sources, start=1):
        metadata = [f"type={source.type}", f"source_id={source.source_id}"]
        if source.identity_hash:
            metadata.append(f"identity_hash={source.identity_hash}")
        metadata.append(f"text={source.text}")
        if source.section_header:
            metadata.append(f"section_header={source.section_header}")
        if source.char_start is not None:
            metadata.append(f"char_start={source.char_start}")
        if source.char_end is not None:
            metadata.append(f"char_end={source.char_end}")
        if source.annotation_id:
            metadata.append(f"annotation_id={source.annotation_id}")
        if source.provider:
            metadata.append(f"provider={source.provider}")
        if source.endpoint_or_filing_id:
            metadata.append(f"endpoint_or_filing_id={source.endpoint_or_filing_id}")
        if source.retrieved_at:
            metadata.append(f"retrieved_at={source.retrieved_at}")
        if source.skill_name:
            metadata.append(f"skill_name={source.skill_name}")
        if source.artifact_path:
            metadata.append(f"artifact_path={source.artifact_path}")
        if source.artifact_id:
            metadata.append(f"artifact_id={source.artifact_id}")
        if source.skill_run_id:
            metadata.append(f"skill_run_id={source.skill_run_id}")
        if source.source_path:
            metadata.append(f"source_path={source.source_path}")
        lines.append(f"{index}. [{source.id}] | " + " | ".join(metadata))
        for excerpt in sorted(source.excerpts, key=lambda item: (item.excerpt_id, item.text)):
            lines.append(
                "   - excerpt: "
                + json.dumps(serialize_source_excerpt(excerpt), sort_keys=True)
            )
    return lines


def _serialize_source_excerpt(excerpt: Excerpt) -> dict[str, Any]:
    payload = excerpt.model_dump(mode="json", exclude_none=True)
    if payload.get("created_by") == "agent":
        payload.pop("created_by", None)
    return payload


def _serialize_from_idea(from_idea: ThesisFromIdea | None) -> list[str]:
    if from_idea is None:
        return []
    render_bullet = _compat("_render_bullet", _render_bullet)
    has_meaningful_value = _compat("_has_meaningful_value", _has_meaningful_value)
    split_lines = _compat("_split_lines", _split_lines)
    lines = [
        line
        for line in (
            render_bullet("Idea ID", from_idea.idea_id),
            render_bullet("Seeded At", from_idea.seeded_at),
            render_bullet("Schema Version", from_idea.schema_version),
        )
        if line
    ]
    if has_meaningful_value(from_idea.thesis_hypothesis):
        if lines:
            lines.append("")
        lines.extend(split_lines(from_idea.thesis_hypothesis))
    return lines


def _serialize_model_linkage(
    model_ref: ThesisModelRef | None,
    business_model_ref: BusinessModelRef | None,
    model_links: list[ThesisLink],
) -> list[str]:
    if model_ref is None and business_model_ref is None and not model_links:
        return []
    render_bullet = _compat("_render_bullet", _render_bullet)
    format_table = _compat("_format_table", _format_table)
    serialize_json_value = _compat("_serialize_json_value", _serialize_json_value)
    lines: list[str] = []
    if model_ref is not None:
        lines.append("### Model Reference")
        for label, value in (
            ("Model ID", model_ref.model_id),
            ("Version", model_ref.version),
            ("File Path", model_ref.file_path),
            ("Last Updated", model_ref.last_updated),
            ("Drivers Locked", ", ".join(model_ref.drivers_locked) if model_ref.drivers_locked else None),
        ):
            line = render_bullet(label, value)
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
            line = render_bullet(label, value)
            if line:
                lines.append(line)
    if model_links:
        if lines:
            lines.append("")
        lines.append("### Links")
        lines.extend(
            format_table(
                ["Link ID", "Point ID", "Category", "Thesis Direction", "Driver Key", "Data Concept ID", "Model Item ID", "BM Node", "Template Version", "Model ID", "Periods", "Thesis Value", "Consensus Value", "Structural Fingerprint JSON", "Thesis Text"],
                [
                    [
                        link.thesis_link_id or "",
                        link.thesis_point_id,
                        link.category,
                        link.thesis_direction,
                        link.driver_key or "",
                        link.data_concept_id or "",
                        link.model_item_id or "",
                        link.business_model_node_id or "",
                        link.template_version or "",
                        link.model_id or "",
                        ",".join(str(period) for period in link.periods),
                        link.thesis_value if link.thesis_value is not None else "",
                        link.consensus_value if link.consensus_value is not None else "",
                        serialize_json_value(link.structural_fingerprint.model_dump(mode="json")) if link.structural_fingerprint else "",
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
    format_table = _compat("_format_table", _format_table)
    serialize_json_value = _compat("_serialize_json_value", _serialize_json_value)
    include_run_fields = any(
        entry.verdict or entry.run_id or entry.artifact_refs for entry in entries
    )
    if not include_run_fields:
        return format_table(
            ["Entry ID", "Date", "Skill", "Decision", "Rationale", "Previous Value", "New Value", "Patch Ops JSON"],
            [
                [entry.entry_id or "", entry.date, entry.skill, entry.decision, entry.rationale, serialize_json_value(entry.previous_value), serialize_json_value(entry.new_value), serialize_json_value(entry.patch_ops_applied)]
                for entry in entries
            ],
        )
    return format_table(
        ["Entry ID", "Date", "Skill", "Verdict", "Decision", "Rationale", "Previous Value", "New Value", "Patch Ops JSON", "Run ID", "Artifact Refs JSON"],
        [
            [
                entry.entry_id or "",
                entry.date,
                entry.skill,
                entry.verdict or "",
                entry.decision,
                entry.rationale,
                serialize_json_value(entry.previous_value),
                serialize_json_value(entry.new_value),
                serialize_json_value(entry.patch_ops_applied),
                entry.run_id or "",
                serialize_json_value(
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
