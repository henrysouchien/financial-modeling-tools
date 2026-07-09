from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable

import yaml

from .business_model import BusinessModel, normalize_legacy_profitability_targets


PROSE_SEPARATOR = " \u2014 "
_SECTION_HEADER_RE = re.compile(r"^## (?P<title>.+?)\s*$")
_FRONTMATTER_DELIMITER = "---"
_YAML_FENCE_RE = re.compile(r"^```(?:yaml)?\s*$", re.IGNORECASE)
_SECTION_TITLES = {
    "segments": "Segments",
    "consolidated": "Consolidated",
    "profitability_targets": "Profitability Targets",
    "change_triggers": "Change Triggers",
    "source_materials": "Source Materials",
    "decisions_log": "Decisions Log",
}
_TITLE_TO_SECTION_KEY = {value.lower(): key for key, value in _SECTION_TITLES.items()}
_FRONTMATTER_COMPANY_FIELDS = ("ticker", "name", "sector", "industry", "business_type")
_FRONTMATTER_METADATA_FIELDS = (
    "revision",
    "created_by",
    "created_at",
    "last_updated",
    "last_reviewed_at",
    "review_basis_filing",
)


class _NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, data: Any) -> bool:  # pragma: no cover - deterministic override
        return True


@dataclass(frozen=True)
class _MarkdownSection:
    title: str
    body_lines: list[str]


def render(model: BusinessModel) -> str:
    """Render a BusinessModel contract to markdown."""

    dumped = model.model_dump(mode="json", exclude_none=True)
    normalized = _strip_default_noderef_fields(dumped)
    lines = _render_frontmatter(normalized)

    company = normalized["company"]
    _append_block(lines, [f"# Business Model: {company['ticker']}{PROSE_SEPARATOR}{company['name']}"])
    _append_block(lines, _render_yaml_section(_SECTION_TITLES["segments"], normalized["segments"]))

    if "consolidated" in normalized:
        _append_block(lines, _render_yaml_section(_SECTION_TITLES["consolidated"], normalized["consolidated"]))
    if "profitability_targets" in normalized:
        _append_block(
            lines,
            _render_yaml_section(_SECTION_TITLES["profitability_targets"], normalized["profitability_targets"]),
        )

    metadata = normalized.get("metadata", {})
    _append_block(lines, _render_bullet_section(_SECTION_TITLES["change_triggers"], metadata.get("change_triggers", [])))
    _append_block(lines, _render_bullet_section(_SECTION_TITLES["source_materials"], metadata.get("source_materials", [])))
    _append_block(lines, _render_decisions_log_section(normalized.get("decisions_log", [])))
    return "\n".join(lines).rstrip() + "\n"


def parse(text: str) -> BusinessModel:
    """Parse markdown into a validated BusinessModel contract."""

    frontmatter, body_lines = _extract_frontmatter(text)
    payload = _parse_frontmatter(frontmatter)

    for section in _split_sections(body_lines):
        section_key = _TITLE_TO_SECTION_KEY.get(section.title.lower())
        if section_key is None:
            continue
        if section_key == "segments":
            payload["segments"] = _parse_yaml_section(section.body_lines, default=[])
            continue
        if section_key == "consolidated":
            payload["consolidated"] = _parse_yaml_section(section.body_lines, default={})
            continue
        if section_key == "profitability_targets":
            payload["profitability_targets"] = _parse_yaml_section(section.body_lines, default={})
            continue
        if section_key == "change_triggers":
            payload.setdefault("metadata", {})["change_triggers"] = _parse_bullet_section(section.body_lines)
            continue
        if section_key == "source_materials":
            payload.setdefault("metadata", {})["source_materials"] = _parse_bullet_section(section.body_lines)
            continue
        if section_key == "decisions_log":
            payload["decisions_log"] = _parse_decisions_log_section(section.body_lines)

    return BusinessModel.model_validate(normalize_legacy_profitability_targets(payload))


def _render_frontmatter(payload: dict[str, Any]) -> list[str]:
    frontmatter: dict[str, Any] = {"schema_version": payload["schema_version"]}

    metadata = payload.get("metadata", {})
    if "revision" in metadata:
        frontmatter["revision"] = metadata["revision"]

    company = payload["company"]
    for field in _FRONTMATTER_COMPANY_FIELDS:
        if field in company:
            frontmatter[field] = company[field]

    if "recommended_depth" in payload:
        frontmatter["recommended_depth"] = payload["recommended_depth"]

    for field in _FRONTMATTER_METADATA_FIELDS:
        if field == "revision":
            continue
        if field in metadata:
            frontmatter[field] = metadata[field]

    yaml_text = _dump_yaml(frontmatter)
    return [_FRONTMATTER_DELIMITER, *yaml_text.splitlines(), _FRONTMATTER_DELIMITER]


def _parse_frontmatter(frontmatter: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}

    if "schema_version" in frontmatter:
        payload["schema_version"] = frontmatter["schema_version"]

    company = {
        field: frontmatter[field]
        for field in _FRONTMATTER_COMPANY_FIELDS
        if field in frontmatter
    }
    if company:
        payload["company"] = company

    if "recommended_depth" in frontmatter:
        payload["recommended_depth"] = frontmatter["recommended_depth"]

    metadata = {
        field: frontmatter[field]
        for field in _FRONTMATTER_METADATA_FIELDS
        if field in frontmatter
    }
    if metadata:
        payload["metadata"] = metadata

    return payload


def _render_yaml_section(title: str, data: Any) -> list[str]:
    yaml_text = _dump_yaml(data)
    return [f"## {title}", "", "```yaml", *yaml_text.splitlines(), "```"]


def _render_bullet_section(title: str, values: list[str]) -> list[str]:
    lines = [f"## {title}"]
    if values:
        lines.append("")
        lines.extend(f"- {_normalize_prose(value)}" for value in values)
    return lines


def _render_decisions_log_section(entries: list[dict[str, Any]]) -> list[str]:
    rows = []
    for entry in entries:
        rows.append(
            [
                entry["date"],
                _normalize_prose(entry["change"]),
                _normalize_prose(entry["rationale"]) if entry.get("rationale") is not None else "",
            ]
        )
    return [
        f"## {_SECTION_TITLES['decisions_log']}",
        "",
        *_format_table(["Date", "Change", "Rationale"], rows),
    ]


def _parse_yaml_section(body_lines: list[str], *, default: list[Any] | dict[str, Any]) -> Any:
    content = _extract_yaml_code_block(body_lines)
    if not content.strip():
        return [] if isinstance(default, list) else {}
    loaded = yaml.safe_load(content)
    if loaded is None:
        return [] if isinstance(default, list) else {}
    return loaded


def _parse_bullet_section(body_lines: list[str]) -> list[str]:
    items: list[str] = []
    for line in body_lines:
        stripped = line.strip()
        if stripped.startswith("- "):
            items.append(stripped[2:].strip())
    return items


def _parse_decisions_log_section(body_lines: list[str]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for row in _parse_table(body_lines):
        entries.append(
            {
                "date": row.get("Date", ""),
                "change": row.get("Change", ""),
                "rationale": row.get("Rationale") or None,
            }
        )
    return entries


def _extract_frontmatter(text: str) -> tuple[dict[str, Any], list[str]]:
    lines = text.lstrip("\ufeff").splitlines()
    if not lines or lines[0].strip() != _FRONTMATTER_DELIMITER:
        return {}, lines

    end_index = None
    for index in range(1, len(lines)):
        if lines[index].strip() == _FRONTMATTER_DELIMITER:
            end_index = index
            break
    if end_index is None:
        return {}, lines

    frontmatter_text = "\n".join(lines[1:end_index]).strip()
    frontmatter = yaml.safe_load(frontmatter_text) if frontmatter_text else {}
    return frontmatter or {}, lines[end_index + 1 :]


def _extract_yaml_code_block(body_lines: list[str]) -> str:
    in_fence = False
    collected: list[str] = []
    found_fence = False

    for line in body_lines:
        stripped = line.strip()
        if _YAML_FENCE_RE.match(stripped):
            if not in_fence:
                in_fence = True
                found_fence = True
                continue
            break
        if in_fence:
            collected.append(line)

    if found_fence:
        return "\n".join(collected).strip("\n")
    return "\n".join(body_lines).strip()


def _split_sections(lines: list[str]) -> list[_MarkdownSection]:
    sections: list[_MarkdownSection] = []
    current_title: str | None = None
    current_lines: list[str] = []
    in_fence = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            if current_title is not None:
                current_lines.append(line)
            in_fence = not in_fence
            continue

        if not in_fence:
            match = _SECTION_HEADER_RE.match(line)
            if match is not None:
                if current_title is not None:
                    sections.append(_MarkdownSection(current_title, _trim_blank_edges(current_lines)))
                current_title = match.group("title").strip()
                current_lines = []
                continue

        if current_title is not None:
            current_lines.append(line)

    if current_title is not None:
        sections.append(_MarkdownSection(current_title, _trim_blank_edges(current_lines)))
    return sections


def _dump_yaml(data: Any) -> str:
    return yaml.dump(
        data,
        Dumper=_NoAliasSafeDumper,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
        width=4096,
    ).rstrip()


def _strip_default_noderef_fields(value: Any) -> Any:
    if isinstance(value, list):
        return [_strip_default_noderef_fields(item) for item in value]
    if isinstance(value, dict):
        normalized = {key: _strip_default_noderef_fields(item) for key, item in value.items()}
        if "node_id" in normalized:
            if normalized.get("t") == 0:
                normalized.pop("t", None)
            if normalized.get("sign") == 1:
                normalized.pop("sign", None)
        return normalized
    return value


def _normalize_prose(value: str | None) -> str:
    if value is None:
        return ""
    pieces = [piece.strip() for piece in str(value).replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return PROSE_SEPARATOR.join(piece for piece in pieces if piece)


def _append_block(lines: list[str], block_lines: Iterable[str]) -> None:
    block = [line.rstrip() for line in block_lines]
    if lines:
        while lines and not lines[-1].strip():
            lines.pop()
        lines.append("")
    lines.extend(block)


def _trim_blank_edges(lines: list[str]) -> list[str]:
    trimmed = list(lines)
    while trimmed and not trimmed[0].strip():
        trimmed.pop(0)
    while trimmed and not trimmed[-1].strip():
        trimmed.pop()
    return trimmed


def _format_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    rendered = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        rendered.append("| " + " | ".join(_escape_table_cell(value) for value in row) + " |")
    return rendered


def _escape_table_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("|", "\\|")


def _unescape_table_cell(value: str) -> str:
    output: list[str] = []
    escape = False
    for char in value:
        if escape:
            output.append(char)
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        output.append(char)
    if escape:
        output.append("\\")
    return "".join(output).strip()


def _split_table_row(row: str) -> list[str]:
    cells: list[str] = []
    buffer: list[str] = []
    escape = False
    for char in row.strip().strip("|"):
        if escape:
            buffer.append(char)
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == "|":
            cells.append("".join(buffer).strip())
            buffer = []
            continue
        buffer.append(char)
    cells.append("".join(buffer).strip())
    return [_unescape_table_cell(cell) for cell in cells]


def _parse_table(lines: list[str]) -> list[dict[str, str]]:
    stripped = [line.strip() for line in lines if line.strip()]
    if len(stripped) < 2 or not stripped[0].startswith("|") or not stripped[1].startswith("|"):
        return []
    headers = _split_table_row(stripped[0])
    rows: list[dict[str, str]] = []
    for row in stripped[2:]:
        if not row.startswith("|"):
            continue
        cells = _split_table_row(row)
        rows.append({headers[index]: cells[index] if index < len(cells) else "" for index in range(len(headers))})
    return rows


__all__ = ["parse", "render"]
