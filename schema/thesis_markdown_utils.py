from __future__ import annotations

import json
import re
import sys
from typing import Any, Iterable


_PARENT_MODULE = "schema.thesis_markdown"
_SOURCE_TOKEN_RE = re.compile(r"\[(src_[1-9]\d*)\]")


def _compat(name: str, fallback: Any) -> Any:
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is not None and hasattr(parent, name):
        return getattr(parent, name)
    return fallback


def _slugify_label(label: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", str(label or "").strip().lower()).strip("_") or "label"


def _append_block(lines: list[str], block_lines: Iterable[str]) -> None:
    block = [line.rstrip() for line in block_lines]
    if lines:
        while lines and not lines[-1].strip():
            lines.pop()
        lines.append("")
    lines.extend(block)


def _has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _render_bullet(label: str, value: Any) -> str | None:
    if not _compat("_has_meaningful_value", _has_meaningful_value)(value):
        return None
    return f"- **{label}**: {value}"


def _render_labeled_line(label: str, value: Any) -> str | None:
    if not _compat("_has_meaningful_value", _has_meaningful_value)(value):
        return None
    return f"**{label}**: {value}"


def _split_lines(text: str | None) -> list[str]:
    if not text:
        return []
    return str(text).splitlines()


def _format_source_tokens(source_ids: list[str]) -> str:
    return " ".join(f"[{source_id}]" for source_id in source_ids)


def _format_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    escape_table_cell = _compat("_escape_table_cell", _escape_table_cell)
    rendered = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        rendered.append("| " + " | ".join(escape_table_cell(value) for value in row) + " |")
    return rendered


def _escape_table_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", "<br>")


def _unescape_table_cell(value: str) -> str:
    text = value.replace("<br>", "\n")
    output: list[str] = []
    escape = False
    for char in text:
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
    unescape_table_cell = _compat("_unescape_table_cell", _unescape_table_cell)
    return [unescape_table_cell(cell) for cell in cells]


def _parse_table(lines: list[str]) -> list[dict[str, str]]:
    stripped = [line for line in lines if line.strip()]
    if len(stripped) < 2 or not stripped[0].startswith("|") or not stripped[1].startswith("|"):
        return []
    split_table_row = _compat("_split_table_row", _split_table_row)
    headers = split_table_row(stripped[0])
    rows: list[dict[str, str]] = []
    for row in stripped[2:]:
        if not row.startswith("|"):
            continue
        cells = split_table_row(row)
        rows.append({headers[index]: cells[index] if index < len(cells) else "" for index in range(len(headers))})
    return rows


def _parse_key_value_segments(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    normalize_label_key = _compat("_normalize_label_key", _normalize_label_key)
    for part in [segment.strip() for segment in text.split("|") if segment.strip()]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        result[normalize_label_key(key)] = value.strip()
    return result


def _normalize_label_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(label or "").strip().lower()).strip("_")


def _serialize_json_value(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _parse_jsonish_value(value: str) -> Any:
    stripped = str(value or "").strip()
    if not stripped:
        return None
    if stripped[0] in "[{":
        return json.loads(stripped)
    lowered = stripped.lower()
    if lowered in {"true", "false", "null"}:
        return json.loads(lowered)
    if re.fullmatch(r"-?\d+(?:\.\d+)?", stripped):
        return json.loads(stripped)
    return stripped


def _extract_source_tokens(text: str) -> tuple[str, list[str]]:
    source_token_re = _compat("_SOURCE_TOKEN_RE", _SOURCE_TOKEN_RE)
    source_ids = source_token_re.findall(text or "")
    cleaned = source_token_re.sub("", text or "")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = re.sub(r"\s+([,.;:])", r"\1", cleaned)
    return cleaned.strip(), source_ids


def _parse_source_token_list(text: str, state: Any) -> list[str]:
    _, source_ids = _compat("_extract_source_tokens", _extract_source_tokens)(text)
    for source_id in source_ids:
        state.referenced_source_ids.add(source_id)
    return source_ids


def _format_source_refs_cell(source_ids: list[str]) -> str:
    return ", ".join(source_ids)


def _parse_source_refs_cell(text: str | None, state: Any) -> list[str]:
    source_ids = re.findall(r"src_[1-9]\d*", text or "")
    for source_id in source_ids:
        state.referenced_source_ids.add(source_id)
    return source_ids


def _nullable_cell(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text == "—":
        return None
    return text


def _parse_boolish(value: str | None) -> bool | str:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"", "0", "false", "no", "n"}:
        return False
    return value or ""


__all__ = [
    "_SOURCE_TOKEN_RE",
    "_append_block",
    "_escape_table_cell",
    "_extract_source_tokens",
    "_format_source_refs_cell",
    "_format_source_tokens",
    "_format_table",
    "_has_meaningful_value",
    "_normalize_label_key",
    "_nullable_cell",
    "_parse_boolish",
    "_parse_jsonish_value",
    "_parse_key_value_segments",
    "_parse_source_refs_cell",
    "_parse_source_token_list",
    "_parse_table",
    "_render_bullet",
    "_render_labeled_line",
    "_serialize_json_value",
    "_slugify_label",
    "_split_lines",
    "_split_table_row",
    "_unescape_table_cell",
]
