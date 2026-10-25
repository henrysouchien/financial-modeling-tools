"""Reviewed required-role bindings onto Edgar presentation lines."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Required, TypedDict

from schema.provider_port.contracts import FilingLineRef


class _EdgarLineBindingRow(TypedDict, total=False):
    tag: Required[str]
    statement_role: Required[str]
    required_role: Required[str]
    notes: str | None


@dataclass(frozen=True)
class EdgarLineBinding:
    """One reviewed build-model role mapped to a presented tag-in-role."""

    tag: str
    statement_role: str
    required_role: str
    notes: str | None = None

    def __post_init__(self) -> None:
        if not self.tag.strip() or ":" not in self.tag:
            raise ValueError("Edgar line binding tag must be a qualified name")
        if not self.statement_role.strip():
            raise ValueError("Edgar statement_role must be non-empty")
        required_role = self.required_role.strip()
        if required_role != self.required_role or not required_role:
            raise ValueError("Edgar required_role must be a non-empty string")
        plane = {
            "income_statement": "is",
            "balance_sheet": "bs",
            "cash_flow": "cf",
        }.get(self.statement_role)
        if plane is None or not required_role.startswith(f"statement.{plane}."):
            raise ValueError(
                "Edgar required_role must agree with its canonical statement role"
            )
        if self.notes is not None and (
            self.notes != self.notes.strip() or not self.notes
        ):
            raise ValueError("Edgar binding notes must be non-empty when present")


class EdgarLineBindingAmbiguous(ValueError):
    """A presented tag belongs to more than one canonical statement plane."""


_CONFIG_PATH = Path(__file__).with_name("line_bindings.json")
_ROW_FIELDS = frozenset({"tag", "statement_role", "required_role", "notes"})
_REQUIRED_ROW_FIELDS = frozenset({"tag", "statement_role", "required_role"})



def _row_to_binding(row: _EdgarLineBindingRow) -> EdgarLineBinding:
    notes = row.get("notes")
    return EdgarLineBinding(
        tag=str(row["tag"]),
        statement_role=str(row["statement_role"]),
        required_role=str(row["required_role"]),
        notes=str(notes) if notes is not None else None,
    )


@lru_cache(maxsize=1)
def load_edgar_line_bindings() -> tuple[EdgarLineBinding, ...]:
    payload = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Edgar line bindings must be a JSON array")
    for index, row in enumerate(payload):
        if not isinstance(row, dict):
            raise ValueError(f"Edgar line binding row {index} must be an object")
        unknown = set(row) - _ROW_FIELDS
        if unknown:
            raise ValueError(
                f"Edgar line binding row {index} has unknown fields: "
                f"{sorted(unknown)!r}"
            )
        missing = _REQUIRED_ROW_FIELDS - set(row)
        if missing:
            raise ValueError(
                f"Edgar line binding row {index} lacks required fields: "
                f"{sorted(missing)!r}"
            )
    bindings = tuple(_row_to_binding(row) for row in payload)
    keys = [(binding.tag, binding.statement_role, binding.required_role) for binding in bindings]
    if len(set(keys)) != len(keys):
        raise ValueError("Edgar line bindings must be unique by (tag, statement_role, required_role)")
    return bindings


def edgar_binding_by_tag() -> dict[str, tuple[EdgarLineBinding, ...]]:
    by_tag: dict[str, list[EdgarLineBinding]] = {}
    for binding in load_edgar_line_bindings():
        by_tag.setdefault(binding.tag, []).append(binding)
    return {tag: tuple(bindings) for tag, bindings in by_tag.items()}


def mapping_by_required_role() -> dict[str, EdgarLineBinding]:
    """Return the reviewed Edgar presentation binding for each model role."""

    out: dict[str, EdgarLineBinding] = {}
    for binding in load_edgar_line_bindings():
        out.setdefault(binding.required_role, binding)
    return out


def binding_for_filing_line(line: FilingLineRef) -> EdgarLineBinding | None:
    candidates = edgar_binding_by_tag().get(line.tag, ())
    planes = {binding.statement_role for binding in candidates}
    if len(planes) > 1:
        raise EdgarLineBindingAmbiguous(
            f"Edgar tag {line.tag!r} belongs to multiple statement planes"
        )
    return candidates[0] if candidates else None


__all__ = [
    "EdgarLineBinding",
    "EdgarLineBindingAmbiguous",
    "binding_for_filing_line",
    "edgar_binding_by_tag",
    "load_edgar_line_bindings",
    "mapping_by_required_role",
]
