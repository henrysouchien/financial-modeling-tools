"""Workbook inventory helpers for schema model tools."""

from __future__ import annotations

from pathlib import Path
import re
import sys
from typing import Any

from .workbook_presentation import workbook_presentation_fingerprint


_PARENT_MODULE = "schema.tools"
_PARSED_WORKBOOK_MODEL_SOURCES = {"disk_cache", "parsed_workbook"}


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


def _workbook_inventory(
    file_path: str,
    sheets_summary: list[dict[str, Any]],
    *,
    model_source: str,
) -> dict[str, Any]:
    """Return actual workbook sheet inventory when the workbook file is readable."""

    readback_sheet_names = [str(sheet.get("name")) for sheet in sheets_summary]
    non_model_sheet_names = _compat("_non_model_workbook_sheet_names", _non_model_workbook_sheet_names)(
        readback_sheet_names,
        model_source,
    )
    non_model_sheet_set = set(non_model_sheet_names)
    model_sheet_names = [
        name for name in readback_sheet_names if name not in non_model_sheet_set
    ]
    result: dict[str, Any] = {
        "model_sheet_names": model_sheet_names,
        "model_source": model_source,
        "workbook_inventory_status": "unavailable",
        "workbook_visible_sheet_names": None,
        "workbook_extra_visible_sheet_names": None,
        "workbook_missing_visible_model_sheet_names": None,
        "workbook_non_model_sheet_names": non_model_sheet_names,
        "workbook_presentation": None,
    }
    if not Path(file_path).exists():
        result["workbook_inventory_status"] = "file_missing"
        return result

    try:
        fingerprint = _compat("workbook_presentation_fingerprint", workbook_presentation_fingerprint)(
            file_path,
            max_cells_per_sheet=1,
        )
    except Exception as exc:
        result["workbook_inventory_status"] = "error"
        result["workbook_inventory_error"] = str(exc)
        return result

    visible_sheet_names = [
        str(name)
        for name in fingerprint.get("visible_sheet_names", [])
        if name is not None
    ]
    visible_set = set(visible_sheet_names)
    model_set = set(model_sheet_names)
    result.update(
        {
            "workbook_inventory_status": "available",
            "workbook_visible_sheet_names": visible_sheet_names,
            "workbook_extra_visible_sheet_names": [
                name for name in visible_sheet_names if name not in model_set
            ],
            "workbook_missing_visible_model_sheet_names": [
                name for name in model_sheet_names if name not in visible_set
            ],
            "workbook_presentation": {
                "visible_sheet_count": fingerprint.get("visible_sheet_count"),
                "visible_sheet_names": visible_sheet_names,
            },
        }
    )
    return result


def _non_model_workbook_sheet_names(sheet_names: list[str], model_source: str) -> list[str]:
    parsed_sources = _compat("_PARSED_WORKBOOK_MODEL_SOURCES", _PARSED_WORKBOOK_MODEL_SOURCES)
    if model_source not in parsed_sources:
        return []
    is_known_non_model_workbook_sheet = _compat(
        "_is_known_non_model_workbook_sheet",
        _is_known_non_model_workbook_sheet,
    )
    return [name for name in sheet_names if is_known_non_model_workbook_sheet(name)]


def _is_known_non_model_workbook_sheet(name: str) -> bool:
    normalized = name.strip().casefold()
    return normalized == "_researchcontext" or re.fullmatch(r"summary(?:_\d+)?", normalized) is not None


_ORIGINALS = {
    "_PARSED_WORKBOOK_MODEL_SOURCES": _PARSED_WORKBOOK_MODEL_SOURCES,
    "_is_known_non_model_workbook_sheet": _is_known_non_model_workbook_sheet,
    "_non_model_workbook_sheet_names": _non_model_workbook_sheet_names,
    "_workbook_inventory": _workbook_inventory,
    "workbook_presentation_fingerprint": workbook_presentation_fingerprint,
}


__all__ = [
    "_PARSED_WORKBOOK_MODEL_SOURCES",
    "_is_known_non_model_workbook_sheet",
    "_non_model_workbook_sheet_names",
    "_workbook_inventory",
    "workbook_presentation_fingerprint",
]
