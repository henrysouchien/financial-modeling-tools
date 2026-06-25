"""Fail-loud invariants for build-produced Valuation sheet schemas."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from .models import FinancialModel

logger = logging.getLogger(__name__)


class ValuationSchemaInvariantError(RuntimeError):
    """Raised when a build-produced model regresses to reader-derived valuation IDs."""

    def __init__(self, message: str, details: dict[str, Any]) -> None:
        super().__init__(message)
        self.details = details


def assert_valuation_template_schema(
    model: FinancialModel,
    *,
    origin: str,
    workbook_path: str | Path | None = None,
    sidecar_path: str | Path | None = None,
    sidecar_hash: str | None = None,
    module_path: str | Path | None = None,
) -> None:
    """Assert build-produced Valuation rows use template ``tpl.v.*`` IDs only."""

    sheet = _valuation_sheet(model)
    item_ids = _sheet_item_ids(sheet)
    template_ids = sorted(item_id for item_id in item_ids if item_id.startswith("tpl.v."))
    legacy_ids = sorted(item_id for item_id in item_ids if item_id.startswith("valuation."))
    if sheet is not None and template_ids and not legacy_ids:
        return

    resolved_workbook = _optional_path(workbook_path)
    resolved_sidecar = _optional_path(sidecar_path) or (
        _sidecar_path_for_workbook(resolved_workbook) if resolved_workbook is not None else None
    )
    details = {
        "origin": origin,
        "template_path": str(_template_path()),
        "module_path": str(Path(module_path).resolve(strict=False) if module_path else Path(__file__).resolve()),
        "invariant_module_path": str(Path(__file__).resolve()),
        "cwd": str(Path.cwd().resolve(strict=False)),
        "workbook_path": str(resolved_workbook) if resolved_workbook is not None else None,
        "sidecar_path": str(resolved_sidecar) if resolved_sidecar is not None else None,
        "sidecar_hash": sidecar_hash or _sha256_path(resolved_sidecar),
        "sidecar-hash": sidecar_hash or _sha256_path(resolved_sidecar),
        "valuation_sheet_present": sheet is not None,
        "tpl_v_count": len(template_ids),
        "legacy_valuation_id_count": len(legacy_ids),
        "legacy_valuation_ids": legacy_ids[:20],
        "sample_tpl_v_ids": template_ids[:20],
    }
    message = (
        "Valuation schema invariant failed: expected Valuation sheet rows to include "
        "tpl.v.* IDs and no reader-derived valuation.* IDs"
    )
    logger.error("%s: %s", message, json.dumps(details, sort_keys=True))
    raise ValuationSchemaInvariantError(message, details)


def should_enforce_valuation_template_schema(
    model: FinancialModel,
    *,
    workbook_path: str | Path | None = None,
) -> bool:
    """Return True for build/template-shaped models that own canonical Valuation IDs."""

    metadata = getattr(model, "metadata", None)
    if bool(getattr(metadata, "is_template", False)):
        return True
    if str(getattr(metadata, "methodology", "") or "").strip().lower() == "sia":
        return True
    if str(getattr(metadata, "source_model", "") or "").strip().lower() == "sia_generic":
        return True
    resolved_workbook = _optional_path(workbook_path)
    if resolved_workbook is not None and "model_workspaces" in resolved_workbook.parts:
        return True
    sheet = _valuation_sheet(model)
    item_ids = _sheet_item_ids(sheet)
    if any(item_id.startswith("tpl.v.") for item_id in item_ids):
        return True
    return False


def _valuation_sheet(model: FinancialModel) -> Any | None:
    sheets = getattr(model, "sheets", {}) or {}
    sheet = sheets.get("Valuation")
    if sheet is not None:
        return sheet
    for candidate in sheets.values():
        if str(getattr(candidate, "name", "") or "").strip().lower() == "valuation":
            return candidate
    return None


def _sheet_item_ids(sheet: Any | None) -> list[str]:
    if sheet is None:
        return []
    item_ids: list[str] = []
    for section in getattr(sheet, "sections", []) or []:
        for item in getattr(section, "line_items", []) or []:
            item_id = getattr(item, "id", None)
            if isinstance(item_id, str):
                item_ids.append(item_id)
    return item_ids


def _template_path() -> Path:
    try:
        from .templates.template_builder_config import SIA_GENERIC_TEMPLATE_PATH

        return Path(SIA_GENERIC_TEMPLATE_PATH).resolve(strict=False)
    except Exception:
        return Path(__file__).resolve().parent / "templates" / "sia_generic.json"


def _optional_path(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    return Path(path).expanduser().resolve(strict=False)


def _sidecar_path_for_workbook(workbook_path: Path | None) -> Path | None:
    if workbook_path is None:
        return None
    return workbook_path.with_name(f"{workbook_path.name}.schema.json")


def _sha256_path(path: Path | None) -> str | None:
    if path is None or not path.is_file():
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()
