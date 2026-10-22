"""Neutral scenario-table row lookup for financial-model readers."""

from __future__ import annotations

import logging
import re

from .build_model_items import _iter_items
from .models import FinancialModel, FormulaType, ItemType, LineItem, Section
from .refs import line_item_ref_from_obj


_SCENARIO_CASE_PATTERNS = {
    "bull": {"bull", "bullcase", "upside", "bullscenario"},
    "base": {"base", "basecase", "central", "basescenario"},
    "bear": {"bear", "bearcase", "downside", "bearscenario"},
}


def find_scenario_value_row(model: FinancialModel, anchor_id: str, case: str) -> str | None:
    """Find the Bull/Base/Bear value row immediately below a scenario-table anchor."""

    anchor_location = _find_item_location(model, anchor_id)
    if anchor_location is None:
        logging.warning("Scenario override anchor not found: %s", anchor_id)
        return None
    _sheet_name, section, anchor_index = anchor_location
    anchor_row = int(section.line_items[anchor_index].row)

    all_anchor_ids = _offset_anchor_ids(model)
    candidates: list[LineItem] = []
    for candidate in section.line_items[anchor_index + 1:]:
        if int(candidate.row) <= anchor_row:
            continue
        if candidate.id in all_anchor_ids:
            break
        if candidate.item_type in {ItemType.header, ItemType.spacer}:
            break
        candidates.append(candidate)

    patterns = _SCENARIO_CASE_PATTERNS[case]
    for candidate in candidates:
        if _normalize_scenario_label(candidate.label) in patterns:
            return candidate.id

    if len(candidates) != 3:
        logging.warning(
            "Scenario override anchor %s is ambiguous for %s: observed %d candidate rows",
            anchor_id,
            case,
            len(candidates),
        )
        return None

    sorted_candidates = sorted(candidates, key=lambda item_obj: int(item_obj.row))
    rows = [int(item_obj.row) for item_obj in sorted_candidates]
    expected_rows = list(range(rows[0], rows[0] + 3))
    if rows != expected_rows:
        logging.warning(
            "Scenario override anchor %s is ambiguous for %s: candidate rows are not contiguous (%s)",
            anchor_id,
            case,
            rows,
        )
        return None

    position = {"bull": 0, "base": 1, "bear": 2}[case]
    return sorted_candidates[position].id


def _find_item_location(model: FinancialModel, item_id: str) -> tuple[str, Section, int] | None:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item_obj in enumerate(section.line_items):
                if item_obj.id == item_id:
                    return sheet_name, section, index
    return None


def _offset_anchor_ids(model: FinancialModel) -> set[str]:
    anchors: set[str] = set()
    for item_obj in _iter_items(model):
        for spec in (item_obj.historical, item_obj.projected):
            if spec is None or spec.type != FormulaType.valuation or spec.subtype != "offset_scenario":
                continue
            anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
            if anchor_ref is not None:
                anchors.add(anchor_ref.id)
    return anchors


def _normalize_scenario_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(label or "").strip().lower())


__all__ = ["find_scenario_value_row"]
