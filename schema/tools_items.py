"""Item lookup and display helpers for schema model tools."""

from __future__ import annotations

from difflib import get_close_matches
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .models import FinancialModel, LineItem, ItemType


_PARENT_MODULE = "schema.tools"


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


def _model_tool_error(
    code: str,
    message: str,
    *,
    details: Dict[str, Any] | None = None,
    recovery: Dict[str, Any] | None = None,
) -> Exception:
    error_type = _compat("ModelToolError", None)
    if error_type is None:
        from .tools import ModelToolError as error_type
    return error_type(code, message, details=details, recovery=recovery)


def _formula_type(item: LineItem) -> Optional[str]:
    spec = item.projected or item.historical
    return spec.type.value if spec else None


def _suggest_items(
    index: Dict[str, LineItem],
    bad_id: str,
    limit: int = 5,
) -> List[str]:
    """Return up to `limit` item IDs that are similar to `bad_id`."""
    needle_full = bad_id.strip().lower()
    needle_leaf = needle_full.split(".")[-1]
    if not needle_leaf:
        return []

    scored: List[Tuple[int, str]] = []
    for item_id, item in index.items():
        id_lower = item_id.lower()
        leaf = id_lower.split(".")[-1]
        label_norm = item.label.strip().lower().replace(" ", "_")

        # Prefer natural leaf/label matches over broad ID substring matches.
        if leaf == needle_leaf:
            scored.append((0, item_id))
        elif needle_leaf in leaf:
            scored.append((1, item_id))
        elif needle_full in id_lower:
            scored.append((2, item_id))
        elif needle_leaf in label_norm:
            scored.append((3, item_id))

    scored.sort(key=lambda row: (row[0], row[1]))
    if scored:
        return [item_id for _, item_id in scored[: max(limit, 0)]]

    if limit <= 0:
        return []

    leaf_to_ids: Dict[str, List[str]] = {}
    for item_id in index:
        leaf_to_ids.setdefault(item_id.lower().split(".")[-1], []).append(item_id)
    for leaf_ids in leaf_to_ids.values():
        leaf_ids.sort()

    suggestions: List[str] = []
    close_leafs = get_close_matches(needle_leaf, list(leaf_to_ids), n=limit, cutoff=0.6)
    for leaf in close_leafs:
        for item_id in leaf_to_ids.get(leaf, []):
            suggestions.append(item_id)
            if len(suggestions) >= limit:
                return suggestions
    return suggestions


def _describe_item_suggestions(model: FinancialModel, item_ids: List[str]) -> List[Dict[str, Any]]:
    item_locations = _compat("_item_locations", _item_locations)
    formula_type = _compat("_formula_type", _formula_type)
    locations = item_locations(model)
    suggestions: List[Dict[str, Any]] = []
    for item_id in item_ids:
        try:
            item = model.get_item(item_id)
        except KeyError:
            continue
        context = locations.get(item_id)
        suggestions.append(
            {
                "id": item.id,
                "label": item.label,
                "sheet": context[0] if context else None,
                "section": context[1] if context else None,
                "row": context[2] if context else None,
                "item_type": item.item_type.value,
                "formula_type": formula_type(item),
            }
        )
    return suggestions


def _unknown_item_error(
    model: FinancialModel,
    bad_id: str,
    id_role: str = "item_id",
) -> Exception:
    suggest_items = _compat("_suggest_items", _suggest_items)
    describe_item_suggestions = _compat("_describe_item_suggestions", _describe_item_suggestions)
    suggestions = suggest_items(model._index, bad_id)
    message = f"Unknown {id_role}: {bad_id}"
    if suggestions:
        message += f". Did you mean: {', '.join(suggestions)}?"
    return _model_tool_error(
        "unknown_item_id",
        message,
        details={
            "id_role": id_role,
            "bad_id": bad_id,
            "suggestions": describe_item_suggestions(model, suggestions),
        },
        recovery={
            "next_actions": [
                "Call model_find(file_path=..., query='<visible label or id leaf>') to resolve current item ids for this workbook.",
                "If this id came from an older build, treat it as stale and rerun model_find/model_summarize(include_items=True) against the current file_path.",
                "Retry the model tool with one of the suggested ids only after confirming the label/context matches the intended line.",
            ]
        },
    )


def _format_unknown_id_error(
    index: Dict[str, LineItem],
    bad_id: str,
    id_role: str = "item_id",
) -> str:
    suggestions = _compat("_suggest_items", _suggest_items)(index, bad_id)
    msg = f"Unknown {id_role}: {bad_id}"
    if suggestions:
        msg += f". Did you mean: {', '.join(suggestions)}?"
    return msg


def _sample_values(values: Dict[int, float], periods: List[int]) -> Dict[int, Optional[float]]:
    if not periods:
        return {}
    idxs = sorted({0, len(periods) // 2, len(periods) - 1})
    sample_periods = [periods[i] for i in idxs]
    return {period: values.get(period) for period in sample_periods}


def _item_locations(model: FinancialModel) -> Dict[str, Tuple[str, Optional[str], int]]:
    locations: Dict[str, Tuple[str, Optional[str], int]] = {}
    for sheet in model.sheets.values():
        for section in sheet.sections:
            section_label = section.label or section.id
            for item in section.line_items:
                locations[item.id] = (sheet.name, section_label, item.row)
    return locations


def _parent_headers(model: FinancialModel) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {}
    for sheet in model.sheets.values():
        for section in sheet.sections:
            current_header: Optional[str] = None
            for item in section.line_items:
                if item.item_type == ItemType.header:
                    current_header = item.label.strip()
                result[item.id] = current_header
    return result


def _format_find_context(
    context: Optional[Tuple[str, Optional[str], int]],
    parent_header: Optional[str],
) -> Optional[str]:
    if context is None:
        return None
    sheet_name, _section_label, row = context
    if parent_header:
        return f"{sheet_name} / {parent_header} / row {row}"
    return f"{sheet_name} / row {row}"


def _format_context_label(context: Optional[Tuple[str, Optional[str], int]]) -> Optional[str]:
    if context is None:
        return None
    sheet_name, section_label, row = context
    if section_label:
        return f"{sheet_name} / {section_label} / row {row}"
    return f"{sheet_name} / row {row}"


def _ambiguous_labels(items: Iterable[LineItem]) -> Set[str]:
    counts: Dict[str, int] = {}
    label_key = _compat("_label_key", _label_key)
    for item in items:
        key = label_key(item.label)
        counts[key] = counts.get(key, 0) + 1
    return {key for key, count in counts.items() if count > 1}


def _label_key(label: str) -> str:
    return " ".join(label.strip().lower().split())


_ORIGINALS = {
    "ModelToolError": None,
    "_ambiguous_labels": _ambiguous_labels,
    "_describe_item_suggestions": _describe_item_suggestions,
    "_format_context_label": _format_context_label,
    "_format_find_context": _format_find_context,
    "_format_unknown_id_error": _format_unknown_id_error,
    "_formula_type": _formula_type,
    "_item_locations": _item_locations,
    "_label_key": _label_key,
    "_parent_headers": _parent_headers,
    "_sample_values": _sample_values,
    "_suggest_items": _suggest_items,
    "_unknown_item_error": _unknown_item_error,
}


__all__ = [
    "_ambiguous_labels",
    "_describe_item_suggestions",
    "_format_context_label",
    "_format_find_context",
    "_format_unknown_id_error",
    "_item_locations",
    "_label_key",
    "_parent_headers",
    "_sample_values",
    "_suggest_items",
    "_unknown_item_error",
]
