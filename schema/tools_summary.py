"""Summary and key-metric helpers for schema model tools."""

from __future__ import annotations

import sys
from typing import Any, Dict, Iterable, List, Optional

from .models import FinancialModel, ItemType, LineItem
from .tools_items import _item_locations, _parent_headers
from .tools_periods import _historical_periods, _projection_periods


_PARENT_MODULE = "schema.tools"

_KEY_METRIC_PATTERNS = [
    "revenue",
    "net_income",
    "free_cash_flow",
    "ebitda",
    "eps",
    "gross_profit",
    "operating_income",
]


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


def _formula_type(item: LineItem) -> Optional[str]:
    spec = item.projected or item.historical
    return spec.type.value if spec else None


def _summarize_line_items(model: FinancialModel, items: Iterable[LineItem]) -> List[Dict]:
    item_locs = _compat("_item_locations", _item_locations)(model)
    parent_headers = _compat("_parent_headers", _parent_headers)(model)
    historical_periods = set(_compat("_historical_periods", _historical_periods)(model))
    projection_periods = set(_compat("_projection_periods", _projection_periods)(model))
    formula_type = _compat("_formula_type", _formula_type)

    rows = []
    for item in items:
        context = item_locs.get(item.id)
        value_periods = sorted((item.values.values if item.values else {}).keys())
        formula_periods = sorted(item.formula_periods or [])
        rows.append(
            {
                "id": item.id,
                "label": item.label,
                "sheet": context[0] if context else None,
                "section": context[1] if context else None,
                "row": item.row,
                "column": item.column,
                "label_column": item.label_column,
                "parent_header": parent_headers.get(item.id),
                "item_type": item.item_type.value,
                "unit": item.unit.value,
                "formula_type": formula_type(item),
                "has_historical_formula": item.historical is not None
                or any(period in historical_periods for period in formula_periods),
                "has_projection_formula": item.projected is not None
                or any(period in projection_periods for period in formula_periods),
                "historical_periods": [period for period in value_periods if period in historical_periods],
                "projection_periods": [period for period in value_periods if period in projection_periods],
                "formula_periods": formula_periods,
            }
        )
    rows.sort(key=lambda row: (str(row["sheet"] or ""), int(row["row"] or 0), row["id"]))
    return rows


def _find_key_metrics(items: Iterable[LineItem]) -> List[LineItem]:
    by_id = list(items)
    selected: List[LineItem] = []
    taken: set[str] = set()
    key_metric_patterns = _compat("_KEY_METRIC_PATTERNS", _KEY_METRIC_PATTERNS)
    for pattern in key_metric_patterns:
        matches = []
        for item in by_id:
            if item.id in taken:
                continue
            item_id = item.id.lower()
            label = item.label.lower()
            haystack = f"{item_id} {label}"
            if pattern not in haystack:
                continue
            id_leaf = item_id.split(".")[-1]
            label_norm = label.replace(" ", "_")
            is_exact = id_leaf == pattern or label_norm == pattern
            is_total = not is_exact and (
                id_leaf in (f"total_{pattern}", f"total_{pattern}s")
                or label_norm in (f"total_{pattern}", f"total_{pattern}s")
            )
            is_plural = not is_exact and not is_total and (
                id_leaf == f"{pattern}s" or label_norm == f"{pattern}s"
            )
            is_prefixed_base = id_leaf.startswith(f"base_{pattern}") or label.startswith(f"base {pattern}")
            tier = 0 if is_exact else (1 if is_total else (2 if is_plural else 3))
            first_idx = haystack.index(pattern)
            is_header_penalty = 1 if item.item_type == ItemType.header else 0
            matches.append(
                (
                    is_header_penalty,
                    tier,
                    1 if is_prefixed_base else 0,
                    first_idx,
                    item.id,
                    item,
                )
            )

        if matches:
            matches.sort(key=lambda row: (row[0], row[1], row[2], row[3], row[4]))
            winner = matches[0][5]
            selected.append(winner)
            taken.add(winner.id)
    return selected


_ORIGINALS = {
    "_KEY_METRIC_PATTERNS": _KEY_METRIC_PATTERNS,
    "_find_key_metrics": _find_key_metrics,
    "_formula_type": _formula_type,
    "_historical_periods": _historical_periods,
    "_item_locations": _item_locations,
    "_parent_headers": _parent_headers,
    "_projection_periods": _projection_periods,
    "_summarize_line_items": _summarize_line_items,
}


__all__ = [
    "_KEY_METRIC_PATTERNS",
    "_find_key_metrics",
    "_formula_type",
    "_summarize_line_items",
]
