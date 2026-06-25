"""Shared model item iteration helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable

from .models import FinancialModel, LineItem


def _iter_items(model: FinancialModel) -> Iterable[LineItem]:
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                yield item


__all__ = ["_iter_items"]
