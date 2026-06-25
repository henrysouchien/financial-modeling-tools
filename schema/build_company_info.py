"""Company metadata population helpers for schema builds."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timezone
import logging
from typing import Optional

from .build_model_items import _iter_items
from .models import (
    BuildStatus,
    FinancialModel,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)


_COMPANY_NAME_TOKEN = "[Company Name] ([TICKER])"
_MONTH_NUMBERS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_YEAR_HEADER_ID = "tpl.a.header.year_header"


def _fiscal_year_end_date(month_name: str, year: int) -> date:
    """Return the last day of the fiscal year ending in the given month."""
    month_num = _MONTH_NUMBERS.get(month_name.strip().lower())
    if month_num is None:
        raise ValueError(f"Unknown fiscal year end month: {month_name!r}")
    last_day = calendar.monthrange(year, month_num)[1]
    return date(year, month_num, last_day)


def _gregorian_to_excel_serial(dt: date) -> int:
    """Convert a Python date to an Excel serial number (Lotus 1-2-3 compatible)."""
    delta = dt - date(1899, 12, 31)
    return delta.days + (1 if dt >= date(1900, 3, 1) else 0)


def update_company_info(
    model: FinancialModel,
    ticker: str,
    name: str,
    fye: str,
    sector: Optional[str] = None,
) -> None:
    """Apply company metadata, resolve the company-name template token, and seed the year header."""

    model.company.ticker = ticker
    model.company.name = name
    model.company.fiscal_year_end = fye
    model.company.sector = sector

    model.time_structure.fiscal_year_end = fye

    model.metadata.build_status = BuildStatus.historicals_populated
    model.metadata.is_template = False
    model.metadata.created_at = datetime.now(timezone.utc).isoformat()

    for item in _iter_items(model):
        if item.template_token == _COMPANY_NAME_TOKEN:
            item.label = f"{name} ({ticker})"

    # Seed the year header with the FY end date for the first historical period.
    # The formula ref(self[t-1], adjustment=365) chains forward from this seed.
    if fye and model.time_structure.historical_periods:
        first_hist = int(model.time_structure.historical_periods[0])
        try:
            seed_date = _fiscal_year_end_date(fye, first_hist)
            serial = _gregorian_to_excel_serial(seed_date)
            if not model._index:
                model.build_index()
            year_header = model.get_item(_YEAR_HEADER_ID)
            if year_header.values is None:
                year_header.values = ValueSeries()
            year_header.values.values[first_hist] = ValueCell(
                period=first_hist,
                value=float(serial),
                provenance=ValueProvenance.imported_other,
            )
            # Remove the first period from formula_periods so the value takes precedence
            if year_header.formula_periods and first_hist in year_header.formula_periods:
                year_header.formula_periods = [
                    p for p in year_header.formula_periods if int(p) != first_hist
                ]
        except (ValueError, KeyError):
            logging.warning("Could not seed year_header for FYE=%s, year=%d", fye, first_hist)


__all__ = [
    "_COMPANY_NAME_TOKEN",
    "_MONTH_NUMBERS",
    "_YEAR_HEADER_ID",
    "_fiscal_year_end_date",
    "_gregorian_to_excel_serial",
    "update_company_info",
]
