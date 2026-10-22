"""Company metadata population helpers for schema builds."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timezone
import re
from typing import Optional

from .build_model_items import _iter_items
from .models import (
    BuildStatus,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    PERIOD_MODE_YEARLY,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from industry_slice_core.industry_types import Unit


_COMPANY_NAME_TOKEN = "[Company Name] ([TICKER])"
_AUTO_CREATED_AT = object()
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
_FINANCIAL_MODEL_YEAR_HEADER_ID = "tpl.fm.header.millions_except_per_share_data"
_NAMED_FISCAL_YEAR_END_RE = re.compile(
    r"^(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)(?: (?P<day>\d{1,2}))?$",
    re.IGNORECASE,
)
_NUMERIC_FISCAL_YEAR_END_RE = re.compile(
    r"^(?P<month>0[1-9]|1[0-2])-(?P<day>0[1-9]|[12]\d|3[01])$"
)


def _parse_fiscal_year_end(fiscal_year_end: str) -> tuple[int, int | None]:
    """Parse the closed fiscal-year-end grammar into month and optional day.

    Accepted inputs are a full month name, a full month name plus day, or an
    exact zero-padded ``MM-DD`` value. A month-only value is the established
    canonical shorthand for that month's final calendar day. No fuzzy token,
    slash-date, ISO-date, abbreviation, or trailing-text fallback is allowed.
    """

    raw = str(fiscal_year_end or "").strip()
    if not raw:
        raise ValueError("fiscal_year_end is required")

    named_match = _NAMED_FISCAL_YEAR_END_RE.fullmatch(raw)
    numeric_match = _NUMERIC_FISCAL_YEAR_END_RE.fullmatch(raw)
    match = named_match or numeric_match
    if match is None:
        raise ValueError(
            "fiscal_year_end must be a full month name, 'Month DD', or zero-padded 'MM-DD'"
        )

    month_token = match.group("month")
    month = (
        _MONTH_NUMBERS[month_token.lower()]
        if named_match is not None
        else int(month_token)
    )
    day_token = match.group("day")
    day = int(day_token) if day_token is not None else None
    if day is not None:
        # Leap year 2000 makes February 29 syntactically valid; the concrete
        # target year is validated by ``_fiscal_year_end_date`` below.
        try:
            date(2000, month, day)
        except ValueError as exc:
            raise ValueError(f"Invalid fiscal year end: {fiscal_year_end!r}") from exc
    return month, day


def _fiscal_year_end_date(fiscal_year_end: str, year: int) -> date:
    """Return the exact fiscal-year-end date for one annual period."""

    month, declared_day = _parse_fiscal_year_end(fiscal_year_end)
    day = (
        declared_day
        if declared_day is not None
        else calendar.monthrange(year, month)[1]
    )
    try:
        return date(year, month, day)
    except ValueError as exc:
        raise ValueError(
            f"Invalid fiscal year end {fiscal_year_end!r} for fiscal year {year}"
        ) from exc


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
    created_at: datetime | None | object = _AUTO_CREATED_AT,
) -> None:
    """Apply company metadata and materialize the complete annual header axis.

    Fiscal-year-end parsing and both header rows are validated before any
    company/model field is mutated. Invalid input therefore fails atomically
    instead of leaving a populated workbook with a blank or partial header.
    """

    if model.time_structure.period_mode != PERIOD_MODE_YEARLY:
        raise ValueError("annual company headers require yearly period_mode")
    historical_period_keys = [
        int(period) for period in model.time_structure.historical_periods
    ]
    historical_year_keys = [
        int(period) for period in model.time_structure.historical_years
    ]
    projection_period_keys = [
        int(period) for period in model.time_structure.projection_periods
    ]
    projection_year_keys = [
        int(period) for period in model.time_structure.projection_years
    ]
    if (
        historical_period_keys
        and historical_year_keys
        and historical_period_keys != historical_year_keys
    ):
        raise ValueError("historical_periods and historical_years must match")
    if (
        projection_period_keys
        and projection_year_keys
        and projection_period_keys != projection_year_keys
    ):
        raise ValueError("projection_periods and projection_years must match")
    historical_periods = historical_period_keys or historical_year_keys
    projection_periods = projection_period_keys or projection_year_keys
    if len(historical_periods) != len(set(historical_periods)):
        raise ValueError("historical annual periods must be unique")
    if len(projection_periods) != len(set(projection_periods)):
        raise ValueError("projection annual periods must be unique")
    if historical_periods != sorted(historical_periods):
        raise ValueError("historical annual periods must be sorted")
    if projection_periods != sorted(projection_periods):
        raise ValueError("projection annual periods must be sorted")
    overlap = sorted(set(historical_periods).intersection(projection_periods))
    if overlap:
        raise ValueError(
            f"annual periods cannot be both actual and estimate: {overlap}"
        )
    if (
        historical_periods
        and projection_periods
        and historical_periods[-1] >= projection_periods[0]
    ):
        raise ValueError("projection annual periods must follow all historical periods")

    annual_periods = historical_periods + projection_periods
    if annual_periods and annual_periods != list(
        range(annual_periods[0], annual_periods[-1] + 1)
    ):
        raise ValueError("annual periods must form one contiguous fiscal axis")
    _parse_fiscal_year_end(fye)
    historical_period_set = set(historical_periods)
    assumptions_header = None
    financial_model_header = None
    assumptions_header_values: ValueSeries | None = None
    financial_model_overrides: dict[int, FormulaSpec] | None = None
    if annual_periods:
        # Parse before touching metadata, and validate every concrete year (for
        # example a February 29 fiscal end against a non-leap fiscal year).
        assumptions_header_values = ValueSeries(
            values={
                period: ValueCell(
                    period=period,
                    value=float(
                        _gregorian_to_excel_serial(_fiscal_year_end_date(fye, period))
                    ),
                    provenance=ValueProvenance.imported_other,
                    note=(
                        "annual_header_status=actual"
                        if period in historical_period_set
                        else "annual_header_status=estimate"
                    ),
                )
                for period in annual_periods
            }
        )
        financial_model_overrides = {
            period: FormulaSpec(
                type=FormulaType.constant,
                params={
                    "value": (
                        str(period) if period in historical_period_set else f"{period}E"
                    )
                },
            )
            for period in annual_periods
        }
        model.build_index()
        try:
            assumptions_header = model.get_item(_YEAR_HEADER_ID)
            financial_model_header = model.get_item(_FINANCIAL_MODEL_YEAR_HEADER_ID)
        except KeyError as exc:
            raise ValueError(
                f"Required annual header row is missing: {exc.args[0]}"
            ) from exc

    model.company.ticker = ticker
    model.company.name = name
    model.company.fiscal_year_end = fye
    model.company.sector = sector

    model.time_structure.fiscal_year_end = fye

    model.metadata.build_status = BuildStatus.historicals_populated
    model.metadata.is_template = False
    if created_at is _AUTO_CREATED_AT:
        model.metadata.created_at = datetime.now(timezone.utc).isoformat()
    elif created_at is not None:
        if not isinstance(created_at, datetime):
            raise TypeError("created_at must be a datetime or None")
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        model.metadata.created_at = created_at.astimezone(timezone.utc).isoformat()

    for item in _iter_items(model):
        if item.template_token == _COMPANY_NAME_TOKEN:
            item.label = f"{name} ({ticker})"

    if assumptions_header is not None and financial_model_header is not None:
        assumptions_header.values = assumptions_header_values
        assumptions_header.historical = None
        assumptions_header.projected = None
        assumptions_header.overrides = None
        assumptions_header.formula_periods = []
        assumptions_header.unit = Unit.date
        assumptions_header.format = ""

        financial_model_header.values = None
        financial_model_header.historical = None
        financial_model_header.projected = None
        financial_model_header.overrides = financial_model_overrides
        financial_model_header.formula_periods = list(annual_periods)
        financial_model_header.unit = None
        financial_model_header.format = "General"


__all__ = [
    "_COMPANY_NAME_TOKEN",
    "_MONTH_NUMBERS",
    "_FINANCIAL_MODEL_YEAR_HEADER_ID",
    "_YEAR_HEADER_ID",
    "_fiscal_year_end_date",
    "_gregorian_to_excel_serial",
    "_parse_fiscal_year_end",
    "update_company_info",
]
