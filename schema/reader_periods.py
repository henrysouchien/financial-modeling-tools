"""Period header detection helpers for :mod:`schema.reader`."""

from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import (
    PERIOD_MODE_YEARLY,
    TimeStructure,
    encode_period,
    period_year,
)
from .reader_cells import (
    _coerce_number,
    _get_cell_value,
    _index_to_col,
    _parse_period_token,
)

_QUARTERLY_RE = re.compile(r"^[1-4]Q\d{2,4}[AEae]?$")


_QUARTERLY_AUTO_THRESHOLD = 4  # Minimum quarterly tokens to trigger auto-detection


def _sheet_has_quarterly_tokens(cells: Dict[Tuple[int, int], Any]) -> bool:
    """Detect quarterly columns in header rows.

    Requires at least _QUARTERLY_AUTO_THRESHOLD quarterly tokens in a single
    row to avoid false positives from stray '1Q24'-like labels in notes cells.
    """
    for row in range(1, 11):
        count = 0
        for col in range(2, 200):
            parsed = _parse_period_token(_get_cell_value(cells, row, col))
            if parsed and parsed[2]:
                count += 1
                if count >= _QUARTERLY_AUTO_THRESHOLD:
                    return True
    return False


def _find_period_header(
    cells: Dict[Tuple[int, int], Any],
    mode: str,
) -> Tuple[Dict[int, int], Set[int], Set[int]]:
    """Heuristically locate period header rows and return period mappings."""
    rows_data = []
    for row in range(1, 11):
        periods: Dict[int, int] = {}
        quarterly_cols: Set[int] = set()
        annual_cols: Set[int] = set()
        is_date_header = _row_is_date_period_header(cells, row)
        for col in range(2, 200):
            value = _get_cell_value(cells, row, col)
            parsed = _parse_period_token(value)
            if parsed is None and is_date_header:
                parsed = _parse_excel_date_period_token(value)
            if parsed:
                year, slot, is_quarterly = parsed
                period = encode_period(year, slot or 5, mode)
                periods[col] = period
                if is_quarterly:
                    quarterly_cols.add(col)
                else:
                    annual_cols.add(col)
        if is_date_header and periods and not quarterly_cols:
            periods = _expand_date_header_periods(cells, row, periods, mode)
            annual_cols = set(periods)
        rows_data.append((row, periods, quarterly_cols, annual_cols))

    rows_data.sort(key=lambda x: -len(x[1]))
    if not rows_data or not rows_data[0][1]:
        return {}, set(), set()

    primary_periods = rows_data[0][1]
    primary_quarterly = rows_data[0][2]
    primary_annual = rows_data[0][3]

    if not primary_quarterly:
        return primary_periods, set(), set(primary_annual)

    merged = dict(primary_periods)
    all_quarterly = set(primary_quarterly)
    all_annual = set(primary_annual)
    primary_count = len(primary_periods)
    for _, periods, qcols, acols in rows_data[1:3]:
        if len(periods) >= max(3, primary_count // 4):
            for col, period in periods.items():
                if col not in merged:
                    merged[col] = period
            all_quarterly.update(qcols)
            all_annual.update(acols)

    return merged, all_quarterly, all_annual


def _is_weak_annual_period_header(
    cells: Dict[Tuple[int, int], Any],
    col_to_period: Dict[int, int],
    quarterly_cols: Set[int],
) -> bool:
    if not col_to_period or quarterly_cols:
        return False
    if len(col_to_period) >= 3:
        return False
    if any(_row_is_date_period_header(cells, row) for row in range(1, 11)):
        return False
    return True


def _row_is_date_period_header(cells: Dict[Tuple[int, int], Any], row: int) -> bool:
    for col in range(1, 4):
        value = _get_cell_value(cells, row, col)
        if value is None:
            continue
        text = str(value).strip().lower()
        if "year ended" in text or "period ended" in text:
            return True
    return False


def _parse_excel_date_period_token(value: Optional[str]) -> Optional[Tuple[int, Optional[int], bool]]:
    serial = _coerce_number(value)
    if serial is None or not serial.is_integer():
        return None
    if serial < 25000 or serial > 80000:
        return None
    date_value = datetime(1899, 12, 30) + timedelta(days=int(serial))
    if 1900 <= date_value.year <= 2100:
        return date_value.year, None, False
    return None


def _expand_date_header_periods(
    cells: Dict[Tuple[int, int], Any],
    row: int,
    periods: Dict[int, int],
    mode: str,
) -> Dict[int, int]:
    if mode != PERIOD_MODE_YEARLY or not periods:
        return periods
    first_col = min(periods)
    first_year = periods[first_col]
    max_used_col = max(
        (
            col
            for (_row, col), cell in cells.items()
            if _row == row and col >= first_col and (cell.value not in (None, "") or cell.formula)
        ),
        default=max(periods),
    )
    expanded = dict(periods)
    for col in range(first_col, max_used_col + 1):
        expanded.setdefault(col, first_year + (col - first_col))
    return expanded


def _sheet_uses_period_columns(
    cells: Dict[Tuple[int, int], Any],
    col_to_period: Dict[int, int],
) -> bool:
    if not col_to_period:
        return False
    min_col = min(col_to_period)
    max_col = max(col_to_period)
    populated = 0
    for (_row, col), cell in cells.items():
        if min_col <= col <= max_col and (cell.value not in (None, "") or cell.formula):
            populated += 1
            if populated >= 3:
                return True
    return False


def _build_time_structure(
    periods: List[int],
    period_mode: str = PERIOD_MODE_YEARLY,
    historical_cutoff_year: Optional[int] = None,
) -> TimeStructure:
    """Build a TimeStructure from detected period keys."""
    if not periods:
        return TimeStructure(
            fiscal_year_end="",
            period_mode=period_mode,
            historical_periods=[],
            projection_periods=[],
            period_column_map={},
            historical_years=[],
            projection_years=[],
            column_map={},
        )

    periods_sorted = sorted(set(periods))
    historical_periods: List[int] = []
    projection_periods: List[int] = []
    if historical_cutoff_year is not None:
        historical_periods = [
            period
            for period in periods_sorted
            if period_year(period, period_mode) <= historical_cutoff_year
        ]
        projection_periods = [
            period
            for period in periods_sorted
            if period_year(period, period_mode) > historical_cutoff_year
        ]
    else:
        historical_periods = periods_sorted

    if period_mode == PERIOD_MODE_YEARLY:
        historical_years = list(historical_periods)
        projection_years = list(projection_periods)
    else:
        historical_years = sorted({period_year(period, period_mode) for period in historical_periods})
        projection_years = sorted({period_year(period, period_mode) for period in projection_periods})

    all_years = sorted(set(historical_years + projection_years))
    column_map = {year: _index_to_col(idx + 1) for idx, year in enumerate(all_years)}
    period_column_map = {period: _index_to_col(idx + 1) for idx, period in enumerate(periods_sorted)}
    return TimeStructure(
        fiscal_year_end="",
        period_mode=period_mode,
        historical_periods=historical_periods,
        projection_periods=projection_periods,
        period_column_map=period_column_map,
        historical_years=historical_years,
        projection_years=projection_years,
        column_map=column_map,
    )


__all__ = [
    "_QUARTERLY_AUTO_THRESHOLD",
    "_QUARTERLY_RE",
    "_build_time_structure",
    "_expand_date_header_periods",
    "_find_period_header",
    "_is_weak_annual_period_header",
    "_parse_excel_date_period_token",
    "_row_is_date_period_header",
    "_sheet_has_quarterly_tokens",
    "_sheet_uses_period_columns",
]
