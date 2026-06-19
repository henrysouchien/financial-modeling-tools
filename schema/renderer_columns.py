"""Column and period mapping helpers for schema rendering."""

from __future__ import annotations

from typing import List, Literal, Optional

from .models import PERIOD_MODE_YEARLY, TimeStructure, shift_period
from .renderer_payload import _col_to_index, _index_to_col


class AbsoluteColumnMapper:
    """Translate relative schema column positions into absolute Excel columns."""

    def __init__(
        self,
        time_structure: TimeStructure,
        first_data_column: str,
        period_scope: Literal["all", "projection", "historical"] = "all",
    ) -> None:
        if not first_data_column:
            raise ValueError("Sheet layout first_data_column is required for rendering")
        if period_scope not in {"all", "projection", "historical"}:
            raise ValueError(f"Unknown period_scope: {period_scope}")

        self._mode = time_structure.period_mode or PERIOD_MODE_YEARLY
        self._first_data_column = first_data_column.upper()
        self._period_scope = period_scope
        self._projection_periods = _projection_periods(time_structure)
        first_index = _col_to_index(self._first_data_column)

        relative_map = time_structure.column_map or time_structure.period_column_map
        if relative_map and period_scope == "all":
            ordered = sorted(
                ((int(period), str(col).upper()) for period, col in relative_map.items()),
                key=lambda item: _col_to_index(item[1]),
            )
            self._periods = [period for period, _col in ordered]
            projection_set = set(self._projection_periods)
            self._rendered_projection_periods = [
                period for period in self._periods if period in projection_set
            ]
            self._absolute_cols = {
                period: _index_to_col(first_index + _col_to_index(relative_col) - 1)
                for period, relative_col in ordered
            }
            return

        periods = _scoped_periods(time_structure, period_scope)
        self._periods = list(periods)
        if period_scope == "projection":
            self._rendered_projection_periods = list(self._periods)
        elif period_scope == "historical":
            self._rendered_projection_periods = []
        else:
            self._rendered_projection_periods = list(self._projection_periods)
        self._absolute_cols = {
            period: _index_to_col(first_index + index)
            for index, period in enumerate(self._periods)
        }

    @property
    def first_data_column(self) -> str:
        return self._first_data_column

    def col_for_period(self, period: int) -> str:
        try:
            return self._absolute_cols[int(period)]
        except KeyError as exc:
            raise KeyError(f"Unknown rendered period: {period}") from exc

    def col_for_offset(self, period: int, t: int) -> Optional[str]:
        shifted = shift_period(int(period), int(t), self._mode)
        if shifted is None:
            return None
        return self._absolute_cols.get(int(shifted))

    def all_periods(self) -> List[int]:
        return list(self._periods)

    def projection_periods(self) -> List[int]:
        return list(self._rendered_projection_periods)

    def last_column(self) -> str:
        if not self._periods:
            return self._first_data_column
        return self._absolute_cols[self._periods[-1]]


def _selector_column(first_data_column: str) -> str:
    index = _col_to_index(first_data_column)
    if index <= 1:
        return first_data_column
    return _index_to_col(index - 1)


def _historical_periods(time_structure: TimeStructure) -> List[int]:
    return [int(period) for period in (time_structure.historical_periods or time_structure.historical_years)]


def _projection_periods(time_structure: TimeStructure) -> List[int]:
    return [int(period) for period in (time_structure.projection_periods or time_structure.projection_years)]


def _time_order(time_structure: TimeStructure) -> List[int]:
    return _historical_periods(time_structure) + _projection_periods(time_structure)


def _scoped_periods(
    time_structure: TimeStructure,
    period_scope: Literal["all", "projection", "historical"],
) -> List[int]:
    if period_scope == "projection":
        return _projection_periods(time_structure)
    if period_scope == "historical":
        return _historical_periods(time_structure)
    return _time_order(time_structure)


__all__ = ["AbsoluteColumnMapper"]
