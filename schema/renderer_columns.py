"""Column and period mapping helpers for schema rendering."""

from __future__ import annotations

from collections.abc import Collection
from typing import List, Literal, Optional

from .models import (
    PERIOD_MODE_YEARLY,
    FinancialModel,
    LineItem,
    TimeStructure,
    shift_period,
)
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


def _fixed_cell_anchor_period(model: FinancialModel, item: LineItem) -> Optional[int]:
    """Period a fixed-cell (single-column) item anchors its formula/value to.

    Sole owner of this policy: :mod:`schema.renderer` re-exports it and
    :class:`schema.dependency_graph.DependencyGraph` delegates to it.
    """
    if item.formula_periods:
        return int(item.formula_periods[0])
    projection_periods = _projection_periods(model.time_structure)
    if projection_periods:
        return int(projection_periods[0])
    periods = _time_order(model.time_structure)
    return int(periods[0]) if periods else None


def item_owns_cell(
    model: FinancialModel,
    item: LineItem,
    period: int,
    *,
    gaps: Collection[tuple[str, int]] = (),
) -> bool:
    """Whether ``(item, period)`` is a cell of the row: the one not-a-cell rule.

    A fixed-cell row (``column`` set) is one cell, at its anchor period
    (``_fixed_cell_anchor_period``), unless retirement cleared its specs, overrides and values
    and left ``formula_periods=[]``. An unauthored input slot has unrestricted periods
    (``None``) and still owns its anchor, rendered ``=NA()`` while required.
    For a per-period row a coordinate is a cell when
    ``formula_periods`` include the period (or are unrestricted), or something claims it: an
    override, a stored value, or a typed data gap in ``gaps``
    (``build_offline_validation.lag_input_gap_readers``: the first historical cell whose lag
    input the pin does not bind, ruled ``#N/A`` beside its gap by FY2022-CLIENT-FUNDS-GAP;
    the compiler's period-average lowering leaves that cell outside ``formula_periods``, so
    the gap is its claim). A coordinate none of these claims is never written
    (``renderer._spec_for_period`` selects no spec and no value or ``=NA()`` fills the slot),
    so it cannot be missing, ``#N/A`` or the subject of a finding.

    This is the rule of ownership, not the render's write list. Every cell the render writes
    is owned; some owned cells render blank: a coordinate inside ``formula_periods`` (or on an
    unrestricted row) with no formula, override or value is an empty input slot, still a cell
    the check enumerates and the build's lineage classifies.
    ``renderer_annual_context._annual_render_context`` turns the rule into the set the renderer
    writes ``=NA()`` for (the findings gate reads that set);
    ``model_build.publish.document_owns_cell`` adds the document's recorded-gap stamp."""

    year = int(period)
    if item.column is not None:
        if (
            item.formula_periods == []
            and item.historical is None
            and item.projected is None
            and not item.overrides
            and not (
                item.values is not None
                and any(cell.value is not None for cell in item.values.values.values())
            )
        ):
            return False
        return year == _fixed_cell_anchor_period(model, item)
    if (item.overrides or {}).get(year) is not None or (str(item.id), year) in gaps:
        return True
    if item.formula_periods is None or year in {int(owned) for owned in item.formula_periods}:
        return True
    stored = None if item.values is None else item.values.values.get(year)
    return stored is not None and stored.value is not None


__all__ = ["AbsoluteColumnMapper"]
