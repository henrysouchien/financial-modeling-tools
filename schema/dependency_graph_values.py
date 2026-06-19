"""Runtime value and period helpers for :mod:`schema.dependency_graph`."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from .dependency_graph_helpers import (
    _col_to_index,
    _is_input_provenance,
    _numeric_label_value,
)
from .models import (
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ValueProvenance,
    PERIOD_MODE_YEARLY,
    shift_period,
)
from .refs import line_item_ref_from_obj


def _seed_inputs(
    self: Any,
    period: int,
    inputs: Dict[str, Dict[int, float]],
    results: Dict[str, Dict[int, float]],
    recompute: Optional[Set[str]] = None,
) -> None:
    """Seed inputs and existing value series into the results matrix."""
    if inputs:
        for line_item_id, by_period in inputs.items():
            if period in by_period:
                results.setdefault(line_item_id, {})[period] = by_period[period]

    if not self.model:
        return
    for item_id, item in self.model._index.items():
        if results.get(item_id, {}).get(period) is not None:
            continue
        if self._compute_seed_results is not None and (not recompute or item_id not in recompute):
            seeded = self._compute_seed_results.get(item_id, {}).get(period)
            if seeded is not None:
                results.setdefault(item_id, {})[period] = seeded
                continue
        if item.overrides and period in item.overrides:
            override = item.overrides[period]
            selected_spec = self._spec_for_period(item, period)
            if selected_spec is override:
                if override.type == FormulaType.constant:
                    value = override.params.get("value") if override.params else None
                    if value is not None:
                        try:
                            results.setdefault(item_id, {})[period] = float(value)
                        except (TypeError, ValueError):
                            pass
                continue
        if recompute and item_id in recompute:
            spec = self._spec_for_period(item, period)
            if spec is not None and spec.type != FormulaType.constant:
                continue
        if not item.values:
            continue
        value_cell = item.values.values.get(period)
        if value_cell is None:
            continue
        results.setdefault(item_id, {})[period] = value_cell.value


def _time_order(self: Any) -> List[int]:
    if not self.model:
        return []
    ts = self.model.time_structure
    if ts.historical_periods or ts.projection_periods:
        return list(ts.historical_periods) + list(ts.projection_periods)
    return list(ts.historical_years) + list(ts.projection_years)


def _spec_for_period(self: Any, item: LineItem, period: int) -> Optional[FormulaSpec]:
    """Select the appropriate FormulaSpec for a period (override/historical/projected)."""
    recompute_skip = False
    if item.overrides and period in item.overrides:
        override = item.overrides[period]
        # When this item is downstream of user input overrides and the override is
        # a constant snapshot, prefer the real formula so upstream changes propagate.
        if (
            override.type == FormulaType.constant
            and self._compute_propagate
            and item.id in self._compute_propagate
            and item.projected
            and item.projected.type != FormulaType.constant
        ):
            recompute_skip = True
        else:
            return override

    if item.formula_periods is not None and period not in item.formula_periods:
        if not recompute_skip:
            return None

    if not self.model:
        return item.projected or item.historical

    ts = self.model.time_structure
    historical_periods = list(ts.historical_periods) or list(ts.historical_years)
    projection_periods = list(ts.projection_periods) or list(ts.projection_years)
    if period in historical_periods:
        return item.historical
    if period in projection_periods:
        return item.projected
    return item.projected or item.historical


def _value_of(
    self: Any,
    obj: Any,
    period: int,
    results: Dict[str, Dict[int, float]],
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Resolve a LineItemRef or primitive value for a given period."""
    if obj is None:
        return None
    if isinstance(obj, LineItemRef):
        target_item = None
        if self.model is not None:
            try:
                target_item = self.model.get_item(obj.id)
            except KeyError:
                return None
        if target_item is not None and target_item.column is not None:
            if int(obj.t) != 0:
                raise ValueError(
                    f"LineItemRef to fixed-cell target {obj.id!r} with t={obj.t} is invalid"
                )
            target_period = self._fixed_cell_anchor_period(target_item)
            if target_period is None:
                return None
            return self._read_with_fallback(obj.id, target_period, results)

        if obj.period_anchor == "last":
            if self.model is None:
                return None
            target_period = self._period_for_anchor_last(obj.id, obj.t)
            return self._read_with_fallback(obj.id, target_period, results)

        idx = time_index.get(period)
        if idx is None:
            return None
        target_idx = idx + obj.t
        if target_idx < 0:
            target_period = self._bootstrap_period(period, obj.t)
            if target_period is None:
                return None
            return self._input_value_fallback(obj.id, target_period)
        if target_idx >= len(time_order):
            return None
        target_period = time_order[target_idx]
        return self._read_with_fallback(obj.id, target_period, results)
    if isinstance(obj, dict):
        coerced = line_item_ref_from_obj(obj)
        if coerced is not None:
            return self._value_of(coerced, period, results, time_index, time_order)
        return self._eval_expr(obj, period, results, time_index, time_order)
    if isinstance(obj, (int, float)):
        return float(obj)
    return None


def _read_with_fallback(
    self: Any,
    item_id: str,
    target_period: int,
    results: Dict[str, Dict[int, float]],
) -> Optional[float]:
    """Read a value from current results, scoped seed results, then input cache."""
    val = results.get(item_id, {}).get(target_period)
    if (
        val is None
        and self._compute_seed_results is not None
        and self._compute_periods is not None
        and target_period not in self._compute_periods
    ):
        val = self._compute_seed_results.get(item_id, {}).get(target_period)
    if val is None and self.model is not None:
        try:
            target_item = self.model.get_item(item_id)
        except KeyError:
            target_item = None
        if target_item is not None and target_item.column is not None and target_item.values:
            value_cell = target_item.values.values.get(target_period)
            if value_cell is not None and value_cell.value is not None:
                val = value_cell.value
    if val is None:
        val = self._input_value_fallback(item_id, target_period)
    return val


def _fixed_cell_anchor_period(self: Any, item: LineItem) -> Optional[int]:
    if item.formula_periods:
        return int(item.formula_periods[0])
    projection_periods = self._projection_periods()
    if projection_periods:
        return int(projection_periods[0])
    periods = self._time_order()
    return int(periods[0]) if periods else None


def _row_item_id(self: Any, sheet_name: str, row: int) -> Optional[str]:
    item_ids = self._row_item_map.get((sheet_name, int(row)), [])
    if not item_ids:
        return None
    for item_id in item_ids:
        item = self.model.get_item(item_id) if self.model else None
        if item is not None and item.column is None:
            return item_id
    return item_ids[0] if len(item_ids) == 1 else None


def _column_for_period(self: Any, period: int) -> Optional[str]:
    if not self.model:
        return None
    ts = self.model.time_structure
    column_map = ts.column_map or ts.period_column_map
    column = column_map.get(int(period))
    return str(column).upper() if column else None


def _period_for_anchor_last(self: Any, target_id: str, t: int) -> int:
    t_int = int(t)
    if t_int > 0:
        raise ValueError(
            f"LineItemRef {target_id!r} with period_anchor='last' requires t<=0 (got t={t_int})"
        )

    target_sheet = self._item_sheet_map.get(target_id)
    if target_sheet is None:
        raise ValueError(f"LineItemRef {target_id!r} target item not found in model index")

    sheet_projection_periods = self._target_sheet_projection_periods(target_sheet)
    if not sheet_projection_periods:
        sheet = self.model.sheets.get(target_sheet) if self.model else None
        layout = sheet.layout if sheet else None
        sheet_period_scope = layout.period_scope if layout else "all"
        raise ValueError(
            f"LineItemRef {target_id!r} with period_anchor='last' but target sheet "
            f"{target_sheet!r} (period_scope={sheet_period_scope!r}) has no projection periods"
        )

    target_idx = len(sheet_projection_periods) - 1 + t_int
    if target_idx < 0:
        raise ValueError(
            f"LineItemRef {target_id!r} with period_anchor='last' t={t_int} falls outside "
            f"target sheet's projection range (len={len(sheet_projection_periods)})"
        )
    return sheet_projection_periods[target_idx]


def _target_sheet_projection_periods(self: Any, target_sheet: str) -> List[int]:
    if not self.model:
        return []
    sheet = self.model.sheets[target_sheet]
    layout = sheet.layout
    period_scope = layout.period_scope if layout else "all"
    if period_scope == "historical":
        return []

    global_projection = self._projection_periods()
    if period_scope == "projection":
        return global_projection

    relative_map = (
        self.model.time_structure.column_map
        or self.model.time_structure.period_column_map
    )
    if not relative_map:
        return global_projection

    projection_set = set(global_projection)
    ordered = sorted(
        ((int(period), str(col).upper()) for period, col in relative_map.items()),
        key=lambda item: _col_to_index(item[1]),
    )
    return [period for period, _col in ordered if period in projection_set]


def _sum_range_target_periods(self: Any, target_id: str) -> List[int]:
    target_sheet = self._item_sheet_map.get(target_id)
    if target_sheet is None:
        raise KeyError(f"SUM_RANGE target {target_id!r} not found in model index")
    return self._target_sheet_all_periods(target_sheet)


def _target_sheet_all_periods(self: Any, target_sheet: str) -> List[int]:
    if not self.model:
        return []
    sheet = self.model.sheets[target_sheet]
    layout = sheet.layout
    period_scope = layout.period_scope if layout else "all"
    if period_scope == "projection":
        return self._projection_periods()
    if period_scope == "historical":
        return self._historical_periods()

    relative_map = (
        self.model.time_structure.column_map
        or self.model.time_structure.period_column_map
    )
    if relative_map:
        ordered = sorted(
            ((int(period), str(col).upper()) for period, col in relative_map.items()),
            key=lambda item: _col_to_index(item[1]),
        )
        return [period for period, _col in ordered]
    return self._time_order()


def _historical_periods(self: Any) -> List[int]:
    if not self.model:
        return []
    ts = self.model.time_structure
    return [int(period) for period in (ts.historical_periods or ts.historical_years)]


def _projection_periods(self: Any) -> List[int]:
    if not self.model:
        return []
    ts = self.model.time_structure
    return [int(period) for period in (ts.projection_periods or ts.projection_years)]


def _input_value_fallback(self: Any, line_item_id: str, target_period: int) -> Optional[float]:
    """Look up a genuine input value for bootstrapping prior-period references.

    Only returns values with input provenance (imported_other, input,
    imported_edgar, imported_fmp). Never falls back to derived/computed
    cached values.
    """
    item = self.model.get_item(line_item_id)
    if not item:
        return None

    if item.values:
        vc = item.values.values.get(target_period)
        if vc is not None and vc.value is not None and _is_input_provenance(vc.provenance):
            return vc.value

        if item.column is not None:
            for fixed_period in sorted(item.values.values):
                fixed_vc = item.values.values[fixed_period]
                if (
                    fixed_vc.value is not None
                    and _is_input_provenance(fixed_vc.provenance)
                ):
                    return fixed_vc.value

    if item.column is not None and item.item_type == ItemType.input:
        return _numeric_label_value(item.label)

    return None


def _bootstrap_period(self: Any, period: int, t: int) -> Optional[int]:
    if not self.model:
        return None
    period_mode = self.model.time_structure.period_mode or PERIOD_MODE_YEARLY
    return shift_period(period, t, period_mode)


def _cached_value_for_period(
    self: Any,
    item_id: str,
    period: int,
    results: Dict[str, Dict[int, float]],
) -> Optional[float]:
    existing = results.get(item_id, {}).get(period)
    if not self.model:
        return existing
    item = self.model.get_item(item_id)
    if item.values:
        value_cell = item.values.values.get(period)
        if value_cell is not None and value_cell.value is not None:
            return value_cell.value
    if self._compute_seed_results is not None:
        seeded = self._compute_seed_results.get(item_id, {}).get(period)
        if seeded is not None:
            return seeded
    return existing


def _cached_computed_value_for_period(
    self: Any,
    item_id: str,
    period: int,
) -> Optional[float]:
    if not self.model:
        return None
    item = self.model.get_item(item_id)
    if not item.values:
        return None
    value_cell = item.values.values.get(period)
    if value_cell is None or value_cell.value is None:
        return None
    if value_cell.provenance == ValueProvenance.computed:
        return value_cell.value
    period_mode = self.model.time_structure.period_mode or PERIOD_MODE_YEARLY
    if period_mode != PERIOD_MODE_YEARLY and value_cell.provenance in (
        ValueProvenance.imported_other,
        ValueProvenance.imported_edgar,
        ValueProvenance.imported_fmp,
    ):
        return value_cell.value
    return None


def _ratio_cached_fallback_enabled(self: Any) -> bool:
    if self._ratio_zero_denominator_policy == "fallback_cached":
        return True
    if self._ratio_zero_denominator_policy == "auto_fallback_cached":
        return self._compute_has_recompute
    return False


def _cycle_cached_fallback_enabled(self: Any) -> bool:
    if self._cycle_fallback_policy == "on":
        return True
    if self._cycle_fallback_policy in {"auto", "auto_propagate"}:
        return self._compute_has_recompute
    return False
