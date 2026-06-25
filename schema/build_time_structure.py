"""Time-axis remapping helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import Any, Dict, Iterable

from .build_formula_refs import _formula_period_is_valid as _formula_period_is_valid_fallback
from .build_model_items import _iter_items as _iter_items_fallback
from .models import (
    FinancialModel,
    FormulaType,
    LineItem,
    PERIOD_MODE_YEARLY,
    shift_period as _shift_period_fallback,
)
from .refs import line_item_ref_from_obj as _line_item_ref_from_obj_fallback
from .renderer import _index_to_col as _index_to_col_fallback


ROLLING_HEADER_EXCLUSIONS = {
    "tpl.a.header.year_header",
}


def _parent_attr(name: str, default: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        return default
    return getattr(parent, name, default)


def remap_time_structure(
    model: FinancialModel,
    most_recent_fy: int,
    n_historical: int = 5,
    n_projection: int = 12,
) -> None:
    """Remap the template time axis to the requested fiscal year window."""

    formula_type_cls = _parent_attr("FormulaType", FormulaType)
    formula_period_is_valid = _parent_attr(
        "_formula_period_is_valid",
        _formula_period_is_valid_fallback,
    )
    is_self_carry_forward_projection_fn = _parent_attr(
        "_is_self_carry_forward_projection",
        _is_self_carry_forward_projection,
    )
    index_to_col = _parent_attr("_index_to_col", _index_to_col_fallback)
    iter_items = _parent_attr("_iter_items", _iter_items_fallback)
    period_mode_yearly = _parent_attr("PERIOD_MODE_YEARLY", PERIOD_MODE_YEARLY)
    shift_period = _parent_attr("shift_period", _shift_period_fallback)
    rolling_header_exclusions = _parent_attr(
        "ROLLING_HEADER_EXCLUSIONS",
        ROLLING_HEADER_EXCLUSIONS,
    )

    old_hist = [
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
        )
    ]
    old_proj = [
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
        )
    ]
    template_projection_periods = list(old_proj)

    max_historical = len(old_hist)
    if n_historical > max_historical:
        raise ValueError(f"n_historical cannot exceed template maximum of {max_historical}")
    if n_projection > len(old_proj):
        if not old_proj:
            raise ValueError(
                "n_projection cannot be extended because the template has no projection periods"
            )
        period_mode = model.time_structure.period_mode or period_mode_yearly
        while len(old_proj) < n_projection:
            next_period = shift_period(old_proj[-1], 1, period_mode)
            if next_period is None:
                next_period = old_proj[-1] + 1
            old_proj.append(int(next_period))

    new_hist = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))
    new_proj = list(range(most_recent_fy + 1, most_recent_fy + n_projection + 1))

    year_map: Dict[int, int] = {}
    year_map.update(zip(old_hist[-n_historical:] if n_historical else [], new_hist))
    year_map.update(zip(old_proj[:n_projection], new_proj))

    all_periods = new_hist + new_proj
    column_map = {
        period: index_to_col(index)
        for index, period in enumerate(all_periods, start=1)
    }
    historical_set = set(new_hist)
    projection_set = set(new_proj)
    available_periods = set(all_periods)

    model.time_structure.historical_periods = list(new_hist)
    model.time_structure.projection_periods = list(new_proj)
    model.time_structure.historical_years = list(new_hist)
    model.time_structure.projection_years = list(new_proj)
    model.time_structure.column_map = dict(column_map)
    model.time_structure.period_column_map = dict(column_map)

    for item in iter_items(model):
        if item.formula_periods is not None:
            original_formula_periods = {int(period) for period in item.formula_periods}
            spans_full_template_projection = (
                bool(template_projection_periods)
                and set(template_projection_periods).issubset(original_formula_periods)
            )
            projection_sentinel_set = (
                set(template_projection_periods[-2:])
                if len(template_projection_periods) >= 2
                else set(template_projection_periods)
            )
            # Sparse projection sentinels use the last two template projection
            # years as compact markers for "apply through the projection
            # horizon." Keep this branch conservative: it only applies to
            # non-fixed-cell items with projected formulas, excludes the dense
            # projection pattern, and carves out rolling date headers whose
            # year-specific overrides should not be naively extended.
            ends_with_projection_sentinel = (
                bool(projection_sentinel_set.intersection(original_formula_periods))
                and not spans_full_template_projection
                and item.column is None
                and item.projected is not None
                and item.id not in rolling_header_exclusions
            )
            is_period_relative_offset_scenario = (
                item.column is None
                and item.projected is not None
                and item.projected.type == formula_type_cls.valuation
                and item.projected.subtype == "offset_scenario"
                and (item.projected.params or {}).get("column_offset_mode")
                == "period_relative"
            )
            is_self_carry_forward_projection = is_self_carry_forward_projection_fn(
                item,
                template_projection_periods=template_projection_periods,
                original_formula_periods=original_formula_periods,
            )
            remapped_periods = [
                year_map[int(period)]
                for period in item.formula_periods
                if int(period) in year_map
            ]
            if (
                spans_full_template_projection
                or ends_with_projection_sentinel
                or is_period_relative_offset_scenario
                or is_self_carry_forward_projection
            ):
                remapped_periods.extend(new_proj)
            item.formula_periods = [
                period
                for period in sorted(set(remapped_periods))
                if formula_period_is_valid(
                    model,
                    item,
                    period,
                    historical_set=historical_set,
                    projection_set=projection_set,
                    available_periods=available_periods,
                )
            ]

        if item.overrides is not None:
            remapped_overrides = {
                year_map[int(period)]: spec
                for period, spec in item.overrides.items()
                if int(period) in year_map
            }
            item.overrides = remapped_overrides or None

    for item in iter_items(model):
        if item.historical is None or item.formula_periods is None:
            continue

        existing = set(item.formula_periods)
        extended = False
        for year in new_hist:
            if year in existing:
                continue
            if not formula_period_is_valid(
                model,
                item,
                year,
                historical_set=historical_set,
                projection_set=projection_set,
                available_periods=available_periods,
            ):
                continue
            item.formula_periods.append(year)
            extended = True

        if extended:
            item.formula_periods.sort()


def _is_self_carry_forward_projection(
    item: LineItem,
    *,
    template_projection_periods: Iterable[int],
    original_formula_periods: set[int],
) -> bool:
    formula_type_cls = _parent_attr("FormulaType", FormulaType)
    line_item_ref_from_obj = _parent_attr(
        "line_item_ref_from_obj",
        _line_item_ref_from_obj_fallback,
    )
    rolling_header_exclusions = _parent_attr(
        "ROLLING_HEADER_EXCLUSIONS",
        ROLLING_HEADER_EXCLUSIONS,
    )

    if (
        item.column is not None
        or item.projected is None
        or item.projected.type != formula_type_cls.ref
        or item.id in rolling_header_exclusions
    ):
        return False
    if not set(template_projection_periods).intersection(original_formula_periods):
        return False
    source_ref = line_item_ref_from_obj((item.projected.params or {}).get("source"))
    return source_ref is not None and source_ref.id == item.id and int(source_ref.t) == -1


__all__ = [
    "ROLLING_HEADER_EXCLUSIONS",
    "_is_self_carry_forward_projection",
    "remap_time_structure",
]
