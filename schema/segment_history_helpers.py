"""Historical value population and formula-period pruning for segments."""

from __future__ import annotations

from typing import List

from .models import FinancialModel
from .segment_model_helpers import _set_imported_value
from .segment_profile_helpers import SegmentProfile, segment_revenue_values
from .segment_template_constants import SEGMENT_ITEM_ID_ROLES


def populate_segment_historicals(
    model: FinancialModel,
    profile: SegmentProfile,
    historical_periods: List[int],
) -> int:
    """Write discovered segment revenue values into FM segment revenue rows."""

    if not model._index:
        model.build_index()

    periods = {int(period) for period in historical_periods}
    writes = 0
    for segment in profile.segments:
        revenue_values = segment_revenue_values(segment)
        if not segment.item_ids or not revenue_values:
            continue
        item = model.get_item(segment.item_ids["revenue_fm"])
        for year, value in revenue_values.items():
            if int(year) not in periods:
                continue
            _set_imported_value(item, int(year), float(value))
            writes += 1
    return writes


def prune_segment_formula_periods(model: FinancialModel, profile: SegmentProfile) -> None:
    """Prune segment-derived formulas for partial historical coverage."""

    if not profile.segments or not model._index:
        if not model._index:
            model.build_index()
        if not profile.segments:
            return

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not historical_periods:
        return

    last_historical = historical_periods[-1]
    all_segment_roles = list(SEGMENT_ITEM_ID_ROLES)

    for segment in profile.segments:
        revenue_values = segment_revenue_values(segment)
        if not segment.item_ids or not revenue_values:
            continue

        available_years = {int(year) for year in revenue_values}
        has_projection_base = last_historical in available_years

        for role in all_segment_roles:
            item_id = segment.item_ids.get(role)
            if not item_id:
                continue
            item = model.get_item(item_id)
            if item.formula_periods is None:
                continue

            kept: List[int] = []
            for period in list(item.formula_periods):
                year = int(period)
                if year in projection_periods:
                    if has_projection_base:
                        kept.append(year)
                    continue

                if role in {"margin_fm", "revenue"}:
                    if year in available_years:
                        kept.append(year)
                    continue

                if role in {"growth", "growth_fm"}:
                    if year in available_years and (year - 1) in available_years:
                        kept.append(year)
                    continue

                kept.append(year)

            item.formula_periods = kept
