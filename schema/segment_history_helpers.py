"""Historical value population for segments."""

from __future__ import annotations

from typing import List

from .models import FinancialModel
from .segment_model_helpers import _set_imported_value
from .segment_profile_helpers import SegmentProfile, segment_revenue_values


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
