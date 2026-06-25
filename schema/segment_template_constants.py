"""Template constants for segment expansion."""

from __future__ import annotations

SEGMENT_ITEM_ID_ROLES = (
    "volume_driver",
    "volume_growth",
    "price_driver",
    "price_growth",
    "revenue",
    "growth",
    "revenue_fm",
    "margin_fm",
    "growth_fm",
)

EXCLUDED_MEMBER_SUFFIXES = {
    "CorporateNonSegmentMember",
    "CorporateMember",
    "IntersegmentEliminationMember",
    "EliminationOfIntersegmentAmountsMember",
}

MATERIALITY_THRESHOLD = 0.02

_ASSUMPTIONS_SEGMENT_IDS = {
    "tpl.a.revenue_drivers.volume_driver_1",
    "tpl.a.revenue_drivers.volume_1_growth",
    "tpl.a.revenue_drivers.volume_driver_2",
    "tpl.a.revenue_drivers.volume_2_growth",
    "tpl.a.revenue_drivers.volume_driver_3",
    "tpl.a.revenue_drivers.volume_3_growth",
    "tpl.a.revenue_drivers.price_driver_1",
    "tpl.a.revenue_drivers.price_1_growth",
    "tpl.a.revenue_drivers.operating_metric",
    "tpl.a.revenue_drivers.operating_metric_growth",
    "tpl.a.revenue_drivers.business_segment_1_revenue",
    "tpl.a.revenue_drivers.business_segment_1_growth",
    "tpl.a.revenue_drivers.business_segment_2_volume_driver_1",
    "tpl.a.revenue_drivers.business_segment_2_volume_growth",
    "tpl.a.revenue_drivers.business_segment_2_price_driver_1",
    "tpl.a.revenue_drivers.business_segment_2_price_growth",
    "tpl.a.revenue_drivers.business_segment_2_revenue",
    "tpl.a.revenue_drivers.business_segment_2_growth",
}

_FM_SEGMENT_IDS = {
    "tpl.fm.income_statement.business_segment_1_revenue",
    "tpl.fm.income_statement.business_segment_2_revenue",
    "tpl.fm.margins.business_segment_1_pct_revenue",
    "tpl.fm.margins.business_segment_2_pct_revenue",
    "tpl.fm.growth_rates.business_segment_1_growth",
    "tpl.fm.growth_rates.business_segment_2_growth",
}

_ASSUMPTIONS_SEGMENT_START_ROW = 7
_ASSUMPTIONS_TOTAL_REVENUE_ROW = 27
_FM_SECTION_START_ROWS = {
    "income_statement": 5,
    "margins": 41,
    "growth_rates": 53,
}
_FM_SECTION_SHIFT_ROWS = {
    "income_statement": 7,
    "margins": 43,
    "growth_rates": 55,
}
_SCENARIO_TABLE_OLD_GROWTH_ID = "tpl.a.revenue_drivers.volume_1_growth"
_SCENARIO_TABLE_NEW_GROWTH_ID = "tpl.a.revenue_drivers.business_segment_1_growth"


def _rebuild_later_ids() -> frozenset[str]:
    """Items whose formulas reference segment IDs but are rebuilt later in the pipeline.

    Skip these during generic dangling-ref repair -- they will be fixed by
    their dedicated rebuild step.
    """

    return frozenset(
        {
            "tpl.a.revenue_drivers.total_revenue",  # rebuilt by _update_total_revenue_formulas()
            "tpl.fm.income_statement.total_revenue",  # rebuilt by _update_total_revenue_formulas()
        }
    )


_REBUILD_LATER_IDS = _rebuild_later_ids()


__all__ = [
    "EXCLUDED_MEMBER_SUFFIXES",
    "MATERIALITY_THRESHOLD",
    "SEGMENT_ITEM_ID_ROLES",
    "_ASSUMPTIONS_SEGMENT_IDS",
    "_ASSUMPTIONS_SEGMENT_START_ROW",
    "_ASSUMPTIONS_TOTAL_REVENUE_ROW",
    "_FM_SECTION_SHIFT_ROWS",
    "_FM_SECTION_START_ROWS",
    "_FM_SEGMENT_IDS",
    "_REBUILD_LATER_IDS",
    "_SCENARIO_TABLE_NEW_GROWTH_ID",
    "_SCENARIO_TABLE_OLD_GROWTH_ID",
    "_rebuild_later_ids",
]
