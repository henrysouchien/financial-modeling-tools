from __future__ import annotations

"""Transitional product-axis hints for KPI catalog bridge classification.

The Edgar catalog does not yet expose ``segment_hint.axis_family``. Until that
upstream field lands, the bridge uses small per-ticker allow-lists for product
members whose XBRL tag names do not carry a generic ProductMember suffix.
Remove this shim once the upstream axis family is available in KpiObservation.
"""


PRODUCT_AXIS_MEMBER_HINTS: dict[str, frozenset[str]] = {
    "AAPL": frozenset(
        {
            "IPhoneMember",
            "MacMember",
            "IPadMember",
            "WearablesHomeandAccessoriesMember",
            "ServicesMember",
            "ServiceMember",
        }
    ),
    "COST": frozenset(),
    "GS": frozenset(),
    "JNJ": frozenset(),
    "LLY": frozenset(),
    "TGT": frozenset(),
}


def product_axis_members_for_ticker(ticker: str) -> frozenset[str]:
    return PRODUCT_AXIS_MEMBER_HINTS.get(str(ticker or "").upper(), frozenset())


__all__ = ["PRODUCT_AXIS_MEMBER_HINTS", "product_axis_members_for_ticker"]
