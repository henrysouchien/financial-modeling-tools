"""The thesis's view of its ModelVersion: model-owned quantities read, never stored.

A ``Thesis`` stores no price target. The price target a thesis shows is the
canonical ``PriceTarget`` bound to the selected ModelVersion
(``schema/price_target.py``); ``thesis_price_target_view`` is the one read that
projects it onto the thesis wire. The caller resolves the PriceTarget for that
version (``research.current_state``) and passes its read status and reason with
it; this module neither fetches nor caches it.
"""

from __future__ import annotations

from typing import Any

from .price_target import PriceTarget


def thesis_price_target_view(
    read_status: str,
    price_target: PriceTarget | None,
    read_reason: str | None = None,
) -> dict[str, Any]:
    """Thesis wire keys for the PriceTarget bound to the selected ModelVersion.

    ``read_status`` is the current-state price-target channel status
    (``present``/``absent``/``invalid``/``unavailable``/``not_evaluable``),
    ``price_target`` is that channel's value (present exactly when the status
    is ``present``), and ``read_reason`` is ``PriceTargetRead.reason``
    (``read_failed``/``source_timestamp_future``/``store_unavailable``/``None``).
    A carried row is the predecessor version's conclusion, not an acceptance on
    the current version's compiled values; the reader tells them apart by
    ``carried_from_price_target_id``.
    """

    if price_target is None:
        return {
            "price_target": None,
            "price_target_status": "missing" if read_status == "absent" else read_status,
            "price_target_reason": read_reason,
            "carried_from_price_target_id": None,
        }
    return {
        "price_target": price_target.model_dump(mode="json"),
        "price_target_status": "current",
        "price_target_reason": None,
        "carried_from_price_target_id": price_target.carried_from_price_target_id,
    }


__all__ = ["thesis_price_target_view"]
