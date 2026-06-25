"""EDGAR registry validation helpers for schema build orchestration."""

from __future__ import annotations

import logging
import sys
from typing import Any

from .models import DataSourceMapping
from .registry_cache import EquivalenceGroup, get_registry_cache as _get_registry_cache_fallback
from .source_values import SourceValue


_SEEN_DEPRECATED_REGISTRY_GROUPS: set[str] = set()


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        from . import build as parent
    return getattr(parent, name, fallback)


def _edgar_negate_enabled(concept: DataSourceMapping) -> bool:
    if concept.edgar_negate is not None:
        return bool(concept.edgar_negate)
    return bool(concept.negate)


def _tags_equivalent(requested: str, returned: str) -> bool:
    def norm(t: str) -> str:
        return t.split(":", 1)[-1].lower()

    return norm(requested) == norm(returned)


def _log_deprecated_registry_group_once(requested_group_id: str, replacement_group_id: str) -> None:
    seen_deprecated_groups = _parent_attr(
        "_SEEN_DEPRECATED_REGISTRY_GROUPS",
        _SEEN_DEPRECATED_REGISTRY_GROUPS,
    )
    key = f"{requested_group_id}->{replacement_group_id}"
    if key in seen_deprecated_groups:
        return
    seen_deprecated_groups.add(key)
    logging.warning(
        "Registry group '%s' is deprecated; update taxonomy to '%s'",
        requested_group_id,
        replacement_group_id,
    )


def _resolve_registry_response_group(
    *,
    ticker: str,
    requested_group_id: str,
    returned_group_id: str | None,
    requested_group: EquivalenceGroup | None,
    allow_refresh: bool,
) -> EquivalenceGroup | None:
    get_registry_cache = _parent_attr("get_registry_cache", _get_registry_cache_fallback)
    log_deprecated_registry_group_once = _parent_attr(
        "_log_deprecated_registry_group_once",
        _log_deprecated_registry_group_once,
    )
    cache = get_registry_cache()
    current_requested_group = requested_group or cache.get_group(requested_group_id)
    replacement_group_id = (
        current_requested_group.replaced_by
        if current_requested_group is not None
        else None
    )
    if returned_group_id == requested_group_id and current_requested_group is not None:
        return current_requested_group
    if (
        replacement_group_id
        and returned_group_id == replacement_group_id
    ):
        replacement_group = cache.get_group(replacement_group_id)
        if replacement_group is not None:
            log_deprecated_registry_group_once(requested_group_id, replacement_group_id)
            return replacement_group

    if allow_refresh:
        cache.refresh()
        refreshed_requested_group = cache.get_group(requested_group_id)
        return _resolve_registry_response_group(
            ticker=ticker,
            requested_group_id=requested_group_id,
            returned_group_id=returned_group_id,
            requested_group=refreshed_requested_group,
            allow_refresh=False,
        )

    logging.warning(
        "edgar_registry_group_mismatch ticker=%s requested=%s returned=%s",
        ticker,
        requested_group_id,
        returned_group_id,
    )
    return None


def _metric_tag_matches_group(
    returned_tag: str,
    *,
    group: EquivalenceGroup,
    ticker: str,
) -> bool:
    tags_equivalent = _parent_attr("_tags_equivalent", _tags_equivalent)
    return any(
        tags_equivalent(candidate, returned_tag)
        for candidate in group.effective_merge_candidates(ticker)
    )


def _equivalent_tag_validated_by_registry(
    *,
    ticker: str,
    requested_tag: str | None,
    returned_tag: str,
    returned_group_id: str | None,
) -> bool:
    if not requested_tag or not returned_group_id:
        return False
    get_registry_cache = _parent_attr("get_registry_cache", _get_registry_cache_fallback)
    metric_tag_matches_group = _parent_attr(
        "_metric_tag_matches_group",
        _metric_tag_matches_group,
    )
    cache = get_registry_cache()
    group = cache.get_group(returned_group_id)
    if group is None:
        cache.refresh()
        group = cache.get_group(returned_group_id)
    if group is None:
        return False
    return metric_tag_matches_group(
        requested_tag,
        group=group,
        ticker=ticker,
    ) and metric_tag_matches_group(
        returned_tag,
        group=group,
        ticker=ticker,
    )


def _validated_registry_group_for_metric_tag(
    *,
    ticker: str,
    requested_group_id: str,
    returned_group_id: str | None,
    returned_tag: str,
    requested_group: EquivalenceGroup | None,
) -> EquivalenceGroup | None:
    resolve_registry_response_group = _parent_attr(
        "_resolve_registry_response_group",
        _resolve_registry_response_group,
    )
    metric_tag_matches_group = _parent_attr(
        "_metric_tag_matches_group",
        _metric_tag_matches_group,
    )
    get_registry_cache = _parent_attr("get_registry_cache", _get_registry_cache_fallback)
    matched_group = resolve_registry_response_group(
        ticker=ticker,
        requested_group_id=requested_group_id,
        returned_group_id=returned_group_id,
        requested_group=requested_group,
        allow_refresh=False,
    )
    if matched_group is not None and metric_tag_matches_group(
        returned_tag,
        group=matched_group,
        ticker=ticker,
    ):
        return matched_group

    cache = get_registry_cache()
    cache.refresh()
    refreshed_requested_group = cache.get_group(requested_group_id)
    matched_group = resolve_registry_response_group(
        ticker=ticker,
        requested_group_id=requested_group_id,
        returned_group_id=returned_group_id,
        requested_group=refreshed_requested_group,
        allow_refresh=False,
    )
    if matched_group is not None and metric_tag_matches_group(
        returned_tag,
        group=matched_group,
        ticker=ticker,
    ):
        return matched_group

    logging.warning(
        "edgar_registry_tag_mismatch ticker=%s requested=%s returned_group=%s returned_tag=%s",
        ticker,
        requested_group_id,
        returned_group_id,
        returned_tag,
    )
    return None


def _registry_equivalence_value_conflict(
    existing: SourceValue,
    incoming: SourceValue,
) -> bool:
    if existing.normalized_value is None or incoming.normalized_value is None:
        return False
    tags_equivalent = _parent_attr("_tags_equivalent", _tags_equivalent)
    if tags_equivalent(existing.tag or "", incoming.tag or ""):
        return False
    existing_value = float(existing.normalized_value)
    incoming_value = float(incoming.normalized_value)
    tolerance = max(
        _parent_attr("_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M", 0.1),
        max(abs(existing_value), abs(incoming_value))
        * _parent_attr("_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT", 0.01),
    )
    return abs(existing_value - incoming_value) > tolerance


__all__ = [
    "_SEEN_DEPRECATED_REGISTRY_GROUPS",
    "_edgar_negate_enabled",
    "_equivalent_tag_validated_by_registry",
    "_log_deprecated_registry_group_once",
    "_metric_tag_matches_group",
    "_registry_equivalence_value_conflict",
    "_resolve_registry_response_group",
    "_tags_equivalent",
    "_validated_registry_group_for_metric_tag",
]
