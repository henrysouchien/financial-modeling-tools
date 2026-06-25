"""Utility helpers for :mod:`schema.build_edgar_fetch`."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .build import EdgarConceptFetchResult, EdgarFetcher


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        from . import build as parent
    return getattr(parent, name, fallback)


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build helper '{name}' is unavailable")
    return helper


def _edgar_tag_lookup_candidates(tags: list[str]) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        raw = str(tag or "").strip()
        if not raw:
            continue
        for candidate in (raw, raw.rsplit(":", 1)[-1]):
            if candidate and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
    return candidates


def _call_edgar_metric_fetcher(
    edgar_fetcher: EdgarFetcher,
    ticker: str,
    metric_name: str,
    most_recent_fy: int,
    n_historical: int,
    *,
    include_equivalents: bool = False,
    axis_key: str | None = None,
) -> dict:
    kwargs: dict[str, object] = {}
    if include_equivalents:
        kwargs["include_equivalents"] = True
    if axis_key is not None:
        kwargs["axis_key"] = axis_key
    try:
        return edgar_fetcher(ticker, metric_name, most_recent_fy, n_historical, **kwargs) or {}
    except TypeError:
        try:
            if include_equivalents and axis_key is not None:
                return edgar_fetcher(
                    ticker,
                    metric_name,
                    most_recent_fy,
                    n_historical,
                    axis_key=axis_key,
                ) or {}
            return edgar_fetcher(ticker, metric_name, most_recent_fy, n_historical) or {}
        except Exception as exc:
            logging.warning("EDGAR fetch failed for metric '%s': %s", metric_name, exc)
            return {
                "status": "error",
                "periods_failed": n_historical,
                "error": str(exc),
            }
    except Exception as exc:
        logging.warning("EDGAR fetch failed for metric '%s': %s", metric_name, exc)
        return {
            "status": "error",
            "periods_failed": n_historical,
            "error": str(exc),
        }


def _edgar_fetch_error_message(response: dict) -> str | None:
    for key in ("error", "message", "detail", "reason"):
        value = response.get(key)
        if value:
            return str(value)
    return None


def _requested_years_for_fetch(most_recent_fy: int, n_historical: int) -> set[int]:
    return set(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))


def _registry_failed_result(
    requested_years: set[int],
    *,
    api_calls: int,
    error_message: str | None = None,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    return edgar_concept_fetch_result(
        values_dict={},
        failed_years=set(requested_years),
        status="failed",
        periods_failed=len(requested_years),
        api_calls=api_calls,
        error_message=error_message,
    )


__all__ = [
    "_call_edgar_metric_fetcher",
    "_edgar_fetch_error_message",
    "_edgar_tag_lookup_candidates",
    "_registry_failed_result",
    "_requested_years_for_fetch",
]
