"""In-memory cache for the upstream EDGAR tag-equivalence registry."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class FilerSpecificTag:
    """Ticker-scoped filer extension for an equivalence group."""

    ticker: str
    tag: str
    kind: str | None = None

    def matches(self, ticker: str | None) -> bool:
        return str(self.ticker).strip().upper() == str(ticker or "").strip().upper()


@dataclass(frozen=True)
class EquivalenceGroup:
    """Cached upstream registry group."""

    group_id: str
    canonical_tag: str
    tags: tuple[str, ...]
    non_equivalent_after: int | None
    text_labels: tuple[str, ...] = ()
    filer_specific: tuple[FilerSpecificTag, ...] = ()
    deprecated: bool = False
    replaced_by: str | None = None
    split_into: tuple[str, ...] = ()

    def effective_merge_candidates(self, ticker: str | None) -> tuple[str, ...]:
        if not self.tags:
            primary: tuple[str, ...] = ()
        elif self.non_equivalent_after is None:
            primary = self.tags[:1]
        else:
            primary = self.tags[: self.non_equivalent_after + 1]
        filer_specific = tuple(
            item.tag
            for item in self.filer_specific
            if item.matches(ticker)
        )
        return primary + filer_specific


@dataclass(frozen=True)
class RegistrySnapshot:
    """Loaded registry payload plus convenience indexes."""

    schema_version: int
    registry_revision: str
    groups: tuple[EquivalenceGroup, ...]
    groups_by_id: dict[str, EquivalenceGroup] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "groups_by_id",
            {group.group_id: group for group in self.groups},
        )

    def get_group(self, group_id: str) -> EquivalenceGroup | None:
        return self.groups_by_id.get(group_id)


def _parse_sequence(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return []
    return list(value)


def _parse_group(payload: Mapping[str, Any]) -> EquivalenceGroup:
    filer_specific = tuple(
        FilerSpecificTag(
            ticker=str(entry.get("ticker") or "").strip(),
            tag=str(entry.get("tag") or "").strip(),
            kind=str(entry.get("kind")).strip() if entry.get("kind") is not None else None,
        )
        for entry in _parse_sequence(payload.get("filer_specific"))
        if isinstance(entry, Mapping)
        and str(entry.get("ticker") or "").strip()
        and str(entry.get("tag") or "").strip()
    )
    non_equivalent_after = payload.get("non_equivalent_after")
    if non_equivalent_after is not None:
        try:
            non_equivalent_after = int(non_equivalent_after)
        except (TypeError, ValueError):
            non_equivalent_after = None
    return EquivalenceGroup(
        group_id=str(payload.get("id") or "").strip(),
        canonical_tag=str(payload.get("canonical") or "").strip(),
        tags=tuple(
            str(tag).strip()
            for tag in _parse_sequence(payload.get("tags"))
            if str(tag).strip()
        ),
        non_equivalent_after=non_equivalent_after,
        text_labels=tuple(
            str(label).strip()
            for label in _parse_sequence(payload.get("text_labels"))
            if str(label).strip()
        ),
        filer_specific=filer_specific,
        deprecated=bool(payload.get("deprecated")),
        replaced_by=str(payload.get("replaced_by")).strip()
        if payload.get("replaced_by") is not None
        else None,
        split_into=tuple(
            str(group_id).strip()
            for group_id in _parse_sequence(payload.get("split_into"))
            if str(group_id).strip()
        ),
    )


def snapshot_from_payload(payload: Mapping[str, Any]) -> RegistrySnapshot:
    groups_payload = _parse_sequence(payload.get("groups"))
    groups = tuple(
        _parse_group(group_payload)
        for group_payload in groups_payload
        if isinstance(group_payload, Mapping)
        and str(group_payload.get("id") or "").strip()
    )
    return RegistrySnapshot(
        schema_version=int(payload.get("schema_version") or 1),
        registry_revision=str(payload.get("registry_revision") or "").strip(),
        groups=groups,
    )


class RegistryCache:
    """Thread-safe in-memory registry cache with conditional refresh support."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        opener=None,
        retry_backoff_seconds: float = 60.0,
        clock=None,
    ) -> None:
        root = (base_url or os.getenv("EDGAR_API_URL", "https://www.edgarparser.com")).rstrip("/")
        self._url = f"{root}/api/tag_equivalence"
        self._api_key = os.getenv("EDGAR_API_KEY", "").strip() if api_key is None else api_key
        self._opener = opener or urllib.request.urlopen
        self._retry_backoff_seconds = float(retry_backoff_seconds)
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._snapshot: RegistrySnapshot | None = None
        self._etag: str | None = None
        self._last_error: Exception | None = None
        self._next_refresh_at = 0.0
        self._initialized = False

    @property
    def registry_revision(self) -> str | None:
        return self._snapshot.registry_revision if self._snapshot is not None else None

    @property
    def last_error(self) -> Exception | None:
        return self._last_error

    @property
    def snapshot(self) -> RegistrySnapshot | None:
        self.ensure_loaded()
        return self._snapshot

    def ensure_loaded(self) -> None:
        with self._lock:
            if self._initialized:
                return
            self._initialized = True
        self.refresh()

    def get_group(self, group_id: str) -> EquivalenceGroup | None:
        self.ensure_loaded()
        snapshot = self._snapshot
        if snapshot is None:
            return None
        return snapshot.get_group(group_id)

    def refresh(self) -> bool:
        with self._lock:
            if self._last_error is not None and self._clock() < self._next_refresh_at:
                return False

            headers = {"User-Agent": "schema-registry-cache"}
            if self._etag:
                headers["If-None-Match"] = self._etag

            params = {}
            if self._api_key:
                params["key"] = self._api_key
            url = self._url
            if params:
                url = f"{url}?{urllib.parse.urlencode(params)}"

            request = urllib.request.Request(url, headers=headers)
            try:
                with self._opener(request, timeout=120) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                    if not isinstance(payload, Mapping):
                        raise ValueError("Tag-equivalence registry response was not a JSON object")
                    snapshot = snapshot_from_payload(payload)
                    etag = response.headers.get("ETag")
            except urllib.error.HTTPError as exc:
                if exc.code == 304:
                    self._last_error = None
                    self._next_refresh_at = 0.0
                    return False
                self._record_refresh_error_locked(exc)
                return False
            except Exception as exc:
                self._record_refresh_error_locked(exc)
                return False

            self._snapshot = snapshot
            self._etag = etag or f'"{snapshot.registry_revision}"'
            self._last_error = None
            self._next_refresh_at = 0.0
            self._initialized = True
            return True

    def _record_refresh_error_locked(self, exc: Exception) -> None:
        self._last_error = exc
        self._next_refresh_at = self._clock() + self._retry_backoff_seconds
        logging.error(
            "Tag-equivalence registry refresh failed: %s%s Suppressing registry refresh "
            "retries for %.0fs.",
            exc,
            self._error_hint(exc),
            self._retry_backoff_seconds,
        )

    def _error_hint(self, exc: Exception) -> str:
        if isinstance(exc, urllib.error.HTTPError) and exc.code == 429:
            if self._api_key:
                return " Check EDGAR_API_KEY quota/tier or retry after the rate-limit window."
            return (
                " Set EDGAR_API_KEY for registered/paid access, or use a "
                "test registry cache for local fixture runs."
            )
        return ""


_REGISTRY_CACHE: RegistryCache | None = None
_REGISTRY_CACHE_LOCK = threading.Lock()


def get_registry_cache() -> RegistryCache:
    global _REGISTRY_CACHE
    with _REGISTRY_CACHE_LOCK:
        if _REGISTRY_CACHE is None:
            _REGISTRY_CACHE = RegistryCache()
        return _REGISTRY_CACHE


def reset_registry_cache() -> None:
    global _REGISTRY_CACHE
    with _REGISTRY_CACHE_LOCK:
        _REGISTRY_CACHE = None
