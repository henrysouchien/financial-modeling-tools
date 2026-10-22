"""In-memory cache for the upstream EDGAR tag-equivalence registry."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping


_REVISION_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RESPONSE_FIELDS = frozenset({"schema_version", "registry_revision", "groups"})
_GROUP_FIELDS = frozenset(
    {
        "id",
        "canonical",
        "tags",
        "non_equivalent_after",
        "text_labels",
        "filer_specific",
        "deprecated",
        "replaced_by",
        "split_into",
    }
)
_GROUP_REQUIRED_FIELDS = frozenset(
    {
        "id",
        "canonical",
        "tags",
        "non_equivalent_after",
        "text_labels",
        "filer_specific",
        "deprecated",
    }
)
_FILER_SPECIFIC_FIELDS = frozenset({"ticker", "tag", "kind"})


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


def _expect_list(value: Any, *, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return list(value)


def _plain_string(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(
            f"{field_name} must be a non-empty string without surrounding whitespace"
        )
    return value


def _reject_unknown_fields(
    payload: Mapping[str, Any],
    *,
    allowed: frozenset[str],
    field_name: str,
) -> None:
    unknown = sorted(str(field) for field in set(payload) - allowed)
    if unknown:
        raise ValueError(
            f"{field_name} contains unknown field(s): {', '.join(unknown)}"
        )


def _normalize_local_name(tag: str) -> str:
    return tag.split(":", 1)[-1].lower()


def _parse_filer_specific(
    value: Any,
    *,
    group_id: str,
) -> tuple[FilerSpecificTag, ...]:
    entries = _expect_list(value, field_name=f"Group '{group_id}' filer_specific")
    parsed: list[FilerSpecificTag] = []
    for index, entry in enumerate(entries):
        field_name = f"Group '{group_id}' filer_specific[{index}]"
        if not isinstance(entry, Mapping):
            raise ValueError(f"{field_name} must be an object")
        _reject_unknown_fields(
            entry,
            allowed=_FILER_SPECIFIC_FIELDS,
            field_name=field_name,
        )
        ticker = _plain_string(entry.get("ticker"), field_name=f"{field_name}.ticker")
        if ticker != ticker.upper():
            raise ValueError(f"{field_name}.ticker must be uppercase")
        tag = _plain_string(entry.get("tag"), field_name=f"{field_name}.tag")
        kind = entry.get("kind")
        if kind is not None:
            kind = _plain_string(kind, field_name=f"{field_name}.kind")
        parsed.append(FilerSpecificTag(ticker=ticker, tag=tag, kind=kind))
    return tuple(parsed)


def _parse_group(payload: Mapping[str, Any], *, index: int) -> EquivalenceGroup:
    field_name = f"groups[{index}]"
    _reject_unknown_fields(payload, allowed=_GROUP_FIELDS, field_name=field_name)
    missing = sorted(_GROUP_REQUIRED_FIELDS - set(payload))
    if missing:
        raise ValueError(
            f"{field_name} missing required field(s): {', '.join(missing)}"
        )

    group_id = _plain_string(payload.get("id"), field_name=f"{field_name}.id")
    raw_tags = _expect_list(payload.get("tags"), field_name=f"Group '{group_id}' tags")
    if not raw_tags:
        raise ValueError(f"Group '{group_id}' must define at least one tag")
    tags = tuple(
        _plain_string(tag, field_name=f"Group '{group_id}' tags[{tag_index}]")
        for tag_index, tag in enumerate(raw_tags)
    )

    non_equivalent_after = payload.get("non_equivalent_after")

    raw_labels = _expect_list(
        payload.get("text_labels"),
        field_name=f"Group '{group_id}' text_labels",
    )
    text_labels = tuple(
        _plain_string(
            label,
            field_name=f"Group '{group_id}' text_labels[{label_index}]",
        )
        for label_index, label in enumerate(raw_labels)
    )
    deprecated = payload.get("deprecated")
    if not isinstance(deprecated, bool):
        raise ValueError(f"Group '{group_id}' deprecated must be a boolean")
    replaced_by = payload.get("replaced_by")
    if replaced_by is not None:
        replaced_by = _plain_string(
            replaced_by,
            field_name=f"Group '{group_id}' replaced_by",
        )
    raw_split_into = _expect_list(
        payload.get("split_into", []),
        field_name=f"Group '{group_id}' split_into",
    )
    split_into = tuple(
        _plain_string(
            target,
            field_name=f"Group '{group_id}' split_into[{target_index}]",
        )
        for target_index, target in enumerate(raw_split_into)
    )

    return EquivalenceGroup(
        group_id=group_id,
        canonical_tag=_plain_string(
            payload.get("canonical"),
            field_name=f"Group '{group_id}' canonical",
        ),
        tags=tags,
        non_equivalent_after=non_equivalent_after,
        text_labels=text_labels,
        filer_specific=_parse_filer_specific(
            payload.get("filer_specific"),
            group_id=group_id,
        ),
        deprecated=deprecated,
        replaced_by=replaced_by,
        split_into=split_into,
    )


def _validate_groups(groups: tuple[EquivalenceGroup, ...]) -> None:
    groups_by_id = {group.group_id: group for group in groups}
    if len(groups_by_id) != len(groups):
        raise ValueError("Tag-equivalence registry contains duplicate group ids")

    seen_active_tags: dict[str, str] = {}
    seen_filer_specific: dict[tuple[str, str], str] = {}
    for group in groups:
        if group.non_equivalent_after is not None and not (
            0 <= group.non_equivalent_after < len(group.tags)
        ):
            raise ValueError(
                f"Group '{group.group_id}' non_equivalent_after must be between 0 "
                f"and {len(group.tags) - 1}"
            )
        if not group.canonical_tag.lower().startswith("us-gaap:"):
            raise ValueError(
                f"Group '{group.group_id}' canonical must be a us-gaap tag"
            )
        if group.canonical_tag not in group.tags:
            raise ValueError(
                f"Group '{group.group_id}' canonical must appear in tags"
            )
        normalized_tags = [_normalize_local_name(tag) for tag in group.tags]
        if len(normalized_tags) != len(set(normalized_tags)):
            raise ValueError(
                f"Group '{group.group_id}' contains duplicate normalized tags"
            )

        has_replacement = group.replaced_by is not None
        has_split = bool(group.split_into)
        if group.deprecated:
            if has_replacement == has_split:
                raise ValueError(
                    f"Deprecated group '{group.group_id}' must define exactly one "
                    "of replaced_by or split_into"
                )
        elif has_replacement or has_split:
            raise ValueError(
                f"Non-deprecated group '{group.group_id}' cannot define "
                "replaced_by or split_into"
            )
        if len(group.split_into) != len(set(group.split_into)):
            raise ValueError(
                f"Deprecated split group '{group.group_id}' contains duplicate targets"
            )

        if not group.deprecated:
            for tag in group.tags:
                normalized = _normalize_local_name(tag)
                owner = seen_active_tags.get(normalized)
                if owner is not None:
                    raise ValueError(
                        f"Tag '{tag}' appears in multiple non-deprecated groups: "
                        f"'{owner}' and '{group.group_id}'"
                    )
                seen_active_tags[normalized] = group.group_id

        for item in group.filer_specific:
            key = (item.ticker, _normalize_local_name(item.tag))
            owner = seen_filer_specific.get(key)
            if owner is not None:
                raise ValueError(
                    f"filer_specific '{item.ticker}:{item.tag}' appears in multiple "
                    f"groups: '{owner}' and '{group.group_id}'"
                )
            seen_filer_specific[key] = group.group_id

    for group in groups:
        if group.replaced_by is not None:
            target = groups_by_id.get(group.replaced_by)
            if target is None or target.deprecated:
                raise ValueError(
                    f"Deprecated rename group '{group.group_id}' references missing "
                    f"or deprecated target '{group.replaced_by}'"
                )
        for target_id in group.split_into:
            target = groups_by_id.get(target_id)
            if target is None or target.deprecated:
                raise ValueError(
                    f"Deprecated split group '{group.group_id}' references missing "
                    f"or deprecated target '{target_id}'"
                )


def snapshot_from_payload(payload: Mapping[str, Any]) -> RegistrySnapshot:
    if not isinstance(payload, Mapping):
        raise ValueError("Tag-equivalence registry response must be an object")
    _reject_unknown_fields(
        payload,
        allowed=_RESPONSE_FIELDS,
        field_name="Tag-equivalence registry response",
    )
    missing = sorted(_RESPONSE_FIELDS - set(payload))
    if missing:
        raise ValueError(
            "Tag-equivalence registry response missing required field(s): "
            + ", ".join(missing)
        )
    registry_revision = payload.get("registry_revision")
    if not isinstance(registry_revision, str) or not _REVISION_PATTERN.fullmatch(
        registry_revision
    ):
        raise ValueError(
            "Tag-equivalence registry_revision must be lowercase sha256:<64hex>"
        )
    groups_payload = _expect_list(payload.get("groups"), field_name="groups")
    if not groups_payload:
        raise ValueError("groups must contain at least one group")
    groups: list[EquivalenceGroup] = []
    for index, group_payload in enumerate(groups_payload):
        if not isinstance(group_payload, Mapping):
            raise ValueError(f"groups[{index}] must be an object")
        groups.append(_parse_group(group_payload, index=index))
    parsed_groups = tuple(groups)
    _validate_groups(parsed_groups)
    return RegistrySnapshot(
        schema_version=payload["schema_version"],
        registry_revision=registry_revision,
        groups=parsed_groups,
    )


def _reject_duplicate_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"Duplicate JSON object key '{key}'")
        payload[key] = value
    return payload


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
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"
            if self._etag:
                headers["If-None-Match"] = self._etag

            request = urllib.request.Request(self._url, headers=headers)
            try:
                with self._opener(request, timeout=120) as response:
                    payload = json.loads(
                        response.read().decode("utf-8"),
                        object_pairs_hook=_reject_duplicate_object_keys,
                    )
                    if not isinstance(payload, Mapping):
                        raise ValueError("Tag-equivalence registry response was not a JSON object")
                    snapshot = snapshot_from_payload(payload)
                    etag = response.headers.get("ETag")
                    expected_etag = f'"{snapshot.registry_revision}"'
                    if etag != expected_etag:
                        raise ValueError(
                            "Tag-equivalence registry ETag did not match registry_revision"
                        )
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
            self._etag = etag
            self._last_error = None
            self._next_refresh_at = 0.0
            self._initialized = True
            return True

    def _record_refresh_error_locked(self, exc: Exception) -> None:
        self._last_error = RuntimeError("Tag-equivalence registry refresh failed")
        self._next_refresh_at = self._clock() + self._retry_backoff_seconds
        logging.error(
            "Tag-equivalence registry refresh failed | exception_type=%s.%s Suppressing "
            "registry refresh retries for %.0fs.",
            type(exc).__name__,
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
