from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


_FROZEN = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
    populate_by_name=True,
    frozen=True,
)
_ID_PATTERN = r"^[a-z][a-z0-9_]{1,30}$"
_NORMALIZE_RE = re.compile(r"[\s\-_]+")


def _normalize(value: str) -> str:
    return _NORMALIZE_RE.sub("_", value.strip().lower())


class StrategyCategory(BaseModel):
    id: str = Field(pattern=_ID_PATTERN)
    display_name: str = Field(min_length=1)
    aliases: list[str] = Field(default_factory=list)
    description: str = ""
    is_builtin: bool = False

    model_config = _FROZEN


class StrategyCategoryRegistry:
    """Registry for canonical strategy category IDs and accepted aliases."""

    def __init__(self) -> None:
        self._by_id: dict[str, StrategyCategory] = {}
        self._alias_to_id: dict[str, str] = {}
        self._load_defaults()
        self._load_user_overrides()

    def _load_defaults(self) -> None:
        path = Path(__file__).parent / "defaults.yaml"
        for cat in self._parse(path, mark_builtin=True):
            self._add(cat, builtin=True)

    def _load_user_overrides(self) -> None:
        env_path = os.environ.get("STRATEGY_CATEGORY_REGISTRY_PATH")
        if not env_path:
            return
        path = Path(env_path)
        if not path.exists():
            raise FileNotFoundError(f"Strategy category registry not found: {path}")
        for cat in self._parse(path, mark_builtin=False):
            if cat.id in self._by_id and self._by_id[cat.id].is_builtin:
                raise ValueError(f"User registry cannot redefine built-in category {cat.id!r}")
            self._add(cat, builtin=False)

    def _parse(self, path: Path, *, mark_builtin: bool) -> list[StrategyCategory]:
        with path.open("r", encoding="utf-8") as fh:
            data: Any = yaml.safe_load(fh) or {}
        categories = data.get("categories", [])
        if not isinstance(categories, list):
            raise ValueError(f"{path} must contain a categories list")
        return [
            StrategyCategory.model_validate(category).model_copy(
                update={"is_builtin": mark_builtin},
            )
            for category in categories
        ]

    def _add(self, cat: StrategyCategory, *, builtin: bool) -> None:
        if cat.id in self._by_id:
            raise ValueError(f"Duplicate category id {cat.id!r}")

        canonical_normalized = _normalize(cat.id)
        existing_owner = self._alias_to_id.get(canonical_normalized)
        if existing_owner is not None and existing_owner != cat.id:
            raise ValueError(
                f"Category id {cat.id!r} (normalized {canonical_normalized!r}) "
                f"collides with existing alias of category {existing_owner!r}"
            )

        seen_alias_normalized: set[str] = set()
        for alias in cat.aliases:
            normalized = _normalize(alias)

            existing = self._alias_to_id.get(normalized)
            if existing is not None and existing != cat.id:
                raise ValueError(
                    f"Alias {alias!r} collides between {existing!r} and {cat.id!r}"
                )

            normalized_canonical_owner = next(
                (cid for cid in self._by_id if _normalize(cid) == normalized),
                None,
            )
            if normalized_canonical_owner is not None and normalized_canonical_owner != cat.id:
                raise ValueError(
                    f"Alias {alias!r} (normalized {normalized!r}) collides with canonical id "
                    f"of category {normalized_canonical_owner!r}"
                )

            if normalized == canonical_normalized:
                continue

            if normalized in seen_alias_normalized:
                raise ValueError(
                    f"Alias {alias!r} (normalized {normalized!r}) appears more than once "
                    f"for category {cat.id!r}"
                )
            seen_alias_normalized.add(normalized)

        cat = cat.model_copy(update={"is_builtin": builtin})
        self._by_id[cat.id] = cat
        self._alias_to_id[canonical_normalized] = cat.id
        for alias in cat.aliases:
            self._alias_to_id[_normalize(alias)] = cat.id

    def is_registered(self, category_id: str) -> bool:
        return category_id in self._by_id

    def canonicalize(self, value: str) -> str | None:
        return self._alias_to_id.get(_normalize(value))

    def all_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_id))

    def builtin_ids(self) -> tuple[str, ...]:
        return tuple(sorted(cat.id for cat in self._by_id.values() if cat.is_builtin))

    def get(self, category_id: str) -> StrategyCategory | None:
        return self._by_id.get(category_id)


STRATEGY_REGISTRY = StrategyCategoryRegistry()


def validate_strategy_category_id(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"strategy category must be a string, got {type(value).__name__}")
    if STRATEGY_REGISTRY.is_registered(value):
        return value
    canonical = STRATEGY_REGISTRY.canonicalize(value)
    if canonical is None:
        valid = ", ".join(STRATEGY_REGISTRY.all_ids())
        raise ValueError(f"unknown strategy category {value!r}; valid: {valid}")
    return canonical


__all__ = [
    "STRATEGY_REGISTRY",
    "StrategyCategory",
    "StrategyCategoryRegistry",
    "validate_strategy_category_id",
]
