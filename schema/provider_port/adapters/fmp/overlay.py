"""Request-scoped FMP binding patches for filing lines."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import TypedDict, overload

from schema.provider_port.adapters.fmp.mapping import ConceptMapping
from schema.provider_port.contracts import FilingLineRef
from schema.provider_port.registry import (
    FMP_PROVIDER_ID,
    ProviderRegistry,
    require_registered_provider_id,
)


def _flag(value: object | None) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None

class _OverrideSectionsPayload(TypedDict):
    overrides: dict[str, dict[str, object]]


@dataclass(frozen=True)
class CustomBindingOverlay:
    """Filing-line-to-FMP binding patches scoped to one request."""

    entries: tuple[ConceptMapping, ...] = ()

    def resolve(self, line: FilingLineRef) -> ConceptMapping | None:
        matches = tuple(entry for entry in self.entries if entry.tag == line.tag)
        if len(matches) > 1:
            raise ValueError(f"ambiguous FMP override for filing tag {line.tag!r}")
        return matches[0] if matches else None


def overlay_from_override_rows(
    rows: list[dict[str, object]],
    *,
    provider_registry: ProviderRegistry | None = None,
) -> CustomBindingOverlay:
    """Build an FMP overlay from canonical required-role binding patches."""

    entries: list[ConceptMapping] = []
    for row in rows:
        binding = row.get("provider_binding")
        if not isinstance(binding, dict):
            continue
        provider_id = require_registered_provider_id(
            binding.get("provider_id"),
            provider_registry=provider_registry,
            entry_point="override provider binding",
        )
        if provider_id != FMP_PROVIDER_ID:
            continue
        endpoint = _optional_text(binding.get("native_endpoint"))
        native_field = _optional_text(binding.get("native_field"))
        tag = _optional_text(row.get("tag"))
        statement_role = _optional_text(row.get("statement_role"))
        if endpoint is None or native_field is None:
            raise ValueError("FMP binding patch requires native_endpoint and native_field")
        if tag is None or statement_role is None:
            raise ValueError("FMP binding patch requires tag and statement_role")
        entries.append(
            ConceptMapping(
                tag=tag,
                statement_role=statement_role,
                binding_kind="filing_line",
                native_endpoint=endpoint,
                native_field=native_field,
                fallback_native_field=_optional_text(
                    binding.get("fallback_native_field")
                ),
                treat_zero_as_missing=(
                    binding.get("missing_value_policy") == "zero_is_missing"
                ),
                negate=_flag(binding.get("negate")),
            )
        )
    return CustomBindingOverlay(entries=tuple(entries))


@overload
def canonicalize_override_payload(
    payload: _OverrideSectionsPayload,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> tuple[_OverrideSectionsPayload, bool]: ...


@overload
def canonicalize_override_payload(
    payload: dict[str, object],
    *,
    provider_registry: ProviderRegistry | None = None,
) -> tuple[dict[str, object], bool]: ...


def canonicalize_override_payload(
    payload: _OverrideSectionsPayload | dict[str, object],
    *,
    provider_registry: ProviderRegistry | None = None,
) -> tuple[_OverrideSectionsPayload | dict[str, object], bool]:
    """Validate canonical binding patches without translating retired shapes."""

    canonical = copy.deepcopy(payload)
    overrides = canonical.get("overrides")
    if not isinstance(overrides, dict):
        return canonical, False
    for required_role, entry in overrides.items():
        if not isinstance(entry, dict):
            raise ValueError(f"override {required_role!r} must be an object")
        binding = entry.get("provider_binding")
        if not isinstance(binding, dict):
            continue
        require_registered_provider_id(
            binding.get("provider_id"),
            provider_registry=provider_registry,
            entry_point="persisted override provider binding",
        )
        if _optional_text(entry.get("tag")) is None:
            raise ValueError(f"override {required_role!r} requires tag")
        if _optional_text(entry.get("statement_role")) is None:
            raise ValueError(f"override {required_role!r} requires statement_role")
        if _optional_text(binding.get("native_endpoint")) is None:
            raise ValueError(
                f"override {required_role!r} requires provider native_endpoint"
            )
        if _optional_text(binding.get("native_field")) is None:
            raise ValueError(
                f"override {required_role!r} requires provider native_field"
            )
    return canonical, False


__all__ = [
    "CustomBindingOverlay",
    "canonicalize_override_payload",
    "overlay_from_override_rows",
]
