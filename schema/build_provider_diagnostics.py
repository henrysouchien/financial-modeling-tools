"""Provider-keyed build diagnostic compatibility helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .provider_port.registry import (
    FMP_PROVIDER_ID,
    ProviderId,
    ProviderRegistry,
    require_registered_provider_id,
)


class ValidatedProviderWarningMap(dict[ProviderId, list[dict[str, object]]]):
    """Marker for maps validated against their request's active registry."""


def validate_quality_warnings_by_provider(
    warnings_by_provider: Mapping[
        ProviderId,
        Iterable[dict[str, object]] | None,
    ],
    *,
    provider_registry: ProviderRegistry | None = None,
) -> ValidatedProviderWarningMap:
    return ValidatedProviderWarningMap(
        {
            require_registered_provider_id(
                provider_id,
                provider_registry=provider_registry,
                entry_point="quality-warning provider",
            ): list(warnings or [])
            for provider_id, warnings in warnings_by_provider.items()
        }
    )


def quality_warnings_by_provider(
    stats: Any,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> dict[ProviderId, list[dict[str, object]]]:
    """Return the neutral warning surface, accepting legacy stats as input.

    ``PopulateStats`` validates new mapping keys at construction. This helper
    only normalizes them for deterministic output; the sole synthesized key is
    the registered legacy FMP id when reading a pre-Phase-5 stats object.
    """

    raw = (
        stats.get("quality_warnings_by_provider")
        if isinstance(stats, Mapping)
        else getattr(stats, "quality_warnings_by_provider", None)
    )
    # A PopulateStats reconstructed from a pre-Phase-5 payload has the new
    # field's empty default as well as a populated legacy warning list. Treat
    # a non-empty neutral map as authoritative, but do not let that empty
    # default mask permanent legacy-input compatibility.
    if isinstance(raw, Mapping) and raw:
        # Marker subclasses are an optimization hint, not a trust boundary.
        # Revalidate at every emitted/consumed surface so a forged or mutated
        # marker cannot carry an unregistered provider ID downstream.
        validated = validate_quality_warnings_by_provider(
            raw,
            provider_registry=provider_registry,
        )
        return {
            provider_id: list(warnings or [])
            for provider_id, warnings in sorted(
                validated.items(),
                key=lambda item: item[0],
            )
            if warnings
        }

    legacy = (
        stats.get("fmp_quality_warnings")
        if isinstance(stats, Mapping)
        else getattr(stats, "fmp_quality_warnings", None)
    )
    warnings = list(legacy or [])
    if warnings:
        return {FMP_PROVIDER_ID: warnings}
    if isinstance(raw, Mapping):
        return {}
    return {}


__all__ = [
    "ValidatedProviderWarningMap",
    "quality_warnings_by_provider",
    "validate_quality_warnings_by_provider",
]
