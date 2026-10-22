"""Neutral input contract for cross-source validation diagnostics."""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import Any

from pydantic import ValidationInfo, model_validator

from .provider_port.registry import (
    ProviderId,
    ProviderRegistry,
    inject_provider_registry_validation_context,
    require_registered_provider_id,
)


@dataclass
class ValidationInput:
    fmp_buffer_by_role: dict[str, Any] = field(default_factory=dict)
    edgar_buffer_by_role: dict[str, Any] = field(default_factory=dict)
    opted_in_roles: list[str] = field(default_factory=list)
    tolerance_pct_by_role: dict[str, float] = field(default_factory=dict)
    historical_years: list[int] = field(default_factory=list)
    served_source_by_role_year: dict[str, dict[int, ProviderId]] = field(
        default_factory=dict
    )
    provider_registry: InitVar[object] = None

    @model_validator(mode="before")
    @classmethod
    def _inject_registry_context(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        return inject_provider_registry_validation_context(cls, value, info)

    def __post_init__(self, provider_registry: object) -> None:
        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        self.served_source_by_role_year = {
            required_role: {
                int(year): require_registered_provider_id(
                    provider_id,
                    provider_registry=registry,
                    entry_point=(
                        f"cross-source validation served provider for role "
                        f"{required_role!r} year {year}"
                    ),
                )
                for year, provider_id in served_by_year.items()
            }
            for required_role, served_by_year in self.served_source_by_role_year.items()
        }
