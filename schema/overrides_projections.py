"""Pydantic contract for durable projection overrides."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import Field, RootModel, field_validator, model_validator

from schema.thesis_shared_slice import SourceRecord, _ContractModel


_YEAR_RE = re.compile(r"^\d{4}$")
ProjectionValueScale = Literal["display", "model"]


class Provenance(_ContractModel):
    source_skill: str
    skill_run_id: str
    written_at: str
    model_build_id: str | None = None


class ScenarioEntry(_ContractModel):
    values: dict[str, float]
    value_scale: ProjectionValueScale = "display"
    rationale: str
    confidence: Literal["low", "medium", "high"]
    held_at_base: bool = False
    disclosed_guidance_horizon: int | None = None
    extension_basis_by_period: dict[str, str | None] | None = None
    source_refs_by_period: dict[str, list[str]] | None = None
    sources: list[SourceRecord] = Field(default_factory=list)
    provenance: Provenance

    @field_validator("values")
    @classmethod
    def _validate_year_keys(cls, value: dict[str, float]) -> dict[str, float]:
        for key in value:
            if _YEAR_RE.fullmatch(key) is None:
                raise ValueError(f"year key must be 4-digit FY, got {key!r}")
        return value

    @model_validator(mode="after")
    def _validate_period_consistency(self) -> "ScenarioEntry":
        value_years = set(self.values)

        if self.source_refs_by_period:
            ref_years = set(self.source_refs_by_period)
            extra = ref_years - value_years
            if extra:
                raise ValueError(
                    f"source_refs_by_period has years not in values: {sorted(extra)}"
                )

        if self.extension_basis_by_period:
            ext_years = set(self.extension_basis_by_period)
            extra_ext = ext_years - value_years
            if extra_ext:
                raise ValueError(
                    f"extension_basis_by_period has years not in values: {sorted(extra_ext)}"
                )

        if self.source_refs_by_period:
            source_ids = {source.id for source in self.sources}
            for year, refs in self.source_refs_by_period.items():
                missing = set(refs) - source_ids
                if missing:
                    raise ValueError(
                        f"source_refs_by_period[{year}] references unknown source IDs: "
                        f"{sorted(missing)} (sources block has "
                        f"{sorted(source_ids) if source_ids else '[]'})"
                    )

        return self


class ProjectionEntry(_ContractModel):
    scenarios: dict[Literal["base", "bull", "bear"], ScenarioEntry]

    @model_validator(mode="after")
    def _at_least_one_scenario(self) -> "ProjectionEntry":
        if not self.scenarios:
            raise ValueError("at least one scenario required")
        return self


class ProjectionsSection(RootModel[dict[str, ProjectionEntry]]):
    """Root dict mapping item_id to ProjectionEntry."""

    @model_validator(mode="after")
    def _validate_keys(self) -> "ProjectionsSection":
        for key in self.root:
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"projection key must be non-empty string, got {key!r}")
        return self


__all__ = [
    "ProjectionEntry",
    "ProjectionsSection",
    "ProjectionValueScale",
    "Provenance",
    "ScenarioEntry",
]
