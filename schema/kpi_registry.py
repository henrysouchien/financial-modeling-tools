from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from .thesis_shared_slice import _ContractModel


class KPIExtraction(_ContractModel):
    kind: Literal["transcript_kpi", "edgar_concept", "derived"]
    pattern_hints: list[str] | None = None
    concept_name: str | None = None
    formula: str | None = None

    @model_validator(mode="after")
    def _validate_kind_config(self) -> "KPIExtraction":
        if self.kind == "transcript_kpi":
            if not self.pattern_hints:
                raise ValueError("transcript_kpi extraction requires pattern_hints")
            if self.concept_name is not None or self.formula is not None:
                raise ValueError(
                    "transcript_kpi extraction only accepts pattern_hints config"
                )
        elif self.kind == "edgar_concept":
            if not self.concept_name:
                raise ValueError("edgar_concept extraction requires concept_name")
            if self.pattern_hints is not None or self.formula is not None:
                raise ValueError(
                    "edgar_concept extraction only accepts concept_name config"
                )
        elif self.kind == "derived":
            if not self.formula:
                raise ValueError("derived extraction requires formula")
            if self.pattern_hints is not None or self.concept_name is not None:
                raise ValueError("derived extraction only accepts formula config")
        return self


class KPI(_ContractModel):
    key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    units: str = Field(min_length=1)
    definition: str = Field(min_length=1)
    aliases: list[str] = Field(default_factory=list)
    extraction: KPIExtraction


class KPIRegistry(_ContractModel):
    industry_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    template_manifest_id: str = Field(min_length=1)
    kpis: list[KPI]
    financial_metrics: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _reject_duplicate_kpi_keys(self) -> "KPIRegistry":
        keys = [kpi.key for kpi in self.kpis]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            raise ValueError(f"duplicate KPI keys: {duplicates}")
        return self


__all__ = [
    "KPI",
    "KPIExtraction",
    "KPIRegistry",
]
