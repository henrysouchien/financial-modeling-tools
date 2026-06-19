from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator, model_validator

from .thesis_shared_slice import _ContractModel


class CompsManifestSourceBinding(_ContractModel):
    kind: Literal["fmp_endpoint", "edgar_concept", "transcript_kpi", "derived", "kpi"]
    fmp_endpoint: str | None = None
    fmp_field: str | None = None
    edgar_concept: str | None = None
    kpi_key: str | None = None
    derived_formula: str | None = None


class CompsManifestMetric(_ContractModel):
    key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    units: str | None = None
    order: int = Field(ge=1)
    aggregation: Literal["median", "mean", "weighted"] = "median"
    null_policy: Literal["skip", "zero", "fail"] = "skip"
    source: CompsManifestSourceBinding


class CompsManifestSection(_ContractModel):
    name: str = Field(min_length=1)
    order: int = Field(ge=1)
    metrics: list[CompsManifestMetric]

    @field_validator("metrics")
    @classmethod
    def _reject_duplicate_metric_keys(
        cls, value: list[CompsManifestMetric]
    ) -> list[CompsManifestMetric]:
        keys = [metric.key for metric in value]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            raise ValueError(f"duplicate metric keys: {duplicates}")
        orders = [metric.order for metric in value]
        duplicate_orders = sorted({order for order in orders if orders.count(order) > 1})
        if duplicate_orders:
            raise ValueError(f"duplicate metric orders: {duplicate_orders}")
        return value


class CompsTemplateManifest(_ContractModel):
    template_id: str = Field(min_length=1)
    template_kind: Literal["industry_comps", "operating_comps"]
    industry_key: str | None = None
    source_sheet_key: str | None = Field(
        default=None,
        description="Comps sheet registry key for manifests bound to reusable sheets.",
    )
    source_gsheet_id: str | None = Field(
        default=None,
        description=(
            "Deprecated legacy direct Google Sheet id; accepted for backcompat."
        ),
    )
    source_gsheet_version: str | None = Field(
        default=None,
        description="Deprecated legacy/source version stamp; accepted for backcompat.",
    )
    years: list[int] | None = None
    years_back: int | None = None
    sections: list[CompsManifestSection]

    @model_validator(mode="after")
    def _validate_manifest(self) -> "CompsTemplateManifest":
        if self.source_sheet_key is not None and self.source_gsheet_id is not None:
            raise ValueError(
                "source_sheet_key and source_gsheet_id are mutually exclusive when non-null"
            )
        if self.template_kind == "operating_comps" and not self.industry_key:
            raise ValueError("industry_key is required for operating_comps templates")
        if self.template_kind == "operating_comps":
            has_years = self.years is not None
            has_years_back = self.years_back is not None
            if has_years == has_years_back:
                raise ValueError(
                    "operating_comps templates require exactly one of years or years_back"
                )
        section_names = [section.name for section in self.sections]
        duplicate_names = sorted(
            {name for name in section_names if section_names.count(name) > 1}
        )
        if duplicate_names:
            raise ValueError(f"duplicate section names: {duplicate_names}")
        section_orders = [section.order for section in self.sections]
        duplicate_orders = sorted(
            {order for order in section_orders if section_orders.count(order) > 1}
        )
        if duplicate_orders:
            raise ValueError(f"duplicate section orders: {duplicate_orders}")
        return self


__all__ = [
    "CompsManifestMetric",
    "CompsManifestSection",
    "CompsManifestSourceBinding",
    "CompsTemplateManifest",
]
