from __future__ import annotations

import re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from .thesis_shared_slice import _ContractModel


_A1_RANGE_WITH_TAB_RE = re.compile(
    r"^(?P<tab>'[^']+'|[^!]+)![A-Z]+[1-9]\d*(?::[A-Z]+[1-9]\d*)?$"
)


class CompsSheetTab(_ContractModel):
    name: str = Field(min_length=1)
    layout_notes: str = Field(min_length=1)
    ticker_roster_range: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_ticker_roster_range(self) -> "CompsSheetTab":
        match = _A1_RANGE_WITH_TAB_RE.match(self.ticker_roster_range)
        if not match:
            raise ValueError("ticker_roster_range must be an A1 range with tab prefix")
        tab_prefix = match.group("tab")
        if tab_prefix.startswith("'") and tab_prefix.endswith("'"):
            tab_prefix = tab_prefix[1:-1]
        if tab_prefix != self.name:
            raise ValueError("ticker_roster_range tab prefix must match tab name")
        return self


class CompsSheetProvenance(_ContractModel):
    source_sheet_key: str | None = None
    created_by: Literal["agent", "operator"] | None = None
    created_at: str | None = None

    def is_empty(self) -> bool:
        return (
            self.source_sheet_key is None
            and self.created_by is None
            and self.created_at is None
        )


class CompsSheetEntry(_ContractModel):
    sheet_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    gsheet_id: str = Field(min_length=1)
    platform: Literal["gsheets", "excel_workbook"] = "gsheets"
    tier: Literal["curated_source", "platform_working"]
    sectors: list[str] = Field(min_length=1)
    tabs: list[CompsSheetTab] = Field(min_length=1)
    notes: str | None = None
    provenance: CompsSheetProvenance | None = None

    @field_validator("sectors")
    @classmethod
    def _reject_empty_sector_aliases(cls, value: list[str]) -> list[str]:
        empty_indexes = [index for index, sector in enumerate(value) if not sector]
        if empty_indexes:
            raise ValueError(f"sector aliases must be non-empty: {empty_indexes}")
        return value

    @model_validator(mode="after")
    def _validate_tier_provenance(self) -> "CompsSheetEntry":
        if self.tier == "platform_working":
            if self.provenance is None:
                raise ValueError("platform_working sheets require provenance")
            if not self.provenance.created_by or not self.provenance.created_at:
                raise ValueError(
                    "platform_working sheets require provenance.created_by and created_at"
                )
        if (
            self.tier == "curated_source"
            and self.provenance is not None
            and not self.provenance.is_empty()
        ):
            raise ValueError("curated_source sheets must have empty provenance")
        return self


class CompsSheetRegistry(_ContractModel):
    version: int
    sheets: list[CompsSheetEntry]

    @model_validator(mode="after")
    def _validate_registry(self) -> "CompsSheetRegistry":
        sheet_keys = [sheet.sheet_key for sheet in self.sheets]
        duplicate_sheet_keys = sorted(
            {sheet_key for sheet_key in sheet_keys if sheet_keys.count(sheet_key) > 1}
        )
        if duplicate_sheet_keys:
            raise ValueError(f"duplicate sheet_key values: {duplicate_sheet_keys}")

        gsheet_ids = [sheet.gsheet_id for sheet in self.sheets]
        duplicate_gsheet_ids = sorted(
            {gsheet_id for gsheet_id in gsheet_ids if gsheet_ids.count(gsheet_id) > 1}
        )
        if duplicate_gsheet_ids:
            raise ValueError(f"duplicate gsheet_id values: {duplicate_gsheet_ids}")
        return self


__all__ = [
    "CompsSheetEntry",
    "CompsSheetProvenance",
    "CompsSheetRegistry",
    "CompsSheetTab",
]
