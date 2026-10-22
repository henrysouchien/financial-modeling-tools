"""The stored expectations-analysis read: ``typed_outputs.expectations_analysis``.

The report door (``fms.modules.expectations_analysis``) constructs this from the
admitted judgment and the sources it registered; the model validates what the
door built. It is report-only: no proposal and no patch ops are ever stored.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from industry_slice_core._contract import _ContractModel
from industry_slice_core.thesis_shared_sources import SourceRecord


ExpectationsVerdict = Literal[
    "LOW_EXPECTATIONS_UPSIDE",
    "HIGH_EXPECTATIONS_RISK",
    "BALANCED_EXPECTATIONS",
    "CONFLICTED_EXPECTATIONS",
    "INSUFFICIENT_DATA",
]
ExpectationsConfidence = Literal["HIGH", "MEDIUM", "LOW"]
ExpectationsChannelName = Literal["valuation", "estimates", "positioning"]
ExpectationsDirection = Literal["low", "high", "balanced", "conflicted", "unknown"]


class ExpectationsChannel(_ContractModel):
    channel: ExpectationsChannelName
    direction: ExpectationsDirection
    evidence: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    data_gap: str | None = None


class ExpectationsAnalysis(_ContractModel):
    ticker: str = Field(..., min_length=1)
    verdict: ExpectationsVerdict
    confidence: ExpectationsConfidence
    summary: str = Field(..., min_length=1)
    expectations_channels: list[ExpectationsChannel]
    priced_in_read: str | None = None
    variant_view: str | None = None
    revision_trigger: str | None = None
    watch_items: list[str] = Field(default_factory=list)
    data_gaps: list[str] = Field(default_factory=list)
    sources: list[SourceRecord] = Field(default_factory=list)
    next_actions: list[str] = Field(default_factory=list)
    proposal_id: None = None
    patch_ops: list[dict[str, Any]] = Field(default_factory=list, max_length=0)


__all__ = [
    "ExpectationsAnalysis",
    "ExpectationsChannel",
    "ExpectationsChannelName",
    "ExpectationsConfidence",
    "ExpectationsDirection",
    "ExpectationsVerdict",
]
