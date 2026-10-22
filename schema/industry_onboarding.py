"""The stored typed outputs of an industry-onboarding sidecar.

The industry-onboarding door (``api/fms/modules/industry_onboarding``) constructs
this model from the admitted judgment and the package it built, and persists its
``model_dump``; the agent never submits it. A construction that does not validate
is the door's defect, raised with the field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


IndustryOnboardingVerdict = Literal["ONBOARDING_STAGED", "INSUFFICIENT_ARGS", "INSUFFICIENT_DATA"]


class IndustryOnboardingOutputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    industry_key: str | None
    display_name: str | None
    verdict: IndustryOnboardingVerdict
    gate_code: Literal["PROCEED", "INSUFFICIENT_DATA"]
    mode: Literal["interactive", "autonomous"]
    methodology: str
    peer_source: str
    peer_tickers: tuple[str, ...]
    edgar_filings: tuple[str, ...]
    canonical_buckets: tuple[dict[str, Any], ...]
    aggregation: dict[str, Any] | None
    registry_payload: dict[str, Any] | None
    manifest_payload: dict[str, Any] | None
    fixture_csv_sha256: str | None
    taxonomy_action: Literal["none", "propose"]
    taxonomy_patch: dict[str, Any] | None
    rejected_kpis: tuple[dict[str, Any], ...]
    pattern_results: dict[str, Any]
    staging: dict[str, Any]
    decision_log_entry: dict[str, Any] | None
    missing: tuple[str, ...]
    data_gaps: tuple[Any, ...]
