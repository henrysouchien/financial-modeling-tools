"""Typed product output for the fundamental-research FMS skill."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .company_diligence import CompanyDiligence
from .initiation_evidence import EvidenceStatus, ResearchStatus


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FundamentalResearchSource(_StrictContract):
    source_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    source_identity: str = Field(min_length=1)
    text: str = ""


class FundamentalResearchCapabilityEvidence(_StrictContract):
    capability: Literal["business_overview", "company_diligence"]
    research_status: ResearchStatus
    evidence_status: EvidenceStatus
    rationale: str = Field(min_length=1)
    source_id: str | None = Field(default=None, min_length=1)
    producer_locator: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _validate_evidence_identity(
        self,
    ) -> "FundamentalResearchCapabilityEvidence":
        requires_evidence = self.evidence_status in {
            "available",
            "partial",
            "not_disclosed",
            "conflicting",
        }
        if requires_evidence and (
            self.source_id is None or self.producer_locator is None
        ):
            raise ValueError(
                f"{self.evidence_status} capability evidence requires source identity"
            )
        if (self.source_id is None) != (self.producer_locator is None):
            raise ValueError(
                "capability source_id and producer_locator must be declared together"
            )
        return self


class FundamentalResearchTypedOutput(_StrictContract):
    source_refs: tuple[str, ...] = ()
    source_registry: tuple[FundamentalResearchSource, ...] = ()
    initiation_capability_evidence: tuple[
        FundamentalResearchCapabilityEvidence, ...
    ] = Field(min_length=2, max_length=2)
    research_date: str | None = None
    step_synthesis: JsonValue | None = None
    step_coverage: JsonValue | None = None
    sources_registered: JsonValue | None = None
    sources: tuple[JsonValue, ...] = ()
    data_gaps: tuple[JsonValue, ...] = ()
    business_overview: dict[str, JsonValue] | None = None
    company_diligence: CompanyDiligence | None = None
    worksheet: JsonValue | None = None
    citation_degradation_warnings: tuple[JsonValue, ...] = ()

    @model_validator(mode="after")
    def _validate_closure(self) -> "FundamentalResearchTypedOutput":
        source_ids = tuple(item.source_id for item in self.source_registry)
        if source_ids != tuple(sorted(set(source_ids))):
            raise ValueError("source_registry must be sorted and unique")
        if self.source_refs != tuple(sorted(set(self.source_refs))):
            raise ValueError("source_refs must be sorted and unique")
        if not set(self.source_refs) <= set(source_ids):
            raise ValueError("source_refs do not close over source_registry")
        capability_ids = tuple(
            item.capability for item in self.initiation_capability_evidence
        )
        if capability_ids != ("business_overview", "company_diligence"):
            raise ValueError(
                "fundamental-research must declare both dossier capabilities in order"
            )
        for declaration in self.initiation_capability_evidence:
            if (
                declaration.source_id is not None
                and declaration.source_id not in source_ids
            ):
                raise ValueError(
                    "capability evidence does not close over source_registry"
                )
        return self


class FundamentalResearchArtifact(_StrictContract):
    artifact_id: str = Field(min_length=1)
    skill_run_id: str = Field(min_length=1)
    user_id: str | None = None
    ticker: str = Field(pattern=r"^[A-Z0-9.-]+$")
    skill: Literal["fundamental-research"] = "fundamental-research"
    contract_name: Literal["fundamental-research"] = "fundamental-research"
    schema_version: Literal["2.0"] = "2.0"
    data_source: Literal["live", "fixture"]
    created_at: str = Field(min_length=1)
    source_path: str | None = Field(default=None, min_length=1)
    typed_outputs: FundamentalResearchTypedOutput
    provenance: dict[str, JsonValue]
    research_file_id: int | None = None
    control_run_id: str | None = Field(default=None, min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    verdict: dict[str, JsonValue] | None = None
    patch_ops: tuple[JsonValue, ...] | None = None
    citation_degradation_warnings: tuple[JsonValue, ...] | None = None
    decisions_log: tuple[JsonValue, ...] | None = None
    dashboard_ref: JsonValue | None = None
    binary_artifact_path: str | None = Field(default=None, min_length=1)
    proposal_id: str | None = Field(default=None, min_length=1)
    proposal_expires_at: float | None = None
    origin_kind: str | None = Field(default=None, min_length=1)
    visibility: str | None = Field(default=None, min_length=1)
    origin_ref: JsonValue | None = None
    staged_proposal: JsonValue | None = None
    next_actions: tuple[JsonValue, ...] | None = None
    lineage: JsonValue | None = None

    @model_validator(mode="before")
    @classmethod
    def _validate_content_hash(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise ValueError("fundamental-research artifact must be an object")
        expected = value.get("content_sha256")
        body = dict(value)
        body.pop("content_sha256", None)
        actual = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if expected != actual:
            raise ValueError("fundamental-research content_sha256 mismatch")
        return value

    @model_validator(mode="after")
    def _validate_created_at(self) -> "FundamentalResearchArtifact":
        try:
            parsed = datetime.fromisoformat(self.created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "fundamental-research created_at must be ISO-8601"
            ) from exc
        if parsed.utcoffset() is None:
            raise ValueError(
                "fundamental-research created_at must include an offset"
            )
        return self


__all__ = [
    "FundamentalResearchArtifact",
    "FundamentalResearchCapabilityEvidence",
    "FundamentalResearchSource",
    "FundamentalResearchTypedOutput",
]
