"""Handoff-free immutable historical basis for pre-compile judgment stages."""

from __future__ import annotations

from datetime import datetime
import hashlib
import re
from typing import Any, Literal, Mapping, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .build_model_product_contracts import (
    AnnualRequirementCoverage,
    BuildDiagnostic,
    HistoricalBasisFact,
    SourceEvidence,
    validate_historical_reconciliation_source_bindings,
)
from .business_model_revision_ref import BusinessModelRevisionRef
from .model_state_identity import validate_sha256_hex
from .stable_control_identity import FiscalAxis
from industry_slice_core.thesis_shared_sources import SourceRecord
from .ticker_contract import normalize_ticker
from .versioned_artifact import canonical_json_bytes


class _Contract(BaseModel, frozen=True):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        domain.encode("utf-8") + b"\0" + canonical_json_bytes(value)
    ).hexdigest()


class HistoricalBasisArtifactRef(_Contract, frozen=True):
    """Stable tagged seam for provisional and later accepted historical bases."""

    kind: Literal["provisional_source_snapshot", "historical_foundation"]
    artifact_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class HistoricalBasisSnapshotRef(HistoricalBasisArtifactRef, frozen=True):
    kind: Literal["provisional_source_snapshot"] = "provisional_source_snapshot"
    artifact_id: str = Field(pattern=r"^hbs_[0-9a-f]{32}$")
    schema_version: Literal[
        "historical-basis-snapshot.v1",
        "historical-basis-snapshot.v2",
    ] = (
        "historical-basis-snapshot.v1"
    )


class HistoricalBasisProfilePin(_Contract, frozen=True):
    schema_version: Literal["historical-basis-profile-pin.v1"] = (
        "historical-basis-profile-pin.v1"
    )
    profile_id: str = Field(min_length=1)
    profile_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class HistoricalBasisCorpusPin(_Contract, frozen=True):
    schema_version: Literal["historical-basis-corpus-pin.v1"] = (
        "historical-basis-corpus-pin.v1"
    )
    corpus_id: str = Field(min_length=1)
    corpus_version: str = Field(min_length=1)
    manifest_sha256: str

    @field_validator("manifest_sha256")
    @classmethod
    def _manifest_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="manifest_sha256")


class HistoricalBasisFreshness(_Contract, frozen=True):
    """Typed readiness of the deterministic basis against the review horizon."""

    schema_version: Literal["historical-basis-freshness.v1"] = (
        "historical-basis-freshness.v1"
    )
    status: Literal["current", "blocking_coverage_lag", "blocking_unknown"]
    axis_historical_through_period_id: str = Field(min_length=1)
    covered_historical_through_period_id: str | None = Field(default=None, min_length=1)
    required_through_period_id: str | None = Field(default=None, min_length=1)
    determination_basis: Literal[
        "business_model_review_basis_filing",
        "terminal_summary",
        "unavailable",
        "conflicting",
    ]
    review_accession: str | None = Field(default=None, min_length=1)
    review_evidence_snapshot_sha256: str | None = None
    message: str = Field(min_length=1)

    @field_validator("review_evidence_snapshot_sha256")
    @classmethod
    def _review_evidence_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(
            value,
            field_name="review_evidence_snapshot_sha256",
        )

    @model_validator(mode="after")
    def _status_shape(self) -> "HistoricalBasisFreshness":
        def period_year(period_id: str | None) -> int | None:
            if period_id is None:
                return None
            match = re.search(r"(?:19|20|21)\d{2}", period_id)
            return None if match is None else int(match.group(0))

        covered = period_year(self.covered_historical_through_period_id)
        required = period_year(self.required_through_period_id)
        if self.status == "current" and (
            covered is None or required is None or covered < required
        ):
            raise ValueError("current freshness requires coverage through review basis")
        if self.status == "blocking_coverage_lag" and (
            covered is None or required is None or covered >= required
        ):
            raise ValueError("coverage lag requires covered period before review basis")
        if self.status == "blocking_unknown" and self.required_through_period_id is not None:
            raise ValueError("unknown freshness cannot claim a required review period")
        bound_review = (
            self.review_accession is not None
            and self.review_evidence_snapshot_sha256 is not None
        )
        if (self.review_accession is None) != (
            self.review_evidence_snapshot_sha256 is None
        ):
            raise ValueError(
                "review accession and evidence snapshot digest must appear together"
            )
        if (
            self.determination_basis == "business_model_review_basis_filing"
            and not bound_review
        ):
            raise ValueError("filing review basis requires exact evidence binding")
        if self.determination_basis in {"unavailable", "conflicting"} and bound_review:
            raise ValueError(
                "unavailable or conflicting review basis forbids evidence binding"
            )
        return self


def fiscal_axis_digest(value: FiscalAxis | Mapping[str, Any]) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, FiscalAxis)
        else dict(value)
    )
    return _digest("historical-basis-fiscal-axis.v1", payload)


def _coverage_evidence_ids(
    coverage: Sequence[AnnualRequirementCoverage],
) -> set[str]:
    return {
        evidence_id
        for requirement in coverage
        for period in requirement.periods
        for evidence_id in (
            *period.evidence_ids,
            *period.applicability_evidence_ids,
            *period.conflict_evidence_ids,
        )
    }


class HistoricalBasisSnapshot(_Contract, frozen=True):
    """Minimal closed fact/evidence projection produced without a handoff."""

    schema_version: Literal[
        "historical-basis-snapshot.v1",
        "historical-basis-snapshot.v2",
    ] = (
        "historical-basis-snapshot.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: HistoricalBasisSnapshotRef
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    profile: HistoricalBasisProfilePin
    corpus: HistoricalBasisCorpusPin
    source_payload_sha256: str
    fiscal_axis: FiscalAxis
    fiscal_axis_sha256: str
    freshness: HistoricalBasisFreshness
    historical_basis_facts: tuple[HistoricalBasisFact, ...] = ()
    evidence: tuple[SourceEvidence, ...] = ()
    coverage: tuple[AnnualRequirementCoverage, ...] = ()
    sources: tuple[SourceRecord, ...] = ()
    gaps: tuple[BuildDiagnostic, ...] = ()
    created_at: datetime

    @field_validator("ticker")
    @classmethod
    def _canonical_ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized

    @field_validator("business_model_stage_receipt_digest")
    @classmethod
    def _business_model_stage_receipt_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="business_model_stage_receipt_digest",
        )

    @field_validator("source_payload_sha256")
    @classmethod
    def _source_payload_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="source_payload_sha256")

    @field_validator("fiscal_axis_sha256")
    @classmethod
    def _fiscal_axis_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="fiscal_axis_sha256")

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _closed_projection(self) -> "HistoricalBasisSnapshot":
        has_reconciliations = any(
            row.reconciliation is not None for row in self.historical_basis_facts
        )
        if (self.schema_version == "historical-basis-snapshot.v2") != (
            has_reconciliations
        ):
            raise ValueError(
                "historical-basis snapshot v2 is required exactly for reconciliations"
            )
        if self.ref.schema_version != self.schema_version:
            raise ValueError("historical-basis snapshot ref version must match payload")
        validate_historical_reconciliation_source_bindings(
            self.historical_basis_facts,
            self.sources,
            allow_reconciliations=has_reconciliations,
        )
        if self.business_model_revision_ref.business_model_id != (
            f"{self.ticker}_business_model"
        ):
            raise ValueError("BusinessModel revision ref does not match snapshot ticker")
        if self.fiscal_axis_sha256 != fiscal_axis_digest(self.fiscal_axis):
            raise ValueError("fiscal axis digest mismatch")

        historical_period_ids = {
            period.period_id for period in self.fiscal_axis.historical_periods
        }
        if self.freshness.axis_historical_through_period_id != (
            self.fiscal_axis.historical_periods[-1].period_id
        ):
            raise ValueError("freshness axis period differs from the fiscal axis")
        fact_ids = tuple(row.fact_id for row in self.historical_basis_facts)
        if fact_ids != tuple(sorted(set(fact_ids))):
            raise ValueError("historical basis facts must be sorted and unique by fact_id")
        if any(
            row.period_id not in historical_period_ids
            for row in self.historical_basis_facts
        ):
            raise ValueError("historical basis fact lies outside the fiscal axis")

        evidence_ids = tuple(row.evidence_id for row in self.evidence)
        if evidence_ids != tuple(sorted(set(evidence_ids))):
            raise ValueError("selected evidence must be sorted and unique by evidence_id")
        coverage_ids = tuple(row.requirement_id for row in self.coverage)
        if coverage_ids != tuple(sorted(set(coverage_ids))):
            raise ValueError("coverage must be sorted and unique by requirement_id")
        referenced_evidence = _coverage_evidence_ids(self.coverage)
        selected_evidence = set(evidence_ids)
        if selected_evidence != referenced_evidence:
            missing = tuple(sorted(referenced_evidence - selected_evidence))
            surplus = tuple(sorted(selected_evidence - referenced_evidence))
            raise ValueError(
                "selected evidence must exactly close coverage references; "
                f"missing={missing!r}, surplus={surplus!r}"
            )

        source_ids = tuple(str(row.id) for row in self.sources)
        if source_ids != tuple(sorted(set(source_ids))):
            raise ValueError("sources must be sorted and unique by source id")
        referenced_sources = {
            *(str(row.source_id) for row in self.historical_basis_facts),
            *(str(row.source_id) for row in self.evidence),
            *(
                str(parent_id)
                for row in self.historical_basis_facts
                if row.reconciliation is not None
                for parent_id in row.reconciliation.raw_parent_source_ids
            ),
            *(
                str(ref.source_id)
                for row in self.historical_basis_facts
                if row.reconciliation is not None
                for ref in row.reconciliation.definition_refs
            ),
        }
        retained_sources = set(source_ids)
        if retained_sources != referenced_sources:
            missing = tuple(sorted(referenced_sources - retained_sources))
            surplus = tuple(sorted(retained_sources - referenced_sources))
            raise ValueError(
                "sources must exactly close retained facts and evidence; "
                f"missing={missing!r}, surplus={surplus!r}"
            )
        if self.ref.content_sha256 != historical_basis_snapshot_digest(self):
            raise ValueError("historical basis snapshot content digest mismatch")
        if self.ref.artifact_id != f"hbs_{self.ref.content_sha256[:32]}":
            raise ValueError("historical basis snapshot id is not content-derived")
        return self


def historical_basis_snapshot_digest(
    value: HistoricalBasisSnapshot | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, HistoricalBasisSnapshot)
        else dict(value)
    )
    payload.pop("ref", None)
    version = str(payload.get("schema_version", "historical-basis-snapshot.v1"))
    if version not in {
        "historical-basis-snapshot.v1",
        "historical-basis-snapshot.v2",
    }:
        raise ValueError("historical-basis snapshot digest version is invalid")
    return _digest(version, payload)


def build_historical_basis_snapshot(
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    profile: HistoricalBasisProfilePin,
    corpus: HistoricalBasisCorpusPin,
    source_payload_sha256: str,
    fiscal_axis: FiscalAxis,
    freshness: HistoricalBasisFreshness,
    historical_basis_facts: Sequence[HistoricalBasisFact],
    evidence: Sequence[SourceEvidence],
    coverage: Sequence[AnnualRequirementCoverage],
    sources: Sequence[SourceRecord],
    gaps: Sequence[BuildDiagnostic],
    created_at: datetime,
) -> HistoricalBasisSnapshot:
    canonical_facts = tuple(sorted(historical_basis_facts, key=lambda row: row.fact_id))
    canonical_evidence = tuple(
        sorted(evidence, key=lambda row: row.evidence_id)
    )
    canonical_coverage = tuple(sorted(coverage, key=lambda row: row.requirement_id))
    canonical_sources = tuple(sorted(sources, key=lambda row: str(row.id)))
    canonical_gaps = tuple(
        sorted(
            gaps,
            key=lambda row: (
                row.code,
                row.semantic_item_id or "",
                row.period_id or "",
                row.message,
            ),
        )
    )
    schema_version = (
        "historical-basis-snapshot.v2"
        if any(row.reconciliation is not None for row in canonical_facts)
        else "historical-basis-snapshot.v1"
    )
    payload = {
        "schema_version": schema_version,
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "business_model_revision_ref": business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "profile": profile,
        "corpus": corpus,
        "source_payload_sha256": source_payload_sha256,
        "fiscal_axis": fiscal_axis,
        "fiscal_axis_sha256": fiscal_axis_digest(fiscal_axis),
        "freshness": freshness,
        "historical_basis_facts": canonical_facts,
        "evidence": canonical_evidence,
        "coverage": canonical_coverage,
        "sources": canonical_sources,
        "gaps": canonical_gaps,
        "created_at": created_at,
    }
    content_sha256 = historical_basis_snapshot_digest(payload)
    return HistoricalBasisSnapshot.model_validate(
        {
            **payload,
            "ref": HistoricalBasisSnapshotRef(
                artifact_id=f"hbs_{content_sha256[:32]}",
                schema_version=schema_version,
                content_sha256=content_sha256,
            ),
        }
    )


__all__ = [
    "HistoricalBasisCorpusPin",
    "HistoricalBasisFreshness",
    "HistoricalBasisArtifactRef",
    "HistoricalBasisProfilePin",
    "HistoricalBasisSnapshot",
    "HistoricalBasisSnapshotRef",
    "build_historical_basis_snapshot",
    "fiscal_axis_digest",
    "historical_basis_snapshot_digest",
]
