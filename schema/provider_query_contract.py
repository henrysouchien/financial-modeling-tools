"""Provider-query request/result DTOs and issuer feature contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal, Mapping, get_args

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from schema.model_state_identity import (
    validate_normalized_ticker,
    validate_sha256_hex,
)
from schema.stable_control_identity import FiscalPeriod
from schema.versioned_artifact import VersionedArtifactRef, domain_sha256


ProviderEvidenceSourceKind = Literal[
    "statement_facts",
    "filing_table",
    "filing_prose",
    "transcript_table",
    "valuation_comps",
    "company_profile",
]


IssuerFeatureId = Literal[
    "normalized_ticker",
    "active_listing",
    "domicile_country",
    "domicile_region",
    "archetype_id",
    "industry_id",
    "subindustry_id",
    "revenue_ttm",
    "market_cap",
]

ISSUER_FEATURE_IDS: tuple[IssuerFeatureId, ...] = get_args(IssuerFeatureId)


class StrictProviderContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _body(
    value: BaseModel | Mapping[str, Any],
    *,
    exclude: set[str],
) -> Mapping[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude=exclude)
    return {key: member for key, member in value.items() if key not in exclude}


def _require_utc(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{field_name} must be an aware UTC datetime")


def _validate_ref_hash(
    ref: VersionedArtifactRef,
    companion_hash: str,
    *,
    field_name: str,
) -> None:
    validate_sha256_hex(companion_hash, field_name=field_name)
    if ref.content_sha256 != companion_hash:
        raise ValueError(f"{field_name} must equal ref content_sha256")


def _finite_decimal(value: object) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError("decimal values must not use binary floating point")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (str, int)):
        try:
            result = Decimal(value)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("decimal value is invalid") from exc
    else:
        raise ValueError("decimal value must be Decimal, canonical text, or integer")
    if not result.is_finite():
        raise ValueError("decimal value must be finite")
    return result


FiniteDecimal = Annotated[
    Decimal,
    BeforeValidator(_finite_decimal),
    Field(allow_inf_nan=False),
]


class ArtifactRefHashBinding(StrictProviderContract):
    artifact_ref: VersionedArtifactRef
    artifact_hash: str

    @model_validator(mode="after")
    def _validate_binding(self) -> "ArtifactRefHashBinding":
        _validate_ref_hash(
            self.artifact_ref,
            self.artifact_hash,
            field_name="artifact_hash",
        )
        return self


class StatementEvidenceProviderRequest(StrictProviderContract):
    request_kind: Literal["statement_facts"]
    schema_version: Literal["statement-evidence-provider-request.v1"]
    normalized_ticker: str
    concept_id: str
    period: FiscalPeriod
    source_kinds: tuple[
        Literal["statement_facts", "filing_table", "filing_prose"], ...
    ] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_request(self) -> "StatementEvidenceProviderRequest":
        validate_normalized_ticker(self.normalized_ticker)
        if len(set(self.source_kinds)) != len(self.source_kinds):
            raise ValueError("source_kinds must be unique")
        return self


class OperatingEvidenceProviderRequest(StrictProviderContract):
    request_kind: Literal["operating_observation"]
    schema_version: Literal["operating-evidence-provider-request.v1"]
    normalized_ticker: str
    metric_id: str
    reconciliation_role: str
    period: FiscalPeriod
    source_kinds: tuple[
        Literal["filing_table", "filing_prose", "transcript_table"], ...
    ] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_request(self) -> "OperatingEvidenceProviderRequest":
        validate_normalized_ticker(self.normalized_ticker)
        if len(set(self.source_kinds)) != len(self.source_kinds):
            raise ValueError("source_kinds must be unique")
        return self


class AdjustmentEvidenceProviderRequest(StrictProviderContract):
    request_kind: Literal["adjustment_component"]
    schema_version: Literal["adjustment-evidence-provider-request.v1"]
    normalized_ticker: str
    adjustment_id: str
    component_role: Literal["amount", "tax_effect"]
    reconciliation_role: str
    period: FiscalPeriod
    source_kinds: tuple[
        Literal["filing_table", "filing_prose", "transcript_table"], ...
    ] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_request(self) -> "AdjustmentEvidenceProviderRequest":
        validate_normalized_ticker(self.normalized_ticker)
        if len(set(self.source_kinds)) != len(self.source_kinds):
            raise ValueError("source_kinds must be unique")
        return self


class ValuationCompsProviderRequest(StrictProviderContract):
    request_kind: Literal["valuation_comps"]
    schema_version: Literal["valuation-comps-provider-request.v1"]
    focal_ticker: str
    subject_ticker: str
    subject_scope: Literal["focal", "peer"]
    metric: Literal[
        "ntm_pe",
        "fy1_pe",
        "ntm_ev_ebitda",
        "fy1_ev_ebitda",
        "peg",
    ]
    fiscal_basis: str
    period: FiscalPeriod
    required_as_of_utc: datetime

    @model_validator(mode="after")
    def _validate_request(self) -> "ValuationCompsProviderRequest":
        validate_normalized_ticker(self.focal_ticker)
        validate_normalized_ticker(self.subject_ticker)
        _require_utc(self.required_as_of_utc, field_name="required_as_of_utc")
        return self


class IssuerUniverseProviderRequest(StrictProviderContract):
    request_kind: Literal["issuer_universe"]
    schema_version: Literal["issuer-universe-provider-request.v1"]
    normalized_focal_ticker: str
    listing_scope: Literal["active_public_equities"]
    required_feature_ids: tuple[IssuerFeatureId, ...]
    capture_cutoff_at_utc: datetime

    @model_validator(mode="after")
    def _validate_request(self) -> "IssuerUniverseProviderRequest":
        validate_normalized_ticker(self.normalized_focal_ticker)
        if self.required_feature_ids != ISSUER_FEATURE_IDS:
            raise ValueError(
                "required_feature_ids must be the exact declared feature set"
            )
        _require_utc(
            self.capture_cutoff_at_utc,
            field_name="capture_cutoff_at_utc",
        )
        return self


EvidenceProviderRequest = Annotated[
    StatementEvidenceProviderRequest
    | OperatingEvidenceProviderRequest
    | AdjustmentEvidenceProviderRequest
    | ValuationCompsProviderRequest,
    Field(discriminator="request_kind"),
]
AnnualProviderRequest = Annotated[
    StatementEvidenceProviderRequest
    | OperatingEvidenceProviderRequest
    | AdjustmentEvidenceProviderRequest
    | ValuationCompsProviderRequest
    | IssuerUniverseProviderRequest,
    Field(discriminator="request_kind"),
]


def provider_request_digest(value: AnnualProviderRequest) -> str:
    return domain_sha256("annual-provider-query-request.v1", value)


def provider_produced_candidate_member_digest(
    value: BaseModel | Mapping[str, Any],
) -> str:
    """Return the frozen digest over one exact provider-produced member."""

    return domain_sha256(
        "provider-produced-candidate-member.v1",
        _body(value, exclude={"member_digest"}),
    )


class ProviderProducedCandidateBinding(StrictProviderContract):
    candidate_member_id: str
    candidate_member_ordinal: int = Field(gt=0)
    provider_native_identity: str
    restatement_series_key: str
    provider_revision_ordinal: int = Field(gt=0)
    source_kind: ProviderEvidenceSourceKind
    source_artifact_ref: VersionedArtifactRef
    source_artifact_hash: str
    source_published_at_utc: datetime
    member_digest: str

    @model_validator(mode="after")
    def _validate_member(self) -> "ProviderProducedCandidateBinding":
        _validate_ref_hash(
            self.source_artifact_ref,
            self.source_artifact_hash,
            field_name="source_artifact_hash",
        )
        _require_utc(self.source_published_at_utc, field_name="source_published_at_utc")
        validate_sha256_hex(self.member_digest, field_name="member_digest")
        if self.member_digest != provider_produced_candidate_member_digest(self):
            raise ValueError("provider-produced candidate member_digest mismatch")
        return self


def provider_result_digest(value: BaseModel | Mapping[str, Any]) -> str:
    return domain_sha256(
        "annual-provider-query-result.v1",
        _body(value, exclude={"result_digest"}),
    )


class CandidateSetProviderResult(StrictProviderContract):
    result_kind: Literal["candidate_set"]
    schema_version: Literal["evidence-provider-result.v1"]
    request_digest: str
    provider_response_ref: VersionedArtifactRef
    provider_response_hash: str
    candidates: tuple[ProviderProducedCandidateBinding, ...] = Field(min_length=1)
    provider_available_at_utc: datetime
    result_digest: str

    @model_validator(mode="after")
    def _validate_result(self) -> "CandidateSetProviderResult":
        _validate_provider_result_common(self)
        ordinals = tuple(member.candidate_member_ordinal for member in self.candidates)
        if ordinals != tuple(range(1, len(self.candidates) + 1)):
            raise ValueError("candidate ordinals must be exactly 1..N")
        if len({member.member_digest for member in self.candidates}) != len(
            self.candidates
        ):
            raise ValueError("candidate members must be unique")
        return self


class ExplicitMissingProviderResult(StrictProviderContract):
    result_kind: Literal["explicit_missing"]
    schema_version: Literal["evidence-provider-result.v1"]
    request_digest: str
    attempt_artifact_ref: VersionedArtifactRef
    attempt_artifact_hash: str
    missing_code: str
    provider_available_at_utc: datetime
    result_digest: str

    @model_validator(mode="after")
    def _validate_result(self) -> "ExplicitMissingProviderResult":
        _validate_provider_result_common(self)
        _validate_ref_hash(
            self.attempt_artifact_ref,
            self.attempt_artifact_hash,
            field_name="attempt_artifact_hash",
        )
        return self


class FailedProviderResult(StrictProviderContract):
    result_kind: Literal["failed"]
    schema_version: Literal["evidence-provider-result.v1"]
    request_digest: str
    attempt_artifact_ref: VersionedArtifactRef
    attempt_artifact_hash: str
    failure_code: str
    provider_available_at_utc: datetime
    result_digest: str

    @model_validator(mode="after")
    def _validate_result(self) -> "FailedProviderResult":
        _validate_provider_result_common(self)
        _validate_ref_hash(
            self.attempt_artifact_ref,
            self.attempt_artifact_hash,
            field_name="attempt_artifact_hash",
        )
        return self


class IssuerUniverseAvailableProviderResult(StrictProviderContract):
    result_kind: Literal["issuer_universe_available"]
    schema_version: Literal["issuer-universe-provider-result.v1"]
    request_digest: str
    provider_response_ref: VersionedArtifactRef
    provider_response_hash: str
    normalized_member_artifact_ref: VersionedArtifactRef
    normalized_member_artifact_hash: str
    member_count: int = Field(gt=0)
    universe_as_of_utc: datetime
    provider_available_at_utc: datetime
    result_digest: str

    @model_validator(mode="after")
    def _validate_result(self) -> "IssuerUniverseAvailableProviderResult":
        _validate_provider_result_common(self)
        _validate_ref_hash(
            self.provider_response_ref,
            self.provider_response_hash,
            field_name="provider_response_hash",
        )
        _validate_ref_hash(
            self.normalized_member_artifact_ref,
            self.normalized_member_artifact_hash,
            field_name="normalized_member_artifact_hash",
        )
        _require_utc(self.universe_as_of_utc, field_name="universe_as_of_utc")
        return self


_ProviderResultCommon = (
    CandidateSetProviderResult
    | ExplicitMissingProviderResult
    | FailedProviderResult
    | IssuerUniverseAvailableProviderResult
)


def _validate_provider_result_common(value: _ProviderResultCommon) -> None:
    validate_sha256_hex(value.request_digest, field_name="request_digest")
    _require_utc(
        value.provider_available_at_utc,
        field_name="provider_available_at_utc",
    )
    validate_sha256_hex(value.result_digest, field_name="result_digest")
    if value.result_digest != provider_result_digest(value):
        raise ValueError("provider result_digest mismatch")


EvidenceProviderResult = Annotated[
    CandidateSetProviderResult | ExplicitMissingProviderResult | FailedProviderResult,
    Field(discriminator="result_kind"),
]
IssuerUniverseProviderResult = Annotated[
    IssuerUniverseAvailableProviderResult
    | ExplicitMissingProviderResult
    | FailedProviderResult,
    Field(discriminator="result_kind"),
]


class KnownTextIssuerFeature(StrictProviderContract):
    feature_state: Literal["known_text"]
    value: str
    evidence: ArtifactRefHashBinding


class KnownDecimalIssuerFeature(StrictProviderContract):
    feature_state: Literal["known_decimal"]
    value: FiniteDecimal
    evidence: ArtifactRefHashBinding


class KnownBooleanIssuerFeature(StrictProviderContract):
    feature_state: Literal["known_boolean"]
    value: bool
    evidence: ArtifactRefHashBinding


class UnavailableIssuerFeature(StrictProviderContract):
    feature_state: Literal["unavailable"]
    reason_code: Literal[
        "provider_missing",
        "not_disclosed",
        "normalization_failed",
    ]
    evidence: tuple[ArtifactRefHashBinding, ...]

    @model_validator(mode="after")
    def _validate_evidence(self) -> "UnavailableIssuerFeature":
        keys = tuple(
            (binding.artifact_ref.uri, binding.artifact_hash)
            for binding in self.evidence
        )
        if keys != tuple(sorted(set(keys))):
            raise ValueError("unavailable feature evidence must be sorted and unique")
        return self


IssuerTextFeature = Annotated[
    KnownTextIssuerFeature | UnavailableIssuerFeature,
    Field(discriminator="feature_state"),
]
IssuerDecimalFeature = Annotated[
    KnownDecimalIssuerFeature | UnavailableIssuerFeature,
    Field(discriminator="feature_state"),
]
IssuerBooleanFeature = Annotated[
    KnownBooleanIssuerFeature | UnavailableIssuerFeature,
    Field(discriminator="feature_state"),
]


def issuer_universe_member_digest(
    value: BaseModel | Mapping[str, Any],
) -> str:
    return domain_sha256(
        "issuer-universe-member.v1",
        _body(value, exclude={"member_digest"}),
    )


class IssuerUniverseMember(StrictProviderContract):
    issuer_id: str
    normalized_ticker: str
    active_listing: IssuerBooleanFeature
    domicile_country: IssuerTextFeature
    domicile_region: IssuerTextFeature
    archetype_id: IssuerTextFeature
    industry_id: IssuerTextFeature
    subindustry_id: IssuerTextFeature
    revenue_ttm: IssuerDecimalFeature
    market_cap: IssuerDecimalFeature
    member_digest: str

    @model_validator(mode="after")
    def _validate_member(self) -> "IssuerUniverseMember":
        validate_normalized_ticker(self.normalized_ticker)
        validate_sha256_hex(self.member_digest, field_name="member_digest")
        if self.member_digest != issuer_universe_member_digest(self):
            raise ValueError("issuer member_digest mismatch")
        return self


__all__ = [
    "AdjustmentEvidenceProviderRequest",
    "AnnualProviderRequest",
    "ArtifactRefHashBinding",
    "CandidateSetProviderResult",
    "EvidenceProviderRequest",
    "EvidenceProviderResult",
    "ExplicitMissingProviderResult",
    "FailedProviderResult",
    "ISSUER_FEATURE_IDS",
    "IssuerBooleanFeature",
    "IssuerDecimalFeature",
    "IssuerFeatureId",
    "IssuerTextFeature",
    "IssuerUniverseAvailableProviderResult",
    "IssuerUniverseMember",
    "IssuerUniverseProviderRequest",
    "IssuerUniverseProviderResult",
    "KnownBooleanIssuerFeature",
    "KnownDecimalIssuerFeature",
    "KnownTextIssuerFeature",
    "OperatingEvidenceProviderRequest",
    "ProviderEvidenceSourceKind",
    "ProviderProducedCandidateBinding",
    "StatementEvidenceProviderRequest",
    "UnavailableIssuerFeature",
    "ValuationCompsProviderRequest",
    "issuer_universe_member_digest",
    "provider_produced_candidate_member_digest",
    "provider_request_digest",
    "provider_result_digest",
]
