"""Durable stage metadata for an accepted BusinessModel revision.

The contracts in this module are deliberately companion metadata, not a second
BusinessModel authority.  A future BusinessModelRevision schema may add an
optional ``stage_metadata`` field of type :class:`BusinessModelStageMetadata`;
the absence of that field on a legacy revision means "not durably pinned" and
must not be repaired by inventing evidence or methodology metadata at read time.

Run-scoped source-map aliases and excerpt handles are intentionally absent.  FMS
must resolve them before persistence and materialize durable provider descriptors
into :class:`BusinessModelSelectedEvidence`.  Bounded canonical values may be
inlined; long corpus sections remain exact and retrievable through their durable
locator and content hash.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
from typing import Annotated, Any, ClassVar, Literal, Mapping, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    TypeAdapter,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_semantic_claims import (
    BusinessModelSemanticClaimBinding,
    canonical_business_model_semantic_claim_bindings,
)
from .model_state_identity import validate_sha256_hex
from .ticker_contract import normalize_ticker
from .versioned_artifact import (
    CanonicalResearchFileArtifactScope,
    VersionedArtifactRef,
    canonical_json_bytes,
)


class _StageContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        domain.encode("utf-8") + b"\0" + canonical_json_bytes(value)
    ).hexdigest()


def _model_payload(value: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError("digest input must be a contract or mapping")


def _require_aware(value: datetime, *, field_name: str | None) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class BusinessModelStageScope(_StageContract):
    """Authenticated research-file scope bound by the terminal receipt."""

    schema_version: Literal["business-model-stage-scope.v1"] = (
        "business-model-stage-scope.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)

    @field_validator("ticker")
    @classmethod
    def _canonical_ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if value != normalized:
            raise ValueError("ticker must be canonical")
        return normalized


class BusinessModelMethodologyPin(_StageContract):
    """Exact declared methodology identity used to author the revision."""

    schema_version: Literal["business-model-methodology-pin.v1"] = (
        "business-model-methodology-pin.v1"
    )
    name: str = Field(min_length=1)
    declared_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


#: The evidence value kinds whose ``canonical_value`` is a numeric scalar; the
#: seed validators accept exactly these when a driver seed cites evidence.
NUMERIC_EVIDENCE_VALUE_KINDS: frozenset[str] = frozenset({"integer", "decimal"})


class BusinessModelEvidenceValue(_StageContract):
    """One canonical value retained from a strong upstream descriptor."""

    schema_version: Literal["business-model-evidence-value.v1"] = (
        "business-model-evidence-value.v1"
    )
    value_id: str
    value_kind: Literal["text", "integer", "decimal", "date", "period"]
    canonical_value: str = Field(min_length=1)
    unit: str | None = Field(default=None, min_length=1)
    scale: str | None = Field(default=None, min_length=1)
    period: str | None = Field(default=None, min_length=1)
    concept: str | None = Field(default=None, min_length=1)

    @field_validator("value_id")
    @classmethod
    def _value_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="value_id")


class BusinessModelCorpusExcerptLocator(_StageContract):
    kind: Literal["corpus_excerpt"] = "corpus_excerpt"
    source_kind: ClassVar[Literal["corpus_excerpt"]] = "corpus_excerpt"
    document_id: str = Field(min_length=1)
    section: str = Field(min_length=1)
    char_start: int = Field(ge=0)
    char_end: int = Field(gt=0)
    excerpt_sha256: str

    @field_validator("excerpt_sha256")
    @classmethod
    def _excerpt_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="excerpt_sha256")

    @model_validator(mode="after")
    def _ordered_coordinates(self) -> "BusinessModelCorpusExcerptLocator":
        if self.char_end <= self.char_start:
            raise ValueError("char_end must be greater than char_start")
        return self


class BusinessModelEdgarTableLocator(_StageContract):
    kind: Literal["edgar_table"] = "edgar_table"
    source_kind: ClassVar[Literal["edgar_table_value"]] = "edgar_table_value"
    document_id: str = Field(min_length=1)
    accession: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    row_index: int = Field(ge=0)
    column_index: int = Field(ge=0)
    row_header: str | None = Field(default=None, min_length=1)
    column_header: str | None = Field(default=None, min_length=1)
    fact_id: str | None = Field(default=None, min_length=1)
    concept: str | None = Field(default=None, min_length=1)
    period: str | None = Field(default=None, min_length=1)
    unit: str | None = Field(default=None, min_length=1)
    extraction_version: str = Field(min_length=1)



class BusinessModelXbrlFactLocator(_StageContract):
    kind: Literal["xbrl_fact"] = "xbrl_fact"
    source_kind: ClassVar[Literal["xbrl_fact"]] = "xbrl_fact"
    accession: str = Field(min_length=1)
    concept: str = Field(min_length=1)
    fact_id: str = Field(min_length=1)
    period: str = Field(min_length=1)

class BusinessModelVendorResponseLocator(_StageContract):
    kind: Literal["vendor_response"] = "vendor_response"
    source_kind: ClassVar[Literal["vendor_response_value"]] = "vendor_response_value"
    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    request_sha256: str
    response_sha256: str
    result_key: str = Field(min_length=1)
    row_index: int | None = Field(default=None, ge=0)

    @field_validator("request_sha256", "response_sha256")
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)


class BusinessModelPortfolioSnapshotLocator(_StageContract):
    kind: Literal["portfolio_snapshot"] = "portfolio_snapshot"
    source_kind: ClassVar[Literal["portfolio_snapshot_value"]] = (
        "portfolio_snapshot_value"
    )
    store_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    record_key: str = Field(min_length=1)
    content_sha256: str
    as_of: str = Field(min_length=1)

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


BusinessModelEvidenceLocator = Annotated[
    BusinessModelCorpusExcerptLocator
    | BusinessModelEdgarTableLocator
    | BusinessModelXbrlFactLocator
    | BusinessModelVendorResponseLocator
    | BusinessModelPortfolioSnapshotLocator,
    Field(discriminator="kind"),
]


_EVIDENCE_LOCATOR: TypeAdapter[BusinessModelEvidenceLocator] = TypeAdapter(
    BusinessModelEvidenceLocator
)


class BusinessModelSelectedEvidence(_StageContract):
    """Durable evidence selected for the stage, independent of its issued handle.

    ``values`` may be empty for locator-backed corpus sections whose exact text is
    too large for the compact canonical-value envelope. The locator and content
    hash still identify the full selected evidence without truncation.
    """

    schema_version: Literal["business-model-selected-evidence.v1"] = (
        "business-model-selected-evidence.v1"
    )
    evidence_id: str
    issuer: str = Field(min_length=1)
    source_kind: Literal[
        "corpus_excerpt",
        "edgar_table_value",
        "xbrl_fact",
        "vendor_response_value",
        "portfolio_snapshot_value",
    ]
    source_identity: str = Field(min_length=1)
    locator: BusinessModelEvidenceLocator
    content_sha256: str
    retrieved_at: datetime
    values: tuple[BusinessModelEvidenceValue, ...]
    # The latest of several retrievals of this one source collapsed into it
    # (SOURCE-IDENTITY-RETRIEVED-AT-IS-METADATA). Absent from the JSON and from
    # evidence_id when None, so ids minted before the field existed hold.
    latest_retrieved_at: datetime | None = None

    @field_validator("evidence_id", "content_sha256")
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("retrieved_at", "latest_retrieved_at")
    @classmethod
    def _retrieval_time(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        return None if value is None else _require_aware(value, field_name=info.field_name)

    @model_serializer(mode="wrap")
    def _omit_absent_latest_retrieval(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        payload = handler(self)
        if payload.get("latest_retrieved_at") is None:
            payload.pop("latest_retrieved_at", None)
        return payload

    @field_validator("values")
    @classmethod
    def _canonical_values(cls, value: object) -> tuple[BusinessModelEvidenceValue, ...]:
        if not isinstance(value, Sequence) or isinstance(
            value, (str, bytes, bytearray)
        ):
            raise TypeError("values must be a sequence")
        rows = [
            row
            if isinstance(row, BusinessModelEvidenceValue)
            else BusinessModelEvidenceValue.model_validate_json(
                canonical_json_bytes(row)
            )
            for row in value
        ]
        by_id: dict[str, BusinessModelEvidenceValue] = {}
        for row in rows:
            prior = by_id.get(row.value_id)
            if prior is not None and prior != row:
                raise ValueError("evidence value_id identifies conflicting values")
            by_id[row.value_id] = row
        return tuple(by_id[key] for key in sorted(by_id))

    @model_validator(mode="after")
    def _closed_descriptor(self) -> "BusinessModelSelectedEvidence":
        # evidence_id digests both fields, so a descriptor whose source_kind
        # names a different evidence kind than its locator is a content-addressed
        # record that contradicts itself.
        if self.source_kind != self.locator.source_kind:
            raise ValueError("source_kind does not match locator kind")
        if (
            self.latest_retrieved_at is not None
            and self.latest_retrieved_at <= self.retrieved_at
        ):
            raise ValueError("latest_retrieved_at must follow retrieved_at")
        if isinstance(self.locator, BusinessModelCorpusExcerptLocator):
            if self.content_sha256 != self.locator.excerpt_sha256:
                raise ValueError("corpus evidence content hash must equal excerpt hash")
        elif isinstance(self.locator, BusinessModelVendorResponseLocator):
            if self.content_sha256 != self.locator.response_sha256:
                raise ValueError(
                    "vendor evidence content hash must equal response hash"
                )
        elif isinstance(self.locator, BusinessModelPortfolioSnapshotLocator):
            if self.content_sha256 != self.locator.content_sha256:
                raise ValueError(
                    "portfolio evidence content hash must equal snapshot hash"
                )
        if self.evidence_id != business_model_selected_evidence_id(self):
            raise ValueError("selected evidence_id does not match durable descriptor")
        return self


def business_model_selected_evidence_id(
    value: BusinessModelSelectedEvidence | Mapping[str, Any],
) -> str:
    payload = _model_payload(value)
    payload.pop("evidence_id", None)
    if payload.get("latest_retrieved_at") is None:
        payload.pop("latest_retrieved_at", None)
    return _digest("business-model-selected-evidence.v1", payload)


def build_business_model_selected_evidence(
    **values: Any,
) -> BusinessModelSelectedEvidence:
    payload = dict(values)
    payload.setdefault("schema_version", "business-model-selected-evidence.v1")
    payload.pop("evidence_id", None)
    raw_locator = payload.get("locator")
    if isinstance(raw_locator, Mapping):
        payload["locator"] = _EVIDENCE_LOCATOR.validate_json(
            canonical_json_bytes(raw_locator)
        )
    payload["values"] = BusinessModelSelectedEvidence._canonical_values(
        payload.get("values")
    )
    payload["evidence_id"] = business_model_selected_evidence_id(payload)
    return BusinessModelSelectedEvidence.model_validate(payload)


class BusinessModelEvidenceSnapshot(_StageContract):
    """Immutable, content-addressed closure of evidence selected for this stage."""

    schema_version: Literal["business-model-evidence-snapshot.v1"] = (
        "business-model-evidence-snapshot.v1"
    )
    evidence: tuple[BusinessModelSelectedEvidence, ...] = ()
    pinned_at: datetime
    content_sha256: str

    @field_validator("evidence")
    @classmethod
    def _canonical_evidence(
        cls, value: object
    ) -> tuple[BusinessModelSelectedEvidence, ...]:
        if not isinstance(value, Sequence) or isinstance(
            value, (str, bytes, bytearray)
        ):
            raise TypeError("evidence must be a sequence")
        rows = [
            row
            if isinstance(row, BusinessModelSelectedEvidence)
            else BusinessModelSelectedEvidence.model_validate_json(
                canonical_json_bytes(row)
            )
            for row in value
        ]
        by_id: dict[str, BusinessModelSelectedEvidence] = {}
        for row in rows:
            prior = by_id.get(row.evidence_id)
            if prior is not None and prior != row:
                raise ValueError("evidence_id identifies conflicting descriptors")
            by_id[row.evidence_id] = row
        return tuple(by_id[key] for key in sorted(by_id))

    @field_validator("pinned_at")
    @classmethod
    def _pin_time(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="pinned_at")

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")

    @model_validator(mode="after")
    def _content_addressed(self) -> "BusinessModelEvidenceSnapshot":
        if self.content_sha256 != business_model_evidence_snapshot_digest(self):
            raise ValueError("evidence snapshot content digest mismatch")
        return self


def business_model_evidence_snapshot_digest(
    value: BusinessModelEvidenceSnapshot | Mapping[str, Any],
) -> str:
    payload = _model_payload(value)
    payload.pop("content_sha256", None)
    return _digest("business-model-evidence-snapshot.v1", payload)


def build_business_model_evidence_snapshot(
    *,
    evidence: Sequence[BusinessModelSelectedEvidence | Mapping[str, Any]],
    pinned_at: datetime,
) -> BusinessModelEvidenceSnapshot:
    canonical = BusinessModelEvidenceSnapshot._canonical_evidence(evidence)
    payload = {
        "schema_version": "business-model-evidence-snapshot.v1",
        "evidence": canonical,
        "pinned_at": pinned_at,
    }
    return BusinessModelEvidenceSnapshot.model_validate(
        {**payload, "content_sha256": business_model_evidence_snapshot_digest(payload)}
    )


class BusinessModelClaimEvidenceBinding(_StageContract):
    """Natural BusinessModel claim key bound to durable evidence identities."""

    schema_version: Literal["business-model-claim-evidence-binding.v1"] = (
        "business-model-claim-evidence-binding.v1"
    )
    claim_key: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)

    @field_validator("evidence_ids")
    @classmethod
    def _canonical_ids(cls, value: object) -> tuple[str, ...]:
        if not isinstance(value, Sequence) or isinstance(
            value, (str, bytes, bytearray)
        ):
            raise TypeError("evidence_ids must be a sequence")
        ids = tuple(sorted({str(item) for item in value}))
        if not ids:
            raise ValueError("evidence_ids must be non-empty")
        for evidence_id in ids:
            validate_sha256_hex(evidence_id, field_name="evidence_ids")
        return ids


class BusinessModelTerminalIssue(_StageContract):
    """One semantically addressable limitation or unresolved question."""

    schema_version: Literal["business-model-terminal-issue.v1"] = (
        "business-model-terminal-issue.v1"
    )
    key: str = Field(min_length=1)
    text: str = Field(min_length=1)
    claim_keys: tuple[str, ...] = ()

    @field_validator("claim_keys")
    @classmethod
    def _canonical_claim_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not claim_key.strip() for claim_key in value):
            raise ValueError("claim_keys entries must be non-blank")
        return tuple(sorted(set(value)))


def _canonical_terminal_issues(
    value: tuple[BusinessModelTerminalIssue, ...],
    *,
    field_name: str | None,
) -> tuple[BusinessModelTerminalIssue, ...]:
    by_key: dict[str, BusinessModelTerminalIssue] = {}
    for row in value:
        prior = by_key.get(row.key)
        if prior is not None and prior.text != row.text:
            raise ValueError(
                f"{field_name} semantic key {row.key!r} identifies conflicting text"
            )
        if prior is None:
            by_key[row.key] = row
        else:
            by_key[row.key] = BusinessModelTerminalIssue(
                key=row.key,
                text=row.text,
                claim_keys=tuple(sorted(set(prior.claim_keys) | set(row.claim_keys))),
            )
    return tuple(by_key[key] for key in sorted(by_key))


class BusinessModelTerminalSummary(_StageContract):
    """Compact analytical orientation authored by the stage agent."""

    schema_version: Literal["business-model-terminal-summary.v1"] = (
        "business-model-terminal-summary.v1"
    )
    authored_by: Literal["agent"] = "agent"
    summary: str = Field(min_length=1)
    key_decisions: tuple[str, ...] = ()
    confidence: Literal["low", "medium", "high"]
    limitations: tuple[BusinessModelTerminalIssue, ...] = ()
    unresolved_questions: tuple[BusinessModelTerminalIssue, ...] = ()
    recommended_next_stage: str = Field(min_length=1)

    @field_validator("key_decisions")
    @classmethod
    def _unique_non_blank(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() for item in value):
            raise ValueError("key_decisions entries must be non-blank")
        if len(value) != len(set(value)):
            raise ValueError("key_decisions entries must be unique")
        return value

    @field_validator("limitations", "unresolved_questions")
    @classmethod
    def _semantic_issues(
        cls,
        value: tuple[BusinessModelTerminalIssue, ...],
        info: ValidationInfo,
    ) -> tuple[BusinessModelTerminalIssue, ...]:
        return _canonical_terminal_issues(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _globally_unique_issue_keys(self) -> "BusinessModelTerminalSummary":
        limitation_keys = {row.key for row in self.limitations}
        unresolved_keys = {row.key for row in self.unresolved_questions}
        overlap = tuple(sorted(limitation_keys & unresolved_keys))
        if overlap:
            raise ValueError(
                "terminal issue keys cannot appear in both limitations and "
                f"unresolved_questions: {overlap!r}"
            )
        return self


def _canonical_claim_bindings(
    value: object,
) -> tuple[BusinessModelClaimEvidenceBinding, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("claim_evidence_bindings must be a sequence")
    by_claim: dict[str, set[str]] = {}
    for raw in value:
        row = (
            raw
            if isinstance(raw, BusinessModelClaimEvidenceBinding)
            else BusinessModelClaimEvidenceBinding.model_validate_json(
                canonical_json_bytes(raw)
            )
        )
        by_claim.setdefault(row.claim_key, set()).update(row.evidence_ids)
    return tuple(
        BusinessModelClaimEvidenceBinding(
            claim_key=claim_key,
            evidence_ids=tuple(sorted(by_claim[claim_key])),
        )
        for claim_key in sorted(by_claim)
    )


def remap_business_model_claim_evidence_bindings(
    *,
    resolved_to_durable_evidence_ids: Mapping[str, str],
    claim_links: Sequence[object],
) -> tuple[BusinessModelClaimEvidenceBinding, ...]:
    """Rebuild FMS claim links with durable selected-evidence identities.

    ``ResolvedSourceEvidenceV1.evidence_id`` binds a run and issued handle, so it
    must never enter persisted BusinessModel metadata.  The caller materializes
    each resolved descriptor as :class:`BusinessModelSelectedEvidence`, records
    the old-to-new ID map in memory, then passes the FMS ``ClaimEvidenceLinkV1``
    rows here.  Value-level IDs remain inside the selected descriptor; the
    BusinessModel contract binds its natural claim keys to durable evidence IDs.
    """

    durable_by_resolved: dict[str, str] = {}
    for resolved_id, durable_id in resolved_to_durable_evidence_ids.items():
        validate_sha256_hex(resolved_id, field_name="resolved evidence_id")
        durable_by_resolved[resolved_id] = validate_sha256_hex(
            durable_id,
            field_name="durable evidence_id",
        )
    rows: list[BusinessModelClaimEvidenceBinding] = []
    for raw in claim_links:
        if isinstance(raw, Mapping):
            claim_key = raw.get("claim_id")
            resolved_ids = raw.get("evidence_ids")
        else:
            claim_key = getattr(raw, "claim_id", None)
            resolved_ids = getattr(raw, "evidence_ids", None)
        if not isinstance(claim_key, str) or not claim_key:
            raise ValueError("claim link requires a non-empty claim_id")
        if not isinstance(resolved_ids, Sequence) or isinstance(
            resolved_ids, (str, bytes, bytearray)
        ):
            raise TypeError("claim link evidence_ids must be a sequence")
        missing = tuple(
            sorted(
                resolved_id
                for resolved_id in resolved_ids
                if resolved_id not in durable_by_resolved
            )
        )
        if missing:
            raise ValueError(
                f"claim {claim_key!r} has unresolved evidence IDs: {missing!r}"
            )
        rows.append(
            BusinessModelClaimEvidenceBinding(
                claim_key=claim_key,
                evidence_ids=tuple(
                    durable_by_resolved[resolved_id] for resolved_id in resolved_ids
                ),
            )
        )
    return _canonical_claim_bindings(rows)


def _canonical_upstream_refs(value: object) -> tuple[VersionedArtifactRef, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("upstream_artifact_refs must be a sequence")
    by_uri: dict[str, VersionedArtifactRef] = {}
    for raw in value:
        row = (
            raw
            if isinstance(raw, VersionedArtifactRef)
            else VersionedArtifactRef.model_validate(raw)
        )
        prior = by_uri.get(row.uri)
        if prior is not None and prior != row:
            raise ValueError("upstream artifact URI identifies conflicting refs")
        by_uri[row.uri] = row
    return tuple(
        sorted(
            by_uri.values(),
            key=lambda row: (
                row.artifact_kind,
                row.artifact_id,
                row.artifact_version,
                row.content_sha256,
            ),
        )
    )


class BusinessModelStageMetadata(_StageContract):
    """Embeddable non-authoritative metadata for one BusinessModel revision."""

    schema_version: Literal["business-model-stage-metadata.v1"] = (
        "business-model-stage-metadata.v1"
    )
    methodology: BusinessModelMethodologyPin
    evidence_snapshot: BusinessModelEvidenceSnapshot
    claim_evidence_bindings: tuple[BusinessModelClaimEvidenceBinding, ...] = ()
    semantic_claim_bindings: tuple[BusinessModelSemanticClaimBinding, ...] = ()
    upstream_artifact_refs: tuple[VersionedArtifactRef, ...] = ()
    terminal_summary: BusinessModelTerminalSummary
    content_sha256: str

    @field_validator("claim_evidence_bindings")
    @classmethod
    def _claims(cls, value: object) -> tuple[BusinessModelClaimEvidenceBinding, ...]:
        return _canonical_claim_bindings(value)

    @field_validator("semantic_claim_bindings")
    @classmethod
    def _semantic_claims(
        cls, value: object
    ) -> tuple[BusinessModelSemanticClaimBinding, ...]:
        return canonical_business_model_semantic_claim_bindings(value)

    @field_validator("upstream_artifact_refs")
    @classmethod
    def _upstreams(cls, value: object) -> tuple[VersionedArtifactRef, ...]:
        return _canonical_upstream_refs(value)

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")

    @model_validator(mode="after")
    def _closed_evidence_and_digest(self) -> "BusinessModelStageMetadata":
        selected = {row.evidence_id for row in self.evidence_snapshot.evidence}
        bound = {
            evidence_id
            for binding in self.claim_evidence_bindings
            for evidence_id in binding.evidence_ids
        }
        missing = tuple(sorted(bound - selected))
        unbound = tuple(sorted(selected - bound))
        if missing:
            raise ValueError(f"claim bindings reference missing evidence: {missing!r}")
        if unbound:
            raise ValueError(f"selected evidence lacks a claim binding: {unbound!r}")
        if self.semantic_claim_bindings:
            mapped_claims = {
                claim_key
                for binding in self.semantic_claim_bindings
                for claim_key in binding.claim_keys
            }
            evidence_bound_claims = {
                binding.claim_key for binding in self.claim_evidence_bindings
            }
            missing_semantic_claims = tuple(
                sorted(evidence_bound_claims - mapped_claims)
            )
            unknown_semantic_claims = tuple(
                sorted(mapped_claims - evidence_bound_claims)
            )
            if missing_semantic_claims:
                raise ValueError(
                    "claim evidence bindings lack semantic item mappings: "
                    f"{missing_semantic_claims!r}"
                )
            if unknown_semantic_claims:
                raise ValueError(
                    "semantic item mappings reference unbound claims: "
                    f"{unknown_semantic_claims!r}"
                )
        if self.content_sha256 != business_model_stage_metadata_digest(self):
            raise ValueError("BusinessModel stage metadata content digest mismatch")
        return self


def business_model_methodology_pin_digest(
    value: BusinessModelMethodologyPin | Mapping[str, Any],
) -> str:
    return _digest("business-model-methodology-pin.v1", _model_payload(value))


def business_model_stage_metadata_digest(
    value: BusinessModelStageMetadata | Mapping[str, Any],
) -> str:
    payload = _model_payload(value)
    payload.pop("content_sha256", None)
    if not payload.get("semantic_claim_bindings"):
        # Preserve the digest of already-persisted v1 metadata that predates
        # this optional compatibility field.
        payload.pop("semantic_claim_bindings", None)
    return _digest("business-model-stage-metadata.v1", payload)


def build_business_model_stage_metadata(
    *,
    methodology: BusinessModelMethodologyPin | Mapping[str, Any],
    evidence_snapshot: BusinessModelEvidenceSnapshot | Mapping[str, Any],
    claim_evidence_bindings: Sequence[
        BusinessModelClaimEvidenceBinding | Mapping[str, Any]
    ],
    upstream_artifact_refs: Sequence[VersionedArtifactRef | Mapping[str, Any]],
    terminal_summary: BusinessModelTerminalSummary | Mapping[str, Any],
    semantic_claim_bindings: Sequence[
        BusinessModelSemanticClaimBinding | Mapping[str, Any]
    ] = (),
) -> BusinessModelStageMetadata:
    canonical_methodology = (
        methodology
        if isinstance(methodology, BusinessModelMethodologyPin)
        else BusinessModelMethodologyPin.model_validate_json(
            canonical_json_bytes(methodology)
        )
    )
    canonical_snapshot = (
        evidence_snapshot
        if isinstance(evidence_snapshot, BusinessModelEvidenceSnapshot)
        else BusinessModelEvidenceSnapshot.model_validate_json(
            canonical_json_bytes(evidence_snapshot)
        )
    )
    canonical_bindings = _canonical_claim_bindings(claim_evidence_bindings)
    canonical_semantic_bindings = (
        canonical_business_model_semantic_claim_bindings(semantic_claim_bindings)
    )
    canonical_upstreams = _canonical_upstream_refs(upstream_artifact_refs)
    canonical_summary = (
        terminal_summary
        if isinstance(terminal_summary, BusinessModelTerminalSummary)
        else BusinessModelTerminalSummary.model_validate_json(
            canonical_json_bytes(terminal_summary)
        )
    )
    payload = {
        "schema_version": "business-model-stage-metadata.v1",
        "methodology": canonical_methodology,
        "evidence_snapshot": canonical_snapshot,
        "claim_evidence_bindings": canonical_bindings,
        "semantic_claim_bindings": canonical_semantic_bindings,
        "upstream_artifact_refs": canonical_upstreams,
        "terminal_summary": canonical_summary,
    }
    return BusinessModelStageMetadata.model_validate(
        {**payload, "content_sha256": business_model_stage_metadata_digest(payload)}
    )


class BusinessModelStageReceipt(_StageContract):
    """Artifact-bound terminal receipt for one exact BusinessModel revision.

    The receipt may be embedded in the revision envelope because the referenced
    content digest covers the canonical ``BusinessModel`` payload, not the
    surrounding envelope.  It may also be projected as a sidecar for consumers.
    """

    schema_version: Literal["business-model-stage-receipt.v1"] = (
        "business-model-stage-receipt.v1"
    )
    stage: Literal["business-model-construction"] = "business-model-construction"
    status: Literal["accepted", "incomplete", "blocked"]
    scope: BusinessModelStageScope
    business_model_revision_ref: BusinessModelRevisionRef
    stage_metadata: BusinessModelStageMetadata
    started_at: datetime
    completed_at: datetime
    receipt_digest: str

    @field_validator("started_at", "completed_at")
    @classmethod
    def _timestamps(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @field_validator("receipt_digest")
    @classmethod
    def _receipt_digest_shape(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="receipt_digest")

    @model_validator(mode="after")
    def _binding(self) -> "BusinessModelStageReceipt":
        expected_business_model_id = f"{self.scope.ticker}_business_model"
        if (
            self.business_model_revision_ref.business_model_id
            != expected_business_model_id
        ):
            raise ValueError("BusinessModel revision ref does not match receipt scope")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        for ref in self.stage_metadata.upstream_artifact_refs:
            scope = ref.scope
            if not isinstance(scope, CanonicalResearchFileArtifactScope):
                raise ValueError(
                    "upstream artifact must use canonical research-file scope"
                )
            if (
                scope.user_id,
                scope.research_file_id,
                scope.ticker,
            ) != (
                self.scope.user_id,
                self.scope.research_file_id,
                self.scope.ticker,
            ):
                raise ValueError("upstream artifact ref does not match receipt scope")
        if self.receipt_digest != business_model_stage_receipt_digest(self):
            raise ValueError("BusinessModel stage receipt digest mismatch")
        return self


def business_model_stage_receipt_digest(
    value: BusinessModelStageReceipt | Mapping[str, Any],
) -> str:
    payload = _model_payload(value)
    payload.pop("receipt_digest", None)
    return _digest("business-model-stage-receipt.v1", payload)


def build_business_model_stage_receipt(
    *,
    status: Literal["accepted", "incomplete", "blocked"],
    scope: BusinessModelStageScope | Mapping[str, Any],
    business_model_revision_ref: BusinessModelRevisionRef | Mapping[str, Any],
    stage_metadata: BusinessModelStageMetadata | Mapping[str, Any],
    started_at: datetime,
    completed_at: datetime,
) -> BusinessModelStageReceipt:
    canonical_scope = (
        scope
        if isinstance(scope, BusinessModelStageScope)
        else BusinessModelStageScope.model_validate_json(canonical_json_bytes(scope))
    )
    canonical_revision_ref = (
        business_model_revision_ref
        if isinstance(business_model_revision_ref, BusinessModelRevisionRef)
        else BusinessModelRevisionRef.model_validate_json(
            canonical_json_bytes(business_model_revision_ref)
        )
    )
    canonical_metadata = (
        stage_metadata
        if isinstance(stage_metadata, BusinessModelStageMetadata)
        else BusinessModelStageMetadata.model_validate_json(
            canonical_json_bytes(stage_metadata)
        )
    )
    payload = {
        "schema_version": "business-model-stage-receipt.v1",
        "stage": "business-model-construction",
        "status": status,
        "scope": canonical_scope,
        "business_model_revision_ref": canonical_revision_ref,
        "stage_metadata": canonical_metadata,
        "started_at": started_at,
        "completed_at": completed_at,
    }
    return BusinessModelStageReceipt.model_validate(
        {**payload, "receipt_digest": business_model_stage_receipt_digest(payload)}
    )


def validate_business_model_stage_receipt(
    value: BusinessModelStageReceipt | Mapping[str, Any],
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    business_model_revision_ref: BusinessModelRevisionRef,
) -> BusinessModelStageReceipt:
    """Validate a receipt and its exact authenticated caller binding."""

    receipt = (
        value
        if isinstance(value, BusinessModelStageReceipt)
        else BusinessModelStageReceipt.model_validate_json(canonical_json_bytes(value))
    )
    expected_scope = BusinessModelStageScope(
        user_id=user_id,
        research_file_id=research_file_id,
        ticker=ticker,
    )
    if receipt.scope != expected_scope:
        raise ValueError("BusinessModel stage receipt belongs to a different scope")
    if receipt.business_model_revision_ref != business_model_revision_ref:
        raise ValueError("BusinessModel stage receipt belongs to a different revision")
    return receipt


def optional_business_model_stage_metadata(
    revision: object,
) -> BusinessModelStageMetadata | None:
    """Decode optional future integration without changing legacy revision meaning."""

    raw = (
        revision.get("stage_metadata")
        if isinstance(revision, Mapping)
        else getattr(revision, "stage_metadata", None)
    )
    if raw is None:
        return None
    return (
        raw
        if isinstance(raw, BusinessModelStageMetadata)
        else BusinessModelStageMetadata.model_validate_json(canonical_json_bytes(raw))
    )


__all__ = [
    "BusinessModelClaimEvidenceBinding",
    "BusinessModelCorpusExcerptLocator",
    "BusinessModelEdgarTableLocator",
    "BusinessModelEvidenceSnapshot",
    "BusinessModelEvidenceValue",
    "BusinessModelMethodologyPin",
    "BusinessModelPortfolioSnapshotLocator",
    "BusinessModelSelectedEvidence",
    "BusinessModelSemanticClaimBinding",
    "BusinessModelStageMetadata",
    "BusinessModelStageReceipt",
    "BusinessModelStageScope",
    "BusinessModelTerminalIssue",
    "BusinessModelTerminalSummary",
    "BusinessModelVendorResponseLocator",
    "BusinessModelXbrlFactLocator",
    "NUMERIC_EVIDENCE_VALUE_KINDS",
    "build_business_model_evidence_snapshot",
    "build_business_model_selected_evidence",
    "build_business_model_stage_metadata",
    "build_business_model_stage_receipt",
    "business_model_evidence_snapshot_digest",
    "business_model_methodology_pin_digest",
    "business_model_selected_evidence_id",
    "business_model_stage_metadata_digest",
    "business_model_stage_receipt_digest",
    "optional_business_model_stage_metadata",
    "remap_business_model_claim_evidence_bindings",
    "validate_business_model_stage_receipt",
]
