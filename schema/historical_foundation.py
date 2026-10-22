"""Immutable code-first historical fact and foundation contracts.

The fact snapshot freezes the complete, server-owned historical denominator and
provider observations.  The mapping plan contains deterministic resolutions and
only material bounded ambiguities.  The final foundation is the accepted
historical basis consumed by forecast and ModelSpec assembly.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
from typing import Any, Literal, Mapping, Protocol, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .build_model_product_contracts import (
    AnnualRequirementCoverage,
    HistoricalBasisFact,
    SourceEvidence,
    validate_historical_reconciliation_source_bindings,
)
from .coverage_profile import AnnualSourceRequirement
from .provider_port.contracts import FilingLineRef
from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_stage import (
    NUMERIC_EVIDENCE_VALUE_KINDS,
    BusinessModelSelectedEvidence,
)
from .forecast_control_profile import (
    ForecastControlProfilePin,
    ForecastControlProfileRef,
    forecast_control_profile_ref as canonical_forecast_control_profile_ref,
)
from .historical_basis_snapshot import (
    HistoricalBasisArtifactRef,
    HistoricalBasisFreshness,
    fiscal_axis_digest,
)
from .historical_driver_seeds import (
    HistoricalDriverSeed,
    HistoricalDriverSeedRequirement,
    historical_driver_seed_denominator_sha256,
    validate_historical_driver_seed_denominator_sha256,
)
from .judgment_revision import (
    Supersession,
    canonical_supersessions,
    validate_revision_fields,
)
from .model_spec_authoring import (
    HistoricalNormalizationDecisionAuthoring,
)
from .models import ModelScale
from industry_slice_core.industry_types import Unit
from .model_state_identity import validate_sha256_hex
from .scale_conversion import Scale, canonical_scale, convert_scale
from .stable_control_identity import FiscalAxis
from industry_slice_core.thesis_shared_sources import SourceRecord
from .ticker_contract import normalize_ticker
from .versioned_artifact import canonical_json_bytes


class _Contract(BaseModel):
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


def _source_seed_measure(
    unit: str | Unit | None,
    scale: str | None,
) -> tuple[Unit, Scale]:
    raw_unit = str(getattr(unit, "value", unit) or "").strip().casefold()
    normalized_unit = raw_unit.replace(" ", "_").replace("-", "_")
    if normalized_unit in {"usd_millions", "dollars_millions"}:
        return Unit.dollars, "millions"
    aliases = {
        "dollars": Unit.dollars,
        "usd": Unit.dollars,
        "count": Unit.item_count,
        "counts": Unit.item_count,
        "units": Unit.item_count,
        "clients": Unit.item_count,
        "share_count": Unit.share_count,
        "percentage": Unit.percentage,
        "ratio": Unit.ratio,
        "per_share": Unit.per_share,
        "days": Unit.days,
        "date": Unit.date,
        "multiple": Unit.multiple,
    }
    canonical_unit = aliases.get(normalized_unit)
    if canonical_unit is None:
        raise ValueError("historical driver seed source unit is not canonical")
    return canonical_unit, canonical_scale(scale)


def _canonical_seed_measure(
    unit: str | Unit | None,
    scale: str | None,
) -> tuple[Unit, ModelScale]:
    """Preserve a source measure only when its scale fits the model contract."""
    canonical_unit, source_scale = _source_seed_measure(unit, scale)
    if source_scale == "units" or source_scale == "millions":
        return canonical_unit, source_scale
    raise ValueError(
        f"historical driver seed source scale {source_scale!r} "
        "requires value conversion to a model scale"
    )


def seed_measure_and_value(
    unit: str | Unit | None,
    scale: str | None,
    value: Decimal,
    *,
    model_scale: ModelScale,
) -> tuple[Unit, Decimal]:
    """Keep the source unit and convert its magnitude to the operand's scale."""
    canonical_unit, source_scale = _source_seed_measure(unit, scale)
    return canonical_unit, convert_scale(
        value,
        source_scale=source_scale,
        target_scale=model_scale,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _sorted_unique(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    if values != tuple(sorted(set(values))) or any(not value for value in values):
        raise ValueError(f"{field_name} must be sorted, unique, and nonblank")
    return values


class HistoricalFoundationNextCall(_Contract):
    """The ``{tool_name, arguments}`` call that resolves one gap."""

    tool_name: str = Field(min_length=1, max_length=128)
    arguments: dict[str, Any] = Field(default_factory=dict)


#: Key prefix of the non-blocking gap HFS records for a seed whose filed source
#: binds no one value (``seed_source_unresolved:<driver hash>:<period>``); the
#: build reports each as ``historical_seed_source_unresolved``.
SEED_SOURCE_UNRESOLVED_GAP_PREFIX = "seed_source_unresolved"

#: Key prefix of the non-blocking gap HFS records for a required forecast
#: control whose applicability the BusinessModel and historical facts leave
#: unknown (``forecast_control_applicability_unresolved:<driver key>``); the
#: control stays in the denominator for the author to answer.
FORECAST_CONTROL_UNRESOLVED_GAP_PREFIX = "forecast_control_applicability_unresolved"

#: Non-blocking historical-spread findings owned by HFF acquisition.
HISTORICAL_BALANCE_SHEET_UNBALANCED_GAP_PREFIX = "historical_balance_sheet_unbalanced"
HISTORICAL_CASH_ROLL_BROKEN_GAP_PREFIX = "historical_cash_roll_broken"
FILED_LINE_UNCLAIMED_GAP_PREFIX = "filed_line_unclaimed"
FILED_LINE_DOUBLE_CLAIMED_GAP_PREFIX = "filed_line_double_claimed"
FILED_VALUE_DISAGREES_GAP_PREFIX = "filed_value_disagrees"


class HistoricalFiledTieOutPeriod(_Contract):
    agree: int = Field(default=0, ge=0)
    disagree: int = Field(default=0, ge=0)
    unclaimed: int = Field(default=0, ge=0)


class HistoricalFiledVintageDifference(_Contract):
    role: str
    fiscal_year: int
    selected_accession: str
    as_filed_accession: str
    selected_value: str
    as_filed_value: str


class HistoricalFiledTieOut(_Contract):
    """Filed comparison against the model-selected vintage, never a render."""

    by_period: dict[str, HistoricalFiledTieOutPeriod] = Field(default_factory=dict)
    vintage_differences: tuple[HistoricalFiledVintageDifference, ...] = ()
    comparisons: tuple[dict[str, Any], ...] = ()


class HistoricalFoundationGap(_Contract):
    key: str = Field(min_length=1, max_length=256)
    text: str = Field(min_length=1, max_length=2000)
    blocking: bool
    repair_target: str | None = Field(default=None, min_length=1, max_length=1000)
    evidence_ids: tuple[str, ...] = ()
    details: dict[str, Any] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    next_call: HistoricalFoundationNextCall | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="evidence_ids")


class HistoricalResolutionFragment(_Contract):
    """Exact server-owned authoring rows selected by a bounded candidate.

    Stage A step A-4 deleted the material-observation arm — statement pairing
    is identity, never an authored resolution. Stage C step C-4 deleted the
    statement-mapping arm with the v1 substrate it authored: a reviewed
    statement window is a corpus/profile-lane artifact in the overrides store,
    not an HFF judgment (governing §5.7, plan §3.5). What remains is the one
    genuine judgment class this fragment was for (design §13).
    """

    historical_normalization_decisions: tuple[
        HistoricalNormalizationDecisionAuthoring, ...
    ] = ()

    @model_validator(mode="after")
    def _nonempty(self) -> "HistoricalResolutionFragment":
        if not self.historical_normalization_decisions:
            raise ValueError("historical resolution fragment cannot be empty")
        return self


class HistoricalMappingCandidate(_Contract):
    candidate_key: str = Field(min_length=1, max_length=256)
    label: str = Field(min_length=1, max_length=512)
    economic_treatment: str = Field(min_length=1, max_length=512)
    requirement_id: str = Field(min_length=1, max_length=256)
    period_id: str = Field(min_length=1, max_length=64)
    required_role: str = Field(min_length=1, max_length=256)
    line: FilingLineRef
    model_item: str = Field(min_length=1, max_length=256)
    unit: str = Field(min_length=1, max_length=128)
    scale: str = Field(min_length=1, max_length=128)
    sign: str = Field(min_length=1, max_length=128)
    rationale: str = Field(min_length=1, max_length=4000)
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    resolution: HistoricalResolutionFragment

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="candidate evidence_ids")


class HistoricalMaterialIssue(_Contract):
    issue_key: str = Field(min_length=1, max_length=256)
    issue_type: Literal[
        "equivalence",
        "reclassification",
        "discontinuity",
        "one_off_normalization",
        "acquisition_comparability",
        "source_conflict",
    ]
    question: str = Field(min_length=1, max_length=4000)
    rationale: str = Field(min_length=1, max_length=4000)
    materiality_basis: str = Field(min_length=1, max_length=2000)
    candidates: tuple[HistoricalMappingCandidate, ...] = Field(
        min_length=2, max_length=16
    )

    @model_validator(mode="after")
    def _candidate_keys(self) -> "HistoricalMaterialIssue":
        keys = tuple(candidate.candidate_key for candidate in self.candidates)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("historical issue candidates must be sorted and unique")
        return self


class HistoricalMappingPlan(_Contract):
    schema_version: Literal["historical-mapping-plan.v1"] = "historical-mapping-plan.v1"
    deterministic_resolution: HistoricalResolutionFragment | None = None
    material_issues: tuple[HistoricalMaterialIssue, ...] = Field(
        default=(), max_length=16
    )
    gaps: tuple[HistoricalFoundationGap, ...] = ()
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")

    @model_validator(mode="after")
    def _closed(self) -> "HistoricalMappingPlan":
        issue_keys = tuple(issue.issue_key for issue in self.material_issues)
        if issue_keys != tuple(sorted(set(issue_keys))):
            raise ValueError("material issue keys must be sorted and unique")
        gap_keys = tuple(gap.key for gap in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("mapping-plan gap keys must be sorted and unique")
        return self


def historical_mapping_plan_digest(
    value: HistoricalMappingPlan | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    )
    payload.pop("content_sha256", None)
    return _digest("historical-mapping-plan.v1", payload)


def build_historical_mapping_plan(
    *,
    deterministic_resolution: HistoricalResolutionFragment | None,
    material_issues: Sequence[HistoricalMaterialIssue],
    gaps: Sequence[HistoricalFoundationGap],
) -> HistoricalMappingPlan:
    payload = {
        "schema_version": "historical-mapping-plan.v1",
        "deterministic_resolution": deterministic_resolution,
        "material_issues": tuple(
            sorted(material_issues, key=lambda row: row.issue_key)
        ),
        "gaps": tuple(sorted(gaps, key=lambda row: row.key)),
    }
    return HistoricalMappingPlan.model_validate(
        {**payload, "content_sha256": historical_mapping_plan_digest(payload)}
    )


class HistoricalFactSnapshotRef(_Contract):
    kind: Literal["historical_fact_snapshot"] = "historical_fact_snapshot"
    snapshot_id: str = Field(pattern=r"^hfs_[0-9a-f]{32}$")
    schema_version: Literal[
        "historical-fact-snapshot.v1",
        "historical-fact-snapshot.v2",
    ] = (
        "historical-fact-snapshot.v1"
    )
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")

    @property
    def artifact_id(self) -> str:
        return self.snapshot_id


def requirement_denominator_digest(
    value: Sequence[AnnualSourceRequirement],
) -> str:
    return _digest(
        "historical-requirement-denominator.v1",
        [row.model_dump(mode="json") for row in value],
    )


class HistoricalSourcePin(_Contract):
    """Exact provider/source identity whose payload produced snapshot facts."""

    source_id: str = Field(min_length=1, max_length=256)
    provider: str = Field(min_length=1, max_length=128)
    accession: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        exclude_if=lambda value: value is None,
    )
    endpoint_or_filing_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=512,
        exclude_if=lambda value: value is None,
    )
    document_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=512,
        exclude_if=lambda value: value is None,
    )
    canonical_payload: dict[str, Any] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    payload_sha256: str
    source_record_sha256: str
    parser_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        exclude_if=lambda value: value is None,
    )
    parser_version: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        exclude_if=lambda value: value is None,
    )
    retrieved_at: datetime
    as_of_date: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        exclude_if=lambda value: value is None,
    )

    @field_validator("payload_sha256")
    @classmethod
    def _payload_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="payload_sha256")

    @field_validator("source_record_sha256")
    @classmethod
    def _source_record_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="source_record_sha256")

    @field_validator("retrieved_at")
    @classmethod
    def _retrieved_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="retrieved_at")

    @model_validator(mode="after")
    def _payload_bound(self) -> "HistoricalSourcePin":
        legacy_values = (
            self.accession,
            self.document_id,
            self.canonical_payload,
            self.parser_name,
            self.parser_version,
            self.as_of_date,
        )
        if self.endpoint_or_filing_id is None:
            if any(value is None for value in legacy_values):
                raise ValueError("historical source pin v1 shape is incomplete")
            assert self.canonical_payload is not None
            observed = hashlib.sha256(
                canonical_json_bytes(self.canonical_payload)
            ).hexdigest()
            if self.payload_sha256 != observed:
                raise ValueError("historical source payload digest mismatch")
        elif any(value is not None for value in legacy_values):
            raise ValueError("historical source pin v2 must use provider identity")
        return self


def build_historical_source_pin_v2(source: SourceRecord) -> HistoricalSourcePin:
    """Bind one retained SourceRecord without reinterpreting provider bytes."""

    key_fields = source.key_fields or {}
    payload_sha256 = key_fields.get("artifact_sha256")
    if not isinstance(payload_sha256, str):
        raise ValueError("historical source pin v2 requires artifact_sha256")
    if source.provider is None or source.endpoint_or_filing_id is None:
        raise ValueError("historical source pin v2 requires provider identity")
    if source.retrieved_at is None:
        raise ValueError("historical source pin v2 requires retrieval time")
    try:
        retrieved_at = datetime.fromisoformat(source.retrieved_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("historical source pin v2 retrieval time is invalid") from exc
    return HistoricalSourcePin(
        source_id=str(source.id),
        provider=source.provider,
        endpoint_or_filing_id=source.endpoint_or_filing_id,
        payload_sha256=payload_sha256,
        source_record_sha256=source_record_digest(source),
        retrieved_at=retrieved_at,
    )


def source_record_digest(value: SourceRecord | Mapping[str, Any]) -> str:
    payload = (
        value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    )
    return _digest("historical-source-record.v1", payload)


def historical_source_manifest_digest(value: Sequence[HistoricalSourcePin]) -> str:
    versions = {
        getattr(row, "endpoint_or_filing_id", None) is not None for row in value
    }
    if len(versions) > 1:
        raise ValueError("historical source pin versions cannot be mixed")
    version = (
        "historical-source-manifest.v2"
        if versions == {True}
        else "historical-source-manifest.v1"
    )
    return _digest(
        version,
        [row.model_dump(mode="json") for row in value],
    )


class _HistoricalSourcePayloadIdentity(Protocol):
    @property
    def source_id(self) -> str: ...

    @property
    def provider(self) -> str: ...

    @property
    def accession(self) -> str | None: ...

    @property
    def document_id(self) -> str | None: ...

    @property
    def endpoint_or_filing_id(self) -> str | None: ...

    @property
    def payload_sha256(self) -> str: ...

    @property
    def parser_name(self) -> str | None: ...

    @property
    def parser_version(self) -> str | None: ...


def historical_source_payload_set_digest(
    value: Sequence[_HistoricalSourcePayloadIdentity],
) -> str:
    """Digest only immutable raw provider bytes, never acquisition clocks.

    One set is one pin version: v1 rows and v2 rows never mix. A snapshot
    whose pin grounds no fact (the empty pin) retains no source, and the empty
    set digests as v1.
    """

    versions = {
        getattr(row, "endpoint_or_filing_id", None) is not None for row in value
    }
    if versions <= {False}:
        return _digest(
            "historical-source-payload-set.v1",
            [
                {
                    "source_id": row.source_id,
                    "provider": row.provider,
                    "accession": row.accession,
                    "document_id": row.document_id,
                    "payload_sha256": row.payload_sha256,
                    "parser_name": row.parser_name,
                    "parser_version": row.parser_version,
                }
                for row in value
            ],
        )
    if versions != {True}:
        raise ValueError("historical source pin versions cannot be mixed")
    return _digest(
        "historical-source-payload-set.v2",
        [
            {
                "source_id": row.source_id,
                "provider": row.provider,
                "endpoint_or_filing_id": row.endpoint_or_filing_id,
                "payload_sha256": row.payload_sha256,
            }
            for row in value
        ],
    )


class HistoricalFactSnapshot(_Contract):
    schema_version: Literal[
        "historical-fact-snapshot.v1",
        "historical-fact-snapshot.v2",
    ] = (
        "historical-fact-snapshot.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: HistoricalFactSnapshotRef
    status: Literal["ready", "blocked"]
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    business_model_driver_keys: tuple[str, ...] = Field(min_length=1)
    forecast_driver_keys: tuple[str, ...] = Field(min_length=1)
    forecast_control_profile_ref: ForecastControlProfileRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    forecast_control_denominator_sha256: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    historical_driver_seed_requirements: tuple[
        HistoricalDriverSeedRequirement, ...
    ] = Field(default=(), exclude_if=lambda value: not value)
    historical_driver_seed_denominator_sha256: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    historical_spread_revision: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    requirement_denominator: tuple[AnnualSourceRequirement, ...] = Field(min_length=1)
    requirement_denominator_sha256: str
    #: Sources grounding numeric facts or per-period absence citations; an
    #: entirely unbound pin retains none.
    source_manifest: tuple[HistoricalSourcePin, ...]
    source_payload_sha256: str
    fiscal_axis: FiscalAxis
    fiscal_axis_sha256: str
    freshness: HistoricalBasisFreshness
    facts: tuple[HistoricalBasisFact, ...] = ()
    evidence: tuple[SourceEvidence, ...] = ()
    coverage: tuple[AnnualRequirementCoverage, ...] = ()
    sources: tuple[SourceRecord, ...] = ()
    mapping_plan: HistoricalMappingPlan
    gaps: tuple[HistoricalFoundationGap, ...] = ()
    filed_tie_out: HistoricalFiledTieOut = Field(
        default_factory=HistoricalFiledTieOut,
        exclude_if=lambda value: not value.by_period,
    )
    created_at: datetime

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        canonical = normalize_ticker(value)
        if canonical != value:
            raise ValueError("ticker must be canonical")
        return canonical

    @field_validator("business_model_stage_receipt_digest")
    @classmethod
    def _business_model_receipt_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="business_model_stage_receipt_digest",
        )

    @field_validator("requirement_denominator_sha256")
    @classmethod
    def _requirement_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="requirement_denominator_sha256",
        )

    @field_validator("source_payload_sha256")
    @classmethod
    def _source_payload_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="source_payload_sha256")

    @field_validator("fiscal_axis_sha256")
    @classmethod
    def _fiscal_axis_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="fiscal_axis_sha256")

    @field_validator("forecast_control_denominator_sha256")
    @classmethod
    def _optional_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(
            value,
            field_name="forecast_control_denominator_sha256",
        )

    @field_validator("historical_driver_seed_denominator_sha256")
    @classmethod
    def _optional_seed_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_historical_driver_seed_denominator_sha256(value)

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _closed(self) -> "HistoricalFactSnapshot":
        has_reconciliations = any(
            row.reconciliation is not None for row in self.facts
        )
        if (self.schema_version == "historical-fact-snapshot.v2") != (
            has_reconciliations
        ):
            raise ValueError(
                "historical-fact snapshot v2 is required exactly for reconciliations"
            )
        if self.ref.schema_version != self.schema_version:
            raise ValueError("historical-fact snapshot ref version must match payload")
        validate_historical_reconciliation_source_bindings(
            self.facts,
            self.sources,
            allow_reconciliations=has_reconciliations,
        )
        if (
            self.business_model_revision_ref.business_model_id
            != f"{self.ticker}_business_model"
        ):
            raise ValueError(
                "BusinessModel revision ref does not match snapshot ticker"
            )
        requirements = tuple(row.requirement_id for row in self.requirement_denominator)
        _sorted_unique(
            self.business_model_driver_keys,
            field_name="business_model_driver_keys",
        )
        _sorted_unique(
            self.forecast_driver_keys,
            field_name="forecast_driver_keys",
        )
        if (self.forecast_control_profile_ref is None) != (
            self.forecast_control_denominator_sha256 is None
        ):
            raise ValueError(
                "forecast-control profile and denominator digest must be paired"
            )
        seed_requirements = self.historical_driver_seed_requirements
        if bool(seed_requirements) != bool(
            self.historical_driver_seed_denominator_sha256
        ):
            raise ValueError(
                "historical driver seed requirements and digest must be paired"
            )
        seed_keys = tuple(row.driver_key for row in seed_requirements)
        seed_ids = tuple(row.driver_id for row in seed_requirements)
        if seed_keys != tuple(sorted(set(seed_keys))) or len(seed_ids) != len(
            set(seed_ids)
        ):
            raise ValueError(
                "historical driver seed requirements must have unique stable identity"
            )
        annual_period_ids = tuple(
            row.period_id
            for row in self.fiscal_axis.historical_periods
            if row.period_type == "annual" and not row.is_estimate
        )
        if seed_requirements:
            if not annual_period_ids:
                raise ValueError(
                    "historical driver seed requirements need an annual fiscal axis"
                )
            terminal_period_id = annual_period_ids[-1]
            for seed_requirement in seed_requirements:
                if seed_requirement.period_id != terminal_period_id:
                    raise ValueError(
                        "historical driver seed requirement must bind terminal history"
                    )
                if seed_requirement.predecessor_period_id is not None and (
                    len(annual_period_ids) < 2
                    or seed_requirement.predecessor_period_id
                    != annual_period_ids[-2]
                ):
                    raise ValueError(
                        "historical driver seed predecessor must be adjacent history"
                    )
        if requirements != tuple(sorted(set(requirements))):
            raise ValueError("historical requirements must be sorted and unique")
        source_ids = tuple(row.source_id for row in self.source_manifest)
        if source_ids != tuple(sorted(set(source_ids))):
            raise ValueError("historical source pins must be sorted and unique")
        if any(
            (row.endpoint_or_filing_id is not None) != has_reconciliations
            for row in self.source_manifest
        ):
            raise ValueError("historical source pin version must match snapshot")
        source_records = {str(row.id): row for row in self.sources}
        if tuple(sorted(source_records)) != source_ids:
            raise ValueError("historical source pins must equal retained SourceRecords")
        for pin in self.source_manifest:
            source_record = source_records[pin.source_id]
            if pin.source_record_sha256 != source_record_digest(source_record):
                raise ValueError("historical source pin differs from SourceRecord")
            key_fields = source_record.key_fields
            if pin.endpoint_or_filing_id is None:
                matches = (
                    key_fields is not None
                    and source_record.provider == pin.provider
                    and source_record.endpoint_or_filing_id == pin.accession
                    and key_fields.get("document_id") == pin.document_id
                    and key_fields.get("response_sha256") == pin.payload_sha256
                    and key_fields.get("parser_name") == pin.parser_name
                    and key_fields.get("parser_version") == pin.parser_version
                    and key_fields.get("as_of_date") == pin.as_of_date
                    and source_record.retrieved_at == pin.retrieved_at.isoformat()
                    and source_record.identity_hash == pin.payload_sha256
                )
            else:
                matches = (
                    key_fields is not None
                    and source_record.provider == pin.provider
                    and source_record.endpoint_or_filing_id
                    == pin.endpoint_or_filing_id
                    and key_fields.get("artifact_sha256") == pin.payload_sha256
                    and source_record.retrieved_at == pin.retrieved_at.isoformat()
                )
            if not matches:
                raise ValueError(
                    "historical source pin metadata differs from SourceRecord"
                )
        fact_ids = tuple(row.fact_id for row in self.facts)
        evidence_ids = tuple(row.evidence_id for row in self.evidence)
        coverage_ids = tuple(row.requirement_id for row in self.coverage)
        if fact_ids != tuple(sorted(set(fact_ids))):
            raise ValueError("historical facts must be sorted and unique")
        if evidence_ids != tuple(sorted(set(evidence_ids))):
            raise ValueError("historical evidence must be sorted and unique")
        if coverage_ids != tuple(sorted(set(coverage_ids))):
            raise ValueError("historical coverage must be sorted and unique")
        if coverage_ids != requirements:
            raise ValueError(
                "historical coverage must equal the exact requirement denominator"
            )
        requirements_by_id = {
            row.requirement_id: row for row in self.requirement_denominator
        }
        retained_evidence_ids = {*fact_ids, *evidence_ids}
        referenced_source_ids = {
            *(str(row.source_id) for row in self.facts),
            *(str(row.source_id) for row in self.evidence),
            *(
                str(period.disclosure_search_source_id)
                for row in self.coverage
                for period in row.periods
                if period.disclosure_search_source_id is not None
            ),
            *(
                str(parent_id)
                for row in self.facts
                if row.reconciliation is not None
                for parent_id in row.reconciliation.raw_parent_source_ids
            ),
            *(
                str(ref.source_id)
                for row in self.facts
                if row.reconciliation is not None
                for ref in row.reconciliation.definition_refs
            ),
        }
        if referenced_source_ids != set(source_ids):
            raise ValueError(
                "historical facts/evidence/coverage must close to the exact source manifest"
            )
        pins_by_id = {pin.source_id: pin for pin in self.source_manifest}
        for row in self.coverage:
            expected_periods = requirements_by_id[row.requirement_id].period_ids
            observed_periods = tuple(period.period_id for period in row.periods)
            if observed_periods != expected_periods:
                raise ValueError(
                    "historical coverage periods must equal the exact requirement periods"
                )
            for period in row.periods:
                if period.disclosure_search_source_id is not None:
                    source = pins_by_id[str(period.disclosure_search_source_id)]
                    if source.accession != period.disclosure_search_accession:
                        raise ValueError(
                            "historical absence coverage must match its source accession"
                        )
                    if period.disposition in {
                        "not_applicable_evidenced", "not_separately_presented"
                    }:
                        cell = requirements_by_id[row.requirement_id].cell_for_period(
                            period.period_id
                        )
                        citation = cell.absence_citation
                        if (
                            citation is None
                            or citation.accession != period.disclosure_search_accession
                        ):
                            raise ValueError(
                                "historical absence coverage must match its period citation"
                            )
                        if period.disposition == "not_separately_presented" and (
                            cell.binding_state != "not_separately_presented"
                            or period.included_in != cell.included_in
                            or period.absence_citation != citation
                            or period.disclosure_search_basis != citation.presentation_role_uri
                        ):
                            raise ValueError(
                                "historical included coverage must match its exact period judgment"
                            )
                referenced = {
                    *period.evidence_ids,
                    *period.applicability_evidence_ids,
                    *period.conflict_evidence_ids,
                }
                if not referenced <= retained_evidence_ids:
                    raise ValueError(
                        "historical coverage references evidence outside the snapshot"
                    )
        # Stage A step A-4: the mapping-plan closure over the supported
        # denominator is retired with the material-observation arm — the
        # bound snapshot is itself total (coverage equals the exact
        # denominator with a typed disposition per period), and supported
        # facts land by role binding at projection, fail-closed. The
        # bounded-candidate lane survives for its statement/normalization
        # arms; candidate metadata stays denominator-checked.
        for issue in self.mapping_plan.material_issues:
            for candidate in issue.candidates:
                if candidate.requirement_id not in requirements_by_id:
                    raise ValueError(
                        "historical mapping candidate is outside the denominator"
                    )
                requirement = requirements_by_id[candidate.requirement_id]
                pinned_by_period = {
                    line.period.canonical_label(): line
                    for line in requirement.pinned_columns
                }
                if (
                    candidate.period_id not in requirement.period_ids
                    or candidate.required_role != requirement.required_role
                    or candidate.line != pinned_by_period.get(candidate.period_id)
                    or candidate.model_item != requirement.model_item
                    or candidate.unit != requirement.unit
                    or candidate.scale != requirement.scale
                    or candidate.sign != requirement.sign
                ):
                    raise ValueError(
                        "historical mapping candidate differs from requirement semantics"
                    )
                if not set(candidate.evidence_ids) <= retained_evidence_ids:
                    raise ValueError(
                        "historical mapping candidate references unretained evidence"
                    )
        for gap in (*self.gaps, *self.mapping_plan.gaps):
            if not set(gap.evidence_ids) <= retained_evidence_ids:
                raise ValueError("historical gap references unretained evidence")
        gap_keys = tuple(row.key for row in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("historical snapshot gaps must be sorted and unique")
        blocking = any(row.blocking for row in (*self.gaps, *self.mapping_plan.gaps))
        if self.status == "ready" and blocking:
            raise ValueError("ready historical fact snapshot requires unblocked inputs")
        if self.status == "blocked" and not blocking:
            raise ValueError(
                "blocked historical fact snapshot requires a blocking reason"
            )
        # Data integrity: this digest covers every stored byte of the snapshot,
        # nested mapping plan and copied sub-digests included, so a mismatch
        # means the persisted row was corrupted after this system wrote it.
        if self.ref.content_sha256 != historical_fact_snapshot_digest(self):
            raise ValueError("historical fact snapshot content digest mismatch")
        return self


def historical_fact_snapshot_digest(
    value: HistoricalFactSnapshot | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    )
    payload.pop("ref", None)
    if payload.get("forecast_control_profile_ref") is None:
        payload.pop("forecast_control_profile_ref", None)
    if payload.get("forecast_control_denominator_sha256") is None:
        payload.pop("forecast_control_denominator_sha256", None)
    if not payload.get("historical_driver_seed_requirements"):
        payload.pop("historical_driver_seed_requirements", None)
    if payload.get("historical_driver_seed_denominator_sha256") is None:
        payload.pop("historical_driver_seed_denominator_sha256", None)
    if payload.get("historical_spread_revision") is None:
        payload.pop("historical_spread_revision", None)
    version = str(payload.get("schema_version", "historical-fact-snapshot.v1"))
    return _digest(version, payload)


def build_historical_fact_snapshot(
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    status: Literal["ready", "blocked"],
    business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    business_model_driver_keys: Sequence[str],
    forecast_driver_keys: Sequence[str],
    requirement_denominator: Sequence[AnnualSourceRequirement],
    source_manifest: Sequence[HistoricalSourcePin],
    fiscal_axis: FiscalAxis,
    freshness: HistoricalBasisFreshness,
    facts: Sequence[HistoricalBasisFact],
    evidence: Sequence[SourceEvidence],
    coverage: Sequence[AnnualRequirementCoverage],
    sources: Sequence[SourceRecord],
    mapping_plan: HistoricalMappingPlan,
    gaps: Sequence[HistoricalFoundationGap],
    created_at: datetime,
    forecast_control_profile_ref: (
        ForecastControlProfileRef | ForecastControlProfilePin | None
    ) = None,
    forecast_control_denominator_sha256: str | None = None,
    historical_driver_seed_requirements: Sequence[
        HistoricalDriverSeedRequirement
    ] = (),
    historical_spread_revision: str | None = None,
    filed_tie_out: HistoricalFiledTieOut | None = None,
) -> HistoricalFactSnapshot:
    requirements = tuple(
        sorted(requirement_denominator, key=lambda row: row.requirement_id)
    )
    canonical_sources = tuple(sorted(sources, key=lambda row: str(row.id)))
    canonical_source_manifest = tuple(
        sorted(source_manifest, key=lambda row: row.source_id)
    )
    canonical_facts = tuple(sorted(facts, key=lambda row: row.fact_id))
    schema_version = (
        "historical-fact-snapshot.v2"
        if any(row.reconciliation is not None for row in canonical_facts)
        else "historical-fact-snapshot.v1"
    )
    payload = {
        "schema_version": schema_version,
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "status": status,
        "business_model_revision_ref": business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "business_model_driver_keys": tuple(sorted(set(business_model_driver_keys))),
        "forecast_driver_keys": tuple(sorted(set(forecast_driver_keys))),
        "requirement_denominator": requirements,
        "requirement_denominator_sha256": requirement_denominator_digest(requirements),
        "source_manifest": canonical_source_manifest,
        "source_payload_sha256": historical_source_payload_set_digest(
            canonical_source_manifest
        ),
        "fiscal_axis": fiscal_axis,
        "fiscal_axis_sha256": fiscal_axis_digest(fiscal_axis),
        "freshness": freshness,
        "facts": canonical_facts,
        "evidence": tuple(sorted(evidence, key=lambda row: row.evidence_id)),
        "coverage": tuple(sorted(coverage, key=lambda row: row.requirement_id)),
        "sources": canonical_sources,
        "mapping_plan": mapping_plan,
        "gaps": tuple(sorted(gaps, key=lambda row: row.key)),
        "created_at": created_at,
    }
    if filed_tie_out is not None and filed_tie_out.by_period:
        payload["filed_tie_out"] = filed_tie_out
    if historical_spread_revision is not None:
        payload["historical_spread_revision"] = historical_spread_revision
    if forecast_control_profile_ref is not None:
        payload["forecast_control_profile_ref"] = canonical_forecast_control_profile_ref(
            forecast_control_profile_ref
        )
    if forecast_control_denominator_sha256 is not None:
        payload["forecast_control_denominator_sha256"] = (
            forecast_control_denominator_sha256
        )
    seed_requirements = tuple(
        sorted(historical_driver_seed_requirements, key=lambda row: row.driver_key)
    )
    if seed_requirements:
        payload["historical_driver_seed_requirements"] = seed_requirements
        payload["historical_driver_seed_denominator_sha256"] = (
            historical_driver_seed_denominator_sha256(seed_requirements)
        )
    content_sha256 = historical_fact_snapshot_digest(payload)
    ref = HistoricalFactSnapshotRef(
        snapshot_id=f"hfs_{content_sha256[:32]}",
        schema_version=schema_version,
        content_sha256=content_sha256,
    )
    return HistoricalFactSnapshot.model_validate({**payload, "ref": ref})


class HistoricalIssueResolution(_Contract):
    issue_key: str = Field(min_length=1, max_length=256)
    selected_candidate_key: str = Field(min_length=1, max_length=256)
    rationale: str = Field(min_length=1, max_length=4000)
    confidence: Literal["low", "medium", "high"]
    evidence_ids: tuple[str, ...] = ()

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="resolution evidence_ids")


class HistoricalFoundationRef(HistoricalBasisArtifactRef, frozen=True):
    kind: Literal["historical_foundation"] = "historical_foundation"
    artifact_id: str = Field(pattern=r"^hff_[0-9a-f]{32}$")
    schema_version: Literal[
        "historical-foundation.v1",
        "historical-foundation.v2",
    ] = "historical-foundation.v1"


class HistoricalFoundationStageScope(_Contract):
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)


class HistoricalFoundationStageReceipt(_Contract):
    schema_version: Literal[
        "historical-foundation-stage-receipt.v1",
        "historical-foundation-stage-receipt.v2",
    ] = (
        "historical-foundation-stage-receipt.v1"
    )
    stage: Literal["historical-foundation"] = "historical-foundation"
    status: Literal["accepted", "incomplete", "blocked"]
    scope: HistoricalFoundationStageScope
    historical_foundation_ref: HistoricalFoundationRef
    historical_fact_snapshot_ref: HistoricalFactSnapshotRef
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    started_at: datetime
    completed_at: datetime
    receipt_digest: str

    @field_validator("business_model_stage_receipt_digest")
    @classmethod
    def _business_model_receipt_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="business_model_stage_receipt_digest",
        )

    @field_validator("receipt_digest")
    @classmethod
    def _receipt_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="receipt_digest")

    @field_validator("started_at")
    @classmethod
    def _started_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="started_at")

    @field_validator("completed_at")
    @classmethod
    def _completed_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="completed_at")

    @model_validator(mode="after")
    def _closed(self) -> "HistoricalFoundationStageReceipt":
        if self.completed_at < self.started_at:
            raise ValueError("historical-foundation receipt completes before it starts")
        expected_version = (
            "historical-foundation-stage-receipt.v2"
            if self.historical_foundation_ref.schema_version
            == "historical-foundation.v2"
            else "historical-foundation-stage-receipt.v1"
        )
        if self.schema_version != expected_version:
            raise ValueError("historical-foundation receipt version must match artifact")
        if self.receipt_digest != historical_foundation_receipt_digest(self):
            raise ValueError("historical-foundation receipt digest mismatch")
        return self


def historical_foundation_receipt_digest(
    value: HistoricalFoundationStageReceipt | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    )
    payload.pop("receipt_digest", None)
    version = str(
        payload.get(
            "schema_version",
            "historical-foundation-stage-receipt.v1",
        )
    )
    return _digest(version, payload)


class HistoricalFoundationTerminalSummary(_Contract):
    summary: str = Field(min_length=1, max_length=6000)
    recommended_next_action: str = Field(min_length=1, max_length=2000)


class HistoricalFoundation(_Contract):
    schema_version: Literal[
        "historical-foundation.v1",
        "historical-foundation.v2",
    ] = "historical-foundation.v1"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: HistoricalFoundationRef
    status: Literal["accepted", "incomplete", "blocked"]
    completion_mode: Literal["deterministic", "agent_assisted"]
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    historical_fact_snapshot_ref: HistoricalFactSnapshotRef
    business_model_driver_keys: tuple[str, ...] = Field(min_length=1)
    forecast_driver_keys: tuple[str, ...] = Field(min_length=1)
    forecast_control_profile_ref: ForecastControlProfileRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    forecast_control_denominator_sha256: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    requirement_denominator: tuple[AnnualSourceRequirement, ...] = Field(min_length=1)
    requirement_denominator_sha256: str
    mapping_plan: HistoricalMappingPlan
    mapping_plan_sha256: str
    fiscal_axis: FiscalAxis
    fiscal_axis_sha256: str
    freshness: HistoricalBasisFreshness
    issue_resolutions: tuple[HistoricalIssueResolution, ...] = ()
    unresolved_issue_keys: tuple[str, ...] = ()
    historical_normalization_decisions: tuple[
        HistoricalNormalizationDecisionAuthoring, ...
    ] = ()
    historical_driver_seed_requirements: tuple[
        HistoricalDriverSeedRequirement, ...
    ] = Field(default=(), exclude_if=lambda value: not value)
    historical_driver_seed_denominator_sha256: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    historical_driver_seeds: tuple[HistoricalDriverSeed, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    historical_basis_facts: tuple[HistoricalBasisFact, ...] = ()
    evidence: tuple[SourceEvidence, ...] = ()
    judgment_evidence: tuple[BusinessModelSelectedEvidence, ...] = ()
    coverage: tuple[AnnualRequirementCoverage, ...] = ()
    source_manifest: tuple[HistoricalSourcePin, ...]
    sources: tuple[SourceRecord, ...] = ()
    gaps: tuple[HistoricalFoundationGap, ...] = ()
    filed_tie_out: HistoricalFiledTieOut = Field(
        default_factory=HistoricalFiledTieOut,
        exclude_if=lambda value: not value.by_period,
    )
    terminal_summary: HistoricalFoundationTerminalSummary
    created_by: str = Field(min_length=1)
    created_at: datetime
    # The Revision fields (``schema/judgment_revision.py``): absent on a first
    # foundation and on every foundation persisted before them.
    predecessor_ref: HistoricalFoundationRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    #: One per predecessor normalization decision (by ``label``) this
    #: foundation changes or drops.
    supersessions: tuple[Supersession, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    stage_receipt: HistoricalFoundationStageReceipt

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        canonical = normalize_ticker(value)
        if canonical != value:
            raise ValueError("ticker must be canonical")
        return canonical

    @field_validator("business_model_stage_receipt_digest")
    @classmethod
    def _business_model_receipt_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="business_model_stage_receipt_digest",
        )

    @field_validator("requirement_denominator_sha256")
    @classmethod
    def _requirement_digest(cls, value: str) -> str:
        return validate_sha256_hex(
            value,
            field_name="requirement_denominator_sha256",
        )

    @field_validator("mapping_plan_sha256")
    @classmethod
    def _mapping_plan_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="mapping_plan_sha256")

    @field_validator("fiscal_axis_sha256")
    @classmethod
    def _fiscal_axis_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="fiscal_axis_sha256")

    @field_validator("forecast_control_denominator_sha256")
    @classmethod
    def _optional_forecast_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(
            value,
            field_name="forecast_control_denominator_sha256",
        )

    @field_validator("historical_driver_seed_denominator_sha256")
    @classmethod
    def _optional_seed_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_historical_driver_seed_denominator_sha256(value)

    @field_validator("unresolved_issue_keys")
    @classmethod
    def _unresolved(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="unresolved_issue_keys")

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _closed(self) -> "HistoricalFoundation":
        has_reconciliations = any(
            row.reconciliation is not None for row in self.historical_basis_facts
        )
        if (self.schema_version == "historical-foundation.v2") != has_reconciliations:
            raise ValueError(
                "historical foundation v2 is required exactly for reconciliations"
            )
        if self.ref.schema_version != self.schema_version:
            raise ValueError("historical foundation ref version must match payload")
        if (
            self.historical_fact_snapshot_ref.schema_version
            != self.schema_version.replace("historical-foundation", "historical-fact-snapshot")
        ):
            raise ValueError("historical foundation snapshot ref version must match")
        validate_historical_reconciliation_source_bindings(
            self.historical_basis_facts,
            self.sources,
            allow_reconciliations=has_reconciliations,
        )
        if (
            self.business_model_revision_ref.business_model_id
            != f"{self.ticker}_business_model"
        ):
            raise ValueError(
                "BusinessModel revision ref does not match foundation ticker"
            )
        requirement_ids = tuple(
            row.requirement_id for row in self.requirement_denominator
        )
        _sorted_unique(
            self.business_model_driver_keys,
            field_name="business_model_driver_keys",
        )
        _sorted_unique(
            self.forecast_driver_keys,
            field_name="forecast_driver_keys",
        )
        if (self.forecast_control_profile_ref is None) != (
            self.forecast_control_denominator_sha256 is None
        ):
            raise ValueError(
                "forecast-control profile and denominator digest must be paired"
            )
        seed_requirements = self.historical_driver_seed_requirements
        seed_requirement_keys = tuple(row.driver_key for row in seed_requirements)
        seed_requirement_ids = tuple(row.driver_id for row in seed_requirements)
        if bool(seed_requirements) != bool(
            self.historical_driver_seed_denominator_sha256
        ):
            raise ValueError(
                "historical driver seed requirements and digest must be paired"
            )
        if seed_requirement_keys != tuple(sorted(set(seed_requirement_keys))):
            raise ValueError(
                "historical driver seed requirements must be sorted and unique"
            )
        if len(seed_requirement_ids) != len(set(seed_requirement_ids)):
            raise ValueError("historical driver seed requirements repeat driver_id")
        seeds = self.historical_driver_seeds
        seed_keys = tuple(row.driver_key for row in seeds)
        seed_ids = tuple(row.driver_id for row in seeds)
        if seed_keys != tuple(sorted(set(seed_keys))):
            raise ValueError("historical driver seeds must be sorted and unique")
        if len(seed_ids) != len(set(seed_ids)):
            raise ValueError("historical driver seeds repeat driver_id")
        requirements_by_key = {row.driver_key: row for row in seed_requirements}
        for seed in seeds:
            requirement = requirements_by_key.get(seed.driver_key)
            if requirement is None or (
                seed.driver_id,
                seed.period_id,
                seed.unit,
                seed.model_scale,
            ) != (
                requirement.driver_id,
                requirement.period_id,
                requirement.unit,
                requirement.model_scale,
            ):
                raise ValueError(
                    "historical driver seed differs from its exact requirement"
                )
        if self.status == "accepted" and seed_requirements and (
            seed_keys != seed_requirement_keys
        ):
            raise ValueError(
                "accepted foundation requires exact historical driver seed bijection"
            )
        if requirement_ids != tuple(sorted(set(requirement_ids))):
            raise ValueError("foundation requirements must be sorted and unique")
        resolution_keys = tuple(row.issue_key for row in self.issue_resolutions)
        if resolution_keys != tuple(sorted(set(resolution_keys))):
            raise ValueError("historical issue resolutions must be sorted and unique")
        gap_keys = tuple(row.key for row in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("historical-foundation gaps must be sorted and unique")
        if self.completion_mode == "deterministic" and self.issue_resolutions:
            raise ValueError(
                "deterministic foundation cannot contain agent resolutions"
            )
        if self.completion_mode == "deterministic" and any(
            seed.disposition not in {"observed", "derived"}
            or seed.derivation_kind == "allocation"
            or any(
                operand.kind == "analyst_literal"
                for operand in seed.derivation_operands
            )
            for seed in seeds
        ):
            raise ValueError(
                "deterministic foundation requires reproducible observed/derived seeds"
            )
        offered_issues = {
            row.issue_key: row for row in self.mapping_plan.material_issues
        }
        if self.completion_mode == "deterministic" and offered_issues:
            raise ValueError(
                "deterministic foundation cannot bypass material ambiguity"
            )
        if (
            self.completion_mode == "agent_assisted"
            and not offered_issues
            and not seed_requirements
        ):
            raise ValueError(
                "agent-assisted foundation requires material issues or driver seeds"
            )
        if set(resolution_keys) & set(self.unresolved_issue_keys):
            raise ValueError("historical issues cannot be resolved and unresolved")
        if {*resolution_keys, *self.unresolved_issue_keys} != set(offered_issues):
            raise ValueError(
                "historical resolutions and unresolved keys must partition exact issues"
            )
        for resolution in self.issue_resolutions:
            offered_candidates = {
                row.candidate_key
                for row in offered_issues[resolution.issue_key].candidates
            }
            if resolution.selected_candidate_key not in offered_candidates:
                raise ValueError(
                    "historical resolution must select an offered candidate"
                )
        if self.status == "accepted" and (
            self.unresolved_issue_keys or any(row.blocking for row in self.gaps)
        ):
            raise ValueError("accepted foundation requires fully resolved history")
        if self.status == "incomplete" and not (
            self.unresolved_issue_keys or self.gaps
        ):
            raise ValueError("incomplete foundation requires unresolved issues or gaps")
        if self.status == "blocked" and not (
            any(row.blocking for row in self.gaps)
            or any(row.blocking for row in self.mapping_plan.gaps)
        ):
            raise ValueError("blocked foundation requires a blocking prerequisite")
        retained_evidence_ids = {
            *(row.fact_id for row in self.historical_basis_facts),
            *(row.evidence_id for row in self.evidence),
            *(row.evidence_id for row in self.judgment_evidence),
        }
        evidence_ids = tuple(row.evidence_id for row in self.evidence)
        judgment_evidence_ids = tuple(row.evidence_id for row in self.judgment_evidence)
        if evidence_ids != tuple(sorted(set(evidence_ids))):
            raise ValueError("foundation source evidence must be sorted and unique")
        if judgment_evidence_ids != tuple(sorted(set(judgment_evidence_ids))):
            raise ValueError("foundation judgment evidence must be sorted and unique")
        if set(evidence_ids) & set(judgment_evidence_ids):
            raise ValueError("foundation evidence namespaces cannot overlap")
        source_ids = tuple(row.source_id for row in self.source_manifest)
        if source_ids != tuple(sorted(set(source_ids))):
            raise ValueError("foundation source pins must be sorted and unique")
        source_records = {str(row.id): row for row in self.sources}
        if tuple(sorted(source_records)) != source_ids:
            raise ValueError("foundation source pins must equal retained SourceRecords")
        for pin in self.source_manifest:
            source_record = source_records[pin.source_id]
            if pin.source_record_sha256 != source_record_digest(source_record):
                raise ValueError("foundation source pin differs from SourceRecord")
        referenced_source_ids = {
            *(str(row.source_id) for row in self.historical_basis_facts),
            *(str(row.source_id) for row in self.evidence),
            *(
                str(period.disclosure_search_source_id)
                for row in self.coverage
                for period in row.periods
                if period.disclosure_search_source_id is not None
            ),
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
        if referenced_source_ids != set(source_ids):
            raise ValueError("foundation evidence must close to its source manifest")
        coverage_by_id = {row.requirement_id: row for row in self.coverage}
        if tuple(sorted(coverage_by_id)) != requirement_ids:
            raise ValueError("foundation coverage must equal exact denominator")
        requirements_by_id = {
            row.requirement_id: row for row in self.requirement_denominator
        }
        for requirement_id, row in coverage_by_id.items():
            if tuple(period.period_id for period in row.periods) != (
                requirements_by_id[requirement_id].period_ids
            ):
                raise ValueError("foundation coverage periods differ from denominator")
            for period in row.periods:
                referenced = {
                    *period.evidence_ids,
                    *period.applicability_evidence_ids,
                    *period.conflict_evidence_ids,
                }
                if not referenced <= retained_evidence_ids:
                    raise ValueError(
                        "foundation coverage references unretained evidence"
                    )
        for row in self.historical_normalization_decisions:
            if not set(row.evidence_ids) <= retained_evidence_ids:
                raise ValueError(
                    "historical normalization references unretained evidence"
                )
        for row in self.issue_resolutions:
            if not set(row.evidence_ids) <= retained_evidence_ids:
                raise ValueError(
                    "historical issue resolution references unretained evidence"
                )
        for gap in self.gaps:
            if not set(gap.evidence_ids) <= retained_evidence_ids:
                raise ValueError(
                    "historical-foundation gap references unretained evidence"
                )
        facts_by_id = {row.fact_id: row for row in self.historical_basis_facts}
        judgment_by_id = {
            row.evidence_id: row for row in self.judgment_evidence
        }
        annual_period_ids = tuple(
            row.period_id
            for row in self.fiscal_axis.historical_periods
            if row.period_type == "annual" and not row.is_estimate
        )
        for seed in seeds:
            requirement = requirements_by_key[seed.driver_key]
            if seed.derivation_kind not in requirement.allowed_derivation_kinds:
                raise ValueError(
                    "historical driver seed derivation is not allowed by BMR intent"
                )
            if not {
                *seed.evidence_ids,
                *seed.supporting_evidence_ids,
            } <= set(judgment_by_id):
                raise ValueError(
                    "historical driver seed evidence must name retained BMR evidence"
                )
            if not set(seed.fact_ids) <= set(facts_by_id):
                raise ValueError("historical driver seed references unretained facts")
            for operand in seed.derivation_operands:
                if operand.kind == "historical_fact":
                    fact = facts_by_id.get(str(operand.fact_id))
                    if fact is None:
                        raise ValueError(
                            "historical driver seed fact operand is not retained"
                        )
                    if (
                        (operand.unit, operand.value)
                        != seed_measure_and_value(
                            fact.unit,
                            fact.scale,
                            fact.value,
                            model_scale=operand.model_scale,
                        )
                        or fact.period_id != operand.period_id
                    ):
                        raise ValueError(
                            "historical driver seed fact operand differs from fact bytes"
                        )
                elif operand.kind == "business_model_evidence_value":
                    selected = judgment_by_id.get(str(operand.evidence_id))
                    if selected is None:
                        raise ValueError(
                            "historical driver seed value operand is not retained judgment evidence"
                        )
                    values_by_id = {row.value_id: row for row in selected.values}
                    evidence_value = values_by_id.get(str(operand.evidence_value_id))
                    if (
                        evidence_value is None
                        or evidence_value.value_kind
                        not in NUMERIC_EVIDENCE_VALUE_KINDS
                    ):
                        raise ValueError(
                            "historical driver seed evidence value is absent or nonnumeric"
                        )
                    try:
                        canonical_value = Decimal(evidence_value.canonical_value)
                    except InvalidOperation as exc:
                        raise ValueError(
                            "historical driver seed evidence value is noncanonical"
                        ) from exc
                    if (
                        (operand.unit, operand.value)
                        != seed_measure_and_value(
                            evidence_value.unit,
                            evidence_value.scale,
                            canonical_value,
                            model_scale=operand.model_scale,
                        )
                        or (
                            evidence_value.period is not None
                            and evidence_value.period != operand.period_id
                        )
                    ):
                        raise ValueError(
                            "historical driver seed evidence operand differs from selected bytes"
                        )
            if seed.period_id not in annual_period_ids:
                raise ValueError("historical driver seed output period is outside axis")
            output_index = annual_period_ids.index(seed.period_id)
            operand_periods = tuple(
                operand.period_id for operand in seed.derivation_operands
            )
            if seed.derivation_kind == "arithmetic_mean":
                if (
                    requirement.observation_basis != "period_average"
                    or output_index == 0
                    or requirement.predecessor_period_id
                    != annual_period_ids[output_index - 1]
                    or operand_periods
                    != (requirement.predecessor_period_id, seed.period_id)
                    or any(
                        operand.observation_basis != "period_end"
                        for operand in seed.derivation_operands
                    )
                ):
                    raise ValueError(
                        "historical driver seed mean requires adjacent ending periods"
                    )
            elif seed.derivation_kind == "allocation":
                if any(period != seed.period_id for period in operand_periods) or any(
                    operand.observation_basis != requirement.observation_basis
                    for operand in seed.derivation_operands
                ):
                    raise ValueError(
                        "historical driver seed allocation requires terminal-period inputs matching BMR intent"
                    )
            elif (
                seed.derivation_operands[0].observation_basis
                != requirement.observation_basis
            ):
                raise ValueError(
                    "direct historical driver seed differs from BMR observation basis"
                )
        # Stage A step A-4: acceptance no longer checks an authored mapping
        # bijection — the bound snapshot is total by construction (coverage
        # equals the exact denominator, every period typed), and supported
        # facts reach compilation by role binding through the total
        # projection, fail-closed there.
        receipt = self.stage_receipt
        scope = HistoricalFoundationStageScope(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
        )
        if (
            receipt.status != self.status
            or receipt.scope != scope
            or receipt.historical_foundation_ref != self.ref
            or receipt.historical_fact_snapshot_ref != self.historical_fact_snapshot_ref
            or receipt.business_model_revision_ref != self.business_model_revision_ref
            or receipt.business_model_selection_epoch
            != self.business_model_selection_epoch
            or receipt.business_model_stage_receipt_digest
            != self.business_model_stage_receipt_digest
        ):
            raise ValueError(
                "historical-foundation receipt does not bind exact artifact"
            )
        validate_revision_fields(self.predecessor_ref, self.supersessions)
        # Data integrity: this digest covers every stored byte of the foundation
        # except its receipt, so a mismatch means the persisted row was
        # corrupted after this system wrote it.
        if self.ref.content_sha256 != historical_foundation_digest(self):
            raise ValueError("historical-foundation content digest mismatch")
        return self


def historical_foundation_digest(
    value: HistoricalFoundation | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    )
    payload.pop("ref", None)
    payload.pop("stage_receipt", None)
    if payload.get("forecast_control_profile_ref") is None:
        payload.pop("forecast_control_profile_ref", None)
    if payload.get("forecast_control_denominator_sha256") is None:
        payload.pop("forecast_control_denominator_sha256", None)
    if not payload.get("historical_driver_seed_requirements"):
        payload.pop("historical_driver_seed_requirements", None)
    if payload.get("historical_driver_seed_denominator_sha256") is None:
        payload.pop("historical_driver_seed_denominator_sha256", None)
    if not payload.get("historical_driver_seeds"):
        payload.pop("historical_driver_seeds", None)
    if payload.get("predecessor_ref") is None:
        payload.pop("predecessor_ref", None)
    if not payload.get("supersessions"):
        payload.pop("supersessions", None)
    version = str(payload.get("schema_version", "historical-foundation.v1"))
    return _digest(version, payload)


def build_historical_foundation(
    *,
    fact_snapshot: HistoricalFactSnapshot,
    status: Literal["accepted", "incomplete", "blocked"],
    completion_mode: Literal["deterministic", "agent_assisted"],
    issue_resolutions: Sequence[HistoricalIssueResolution],
    unresolved_issue_keys: Sequence[str],
    historical_normalization_decisions: Sequence[
        HistoricalNormalizationDecisionAuthoring
    ],
    judgment_evidence: Sequence[BusinessModelSelectedEvidence],
    gaps: Sequence[HistoricalFoundationGap],
    terminal_summary: HistoricalFoundationTerminalSummary,
    created_by: str,
    historical_driver_seeds: Sequence[HistoricalDriverSeed] = (),
    predecessor_ref: HistoricalFoundationRef | None = None,
    supersessions: Sequence[Supersession] = (),
    filed_tie_out: HistoricalFiledTieOut | None = None,
) -> HistoricalFoundation:
    # The exact fact snapshot time is the stable stage clock. Run metadata is
    # deliberately excluded so deterministic retries are byte-identical.
    stage_time = fact_snapshot.created_at
    schema_version = (
        "historical-foundation.v2"
        if fact_snapshot.schema_version == "historical-fact-snapshot.v2"
        else "historical-foundation.v1"
    )
    payload = {
        "schema_version": schema_version,
        "user_id": fact_snapshot.user_id,
        "research_file_id": fact_snapshot.research_file_id,
        "ticker": fact_snapshot.ticker,
        "status": status,
        "completion_mode": completion_mode,
        "business_model_revision_ref": fact_snapshot.business_model_revision_ref,
        "business_model_selection_epoch": fact_snapshot.business_model_selection_epoch,
        "business_model_stage_receipt_digest": fact_snapshot.business_model_stage_receipt_digest,
        "historical_fact_snapshot_ref": fact_snapshot.ref,
        "business_model_driver_keys": fact_snapshot.business_model_driver_keys,
        "forecast_driver_keys": fact_snapshot.forecast_driver_keys,
        "requirement_denominator": fact_snapshot.requirement_denominator,
        "requirement_denominator_sha256": fact_snapshot.requirement_denominator_sha256,
        "mapping_plan": fact_snapshot.mapping_plan,
        "mapping_plan_sha256": fact_snapshot.mapping_plan.content_sha256,
        "fiscal_axis": fact_snapshot.fiscal_axis,
        "fiscal_axis_sha256": fact_snapshot.fiscal_axis_sha256,
        "freshness": fact_snapshot.freshness,
        "issue_resolutions": tuple(
            sorted(issue_resolutions, key=lambda row: row.issue_key)
        ),
        "unresolved_issue_keys": tuple(sorted(set(unresolved_issue_keys))),
        "historical_normalization_decisions": tuple(historical_normalization_decisions),
        "historical_basis_facts": fact_snapshot.facts,
        "evidence": fact_snapshot.evidence,
        "judgment_evidence": tuple(
            sorted(judgment_evidence, key=lambda row: row.evidence_id)
        ),
        "coverage": fact_snapshot.coverage,
        "source_manifest": fact_snapshot.source_manifest,
        "sources": fact_snapshot.sources,
        "gaps": tuple(sorted(gaps, key=lambda row: row.key)),
        "terminal_summary": terminal_summary,
        "created_by": created_by,
        "created_at": stage_time,
    }
    if filed_tie_out is not None and filed_tie_out.by_period:
        payload["filed_tie_out"] = filed_tie_out
    seed_requirements = fact_snapshot.historical_driver_seed_requirements
    if seed_requirements:
        payload["historical_driver_seed_requirements"] = seed_requirements
        payload["historical_driver_seed_denominator_sha256"] = (
            fact_snapshot.historical_driver_seed_denominator_sha256
        )
    if historical_driver_seeds:
        payload["historical_driver_seeds"] = tuple(
            sorted(historical_driver_seeds, key=lambda row: row.driver_key)
        )
    if fact_snapshot.forecast_control_profile_ref is not None:
        payload["forecast_control_profile_ref"] = (
            fact_snapshot.forecast_control_profile_ref
        )
    if fact_snapshot.forecast_control_denominator_sha256 is not None:
        payload["forecast_control_denominator_sha256"] = (
            fact_snapshot.forecast_control_denominator_sha256
        )
    if predecessor_ref is not None:
        payload["predecessor_ref"] = predecessor_ref
    if supersessions:
        payload["supersessions"] = canonical_supersessions(supersessions)
    content_sha256 = historical_foundation_digest(payload)
    ref = HistoricalFoundationRef(
        artifact_id=f"hff_{content_sha256[:32]}",
        schema_version=schema_version,
        content_sha256=content_sha256,
    )
    scope = HistoricalFoundationStageScope(
        user_id=fact_snapshot.user_id,
        research_file_id=fact_snapshot.research_file_id,
        ticker=fact_snapshot.ticker,
    )
    receipt_schema_version = (
        "historical-foundation-stage-receipt.v2"
        if schema_version == "historical-foundation.v2"
        else "historical-foundation-stage-receipt.v1"
    )
    receipt_payload = {
        "schema_version": receipt_schema_version,
        "stage": "historical-foundation",
        "status": status,
        "scope": scope,
        "historical_foundation_ref": ref,
        "historical_fact_snapshot_ref": fact_snapshot.ref,
        "business_model_revision_ref": fact_snapshot.business_model_revision_ref,
        "business_model_selection_epoch": fact_snapshot.business_model_selection_epoch,
        "business_model_stage_receipt_digest": fact_snapshot.business_model_stage_receipt_digest,
        "started_at": stage_time,
        "completed_at": stage_time,
    }
    receipt = HistoricalFoundationStageReceipt.model_validate(
        {
            **receipt_payload,
            "receipt_digest": historical_foundation_receipt_digest(receipt_payload),
        }
    )
    return HistoricalFoundation.model_validate(
        {**payload, "ref": ref, "stage_receipt": receipt}
    )


def project_historical_model_spec_authoring(
    value: HistoricalFoundation,
) -> tuple[HistoricalNormalizationDecisionAuthoring, ...]:
    artifact = HistoricalFoundation.model_validate(value)
    return artifact.historical_normalization_decisions


__all__ = [
    "FILED_LINE_DOUBLE_CLAIMED_GAP_PREFIX",
    "FORECAST_CONTROL_UNRESOLVED_GAP_PREFIX",
    "HistoricalFactSnapshot",
    "HistoricalFactSnapshotRef",
    "HistoricalFoundation",
    "HistoricalFoundationGap",
    "HistoricalFoundationNextCall",
    "HistoricalFoundationRef",
    "HistoricalFoundationStageReceipt",
    "HistoricalFoundationStageScope",
    "HistoricalFoundationTerminalSummary",
    "HistoricalIssueResolution",
    "HistoricalMappingCandidate",
    "HistoricalMappingPlan",
    "HistoricalMaterialIssue",
    "HistoricalResolutionFragment",
    "HistoricalSourcePin",
    "SEED_SOURCE_UNRESOLVED_GAP_PREFIX",
    "build_historical_fact_snapshot",
    "build_historical_foundation",
    "build_historical_mapping_plan",
    "build_historical_source_pin_v2",
    "historical_fact_snapshot_digest",
    "historical_foundation_digest",
    "historical_foundation_receipt_digest",
    "historical_mapping_plan_digest",
    "historical_source_manifest_digest",
    "historical_source_payload_set_digest",
    "project_historical_model_spec_authoring",
    "requirement_denominator_digest",
    "source_record_digest",
]
