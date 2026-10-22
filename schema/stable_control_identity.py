"""Stable economic-identity definitions for the BMA-104 control plane.

The contracts in this module own the semantic identity vocabulary.  They do
not allocate IDs, resolve named-object authorities, or publish artifacts;
those operations belong to the dedicated issuance transaction.
"""

from __future__ import annotations

import re
import secrets
import unicodedata
from typing import Annotated, Any, Literal, Mapping

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)

from .models import ModelScale
from industry_slice_core.industry_types import Unit
from .versioned_artifact import VersionedArtifactRef, domain_sha256


IdentityKind = Literal["driver", "control", "binding"]
DriverId = Annotated[str, Field(strict=True, pattern=r"^drv_[0-9a-f]{32}$")]
ControlId = Annotated[str, Field(strict=True, pattern=r"^ctl_[0-9a-f]{32}$")]
BindingId = Annotated[str, Field(strict=True, pattern=r"^bnd_[0-9a-f]{32}$")]
StableIdentityId = DriverId | ControlId | BindingId
#: One cell of a model sheet: its line item and fiscal period (the key every
#: cell-indexed map — columns, values, readers — is keyed by).
CellKey = tuple[str, int]
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SignConvention = Literal[
    "natural",
    "nonnegative_as_reported",
    "positive_add_back",
    "nonnegative_output",
    "signed_as_disclosed",
    "signed_tax_offset",
]
PeriodAxisRole = Literal["scalar", "fiscal_period"]
ControlDomain = Literal["forecast", "scenario", "valuation"]
ControlKind = Literal[
    "level",
    "growth_rate",
    "margin",
    "yield",
    "days",
    "turnover",
    "probability",
    "multiple",
    "discount_rate",
    "terminal_growth",
    "exit_multiple",
    "target_horizon",
]
ScalarKind = Literal["binary64", "decimal"]


def _validate_sha256_hex(value: str, *, field_name: str) -> str:
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be exactly 64 lowercase hex characters")
    return value


def _canonical_zero(value: float) -> float:
    return 0.0 if value == 0.0 else value


FiniteFloat = Annotated[
    float,
    Field(allow_inf_nan=False),
    AfterValidator(_canonical_zero),
]


def _require_canonical_text(value: str, *, field_name: str) -> str:
    if not value or value.strip() != value:
        raise ValueError(f"{field_name} must be non-empty canonical text")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ValueError(f"{field_name} must not contain control bytes")
    return value


def _require_named_object_id(value: str) -> str:
    return _require_canonical_text(value, field_name="named object ID")


CanonicalNamedObjectId = Annotated[
    str,
    Field(strict=True, min_length=1),
    AfterValidator(_require_named_object_id),
]


def _require_slot_key(value: str) -> str:
    return _require_canonical_text(value, field_name="slot_key")


class FiscalPeriod(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    period_id: Annotated[str, Field(strict=True, min_length=1)]
    fiscal_year: Annotated[int, Field(strict=True, gt=0)]
    period_type: Literal["annual"]
    is_estimate: StrictBool


class FiscalAxis(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    historical_periods: tuple[FiscalPeriod, ...] = Field(min_length=1)
    forecast_periods: tuple[FiscalPeriod, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_canonical_annual_axis(self) -> "FiscalAxis":
        historical_ids = tuple(period.period_id for period in self.historical_periods)
        forecast_ids = tuple(period.period_id for period in self.forecast_periods)
        historical_years = tuple(
            period.fiscal_year for period in self.historical_periods
        )
        forecast_years = tuple(period.fiscal_year for period in self.forecast_periods)

        if any(period.is_estimate for period in self.historical_periods):
            raise ValueError("historical_periods must contain only actual periods")
        if any(not period.is_estimate for period in self.forecast_periods):
            raise ValueError("forecast_periods must contain only estimate periods")
        if historical_ids != tuple(sorted(historical_ids)) or len(
            historical_ids
        ) != len(set(historical_ids)):
            raise ValueError("historical_periods must be sorted and unique by period_id")
        if forecast_ids != tuple(sorted(forecast_ids)) or len(forecast_ids) != len(
            set(forecast_ids)
        ):
            raise ValueError("forecast_periods must be sorted and unique by period_id")
        if historical_years != tuple(sorted(historical_years)) or len(
            historical_years
        ) != len(set(historical_years)):
            raise ValueError(
                "historical_periods must be strictly increasing and unique by "
                "fiscal_year"
            )
        if forecast_years != tuple(sorted(forecast_years)) or len(
            forecast_years
        ) != len(set(forecast_years)):
            raise ValueError(
                "forecast_periods must be strictly increasing and unique by fiscal_year"
            )
        if set(historical_ids) & set(forecast_ids):
            raise ValueError("historical and forecast period IDs must be disjoint")
        if historical_years[-1] >= forecast_years[0]:
            raise ValueError("forecast fiscal years must follow historical fiscal years")
        return self


class WorkbookTarget(BaseModel):
    """Semantic template-item locator; physical coordinates are forbidden."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    template_item_id: CanonicalNamedObjectId
    period_axis_role: PeriodAxisRole


_IDENTITY_PREFIX_BY_KIND: dict[str, str] = {
    "driver": "drv_",
    "control": "ctl_",
    "binding": "bnd_",
}
_IDENTITY_PATTERN_BY_KIND: dict[str, re.Pattern[str]] = {
    kind: re.compile(rf"^{prefix}[0-9a-f]{{32}}$")
    for kind, prefix in _IDENTITY_PREFIX_BY_KIND.items()
}


def require_identity_kind(
    identity_id: str | None, identity_kind: IdentityKind
) -> str:
    """Return a stable ID or reject a kind/prefix substitution."""

    expected_prefix = _IDENTITY_PREFIX_BY_KIND[identity_kind]
    if (
        identity_id is None
        or _IDENTITY_PATTERN_BY_KIND[identity_kind].fullmatch(identity_id) is None
    ):
        raise ValueError(
            f"{identity_kind} identity requires one canonical {expected_prefix!r} ID"
        )
    return identity_id


def issue_random_identity_id(identity_kind: IdentityKind) -> str:
    """Allocate one opaque uniformly random 128-bit stable identity."""

    return _IDENTITY_PREFIX_BY_KIND[identity_kind] + secrets.token_hex(16)


class ExistingIdentitySelector(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    selector_kind: Literal["existing_id"]
    identity_kind: IdentityKind
    identity_id: StableIdentityId

    @model_validator(mode="after")
    def _validate_identity_kind(self) -> "ExistingIdentitySelector":
        require_identity_kind(self.identity_id, self.identity_kind)
        return self


class IssuanceSlotSelector(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    selector_kind: Literal["same_request_slot"]
    identity_kind: IdentityKind
    slot_key: str

    @field_validator("slot_key", mode="after")
    @classmethod
    def _validate_slot_key(cls, value: str) -> str:
        return _require_slot_key(value)


IdentityDefinitionSelector = Annotated[
    ExistingIdentitySelector | IssuanceSlotSelector,
    Field(discriminator="selector_kind"),
]


class DriverDefinitionIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    economic_concept_id: CanonicalNamedObjectId
    output_unit: Unit
    model_scale: ModelScale
    sign_convention: SignConvention
    equation_contract_id: CanonicalNamedObjectId
    factor_ids: tuple[CanonicalNamedObjectId, ...]

    @model_validator(mode="after")
    def _validate_unit_scale(self) -> "DriverDefinitionIntent":
        _validate_intrinsic_model_scale(self.output_unit, self.model_scale)
        return self


class DriverControlOwnerIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["driver"]
    driver_selector: IdentityDefinitionSelector

    @model_validator(mode="after")
    def _validate_driver_selector(self) -> "DriverControlOwnerIntent":
        if self.driver_selector.identity_kind != "driver":
            raise ValueError("driver owner intent requires a driver identity selector")
        return self


class SemanticItemControlOwnerIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["semantic_item"]
    semantic_item_id: CanonicalNamedObjectId


class ValuationMethodControlOwnerIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["valuation_method"]
    valuation_method_id: CanonicalNamedObjectId


ControlOwnerIntent = Annotated[
    DriverControlOwnerIntent
    | SemanticItemControlOwnerIntent
    | ValuationMethodControlOwnerIntent,
    Field(discriminator="kind"),
]


_CONTROL_KINDS_BY_DOMAIN: dict[str, frozenset[str]] = {
    "forecast": frozenset(
        {"level", "growth_rate", "margin", "yield", "days", "turnover"}
    ),
    "scenario": frozenset(
        {
            "level",
            "growth_rate",
            "margin",
            "yield",
            "days",
            "turnover",
            "probability",
            "multiple",
        }
    ),
    "valuation": frozenset(
        {
            "level",
            "multiple",
            "discount_rate",
            "terminal_growth",
            "exit_multiple",
            "target_horizon",
        }
    ),
}
_UNIT_BY_CONTROL_KIND: dict[str, Unit] = {
    "growth_rate": Unit.percentage,
    "margin": Unit.percentage,
    "yield": Unit.percentage,
    "discount_rate": Unit.percentage,
    "terminal_growth": Unit.percentage,
    "days": Unit.days,
    "turnover": Unit.ratio,
    "probability": Unit.percentage,
    "multiple": Unit.multiple,
    "exit_multiple": Unit.multiple,
    "target_horizon": Unit.date,
}
_SCALAR_ONLY_CONTROL_KINDS = frozenset(
    {
        "probability",
        "discount_rate",
        "terminal_growth",
        "exit_multiple",
        "target_horizon",
    }
)
_INTRINSICALLY_UNSCALED_UNITS = frozenset(
    {
        Unit.percentage,
        Unit.ratio,
        Unit.days,
        Unit.date,
        Unit.multiple,
        Unit.per_share,
    }
)


def _validate_intrinsic_model_scale(unit: Unit, model_scale: ModelScale) -> None:
    if unit in _INTRINSICALLY_UNSCALED_UNITS and model_scale != "units":
        raise ValueError(f"unit {unit.value!r} requires model_scale='units'")


def _validate_control_matrix(
    *,
    domain: ControlDomain,
    owner: BaseModel,
    control_kind: ControlKind,
    scalar_kind: ScalarKind,
    unit: Unit,
    model_scale: ModelScale,
    sign_convention: SignConvention,
    periodicity: PeriodAxisRole,
) -> None:
    expected_owner_kind = {
        "forecast": "driver",
        "scenario": "semantic_item",
        "valuation": "valuation_method",
    }[domain]
    if getattr(owner, "kind") != expected_owner_kind:
        raise ValueError(f"{domain} controls require a {expected_owner_kind} owner")
    if control_kind not in _CONTROL_KINDS_BY_DOMAIN[domain]:
        raise ValueError(
            f"control kind {control_kind!r} is not allowed in domain {domain!r}"
        )
    expected_unit = _UNIT_BY_CONTROL_KIND.get(control_kind)
    if expected_unit is not None and unit != expected_unit:
        raise ValueError(
            f"control kind {control_kind!r} requires unit={expected_unit.value!r}"
        )
    _validate_intrinsic_model_scale(unit, model_scale)
    if control_kind in _SCALAR_ONLY_CONTROL_KINDS and periodicity != "scalar":
        raise ValueError(f"control kind {control_kind!r} must be scalar")
    if control_kind == "probability":
        if scalar_kind != "decimal":
            raise ValueError("probability controls require scalar_kind='decimal'")
        if sign_convention != "nonnegative_output":
            raise ValueError(
                "probability controls require sign_convention='nonnegative_output'"
            )


class ControlDefinitionIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    domain: ControlDomain
    owner: ControlOwnerIntent
    control_kind: ControlKind
    scalar_kind: ScalarKind
    unit: Unit
    model_scale: ModelScale
    sign_convention: SignConvention
    periodicity: PeriodAxisRole
    mutation_owner_policy_id: CanonicalNamedObjectId

    @model_validator(mode="after")
    def _validate_matrix(self) -> "ControlDefinitionIntent":
        _validate_control_matrix(
            domain=self.domain,
            owner=self.owner,
            control_kind=self.control_kind,
            scalar_kind=self.scalar_kind,
            unit=self.unit,
            model_scale=self.model_scale,
            sign_convention=self.sign_convention,
            periodicity=self.periodicity,
        )
        return self


class CompileBindingDefinitionIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    control_selector: IdentityDefinitionSelector
    role: Literal["primary_compile", "display"]
    semantic_item_id: CanonicalNamedObjectId
    transform_contract_id: CanonicalNamedObjectId
    compiler_contract_id: CanonicalNamedObjectId
    template_contract_id: CanonicalNamedObjectId
    workbook_target: WorkbookTarget | None = None

    @model_validator(mode="after")
    def _validate_control_selector(self) -> "CompileBindingDefinitionIntent":
        if self.control_selector.identity_kind != "control":
            raise ValueError("compile binding intent requires a control selector")
        return self


class DriverControlOwner(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["driver"]
    driver_id: DriverId


class SemanticItemControlOwner(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["semantic_item"]
    semantic_item_id: CanonicalNamedObjectId


class ValuationMethodControlOwner(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["valuation_method"]
    valuation_method_id: CanonicalNamedObjectId


ControlOwner = Annotated[
    DriverControlOwner | SemanticItemControlOwner | ValuationMethodControlOwner,
    Field(discriminator="kind"),
]


def _digest_body(value: BaseModel | Mapping[str, Any], field_name: str) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude={field_name})
    return {key: child for key, child in value.items() if key != field_name}


def definition_intent_digest(
    identity_kind: IdentityKind,
    intent: BaseModel | Mapping[str, Any],
) -> str:
    domain_kind = "compile-binding" if identity_kind == "binding" else identity_kind
    return domain_sha256(f"{domain_kind}-definition-intent.v1", intent)


def stable_identity_definition_digest(
    definition: StableIdentityDefinition | Mapping[str, Any],
    *,
    identity_kind: IdentityKind | None = None,
) -> str:
    kind = identity_kind
    if kind is None:
        if isinstance(definition, BaseModel):
            kind = definition.identity_kind
        else:
            kind = definition.get("identity_kind")
    if kind not in _IDENTITY_PREFIX_BY_KIND:
        raise ValueError("definition requires one exact identity_kind")
    domain_kind = "compile-binding" if kind == "binding" else kind
    return domain_sha256(
        f"{domain_kind}-definition.v1",
        _digest_body(definition, "definition_digest"),
    )


class DriverDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_kind: Literal["driver"]
    driver_id: DriverId
    definition_version: int = Field(gt=0)
    lifecycle: Literal["reserved", "active", "retired"]
    economic_concept_id: CanonicalNamedObjectId
    output_unit: Unit
    model_scale: ModelScale
    sign_convention: SignConvention
    equation_contract_id: CanonicalNamedObjectId
    factor_ids: tuple[CanonicalNamedObjectId, ...]
    definition_digest: str

    @model_validator(mode="after")
    def _validate_definition(self) -> "DriverDefinition":
        _validate_intrinsic_model_scale(self.output_unit, self.model_scale)
        _validate_sha256_hex(self.definition_digest, field_name="definition_digest")
        if self.definition_digest != stable_identity_definition_digest(self):
            raise ValueError("driver definition digest mismatch")
        return self


class ControlDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_kind: Literal["control"]
    control_id: ControlId
    definition_version: int = Field(gt=0)
    lifecycle: Literal["reserved", "active", "retired"]
    domain: ControlDomain
    owner: ControlOwner
    control_kind: ControlKind
    scalar_kind: ScalarKind
    unit: Unit
    model_scale: ModelScale
    sign_convention: SignConvention
    periodicity: PeriodAxisRole
    mutation_owner_policy_id: CanonicalNamedObjectId
    definition_digest: str

    @model_validator(mode="after")
    def _validate_definition(self) -> "ControlDefinition":
        _validate_control_matrix(
            domain=self.domain,
            owner=self.owner,
            control_kind=self.control_kind,
            scalar_kind=self.scalar_kind,
            unit=self.unit,
            model_scale=self.model_scale,
            sign_convention=self.sign_convention,
            periodicity=self.periodicity,
        )
        _validate_sha256_hex(self.definition_digest, field_name="definition_digest")
        if self.definition_digest != stable_identity_definition_digest(self):
            raise ValueError("control definition digest mismatch")
        return self


class CompileBindingDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_kind: Literal["binding"]
    binding_id: BindingId
    definition_version: int = Field(gt=0)
    lifecycle: Literal["reserved", "active", "retired"]
    control_id: ControlId
    role: Literal["primary_compile", "display"]
    semantic_item_id: CanonicalNamedObjectId
    transform_contract_id: CanonicalNamedObjectId
    compiler_contract_id: CanonicalNamedObjectId
    template_contract_id: CanonicalNamedObjectId
    workbook_target: WorkbookTarget | None = None
    definition_digest: str

    @model_validator(mode="after")
    def _validate_definition(self) -> "CompileBindingDefinition":
        _validate_sha256_hex(self.definition_digest, field_name="definition_digest")
        if self.definition_digest != stable_identity_definition_digest(self):
            raise ValueError("compile-binding definition digest mismatch")
        return self


StableIdentityDefinition = Annotated[
    DriverDefinition | ControlDefinition | CompileBindingDefinition,
    Field(discriminator="identity_kind"),
]


class IdentityAliasRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    alias_kind: Literal[
        "legacy_driver_key",
        "business_model_node_path",
        "semantic_item_id",
        "template_item_id",
        "workbook_item_id",
    ]
    raw_alias: str
    normalized_alias: str
    alias_normalizer_id: CanonicalNamedObjectId
    identity_kind: IdentityKind
    target_id: StableIdentityId
    valid_from_business_model_ref: VersionedArtifactRef
    valid_from_business_model_hash: str
    valid_through_business_model_ref: VersionedArtifactRef | None = None
    valid_through_business_model_hash: str | None = None
    status: Literal["active", "historical", "quarantined"]
    migration_plan_ref: VersionedArtifactRef | None = None
    migration_plan_hash: str | None = None

    @field_validator("raw_alias", "normalized_alias")
    @classmethod
    def _validate_alias_text(cls, value: str) -> str:
        return _require_canonical_text(value, field_name="alias")

    @model_validator(mode="after")
    def _validate_alias(self) -> "IdentityAliasRecord":
        require_identity_kind(self.target_id, self.identity_kind)
        _validate_ref_hash(
            self.valid_from_business_model_ref,
            self.valid_from_business_model_hash,
            field_name="valid_from_business_model_hash",
        )
        _validate_optional_ref_hash_pair(
            self.valid_through_business_model_ref,
            self.valid_through_business_model_hash,
            field_name="valid_through_business_model",
        )
        _validate_optional_ref_hash_pair(
            self.migration_plan_ref,
            self.migration_plan_hash,
            field_name="migration_plan",
        )
        return self


class StableControlIdentityPlanInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    aliases: tuple[IdentityAliasRecord, ...]
    drivers: tuple[DriverDefinition, ...]
    controls: tuple[ControlDefinition, ...]
    bindings: tuple[CompileBindingDefinition, ...]
    business_model_ref: VersionedArtifactRef
    business_model_hash: str


class ActiveIdentityQuarantineBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_kind: IdentityKind
    identity_id: StableIdentityId
    opened_event_ref: VersionedArtifactRef
    opened_event_hash: str
    underlying_definition_version: int = Field(gt=0)
    underlying_definition_digest: str

    @model_validator(mode="after")
    def _validate_binding(self) -> "ActiveIdentityQuarantineBinding":
        require_identity_kind(self.identity_id, self.identity_kind)
        _validate_ref_hash(
            self.opened_event_ref,
            self.opened_event_hash,
            field_name="opened_event_hash",
        )
        _validate_sha256_hex(
            self.underlying_definition_digest,
            field_name="underlying_definition_digest",
        )
        return self


def _validate_ref_hash(
    ref: VersionedArtifactRef,
    companion_hash: str,
    *,
    field_name: str,
) -> None:
    _validate_sha256_hex(companion_hash, field_name=field_name)
    if companion_hash != ref.content_sha256:
        raise ValueError(f"{field_name} does not equal the ref content_sha256")


def _validate_optional_ref_hash_pair(
    ref: VersionedArtifactRef | None,
    companion_hash: str | None,
    *,
    field_name: str,
) -> None:
    if (ref is None) != (companion_hash is None):
        raise ValueError(f"{field_name} ref and hash must be supplied together")
    if ref is not None and companion_hash is not None:
        _validate_ref_hash(ref, companion_hash, field_name=f"{field_name}_hash")


__all__ = [
    "ActiveIdentityQuarantineBinding",
    "BindingId",
    "CanonicalNamedObjectId",
    "CellKey",
    "CompileBindingDefinition",
    "CompileBindingDefinitionIntent",
    "ControlDefinition",
    "ControlDefinitionIntent",
    "ControlDomain",
    "ControlId",
    "ControlKind",
    "ControlOwner",
    "ControlOwnerIntent",
    "DriverControlOwner",
    "DriverControlOwnerIntent",
    "DriverDefinition",
    "DriverDefinitionIntent",
    "DriverId",
    "ExistingIdentitySelector",
    "FiniteFloat",
    "FiscalAxis",
    "FiscalPeriod",
    "IdentityAliasRecord",
    "IdentityDefinitionSelector",
    "IdentityKind",
    "IssuanceSlotSelector",
    "ModelScale",
    "PeriodAxisRole",
    "ScalarKind",
    "SemanticItemControlOwner",
    "SemanticItemControlOwnerIntent",
    "SignConvention",
    "StableControlIdentityPlanInputs",
    "StableIdentityDefinition",
    "StableIdentityId",
    "Unit",
    "ValuationMethodControlOwner",
    "ValuationMethodControlOwnerIntent",
    "WorkbookTarget",
    "definition_intent_digest",
    "issue_random_identity_id",
    "require_identity_kind",
    "stable_identity_definition_digest",
]
