"""Canonical typed numeric-value algebra and immutable provenance contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
import re
import unicodedata
from typing import Annotated, Any, Final, Literal, TypeAlias, TypeIs, get_args

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .coverage_profile import AbsenceCitation
from .stable_control_identity import (
    BindingId,
    ControlId,
    FiniteFloat,
    FiscalPeriod,
)
from .versioned_artifact import (
    VersionedArtifactRef,
    canonical_json_bytes,
    compute_artifact_content_sha256,
)


_SHA256_HEX_RE = re.compile(r"[0-9a-f]{64}\Z")

ValueOrigin: TypeAlias = Literal[
    "observed",
    "derived",
    "residual",
    "manual_input",
    "cited_input",
]
MissingReasonCode: TypeAlias = Literal[
    "source_observation_absent",
    "control_period_absent",
    "required_dependency_missing",
    "period_out_of_axis",
    "prior_period_unavailable",
    "cycle_seed_missing",
    "source_value_unavailable",
    "scenario_output_source_unavailable",
    "business_model_node_unresolved",
]
UnsupportedReasonCode: TypeAlias = Literal[
    "formula_type_unsupported",
    "operator_unsupported",
    "valuation_method_unsupported",
    "period_mode_unsupported",
    "unit_conversion_unsupported",
    "cycle_solver_unsupported",
    "raw_excel_formula_semantics_unavailable",
]
InvalidReasonCode: TypeAlias = Literal[
    "non_finite_input",
    "non_finite_result",
    "arithmetic_overflow",
    "division_by_zero",
    "domain_error",
    "unit_mismatch",
    "scale_mismatch",
    "sign_mismatch",
    "dependency_policy_incomplete",
    "dependency_policy_mismatch",
    "optional_substitution_not_authorized",
    "fallback_policy_violation",
    "seed_identity_mismatch",
    "cycle_non_convergent",
    "cycle_diverged",
    "workbook_formula_error",
    "readback_type_mismatch",
]
#: Why a cell is evidenced absent: the filing or the business structure proves the line is not
#: an owed input. ``not_applicable_evidenced`` — cited not applicable (and every absence a formula
#: law derives: a growth over a zero or absent prior base, a ratio over an absent denominator or
#: an all-evidenced-zero ``0/0``, a declared-domain valuation outside its domain);
#: ``not_separately_presented`` — bundled into ``included_in``; ``not_disclosed_in_vintage`` —
#: the filing of that vintage does not print the line; ``optional_unreported_evidenced`` — an
#: optional role the issuer reports nowhere.
AbsenceDisposition: TypeAlias = Literal[
    "not_applicable_evidenced",
    "not_separately_presented",
    "not_disclosed_in_vintage",
    "optional_unreported_evidenced",
]
#: Why a cell is unavailable: either an unresolved missing input (``MissingReasonCode``) or an
#: evidenced absence (``AbsenceDisposition``). The two are strictly disjoint; this union types
#: document stamps that may hold either.
CellUnavailableReason: TypeAlias = MissingReasonCode | AbsenceDisposition
#: Why a known ``0.0`` is evidenced, never a bare number: a filed ``$0`` fact, a closure-corroborated
#: blank (``build_model_product_contracts.CLOSURE_CORROBORATED_ZERO``), or the terminal FX policy
#: (``terminal_zero_fx_policy``). An evidenced zero evaluates as ``0.0`` and carries its own
#: evidence, so it owes no source; it is never a non-zero divisor.
ZeroDisposition: TypeAlias = Literal[
    "observed_zero",
    "closure_corroborated_zero",
    "terminal_zero_fx_policy",
]

class StrictModelNumberValueContract(BaseModel):
    """Strict immutable base for numeric values and their provenance."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


def _require_canonical_text(value: str, *, field_name: str) -> None:
    if (
        not value
        or value.strip() != value
        or (
            not value.isprintable()
            if value.isascii()
            else any(unicodedata.category(character).startswith("C") for character in value)
        )
    ):
        raise ValueError(f"{field_name} must be non-empty canonical text")


def _require_sha256(value: str, *, field_name: str) -> None:
    if _SHA256_HEX_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be lowercase 64-hex")


def _require_ref_hash(
    ref: VersionedArtifactRef,
    companion_hash: str,
    *,
    field_name: str,
) -> None:
    _require_sha256(companion_hash, field_name=field_name)
    if ref.content_sha256 != companion_hash:
        raise ValueError(f"{field_name} differs from ref content_sha256")


def _require_optional_ref_hash(
    ref: VersionedArtifactRef | None,
    companion_hash: str | None,
    *,
    field_name: str,
) -> None:
    if (ref is None) != (companion_hash is None):
        raise ValueError(f"{field_name} ref/hash must be present or absent together")
    if ref is not None and companion_hash is not None:
        _require_ref_hash(ref, companion_hash, field_name=field_name)


def _require_exact_contract(
    ref: VersionedArtifactRef,
    *,
    artifact_kind: str,
    payload_schema_version: str,
    field_name: str,
) -> None:
    if (
        ref.artifact_kind != artifact_kind
        or ref.payload_schema_version != payload_schema_version
        or ref.media_type != "application/json"
        or ref.digest_codec != "canonical_json_v1"
    ):
        raise ValueError(f"{field_name} does not match its exact JSON contract")


def _period_key(period: FiscalPeriod | None) -> tuple[int, str, bool]:
    if period is None:
        return (-1, "", False)
    return (period.fiscal_year, period.period_id, period.is_estimate)


def _cause_key(
    cause: "MissingCause | UnsupportedCause | InvalidCause | AbsentCause",
) -> tuple[str, str, tuple[int, str, bool], str, str]:
    return (
        cause.expression_path,
        cause.node_id,
        _period_key(cause.period),
        cause.state,
        cause.reason_code,
    )


def _require_sorted_unique_models(
    values: Sequence[BaseModel],
    *,
    field_name: str,
) -> None:
    encoded = tuple(canonical_json_bytes(value) for value in values)
    if encoded != tuple(sorted(set(encoded))):
        raise ValueError(f"{field_name} must be sorted and unique by canonical bytes")


class ModelValueCauseBase(StrictModelNumberValueContract):
    node_id: str
    period: FiscalPeriod
    expression_path: str

    @field_validator("node_id", "expression_path", mode="after")
    @classmethod
    def _validate_base(cls, value: str, info: ValidationInfo) -> str:
        _require_canonical_text(value, field_name=str(info.field_name))
        return value


class MissingCause(ModelValueCauseBase):
    state: Literal["missing"]
    reason_code: MissingReasonCode


class UnsupportedCause(ModelValueCauseBase):
    state: Literal["unsupported"]
    reason_code: UnsupportedReasonCode


class InvalidCause(ModelValueCauseBase):
    state: Literal["invalid"]
    reason_code: InvalidReasonCode


class AbsentCause(ModelValueCauseBase):
    """The cell whose evidenced absence a value carries: its own for a leaf, the operand's
    for an absence a formula law derived."""

    state: Literal["evidenced_absent"]
    reason_code: AbsenceDisposition


ModelValueCause: TypeAlias = Annotated[
    MissingCause | UnsupportedCause | InvalidCause | AbsentCause,
    Field(discriminator="state"),
]


class ObservationOriginBinding(StrictModelNumberValueContract):
    binding_kind: Literal["observed"]
    observation_snapshot_ref: VersionedArtifactRef
    observation_snapshot_hash: str
    observation_id: str
    evidence_refs: tuple[VersionedArtifactRef, ...] = Field(min_length=1)
    evidence_hashes: tuple[str, ...] = Field(min_length=1)
    semantic_identity: str
    period: FiscalPeriod
    native_value: Decimal | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )
    applied_polarity: Literal[1, -1, "unmapped"] | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def _validate_binding(self) -> "ObservationOriginBinding":
        _require_ref_hash(
            self.observation_snapshot_ref,
            self.observation_snapshot_hash,
            field_name="observation_snapshot_hash",
        )
        _require_canonical_text(self.observation_id, field_name="observation_id")
        _require_canonical_text(self.semantic_identity, field_name="semantic_identity")
        _validate_ref_hash_sequence(
            self.evidence_refs,
            self.evidence_hashes,
            field_name="evidence",
        )
        return self


class ControlOriginBinding(StrictModelNumberValueContract):
    binding_kind: Literal["manual_input", "cited_input"]
    control_snapshot_ref: VersionedArtifactRef
    control_snapshot_hash: str
    control_id: ControlId
    binding_id: BindingId
    semantic_identity: str
    period: FiscalPeriod | None
    scenario_id: str | None = None

    @model_validator(mode="after")
    def _validate_binding(self) -> "ControlOriginBinding":
        _require_ref_hash(
            self.control_snapshot_ref,
            self.control_snapshot_hash,
            field_name="control_snapshot_hash",
        )
        _require_canonical_text(self.semantic_identity, field_name="semantic_identity")
        if self.scenario_id is not None:
            _require_canonical_text(self.scenario_id, field_name="scenario_id")
        return self


class HypotheticalControlOriginBinding(StrictModelNumberValueContract):
    """Authenticated one-step what-if value derived from a canonical control."""

    binding_kind: Literal["hypothetical_control"]
    control_snapshot_ref: VersionedArtifactRef
    control_snapshot_hash: str
    control_id: ControlId
    binding_id: BindingId
    semantic_identity: str
    period: FiscalPeriod | None
    scenario_id: str | None = None
    base_resolved_inputs_ref: VersionedArtifactRef
    base_resolved_inputs_hash: str
    base_resolved_inputs_digest: str
    factor_node_id: str
    base_value: FiniteFloat
    bump_value: FiniteFloat = Field(gt=0)
    direction: Literal["up", "down"]
    derived_value: FiniteFloat
    perturbation_digest: str

    @model_validator(mode="after")
    def _validate_binding(self) -> "HypotheticalControlOriginBinding":
        _require_ref_hash(
            self.control_snapshot_ref,
            self.control_snapshot_hash,
            field_name="control_snapshot_hash",
        )
        _require_ref_hash(
            self.base_resolved_inputs_ref,
            self.base_resolved_inputs_hash,
            field_name="base_resolved_inputs_hash",
        )
        _require_exact_contract(
            self.base_resolved_inputs_ref,
            artifact_kind="resolved_model_computation_inputs",
            payload_schema_version="resolved-model-computation-inputs.v2",
            field_name="base_resolved_inputs_ref",
        )
        for field_name in ("semantic_identity", "factor_node_id"):
            _require_canonical_text(
                getattr(self, field_name),
                field_name=field_name,
            )
        if self.scenario_id is not None:
            _require_canonical_text(self.scenario_id, field_name="scenario_id")
        _require_sha256(
            self.base_resolved_inputs_digest,
            field_name="base_resolved_inputs_digest",
        )
        _require_sha256(
            self.perturbation_digest,
            field_name="perturbation_digest",
        )
        base_decimal = Decimal(str(self.base_value))
        bump_decimal = Decimal(str(self.bump_value))
        expected_value = float(
            base_decimal + bump_decimal
            if self.direction == "up"
            else base_decimal - bump_decimal
        )
        if self.derived_value != expected_value:
            raise ValueError(
                "derived_value differs from base_value, bump_value, and direction"
            )
        if self.perturbation_digest != hypothetical_control_perturbation_digest(self):
            raise ValueError("perturbation_digest mismatch")
        return self


def hypothetical_control_perturbation_digest(
    value: HypotheticalControlOriginBinding | Mapping[str, Any],
) -> str:
    """Hash the full what-if provenance tuple except its derived digest."""

    body = (
        value.model_dump(mode="json", exclude={"perturbation_digest"})
        if isinstance(value, BaseModel)
        else {
            key: child for key, child in value.items() if key != "perturbation_digest"
        }
    )
    return compute_artifact_content_sha256(body, "canonical_json_v1")


class FormulaOriginBinding(StrictModelNumberValueContract):
    binding_kind: Literal["derived"]
    formula_node_id: str
    semantic_graph_ref: VersionedArtifactRef
    semantic_graph_hash: str
    semantic_model_hash: str
    dependency_policy_manifest_ref: VersionedArtifactRef
    dependency_policy_manifest_hash: str
    computation_request_digest: str
    resolved_inputs_digest: str
    semantic_identity: str
    period: FiscalPeriod
    scc_solver_policy_ref: VersionedArtifactRef | None = None
    scc_solver_policy_hash: str | None = None
    scc_seed_policy_ref: VersionedArtifactRef | None = None
    scc_seed_policy_hash: str | None = None

    @model_validator(mode="after")
    def _validate_binding(self) -> "FormulaOriginBinding":
        _require_canonical_text(self.formula_node_id, field_name="formula_node_id")
        _require_canonical_text(self.semantic_identity, field_name="semantic_identity")
        for ref, companion_hash, field_name in (
            (
                self.semantic_graph_ref,
                self.semantic_graph_hash,
                "semantic_graph_hash",
            ),
            (
                self.dependency_policy_manifest_ref,
                self.dependency_policy_manifest_hash,
                "dependency_policy_manifest_hash",
            ),
        ):
            _require_ref_hash(ref, companion_hash, field_name=field_name)
        for field_name in (
            "semantic_model_hash",
            "computation_request_digest",
            "resolved_inputs_digest",
        ):
            _require_sha256(getattr(self, field_name), field_name=field_name)
        _require_optional_ref_hash(
            self.scc_solver_policy_ref,
            self.scc_solver_policy_hash,
            field_name="scc_solver_policy_hash",
        )
        _require_optional_ref_hash(
            self.scc_seed_policy_ref,
            self.scc_seed_policy_hash,
            field_name="scc_seed_policy_hash",
        )
        if (self.scc_solver_policy_ref is None) != (self.scc_seed_policy_ref is None):
            raise ValueError("SCC solver and seed policy pairs must appear together")
        return self


class ReconciliationOriginBinding(StrictModelNumberValueContract):
    binding_kind: Literal["residual"]
    reconciliation_spec_ref: VersionedArtifactRef
    reconciliation_spec_hash: str
    residual_semantic_item_id: str
    period: FiscalPeriod

    @model_validator(mode="after")
    def _validate_binding(self) -> "ReconciliationOriginBinding":
        _require_ref_hash(
            self.reconciliation_spec_ref,
            self.reconciliation_spec_hash,
            field_name="reconciliation_spec_hash",
        )
        _require_canonical_text(
            self.residual_semantic_item_id,
            field_name="residual_semantic_item_id",
        )
        return self


ModelValueOriginBinding: TypeAlias = Annotated[
    ObservationOriginBinding
    | ControlOriginBinding
    | HypotheticalControlOriginBinding
    | FormulaOriginBinding
    | ReconciliationOriginBinding,
    Field(discriminator="binding_kind"),
]


class OptionalSubstitutionTrace(StrictModelNumberValueContract):
    consumer_node_id: str
    consumer_period: FiscalPeriod
    expression_path: tuple[Annotated[int, Field(ge=0)], ...]
    occurrence_digest: str
    source_node_id: str
    source_period: FiscalPeriod
    source_missing_reason: MissingReasonCode
    action: Literal["substitute_zero", "omit_operand", "evaluate_fallback"]
    authorization_ref: VersionedArtifactRef
    authorization_hash: str
    fallback_policy_ref: VersionedArtifactRef | None = None
    fallback_policy_hash: str | None = None

    @model_validator(mode="after")
    def _validate_trace(self) -> "OptionalSubstitutionTrace":
        for field_name in ("consumer_node_id", "source_node_id"):
            _require_canonical_text(getattr(self, field_name), field_name=field_name)
        _require_sha256(self.occurrence_digest, field_name="occurrence_digest")
        _require_ref_hash(
            self.authorization_ref,
            self.authorization_hash,
            field_name="authorization_hash",
        )
        _require_optional_ref_hash(
            self.fallback_policy_ref,
            self.fallback_policy_hash,
            field_name="fallback_policy_hash",
        )
        if self.action == "evaluate_fallback":
            if self.fallback_policy_ref is None:
                raise ValueError("evaluate_fallback requires a fallback policy")
        elif self.fallback_policy_ref is not None:
            raise ValueError("zero/omit substitution forbids a fallback policy")
        return self


class KnownNumberValueBase(StrictModelNumberValueContract):
    """A finite number. ``evidenced_zero`` names why a ``0.0`` is evidenced (``ZeroDisposition``):
    such a value is ``EvidencedZero`` in the cell algebra — it evaluates as ``0.0``, owes no
    source, and is never a non-zero divisor; ``None`` is a plain ``Known``."""

    state: Literal["known"]
    value: FiniteFloat
    substitutions: tuple[OptionalSubstitutionTrace, ...] = ()
    evidenced_zero: ZeroDisposition | None = None

    @field_validator("value", mode="after")
    @classmethod
    def _canonicalize_signed_zero(cls, value: float) -> float:
        return 0.0 if value == 0.0 else value

    @model_validator(mode="after")
    def _validate_substitutions(self) -> "KnownNumberValueBase":
        _require_sorted_unique_models(
            self.substitutions,
            field_name="substitutions",
        )
        if self.evidenced_zero is not None and self.value != 0.0:
            raise ValueError("an evidenced zero is 0.0")
        return self


def _validate_known_bindings(
    bindings: Sequence[BaseModel],
    *,
    field_name: str = "origin_bindings",
) -> None:
    if not bindings:
        raise ValueError(f"{field_name} must be non-empty")
    _require_sorted_unique_models(bindings, field_name=field_name)


class ObservedKnownNumberValue(KnownNumberValueBase):
    origin: Literal["observed"]
    origin_bindings: tuple[ObservationOriginBinding, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_origins(self) -> "ObservedKnownNumberValue":
        _validate_known_bindings(self.origin_bindings)
        return self


class DerivedKnownNumberValue(KnownNumberValueBase):
    origin: Literal["derived"]
    origin_bindings: tuple[FormulaOriginBinding, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_origins(self) -> "DerivedKnownNumberValue":
        _validate_known_bindings(self.origin_bindings)
        return self


class ResidualKnownNumberValue(KnownNumberValueBase):
    origin: Literal["residual"]
    origin_bindings: tuple[ReconciliationOriginBinding, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_origins(self) -> "ResidualKnownNumberValue":
        _validate_known_bindings(self.origin_bindings)
        return self


class ManualInputKnownNumberValue(KnownNumberValueBase):
    origin: Literal["manual_input", "cited_input"]
    origin_bindings: tuple[
        ControlOriginBinding | HypotheticalControlOriginBinding,
        ...,
    ] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_origins(self) -> "ManualInputKnownNumberValue":
        _validate_known_bindings(self.origin_bindings)
        return self


KnownNumberValue: TypeAlias = Annotated[
    ObservedKnownNumberValue
    | DerivedKnownNumberValue
    | ResidualKnownNumberValue
    | ManualInputKnownNumberValue,
    Field(discriminator="origin"),
]


def _validate_causes(
    causes: Sequence[MissingCause | UnsupportedCause | InvalidCause | AbsentCause],
    *,
    primary_reason: str,
) -> None:
    if not causes:
        raise ValueError("causes must be non-empty")
    if tuple(causes) != tuple(sorted(set(causes), key=_cause_key)):
        raise ValueError("causes must be canonically sorted and unique")
    if primary_reason != causes[0].reason_code:
        raise ValueError("primary_reason must equal the first canonical cause")


class MissingNumberValue(StrictModelNumberValueContract):
    state: Literal["missing"]
    primary_reason: MissingReasonCode
    causes: tuple[MissingCause, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_value(self) -> "MissingNumberValue":
        _validate_causes(self.causes, primary_reason=self.primary_reason)
        return self


class UnsupportedNumberValue(StrictModelNumberValueContract):
    state: Literal["unsupported"]
    primary_reason: UnsupportedReasonCode
    causes: tuple[UnsupportedCause, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_value(self) -> "UnsupportedNumberValue":
        _validate_causes(self.causes, primary_reason=self.primary_reason)
        return self


class InvalidNumberValue(StrictModelNumberValueContract):
    state: Literal["invalid"]
    primary_reason: InvalidReasonCode
    causes: tuple[InvalidCause, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_value(self) -> "InvalidNumberValue":
        _validate_causes(self.causes, primary_reason=self.primary_reason)
        return self


class EvidencedAbsentNumberValue(StrictModelNumberValueContract):
    """``EvidencedAbsent``: the filing or the business structure proves this cell is not an owed
    input (``primary_reason`` is its ``AbsenceDisposition``), so it is never a missing source and
    never a number. ``causes[0]`` is the cell whose absence this is — the cell itself for a leaf
    (a cited absence, a not-separately-presented line, a vintage that does not print the line),
    the operand for an absence a formula law derived (a growth over a zero or absent prior base,
    a ratio over an absent denominator or an all-evidenced-zero ``0/0``). ``citation`` is the
    filing evidence when one was authored; ``included_in`` the line that carries a
    not-separately-presented amount.

    The cell algebra is ``Known | EvidencedZero | EvidencedAbsent | UnresolvedInput``:
    ``EvidencedZero`` is a known value with ``evidenced_zero`` set; ``UnresolvedInput`` is a
    missing, unsupported or invalid value — required by the structure with neither a number nor
    absence evidence, or an arithmetic failure.
    """

    state: Literal["evidenced_absent"]
    primary_reason: AbsenceDisposition
    causes: tuple[AbsentCause, ...] = Field(min_length=1)
    citation: AbsenceCitation | None = None
    included_in: str | None = None

    @model_validator(mode="after")
    def _validate_value(self) -> "EvidencedAbsentNumberValue":
        _validate_causes(self.causes, primary_reason=self.primary_reason)
        if self.included_in is not None:
            _require_canonical_text(self.included_in, field_name="included_in")
        return self


ModelNumberValue: TypeAlias = (
    KnownNumberValue
    | MissingNumberValue
    | UnsupportedNumberValue
    | InvalidNumberValue
    | EvidencedAbsentNumberValue
)
#: The states that are ``UnresolvedInput``: required with neither a number nor absence evidence,
#: or an arithmetic failure. A consumer deciding "is this cell owed" reads the state, never a
#: reason code: an ``evidenced_absent`` cell is never owed, an unresolved one always is.
UNRESOLVED_INPUT_STATES: Final[frozenset[str]] = frozenset({"missing", "unsupported", "invalid"})


def is_absence_disposition(reason: str) -> TypeIs[AbsenceDisposition]:
    """Whether a stamped reason is an ``AbsenceDisposition`` — an evidenced absence, never owed."""

    return reason in get_args(AbsenceDisposition)


def is_evidenced_zero(value: object) -> bool:
    """Whether ``value`` is ``EvidencedZero``: a known ``0.0`` carrying its zero evidence."""

    return isinstance(value, KnownNumberValueBase) and value.evidenced_zero is not None


def _validate_ref_hash_sequence(
    refs: Sequence[VersionedArtifactRef],
    hashes: Sequence[str],
    *,
    field_name: str,
) -> None:
    if len(refs) != len(hashes):
        raise ValueError(f"{field_name} ref/hash lists must have equal cardinality")
    pairs = tuple(zip(refs, hashes, strict=True))
    encoded: list[bytes] = []
    for ref, companion_hash in pairs:
        _require_ref_hash(
            ref,
            companion_hash,
            field_name=f"{field_name}_hash",
        )
        encoded.append(canonical_json_bytes((ref, companion_hash)))
    if tuple(encoded) != tuple(sorted(set(encoded))):
        raise ValueError(f"{field_name} ref/hash pairs must be sorted and unique")
