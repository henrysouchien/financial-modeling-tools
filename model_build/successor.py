"""A successor ModelVersion as publishable bytes: its candidate identity, its bindings, its compile.

Owner: ``docs/design/build-model-package-boundary-plan-2026-10-05.md`` §2.1–§2.2. This root compiles
a successor and proves its identity; the repository keeps the write, the lineage, the lock and the
pointer CAS. The product adapter (``api/fms/core/persist_runner.py``:
``prepare_compiled_model_successor`` and ``SuccessorModelPublisher``) resolves the proposal handle,
loads the predecessor and the winning SourceBundle, allocates the ref, calls
``compile_successor_version`` once, then inserts the row, writes the immutable files and advances
the pointer.

The candidate digest's primitive preimage is intentionally ref-independent. Candidate assembly and
durable hydration are separate constructors because hydration must prove that stored hashes and
compilation identities still describe the typed inputs; copying stored digest strings into a
preimage is not sufficient. The SourceBundle identity digest is the repository's sole codec; it
arrives as the ``identity_codec`` parameter, never a second copy here.

What crosses in from the product arrives as the Protocols below (a proposal handle, a loaded
SourceBundle row, a publication request) or as explicit fields.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import json
from typing import Any, Generic, Literal, NoReturn, Protocol, TypeVar, runtime_checkable

from pydantic import (
  BaseModel,
  ConfigDict,
  Field,
  ValidationError,
  ValidationInfo,
  field_validator,
)

from schema import serialization
from schema.build import (
  CompiledModelIntermediate,
  FinalizedBuildArtifacts,
  finalize_compiled_model,
)
from schema.business_model_revision import BusinessModelRevision
from schema.build_model_product_contracts import (
  ModelSpec,
  ModelVersion,
  SourceBundle,
  SourceBundleRebuildRequired,
  SuccessorModelVersionOrigin,
  decode_source_bundle_payload,
  decode_validation_report_payload,
  resolve_model_spec_business_model_revision,
)
from schema.finding_vocabulary import BuildVerdict
from schema.model_ref import ModelVersionRef
from schema.model_semantic_document import (
  ModelSemanticDocument,
  validate_stored_provenance_pair,
)
from schema.model_state_identity import validate_sha256_hex
from schema.render_state import compiler_input_content_hash
from schema.versioned_artifact import canonical_json_bytes


SUCCESSOR_CANDIDATE_DIGEST_SCHEMA_VERSION = "1.0"
SUCCESSOR_CANDIDATE_DIGEST_INTEGRITY_ERROR = (
  "successor_candidate_digest_integrity_failed"
)
SUCCESSOR_CANDIDATE_DIGEST_FIELD_ORDER = (
  "schema_version",
  "door",
  "proposal_handle",
  "semantic_base_model_id",
  "semantic_base_version",
  "pointer_epoch",
  "bundle_id",
  "bundle_identity_digest",
  "bundle_content_sha256",
  "model_spec_digest",
  "bmr_business_model_id",
  "bmr_revision",
  "bmr_content_sha256",
  "driver_assumption_plan_sha256",
  "compilation_identity",
)
SUCCESSOR_COMPILATION_IDENTITY_FIELD_ORDER = (
  "compiler_version",
  "template_version",
  "model_version_schema_version",
  "source_bundle_schema_version",
  "source_bundle_schema_epoch",
  "model_spec_schema_version",
  "business_model_revision_schema_version",
  "business_model_schema_version",
  "driver_assumption_plan_schema_version",
)
#: The version's artifacts, in ``FinalizedBuildArtifacts.files`` order.
_ARTIFACT_FILENAMES = (
  "model.xlsx",
  "semantic.json",
  "provenance.json",
  "validation.json",
)


class SourceBundleRefFields(Protocol):
  """The immutable SourceBundle row ref."""

  @property
  def bundle_id(self) -> str: ...

  @property
  def content_sha256(self) -> str: ...


@runtime_checkable
class LoadedBundle(Protocol):
  """The repository's verified SourceBundle row: ref, identity columns, payload.

  ``identity`` carries every ``SuccessorSourceBundleIdentityComponents`` field as an attribute.
  """

  @property
  def ref(self) -> SourceBundleRefFields: ...

  @property
  def identity(self) -> object: ...

  @property
  def payload(self) -> Mapping[str, Any]: ...


class TransformedBundleIdentity(Protocol):
  """The identity columns a transformed SourceBundle record binds."""

  @property
  def user_id(self) -> str: ...

  @property
  def research_file_id(self) -> int: ...

  @property
  def ticker(self) -> str: ...

  @property
  def operation(self) -> str: ...

  @property
  def proposal_id(self) -> str | None: ...


class TransformedBundleRecord(Protocol):
  """A transformed SourceBundle candidate the publication materializes."""

  @property
  def identity(self) -> TransformedBundleIdentity: ...

  @property
  def payload(self) -> Mapping[str, Any]: ...


class SuccessorHandle(Protocol):
  """One immutable successor authorization hydrated under the caller's lock."""

  @property
  def user_id(self) -> str: ...

  @property
  def research_file_id(self) -> int: ...

  @property
  def ticker(self) -> str: ...

  @property
  def door(self) -> str: ...

  @property
  def proposal_handle(self) -> str: ...

  @property
  def proposal_id(self) -> str: ...

  @property
  def semantic_base(self) -> ModelVersionRef: ...

  @property
  def pointer_epoch(self) -> int: ...


class SuccessorRequest(Protocol):
  """A ref-independent successor publication request."""

  @property
  def user_id(self) -> str: ...

  @property
  def research_file_id(self) -> int: ...

  @property
  def ticker(self) -> str: ...

  @property
  def created_by(self) -> str: ...

  @property
  def intermediate(self) -> CompiledModelIntermediate: ...

  @property
  def source_bundle_record(self) -> TransformedBundleRecord | None: ...


class SuccessorPublicationError(RuntimeError):
  def __init__(self, code: str, message: str) -> None:
    super().__init__(message)
    self.code = code


class SuccessorCandidateDigestIntegrityError(ValueError):
  """Codec-local integrity failure for repository adapters to map."""

  code = SUCCESSOR_CANDIDATE_DIGEST_INTEGRITY_ERROR

  def __init__(self, reason: str, message: str) -> None:
    super().__init__(message)
    self.reason = reason


class _StrictFrozenContract(BaseModel):
  model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SuccessorCompilationIdentity(_StrictFrozenContract):
  """Persisted compilation identities with one field per durable source.

  ``compiler_version`` and ``template_version`` map directly to ModelVersion
  fields.  Every other field maps directly to a current typed payload's
  schema field; no composite or caller-defined ``schema_identity`` is allowed.
  """

  compiler_version: str = Field(min_length=1)
  template_version: str = Field(min_length=1)
  model_version_schema_version: Literal["2.0"]
  source_bundle_schema_version: Literal["2.0"]
  source_bundle_schema_epoch: Literal[12]
  model_spec_schema_version: Literal["3.0"]
  business_model_revision_schema_version: Literal["1.0"]
  business_model_schema_version: Literal["1.0"]
  driver_assumption_plan_schema_version: Literal["1.0"]

  @field_validator("compiler_version", "template_version")
  @classmethod
  def _normalized_text(cls, value: str, info: ValidationInfo) -> str:
    if value != value.strip():
      raise ValueError(f"{info.field_name} must not have surrounding whitespace")
    return value


class SuccessorSourceBundleIdentityComponents(_StrictFrozenContract):
  """Exact durable SourceBundle identity-column projection."""

  user_id: str = Field(min_length=1)
  research_file_id: int = Field(gt=0)
  ticker: str = Field(min_length=1)
  operation: str = Field(min_length=1)
  handoff_id: int = Field(gt=0)
  handoff_version: int = Field(gt=0)
  profile_id: str = Field(min_length=1)
  profile_version: str = Field(min_length=1)
  profile_sha256: str
  schema_epoch: int = Field(gt=0)
  corpus_id: str = Field(min_length=1)
  corpus_version: str = Field(min_length=1)
  corpus_manifest_sha256: str
  bmr_business_model_id: str = Field(min_length=1)
  bmr_revision: str = Field(min_length=1)
  bmr_content_sha256: str
  bmr_selection_epoch: int = Field(gt=0)
  proposal_id: str | None = Field(min_length=1)
  content_sha256: str | None = None
  @field_validator(
    "user_id",
    "ticker",
    "operation",
    "profile_id",
    "profile_version",
    "corpus_id",
    "corpus_version",
    "bmr_business_model_id",
    "bmr_revision",
    "proposal_id",
  )
  @classmethod
  def _normalized_text(
    cls,
    value: str | None,
    info: ValidationInfo,
  ) -> str | None:
    if value is not None and value != value.strip():
      raise ValueError(f"{info.field_name} must not have surrounding whitespace")
    return value

  @field_validator(
    "profile_sha256",
    "corpus_manifest_sha256",
    "bmr_content_sha256",
  )
  @classmethod
  def _sha256(cls, value: str, info: ValidationInfo) -> str:
    return validate_sha256_hex(value, field_name=info.field_name)

  @field_validator("content_sha256")
  @classmethod
  def _optional_sha256(cls, value: str | None, info: ValidationInfo) -> str | None:
    if value is None:
      return None
    return validate_sha256_hex(value, field_name=info.field_name)

#: The repository's sole SourceBundle identity codec over the durable identity columns.
SourceBundleIdentityCodec = Callable[[SuccessorSourceBundleIdentityComponents], str]


class SuccessorCandidateDigestPreimage(_StrictFrozenContract):
  """Complete ref-independent successor publication identity."""

  schema_version: Literal["1.0"]
  door: str = Field(min_length=1)
  proposal_handle: str = Field(min_length=1)
  semantic_base_model_id: str = Field(min_length=1)
  semantic_base_version: int = Field(ge=1)
  pointer_epoch: int = Field(ge=1)
  bundle_id: str = Field(min_length=1)
  bundle_identity_digest: str
  bundle_content_sha256: str
  model_spec_digest: str
  bmr_business_model_id: str = Field(min_length=1)
  bmr_revision: str = Field(min_length=1)
  bmr_content_sha256: str
  driver_assumption_plan_sha256: str
  compilation_identity: SuccessorCompilationIdentity

  @field_validator(
    "door",
    "proposal_handle",
    "semantic_base_model_id",
    "bundle_id",
    "bmr_business_model_id",
    "bmr_revision",
  )
  @classmethod
  def _normalized_text(cls, value: str, info: ValidationInfo) -> str:
    if value != value.strip():
      raise ValueError(f"{info.field_name} must not have surrounding whitespace")
    return value

  @field_validator(
    "bundle_identity_digest",
    "bundle_content_sha256",
    "model_spec_digest",
    "bmr_content_sha256",
    "driver_assumption_plan_sha256",
  )
  @classmethod
  def _sha256(cls, value: str, info: ValidationInfo) -> str:
    return validate_sha256_hex(value, field_name=info.field_name)


class SuccessorCandidateDurableComponents(_StrictFrozenContract):
  """Hydration inputs with every digest and its authoritative source payload.

  The scalar fields map to handle, SourceBundle, ModelVersion, and BMR rows.
  Payload fields must already be decoded into their current canonical types;
  reconstruction independently recomputes all content digests and schema
  identities before it emits a hashable preimage.
  """

  schema_version: Literal["1.0"]
  door: str = Field(min_length=1)
  proposal_handle: str = Field(min_length=1)
  semantic_base_model_id: str = Field(min_length=1)
  semantic_base_version: int = Field(ge=1)
  pointer_epoch: int = Field(ge=1)
  bundle_id: str = Field(min_length=1)
  bundle_identity: SuccessorSourceBundleIdentityComponents
  bundle_identity_digest: str
  bundle_content_sha256: str
  source_bundle_payload: SourceBundle
  model_spec_digest: str
  model_spec_payload: ModelSpec
  bmr_business_model_id: str = Field(min_length=1)
  bmr_revision: str = Field(min_length=1)
  bmr_content_sha256: str
  driver_assumption_plan_sha256: str
  business_model_revision_payload: BusinessModelRevision
  compilation_identity: SuccessorCompilationIdentity

  @field_validator(
    "door",
    "proposal_handle",
    "semantic_base_model_id",
    "bundle_id",
    "bmr_business_model_id",
    "bmr_revision",
  )
  @classmethod
  def _normalized_text(cls, value: str, info: ValidationInfo) -> str:
    if value != value.strip():
      raise ValueError(f"{info.field_name} must not have surrounding whitespace")
    return value

  @field_validator(
    "bundle_identity_digest",
    "bundle_content_sha256",
    "model_spec_digest",
    "bmr_content_sha256",
    "driver_assumption_plan_sha256",
  )
  @classmethod
  def _sha256(cls, value: str, info: ValidationInfo) -> str:
    return validate_sha256_hex(value, field_name=info.field_name)

  @field_validator(
    "source_bundle_payload",
    "model_spec_payload",
    "business_model_revision_payload",
    mode="before",
  )
  @classmethod
  def _typed_payload(cls, value: object, info: ValidationInfo) -> object:
    expected_by_field: dict[str | None, type[BaseModel]] = {
      "source_bundle_payload": SourceBundle,
      "model_spec_payload": ModelSpec,
      "business_model_revision_payload": BusinessModelRevision,
    }
    expected = expected_by_field[info.field_name]
    if not isinstance(value, expected):
      raise ValueError(f"{info.field_name} must be a decoded {expected.__name__}")
    return value


SuccessorCandidateDigestInput = SuccessorCandidateDigestPreimage | Mapping[str, object]
SuccessorCandidateDurableInput = (
  SuccessorCandidateDurableComponents | Mapping[str, object]
)


def _bare_compiler_input_digest(value: object) -> str:
  return compiler_input_content_hash(value).removeprefix("sha256:")


def _canonical_payload_digest(value: BaseModel) -> str:
  return hashlib.sha256(
    canonical_json_bytes(value.model_dump(mode="json"))
  ).hexdigest()


def _derived_compilation_identity(
  *,
  compiler_version: str,
  template_version: str,
  source_bundle: SourceBundle,
  model_spec: ModelSpec,
  business_model_revision: BusinessModelRevision,
) -> SuccessorCompilationIdentity:
  return SuccessorCompilationIdentity(
    compiler_version=compiler_version,
    template_version=template_version,
    model_version_schema_version="2.0",
    source_bundle_schema_version=source_bundle.schema_version,
    source_bundle_schema_epoch=source_bundle.schema_epoch,
    model_spec_schema_version=model_spec.schema_version,
    business_model_revision_schema_version=business_model_revision.schema_version,
    business_model_schema_version=business_model_revision.business_model.schema_version,
    driver_assumption_plan_schema_version=(
      business_model_revision.driver_assumption_plan.schema_version
    ),
  )


def _integrity(reason: str, message: str, cause: Exception | None = None) -> NoReturn:
  error = SuccessorCandidateDigestIntegrityError(reason, message)
  if cause is None:
    raise error
  raise error from cause


def _validated_durable_components(
  persisted_components: SuccessorCandidateDurableInput,
) -> SuccessorCandidateDurableComponents:
  if isinstance(persisted_components, SuccessorCandidateDurableComponents):
    return persisted_components
  try:
    return SuccessorCandidateDurableComponents.model_validate(persisted_components)
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "durable_components_malformed",
      "successor candidate durable components are malformed",
      exc,
    )


def reconstruct_successor_candidate_digest_preimage(
  persisted_components: SuccessorCandidateDurableInput,
  *,
  identity_codec: SourceBundleIdentityCodec,
) -> SuccessorCandidateDigestPreimage:
  """Prove durable components and reconstruct the normalized preimage."""

  value = _validated_durable_components(persisted_components)
  bundle = value.source_bundle_payload
  model_spec = value.model_spec_payload
  revision = value.business_model_revision_payload

  try:
    actual_identity_digest = identity_codec(value.bundle_identity)
    actual_bundle_digest = _bare_compiler_input_digest(bundle)
    actual_model_spec_digest = _bare_compiler_input_digest(model_spec)
    actual_bmr_digest = _canonical_payload_digest(revision.business_model)
    actual_plan_digest = _canonical_payload_digest(revision.driver_assumption_plan)
    expected_compilation_identity = _derived_compilation_identity(
      compiler_version=value.compilation_identity.compiler_version,
      template_version=value.compilation_identity.template_version,
      source_bundle=bundle,
      model_spec=model_spec,
      business_model_revision=revision,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "typed_payload_malformed",
      "successor candidate typed payload could not be canonicalized",
      exc,
    )

  digest_checks = (
    (
      "bundle_identity_digest_mismatch",
      value.bundle_identity_digest,
      actual_identity_digest,
    ),
    (
      "bundle_content_digest_mismatch",
      value.bundle_content_sha256,
      actual_bundle_digest,
    ),
    (
      "model_spec_digest_mismatch",
      value.model_spec_digest,
      actual_model_spec_digest,
    ),
    (
      "bmr_content_digest_mismatch",
      value.bmr_content_sha256,
      actual_bmr_digest,
    ),
    (
      "driver_assumption_plan_digest_mismatch",
      value.driver_assumption_plan_sha256,
      actual_plan_digest,
    ),
  )
  for reason, expected, actual in digest_checks:
    if not hmac.compare_digest(expected, actual):
      _integrity(reason, f"successor candidate {reason.replace('_', ' ')}")

  if value.compilation_identity != expected_compilation_identity:
    _integrity(
      "compilation_identity_mismatch",
      "successor candidate compilation identity disagrees with typed payloads",
    )

  semantic_base = ModelVersionRef(
    model_id=value.semantic_base_model_id,
    version=value.semantic_base_version,
  )
  bmr_binding = (
    value.bmr_business_model_id,
    value.bmr_revision,
    value.bmr_content_sha256,
    value.driver_assumption_plan_sha256,
  )
  identity_bmr_binding = (
    value.bundle_identity.bmr_business_model_id,
    value.bundle_identity.bmr_revision,
    value.bundle_identity.bmr_content_sha256,
    revision.driver_assumption_plan_sha256,
  )
  revision_binding = (
    revision.ref.business_model_id,
    revision.ref.revision,
    revision.ref.content_sha256,
    revision.driver_assumption_plan_sha256,
  )
  model_spec_binding = (
    model_spec.business_model_revision_ref.business_model_id,
    model_spec.business_model_revision_ref.revision,
    model_spec.business_model_revision_ref.content_sha256,
    model_spec.driver_assumption_plan_sha256,
  )
  if not (
    bmr_binding == identity_bmr_binding == revision_binding == model_spec_binding
  ):
    _integrity(
      "business_model_binding_mismatch",
      "successor candidate BMR bindings disagree",
    )
  if (
    model_spec.predecessor != semantic_base
    or model_spec.source_bundle_sha256 != value.bundle_content_sha256
  ):
    _integrity(
      "model_spec_binding_mismatch",
      "successor candidate ModelSpec bindings disagree",
    )
  if (
    value.bundle_identity.schema_epoch != bundle.schema_epoch
    or value.bundle_identity.ticker != bundle.ticker
    or bundle.ticker != model_spec.ticker
    or model_spec.ticker != revision.ticker
  ):
    _integrity(
      "scope_binding_mismatch",
      "successor candidate typed inputs disagree on scope or schema epoch",
    )

  try:
    return SuccessorCandidateDigestPreimage(
      schema_version=value.schema_version,
      door=value.door,
      proposal_handle=value.proposal_handle,
      semantic_base_model_id=value.semantic_base_model_id,
      semantic_base_version=value.semantic_base_version,
      pointer_epoch=value.pointer_epoch,
      bundle_id=value.bundle_id,
      bundle_identity_digest=value.bundle_identity_digest,
      bundle_content_sha256=value.bundle_content_sha256,
      model_spec_digest=value.model_spec_digest,
      bmr_business_model_id=value.bmr_business_model_id,
      bmr_revision=value.bmr_revision,
      bmr_content_sha256=value.bmr_content_sha256,
      driver_assumption_plan_sha256=(value.driver_assumption_plan_sha256),
      compilation_identity=value.compilation_identity,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "preimage_malformed",
      "successor candidate preimage is malformed",
      exc,
    )


def assemble_successor_candidate_digest_preimage(
  *,
  door: str,
  proposal_handle: str,
  semantic_base_ref: ModelVersionRef,
  pointer_epoch: int,
  loaded_source_bundle: LoadedBundle,
  model_spec: ModelSpec,
  business_model_revision: BusinessModelRevision,
  compiler_version: str,
  template_version: str,
  identity_codec: SourceBundleIdentityCodec,
) -> SuccessorCandidateDigestPreimage:
  """Assemble a candidate from exact typed publication inputs."""

  try:
    if not isinstance(loaded_source_bundle, LoadedBundle):
      raise ValueError("loaded_source_bundle must be a loaded SourceBundle row")
    if not isinstance(semantic_base_ref, ModelVersionRef):
      raise ValueError("semantic_base_ref must be a ModelVersionRef")
    if not isinstance(model_spec, ModelSpec):
      raise ValueError("model_spec must be a decoded ModelSpec")
    if not isinstance(business_model_revision, BusinessModelRevision):
      raise ValueError(
        "business_model_revision must be a decoded BusinessModelRevision"
      )
    source_bundle = decode_source_bundle_payload(loaded_source_bundle.payload)
    identity_fields = {
      field_name: getattr(loaded_source_bundle.identity, field_name)
      for field_name in SuccessorSourceBundleIdentityComponents.model_fields
    }
    durable_identity = SuccessorSourceBundleIdentityComponents.model_validate(
      identity_fields
    )
    compilation_identity = _derived_compilation_identity(
      compiler_version=compiler_version,
      template_version=template_version,
      source_bundle=source_bundle,
      model_spec=model_spec,
      business_model_revision=business_model_revision,
    )
    components = SuccessorCandidateDurableComponents(
      schema_version=SUCCESSOR_CANDIDATE_DIGEST_SCHEMA_VERSION,
      door=door,
      proposal_handle=proposal_handle,
      semantic_base_model_id=semantic_base_ref.model_id,
      semantic_base_version=semantic_base_ref.version,
      pointer_epoch=pointer_epoch,
      bundle_id=loaded_source_bundle.ref.bundle_id,
      bundle_identity=durable_identity,
      bundle_identity_digest=identity_codec(durable_identity),
      bundle_content_sha256=loaded_source_bundle.ref.content_sha256,
      source_bundle_payload=source_bundle,
      model_spec_digest=_bare_compiler_input_digest(model_spec),
      model_spec_payload=model_spec,
      bmr_business_model_id=business_model_revision.ref.business_model_id,
      bmr_revision=business_model_revision.ref.revision,
      bmr_content_sha256=business_model_revision.ref.content_sha256,
      driver_assumption_plan_sha256=(
        business_model_revision.driver_assumption_plan_sha256
      ),
      business_model_revision_payload=business_model_revision,
      compilation_identity=compilation_identity,
    )
  except SuccessorCandidateDigestIntegrityError:
    raise
  except SourceBundleRebuildRequired:
    # Not an assembly failure: the decode boundary decided the row's bytes
    # cannot be read by this binary and named the repair.  This codec does
    # not restate that in its own vocabulary; the publication door that
    # owns the seam carries it.
    raise
  except (AttributeError, TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "candidate_assembly_malformed",
      "successor candidate assembly inputs are malformed",
      exc,
    )
  return reconstruct_successor_candidate_digest_preimage(
    components, identity_codec=identity_codec
  )


def _validated_preimage(
  persisted_components: SuccessorCandidateDigestInput,
) -> SuccessorCandidateDigestPreimage:
  if isinstance(persisted_components, SuccessorCandidateDigestPreimage):
    return persisted_components
  return SuccessorCandidateDigestPreimage.model_validate(persisted_components)


def successor_candidate_digest_preimage_bytes(
  persisted_components: SuccessorCandidateDigestInput,
) -> bytes:
  """Serialize the versioned preimage as one fixed-order canonical JSON array."""

  value = _validated_preimage(persisted_components)
  compilation = value.compilation_identity
  canonical_preimage = (
    value.schema_version,
    value.door,
    value.proposal_handle,
    (value.semantic_base_model_id, value.semantic_base_version),
    value.pointer_epoch,
    (
      value.bundle_id,
      value.bundle_identity_digest,
      value.bundle_content_sha256,
    ),
    value.model_spec_digest,
    (
      value.bmr_business_model_id,
      value.bmr_revision,
      value.bmr_content_sha256,
      value.driver_assumption_plan_sha256,
    ),
    (
      compilation.compiler_version,
      compilation.template_version,
      compilation.model_version_schema_version,
      compilation.source_bundle_schema_version,
      compilation.source_bundle_schema_epoch,
      compilation.model_spec_schema_version,
      compilation.business_model_revision_schema_version,
      compilation.business_model_schema_version,
      compilation.driver_assumption_plan_schema_version,
    ),
  )
  return json.dumps(
    canonical_preimage,
    ensure_ascii=False,
    separators=(",", ":"),
    allow_nan=False,
  ).encode("utf-8")


def successor_candidate_digest(
  persisted_components: SuccessorCandidateDigestInput,
) -> str:
  """Return the sole SHA-256 identity for a proven successor candidate."""

  return hashlib.sha256(
    successor_candidate_digest_preimage_bytes(persisted_components)
  ).hexdigest()


def verify_hydrated_successor_candidate_digest(
  *,
  persisted_components: SuccessorCandidateDurableInput,
  stored_digest: object,
  identity_codec: SourceBundleIdentityCodec,
) -> str:
  """Reconstruct, recompute, and compare a hydrated candidate identity."""

  try:
    validated_stored_digest = validate_sha256_hex(
      stored_digest,
      field_name="successor_candidate_digest",
    )
    preimage = reconstruct_successor_candidate_digest_preimage(
      persisted_components, identity_codec=identity_codec
    )
    recomputed_digest = successor_candidate_digest(preimage)
  except SuccessorCandidateDigestIntegrityError:
    raise
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "stored_digest_malformed",
      "stored successor candidate digest is malformed",
      exc,
    )
  if not hmac.compare_digest(validated_stored_digest, recomputed_digest):
    _integrity(
      "stored_digest_mismatch",
      "stored successor candidate digest does not match durable state",
    )
  return recomputed_digest


def _sha256(value: bytes) -> str:
  return hashlib.sha256(value).hexdigest()


def decode_strict_json_object(content: bytes, *, name: str) -> dict[str, Any]:
  """Decode one UTF-8 JSON object without lossy duplicate/nonfinite values."""

  def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
      if key in result:
        raise ValueError(f"duplicate JSON key in {name}: {key}")
      result[key] = value
    return result

  def reject_constant(token: str) -> NoReturn:
    raise ValueError(f"non-finite JSON constant in {name}: {token}")

  try:
    value = json.loads(
      content.decode("utf-8"),
      object_pairs_hook=no_duplicates,
      parse_constant=reject_constant,
    )
  except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
    raise ValueError(f"stored {name} must be strict UTF-8 JSON") from exc
  if not isinstance(value, dict):
    raise ValueError(f"stored {name} must contain one JSON object")
  return value


GenerationT = TypeVar("GenerationT")


@dataclass(frozen=True, slots=True)
class HydratedModelArtifacts(Generic[GenerationT]):
  semantic_document: ModelSemanticDocument
  source_bundle_id: str
  source_bundle_identity_digest: str
  source_bundle_content_sha256: str
  source_bundle_payload: dict[str, Any]
  model_spec_content_sha256: str
  model_spec_payload: dict[str, Any]
  #: What the sealed compiler inputs record about their own wire generation,
  #: read without decoding either contract (Stage D plan §4.2, step D-2).
  recorded_generation: GenerationT


def decode_model_artifacts(
  model_version: ModelVersion,
  *,
  sidecar: bytes,
  provenance: bytes,
  validation_content: bytes,
  classify_generation: Callable[..., GenerationT],
) -> HydratedModelArtifacts[GenerationT]:
  """Decode a committed version's sidecar, provenance and validation bytes against its row.

  The caller reads the four committed files (the workbook only to prove it exists under its
  digest) and supplies ``classify_generation``, which reads the sealed compiler inputs'
  recorded wire generation from ``model_spec_payload`` and ``source_bundle_payload``.
  """

  semantic_payload = decode_strict_json_object(
    sidecar, name="semantic sidecar"
  )
  provenance_payload = decode_strict_json_object(
    provenance, name="provenance map"
  )
  validation_payload = decode_strict_json_object(
    validation_content, name="validation report"
  )
  semantic, decoded_provenance = validate_stored_provenance_pair(
    semantic_payload, provenance_payload
  )
  validation = decode_validation_report_payload(validation_payload)

  if (
    semantic.model_id != model_version.model_id
    or semantic.model_version != model_version.version
    or semantic.ticker != model_version.ticker
    or semantic.provenance_map != model_version.provenance_map
    or decoded_provenance.source_bundle_digest
    != model_version.source_bundle_digest
    or decoded_provenance.model_spec_digest != model_version.model_spec_digest
    or semantic.validation_status != validation.status
    or semantic.validation_blocker_count != validation.error_count
    or semantic.validation_warning_count != validation.uncleared_warning_count
    or model_version.status != validation.status
    or validation.validated_at != model_version.committed_at
  ):
    raise ValueError(
      "stored artifact identities or validation state disagree with ModelVersion"
    )

  source = semantic.render_state.compiler_inputs.source_bundle
  model_spec = semantic.render_state.compiler_inputs.model_spec
  if not isinstance(source.payload, dict) or not isinstance(model_spec.payload, dict):
    raise ValueError("stored CompilerInput payloads must be objects")
  if (
    source.ticker != model_version.ticker
    or model_spec.ticker != model_version.ticker
    or source.bundle_id is None
    or source.identity_digest is None
    or model_spec.bundle_id is not None
    or model_spec.identity_digest is not None
  ):
    raise ValueError("stored CompilerInput wrappers carry malformed lineage")
  source_content_sha256 = str(source.content_hash).removeprefix("sha256:")
  model_spec_content_sha256 = str(model_spec.content_hash).removeprefix("sha256:")
  if (
    source_content_sha256 != model_version.source_bundle_digest
    or model_spec_content_sha256 != model_version.model_spec_digest
  ):
    raise ValueError(
      "stored CompilerInput digests disagree with their ModelVersion"
    )
  return HydratedModelArtifacts(
    semantic_document=semantic,
    source_bundle_id=str(source.bundle_id),
    source_bundle_identity_digest=str(source.identity_digest),
    source_bundle_content_sha256=source_content_sha256,
    source_bundle_payload=dict(source.payload),
    model_spec_content_sha256=model_spec_content_sha256,
    model_spec_payload=dict(model_spec.payload),
    recorded_generation=classify_generation(
      model_spec_payload=model_spec.payload,
      source_bundle_payload=source.payload,
    ),
  )


def publication_integrity(message: str, cause: BaseException | None = None) -> NoReturn:
  error = SuccessorPublicationError("model_version_integrity_failed", message)
  if cause is None:
    raise error
  raise error from cause


def candidate_collision(message: str, cause: BaseException | None = None) -> NoReturn:
  error = SuccessorPublicationError(
    "model_spec_idempotency_key_collision", message
  )
  if cause is None:
    raise error
  raise error from cause


def _expected_template_identity(
  intermediate: CompiledModelIntermediate,
) -> str:
  metadata = getattr(intermediate.model, "metadata", None)
  return str(
    getattr(metadata, "template_version", None)
    or getattr(metadata, "source_model", None)
    or "unversioned"
  )


def _compilation_identity(
  *,
  compiler_version: str,
  template_version: str,
  source_bundle: SourceBundle,
  model_spec: ModelSpec,
  business_model_revision: BusinessModelRevision,
) -> SuccessorCompilationIdentity:
  return SuccessorCompilationIdentity(
    compiler_version=compiler_version,
    template_version=template_version,
    model_version_schema_version="2.0",
    source_bundle_schema_version=source_bundle.schema_version,
    source_bundle_schema_epoch=source_bundle.schema_epoch,
    model_spec_schema_version=model_spec.schema_version,
    business_model_revision_schema_version=business_model_revision.schema_version,
    business_model_schema_version=business_model_revision.business_model.schema_version,
    driver_assumption_plan_schema_version=(
      business_model_revision.driver_assumption_plan.schema_version
    ),
  )


def durable_candidate_components(
  *,
  handle: SuccessorHandle,
  loaded_bundle: LoadedBundle,
  bundle_identity_digest: str,
  model_version: ModelVersion,
  source_bundle: SourceBundle,
  model_spec: ModelSpec,
  business_model_revision: BusinessModelRevision,
) -> SuccessorCandidateDurableComponents:
  """``bundle_identity_digest`` is the repository codec over ``loaded_bundle.identity``."""

  identity = SuccessorSourceBundleIdentityComponents.model_validate({
    field_name: getattr(loaded_bundle.identity, field_name)
    for field_name in SuccessorSourceBundleIdentityComponents.model_fields
  })
  return SuccessorCandidateDurableComponents(
    schema_version=SUCCESSOR_CANDIDATE_DIGEST_SCHEMA_VERSION,
    door=handle.door,
    proposal_handle=handle.proposal_handle,
    semantic_base_model_id=handle.semantic_base.model_id,
    semantic_base_version=handle.semantic_base.version,
    pointer_epoch=handle.pointer_epoch,
    bundle_id=loaded_bundle.ref.bundle_id,
    bundle_identity=identity,
    bundle_identity_digest=bundle_identity_digest,
    bundle_content_sha256=loaded_bundle.ref.content_sha256,
    source_bundle_payload=source_bundle,
    model_spec_digest=model_version.model_spec_digest,
    model_spec_payload=model_spec,
    bmr_business_model_id=business_model_revision.ref.business_model_id,
    bmr_revision=business_model_revision.ref.revision,
    bmr_content_sha256=business_model_revision.ref.content_sha256,
    driver_assumption_plan_sha256=(
      business_model_revision.driver_assumption_plan_sha256
    ),
    business_model_revision_payload=business_model_revision,
    compilation_identity=_compilation_identity(
      compiler_version=model_version.compiler_version,
      template_version=model_version.template_version,
      source_bundle=source_bundle,
      model_spec=model_spec,
      business_model_revision=business_model_revision,
    ),
  )


def validate_handle_against_version(
  handle: SuccessorHandle,
  model_version: ModelVersion,
) -> None:
  origin = model_version.origin
  if (
    not isinstance(origin, SuccessorModelVersionOrigin)
    or origin.door != handle.door
    or origin.proposal_handle != handle.proposal_handle
    or origin.semantic_base != handle.semantic_base
  ):
    publication_integrity(
      "stored successor origin disagrees with its immutable proposal handle"
    )


def _validate_model_spec_handle_binding(
  model_spec: ModelSpec,
  handle: SuccessorHandle,
  *,
  collision: bool,
) -> None:
  if (
    model_spec.predecessor != handle.semantic_base
    or model_spec.idempotency_key != handle.proposal_id
  ):
    if collision:
      candidate_collision(
        "successor retry ModelSpec differs from the committed handle binding"
      )
    raise SuccessorPublicationError(
      "successor_proposal_handle_binding_mismatch",
      "successor ModelSpec does not bind the handle proposal and semantic base",
    )


def validate_durable_model_spec_binding(
  model_spec: ModelSpec,
  handle: SuccessorHandle,
  model_version: ModelVersion,
) -> None:
  """Prove persisted successor proposal identity across all durable carriers."""

  if (
    model_spec.predecessor != handle.semantic_base
    or model_spec.idempotency_key != handle.proposal_id
    or model_version.model_spec_predecessor != handle.semantic_base
    or model_version.model_spec_idempotency_key != handle.proposal_id
    or model_version.model_spec_predecessor != model_spec.predecessor
    or model_version.model_spec_idempotency_key != model_spec.idempotency_key
  ):
    raise ValueError(
      "stored ModelSpec, successor handle, and ModelVersion proposal binding differ"
    )


def validate_transformed_record_binding(
  request: SuccessorRequest,
  handle: SuccessorHandle,
  *,
  collision: bool,
) -> None:
  record = request.source_bundle_record
  if record is None:
    return
  identity = record.identity
  expected_operation = (
    f"successor:{handle.door}:"
    f"{handle.semantic_base.model_id}:{handle.semantic_base.version}"
  )
  valid = (
    (identity.user_id, identity.research_file_id, identity.ticker)
    == (request.user_id, request.research_file_id, request.ticker)
    and identity.operation == expected_operation
    and identity.proposal_id == handle.proposal_id
    and dict(record.payload)
    == request.intermediate.source_bundle.model_dump(mode="json")
  )
  if valid:
    return
  if collision:
    candidate_collision(
      "successor retry transformed SourceBundle differs from committed input"
    )
  raise SuccessorPublicationError(
    "source_bundle_purpose_mismatch",
    "transformed SourceBundle does not bind the exact successor request",
  )


def _validate_request_against_loaded_bundle(
  request: SuccessorRequest,
  handle: SuccessorHandle,
  loaded_bundle: LoadedBundle,
  business_model_revision: BusinessModelRevision,
  *,
  collision: bool,
) -> tuple[ModelSpec, str, str, str]:
  try:
    canonical_bundle = decode_source_bundle_payload(loaded_bundle.payload)
    model_spec = request.intermediate.model_spec
    _validate_model_spec_handle_binding(model_spec, handle, collision=collision)
    resolve_model_spec_business_model_revision(model_spec, business_model_revision)
    model_spec_digest = compiler_input_content_hash(model_spec).removeprefix("sha256:")
    if (
      canonical_bundle != request.intermediate.source_bundle
      or request.intermediate.source_bundle_content_sha256 != loaded_bundle.ref.content_sha256
      or model_spec.source_bundle_sha256 != loaded_bundle.ref.content_sha256
    ):
      raise ValueError("compiled inputs differ from the winning SourceBundle")
    compiler_identity = serialization.COMPUTE_ENGINE_VERSION
    template_identity = _expected_template_identity(request.intermediate)
    return model_spec, model_spec_digest, compiler_identity, template_identity
  except SuccessorPublicationError:
    raise
  except SourceBundleRebuildRequired as exc:
    # Nothing differs and nothing collided: the winning row's bytes are intact
    # and this binary's contract classes cannot read them.  The decode boundary
    # decided that and named the repair; carry it under the code this module
    # already uses for a predecessor that must be rebuilt.
    raise SuccessorPublicationError(
      "successor_base_rebuild_required",
      str(exc),
    ) from exc
  except Exception as exc:
    if collision:
      candidate_collision(
        "successor retry differs from its committed ref-independent candidate",
        exc,
      )
    raise SuccessorPublicationError(
      "source_bundle_binding_mismatch",
      "compiled successor inputs differ from authoritative durable inputs",
    ) from exc


def assemble_request_candidate(
  *,
  request: SuccessorRequest,
  handle: SuccessorHandle,
  loaded_bundle: LoadedBundle,
  business_model_revision: BusinessModelRevision,
  collision: bool,
  identity_codec: SourceBundleIdentityCodec,
) -> tuple[str, ModelSpec, str, str, str]:
  model_spec, model_spec_digest, compiler_identity, template_identity = (
    _validate_request_against_loaded_bundle(
      request,
      handle,
      loaded_bundle,
      business_model_revision,
      collision=collision,
    )
  )
  try:
    preimage = assemble_successor_candidate_digest_preimage(
      door=handle.door,
      proposal_handle=handle.proposal_handle,
      semantic_base_ref=handle.semantic_base,
      pointer_epoch=handle.pointer_epoch,
      loaded_source_bundle=loaded_bundle,
      model_spec=model_spec,
      business_model_revision=business_model_revision,
      compiler_version=compiler_identity,
      template_version=template_identity,
      identity_codec=identity_codec,
    )
    return (
      successor_candidate_digest(preimage),
      model_spec,
      model_spec_digest,
      compiler_identity,
      template_identity,
    )
  except SuccessorCandidateDigestIntegrityError as exc:
    if collision:
      candidate_collision(
        "successor retry candidate could not reproduce committed identity", exc
      )
    raise SuccessorPublicationError(
      "source_bundle_binding_mismatch",
      "successor candidate inputs are not coherently bound",
    ) from exc


def _validate_finalized_artifacts(
  finalized: FinalizedBuildArtifacts,
  *,
  request: SuccessorRequest,
  allocated_ref: ModelVersionRef,
  sequence_predecessor: ModelVersionRef | None,
  handle: SuccessorHandle,
  origin: SuccessorModelVersionOrigin,
  loaded_bundle: LoadedBundle,
  identity_digest: str,
  model_spec_digest: str,
  finalized_at: datetime,
) -> BuildVerdict:
  if not isinstance(finalized, FinalizedBuildArtifacts):
    raise SuccessorPublicationError(
      "model_compile_failed", "model finalizer returned an invalid artifact carrier"
    )
  if (
    finalized.intermediate_digest != request.intermediate.content_digest
    or finalized.allocated_ref != allocated_ref
    or finalized.sequence_predecessor != sequence_predecessor
    or finalized.semantic_base != handle.semantic_base
    or finalized.origin != origin
    or finalized.source_bundle_id != loaded_bundle.ref.bundle_id
    or finalized.source_bundle_identity_digest != identity_digest
    or finalized.finalized_at != finalized_at
  ):
    raise SuccessorPublicationError(
      "model_compile_failed",
      "finalized artifacts disagree with successor publication identity",
    )
  expected_locations = tuple(
    f"model-versions/{allocated_ref.model_id}/v{allocated_ref.version}/{name}"
    for name in _ARTIFACT_FILENAMES
  )
  if tuple(artifact.location for artifact in finalized.files) != expected_locations:
    raise SuccessorPublicationError(
      "model_compile_failed",
      "finalized artifact locations do not match allocated successor ref",
    )
  try:
    semantic_payload = decode_strict_json_object(
      finalized.semantic_sidecar.content,
      name="semantic sidecar",
    )
    provenance_payload = decode_strict_json_object(
      finalized.provenance_map.content,
      name="provenance map",
    )
    validation_payload = decode_strict_json_object(
      finalized.validation_report.content,
      name="validation report",
    )
    validation = decode_validation_report_payload(
      validation_payload
    )
    semantic, decoded_provenance = validate_stored_provenance_pair(
      semantic_payload,
      provenance_payload,
    )
  except (TypeError, ValueError, json.JSONDecodeError) as exc:
    raise SuccessorPublicationError(
      "model_compile_failed",
      "finalized successor semantic, provenance, or validation bytes are invalid",
    ) from exc
  compiler_source = semantic.render_state.compiler_inputs.source_bundle
  compiler_spec = semantic.render_state.compiler_inputs.model_spec
  if (
    ModelVersionRef(model_id=semantic.model_id, version=semantic.model_version)
    != allocated_ref
    or semantic.ticker != request.ticker
    or semantic.provenance_map != finalized.provenance_map.artifact_location
    or decoded_provenance.source_bundle_digest
    != loaded_bundle.ref.content_sha256
    or decoded_provenance.model_spec_digest != model_spec_digest
    or semantic.validation_status != validation.status
    or semantic.validation_blocker_count != validation.error_count
    or semantic.validation_warning_count != validation.uncleared_warning_count
    or compiler_source.ticker != request.ticker
    or compiler_spec.ticker != request.ticker
    or compiler_source.bundle_id != loaded_bundle.ref.bundle_id
    or compiler_source.identity_digest != identity_digest
    or compiler_spec.bundle_id is not None
    or compiler_spec.identity_digest is not None
    or compiler_source.content_hash != f"sha256:{loaded_bundle.ref.content_sha256}"
    or compiler_spec.content_hash != f"sha256:{model_spec_digest}"
    or compiler_source.payload
    != request.intermediate.source_bundle.model_dump(mode="json")
    or compiler_spec.payload != request.intermediate.model_spec.model_dump(mode="json")
    or validation.validated_at != finalized_at
  ):
    raise SuccessorPublicationError(
      "model_compile_failed",
      "finalized bytes do not carry exact successor CompilerInput identity",
    )
  return validation.status


def _successor_request_digest(
  *,
  candidate_digest: str,
  finalized: FinalizedBuildArtifacts,
) -> str:
  preimage = {
    "schema_version": "successor_publication.v1",
    "candidate_digest": candidate_digest,
    "allocated_ref": finalized.allocated_ref.model_dump(mode="json"),
    "sequence_predecessor": (
      None
      if finalized.sequence_predecessor is None
      else finalized.sequence_predecessor.model_dump(mode="json")
    ),
    "intermediate_digest": finalized.intermediate_digest,
    "finalized_at": finalized.finalized_at.isoformat(),
    "artifacts": [
      value.artifact_location.model_dump(mode="json") for value in finalized.files
    ],
  }
  return _sha256(canonical_json_bytes(preimage))


@dataclass(frozen=True, slots=True)
class CompiledSuccessorVersion:
  """The successor's ``ModelVersion`` row and its four finalized artifacts."""

  model_version: ModelVersion
  finalized: FinalizedBuildArtifacts


def compile_successor_version(
  *,
  request: SuccessorRequest,
  handle: SuccessorHandle,
  loaded_bundle: LoadedBundle,
  bundle_identity_digest: str,
  allocated_ref: ModelVersionRef,
  sequence_predecessor: ModelVersionRef | None,
  finalized_at: datetime,
  semantic_base_document: ModelSemanticDocument,
  candidate_digest: str,
  model_spec: ModelSpec,
  model_spec_digest: str,
  compiler_identity: str,
  template_identity: str,
  finalizer: Callable[..., FinalizedBuildArtifacts] = finalize_compiled_model,
  native: Any | None = None,
) -> CompiledSuccessorVersion:
  """Finalize a successor at its allocated ref, prove the bytes, and build its row.

  ``bundle_identity_digest`` is the repository codec over ``loaded_bundle.identity``;
  ``candidate_digest`` and the spec / compiler / template identities are
  ``assemble_request_candidate``'s answer for this request.
  """

  origin = SuccessorModelVersionOrigin(
    door=handle.door,
    proposal_handle=handle.proposal_handle,
    semantic_base=handle.semantic_base,
  )
  try:
    finalizer_kwargs: dict[str, Any] = {
      "allocated_ref": allocated_ref,
      "sequence_predecessor": sequence_predecessor,
      "semantic_base": handle.semantic_base,
      "origin": origin,
      "source_bundle_id": loaded_bundle.ref.bundle_id,
      "source_bundle_identity_digest": bundle_identity_digest,
      "finalized_at": finalized_at,
      "semantic_base_document": semantic_base_document,
    }
    if native is not None:
      finalizer_kwargs["native"] = native
    finalized = finalizer(
      request.intermediate,
      **finalizer_kwargs,
    )
  except SuccessorPublicationError:
    raise
  except Exception as exc:
    raise SuccessorPublicationError(
      "model_compile_failed",
      "compiled successor intermediate could not be finalized",
    ) from exc
  status = _validate_finalized_artifacts(
    finalized,
    request=request,
    allocated_ref=allocated_ref,
    sequence_predecessor=sequence_predecessor,
    handle=handle,
    origin=origin,
    loaded_bundle=loaded_bundle,
    identity_digest=bundle_identity_digest,
    model_spec_digest=model_spec_digest,
    finalized_at=finalized_at,
  )
  model_version = ModelVersion(
    model_id=allocated_ref.model_id,
    version=allocated_ref.version,
    user_id=handle.user_id,
    research_file_id=handle.research_file_id,
    ticker=handle.ticker,
    origin=origin,
    predecessor=sequence_predecessor,
    model_spec_predecessor=handle.semantic_base,
    model_spec_idempotency_key=model_spec.idempotency_key,
    request_digest=_successor_request_digest(
      candidate_digest=candidate_digest,
      finalized=finalized,
    ),
    source_bundle_digest=loaded_bundle.ref.content_sha256,
    model_spec_digest=model_spec_digest,
    compiler_version=compiler_identity,
    template_version=template_identity,
    workbook=finalized.workbook.artifact_location,
    semantic_sidecar=finalized.semantic_sidecar.artifact_location,
    provenance_map=finalized.provenance_map.artifact_location,
    validation_report=finalized.validation_report.artifact_location,
    status=status,
    successor_candidate_digest=candidate_digest,
    created_by=request.created_by,
    created_at=finalized_at,
    committed_at=finalized_at,
  )
  return CompiledSuccessorVersion(model_version=model_version, finalized=finalized)


def bind_successor_model_spec(
  *,
  predecessor_source_bundle: SourceBundle,
  source_bundle: SourceBundle,
  model_spec: ModelSpec,
  passthrough: bool,
  semantic_base: ModelVersionRef,
  proposal_id: str,
  ticker: str,
) -> ModelSpec:
  """Bind a door's transformed ModelSpec to its semantic base and proposal.

  ``passthrough`` says the door supplied no transformed SourceBundle record, so its
  ``source_bundle`` must be the predecessor's. The SourceBundle binding is checked
  once, by the compile (``compile_model_intermediate``).
  """

  successor_spec = ModelSpec.model_validate(model_spec).bind_successor(
    predecessor=semantic_base,
    idempotency_key=proposal_id,
  )
  if (
    source_bundle.ticker != ticker
    or successor_spec.ticker != ticker
  ):
    raise ValueError("compiled successor inputs must match the scoped ticker")
  if passthrough and source_bundle != predecessor_source_bundle:
    raise SuccessorPublicationError(
      "source_bundle_purpose_mismatch",
      "passthrough successor changed its predecessor SourceBundle",
    )
  return successor_spec
