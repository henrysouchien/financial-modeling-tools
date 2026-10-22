"""The stored-artifact read allowance for closed ModelVersion generations.

Authority: governing design ``build-model-data-identity-and-correspondence-
architecture-2026-08-17`` §15.3 (ModelSpec 3.0 plus the pre-D replay read
path, settled) and §7.5 (a published ModelVersion is a closed, self-describing
artifact); Stage D plan §4.2, step D-2.

A committed ``ModelVersion`` seals its two compiler inputs by sha256 into its
own immutable row.  The wire version each input carries is therefore a
*recorded* fact about the generation that version was built at — not a claim to
be re-validated against whatever contract classes this binary happens to carry.
Reading that record is what lets a pre-cut ModelVersion keep replaying after a
contract cut, without a migration and without a second writer.

Two rules make the allowance safe to be a product path rather than a
compatibility shim:

* **It is read-only by construction.**  Everything here is a pure function over
  payload mappings the caller already holds under digest.  It opens no file,
  runs no query, decodes no ``ModelSpec`` and no ``SourceBundle``, and returns
  no object a writer could mistake for a decoded contract.
* **It never upgrades and never infers.**  A payload that does not state its own
  version is malformed, not pre-cut: it refuses.  A descriptor cannot be
  constructed claiming a generation its own recorded fields contradict.  New
  work on a pre-cut version refuses with a typed code that names the repair —
  rebuild, never migrate (D-DP5, D-DP9).
* **Direction is derived, never assumed.**  "Pre-cut" is a claim about time, so
  it is computed from the recorded ModelSpec version, ordered against this
  binary's constant as a ``<major>.<minor>`` numeral.  A SourceBundle epoch
  above the largest this binary knows is separately a forward-generation
  rollback guard.  Bytes recording a generation *ahead* of this binary are not
  pre-cut and are never served through the allowance: they refuse.  Design
  §15.3 scopes the allowance to stored specs that predate the cut, and Stage D
  plan §5's D-4 rollback contract requires a rolled-back binary to typed-reject
  what the newer generation built.
"""

from __future__ import annotations

import re
from typing import Any, Final, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from schema.build_model_product_contracts import (
  MODEL_SPEC_SCHEMA_VERSION,
  SUPPORTED_SOURCE_BUNDLE_SCHEMA_EPOCHS,
)


#: Stored bytes that do not state their own wire version.  Distinct from
#: ``pre_cut``: an artifact that cannot self-describe is corrupt, and inferring
#: a generation for it would be the silent upgrade this allowance exists to
#: prevent.
STORED_CONTRACT_SELF_DESCRIPTION_ERROR: Final = (
  "stored_contract_self_description_malformed"
)

#: New work was asked of a ModelVersion whose sealed compiler inputs predate
#: this binary's contract generation.
MODEL_VERSION_PRE_CUT_REBUILD_ERROR: Final = "model_version_pre_cut_rebuild_required"

#: A ModelVersion whose sealed compiler inputs record a generation *ahead* of
#: this binary's.  Distinct from ``pre_cut`` in both directions: the allowance
#: does not cover it (design §15.3 scopes the read path to specs that predate
#: the cut), and no rebuild on this binary can produce it.
MODEL_VERSION_FUTURE_GENERATION_ERROR: Final = (
  "model_version_future_generation_unreadable"
)

#: The one repair.  Stored ModelVersions are never migrated (design §7.5,
#: §15.3; Stage D plan D-DP5/D-DP9), so every refusal below names a rebuild.
PRE_CUT_REBUILD_REPAIR: Final = (
  "rebuild and republish this model on the current contract generation; "
  "stored ModelVersions are never migrated"
)

#: The same repair for the stale-current-pointer case (D-DP5): the pointer is
#: not migrated onto the rebuilt version either — republishing moves it.
PRE_CUT_CURRENT_POINTER_REPAIR: Final = (
  "rebuild and republish this model on the current contract generation, then "
  "select the rebuilt version as current; stored ModelVersions and current "
  "pointers are never migrated"
)

#: The repair for the other direction.  A rolled-back binary cannot rebuild its
#: way to bytes a newer generation wrote, so the repair names both live moves
#: (Stage D plan §5, D-4 rollback contract).
FUTURE_GENERATION_REPAIR: Final = (
  "serve this model from the contract generation that built it, or rebuild it "
  "on this one; stored ModelVersions are never migrated"
)


class StoredContractSelfDescriptionMalformed(ValueError):
  """A sealed compiler input does not state its own wire version.

  The bytes are corrupt, so no generation is inferred and no retry reads them;
  the message names the one repair, the same rebuild a pre-cut version takes.
  """

  code = STORED_CONTRACT_SELF_DESCRIPTION_ERROR

  def __init__(self, detail: str) -> None:
    super().__init__(f"{detail} — {PRE_CUT_REBUILD_REPAIR}")


class StoredModelVersionGenerationRefused(ValueError):
  """A stored ModelVersion is not of the generation the operation requires.

  One base for both directions, so a door refuses "this binary cannot author
  from these bytes" once rather than enumerating which way the mismatch ran.
  Every subclass carries the operation it refused, the record it read, and the
  repair — a refusal here never leaves the operator without a next move.
  """

  code: str = ""
  #: Filled in by the subclass; used only to build the message.
  _direction = "is not the current generation"

  def __init__(
    self,
    *,
    operation: str,
    generation: "StoredContractGeneration",
    repair: str,
  ) -> None:
    if not isinstance(generation, StoredContractGeneration):
      raise TypeError("generation must be a StoredContractGeneration")
    operation_text = str(operation).strip()
    if not operation_text:
      raise ValueError("a generation refusal must name the operation it refused")
    repair_text = str(repair).strip()
    if not repair_text:
      raise ValueError("a generation refusal must name its repair")
    self.operation = operation_text
    self.generation = generation
    self.repair = repair_text
    super().__init__(
      f"{operation_text} requires a current-generation ModelVersion; the "
      "sealed compiler inputs record ModelSpec "
      f"{generation.model_spec_schema_version!r} and SourceBundle epoch "
      f"{generation.source_bundle_schema_epoch}, which "
      f"{type(self)._direction} — {repair_text}"
    )


class PreCutModelVersionRebuildRequired(StoredModelVersionGenerationRefused):
  """New work was requested on a ModelVersion this binary cannot author from."""

  code = MODEL_VERSION_PRE_CUT_REBUILD_ERROR
  _direction = "predates this binary's contract generation"

  def __init__(
    self,
    *,
    operation: str,
    generation: "StoredContractGeneration",
    repair: str = PRE_CUT_REBUILD_REPAIR,
  ) -> None:
    super().__init__(operation=operation, generation=generation, repair=repair)


class FutureGenerationModelVersionUnreadable(StoredModelVersionGenerationRefused):
  """The stored bytes record a generation this binary does not yet carry.

  Not a pre-cut version and not corruption: a binary rolled back across a
  contract cut meets exactly this when it is handed something the newer
  generation built (Stage D plan §5, D-4 rollback contract).  The read
  allowance does not cover it — §15.3 scopes the allowance to stored specs that
  *predate* the cut — so this refuses at the replay door as well as at the
  new-work doors.
  """

  code = MODEL_VERSION_FUTURE_GENERATION_ERROR
  _direction = "postdates this binary's contract generation"

  def __init__(
    self,
    *,
    operation: str,
    generation: "StoredContractGeneration",
    repair: str = FUTURE_GENERATION_REPAIR,
  ) -> None:
    super().__init__(operation=operation, generation=generation, repair=repair)


class StoredContractGeneration(BaseModel):
  """What a ModelVersion's sealed compiler inputs record about themselves.

  ``generation`` is derived, never asserted: the validator recomputes it from
  the two recorded fields, so no caller can hand a version around wearing a
  label its own record contradicts — neither a pre-cut one labelled
  ``current`` nor a forward one labelled ``pre_cut`` to slip it through the
  read allowance.
  """

  model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

  generation: Literal["current", "pre_cut", "future"]
  model_spec_schema_version: str = Field(min_length=1)
  source_bundle_schema_epoch: int = Field(gt=0)

  @model_validator(mode="after")
  def _generation_is_derived_from_the_record(self) -> "StoredContractGeneration":
    if self.generation != _derive_generation(
      model_spec_schema_version=self.model_spec_schema_version,
      source_bundle_schema_epoch=self.source_bundle_schema_epoch,
    ):
      raise ValueError(
        "stored contract generation must be derived from the recorded "
        "ModelSpec version and SourceBundle epoch"
      )
    return self

  @property
  def is_current(self) -> bool:
    return self.generation == "current"

  @property
  def is_pre_cut(self) -> bool:
    return self.generation == "pre_cut"


  def require_current(
    self,
    *,
    operation: str,
    repair: str | None = None,
  ) -> None:
    """Refuse new work on a non-current generation with a repair-naming code.

    Both directions refuse, and each names its own repair: a pre-cut version is
    rebuilt on this generation, while a future one cannot be — it is served
    from the generation that built it, or rebuilt here.
    """

    if self.generation == "current":
      return
    refusal = (
      FutureGenerationModelVersionUnreadable
      if self.generation == "future"
      else PreCutModelVersionRebuildRequired
    )
    if repair is None:
      raise refusal(operation=operation, generation=self)
    raise refusal(operation=operation, generation=self, repair=repair)


_MODEL_SPEC_VERSION = re.compile(r"(\d+)\.(\d+)")


def _version_ordinal(value: str, *, role: str = "ModelSpec") -> tuple[int, int]:
  """Order a recorded wire version against this binary's own.

  Versions are ``<major>.<minor>`` numerals (2.0 → 2.1 → 2.2 → 3.0), so the
  comparison is arithmetic and needs no enumerated set of retired versions to
  drift out of date.  A recorded value that cannot be ordered is not a
  direction this allowance may guess at: it fails closed as malformed.
  """

  match = _MODEL_SPEC_VERSION.fullmatch(value)
  if match is None:
    raise StoredContractSelfDescriptionMalformed(
      f"stored {role} compiler input records a schema_version this binary "
      f"cannot order against its own generation; observed={value!r}"
    )
  return int(match.group(1)), int(match.group(2))


def _derive_generation(
  *,
  model_spec_schema_version: str,
  source_bundle_schema_epoch: int,
) -> Literal["current", "pre_cut", "future"]:
  """Place recorded bytes before, on, or after this binary's generation.

  ModelSpec version determines the temporal generation.  SourceBundle epochs
  are monotonically assigned, so an epoch above the largest this binary knows
  is separately classified ``future`` for rollback safety.  Older epochs do
  not imply an older generation or answer whether the stored bytes decode.
  """

  recorded_version = _version_ordinal(model_spec_schema_version)
  current_version = _version_ordinal(MODEL_SPEC_SCHEMA_VERSION)
  if recorded_version > current_version or source_bundle_schema_epoch > max(
    SUPPORTED_SOURCE_BUNDLE_SCHEMA_EPOCHS
  ):
    return "future"
  if recorded_version == current_version:
    return "current"
  return "pre_cut"


def _recorded_text(payload: Any, *, key: str, role: str) -> str:
  if not isinstance(payload, Mapping):
    raise StoredContractSelfDescriptionMalformed(
      f"stored {role} compiler input must be an object to record its {key}"
    )
  observed = payload.get(key)
  if type(observed) is not str or not observed:
    raise StoredContractSelfDescriptionMalformed(
      f"stored {role} compiler input does not record a {key}; "
      f"observed={observed!r}"
    )
  return observed


def _recorded_int(payload: Any, *, key: str, role: str) -> int:
  if not isinstance(payload, Mapping):
    raise StoredContractSelfDescriptionMalformed(
      f"stored {role} compiler input must be an object to record its {key}"
    )
  observed = payload.get(key)
  if type(observed) is not int or observed <= 0:
    raise StoredContractSelfDescriptionMalformed(
      f"stored {role} compiler input does not record a {key}; "
      f"observed={observed!r}"
    )
  return observed


def classify_stored_contract_generation(
  *,
  model_spec_payload: Any,
  source_bundle_payload: Any,
) -> StoredContractGeneration:
  """Read the generation a ModelVersion's sealed compiler inputs record.

  The caller must already have verified both payloads against the digests the
  ModelVersion row seals them under; this function reads the record, it does
  not establish it.  Nothing here decodes either contract.

  The result is three-valued — ``current``, ``pre_cut``, ``future`` — because
  only ``pre_cut`` is what the read allowance covers.  Classifying is not
  deciding: each door decides what to do with the record it gets, and the
  replay door refuses ``future`` rather than serving it as pre-cut.
  """

  model_spec_schema_version = _recorded_text(
    model_spec_payload,
    key="schema_version",
    role="ModelSpec",
  )
  source_bundle_schema_epoch = _recorded_int(
    source_bundle_payload,
    key="schema_epoch",
    role="SourceBundle",
  )
  return StoredContractGeneration(
    generation=_derive_generation(
      model_spec_schema_version=model_spec_schema_version,
      source_bundle_schema_epoch=source_bundle_schema_epoch,
    ),
    model_spec_schema_version=model_spec_schema_version,
    source_bundle_schema_epoch=source_bundle_schema_epoch,
  )


def classify_stored_compiler_inputs(compiler_inputs: Any) -> StoredContractGeneration:
  """Classify from a stored render state's ``compiler_inputs`` wrapper pair."""

  source_input = getattr(compiler_inputs, "source_bundle", None)
  model_spec_input = getattr(compiler_inputs, "model_spec", None)
  if source_input is None or model_spec_input is None:
    if not isinstance(compiler_inputs, Mapping):
      raise StoredContractSelfDescriptionMalformed(
        "stored compiler inputs must carry a SourceBundle and a ModelSpec"
      )
    source_input = compiler_inputs.get("source_bundle")
    model_spec_input = compiler_inputs.get("model_spec")
  return classify_stored_contract_generation(
    model_spec_payload=_input_payload(model_spec_input, role="ModelSpec"),
    source_bundle_payload=_input_payload(source_input, role="SourceBundle"),
  )


def _input_payload(compiler_input: Any, *, role: str) -> Any:
  if compiler_input is None:
    raise StoredContractSelfDescriptionMalformed(
      f"stored compiler inputs do not carry a {role}"
    )
  payload = getattr(compiler_input, "payload", None)
  if payload is None and isinstance(compiler_input, Mapping):
    payload = compiler_input.get("payload")
  return payload


__all__ = [
  "FUTURE_GENERATION_REPAIR",
  "MODEL_VERSION_FUTURE_GENERATION_ERROR",
  "MODEL_VERSION_PRE_CUT_REBUILD_ERROR",
  "PRE_CUT_CURRENT_POINTER_REPAIR",
  "PRE_CUT_REBUILD_REPAIR",
  "STORED_CONTRACT_SELF_DESCRIPTION_ERROR",
  "FutureGenerationModelVersionUnreadable",
  "PreCutModelVersionRebuildRequired",
  "StoredContractGeneration",
  "StoredContractSelfDescriptionMalformed",
  "StoredModelVersionGenerationRefused",
  "classify_stored_compiler_inputs",
  "classify_stored_contract_generation",
]
