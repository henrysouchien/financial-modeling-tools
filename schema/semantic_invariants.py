"""Portable invariant IR for exact semantic-cohort contracts."""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class FieldEqualsInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["field_equals.v1"]
    field_path: tuple[str, ...] = Field(min_length=1)
    expected_json_value: Any


class FieldImplicationInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["field_implication.v1"]
    when_field_path: tuple[str, ...] = Field(min_length=1)
    when_json_value: Any
    then_field_path: tuple[str, ...] = Field(min_length=1)
    allowed_json_values: tuple[Any, ...] = Field(min_length=1)


class CollectionsEmptyWhenInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["collections_empty_when.v1"]
    scope_field_path: tuple[str, ...] = Field(min_length=1)
    when_relative_path: tuple[str, ...] = Field(min_length=1)
    when_json_value: Any
    collection_relative_paths: tuple[tuple[str, ...], ...] = Field(min_length=1)


class MethodAdmissibilityInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["method_admissibility.v1"]
    state_field_path: tuple[str, ...] = Field(min_length=1)
    resolved_state_value: Any
    method_field_path: tuple[str, ...] = Field(min_length=1)
    admissible_methods: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _methods(self) -> MethodAdmissibilityInvariant:
        if self.admissible_methods != tuple(sorted(set(self.admissible_methods))):
            raise ValueError("admissible_methods must be sorted and unique")
        return self


class UniquenessInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["uniqueness.v1"]
    collection_field_path: tuple[str, ...] = Field(min_length=1)
    key_field_paths: tuple[tuple[str, ...], ...] = Field(min_length=1)


class EvidenceClosureInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["evidence_closure.v1"]
    reference_collection_path: tuple[str, ...] = Field(min_length=1)
    registry_collection_path: tuple[str, ...] = Field(min_length=1)
    reference_id_path: tuple[str, ...] = Field(min_length=1)
    registry_id_path: tuple[str, ...] = Field(min_length=1)


class ReviewChainInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["review_chain.v1"]
    events_field_path: tuple[str, ...] = Field(min_length=1)
    event_type_field: str = Field(min_length=1)
    receipt_type: str = Field(min_length=1)
    revocation_type: str = Field(min_length=1)
    decision_id_field: str = Field(min_length=1)
    revocation_target_field: str = Field(min_length=1)
    supersedes_field: str = Field(min_length=1)
    subject_key_fields: tuple[str, ...] = Field(min_length=1)


class ReadinessInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    operator: Literal["readiness.v1"]
    state_field_path: tuple[str, ...] = Field(min_length=1)
    ready_state_value: Any
    blocking_reasons_field_path: tuple[str, ...] = Field(min_length=1)


SemanticInvariant = Annotated[
    FieldEqualsInvariant
    | FieldImplicationInvariant
    | CollectionsEmptyWhenInvariant
    | MethodAdmissibilityInvariant
    | UniquenessInvariant
    | EvidenceClosureInvariant
    | ReviewChainInvariant
    | ReadinessInvariant,
    Field(discriminator="operator"),
]
_INVARIANT_ADAPTER = TypeAdapter(SemanticInvariant)


class SemanticInvariantSet(BaseModel):
    """Producer-owned closed invariant program."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    contract_name: Literal["semantic-invariant-set"]
    schema_version: Literal["1"]
    producer_contract_name: str = Field(min_length=1)
    producer_schema_version: str = Field(min_length=1)
    invariants: tuple[SemanticInvariant, ...] = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _digest(self) -> SemanticInvariantSet:
        if self.content_sha256 != semantic_invariant_set_sha256(self):
            raise ValueError("semantic invariant-set digest mismatch")
        return self


class SemanticInvariantRegistration(BaseModel):
    """Consumer pin for one exact producer schema and invariant interpreter."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    artifact_path: str = Field(min_length=1)
    producer_contract_name: str = Field(min_length=1)
    producer_schema_version: str = Field(min_length=1)
    invariant_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    interpreter_id: Literal["openai.semantic-invariant-interpreter"]
    interpreter_version: Literal["1"]
    generated_validator_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class SemanticInvariantRegistry(BaseModel):
    """Closed registry that pins programs and this exact interpreter source."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    contract_name: Literal["semantic-invariant-registry"]
    schema_version: Literal["1"]
    registrations: tuple[SemanticInvariantRegistration, ...] = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _validate_registry(self) -> "SemanticInvariantRegistry":
        identities = tuple(
            (item.producer_contract_name, item.producer_schema_version)
            for item in self.registrations
        )
        if identities != tuple(sorted(set(identities))):
            raise ValueError(
                "semantic invariant registrations must be sorted and unique"
            )
        if self.content_sha256 != semantic_invariant_registry_sha256(self):
            raise ValueError("semantic invariant registry digest mismatch")
        return self


def semantic_invariant_registry_sha256(
    registry: SemanticInvariantRegistry | dict[str, Any],
) -> str:
    payload = (
        registry.model_dump(mode="json", exclude={"content_sha256"})
        if isinstance(registry, SemanticInvariantRegistry)
        else {
            key: item
            for key, item in registry.items()
            if key != "content_sha256"
        }
    )
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


_SCHEMA_ROOT = Path(__file__).resolve().parent
_REPOSITORY_ROOT = _SCHEMA_ROOT.parent
_INVARIANT_REGISTRY_PATH = _SCHEMA_ROOT / "semantic-invariant-sets" / "registry.json"


@lru_cache(maxsize=1)
def load_semantic_invariant_registry() -> SemanticInvariantRegistry:
    registry = SemanticInvariantRegistry.model_validate_json(
        _INVARIANT_REGISTRY_PATH.read_bytes()
    )
    interpreter_digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if any(
        item.generated_validator_sha256 != interpreter_digest
        for item in registry.registrations
    ):
        raise ValueError("semantic invariant interpreter digest mismatch")
    return registry


@lru_cache(maxsize=None)
def load_registered_semantic_invariant_set(
    contract_name: str,
    schema_version: str,
) -> SemanticInvariantSet:
    registry = load_semantic_invariant_registry()
    matches = tuple(
        item
        for item in registry.registrations
        if (
            item.producer_contract_name,
            item.producer_schema_version,
        )
        == (contract_name, schema_version)
    )
    if len(matches) != 1:
        raise ValueError(
            "semantic invariant registry requires one exact producer identity"
        )
    registration = matches[0]
    artifact_path = (_REPOSITORY_ROOT / registration.artifact_path).resolve()
    allowed_root = (_SCHEMA_ROOT / "semantic-invariant-sets").resolve()
    if artifact_path.parent != allowed_root:
        raise ValueError("semantic invariant artifact path leaves the pinned registry")
    program = SemanticInvariantSet.model_validate_json(artifact_path.read_bytes())
    if (
        program.producer_contract_name != contract_name
        or program.producer_schema_version != schema_version
        or program.content_sha256 != registration.invariant_set_sha256
    ):
        raise ValueError("semantic invariant registration/program mismatch")
    return program


def evaluate_registered_semantic_invariants(
    payload: dict[str, Any],
    *,
    contract_name: str,
    schema_version: str,
) -> None:
    evaluate_semantic_invariants(
        payload,
        load_registered_semantic_invariant_set(contract_name, schema_version),
        contract_name=contract_name,
        schema_version=schema_version,
    )


def semantic_invariant_set_sha256(invariant_set: SemanticInvariantSet) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(
            invariant_set.model_dump(mode="json", exclude={"content_sha256"})
        )
    ).hexdigest()


def evaluate_semantic_invariants(
    payload: dict[str, Any],
    invariant_set: SemanticInvariantSet,
    *,
    contract_name: str,
    schema_version: str,
) -> None:
    """Evaluate a known v1 program or fail at the exact producer boundary."""
    if (
        contract_name != invariant_set.producer_contract_name
        or schema_version != invariant_set.producer_schema_version
    ):
        raise ValueError("semantic invariant-set producer identity mismatch")
    for raw_invariant in invariant_set.invariants:
        invariant = _INVARIANT_ADAPTER.validate_python(raw_invariant)
        if isinstance(invariant, FieldEqualsInvariant):
            if _at(payload, invariant.field_path) != invariant.expected_json_value:
                raise ValueError(f"field_equals failed at {invariant.field_path!r}")
        elif isinstance(invariant, FieldImplicationInvariant):
            if (
                _at(payload, invariant.when_field_path) == invariant.when_json_value
                and _at(payload, invariant.then_field_path)
                not in invariant.allowed_json_values
            ):
                raise ValueError(
                    f"field_implication failed at {invariant.then_field_path!r}"
                )
        elif isinstance(invariant, CollectionsEmptyWhenInvariant):
            scope = _at(payload, invariant.scope_field_path)
            if scope is not None:
                if not isinstance(scope, dict):
                    raise ValueError(
                        "collections_empty_when scope must be an object or null"
                    )
                if (
                    _at(scope, invariant.when_relative_path)
                    == invariant.when_json_value
                    and any(
                        bool(_at(scope, path))
                        for path in invariant.collection_relative_paths
                    )
                ):
                    raise ValueError("collections_empty_when invariant failed")
        elif isinstance(invariant, MethodAdmissibilityInvariant):
            if (
                _at(payload, invariant.state_field_path)
                == invariant.resolved_state_value
                and _at(payload, invariant.method_field_path)
                not in invariant.admissible_methods
            ):
                raise ValueError(
                    f"method_admissibility failed at {invariant.method_field_path!r}"
                )
        elif isinstance(invariant, UniquenessInvariant):
            rows = _collection(payload, invariant.collection_field_path)
            keys = [
                tuple(_at(row, path) for path in invariant.key_field_paths)
                for row in rows
            ]
            if len(keys) != len(set(keys)):
                raise ValueError("uniqueness invariant failed")
        elif isinstance(invariant, EvidenceClosureInvariant):
            refs = _collection(payload, invariant.reference_collection_path)
            registry = _collection(payload, invariant.registry_collection_path)
            issued = {
                _at(row, invariant.registry_id_path)
                for row in registry
            }
            missing = {
                reference_id
                for row in refs
                if (reference_id := _at(row, invariant.reference_id_path))
                is not None
            } - issued
            if missing:
                raise ValueError(f"evidence_closure failed: {sorted(missing)!r}")
        elif isinstance(invariant, ReviewChainInvariant):
            _evaluate_review_chain(payload, invariant)
        elif isinstance(invariant, ReadinessInvariant):
            ready = (
                _at(payload, invariant.state_field_path)
                == invariant.ready_state_value
            )
            blocked = bool(_at(payload, invariant.blocking_reasons_field_path))
            if ready == blocked:
                raise ValueError("readiness invariant failed")
        else:  # pragma: no cover - the discriminated adapter is closed
            raise ValueError("unknown semantic invariant operator")


def _at(value: Any, path: tuple[str, ...]) -> Any:
    current = value
    for component in path:
        if not isinstance(current, dict) or component not in current:
            raise ValueError(f"semantic invariant path does not exist: {path!r}")
        current = current[component]
    return current


def _collection(value: Any, path: tuple[str, ...]) -> list[dict[str, Any]]:
    rows = _at(value, path)
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"semantic invariant path is not an object collection: {path!r}")
    return rows


def _evaluate_review_chain(
    payload: dict[str, Any],
    invariant: ReviewChainInvariant,
) -> None:
    events = _collection(payload, invariant.events_field_path)
    receipts: dict[str, dict[str, Any]] = {
        row[invariant.decision_id_field]: row
        for row in events
        if row.get(invariant.event_type_field) == invariant.receipt_type
    }
    if len(receipts) != sum(
        row.get(invariant.event_type_field) == invariant.receipt_type
        for row in events
    ):
        raise ValueError("review_chain duplicate decision ID")
    inactive: set[str] = set()
    for row in events:
        event_type = row.get(invariant.event_type_field)
        if event_type == invariant.revocation_type:
            target = row.get(invariant.revocation_target_field)
            if target not in receipts or target in inactive:
                raise ValueError("review_chain invalid revocation")
            inactive.add(target)
        elif event_type == invariant.receipt_type:
            target = row.get(invariant.supersedes_field)
            if target is not None:
                if target not in receipts or target in inactive:
                    raise ValueError("review_chain invalid supersession")
                subject = tuple(row.get(field) for field in invariant.subject_key_fields)
                target_subject = tuple(
                    receipts[target].get(field)
                    for field in invariant.subject_key_fields
                )
                if subject != target_subject:
                    raise ValueError("review_chain supersession subject mismatch")
                inactive.add(target)
        else:
            raise ValueError("review_chain unknown event type")
    active_subjects: set[tuple[Any, ...]] = set()
    for identifier, row in receipts.items():
        if identifier in inactive:
            continue
        subject = tuple(row.get(field) for field in invariant.subject_key_fields)
        if subject in active_subjects:
            raise ValueError("review_chain duplicate active subject")
        active_subjects.add(subject)


__all__ = [
    "SemanticInvariantRegistration",
    "SemanticInvariantRegistry",
    "SemanticInvariantSet",
    "evaluate_registered_semantic_invariants",
    "evaluate_semantic_invariants",
    "load_registered_semantic_invariant_set",
    "load_semantic_invariant_registry",
    "semantic_invariant_registry_sha256",
    "semantic_invariant_set_sha256",
]
