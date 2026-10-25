"""Canonical semantic document for one compiled ModelVersion."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Mapping, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .finding_vocabulary import BuildVerdict
from .build_model_product_contracts import (
    ArtifactLocation,
    ProvenanceMap,
    ProvenanceMapRebuildRequired,
    ProvenanceMapV1_0,
    ScheduleInputAudit,
    ScheduleInputAuditNotRecorded,
    ValuationEvidenceStatus,
    ValuationEvidenceStatusNotRecorded,
    decode_provenance_map,
)
from .handoff import ModelRenderState
from .models import FinancialModel
from .ticker_contract import normalize_ticker


#: Any status a stored semantic document may carry: its own version's literal.
RecordedValidationStatus = Literal["pass", "blocked", "ready_with_warnings", "ready"]


class SemanticValueSnapshot(BaseModel):
    """Persisted compare input for one rendered semantic cell."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sheet: str = Field(min_length=1)
    cell: str = Field(pattern=r"^[A-Z]+[1-9][0-9]*$")
    semantic_item_id: str = Field(min_length=1)
    semantic_label: str = Field(min_length=1)
    period: int
    state: str = Field(min_length=1)
    value: Any = None
    tolerance: float = Field(ge=0)


class _ModelSemanticDocumentCore(BaseModel):
    """Fields and invariants shared by every semantic-document wire version."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    model_id: str = Field(min_length=1)
    model_version: int = Field(ge=1)
    ticker: str
    render_state: ModelRenderState
    model: FinancialModel
    provenance_map: ArtifactLocation
    semantic_values: tuple[SemanticValueSnapshot, ...]
    validation_status: RecordedValidationStatus
    validation_blocker_count: int = Field(ge=0)

    def enforce_identity(self) -> None:
        canonical_ticker = normalize_ticker(self.ticker)
        if canonical_ticker != self.ticker:
            raise ValueError("ticker must be canonical")
        identity = self.render_state.identity
        if (
            identity.model_id != self.model_id
            or identity.model_version != self.model_version
        ):
            raise ValueError(
                "semantic document identity must match its render state"
            )
        if normalize_ticker(self.model.company.ticker) != self.ticker:
            raise ValueError(
                "semantic document ticker must match its compiled model"
            )
        identities = [
            (cell.semantic_item_id, cell.period)
            for cell in self.semantic_values
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("semantic value item/period identities must be unique")
        self._enforce_validation_counts()

    def _enforce_validation_counts(self) -> None:
        if self.validation_status not in ("pass", "blocked"):
            raise ValueError("this semantic document version records pass or blocked")
        if self.validation_status == "pass" and self.validation_blocker_count:
            raise ValueError("passing semantic document cannot report blockers")
        if self.validation_status == "blocked" and not self.validation_blocker_count:
            raise ValueError("blocked semantic document requires blockers")

    @model_validator(mode="after")
    def _validate_identity(self) -> Self:
        self.enforce_identity()
        return self


class ModelSemanticDocumentV3_0(_ModelSemanticDocumentCore):
    """Direct compiler semantics persisted beside an immutable workbook."""

    schema_version: Literal["3.0"] = "3.0"
    provenance: ProvenanceMapV1_0


class ModelSemanticDocumentV3_1(_ModelSemanticDocumentCore):
    schema_version: Literal["3.1"] = "3.1"
    provenance: ProvenanceMapV1_0
    valuation_evidence_status: Annotated[
        ValuationEvidenceStatus | ValuationEvidenceStatusNotRecorded,
        Field(discriminator="kind"),
    ]


class ModelSemanticDocument(_ModelSemanticDocumentCore):
    """Canonical semantic document.

    3.3 records the build verdict: ``validation_status`` is the
    ValidationReport's ``derive_build_verdict`` status, with the error-finding
    count and the uncleared-warning count. The document holds counts, not
    findings, so it checks only what the counts fix: an error is ``blocked``,
    no error and no uncleared warning is ``ready``, and an uncleared warning
    with no error is ``ready_with_warnings`` or — when its rule refuses
    publish uncleared — ``blocked``. A stored 3.2 document keeps the
    ``pass``/``blocked`` literal it recorded and has no warning count; it is
    never upgraded to a verdict it did not record.
    """

    schema_version: Literal["3.2", "3.3"] = "3.3"
    provenance: ProvenanceMap
    valuation_evidence_status: Annotated[
        ValuationEvidenceStatus | ValuationEvidenceStatusNotRecorded,
        Field(discriminator="kind"),
    ]
    schedule_input_audit: Annotated[
        ScheduleInputAudit | ScheduleInputAuditNotRecorded,
        Field(discriminator="kind"),
    ]
    validation_warning_count: int | None = Field(default=None, ge=0)
    scenario_controls: dict[str, Any] | None = None

    def _enforce_validation_counts(self) -> None:
        if self.schema_version == "3.2":
            if self.validation_warning_count is not None:
                raise ValueError("semantic document 3.2 records no warning count")
            super()._enforce_validation_counts()
            return
        if self.validation_warning_count is None:
            raise ValueError("semantic document 3.3 requires a warning count")
        admitted: tuple[BuildVerdict, ...] = (
            ("blocked",)
            if self.validation_blocker_count
            else ("blocked", "ready_with_warnings")
            if self.validation_warning_count
            else ("ready",)
        )
        if self.validation_status not in admitted:
            raise ValueError(
                "semantic document validation status must match its "
                f"blocker and warning counts (one of {admitted!r})"
            )


class ModelSemanticDocumentRebuildRequired(ValueError):
    """Stored ModelSemanticDocument bytes cannot be read by this binary's contract."""

    code = "model_semantic_document_rebuild_required"
    fix = (
        "rebuild and republish this model on the current contract generation; "
        "stored ModelVersions are never migrated"
    )



class StoredProvenancePairMismatchError(ValueError):
    """Stored semantic sidecar and standalone ProvenanceMap disagree as a pair."""

    code = "semantic_provenance_malformed"

def decode_model_semantic_document(payload: Any) -> ModelSemanticDocument:
    """Decode V3.3, keep V3.2 as recorded, deterministically upgrade V3.0/V3.1."""

    if isinstance(payload, ModelSemanticDocument):
        return payload
    if isinstance(payload, (str, bytes, bytearray)):
        payload = json.loads(payload)
    if isinstance(payload, BaseModel):
        payload = payload.model_dump(mode="json")
    if not isinstance(payload, Mapping):
        raise TypeError("semantic document payload must be a mapping or JSON")
    body = dict(payload)
    schema_version = body.get("schema_version", "3.0")
    if type(schema_version) is not str or schema_version not in (
        "3.0",
        "3.1",
        "3.2",
        "3.3",
    ):
        raise ModelSemanticDocumentRebuildRequired(
            f"unsupported ModelSemanticDocument schema_version: {schema_version!r}; "
            "the stored ModelSemanticDocument is rebuild-required — "
            f"{ModelSemanticDocumentRebuildRequired.fix}"
        )
    try:
        if schema_version == "3.0":
            legacy = ModelSemanticDocumentV3_0.model_validate(body)
            upgraded = legacy.model_dump(mode="python")
            upgraded["schema_version"] = "3.2"
            upgraded["provenance"] = decode_provenance_map(
                upgraded["provenance"]
            ).model_dump(mode="python")
            upgraded["valuation_evidence_status"] = {
                "kind": "pre_slice6_not_recorded"
            }
            upgraded["schedule_input_audit"] = {
                "kind": "pre_slice7_not_recorded"
            }
            return ModelSemanticDocument.model_validate(upgraded)
        if schema_version == "3.1":
            legacy = ModelSemanticDocumentV3_1.model_validate(body)
            upgraded = legacy.model_dump(mode="python")
            upgraded["schema_version"] = "3.2"
            upgraded["provenance"] = decode_provenance_map(
                upgraded["provenance"]
            ).model_dump(mode="python")
            upgraded["schedule_input_audit"] = {
                "kind": "pre_slice7_not_recorded"
            }
            return ModelSemanticDocument.model_validate(upgraded)
        return ModelSemanticDocument.model_validate(body)
    except ProvenanceMapRebuildRequired as exc:
        raise ModelSemanticDocumentRebuildRequired(
            "stored ModelSemanticDocument carries a provenance section this binary "
            f"cannot decode ({exc}); the stored ModelSemanticDocument is "
            f"rebuild-required — {ModelSemanticDocumentRebuildRequired.fix}"
        ) from exc
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "<root>"
        raise ModelSemanticDocumentRebuildRequired(
            "stored ModelSemanticDocument does not satisfy this binary's contract "
            f"({exc.error_count()} rejections, first at {location}: "
            f"{first['msg']}); the stored ModelSemanticDocument is rebuild-required — "
            f"{ModelSemanticDocumentRebuildRequired.fix}"
        ) from exc


def validate_stored_provenance_pair(
    semantic_payload: Any,
    provenance_payload: Any,
) -> tuple[ModelSemanticDocument, ProvenanceMap]:
    """Fail closed when stored sidecar and standalone provenance diverge."""

    def _mapping(value: Any, *, name: str) -> dict[str, Any]:
        if isinstance(value, (str, bytes, bytearray)):
            value = json.loads(value)
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        if not isinstance(value, Mapping):
            raise TypeError(f"{name} payload must be a mapping or JSON")
        return dict(value)

    semantic_body = _mapping(semantic_payload, name="semantic document")
    provenance_body = _mapping(provenance_payload, name="provenance")
    document = decode_model_semantic_document(semantic_body)
    provenance = decode_provenance_map(provenance_body)
    semantic_version = str(semantic_body.get("schema_version", "3.0"))
    provenance_version = str(provenance_body.get("schema_version", "1.0"))
    required = "1.1" if semantic_version in ("3.2", "3.3") else "1.0"
    if provenance_version != required:
        raise StoredProvenancePairMismatchError(
            "stored provenance version is incompatible with semantic document: "
            f"document {semantic_version!r} requires {required!r}, got "
            f"{provenance_version!r}"
        )
    if provenance.model_dump(mode="json") != document.provenance.model_dump(
        mode="json"
    ):
        raise StoredProvenancePairMismatchError(
            "stored provenance content differs from semantic document provenance"
        )
    return document, provenance


def semantic_value_snapshots(render_plan: Any) -> tuple[SemanticValueSnapshot, ...]:
    """Project the existing rendered-cell tolerance contract for comparison."""

    values = (
        SemanticValueSnapshot(
            sheet=cell.sheet,
            cell=cell.cell,
            semantic_item_id=cell.semantic_item_id,
            semantic_label=cell.semantic_label.strip() or "Model line",
            period=cell.period,
            state=cell.expected_state,
            value=cell.expected_value,
            tolerance=cell.tolerance,
        )
        for cell in render_plan.semantic_cells
        if cell.cell_kind != "presentation"
    )
    return tuple(
        sorted(
            values,
            key=lambda cell: (
                cell.semantic_item_id,
                cell.period,
                cell.sheet,
                cell.cell,
            ),
        )
    )


__all__ = [
    "RecordedValidationStatus",
    "ModelSemanticDocument",
    "ModelSemanticDocumentRebuildRequired",
    "ModelSemanticDocumentV3_0",
    "ModelSemanticDocumentV3_1",
    "ProvenanceMapRebuildRequired",
    "SemanticValueSnapshot",
    "StoredProvenancePairMismatchError",
    "semantic_value_snapshots",
    "decode_model_semantic_document",
    "validate_stored_provenance_pair",
]
