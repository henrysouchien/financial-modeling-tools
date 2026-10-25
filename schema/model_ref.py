from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Annotated, Any, Final, Literal, Protocol

from agent_workflow_contracts.research_file_contract import MAX_RESEARCH_FILE_ID
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ._insights_shared import _FROZEN_CONTRACT
from .ticker_contract import require_canonical_contract_ticker


ModelStatus = Literal["draft", "blocked", "ready_with_warnings", "ready"]
#: The ModelVersion statuses that advance the current pointer; ``blocked``
#: never does. The one predicate owner for every publish/select gate.
PUBLISHABLE_MODEL_STATUSES: Final[frozenset[str]] = frozenset(
    {"ready", "ready_with_warnings"}
)


SqliteInt64Id = Annotated[int, Field(ge=1, le=MAX_RESEARCH_FILE_ID)]


class ModelVersionRef(BaseModel):
    model_id: str = Field(min_length=1)
    version: SqliteInt64Id

    model_config = _FROZEN_CONTRACT


class ModelStamp(BaseModel):
    """The identity a published ``model.xlsx`` carries in its own properties.

    Written by ``schema.excel_writer.render_xlsx_bytes`` as the custom document
    properties ``hank_model_id``, ``hank_model_version`` and ``hank_build_id``
    (``build_id`` is ``render_state.identity.build_id``), and read back by the
    pane from the open workbook. A workbook without them is hand-built.
    """

    model_id: str = Field(min_length=1)
    version: SqliteInt64Id
    build_id: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT



class CurrentModelPointer(BaseModel):
    """The sole mutable selector, advanced through compare-and-swap."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    schema_version: Literal["1.0"] = "1.0"
    user_id: str = Field(min_length=1)
    research_file_id: SqliteInt64Id
    ticker: str
    selected: ModelVersionRef | None = None
    epoch: int = Field(ge=0)
    updated_by: str = Field(min_length=1)
    updated_at: datetime

    @field_validator("ticker")
    @classmethod
    def _require_canonical_ticker(cls, value: str) -> str:
        return require_canonical_contract_ticker(value)


class ModelRecordArtifact(BaseModel):
    """Where a committed model's workbook lives and the digest of its bytes."""

    location: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    model_config = _FROZEN_CONTRACT


class ModelRecord(BaseModel):
    """The repository's record of one committed model version.

    Identity, status, commit time and workbook location only: the fields a
    reader needs without decoding the engine payload stored beside it.
    """

    ref: ModelVersionRef
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    status: ModelStatus
    committed_at: datetime
    workbook: ModelRecordArtifact

    model_config = _FROZEN_CONTRACT


class ModelRecordLineage(BaseModel):
    """The writer-supplied lineage columns of a committed model version.

    Opaque to readers; the repository indexes them for replay and uniqueness.
    """

    request_digest: str = Field(min_length=1)
    origin: Literal["handoff_build", "successor", "legacy"]
    origin_handoff_id: int | None = None
    origin_handoff_version: int | None = None
    origin_successor_door: str | None = None
    origin_proposal_handle: str | None = None
    origin_semantic_base_model_id: str | None = None
    origin_semantic_base_version: int | None = None
    successor_candidate_digest: str | None = None

    model_config = _FROZEN_CONTRACT


class ModelRecordWriter(Protocol):
    """The repository's writer seam for model records and the current pointer.

    ``payload_json`` is the engine's encoding of the version, tagged by
    ``payload_generation``; the record and lineage are the columns the
    repository stores beside it.
    """

    def commit_model_record_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        record: ModelRecord,
        lineage: ModelRecordLineage,
        payload_generation: int,
        payload_json: str,
    ) -> ModelRecord: ...

    def compare_and_swap_current_model_pointer_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        expected_epoch: int,
        pointer: Any,
    ) -> Any: ...


__all__ = [
    "CurrentModelPointer",
    "ModelRecord",
    "ModelRecordArtifact",
    "ModelRecordLineage",
    "ModelRecordWriter",
    "ModelStamp",
    "ModelStatus",
    "PUBLISHABLE_MODEL_STATUSES",
    "ModelVersionRef",
]
