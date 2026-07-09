"""Durable model-handle token primitives."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from schema import serialization
from schema.handle import ModelHandle, _canonical_model_hash, evict_handle, load_handle
from schema.load_core import _file_signature
from schema.modify_persistence import file_sha256


HANDLE_CONTRACT_VERSION = 1
TOKEN_TYPE = "schema_model_handle"


class ModelHandleFileSignature(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mtime_ns: int = Field(ge=0)
    size: int = Field(ge=0)


class ModelHandleToken(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    type: Literal["schema_model_handle"] = TOKEN_TYPE
    version: int = 1
    workbook_path: str
    sidecar_path: str
    historical_cutoff_year: int
    compute_engine_version: str
    handle_contract_version: int
    model_revision: str
    computed_sha256: str
    source_sha256: str
    file_signature: ModelHandleFileSignature
    issued_by: str
    issued_at: str


class ModelHandleTokenError(ValueError):
    """Structured error raised when a model-handle token cannot be used."""

    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        next_actions: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.details = dict(details or {})
        self.next_actions = list(next_actions or _default_next_actions(error_code))

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": "error",
            "error": str(self),
            "error_code": self.error_code,
            "details": self.details,
            "recovery": {"next_actions": self.next_actions},
        }


def canonical_workbook_path(file_path: str | Path) -> str:
    """Return the shared canonical workbook path used by model-handle tokens."""

    return str(Path(file_path).expanduser().resolve(strict=False))


def model_handle_sidecar_path(workbook_path: str | Path) -> str:
    """Return the canonical sidecar path derived from a canonical workbook path."""

    canonical = canonical_workbook_path(workbook_path)
    return canonical_workbook_path(serialization.sidecar_path(canonical))


def issue_model_handle_token(
    file_path: str | Path,
    handle: ModelHandle,
    *,
    historical_cutoff_year: int,
    issued_by: str,
) -> ModelHandleToken:
    """Issue a token for a disk-persisted model handle."""

    workbook_path = _canonicalize_issue_path(file_path)
    source_sha256 = file_sha256(workbook_path)
    if source_sha256 is None:
        raise _error(
            "model_handle_unloadable",
            "Cannot issue model_handle_token because the workbook cannot be read.",
            workbook_path=workbook_path,
        )

    signature = _file_signature(workbook_path)
    if signature is None:
        raise _error(
            "model_handle_unloadable",
            "Cannot issue model_handle_token because the workbook cannot be statted.",
            workbook_path=workbook_path,
        )

    durable_handle = _durable_sidecar_handle(
        workbook_path,
        historical_cutoff_year=historical_cutoff_year,
    )
    if durable_handle is None:
        raise _error(
            "non_durable_model_handle_token",
            "Cannot issue model_handle_token because the model state is not persisted to a reloadable sidecar.",
            workbook_path=workbook_path,
            sidecar_path=model_handle_sidecar_path(workbook_path),
        )
    if (
        durable_handle.revision != handle.revision
        or _computed_sha256(durable_handle.computed) != _computed_sha256(handle.computed)
    ):
        raise _error(
            "non_durable_model_handle_token",
            "Cannot issue model_handle_token because the supplied handle does not match the persisted sidecar state.",
            workbook_path=workbook_path,
            sidecar_path=model_handle_sidecar_path(workbook_path),
            handle_revision=handle.revision,
            durable_revision=durable_handle.revision,
            handle_computed_sha256=_computed_sha256(handle.computed),
            durable_computed_sha256=_computed_sha256(durable_handle.computed),
        )

    return ModelHandleToken(
        workbook_path=workbook_path,
        sidecar_path=model_handle_sidecar_path(workbook_path),
        historical_cutoff_year=historical_cutoff_year,
        compute_engine_version=serialization.COMPUTE_ENGINE_VERSION,
        handle_contract_version=HANDLE_CONTRACT_VERSION,
        model_revision=handle.revision,
        computed_sha256=_computed_sha256(handle.computed),
        source_sha256=source_sha256,
        file_signature=ModelHandleFileSignature(
            mtime_ns=signature[0],
            size=signature[1],
        ),
        issued_by=issued_by,
        issued_at=_utc_now(),
    )


def validate_model_handle_token(
    token: ModelHandleToken | Mapping[str, Any] | None,
    *,
    file_path: str | Path | None = None,
    historical_cutoff_year: int | None = None,
) -> ModelHandle:
    """Validate a durable token and return a freshly reloaded model handle."""

    parsed = _parse_token(token)
    token_workbook_path = _canonicalize_token_path(
        parsed.workbook_path,
        field_name="workbook_path",
    )
    derived_sidecar_path = model_handle_sidecar_path(token_workbook_path)
    token_sidecar_path = _canonicalize_token_path(
        parsed.sidecar_path,
        field_name="sidecar_path",
    )

    if parsed.version != 1:
        raise _error(
            "invalid_model_handle_token",
            "Unsupported model_handle_token version.",
            token_version=parsed.version,
            supported_version=1,
        )
    if token_workbook_path != parsed.workbook_path:
        raise _error(
            "invalid_model_handle_token",
            "model_handle_token workbook_path is not canonical.",
            workbook_path=parsed.workbook_path,
            canonical_workbook_path=token_workbook_path,
        )
    if token_sidecar_path != derived_sidecar_path:
        raise _error(
            "invalid_model_handle_token",
            "model_handle_token sidecar_path does not match the derived workbook sidecar path.",
            sidecar_path=parsed.sidecar_path,
            derived_sidecar_path=derived_sidecar_path,
        )

    if file_path is not None:
        explicit_path = _canonicalize_explicit_path(file_path)
        if explicit_path != token_workbook_path:
            raise _error(
                "model_handle_path_mismatch",
                "model_handle_token workbook_path and explicit file_path disagree.",
                token_workbook_path=token_workbook_path,
                explicit_file_path=explicit_path,
            )

    if (
        historical_cutoff_year is not None
        and historical_cutoff_year != parsed.historical_cutoff_year
    ):
        raise _error(
            "model_handle_cutoff_mismatch",
            "model_handle_token cutoff and explicit cutoff disagree.",
            token_cutoff=parsed.historical_cutoff_year,
            explicit_cutoff=historical_cutoff_year,
        )

    if parsed.compute_engine_version != serialization.COMPUTE_ENGINE_VERSION:
        raise _stale_error(
            "model_handle_token compute engine version is stale.",
            token_compute_engine_version=parsed.compute_engine_version,
            current_compute_engine_version=serialization.COMPUTE_ENGINE_VERSION,
        )
    if parsed.handle_contract_version != HANDLE_CONTRACT_VERSION:
        raise _stale_error(
            "model_handle_token handle contract version is stale.",
            token_handle_contract_version=parsed.handle_contract_version,
            current_handle_contract_version=HANDLE_CONTRACT_VERSION,
        )

    current_sha256 = file_sha256(token_workbook_path)
    if current_sha256 is None:
        raise _error(
            "model_handle_unloadable",
            "model_handle_token workbook_path cannot be read.",
            workbook_path=token_workbook_path,
        )
    if current_sha256 != parsed.source_sha256:
        raise _stale_error(
            "model_handle_token source workbook bytes changed.",
            token_source_sha256=parsed.source_sha256,
            current_source_sha256=current_sha256,
            workbook_path=token_workbook_path,
        )

    durable_handle = _durable_sidecar_handle(
        token_workbook_path,
        historical_cutoff_year=parsed.historical_cutoff_year,
    )
    if durable_handle is None:
        raise _error(
            "model_handle_unloadable",
            "model_handle_token sidecar cannot be reloaded.",
            workbook_path=token_workbook_path,
            sidecar_path=derived_sidecar_path,
        )
    if durable_handle.revision != parsed.model_revision:
        raise _stale_error(
            "model_handle_token semantic model revision changed.",
            token_model_revision=parsed.model_revision,
            durable_model_revision=durable_handle.revision,
            workbook_path=token_workbook_path,
            sidecar_path=derived_sidecar_path,
        )
    durable_computed_sha256 = _computed_sha256(durable_handle.computed)
    if durable_computed_sha256 != parsed.computed_sha256:
        raise _stale_error(
            "model_handle_token computed model state changed.",
            token_computed_sha256=parsed.computed_sha256,
            durable_computed_sha256=durable_computed_sha256,
            workbook_path=token_workbook_path,
        )
    return durable_handle


def _durable_sidecar_handle(
    workbook_path: str,
    *,
    historical_cutoff_year: int,
) -> ModelHandle | None:
    sidecar_hit = serialization.try_load_sidecar(workbook_path)
    if sidecar_hit is None:
        return None
    model, base_results = sidecar_hit
    if base_results is None:
        return None
    durable_revision = f"{_canonical_model_hash(model)}:{serialization.COMPUTE_ENGINE_VERSION}"
    evict_handle(workbook_path, historical_cutoff_year)
    try:
        handle = load_handle(
            workbook_path,
            model=model,
            historical_cutoff_year=historical_cutoff_year,
            persist=False,
        )
    except Exception:
        return None
    if handle.revision != durable_revision:
        return None
    return handle


def _computed_sha256(computed: Mapping[str, Mapping[int, float | None]]) -> str:
    payload = {
        item_id: {str(period): value for period, value in values.items()}
        for item_id, values in computed.items()
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _parse_token(token: ModelHandleToken | Mapping[str, Any] | None) -> ModelHandleToken:
    if token is None:
        raise _error(
            "missing_model_handle_token",
            "model_handle_token is required for this path.",
        )
    if isinstance(token, ModelHandleToken):
        return token
    try:
        return ModelHandleToken.model_validate(token)
    except ValidationError as exc:
        raise _error(
            "invalid_model_handle_token",
            "model_handle_token is malformed.",
            pydantic_errors=exc.errors(include_url=False),
        ) from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonicalize_issue_path(file_path: str | Path) -> str:
    try:
        return canonical_workbook_path(file_path)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise _error(
            "model_handle_unloadable",
            "Cannot canonicalize workbook path for model_handle_token issue.",
            file_path=repr(file_path),
            cause=str(exc),
        ) from exc


def _canonicalize_token_path(file_path: str | Path, *, field_name: str) -> str:
    try:
        return canonical_workbook_path(file_path)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise _error(
            "invalid_model_handle_token",
            f"model_handle_token {field_name} cannot be canonicalized.",
            field=field_name,
            value=repr(file_path),
            cause=str(exc),
        ) from exc


def _canonicalize_explicit_path(file_path: str | Path) -> str:
    try:
        return canonical_workbook_path(file_path)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise _error(
            "model_handle_path_mismatch",
            "Explicit file_path cannot be canonicalized for model_handle_token validation.",
            file_path=repr(file_path),
            cause=str(exc),
        ) from exc


def _error(error_code: str, message: str, **details: Any) -> ModelHandleTokenError:
    return ModelHandleTokenError(error_code, message, details=details)


def _stale_error(message: str, **details: Any) -> ModelHandleTokenError:
    return _error("stale_model_handle_token", message, **details)


def _default_next_actions(error_code: str) -> list[str]:
    if error_code == "missing_model_handle_token":
        return [
            "Call a model read/build tool that returns model_handle_token, then pass that token unchanged to the mutating tool.",
        ]
    if error_code == "invalid_model_handle_token":
        return [
            "Pass the model_handle_token exactly as returned by the prior model read/build tool; do not edit token fields.",
        ]
    if error_code in {
        "stale_model_handle_token",
        "model_handle_path_mismatch",
        "model_handle_cutoff_mismatch",
        "model_handle_unloadable",
    }:
        return [
            "Rerun model_summarize or model_build for the current workbook path to get a fresh model_handle_token.",
            "Retry the mutating tool with the fresh token and the same workbook path.",
        ]
    if error_code == "non_durable_model_handle_token":
        return [
            "Persist the model state to the workbook and sidecar before requesting a fresh model_handle_token.",
        ]
    return ["Retry after obtaining a fresh model_handle_token."]


__all__ = [
    "HANDLE_CONTRACT_VERSION",
    "TOKEN_TYPE",
    "ModelHandleFileSignature",
    "ModelHandleToken",
    "ModelHandleTokenError",
    "canonical_workbook_path",
    "issue_model_handle_token",
    "model_handle_sidecar_path",
    "validate_model_handle_token",
]
