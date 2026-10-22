"""Read-only hydration of one exact immutable ModelVersion artifact set.

The expected ModelVersion is the authority. This module does not consult a
CurrentModelPointer, select an origin, regenerate artifacts, or perform writes.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import errno
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
from typing import Any, Iterator, NoReturn, Protocol

from pydantic import ValidationError

from model_build.stored_reads import PRE_CUT_REBUILD_REPAIR
from schema.business_model_revision import BusinessModelRevision, BusinessModelRevisionAbsent
from schema.business_model_revision_ref import BusinessModelRevisionRef
from schema.build_model_product_contracts import (
  ArtifactLocation,
  ModelSpec,
  ModelVersion,
  ProvenanceMap,
  SourceBundle,
  SourceBundleBindingError,
  SourceBundleRebuildRequired,
  ValidationReport,
  ValidationReportRebuildRequired,
  decode_persisted_source_bundle_payload,
  decode_validation_report_payload,
  resolve_model_spec_business_model_revision,
)
from schema.valuation_evidence_validation import (
  validate_source_bundle_binding_and_selection,
)
from schema.model_ref import ModelVersionRef
from schema.model_semantic_document import (
  ModelSemanticDocument,
  validate_stored_provenance_pair,
)
from schema.model_spec_decoder import (
  ModelSpecRebuildRequired,
  VerifiedModelSpecDecodeRequest,
  decode_verified_model_spec_compiler_input,
)
from schema.render_state import compiler_input_content_hash



EXACT_MODEL_VERSION_ARTIFACT_INTEGRITY_ERROR = (
  "model_version_artifact_integrity_failed"
)
EXACT_MODEL_VERSION_ARTIFACT_REBUILD_ERROR = (
  "model_version_artifact_rebuild_required"
)
EXACT_MODEL_VERSION_ARTIFACT_IO_RETRYABLE = "model_version_artifact_io_retryable"


class ExactModelVersionArtifactRepository(Protocol):
  @property
  def db_path(self) -> Path: ...

  def _conn(self) -> AbstractContextManager[sqlite3.Connection]: ...

  def get_model_version_in_snapshot(
    self,
    conn: sqlite3.Connection,
    *,
    user_id: object,
    research_file_id: int,
    ticker: object,
    model_ref: ModelVersionRef,
  ) -> ModelVersion | None: ...



@dataclass(frozen=True, slots=True)
class ArtifactSourceBundle:
  """Authoritative lineage prepared by the caller's source-bundle owner."""

  bundle_id: str
  content_sha256: str
  identity_digest: str
  business_model_revision_ref: BusinessModelRevisionRef
  payload: dict[str, Any]


class LoadArtifactSourceBundle(Protocol):
  def __call__(
    self,
    conn: sqlite3.Connection,
    *,
    user_id: object,
    research_file_id: int,
    ticker: object,
    bundle_id: str,
  ) -> ArtifactSourceBundle: ...


class LoadArtifactBusinessModelRevision(Protocol):
  def __call__(
    self,
    conn: sqlite3.Connection,
    *,
    bundle_id: str,
  ) -> BusinessModelRevision | BusinessModelRevisionAbsent: ...


@dataclass(frozen=True, slots=True)
class ArtifactReadDependencies:
  """Typed acquisition and exception ports supplied by the storage owner."""

  load_source_bundle: LoadArtifactSourceBundle
  load_business_model_revision: LoadArtifactBusinessModelRevision
  model_version_error: type[Exception]
  source_bundle_error: type[Exception]


class ExactModelVersionArtifactHydrationError(RuntimeError):
  """Typed permanent-corruption, rebuild-required, or retryable failure."""

  def __init__(
    self,
    code: str,
    message: str,
    *,
    reason: str,
    retryable: bool,
    next_call: dict[str, Any] | None = None,
    fix: str | None = None,
  ) -> None:
    super().__init__(message)
    self.code = code
    self.reason = reason
    self.retryable = retryable
    self.next_call = next_call
    # The operator move that clears this refusal, when there is exactly one.
    # Rebuild-class refusals carry PRE_CUT_REBUILD_REPAIR here so every door
    # surfaces the repair from the error instead of naming it at the seam.
    self.fix = fix


@dataclass(frozen=True, slots=True)
class ExactSourceBundleLineage:
  bundle_id: str
  identity_digest: str
  content_sha256: str


@dataclass(frozen=True, slots=True)
class ExactModelVersionArtifacts:
  """Verified bytes and decoded contracts for one exact ModelVersion."""

  model_version: ModelVersion
  workbook_path: Path
  semantic_sidecar_path: Path
  provenance_map_path: Path
  validation_report_path: Path
  workbook_bytes: bytes
  semantic_document: ModelSemanticDocument
  provenance: ProvenanceMap
  validation_report: ValidationReport
  source_bundle: SourceBundle
  model_spec: ModelSpec
  source_bundle_lineage: ExactSourceBundleLineage


@dataclass(frozen=True, slots=True)
class VerifiedModelVersionArtifacts:
  """Path-free verified bytes and contracts for an immutable ModelVersion."""

  model_version: ModelVersion
  workbook_bytes: bytes
  semantic_document: ModelSemanticDocument
  provenance: ProvenanceMap
  validation_report: ValidationReport
  source_bundle: SourceBundle
  model_spec: ModelSpec
  source_bundle_lineage: ExactSourceBundleLineage


@dataclass(frozen=True, slots=True)
class _VerifiedArtifact:
  path: Path
  content: bytes


# Decoded artifacts of published versions, shared by every reader in the
# process: a published version is immutable and readers never mutate it.
# Bounds (C1 ruling, refined): two cached versions per (user, research file,
# ticker), two across files, and a per-version ceiling of 150 MiB of on-disk
# artifact bytes. A version over the ceiling is read directly from disk on
# every call, never inserted and never an error. The ceiling is sized so the
# largest issuer versions measured 2026-10-19 cache: ISRG's version is
# 116,209,851 B and ETN's 136,081,713 B on disk (a decoded ETN version holds
# about 0.9 GB of RSS, about 7 times its disk bytes). Every cached version is
# at most 150 MiB on disk, so memory is bounded by version count times that
# ceiling's decoded size.
# Eviction order on insert: the same file's older versions beyond two, then
# the least recently used version while the total count is at its bound.
_ARTIFACT_CACHE_MAX_VERSIONS_PER_FILE = 2
_ARTIFACT_CACHE_MAX_TOTAL_VERSIONS = 2
_ARTIFACT_CACHE_MAX_VERSION_DISK_BYTES = 150 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class _DecodedArtifacts:
  file_scope: tuple[str, int, str]
  nbytes: int
  workbook_bytes: bytes
  semantic_document: ModelSemanticDocument
  provenance: ProvenanceMap
  validation_report: ValidationReport
  source_bundle: SourceBundle
  model_spec: ModelSpec


class _DecodedArtifactCache:
  """LRU of decoded artifact sets under the bounds above, under one lock."""

  def __init__(self) -> None:
    self._lock = threading.Lock()
    self._entries: OrderedDict[str, _DecodedArtifacts] = OrderedDict()

  def get(self, key: str) -> _DecodedArtifacts | None:
    with self._lock:
      entry = self._entries.get(key)
      if entry is not None:
        self._entries.move_to_end(key)
      return entry

  def put(self, key: str, entry: _DecodedArtifacts) -> None:
    with self._lock:
      if key in self._entries:
        self._entries.move_to_end(key)
        return
      if entry.nbytes > _ARTIFACT_CACHE_MAX_VERSION_DISK_BYTES:
        return
      same_file = [
        other for other, held in self._entries.items()
        if held.file_scope == entry.file_scope
      ]
      while len(same_file) >= _ARTIFACT_CACHE_MAX_VERSIONS_PER_FILE:
        del self._entries[same_file.pop(0)]
      while len(self._entries) >= _ARTIFACT_CACHE_MAX_TOTAL_VERSIONS:
        del self._entries[next(iter(self._entries))]
      self._entries[key] = entry

  def discard(self, key: str) -> None:
    with self._lock:
      self._entries.pop(key, None)

  def clear(self) -> None:
    with self._lock:
      self._entries.clear()


_ARTIFACT_CACHE = _DecodedArtifactCache()


_DIRECTORY_READ_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
_RETRYABLE_ERRNOS = frozenset(
  value
  for value in (
    getattr(errno, "EAGAIN", None),
    getattr(errno, "EBUSY", None),
    getattr(errno, "EINTR", None),
    getattr(errno, "EMFILE", None),
    getattr(errno, "ENFILE", None),
    getattr(errno, "EIO", None),
    getattr(errno, "ENOMEM", None),
    getattr(errno, "ESTALE", None),
    getattr(errno, "ETIMEDOUT", None),
  )
  if value is not None
)
_RETRYABLE_SQLITE_BASE_CODES = frozenset(
  value
  for value in (
    getattr(sqlite3, "SQLITE_BUSY", None),
    getattr(sqlite3, "SQLITE_LOCKED", None),
    getattr(sqlite3, "SQLITE_IOERR", None),
    getattr(sqlite3, "SQLITE_FULL", None),
    getattr(sqlite3, "SQLITE_CANTOPEN", None),
    getattr(sqlite3, "SQLITE_PROTOCOL", None),
    getattr(sqlite3, "SQLITE_INTERRUPT", None),
  )
  if value is not None
)


def _raise_hydration_error(
  code: str,
  reason: str,
  message: str,
  *,
  retryable: bool,
  cause: BaseException | None = None,
  fix: str | None = None,
) -> NoReturn:
  error = ExactModelVersionArtifactHydrationError(
    code,
    message,
    reason=reason,
    retryable=retryable,
    fix=fix,
  )
  if cause is None:
    raise error
  raise error from cause


def _integrity(
  reason: str,
  message: str,
  cause: BaseException | None = None,
) -> NoReturn:
  _raise_hydration_error(
    EXACT_MODEL_VERSION_ARTIFACT_INTEGRITY_ERROR,
    reason,
    message,
    retryable=False,
    cause=cause,
  )


def _rebuild(
  reason: str,
  message: str,
  cause: BaseException | None = None,
  *,
  fix: str,
) -> NoReturn:
  _raise_hydration_error(
    EXACT_MODEL_VERSION_ARTIFACT_REBUILD_ERROR,
    reason,
    message,
    retryable=False,
    cause=cause,
    fix=fix,
  )


def _retryable(
  reason: str,
  message: str,
  cause: BaseException,
) -> NoReturn:
  _raise_hydration_error(
    EXACT_MODEL_VERSION_ARTIFACT_IO_RETRYABLE,
    reason,
    message,
    retryable=True,
    cause=cause,
  )


def _is_retryable_os_error(exc: OSError) -> bool:
  return exc.errno in _RETRYABLE_ERRNOS


def _is_retryable_sqlite_error(exc: sqlite3.Error) -> bool:
  error_code = getattr(exc, "sqlite_errorcode", None)
  if type(error_code) is int:
    if error_code & 0xFF in _RETRYABLE_SQLITE_BASE_CODES:
      return True
  message = str(exc).lower()
  return (
    "database is locked" in message
    or "database table is locked" in message
    or "database schema is locked" in message
    or "database is busy" in message
    or "database is full" in message
    or "database or disk is full" in message
    or "disk i/o error" in message
    or "locking protocol" in message
    or "operation interrupted" in message
    or "unable to open database file" in message
  )


def _raise_os_error(*, role: str, exc: OSError) -> NoReturn:
  if _is_retryable_os_error(exc):
    _retryable(
      f"{role}_artifact_io_retryable",
      f"{role} artifact hydration encountered a retryable I/O failure",
      exc,
    )
  _integrity(
    f"{role}_artifact_unavailable",
    f"{role} artifact is absent, unsafe, or unreadable",
    exc,
  )


def _artifact_components(role: str, location: str) -> tuple[str, ...]:
  try:
    candidate = Path(location)
  except (TypeError, ValueError) as exc:
    _integrity(
      f"{role}_artifact_path_invalid",
      f"{role} artifact path is malformed",
      exc,
    )
  if (
    type(location) is not str
    or not location
    or location != location.strip()
    or candidate.is_absolute()
    or "\\" in location
    or not candidate.parts
    or any(part in {"", ".", ".."} for part in candidate.parts)
  ):
    _integrity(
      f"{role}_artifact_path_invalid",
      f"{role} artifact path escapes or aliases the repository root",
    )
  return tuple(candidate.parts)


def _stream_regular_file_at(
  parent_fd: int,
  name: str,
  *,
  role: str,
  consume: Callable[[bytes], object],
) -> None:
  try:
    file_fd = os.open(name, _FILE_READ_FLAGS, dir_fd=parent_fd)
  except OSError as exc:
    _raise_os_error(role=role, exc=exc)
  try:
    before = os.fstat(file_fd)
    if not stat.S_ISREG(before.st_mode) or int(before.st_nlink) != 1:
      _integrity(
        f"{role}_artifact_not_regular",
        f"{role} artifact is not a singly linked regular file",
      )
    # No size cap here: these are the system's own published bytes. The read is
    # bounded by the descriptor's size, held stable across the read, and the
    # caller verifies the bytes against the sha256 the publisher recorded.
    expected_size = int(before.st_size)
    total = 0
    remaining = expected_size + 1
    while remaining and (
      chunk := os.read(file_fd, min(1024 * 1024, remaining))
    ):
      consume(chunk)
      total += len(chunk)
      remaining -= len(chunk)
    after = os.fstat(file_fd)
    before_identity = (
      before.st_dev,
      before.st_ino,
      before.st_mode,
      before.st_nlink,
      before.st_size,
      before.st_mtime_ns,
      before.st_ctime_ns,
    )
    after_identity = (
      after.st_dev,
      after.st_ino,
      after.st_mode,
      after.st_nlink,
      after.st_size,
      after.st_mtime_ns,
      after.st_ctime_ns,
    )
    if before_identity != after_identity or total != expected_size:
      _integrity(
        f"{role}_artifact_changed_during_read",
        f"{role} artifact changed while it was read",
      )
  except OSError as exc:
    _raise_os_error(role=role, exc=exc)
  finally:
    os.close(file_fd)


def _read_regular_file_at(parent_fd: int, name: str, *, role: str) -> bytes:
  chunks: list[bytes] = []
  _stream_regular_file_at(parent_fd, name, role=role, consume=chunks.append)
  return b"".join(chunks)


def _stream_artifact(
  *,
  root_fd: int,
  role: str,
  artifact: ArtifactLocation,
  consume: Callable[[bytes], object],
) -> tuple[str, ...]:
  components = _artifact_components(role, artifact.location)
  try:
    current_fd = os.dup(root_fd)
  except OSError as exc:
    _raise_os_error(role=role, exc=exc)
  try:
    for component in components[:-1]:
      try:
        child_fd = os.open(
          component,
          _DIRECTORY_READ_FLAGS,
          dir_fd=current_fd,
        )
      except OSError as exc:
        if _is_retryable_os_error(exc):
          _raise_os_error(role=role, exc=exc)
        reason = (
          f"{role}_artifact_symlink_component"
          if exc.errno in {errno.ELOOP, errno.ENOTDIR}
          else f"{role}_artifact_unavailable"
        )
        _integrity(
          reason,
          f"{role} artifact has an unavailable or symlinked path component",
          exc,
        )
      os.close(current_fd)
      current_fd = child_fd
    _stream_regular_file_at(
      current_fd, components[-1], role=role, consume=consume,
    )
  finally:
    os.close(current_fd)
  return components


def _require_recorded_digest(
  role: str, actual_sha256: str, artifact: ArtifactLocation,
) -> None:
  if not hmac.compare_digest(actual_sha256, artifact.sha256):
    _integrity(
      f"{role}_artifact_digest_mismatch",
      f"{role} artifact differs from its immutable ModelVersion digest",
    )


def _read_artifact(
  *,
  root_fd: int,
  root_path: Path,
  role: str,
  artifact: ArtifactLocation,
) -> _VerifiedArtifact:
  chunks: list[bytes] = []
  components = _stream_artifact(
    root_fd=root_fd, role=role, artifact=artifact, consume=chunks.append,
  )
  content = b"".join(chunks)
  _require_recorded_digest(role, hashlib.sha256(content).hexdigest(), artifact)
  return _VerifiedArtifact(
    path=root_path.joinpath(*components),
    content=content,
  )


def _repository_root(
  repository: ExactModelVersionArtifactRepository,
) -> tuple[Path, int]:
  db_path = getattr(repository, "db_path", None)
  if not isinstance(db_path, Path):
    _integrity(
      "repository_scope_invalid",
      "repository does not expose a server-owned database path",
    )
  try:
    root = db_path.parent.resolve(strict=True)
    root_fd = os.open(root, _DIRECTORY_READ_FLAGS)
    if not stat.S_ISDIR(os.fstat(root_fd).st_mode):
      raise OSError(errno.ENOTDIR, "repository root is not a directory")
  except OSError as exc:
    if _is_retryable_os_error(exc):
      _retryable(
        "repository_artifact_root_io_retryable",
        "repository artifact root encountered a retryable I/O failure",
        exc,
      )
    _integrity(
      "repository_scope_invalid",
      "repository artifact root is unavailable",
      exc,
    )
  except (RuntimeError, ValueError) as exc:
    _integrity(
      "repository_scope_invalid",
      "repository artifact root is unavailable",
      exc,
    )
  return root, root_fd


def _decode_json_object(content: bytes, *, role: str) -> dict[str, object]:
  def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
      if key in result:
        raise ValueError(f"duplicate JSON key: {key}")
      result[key] = value
    return result

  try:
    value = json.loads(
      content.decode("utf-8"),
      object_pairs_hook=no_duplicates,
      parse_constant=lambda token: (_ for _ in ()).throw(
        ValueError(f"non-finite JSON constant: {token}")
      ),
    )
  except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
    _integrity(
      f"{role}_artifact_malformed",
      f"{role} artifact is not strict UTF-8 JSON",
      exc,
    )
  if not isinstance(value, dict):
    _integrity(
      f"{role}_artifact_malformed",
      f"{role} artifact must contain one JSON object",
    )
  return value


def _read_all_artifacts(
  repository: ExactModelVersionArtifactRepository,
  model_version: ModelVersion,
) -> dict[str, _VerifiedArtifact]:
  root_path, root_fd = _repository_root(repository)
  try:
    return {
      role: _read_artifact(
        root_fd=root_fd,
        root_path=root_path,
        role=role,
        artifact=artifact,
      )
      for role, artifact in (
        ("workbook", model_version.workbook),
        ("semantic_sidecar", model_version.semantic_sidecar),
        ("provenance_map", model_version.provenance_map),
        ("validation_report", model_version.validation_report),
      )
    }
  finally:
    os.close(root_fd)


def _verify_artifact_digests(
  repository: ExactModelVersionArtifactRepository,
  model_version: ModelVersion,
) -> None:
  """Stream-hash every artifact file against the row's sha256; keep no bytes."""

  root_path, root_fd = _repository_root(repository)
  del root_path
  try:
    for role, artifact in (
      ("workbook", model_version.workbook),
      ("semantic_sidecar", model_version.semantic_sidecar),
      ("provenance_map", model_version.provenance_map),
      ("validation_report", model_version.validation_report),
    ):
      digest = hashlib.sha256()
      _stream_artifact(
        root_fd=root_fd, role=role, artifact=artifact, consume=digest.update,
      )
      _require_recorded_digest(role, digest.hexdigest(), artifact)
  finally:
    os.close(root_fd)


def _decode_artifacts(
  *,
  model_version: ModelVersion,
  artifacts: dict[str, _VerifiedArtifact],
) -> tuple[
  ModelSemanticDocument,
  ProvenanceMap,
  ValidationReport,
  SourceBundle,
  ModelSpec,
]:
  semantic_payload = _decode_json_object(
    artifacts["semantic_sidecar"].content,
    role="semantic_sidecar",
  )
  provenance_payload = _decode_json_object(
    artifacts["provenance_map"].content,
    role="provenance_map",
  )
  validation_payload = _decode_json_object(
    artifacts["validation_report"].content,
    role="validation_report",
  )
  try:
    # A stored 3.2 sidecar travels with a 2.1 report, refused below as
    # rebuild-required rather than as malformed.
    if semantic_payload.get("schema_version") not in {"3.2", "3.3"}:
      raise ValueError("semantic sidecar must use schema version 3.2 or 3.3")
    if provenance_payload.get("schema_version") != "1.1":
      raise ValueError("provenance map must use exact schema version 1.1")
    semantic_document, provenance = validate_stored_provenance_pair(
      semantic_payload,
      provenance_payload,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "semantic_provenance_malformed",
      "stored semantic sidecar and provenance do not form an exact current pair",
      exc,
    )
  try:
    validation_report = decode_validation_report_payload(validation_payload)
  except ValidationReportRebuildRequired as exc:
    _rebuild(
      "validation_report_rebuild_required",
      "stored ValidationReport requires a current rebuild",
      exc,
      fix=PRE_CUT_REBUILD_REPAIR,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "validation_report_malformed",
      "stored ValidationReport is malformed",
      exc,
    )

  if (
    semantic_document.model_id != model_version.model_id
    or semantic_document.model_version != model_version.version
    or semantic_document.ticker != model_version.ticker
    or semantic_document.provenance_map != model_version.provenance_map
    or provenance.source_bundle_digest != model_version.source_bundle_digest
    or provenance.model_spec_digest != model_version.model_spec_digest
    or semantic_document.validation_status != validation_report.status
    or semantic_document.validation_blocker_count != validation_report.error_count
    or semantic_document.validation_warning_count
    != validation_report.uncleared_warning_count
    or model_version.status != validation_report.status
    or validation_report.validated_at != model_version.committed_at
  ):
    _integrity(
      "artifact_lineage_mismatch",
      "stored artifacts disagree with the immutable ModelVersion identity",
    )

  source_input = semantic_document.render_state.compiler_inputs.source_bundle
  model_spec_input = semantic_document.render_state.compiler_inputs.model_spec
  if (
    source_input.ticker != model_version.ticker
    or model_spec_input.ticker != model_version.ticker
    or source_input.bundle_id is None
    or source_input.identity_digest is None
    or model_spec_input.bundle_id is not None
    or model_spec_input.identity_digest is not None
  ):
    _integrity(
      "compiler_input_lineage_mismatch",
      "stored compiler inputs have malformed SourceBundle lineage",
    )
  try:
    source_bundle = decode_persisted_source_bundle_payload(
      source_input.payload,
      expected_wire_digest=source_input.content_hash,
    )
  except SourceBundleRebuildRequired as exc:
    _rebuild(
      "source_bundle_rebuild_required",
      "stored SourceBundle cannot be read by this binary's contract",
      exc,
      fix=PRE_CUT_REBUILD_REPAIR,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "source_bundle_payload_malformed",
      "stored SourceBundle compiler input is malformed",
      exc,
    )
  try:
    verified_model_spec = decode_verified_model_spec_compiler_input(
      VerifiedModelSpecDecodeRequest(
        payload_mapping=model_spec_input.payload,
        compiler_input_hash=model_spec_input.content_hash,
        external_model_version_digest=model_version.model_spec_digest,
      )
    )
  except ModelSpecRebuildRequired as exc:
    _rebuild(
      exc.code,
      "stored ModelSpec cannot be read by this binary's contract",
      exc,
      fix=PRE_CUT_REBUILD_REPAIR,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "model_spec_payload_malformed",
      "stored ModelSpec compiler input is malformed",
      exc,
    )
  if verified_model_spec.verification_mode != "persisted_model_version":
    _integrity(
      "model_spec_verification_mode_invalid",
      "stored ModelSpec was not verified against its ModelVersion digest",
    )
  if (
    compiler_input_content_hash(source_input.payload) != source_input.content_hash
    or source_input.content_hash.removeprefix("sha256:")
    != model_version.source_bundle_digest
  ):
    _integrity(
      "source_bundle_compiler_digest_mismatch",
      "stored SourceBundle compiler input disagrees with ModelVersion digest",
    )
  model_spec = verified_model_spec.model_spec
  if (
    model_spec.predecessor != model_version.model_spec_predecessor
    or model_spec.idempotency_key != model_version.model_spec_idempotency_key
  ):
    _integrity(
      "model_spec_lineage_mismatch",
      "stored ModelSpec lineage disagrees with the immutable ModelVersion",
    )
  return (
    semantic_document,
    provenance,
    validation_report,
    source_bundle,
    model_spec,
  )


def _verify_source_bundle_lineage(
  repository: ExactModelVersionArtifactRepository,
  conn: sqlite3.Connection,
  *,
  model_version: ModelVersion,
  expected_bundle_id: str,
  semantic_document: ModelSemanticDocument,
  embedded_source_bundle: SourceBundle,
  model_spec: ModelSpec,
  dependencies: ArtifactReadDependencies,
) -> ExactSourceBundleLineage:

  source_input = semantic_document.render_state.compiler_inputs.source_bundle
  assert source_input.bundle_id is not None
  assert source_input.identity_digest is not None
  if source_input.bundle_id != expected_bundle_id:
    _integrity(
      "source_bundle_id_mismatch",
      "embedded SourceBundle id differs from the expected exact bundle",
    )
  try:
    loaded = dependencies.load_source_bundle(
      conn,
      user_id=model_version.user_id,
      research_file_id=model_version.research_file_id,
      ticker=model_version.ticker,
      bundle_id=expected_bundle_id,
    )
  except dependencies.source_bundle_error as exc:
    code = str(getattr(exc, "code"))
    if code == "source_bundle_rebuild_required":
      _rebuild(
        "source_bundle_rebuild_required",
        "stored SourceBundle row cannot be read by this binary's contract",
        exc,
        fix=PRE_CUT_REBUILD_REPAIR,
      )
    _integrity(
      code,
      "stored SourceBundle row cannot satisfy exact-version lineage",
      exc,
    )
  if loaded.bundle_id != expected_bundle_id:
    _integrity(
      "source_bundle_id_mismatch",
      "authoritative SourceBundle row differs from the expected exact bundle",
    )
  actual_identity_digest = loaded.identity_digest
  try:
    loaded_source_bundle = decode_persisted_source_bundle_payload(
      loaded.payload,
      expected_wire_digest=f"sha256:{loaded.content_sha256}",
    )
  except SourceBundleRebuildRequired as exc:
    _rebuild(
      "source_bundle_rebuild_required",
      "stored SourceBundle row cannot be read by this binary's contract",
      exc,
      fix=PRE_CUT_REBUILD_REPAIR,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "source_bundle_row_malformed",
      "stored SourceBundle row payload is malformed",
      exc,
    )
  if (
    loaded.payload != source_input.payload
    or loaded_source_bundle != embedded_source_bundle
    or loaded.content_sha256 != model_version.source_bundle_digest
    or loaded.content_sha256
    != source_input.content_hash.removeprefix("sha256:")
    or actual_identity_digest != source_input.identity_digest
    or model_spec.business_model_revision_ref != loaded.business_model_revision_ref
  ):
    _integrity(
      "source_bundle_row_lineage_mismatch",
      "stored SourceBundle row differs from embedded compiler lineage",
    )
  try:
    revision = dependencies.load_business_model_revision(
      conn,
      bundle_id=loaded.bundle_id,
    )
  except dependencies.source_bundle_error as exc:
    _integrity(
      str(getattr(exc, "code")),
      "exact SourceBundle BusinessModel revision cannot be hydrated",
      exc,
    )
  if isinstance(revision, BusinessModelRevisionAbsent):
    raise ExactModelVersionArtifactHydrationError(
      "business_model_revision_absent",
      revision.message,
      reason="absent",
      retryable=False,
      next_call=revision.next_call,
    )
  try:
    resolve_model_spec_business_model_revision(model_spec, revision)
  except dependencies.source_bundle_error as exc:
    _integrity(
      str(getattr(exc, "code")),
      "exact SourceBundle BusinessModel revision cannot be hydrated",
      exc,
    )
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "business_model_revision_lineage_mismatch",
      "ModelSpec does not pin the exact SourceBundle BusinessModel revision",
      exc,
    )
  try:
    validate_source_bundle_binding_and_selection(
      loaded_source_bundle,
      model_spec,
      persisted_source_bundle_payload=loaded.payload,
      persisted_source_bundle_wire_digest=source_input.content_hash,
    )
  except SourceBundleBindingError as exc:
    _integrity(
      "source_bundle_model_spec_binding_mismatch",
      "stored ModelSpec does not bind the exact SourceBundle row",
      exc,
    )
  return ExactSourceBundleLineage(
    bundle_id=loaded.bundle_id,
    identity_digest=actual_identity_digest,
    content_sha256=loaded.content_sha256,
  )


def _validate_request(
  *,
  scope: tuple[str, int, str],
  model_ref: ModelVersionRef,
  expected_model_version: ModelVersion,
  expected_bundle_id: str,
) -> tuple[tuple[str, int, str], ModelVersionRef, ModelVersion, str]:
  try:
    validated_model_version = ModelVersion.model_validate(expected_model_version)
    validated_ref = ModelVersionRef.model_validate(model_ref)
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "expected_model_version_invalid",
      "expected ModelVersion identity is malformed",
      exc,
    )
  if (
    not isinstance(scope, tuple)
    or len(scope) != 3
    or type(scope[0]) is not str
    or type(scope[1]) is not int
    or type(scope[2]) is not str
  ):
    _integrity(
      "model_version_scope_invalid",
      "exact-version snapshot scope is malformed",
    )
  validated_scope = (scope[0], scope[1], scope[2])
  if validated_scope != (
    validated_model_version.user_id,
    validated_model_version.research_file_id,
    validated_model_version.ticker,
  ):
    _integrity(
      "model_version_scope_mismatch",
      "expected ModelVersion differs from the requested repository scope",
    )
  if validated_ref != ModelVersionRef(
    model_id=validated_model_version.model_id,
    version=validated_model_version.version,
  ):
    _integrity(
      "model_version_ref_mismatch",
      "expected ModelVersion differs from the requested exact ref",
    )
  if (
    type(expected_bundle_id) is not str
    or not expected_bundle_id
    or expected_bundle_id != expected_bundle_id.strip()
  ):
    _integrity(
      "source_bundle_id_invalid",
      "expected SourceBundle id must be canonical non-empty text",
    )
  if validated_model_version.status == "draft":
    _integrity(
      "model_version_status_invalid",
      "exact artifact hydration requires a committed version",
    )
  return (
    validated_scope,
    validated_ref,
    validated_model_version,
    expected_bundle_id,
  )


def _hydrate_exact_model_version_artifacts_in_snapshot(
  repository: ExactModelVersionArtifactRepository,
  conn: sqlite3.Connection,
  *,
  scope: tuple[str, int, str],
  model_ref: ModelVersionRef,
  expected_model_version: ModelVersion,
  expected_bundle_id: str,
  dependencies: ArtifactReadDependencies,
) -> ExactModelVersionArtifacts:
  if not isinstance(conn, sqlite3.Connection) or not conn.in_transaction:
    _integrity(
      "active_snapshot_required",
      "exact artifact hydration requires an active read transaction",
    )
  (
    validated_scope,
    validated_ref,
    validated_expected,
    validated_bundle_id,
  ) = _validate_request(
    scope=scope,
    model_ref=model_ref,
    expected_model_version=expected_model_version,
    expected_bundle_id=expected_bundle_id,
  )
  canonical = repository.get_model_version_in_snapshot(
    conn,
    user_id=validated_scope[0],
    research_file_id=validated_scope[1],
    ticker=validated_scope[2],
    model_ref=validated_ref,
  )
  if canonical is None:
    _integrity(
      "model_version_absent",
      "expected immutable ModelVersion row is absent",
    )
  if canonical != validated_expected:
    _integrity(
      "model_version_payload_mismatch",
      "stored ModelVersion differs from the complete expected payload",
    )
  artifacts = _read_all_artifacts(repository, canonical)
  (
    semantic_document,
    provenance,
    validation_report,
    source_bundle,
    model_spec,
  ) = _decode_artifacts(model_version=canonical, artifacts=artifacts)
  source_bundle_lineage = _verify_source_bundle_lineage(
    repository,
    conn,
    model_version=canonical,
    expected_bundle_id=validated_bundle_id,
    semantic_document=semantic_document,
    embedded_source_bundle=source_bundle,
    model_spec=model_spec,
    dependencies=dependencies,
  )
  return ExactModelVersionArtifacts(
    model_version=canonical,
    workbook_path=artifacts["workbook"].path,
    semantic_sidecar_path=artifacts["semantic_sidecar"].path,
    provenance_map_path=artifacts["provenance_map"].path,
    validation_report_path=artifacts["validation_report"].path,
    workbook_bytes=artifacts["workbook"].content,
    semantic_document=semantic_document,
    provenance=provenance,
    validation_report=validation_report,
    source_bundle=source_bundle,
    model_spec=model_spec,
    source_bundle_lineage=source_bundle_lineage,
  )


@contextmanager
def _exact_model_version_artifact_error_adapter(
  dependencies: ArtifactReadDependencies,
) -> Iterator[None]:
  """Classify repository reads and artifact hydration through one error owner."""


  try:
    yield
  except ExactModelVersionArtifactHydrationError:
    raise
  except dependencies.model_version_error as exc:
    _integrity(
      str(getattr(exc, "code")),
      "stored ModelVersion row cannot satisfy exact-version hydration",
      exc,
    )
  except dependencies.source_bundle_error as exc:
    code = str(getattr(exc, "code"))
    if code == "source_bundle_rebuild_required":
      _rebuild(code, str(exc), exc, fix=PRE_CUT_REBUILD_REPAIR)
    _integrity(code, str(exc), exc)
  except sqlite3.Error as exc:
    if _is_retryable_sqlite_error(exc):
      _retryable(
        "repository_snapshot_retryable",
        "repository read snapshot is temporarily unavailable",
        exc,
      )
    _integrity(
      "repository_snapshot_failed",
      "repository read snapshot could not hydrate the exact version",
      exc,
    )
  except OSError as exc:
    _raise_os_error(role="model_version", exc=exc)
  except (TypeError, ValueError, ValidationError, RuntimeError) as exc:
    _integrity(
      "model_version_hydration_failed",
      "stored exact ModelVersion artifacts could not be hydrated",
      exc,
    )


def hydrate_exact_model_version_artifacts_in_snapshot(
  repository: ExactModelVersionArtifactRepository,
  conn: sqlite3.Connection,
  *,
  scope: tuple[str, int, str],
  model_ref: ModelVersionRef,
  expected_model_version: ModelVersion,
  expected_bundle_id: str,
  dependencies: ArtifactReadDependencies,
) -> ExactModelVersionArtifacts:
  """Hydrate one exact version without leaving the caller's read snapshot."""

  with _exact_model_version_artifact_error_adapter(dependencies):
    return _hydrate_exact_model_version_artifacts_in_snapshot(
      repository,
      conn,
      scope=scope,
      model_ref=model_ref,
      expected_model_version=expected_model_version,
      expected_bundle_id=expected_bundle_id,
      dependencies=dependencies,
    )


def hydrate_exact_model_version_artifacts(
  repository: ExactModelVersionArtifactRepository,
  *,
  expected_model_version: ModelVersion,
  expected_bundle_id: str,
  dependencies: ArtifactReadDependencies,
) -> ExactModelVersionArtifacts:
  """Open one ordinary read snapshot and hydrate the expected exact version."""

  try:
    validated = ModelVersion.model_validate(expected_model_version)
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "expected_model_version_invalid",
      "expected ModelVersion identity is malformed",
      exc,
    )
  scope = (validated.user_id, validated.research_file_id, validated.ticker)
  model_ref = ModelVersionRef(
    model_id=validated.model_id,
    version=validated.version,
  )
  try:
    with repository._conn() as conn:
      if not conn.in_transaction:
        conn.execute("BEGIN")
      return hydrate_exact_model_version_artifacts_in_snapshot(
        repository,
        conn,
        scope=scope,
        model_ref=model_ref,
        expected_model_version=validated,
        expected_bundle_id=expected_bundle_id,
        dependencies=dependencies,
      )
  except ExactModelVersionArtifactHydrationError:
    raise
  except sqlite3.Error as exc:
    if _is_retryable_sqlite_error(exc):
      _retryable(
        "repository_snapshot_retryable",
        "repository read snapshot is temporarily unavailable",
        exc,
      )
    _integrity(
      "repository_snapshot_failed",
      "repository read snapshot could not hydrate the exact version",
      exc,
    )

  except OSError as exc:
    _raise_os_error(role="repository", exc=exc)
  except (RuntimeError, TypeError, ValueError) as exc:
    _integrity(
      "repository_snapshot_failed",
      "repository read snapshot could not hydrate the exact version",
      exc,
    )


def _hydrate_verified_model_version_artifacts_in_snapshot(
  repository: ExactModelVersionArtifactRepository,
  conn: sqlite3.Connection,
  expected_model_version: ModelVersion,
  *,
  dependencies: ArtifactReadDependencies,
) -> VerifiedModelVersionArtifacts:
  canonical = repository.get_model_version_in_snapshot(
    conn,
    user_id=expected_model_version.user_id,
    research_file_id=expected_model_version.research_file_id,
    ticker=expected_model_version.ticker,
    model_ref=ModelVersionRef(
      model_id=expected_model_version.model_id,
      version=expected_model_version.version,
    ),
  )
  if canonical is None:
    _integrity(
      "model_version_absent",
      "expected immutable ModelVersion row is absent",
    )
  if canonical != expected_model_version:
    _integrity(
      "model_version_payload_mismatch",
      "stored ModelVersion differs from the complete expected payload",
    )
  # The row carries every artifact sha256, so its digest names the decoded set.
  # A hit skips the decode but not the integrity check: every artifact file is
  # stream-hashed against the row's sha256 (a stat memo is no integrity check),
  # and a mismatch evicts the entry and raises as a miss would.
  # SourceBundle and BusinessModelRevision are rows a later write can change,
  # so the lineage check below runs on every read.
  key = hashlib.sha256(canonical.model_dump_json().encode()).hexdigest()
  decoded = _ARTIFACT_CACHE.get(key)
  cached = decoded is not None
  if decoded is not None:
    try:
      _verify_artifact_digests(repository, canonical)
    except BaseException:
      _ARTIFACT_CACHE.discard(key)
      raise
  else:
    artifacts = _read_all_artifacts(repository, canonical)
    (
      semantic_document,
      provenance,
      validation_report,
      source_bundle,
      model_spec,
    ) = _decode_artifacts(model_version=canonical, artifacts=artifacts)
    decoded = _DecodedArtifacts(
      file_scope=(canonical.user_id, canonical.research_file_id, canonical.ticker),
      nbytes=sum(len(artifact.content) for artifact in artifacts.values()),
      workbook_bytes=artifacts["workbook"].content,
      semantic_document=semantic_document,
      provenance=provenance,
      validation_report=validation_report,
      source_bundle=source_bundle,
      model_spec=model_spec,
    )
  embedded_bundle_id = decoded.semantic_document.render_state.compiler_inputs.source_bundle.bundle_id
  if embedded_bundle_id is None:
    _integrity(
      "source_bundle_id_invalid",
      "stored SourceBundle compiler input does not identify its exact bundle",
    )
  source_bundle_lineage = _verify_source_bundle_lineage(
    repository,
    conn,
    model_version=canonical,
    expected_bundle_id=embedded_bundle_id,
    semantic_document=decoded.semantic_document,
    embedded_source_bundle=decoded.source_bundle,
    model_spec=decoded.model_spec,
    dependencies=dependencies,
  )
  if not cached:
    _ARTIFACT_CACHE.put(key, decoded)
  return VerifiedModelVersionArtifacts(
    model_version=canonical,
    workbook_bytes=decoded.workbook_bytes,
    semantic_document=decoded.semantic_document,
    provenance=decoded.provenance,
    validation_report=decoded.validation_report,
    source_bundle=decoded.source_bundle,
    model_spec=decoded.model_spec,
    source_bundle_lineage=source_bundle_lineage,
  )


def hydrate_verified_model_version_artifacts(
  repository: ExactModelVersionArtifactRepository,
  *,
  expected_model_version: ModelVersion,
  dependencies: ArtifactReadDependencies,
) -> VerifiedModelVersionArtifacts:
  """Hydrate one exact ModelVersion without exposing artifact paths."""

  try:
    validated = ModelVersion.model_validate(expected_model_version)
  except (TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "expected_model_version_invalid",
      "expected ModelVersion identity is malformed",
      exc,
    )
  try:
    with repository._conn() as conn:
      if not conn.in_transaction:
        conn.execute("BEGIN")
      return _hydrate_verified_model_version_artifacts_in_snapshot(
        repository,
        conn,
        validated,
        dependencies=dependencies,
      )
  except ExactModelVersionArtifactHydrationError:
    raise
  except dependencies.model_version_error as exc:
    _integrity(str(getattr(exc, "code")), str(exc), exc)
  except dependencies.source_bundle_error as exc:
    code = str(getattr(exc, "code"))
    if code == "source_bundle_rebuild_required":
      _rebuild(code, str(exc), exc, fix=PRE_CUT_REBUILD_REPAIR)
    _integrity(code, str(exc), exc)
  except sqlite3.Error as exc:
    if _is_retryable_sqlite_error(exc):
      _retryable(
        "repository_snapshot_retryable",
        "repository read snapshot is temporarily unavailable",
        exc,
      )
    _integrity(
      "repository_snapshot_failed",
      "repository read snapshot could not hydrate the exact version",
      exc,
    )
  except OSError as exc:
    _raise_os_error(role="repository", exc=exc)
  except (RuntimeError, TypeError, ValueError, ValidationError) as exc:
    _integrity(
      "model_version_hydration_failed",
      "stored exact ModelVersion artifacts could not be hydrated",
      exc,
    )


__all__ = [
  "EXACT_MODEL_VERSION_ARTIFACT_INTEGRITY_ERROR",
  "EXACT_MODEL_VERSION_ARTIFACT_IO_RETRYABLE",
  "EXACT_MODEL_VERSION_ARTIFACT_REBUILD_ERROR",
  "ArtifactReadDependencies",
  "ArtifactSourceBundle",
  "ExactModelVersionArtifactHydrationError",
  "ExactModelVersionArtifacts",
  "ExactSourceBundleLineage",
  "VerifiedModelVersionArtifacts",
  "hydrate_exact_model_version_artifacts",
  "hydrate_exact_model_version_artifacts_in_snapshot",
  "hydrate_verified_model_version_artifacts",
]
