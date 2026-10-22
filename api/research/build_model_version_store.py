from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from schema.build_model_product_contracts import ModelVersion
from schema.model_ref import ModelVersionRef

if TYPE_CHECKING:
  from contextlib import AbstractContextManager


def _hydrate_model_version(row: sqlite3.Row) -> ModelVersion:
  # The record owner composes this store and uses its decoder for write replay.
  # Import its column validators at call time to keep that composition acyclic.
  from .repository.model_versions import (
    MODEL_PAYLOAD_GENERATION,
    ModelVersionRepositoryError,
    _CORRUPT_STORAGE,
    _model_record_from_row,
    _model_record_lineage_from_row,
    _model_record_lineage_of,
    _model_record_of,
  )

  try:
    model_version = ModelVersion.model_validate_json(str(row["payload_json"]))
  except ValueError as exc:
    raise ModelVersionRepositoryError(
      "model_version_storage_malformed",
      f"stored ModelVersion payload is malformed; {_CORRUPT_STORAGE}",
    ) from exc
  if (
    not isinstance(row["payload_json"], str)
    or type(row["payload_generation"]) is not int
    or row["payload_generation"] != MODEL_PAYLOAD_GENERATION
    or _model_record_from_row(row) != _model_record_of(model_version)
    or _model_record_lineage_from_row(row) != _model_record_lineage_of(model_version)
  ):
    raise ModelVersionRepositoryError(
      "model_version_storage_malformed",
      f"stored ModelVersion identity columns do not match its payload; {_CORRUPT_STORAGE}",
    )
  return model_version


class BuildModelVersionStoreMixin:
  if TYPE_CHECKING:
    def _conn(self) -> AbstractContextManager[sqlite3.Connection]: ...

    def _require_model_scope(
      self,
      conn: sqlite3.Connection,
      *,
      user_id: object,
      research_file_id: int,
      ticker: object,
    ) -> tuple[str, int, str]: ...

  def get_model_version_in_transaction(
    self,
    conn: sqlite3.Connection,
    *,
    user_id: object,
    research_file_id: int,
    ticker: object,
    model_ref: ModelVersionRef,
  ) -> ModelVersion | None:
    from .repository.connection import require_repository_write_transaction

    require_repository_write_transaction(conn)
    validated_ref = ModelVersionRef.model_validate(model_ref)
    scope = self._require_model_scope(
      conn,
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
    )
    row = conn.execute(
      """
      SELECT *
      FROM model_versions
      WHERE user_id=?
        AND research_file_id=?
        AND ticker=?
        AND model_id=?
        AND version=?
      """,
      (*scope, validated_ref.model_id, validated_ref.version),
    ).fetchone()
    return None if row is None else _hydrate_model_version(row)

  def get_model_version_in_snapshot(
    self,
    conn: sqlite3.Connection,
    *,
    user_id: object,
    research_file_id: int,
    ticker: object,
    model_ref: ModelVersionRef,
  ) -> ModelVersion | None:
    """Read one exact immutable version inside the caller's read snapshot."""

    if not isinstance(conn, sqlite3.Connection) or not conn.in_transaction:
      raise RuntimeError(
        "get_model_version_in_snapshot requires an active transaction"
      )
    validated_ref = ModelVersionRef.model_validate(model_ref)
    scope = self._require_model_scope(
      conn,
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
    )
    row = conn.execute(
      """
      SELECT *
      FROM model_versions
      WHERE user_id=?
        AND research_file_id=?
        AND ticker=?
        AND model_id=?
        AND version=?
      """,
      (*scope, validated_ref.model_id, validated_ref.version),
    ).fetchone()
    return None if row is None else _hydrate_model_version(row)

  def get_model_version(
    self,
    *,
    user_id: object,
    research_file_id: int,
    ticker: object,
    model_ref: ModelVersionRef,
  ) -> ModelVersion | None:
    validated_ref = ModelVersionRef.model_validate(model_ref)
    with self._conn() as conn:
      scope = self._require_model_scope(
        conn,
        user_id=user_id,
        research_file_id=research_file_id,
        ticker=ticker,
      )
      row = conn.execute(
        """
        SELECT *
        FROM model_versions
        WHERE user_id=?
          AND research_file_id=?
          AND ticker=?
          AND model_id=?
          AND version=?
        """,
        (*scope, validated_ref.model_id, validated_ref.version),
      ).fetchone()
      return None if row is None else _hydrate_model_version(row)
