"""The agent's ModelVersion query door: trace, enumerate and history on one version.

``schema.model_queries`` owns ``trace`` and ``enumerate`` and
``schema.judgment_history`` owns ``history``. ``build_model_reads`` owns the
exact-version hydration and query projection behind this door. This module
owns the read contracts and the agent-facing entrypoint, and writes nothing.

- **trace** — when the selection names an item and a period: the rendered
  cell, every cell it depends on with its operation kind, and the facts,
  judgments and house defaults it rests on. Null otherwise, and for an item
  and period the version does not render.
- **enumerate** — always: every judgment row of the five stage artifacts the
  version binds and this research file holds, every house ``default`` value,
  and the count of compiled values by kind. ``bound_artifacts`` names each
  binding and whether the file holds it; one it does not hold adds no rows.
- **history** — per requested ``{artifact, subject}``: the subject's rows from
  the artifact the version binds back along ``predecessor_ref``, oldest first,
  each with the Supersession that revised it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_serializer

from research.model_version_compare_view import ModelVersionSelection, WorkbookCellState
from schema.judgment_revision import Supersession
from schema.model_queries import Enumeration, JudgmentArtifactKind, Trace


class _ViewContract(BaseModel):
  model_config = ConfigDict(extra="forbid", frozen=True)


class HistorySubject(_ViewContract):
  """One judgment subject whose revisions the door reads."""

  artifact: JudgmentArtifactKind = "forecast_assumption_set"
  #: The row key: a FAS driver key, an HFF normalization label, a BMR plan
  #: entry's ``driver_id``, a ScenarioSet curve's driver key, or a
  #: ValuationPolicy input name, method, ``terminal_policy`` or ``model_policy``.
  subject: str = Field(min_length=1)


class BoundJudgmentArtifact(_ViewContract):
  artifact: JudgmentArtifactKind
  artifact_id: str = Field(min_length=1)
  #: Whether this research file holds the artifact the binding names.
  held: bool


class JudgmentHistoryRow(_ViewContract):
  artifact_id: str = Field(min_length=1)
  created_at: datetime
  #: The subject's row on this artifact; null where this artifact dropped it.
  row: dict[str, Any] | None = None
  #: This artifact's revision of the predecessor's row, if it recorded one.
  revision: Supersession | None = None


class JudgmentHistoryView(_ViewContract):
  artifact: JudgmentArtifactKind
  subject: str = Field(min_length=1)
  #: Oldest first, ending at the artifact this version binds; empty when the
  #: version binds no such artifact or no artifact on its chain holds the subject.
  entries: tuple[JudgmentHistoryRow, ...] = ()


class ModelVersionQueryView(_ViewContract):
  #: The selection, echoed; ``item`` and ``period`` select the traced cell.
  selection: ModelVersionSelection
  trace: Trace | None = None
  enumeration: Enumeration
  bound_artifacts: tuple[BoundJudgmentArtifact, ...]
  history: tuple[JudgmentHistoryView, ...] = ()
  #: When ``selection`` supplies a workbook cell or ``workbook_value``, whether
  #: the open cell matches the published ``semantic.json`` snapshot.
  workbook_cell_state: WorkbookCellState | None = None

  @model_serializer(mode="wrap")
  def _serialize_query_view(self, handler: Any) -> dict[str, Any]:
    data = dict(handler(self))
    if data.get("workbook_cell_state") is None:
      data.pop("workbook_cell_state", None)
    return data

def query_model_version(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  selection: ModelVersionSelection,
  history_subjects: tuple[HistorySubject, ...] = (),
) -> ModelVersionQueryView:
  """Trace, enumerate and read history on one persisted ModelVersion."""

  from research.build_model_reads import read_model_semantics

  return read_model_semantics(
    repo,
    user_id=user_id,
    research_file_id=research_file_id,
    view="queries",
    selection=selection,
    history_subjects=history_subjects,
  )


__all__ = [
  "BoundJudgmentArtifact",
  "HistorySubject",
  "JudgmentHistoryRow",
  "JudgmentHistoryView",
  "ModelVersionQueryView",
  "query_model_version",
]
