"""The agent's ModelVersion compare door: a bounded view of one comparison.

``schema.model_version_compare`` owns the pure diff; ``build_model_reads``
owns exact-version hydration and the bounded projection, including valuation
policy changes and carried judgments. This module owns the read contracts
and the agent-facing entrypoint. No comparison is persisted.

- **Selection identity.** Every request and result carries
  ``{model_id, version, item, period}``: the version compared against the
  baseline, and the item and period that narrow the deltas. ``model_id`` and
  ``version`` are the ``ModelVersionRef`` coordinates the publisher stamps into
  the workbook; there is no second binding.
- **Bounding** (default proposed in
  ``docs/queue/compare-model-versions-and-selection-identity-exposed.md``; this
  module is its revert site). Each delta family (financial, assumption,
  scenario, valuation_policy) reports its counts, plus the ``row_limit``
  largest changed rows by absolute change. Rows beyond the limit are
  reachable only by narrowing the selection.
- **Carried judgments.** The judgments that bind a change and were carried
  unchanged, each named by artifact and path: a changed driver's rationale and
  scenario-curve rationales; and, for any change, the case weights and the
  valuation-policy selections. They are reported, never invalidated: whether
  a revision invalidates them is methodology (R2-Q10).
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer

from schema.model_ref import ModelVersionRef
from schema.model_version_compare import (
  AssumptionDelta,
  ModelStatusDelta,
  ModelVersionIdentityComparison,
  ScenarioReviewDelta,
  SemanticValueDelta,
)


#: How many changed rows of each delta family the view returns, largest first.
ROW_LIMIT = 20


class _ViewContract(BaseModel):
  model_config = ConfigDict(extra="forbid", frozen=True)


WorkbookCellState = Literal["verified", "modified_locally", "unmapped_cell"]


class ModelVersionSelection(_ViewContract):
  """What the analyst has selected: a version, and optionally one item/period or workbook cell."""

  model_id: str = Field(min_length=1)
  version: int = Field(ge=1)
  #: A rendered item id (``revenue``) or a driver key / assumption id;
  #: absent for every item (or resolved from ``sheet`` and ``cell``).
  item: str | None = Field(default=None, min_length=1)
  #: A fiscal year (``2027``); absent for every period (or resolved from ``sheet`` and ``cell``).
  period: int | None = None
  #: Optional worksheet name and A1 cell / range coordinate from an Excel selection.
  sheet: str | None = Field(default=None, min_length=1)
  cell: str | None = Field(default=None, min_length=1)
  #: Optional cell value read from the open workbook to check against ``semantic.json``.
  workbook_value: float | int | str | None = None

  @model_serializer(mode="wrap")
  def _serialize_selection(self, handler: Any) -> dict[str, Any]:
    data = dict(handler(self))
    for optional_key in ("sheet", "cell", "workbook_value"):
      if data.get(optional_key) is None:
        data.pop(optional_key, None)
    return data

  def model_version_ref(self) -> ModelVersionRef:
    return ModelVersionRef(model_id=self.model_id, version=self.version)


def _workbook_value_matches_snapshot(snapshot: Any, workbook_value: Any) -> bool:
  from schema.published_model_readback import values_match

  candidate_value = workbook_value
  if (
    isinstance(snapshot.value, (int, float))
    and not isinstance(snapshot.value, bool)
    and isinstance(workbook_value, str)
  ):
    try:
      candidate_value = float(workbook_value.strip())
    except ValueError:
      candidate_value = workbook_value
  return values_match(snapshot.value, candidate_value, snapshot.tolerance)


def resolve_workbook_cell_selection(
  semantic_values: tuple[Any, ...],
  selection: ModelVersionSelection,
) -> tuple[ModelVersionSelection, WorkbookCellState | None]:
  """Resolve ``selection.sheet`` / ``selection.cell`` against ``semantic_values`` and check ``workbook_value``."""

  from schema.model_queries import resolve_cell_snapshot

  if (
    selection.cell is None
    and selection.sheet is None
    and selection.workbook_value is None
  ):
    return selection, None

  snapshot = None
  if selection.cell is not None:
    snapshot = resolve_cell_snapshot(
      semantic_values, sheet=selection.sheet, cell=selection.cell,
    )
    if snapshot is None:
      return selection, "unmapped_cell"
    if (
      (selection.item is not None and selection.item != snapshot.semantic_item_id)
      or (selection.period is not None and selection.period != snapshot.period)
    ):
      return selection, None
  elif selection.item is not None and selection.period is not None:
    for candidate in semantic_values:
      if (
        candidate.semantic_item_id == selection.item
        and candidate.period == selection.period
      ):
        snapshot = candidate
        break
    if snapshot is None:
      return selection, "unmapped_cell" if selection.workbook_value is not None else None
  else:
    return selection, None

  cell_state: WorkbookCellState | None = None
  if selection.workbook_value is not None:
    cell_state = (
      "verified"
      if _workbook_value_matches_snapshot(snapshot, selection.workbook_value)
      else "modified_locally"
    )
  resolved = selection.model_copy(
    update={
      "item": snapshot.semantic_item_id,
      "period": snapshot.period,
      "sheet": snapshot.sheet,
      "cell": snapshot.cell,
    }
  )
  return resolved, cell_state

class DriverAssumptionDelta(AssumptionDelta):
  #: The driver the assumption sets; null when neither spec names the id.
  driver_key: str | None = None


class DriverScenarioDelta(ScenarioReviewDelta):
  #: The driver the overridden assumption sets; null when neither spec names it.
  driver_key: str | None = None


class _DeltaFamily(_ViewContract):
  #: Changed rows the selection holds; ``largest`` is at most ``row_limit`` of them.
  changed_count: int = Field(ge=0)
  #: Distinct items (financial) or drivers (assumption, scenario) among them.
  subject_count: int = Field(ge=0)
  #: ``changed_count`` by the row's ``change``.
  by_change: dict[str, int] = Field(default_factory=dict)


class FinancialDeltas(_DeltaFamily):
  #: Rendered values by item and period; each row carries its tolerance.
  largest: tuple[SemanticValueDelta, ...] = ()


class AssumptionDeltas(_DeltaFamily):
  largest: tuple[DriverAssumptionDelta, ...] = ()


class ScenarioDeltas(_DeltaFamily):
  largest: tuple[DriverScenarioDelta, ...] = ()


class ValuationPolicyDelta(_ViewContract):
  """One changed ValuationPolicy selection: a method, a terminal case or a year's rate."""

  #: The selection's field: ``dcf_methods[terminal_growth]``,
  #: ``relative_methods[forward_pe]``, ``terminal_growth_cases``,
  #: ``exit_multiple_cases`` or ``net_share_dilution``. ``item`` selects by it.
  policy_field: str = Field(min_length=1)
  #: Where the value sits under ``valuation_policy``.
  path: str = Field(min_length=1)
  change: Literal["added", "removed", "changed"]
  #: The fiscal year of a net share dilution rate; null otherwise.
  period: int | None = None
  old_value: Any = None
  new_value: Any = None


class ValuationPolicyDeltas(_DeltaFamily):
  #: The ValuationPolicy each version binds (``vpl_…``); null when it binds none.
  baseline_artifact_id: str | None = None
  artifact_id: str | None = None
  largest: tuple[ValuationPolicyDelta, ...] = ()


CarriedJudgmentKind = Literal[
  "assumption_rationale",
  "scenario_curve_rationale",
  "case_weights",
  "valuation_policy_selection",
]


class CarriedJudgment(_ViewContract):
  """One judgment binding a changed driver or policy, carried unchanged into the compared version."""

  kind: CarriedJudgmentKind
  #: The accepted artifact holding it in the compared version (``fas_…``,
  #: ``scn_…``, ``vpl_…``); null when that version binds none.
  artifact_id: str | None = None
  #: Where the judgment sits inside that artifact.
  path: str = Field(min_length=1)
  #: The changed driver it binds; null for model-wide judgments (case weights,
  #: valuation-policy selections), which every change carries.
  driver_key: str | None = None
  rationale: str | None = None
  value: Any = None


class ModelVersionCompareView(_ViewContract):
  #: The selection, echoed; its ``model_id``/``version`` is the compared version.
  selection: ModelVersionSelection
  #: The version compared against; every delta reads baseline → selection.
  baseline: ModelVersionRef
  identity: ModelVersionIdentityComparison
  row_limit: int = Field(ge=1)
  financial: FinancialDeltas
  assumption: AssumptionDeltas
  scenario: ScenarioDeltas
  valuation_policy: ValuationPolicyDeltas
  #: Every carried judgment of the changes the selected item holds.
  carried_judgment_count: int = Field(ge=0)
  carried_judgments: tuple[CarriedJudgment, ...] = ()
  status: ModelStatusDelta


class ModelVersionReadError(RuntimeError):
  """Typed refusal of a ModelVersion read door (compare, query).

  ``next_call`` names the call that repairs it, if any; ``fix`` carries the
  owner error's repair text through to the tool result, when there is one.
  """

  def __init__(
    self,
    status_code: int,
    code: str,
    message: str,
    *,
    next_call: dict[str, Any] | None = None,
    fix: str | None = None,
  ) -> None:
    super().__init__(message)
    self.status_code = status_code
    self.code = code
    self.next_call = next_call
    self.fix = fix


def research_file_ticker(repo: Any, *, user_id: str, research_file_id: int) -> str:
  """The authenticated research file's ticker, the scope every version read takes."""

  from research.forecast_assumptions_context_service import (
    ForecastAssumptionsContextServiceError,
    _authenticated_research_ticker,
  )

  try:
    return _authenticated_research_ticker(
      repo,
      user_id=user_id,
      research_file_id=research_file_id,
    )
  except ForecastAssumptionsContextServiceError as exc:
    raise ModelVersionReadError(exc.status_code, exc.code, str(exc)) from exc


def compare_model_versions_for_selection(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  baseline: ModelVersionRef,
  selection: ModelVersionSelection,
) -> ModelVersionCompareView:
  """Compare two persisted ModelVersions of one research file, baseline → selection."""

  from research.build_model_reads import compare_model_versions

  return compare_model_versions(
    repo,
    user_id=user_id,
    research_file_id=research_file_id,
    baseline=baseline,
    selection=selection,
  )
def compare_model_version_candidate_for_selection(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  baseline: ModelVersionRef,
  selection: ModelVersionSelection,
  successor_spec: Any,
  semantic_values: tuple[Any, ...],
  provenance: Any,
  schedule_input_audit: Any,
  validation_status: Any,
  validation_blocker_count: int,
  valuation_evidence_status: Any,
) -> ModelVersionCompareView:
  """Compare a persisted baseline ModelVersion with an unpersisted try candidate."""

  from model_build import reads as engine_reads
  from research.build_model_reads import _read_model_version, comparison_view_factories
  from schema.model_version_compare import compare_model_version_candidate

  ticker = research_file_ticker(
    repo,
    user_id=user_id,
    research_file_id=research_file_id,
  )
  predecessor = _read_model_version(
    repo,
    user_id=user_id,
    research_file_id=research_file_id,
    ticker=ticker,
    model_ref=baseline,
  )
  resolved_selection, _ = resolve_workbook_cell_selection(
    semantic_values or getattr(predecessor.semantic_document, "semantic_values", ()),
    selection,
  )
  comparison = compare_model_version_candidate(
    predecessor.semantic_document,
    successor_ref=resolved_selection.model_version_ref(),
    successor_spec=successor_spec,
    semantic_values=semantic_values,
    provenance=provenance,
    schedule_input_audit=schedule_input_audit,
    validation_status=validation_status,
    validation_blocker_count=validation_blocker_count,
    valuation_evidence_status=valuation_evidence_status,
  )
  view = engine_reads._bound_model_version_comparison(
    comparison,
    predecessor_spec=predecessor.model_spec,
    successor_spec=successor_spec,
    item=resolved_selection.item,
    period=resolved_selection.period,
    views=comparison_view_factories(),
  )
  if resolved_selection != view.selection:
    return view.model_copy(update={"selection": resolved_selection})
  return view

__all__ = [
  "ROW_LIMIT",
  "AssumptionDeltas",
  "CarriedJudgment",
  "DriverAssumptionDelta",
  "DriverScenarioDelta",
  "FinancialDeltas",
  "ModelVersionCompareView",
  "ModelVersionReadError",
  "ModelVersionSelection",
  "ScenarioDeltas",
  "ValuationPolicyDelta",
  "ValuationPolicyDeltas",
  "WorkbookCellState",
  "compare_model_version_candidate_for_selection",
  "compare_model_versions_for_selection",
  "research_file_ticker",
  "resolve_workbook_cell_selection",
]
