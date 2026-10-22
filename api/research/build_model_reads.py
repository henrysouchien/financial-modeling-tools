"""Product acquisition adapters for the model-build root's committed reads."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from model_build import reads as engine_reads
from model_build.artifact_reads import (
  EXACT_MODEL_VERSION_ARTIFACT_REBUILD_ERROR,
  ArtifactReadDependencies, ArtifactSourceBundle,
  ExactModelVersionArtifactHydrationError, ExactModelVersionArtifacts,
  VerifiedModelVersionArtifacts, hydrate_verified_model_version_artifacts,
  hydrate_exact_model_version_artifacts, hydrate_exact_model_version_artifacts_in_snapshot,
  _hydrate_verified_model_version_artifacts_in_snapshot,
  _exact_model_version_artifact_error_adapter, _integrity,
)
from research.current_model_version_resolver import (
  CurrentModelNotFound, CurrentModelResolutionError, CurrentModelSelectionChanged,
  ResolvedCurrentModel,
)
from research.model_version_compare_view import (
  ROW_LIMIT, AssumptionDeltas, CarriedJudgment, DriverAssumptionDelta,
  DriverScenarioDelta, FinancialDeltas, ModelVersionCompareView,
  ModelVersionReadError, ModelVersionSelection, ScenarioDeltas,
  ValuationPolicyDelta, ValuationPolicyDeltas, research_file_ticker,
  resolve_workbook_cell_selection,
)
from research.model_queries_view import (
  BoundJudgmentArtifact, HistorySubject, JudgmentHistoryRow,
  JudgmentHistoryView, ModelVersionQueryView,
)
from research.postcompile_valuation_service import PostcompileValuationServiceError
from schema.build_model_product_contracts import (
  ModelVersion, AnnualRequirementCoverage, FiscalPeriodFacts, HistoricalBasisFact,
  MarketCalendarFacts, SourceEvidence, ValuationCoverage, ValuationObservation,
)
from schema.business_model_revision_ref import BusinessModelRevisionRef
from schema.coverage_profile import AnnualSourceRequirement, ValuationRequirement
from schema.judgment_history import JudgmentArtifact
from schema.model_queries import JudgmentArtifactKind
from schema.model_ref import CurrentModelPointer, ModelVersionRef
from industry_slice_core.thesis_shared_sources import SourceRecord


if TYPE_CHECKING:
  from research.repository.model_versions import ModelRecordCommit


def artifact_read_dependencies(repository: Any) -> ArtifactReadDependencies:
  """Adapt scoped repository bundles to the root's immutable hydration inputs."""
  from research.repository import ModelVersionRepositoryError
  from research.repository.build_model_infrastructure import (
    BuildModelStoreError, source_bundle_identity_digest,
  )

  loaded_bundles: dict[str, Any] = {}

  def load_source_bundle(conn: sqlite3.Connection, **scope: Any) -> ArtifactSourceBundle:
    loaded = repository.load_source_bundle_for_build_in_snapshot(conn, **scope)
    loaded_bundles[loaded.ref.bundle_id] = loaded
    identity = loaded.identity
    return ArtifactSourceBundle(
      bundle_id=loaded.ref.bundle_id,
      content_sha256=loaded.ref.content_sha256,
      identity_digest=source_bundle_identity_digest(identity),
      business_model_revision_ref=BusinessModelRevisionRef(
        business_model_id=identity.bmr_business_model_id,
        revision=identity.bmr_revision,
        content_sha256=identity.bmr_content_sha256,
      ),
      payload=loaded.payload,
    )

  def load_business_model_revision(conn: sqlite3.Connection, *, bundle_id: str) -> Any:
    return repository.load_source_bundle_business_model_revision_in_snapshot(
      conn, loaded_bundle=loaded_bundles[bundle_id],
    )

  return ArtifactReadDependencies(
    load_source_bundle=load_source_bundle,
    load_business_model_revision=load_business_model_revision,
    model_version_error=ModelVersionRepositoryError,
    source_bundle_error=BuildModelStoreError,
  )


def comparison_view_factories() -> engine_reads.CompareViewFactories:
  return engine_reads.CompareViewFactories(
    selection=ModelVersionSelection,
    driver_assumption=DriverAssumptionDelta,
    driver_scenario=DriverScenarioDelta,
    valuation_policy_delta=ValuationPolicyDelta,
    carried_judgment=CarriedJudgment,
    compare_view=ModelVersionCompareView,
    financial=FinancialDeltas,
    assumption=AssumptionDeltas,
    scenario=ScenarioDeltas,
    valuation_policy=ValuationPolicyDeltas,
    row_limit=ROW_LIMIT,
  )


def query_view_factories() -> engine_reads.QueryViewFactories[ModelVersionQueryView]:
  return engine_reads.QueryViewFactories(
    bound_artifact=BoundJudgmentArtifact,
    history_row=JudgmentHistoryRow,
    history_view=JudgmentHistoryView,
    query_view=ModelVersionQueryView,
  )


@dataclass(frozen=True, slots=True)
class _ResolvedCurrentEngine:
  pointer: CurrentModelPointer
  model_version: ModelVersion
  verified_artifacts: VerifiedModelVersionArtifacts | None = field(
    default=None,
    compare=False,
    repr=False,
  )




def _require_same_pointer(repository: Any, current_model: _ResolvedCurrentEngine) -> None:
  version = current_model.model_version
  refreshed = repository.get_current_model_pointer(
    user_id=version.user_id,
    research_file_id=version.research_file_id,
    ticker=version.ticker,
  )
  if refreshed != current_model.pointer:
    raise CurrentModelSelectionChanged()


def _hydrate_current_model_artifacts(
  repository: Any,
  version: ModelVersion,
) -> VerifiedModelVersionArtifacts:
  """Resolve the selected version's decoded contracts, or refuse by type.

  D-DP5 (Stage D plan §4.6, ratified): a current pointer that selects a
  ModelVersion this binary's contract classes cannot decode is a **forced
  rebuild**, not a migration.  Rebuild-not-migrate is the stage's rule, so
  there is no pointer-migration step here and never will be — the refusal
  names the repair instead.  Serving such a version read-only is a separate,
  narrower path (the D-2 stored-artifact read allowance); inspection needs the
  decoded spec, so inspection refuses.
  """

  try:
    return hydrate_verified_model_version_artifacts(
      repository,
      expected_model_version=version,
      dependencies=artifact_read_dependencies(repository),
    )
  except ExactModelVersionArtifactHydrationError as exc:
    if exc.code == EXACT_MODEL_VERSION_ARTIFACT_REBUILD_ERROR:
      # The pointer repair is deliberately a different sentence from the
      # version repair: republishing moves the pointer, so the analyst must
      # also select the rebuilt version as current. It derives from the
      # owner's fix instead of naming a second constant at this seam.
      pointer_fix = f"{exc.fix}; then select the rebuilt version as current"
      raise CurrentModelResolutionError(
        "current_model_artifact_rebuild_required",
        f"{exc} — {pointer_fix}",
        retryable=exc.retryable,
        fix=pointer_fix,
      ) from exc
    raise CurrentModelResolutionError(
      "current_model_artifact_integrity_failed",
      str(exc),
      retryable=exc.retryable,
    ) from exc


def _read_current_engine(
  repository: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
) -> _ResolvedCurrentEngine:
  """Resolve one authenticated pointer/version pair without exposing a path."""

  from research.repository import ModelVersionRepositoryError

  try:
    pointer = repository.get_current_model_pointer(
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
    )
    if pointer is None or pointer.selected is None:
      raise CurrentModelNotFound(research_file_id)
    model_version = repository.get_model_version(
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
      model_ref=pointer.selected,
    )
    if model_version is None:
      raise CurrentModelResolutionError(
        "current_model_version_missing",
        "current model pointer does not resolve to an immutable ModelVersion",
      )
    resolved = _ResolvedCurrentEngine(
      pointer=pointer,
      model_version=model_version,
      verified_artifacts=_hydrate_current_model_artifacts(
        repository,
        model_version,
      ),
    )
    engine_reads._current_model_identity(
      resolved.pointer, resolved.model_version, error_type=CurrentModelResolutionError,
    )
    _require_same_pointer(repository, resolved)
    return resolved
  except CurrentModelResolutionError:
    raise
  except ModelVersionRepositoryError as exc:
    raise CurrentModelResolutionError(exc.code, str(exc)) from exc




def _current_workbook_path(
  repository: Any,
  current_model: _ResolvedCurrentEngine,
) -> Path:
  db_path = getattr(repository, "db_path", None)
  if not isinstance(db_path, Path):
    raise CurrentModelResolutionError(
      "current_model_repository_invalid",
      "current model repository does not expose its scoped database path",
    )
  user_root = db_path.parent.resolve(strict=True)
  candidate = Path(current_model.model_version.workbook.location)
  if candidate.is_absolute() or any(
    part in {"", ".", ".."} for part in candidate.parts
  ):
    raise CurrentModelResolutionError(
      "current_model_artifact_integrity_failed",
      "workbook artifact path is not a repository-relative location",
    )
  try:
    resolved = user_root.joinpath(*candidate.parts).resolve(strict=True)
    resolved.relative_to(user_root)
  except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
    raise CurrentModelResolutionError(
      "current_model_artifact_integrity_failed",
      "workbook artifact path is unavailable or outside repository scope",
    ) from exc
  return resolved




@dataclass(frozen=True, slots=True)
class PrecompileInputsRead:
  """Validated corpus facts for judgment authorship, before a model exists."""

  cutoff_at: datetime
  retrieved_at: datetime
  requirements: tuple[AnnualSourceRequirement, ...]
  coverage: tuple[AnnualRequirementCoverage, ...]
  evidence: tuple[SourceEvidence, ...]
  historical_basis_facts: tuple[HistoricalBasisFact, ...]
  valuation_requirements: tuple[ValuationRequirement, ...]
  valuation_observations: tuple[ValuationObservation, ...]
  valuation_coverage: tuple[ValuationCoverage, ...]
  market_calendar: MarketCalendarFacts
  fiscal_calendar: tuple[FiscalPeriodFacts, ...]
  sources: tuple[SourceRecord, ...]


def read_stage_judgment_inputs(
  repository: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
  reviewed_windows: bool = True,
) -> PrecompileInputsRead:
  """Read the research corpus without selecting or hydrating a committed model.

  House values use the compile's reviewed-window recipe. The required valuation
  snapshot instead reads the plain corpus/profile recipe, independently of
  statement correspondence. Producer errors pass through to the context owner.
  """

  from model_build.corpus_facts import resolve_annual_coverage_profile
  from research.build_model_source_bundle import build_source_bundle
  from research.capture_store import research_user_root
  from research.model_build_inputs import build_research_file_source_bundle
  from research.runtime_corpus import resolve_research_corpus

  corpus = resolve_research_corpus(
    user_root=research_user_root(repository),
    research_file_id=research_file_id,
    ticker=ticker,
  )
  profile = resolve_annual_coverage_profile(corpus)
  bundle = (
    build_research_file_source_bundle(
      repository, corpus, profile,
      user_id=user_id, research_file_id=research_file_id, ticker=ticker,
    )
    if reviewed_windows
    else build_source_bundle(corpus, profile)
  )
  return PrecompileInputsRead(
    cutoff_at=bundle.cutoff_at,
    retrieved_at=bundle.retrieved_at,
    requirements=bundle.requirements,
    coverage=bundle.coverage,
    evidence=bundle.evidence,
    historical_basis_facts=bundle.historical_basis_facts,
    valuation_requirements=bundle.valuation_requirements,
    valuation_observations=bundle.valuation_observations,
    valuation_coverage=bundle.valuation_coverage,
    market_calendar=bundle.market_calendar,
    fiscal_calendar=bundle.fiscal_calendar,
    sources=bundle.sources,
  )


def read_current_model(
  repository: Any,
  current_model: ResolvedCurrentModel | None = None,
  *,
  user_id: str | None = None,
  research_file_id: int | None = None,
  ticker: str | None = None,
  model_ref: ModelVersionRef | None = None,
  source_bundle_id: str | None = None,
  conn: sqlite3.Connection | None = None,
) -> engine_reads.BuildModelReadProjection[ModelRecordCommit]:
  """Hydrate the selected current model or one explicitly scoped immutable ref."""

  from research.repository.model_versions import model_record_commit_of

  user_id = cast(str, user_id)
  research_file_id = cast(int, research_file_id)
  ticker = cast(str, ticker)

  if current_model is not None:
    record = current_model.model_version
    user_id, research_file_id, ticker = record.user_id, record.research_file_id, record.ticker
    engine = _read_current_engine(
      repository, user_id=user_id, research_file_id=research_file_id, ticker=ticker,
    )
    if engine.pointer != current_model.pointer:
      raise CurrentModelSelectionChanged()
  elif model_ref is None:
    engine = _read_current_engine(
      repository, user_id=user_id, research_file_id=research_file_id, ticker=ticker,
    )
  else:
    dependencies = artifact_read_dependencies(repository)
    with _exact_model_version_artifact_error_adapter(dependencies):
      if conn is None:
        version = repository.get_model_version(
          user_id=user_id, research_file_id=research_file_id, ticker=ticker, model_ref=model_ref,
        )
      else:
        version = repository.get_model_version_in_snapshot(
          conn, user_id=user_id, research_file_id=research_file_id, ticker=ticker, model_ref=model_ref,
        )
      if version is None:
        _integrity(
          "model_version_absent",
          "expected immutable ModelVersion row is absent",
        )
      if source_bundle_id is None:
        hydrated = (
          hydrate_verified_model_version_artifacts(
            repository, expected_model_version=version, dependencies=dependencies,
          )
          if conn is None
          else _hydrate_verified_model_version_artifacts_in_snapshot(
            repository, conn, version, dependencies=dependencies,
          )
        )
      elif conn is None:
        hydrated = hydrate_exact_model_version_artifacts(
          repository, expected_model_version=version, expected_bundle_id=source_bundle_id,
          dependencies=dependencies,
        )
      else:
        hydrated = hydrate_exact_model_version_artifacts_in_snapshot(
          repository, conn, scope=(version.user_id, version.research_file_id, version.ticker),
          model_ref=model_ref, expected_model_version=version, expected_bundle_id=source_bundle_id,
          dependencies=dependencies,
        )
      return engine_reads.read_current_model(
        commit=model_record_commit_of(version), artifacts=hydrated,
        workbook_path=hydrated.workbook_path if isinstance(hydrated, ExactModelVersionArtifacts) else None,
        error_type=CurrentModelResolutionError,
      )

  version = engine.model_version
  selected = repository.get_selected_business_model_revision(
    user_id=version.user_id, research_file_id=version.research_file_id, ticker=version.ticker,
  )
  projection = engine_reads.read_current_model(
    commit=model_record_commit_of(version),
    artifacts=cast(VerifiedModelVersionArtifacts, engine.verified_artifacts),
    pointer=engine.pointer,
    selected_business_model_revision=None if selected is None else selected[1],
    workbook_path=_current_workbook_path(repository, engine),
    error_type=CurrentModelResolutionError,
  )
  _require_same_pointer(repository, engine)
  return projection


def _read_model_version(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
  model_ref: ModelVersionRef,
) -> VerifiedModelVersionArtifacts:
  """Resolve the exact ModelVersion in the research file and hydrate its artifacts."""

  from research.repository import ModelVersionRepositoryError

  try:
    model_version = repo.get_model_version(
      user_id=user_id,
      research_file_id=research_file_id,
      ticker=ticker,
      model_ref=model_ref,
    )
  except ModelVersionRepositoryError as exc:
    raise ModelVersionReadError(409, exc.code, str(exc)) from exc
  if model_version is None:
    raise ModelVersionReadError(
      404,
      "model_version_not_found",
      f"ModelVersion {model_ref.model_id} v{model_ref.version} is not a version of "
      f"research file {research_file_id}.",
      next_call={
        "tool_name": "get_current_model",
        "arguments": {"research_file_id": research_file_id},
      },
    )
  try:
    return hydrate_verified_model_version_artifacts(
      repo,
      expected_model_version=model_version,
      dependencies=artifact_read_dependencies(repo),
    )
  except ExactModelVersionArtifactHydrationError as exc:
    raise ModelVersionReadError(
      409,
      exc.code,
      str(exc),
      next_call=(
        {
          "tool_name": "build_model",
          "arguments": {"research_file_id": research_file_id, "operation": "build"},
        }
        if exc.code == EXACT_MODEL_VERSION_ARTIFACT_REBUILD_ERROR
        else None
      ),
      fix=exc.fix,
    ) from exc


def compare_model_versions(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  baseline: ModelVersionRef,
  selection: ModelVersionSelection,
) -> ModelVersionCompareView:
  """Compare two persisted ModelVersions of one research file, baseline → selection."""

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
  successor = _read_model_version(
    repo,
    user_id=user_id,
    research_file_id=research_file_id,
    ticker=ticker,
    model_ref=selection.model_version_ref(),
  )
  resolved_selection, _ = resolve_workbook_cell_selection(
    getattr(successor.semantic_document, "semantic_values", ()),
    selection,
  )
  view = engine_reads.compare_model_versions(
    predecessor, successor, item=resolved_selection.item, period=resolved_selection.period,
    views=comparison_view_factories(),
  )
  if resolved_selection != view.selection:
    return view.model_copy(update={"selection": resolved_selection})
  return view




_Loader = Callable[[Any], JudgmentArtifact | None]


def _loaders(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
) -> dict[JudgmentArtifactKind, _Loader]:
  """Each artifact kind's exact-ref read, in this research file's scope."""

  scope: dict[str, Any] = {
    "user_id": user_id,
    "research_file_id": research_file_id,
    "ticker": ticker,
  }
  return {
    "business_model_revision": lambda ref: repo.get_business_model_revision(
      **scope, ref=ref
    ),
    "historical_foundation": lambda ref: repo.get_historical_foundation(
      **scope, ref=ref
    ),
    "forecast_assumption_set": lambda ref: repo.get_forecast_assumption_set(
      **scope, ref=ref
    ),
    "scenario_set": lambda ref: repo.get_scenario_set(**scope, ref=ref),
    "valuation_policy": lambda ref: repo.get_valuation_policy(**scope, ref=ref),
  }



@overload
def read_model_semantics(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str | None = None,
  view: Literal["sections"] = "sections",
  selection: ModelVersionSelection | None = None,
  history_subjects: tuple[HistorySubject, ...] = (),
) -> engine_reads.ModelSemanticsRead: ...


@overload
def read_model_semantics(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str | None = None,
  view: Literal["queries"],
  selection: ModelVersionSelection,
  history_subjects: tuple[HistorySubject, ...] = (),
) -> ModelVersionQueryView: ...


def read_model_semantics(
  repo: Any,
  *,
  user_id: str,
  research_file_id: int,
  ticker: str | None = None,
  view: Literal["sections", "queries"] = "sections",
  selection: ModelVersionSelection | None = None,
  history_subjects: tuple[HistorySubject, ...] = (),
) -> engine_reads.ModelSemanticsRead | ModelVersionQueryView:
  """Read current compiler semantics or the selected version's existing query view."""

  if view == "queries":
    selection = cast(ModelVersionSelection, selection)
    selected_ticker = research_file_ticker(
      repo, user_id=user_id, research_file_id=research_file_id,
    )
    artifacts = _read_model_version(
      repo, user_id=user_id, research_file_id=research_file_id,
      ticker=selected_ticker, model_ref=selection.model_version_ref(),
    )
    resolved_selection, cell_state = resolve_workbook_cell_selection(
      getattr(artifacts.semantic_document, "semantic_values", ()),
      selection,
    )
    query_view = engine_reads.read_model_semantics(
      artifacts,
      engine_reads.ModelQueryRead(
        selection=resolved_selection,
        loaders=_loaders(
          repo, user_id=user_id, research_file_id=research_file_id, ticker=selected_ticker,
        ),
        views=query_view_factories(), history_subjects=history_subjects,
      ),
    )
    if cell_state is not None:
      return query_view.model_copy(update={"workbook_cell_state": cell_state})
    return query_view

  if ticker is None:
    ticker = research_file_ticker(
      repo,
      user_id=user_id,
      research_file_id=research_file_id,
    )
  engine = _read_current_engine(
    repo,
    user_id=user_id,
    research_file_id=research_file_id,
    ticker=ticker,
  )
  return engine_reads.read_model_semantics(
    cast(VerifiedModelVersionArtifacts, engine.verified_artifacts),
    engine.pointer,
  )



def read_postcompile_binding(
    repo,
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    model_ref: ModelVersionRef,
    source_bundle_id: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> engine_reads.PostcompileBindingRead[ModelRecordCommit] | None:
    """Read exact compiled authority within one snapshot or the writer transaction.

    An absent version returns None; an absent bound policy returns the model
    record with no snapshot. Corrupt artifacts still raise their owning error.
    """
    from research.repository.model_versions import model_record_commit_of
    from research.repository.valuation_policies import _hydrate_valuation_policy

    if conn is None:
        with repo._conn() as snapshot_conn:
            if not snapshot_conn.in_transaction:
                snapshot_conn.execute("BEGIN")
            return read_postcompile_binding(
                repo, user_id=user_id, research_file_id=research_file_id,
                ticker=ticker, model_ref=model_ref,
                source_bundle_id=source_bundle_id, conn=snapshot_conn,
            )
    user_id, research_file_id, ticker = repo._require_model_scope(
        conn, user_id=user_id, research_file_id=research_file_id, ticker=ticker,
    )
    version = repo.get_model_version_in_snapshot(
        conn, user_id=user_id, research_file_id=research_file_id,
        ticker=ticker, model_ref=model_ref,
    )
    if version is None:
        return None
    dependencies = artifact_read_dependencies(repo)
    if source_bundle_id is None:
        artifacts = _hydrate_verified_model_version_artifacts_in_snapshot(
            repo, conn, version, dependencies=dependencies,
        )
    else:
        artifacts = hydrate_exact_model_version_artifacts_in_snapshot(
            repo, conn, scope=(user_id, research_file_id, ticker),
            model_ref=model_ref, expected_model_version=version,
            expected_bundle_id=source_bundle_id,
            dependencies=dependencies,
        )
    commit = model_record_commit_of(version)
    binding = artifacts.model_spec.valuation_policy_binding
    policy_row = None if binding is None else conn.execute(
        """SELECT * FROM valuation_policies
           WHERE user_id=? AND research_file_id=? AND ticker=?
             AND policy_id=? AND content_sha256=?""",
        (user_id, research_file_id, ticker, binding.valuation_policy_id,
         binding.valuation_policy_content_sha256),
    ).fetchone()
    policy = None if policy_row is None else _hydrate_valuation_policy(policy_row)
    return engine_reads.read_postcompile_binding(
        commit=commit, artifacts=artifacts, valuation_policy=policy,
        error_type=PostcompileValuationServiceError,
    )


__all__ = [
  "read_current_model",
  "read_model_semantics",
  "compare_model_versions",
  "read_postcompile_binding",
  "read_stage_judgment_inputs",
]
