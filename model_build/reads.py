"""Committed-model projections over acquired, verified artifacts.

Product adapters own scoped storage reads, snapshots and error/view classes;
these four reads own the engine-derived computation.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
import hashlib
import json
from pathlib import Path
from typing import Any, Generic, Literal, Protocol, TypeVar, overload

from pydantic import BaseModel
from model_build.artifact_reads import ExactModelVersionArtifacts, VerifiedModelVersionArtifacts
from schema.build_model_product_contracts import ModelSpec, ModelVersion, resolve_model_spec_business_model_revision
from schema.model_ref import CurrentModelPointer, ModelRecord, ModelRecordArtifact, ModelVersionRef, PUBLISHABLE_MODEL_STATUSES
from schema.model_semantic_document import ModelSemanticDocument
from schema.model_spec_decoder import VerifiedModelSpecResult
from schema.model_state_identity import CurrentModelIdentity
from schema.valuation_evidence_derivations import derive_covered_market_inputs
from schema.model_version_compare import ModelVersionComparison, SemanticValueDelta, compare_model_versions as _compare_semantic_model_versions
from schema import model_queries
from schema.forecast_assumption_set import ForecastAssumptionSetRef
from schema.historical_foundation import HistoricalFoundationRef
from schema.judgment_history import JudgmentArtifact, history
from schema.model_queries import JudgmentArtifactKind
from schema.scenario_set import ScenarioSetRef
from schema.valuation_policy import ValuationPolicyRef, ValuationPolicyArtifact
from schema.analysis import (
    DcfMethod,
    EXIT_EARNINGS_ITEMS,
    ExitEarningsMeasure,
    dcf_case_prices,
    judgment_drivers,
    sensitize,
)
from schema.build_findings import make_finding
from schema.finding_vocabulary import Finding
from schema.postcompile_valuation import (
    CONSENSUS_NON_GAAP_EBITDA_DENOMINATOR_LIMITATION,
    CompiledCurrentPrice, CompiledDriverSensitivity, CompiledPriceCases,
    CompiledValuationComponent, CompiledValuationMethodOutput,
    CompiledValuationSnapshot, ExactModelVersionBinding,
    compiled_valuation_snapshot_digest,
)
from schema.versioned_artifact import canonical_json_bytes


class ModelCommit(Protocol):
  @property
  def record(self) -> ModelRecord: ...


_Commit = TypeVar("_Commit", bound=ModelCommit)
_QueryView = TypeVar("_QueryView")


@dataclass(frozen=True, slots=True)
class CompareViewFactories:
  """Product-owned view constructors and the product's presentation row limit."""
  selection: Callable[..., Any]
  driver_assumption: Callable[..., Any]
  driver_scenario: Callable[..., Any]
  valuation_policy_delta: Callable[..., Any]
  carried_judgment: Callable[..., Any]
  compare_view: Callable[..., Any]
  financial: Callable[..., Any]
  assumption: Callable[..., Any]
  scenario: Callable[..., Any]
  valuation_policy: Callable[..., Any]
  row_limit: int


@dataclass(frozen=True, slots=True)
class QueryViewFactories(Generic[_QueryView]):
  bound_artifact: Callable[..., Any]
  history_row: Callable[..., Any]
  history_view: Callable[..., Any]
  query_view: Callable[..., _QueryView]


@dataclass(frozen=True, slots=True)
class ModelQueryRead(Generic[_QueryView]):
  """One query selection with its acquired judgment loaders and view constructors."""
  selection: Any
  loaders: Mapping[JudgmentArtifactKind, Callable[[Any], JudgmentArtifact | None]]
  views: QueryViewFactories[_QueryView]
  history_subjects: tuple[Any, ...] = ()

def _current_model_identity(pointer: CurrentModelPointer, version: ModelVersion, *, error_type: type[Exception]) -> CurrentModelIdentity:
  if pointer.selected is None or (
    pointer.selected.model_id,
    pointer.selected.version,
  ) != (version.model_id, version.version):
    raise error_type(
      "current_model_identity_invalid",
      "current model pointer and immutable ModelVersion identities differ",
    )
  return CurrentModelIdentity(
    research_file_id=version.research_file_id,
    ticker=version.ticker,
    model_id=version.model_id,
    version=version.version,
    pointer_epoch=pointer.epoch,
    workbook_xlsx_sha256=version.workbook.sha256,
    model_spec_sha256=version.model_spec_digest,
  )

@dataclass(frozen=True, slots=True)
class BuildModelReadProjection(Generic[_Commit]):
  """The committed record and immutable values read from its engine artifacts."""

  commit: _Commit
  _artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts = field(repr=False)
  pointer: CurrentModelPointer | None = None
  selected_business_model_revision: Any = None
  workbook_path: Path | None = None

  @property
  def record(self) -> ModelRecord:
    return self.commit.record

  @property
  def model_spec_sha256(self) -> str:
    return self._artifacts.model_version.model_spec_digest

  @property
  def semantic_document(self) -> ModelSemanticDocument:
    return self._artifacts.semantic_document

  @property
  def provenance(self) -> Any:
    return self._artifacts.provenance

  @property
  def validation_report(self) -> Any:
    return self._artifacts.validation_report

  @property
  def workbook(self) -> bytes:
    return self._artifacts.workbook_bytes

  @property
  def source_bundle_lineage(self) -> Any:
    return self._artifacts.source_bundle_lineage

  @property
  def bundle_id(self) -> str:
    return self._artifacts.source_bundle_lineage.bundle_id

  @property
  def bundle_ticker(self) -> str:
    return self._artifacts.source_bundle.ticker

  @property
  def spec_ticker(self) -> str:
    return self._artifacts.model_spec.ticker

  @property
  def forecast_assumptions(self) -> tuple[Any, ...]:
    return tuple(self._artifacts.model_spec.forecast_assumptions)

  @property
  def forecast_assumption_set_ref(self) -> ForecastAssumptionSetRef | None:
    binding = self._artifacts.model_spec.forecast_assumption_set_binding
    return None if binding is None else ForecastAssumptionSetRef(
      set_id=binding.forecast_assumption_set_id,
      content_sha256=binding.forecast_assumption_set_content_sha256,
    )

  @property
  def scenario_set_ref(self) -> ScenarioSetRef | None:
    binding = self._artifacts.model_spec.scenario_set_binding
    return None if binding is None else ScenarioSetRef(
      set_id=binding.scenario_set_id,
      content_sha256=binding.scenario_set_content_sha256,
    )

  @property
  def valuation_policy_ref(self) -> ValuationPolicyRef | None:
    binding = self._artifacts.model_spec.valuation_policy_binding
    return None if binding is None else ValuationPolicyRef(
      policy_id=binding.valuation_policy_id,
      content_sha256=binding.valuation_policy_content_sha256,
    )

  @property
  def claims(self) -> tuple[Any, ...]:
    return tuple(self._artifacts.source_bundle.claims)

  @property
  def references(self) -> tuple[Any, ...]:
    return tuple(self._artifacts.source_bundle.references)

  @property
  def displayed_method(self) -> str | None:
    policy = self._artifacts.model_spec.valuation_policy
    return None if policy is None else policy.summary.displayed_method

  @property
  def valuation_observations(self) -> tuple[Any, ...]:
    return self._artifacts.source_bundle.valuation_observations

  @property
  def stock_price(self) -> Any:
    bundle = self._artifacts.source_bundle
    return derive_covered_market_inputs(
      bundle.valuation_observations, bundle.cutoff_at, bundle.market_calendar,
      bundle.fiscal_calendar, requirements=bundle.valuation_requirements,
      focal_ticker=bundle.coverage_profile.focal_ticker,
      sources=bundle.sources, retrieved_at=bundle.retrieved_at,
    ).stock_price

  @property
  def verified_model_spec(self) -> VerifiedModelSpecResult:
    return VerifiedModelSpecResult(
      model_spec=self._artifacts.model_spec,
      verification_mode="persisted_model_version",
    )

  @property
  def identity(self) -> CurrentModelIdentity:
    if self.pointer is None:
      raise TypeError("current model identity requires its selected pointer")
    return _current_model_identity(
      self.pointer, self._artifacts.model_version, error_type=ValueError,
    )

def read_current_model(
  *,
  commit: _Commit,
  artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts,
  pointer: CurrentModelPointer | None = None,
  selected_business_model_revision: Any = None,
  workbook_path: Path | None = None,
  error_type: type[Exception],
) -> BuildModelReadProjection[_Commit]:
  """Project an exact model, validating the current selection's bound revision."""
  if pointer is not None:
    _current_model_identity(pointer, artifacts.model_version, error_type=error_type)
    if selected_business_model_revision is None:
      raise error_type(
        "current_model_business_model_revision_stale",
        "current ModelVersion was not built from the selected BusinessModel revision",
      )
    try:
      resolve_model_spec_business_model_revision(
        artifacts.model_spec, selected_business_model_revision,
      )
    except ValueError as exc:
      raise error_type(
        "current_model_business_model_revision_stale",
        "current ModelVersion was not built from the selected BusinessModel revision",
      ) from exc
  return BuildModelReadProjection(
    commit=commit, _artifacts=artifacts, pointer=pointer,
    selected_business_model_revision=selected_business_model_revision,
    workbook_path=workbook_path,
  )

class _DeltaRow(Protocol):
  @property
  def old_value(self) -> Any: ...

  @property
  def new_value(self) -> Any: ...


_Row = TypeVar("_Row", bound=_DeltaRow)


def _is_number(value: object) -> bool:
  return isinstance(value, int | float) and not isinstance(value, bool)


def _largest_first(rows: Sequence[_Row]) -> tuple[_Row, ...]:
  """Numeric changes by absolute size, then structural ones, each in schema order."""

  def key(row: _Row) -> tuple[int, float]:
    if _is_number(row.old_value) and _is_number(row.new_value):
      return (0, -abs(float(row.new_value) - float(row.old_value)))
    return (1, 0.0)

  return tuple(sorted(rows, key=key))


def _in_period(row_period: int | None, period: int | None) -> bool:
  # A scalar assumption (no period) holds in every period.
  return period is None or row_period is None or row_period == period


def _driver_keys(*specs: ModelSpec) -> dict[str, str]:
  return {
    str(assumption.assumption_id): str(assumption.driver)
    for spec in specs
    for assumption in spec.forecast_assumptions
    if assumption.assumption_id is not None
  }


def _names_item(item: str | None, *names: str | None) -> bool:
  return item is None or item in names


def _family_counts(rows: Sequence[Any], subjects: Iterable[object]) -> dict[str, Any]:
  return {
    "changed_count": len(rows),
    "subject_count": len(set(subjects)),
    "by_change": dict(sorted(Counter(row.change for row in rows).items())),
  }


def _assumptions_by_id(spec: ModelSpec) -> dict[str, Any]:
  return {
    str(assumption.assumption_id): assumption
    for assumption in spec.forecast_assumptions
    if assumption.assumption_id is not None
  }


def _overrides(spec: ModelSpec) -> dict[tuple[str, str], Any]:
  return {
    (review.scenario_name, override.assumption_id): override
    for review in spec.scenario_reviews
    for override in review.overrides
  }


def _policy_id(spec: ModelSpec) -> str | None:
  binding = spec.valuation_policy_binding
  return None if binding is None else binding.valuation_policy_id


def _dump(value: Any) -> Any:
  if isinstance(value, BaseModel):
    return value.model_dump(mode="json")
  if isinstance(value, tuple):
    return [_dump(element) for element in value]
  return value


def _change(old: object, new: object) -> Literal["added", "removed", "changed"]:
  return "added" if old is None else "removed" if new is None else "changed"


_WHOLE_POLICY_FIELDS = ("peer_policy", "sensitivities", "summary")


def _valuation_policy_deltas(
  predecessor: ModelSpec,
  successor: ModelSpec,
  *, views: CompareViewFactories,
) -> list[Any]:
  """Every changed ValuationPolicy selection, baseline → selection."""

  old_policy = predecessor.valuation_policy
  new_policy = successor.valuation_policy
  deltas: list[Any] = []

  def add(
    policy_field: str,
    path: str,
    old: object,
    new: object,
    period: int | None = None,
  ) -> None:
    if old != new:
      deltas.append(
        views.valuation_policy_delta(
          policy_field=policy_field,
          path=path,
          change=_change(old, new),
          period=period,
          old_value=old,
          new_value=new,
        )
      )

  for family in ("dcf_methods", "relative_methods"):
    old_methods = {
      method.method: method
      for method in (() if old_policy is None else getattr(old_policy, family))
    }
    new_methods = {
      method.method: method
      for method in (() if new_policy is None else getattr(new_policy, family))
    }
    for name in sorted(set(old_methods) | set(new_methods)):
      add(
        f"{family}[{name}]",
        f"{family}[{name}]",
        _dump(old_methods.get(name)),
        _dump(new_methods.get(name)),
      )
  for field_name in ("terminal_growth_cases", "exit_multiple_cases"):
    old_cases = None if old_policy is None else getattr(old_policy, field_name)
    new_cases = None if new_policy is None else getattr(new_policy, field_name)
    for case in ("bull", "base", "bear"):
      add(
        field_name,
        f"{field_name}.{case}",
        None if old_cases is None else float(getattr(old_cases, case)),
        None if new_cases is None else float(getattr(new_cases, case)),
      )
  old_dilution = None if old_policy is None else old_policy.net_share_dilution
  new_dilution = None if new_policy is None else new_policy.net_share_dilution
  old_rates = (
    {} if old_dilution is None
    else {row.fiscal_year: float(row.rate) for row in old_dilution.rates}
  )
  new_rates = (
    {} if new_dilution is None
    else {row.fiscal_year: float(row.rate) for row in new_dilution.rates}
  )
  for year in sorted(set(old_rates) | set(new_rates)):
    add(
      "net_share_dilution",
      f"net_share_dilution.rates[{year}]",
      old_rates.get(year),
      new_rates.get(year),
      period=year,
    )
  add(
    "net_share_dilution",
    "net_share_dilution.rationale",
    None if old_dilution is None else old_dilution.rationale,
    None if new_dilution is None else new_dilution.rationale,
  )
  for field_name in _WHOLE_POLICY_FIELDS:
    add(
      field_name,
      field_name,
      None if old_policy is None else _dump(getattr(old_policy, field_name)),
      None if new_policy is None else _dump(getattr(new_policy, field_name)),
    )
  return deltas


def _carried_judgments(
  predecessor: ModelSpec,
  successor: ModelSpec,
  *,
  changed: Sequence[tuple[str, str | None]],
  policy_changed: bool,
  views: CompareViewFactories,
) -> tuple[Any, ...]:
  """Judgments equal in both specs that bind what changed.

  A driver's rationale and scenario-curve rationales bind that driver
  (``changed``: ``(assumption_id, driver_key)`` pairs); the case weights and
  the valuation-policy selections bind every change, a policy change included.
  """

  if not changed and not policy_changed:
    return ()
  fas = successor.forecast_assumption_set_binding
  scenario_set = successor.scenario_set_binding
  fas_id = None if fas is None else fas.forecast_assumption_set_id
  scenario_set_id = None if scenario_set is None else scenario_set.scenario_set_id
  policy_id = _policy_id(successor)
  carried: list[Any] = []

  old_assumptions = _assumptions_by_id(predecessor)
  new_assumptions = _assumptions_by_id(successor)
  old_overrides = _overrides(predecessor)
  new_overrides = _overrides(successor)
  for assumption_id, driver_key in changed:
    prior = old_assumptions.get(assumption_id)
    current = new_assumptions.get(assumption_id)
    if (
      prior is not None
      and current is not None
      and current.rationale
      and current.rationale == prior.rationale
    ):
      carried.append(
        views.carried_judgment(
          kind="assumption_rationale",
          artifact_id=fas_id,
          path=f"forecast_assumptions[{assumption_id}].rationale",
          driver_key=driver_key,
          rationale=current.rationale,
        )
      )
    for (scenario_name, override_id), override in sorted(new_overrides.items()):
      if override_id != assumption_id:
        continue
      prior_override = old_overrides.get((scenario_name, override_id))
      if prior_override is None or prior_override.rationale != override.rationale:
        continue
      carried.append(
        views.carried_judgment(
          kind="scenario_curve_rationale",
          artifact_id=scenario_set_id,
          path=f"scenario_reviews[{scenario_name}].overrides[{assumption_id}].rationale",
          driver_key=driver_key,
          rationale=override.rationale,
        )
      )

  old_set = predecessor.scenario_set_binding
  old_weights = None if old_set is None else old_set.case_probabilities
  new_weights = None if scenario_set is None else scenario_set.case_probabilities
  if new_weights is not None and new_weights == old_weights:
    carried.append(
      views.carried_judgment(
        kind="case_weights",
        artifact_id=scenario_set_id,
        path="case_probabilities",
        rationale=new_weights.rationale,
        value={
          "bull": new_weights.bull,
          "base": new_weights.base,
          "bear": new_weights.bear,
        },
      )
    )

  old_policy = predecessor.valuation_policy
  new_policy = successor.valuation_policy
  if old_policy is not None and new_policy is not None:
    old_dcf = {method.method: method for method in old_policy.dcf_methods}
    old_relative = {method.method: method for method in old_policy.relative_methods}
    methods: list[tuple[str, str, str | None, Any]] = []
    methods.extend(
      ("dcf_methods", method.method, method.rationale, method.display_role)
      for method in new_policy.dcf_methods
      if old_dcf.get(method.method) == method
    )
    methods.extend(
      ("relative_methods", method.method, method.rationale, method.selection_basis)
      for method in new_policy.relative_methods
      if old_relative.get(method.method) == method
    )
    for family, method_name, rationale, selected in methods:
      carried.append(
        views.carried_judgment(
          kind="valuation_policy_selection",
          artifact_id=policy_id,
          path=f"valuation_policy.{family}[{method_name}]",
          rationale=rationale,
          value=selected,
        )
      )
    for field_name in (
      "terminal_growth_cases",
      "exit_multiple_cases",
      "net_share_dilution",
    ):
      value = getattr(new_policy, field_name)
      if value is None or value != getattr(old_policy, field_name):
        continue
      dumped = value.model_dump(mode="json")
      carried.append(
        views.carried_judgment(
          kind="valuation_policy_selection",
          artifact_id=policy_id,
          path=f"valuation_policy.{field_name}",
          rationale=dumped.pop("rationale", None),
          value=dumped,
        )
      )
  return tuple(carried)


def _bound_model_version_comparison(
  comparison: ModelVersionComparison,
  *,
  predecessor_spec: ModelSpec,
  successor_spec: ModelSpec,
  item: str | None = None,
  period: int | None = None,
  views: CompareViewFactories,
) -> Any:
  """Bound one comparison to the selection and report the carried judgments.

  The selection's ``model_id``/``version`` is the comparison's successor and
  the baseline its predecessor, read from the comparison itself, so a try
  candidate compared through ``compare_model_version_candidate`` and a kept
  successor compared through ``compare_model_versions`` read the same way.
  """

  drivers = _driver_keys(predecessor_spec, successor_spec)
  successor = comparison.identity.successor
  selection = views.selection(
    model_id=successor.model_id,
    version=successor.version,
    item=item,
    period=period,
  )

  financial = [
    row
    for row in comparison.semantic_value_deltas
    if _names_item(item, row.item_id) and (period is None or row.period == period)
  ]
  item_assumptions = [
    views.driver_assumption(
      **row.model_dump(),
      driver_key=drivers.get(row.assumption_id),
    )
    for row in comparison.assumption_deltas
    if _names_item(item, row.assumption_id, drivers.get(row.assumption_id))
  ]
  item_scenarios = [
    views.driver_scenario(
      **row.model_dump(),
      driver_key=drivers.get(row.assumption_id),
    )
    for row in comparison.scenario_review_deltas
    if _names_item(item, row.assumption_id, drivers.get(row.assumption_id))
  ]
  assumptions = [row for row in item_assumptions if _in_period(row.period, period)]
  scenarios = [row for row in item_scenarios if _in_period(row.period, period)]
  item_policy = [
    row
    for row in _valuation_policy_deltas(predecessor_spec, successor_spec, views=views)
    if _names_item(item, row.policy_field)
  ]
  policy = [row for row in item_policy if _in_period(row.period, period)]

  # A carried judgment binds the driver, not the period: a revised FY2027 row
  # still carries the FY2028 rationale that leans on it.
  changed = list(
    dict.fromkeys(
      (row.assumption_id, row.driver_key)
      for row in (*_largest_first(item_assumptions), *_largest_first(item_scenarios))
    )
  )
  carried = _carried_judgments(
    predecessor_spec,
    successor_spec,
    changed=changed,
    policy_changed=bool(item_policy),
    views=views,
  )

  return views.compare_view(
    selection=selection,
    baseline=comparison.identity.predecessor,
    identity=comparison.identity,
    row_limit=views.row_limit,
    financial=views.financial(
      **_family_counts(financial, (row.item_id for row in financial)),
      largest=_largest_first(financial)[:views.row_limit],
    ),
    assumption=views.assumption(
      **_family_counts(
        assumptions,
        (row.driver_key or row.assumption_id for row in assumptions),
      ),
      largest=_largest_first(assumptions)[:views.row_limit],
    ),
    scenario=views.scenario(
      **_family_counts(
        scenarios,
        (row.driver_key or row.assumption_id for row in scenarios),
      ),
      largest=_largest_first(scenarios)[:views.row_limit],
    ),
    valuation_policy=views.valuation_policy(
      **_family_counts(policy, (row.policy_field for row in policy)),
      baseline_artifact_id=_policy_id(predecessor_spec),
      artifact_id=_policy_id(successor_spec),
      largest=_largest_first(policy)[:views.row_limit],
    ),
    carried_judgment_count=len(carried),
    carried_judgments=carried[:views.row_limit],
    status=comparison.status,
  )

def compare_model_versions(
  predecessor: VerifiedModelVersionArtifacts,
  successor: VerifiedModelVersionArtifacts,
  *,
  item: str | None = None,
  period: int | None = None,
  views: CompareViewFactories,
) -> Any:
  """Compare acquired versions baseline → selection, including carried judgments."""
  return _bound_model_version_comparison(
    _compare_semantic_model_versions(predecessor.semantic_document, successor.semantic_document),
    predecessor_spec=predecessor.model_spec,
    successor_spec=successor.model_spec,
    item=item, period=period, views=views,
  )

def _assumption_key(assumption: Any) -> str:
  return str(assumption.assumption_id or assumption.driver).strip()


def _assumption_value_payload(
  assumption: Any,
  *,
  years: tuple[int, ...],
  value: Any | None = None,
) -> dict[str, Any]:
  resolved_value = (
    assumption.period_values
    if value is None and assumption.period_values is not None
    else assumption.value if value is None else value
  )
  source_ids = sorted(set(assumption.source_refs))
  values = (
    {
      str(year): float(resolved_value[year])
      for year in years
      if year in resolved_value
    }
    if isinstance(resolved_value, dict)
    else {str(year): resolved_value for year in years}
  )
  refs_by_period = {
    str(year): sorted(
      set(assumption.source_refs_by_period.get(year, source_ids))
    )
    for year in years
  }
  return {
    "values": values,
    "value_scale": "model",
    "unit": (
      assumption.unit.value
      if getattr(assumption, "unit", None) is not None
      else None
    ),
    "rationale": assumption.rationale,
    "confidence": assumption.confidence,
    "held_at_base": assumption.held_at_base,
    "source_refs_by_period": refs_by_period,
    "extension_basis_by_period": {
      str(year): basis
      for year, basis in sorted(assumption.extension_basis_by_period.items())
      if year in years
    },
    "sources": [],
  }


def _canonical_semantics_sections(
  model_spec: ModelSpec,
) -> dict[str, Any]:
  model_spec = ModelSpec.model_validate(model_spec)
  years = tuple(
    period.fiscal_year for period in model_spec.fiscal_axis.forecast_periods
  )
  assumptions = {
    _assumption_key(assumption): assumption
    for assumption in model_spec.forecast_assumptions
  }
  # ModelSpec owns input validation; these two raises keep the read projection
  # from silently collapsing two assumptions onto one control key or placing a
  # scenario value on no assumption.
  assumptions_by_control: dict[str, Any] = {}
  for assumption in model_spec.forecast_assumptions:
    for control_key in {
      _assumption_key(assumption),
      str(assumption.assumption_id or "").strip(),
      str(assumption.driver or "").strip(),
    }:
      if not control_key:
        continue
      existing = assumptions_by_control.get(control_key)
      if existing is not None and existing is not assumption:
        raise ValueError(
          "ModelSpec forecast assumption control keys must be unique: "
          f"{control_key}"
        )
      assumptions_by_control[control_key] = assumption
  forecast_entries = [
    {
      "assumption_id": assumption.assumption_id,
      "driver_id": key,
      "item_id": assumption.driver,
      "base": _assumption_value_payload(assumption, years=years),
    }
    for key, assumption in assumptions.items()
  ]
  scenario_entries: dict[str, dict[str, Any]] = {}
  for scenario in model_spec.scenarios:
    for key, value in scenario.assumptions.items():
      assumption = assumptions_by_control.get(key)
      if assumption is None:
        raise ValueError(
          f"ModelSpec scenario references unknown assumption key: {key}"
        )
      entry = scenario_entries.setdefault(
        key,
        {
          "assumption_id": assumption.assumption_id,
          "driver_id": key,
          "item_id": assumption.driver,
          "scenarios": {},
        },
      )
      entry["scenarios"][scenario.name] = _assumption_value_payload(
        assumption,
        years=years,
        value=value,
      )

  return {
    "forecast": {
      "schema_version": "model-spec-semantics.v1",
      "entries": sorted(
        forecast_entries,
        key=lambda entry: (
          str(entry["item_id"]).encode("utf-8"),
          str(entry["driver_id"]),
        ),
      ),
    },
    "scenarios": {
      "schema_version": "model-spec-semantics.v1",
      "entries": [
        scenario_entries[key]
        for key in sorted(scenario_entries)
      ],
    },
    "valuation": {
      "schema_version": "model-spec-semantics.v1",
      "entries": [
        control.model_dump(mode="json")
        for control in model_spec.valuation_controls
      ],
      "policy": (
        None
        if model_spec.valuation_policy is None
        else model_spec.valuation_policy.model_dump(mode="json")
      ),
    },
  }


def _canonical_model_spec_semantics_hash(
  *,
  ticker: str,
  semantics: Mapping[str, Any],
) -> str:
  payload = {
    "schema_version": "model-spec-semantics.v1",
    "ticker": ticker,
    "sections": semantics,
  }
  return hashlib.sha256(
    json.dumps(
      payload,
      sort_keys=True,
      separators=(",", ":"),
      ensure_ascii=False,
    ).encode("utf-8")
  ).hexdigest()

def _bound_refs(spec: ModelSpec) -> tuple[tuple[JudgmentArtifactKind, str, Any], ...]:
  """The stage artifacts the ModelSpec binds, in stage order, with their refs."""

  revision_ref = spec.business_model_revision_ref
  bound: list[tuple[JudgmentArtifactKind, str, Any]] = [
    ("business_model_revision", revision_ref.revision, revision_ref),
  ]
  forecast = spec.forecast_assumption_set_binding
  if forecast is not None:
    bound.append((
      "historical_foundation",
      forecast.historical_foundation_artifact_id,
      HistoricalFoundationRef(
        artifact_id=forecast.historical_foundation_artifact_id,
        content_sha256=forecast.historical_foundation_content_sha256,
      ),
    ))
    bound.append((
      "forecast_assumption_set",
      forecast.forecast_assumption_set_id,
      ForecastAssumptionSetRef(
        set_id=forecast.forecast_assumption_set_id,
        content_sha256=forecast.forecast_assumption_set_content_sha256,
      ),
    ))
  scenario = spec.scenario_set_binding
  if scenario is not None:
    bound.append((
      "scenario_set",
      scenario.scenario_set_id,
      ScenarioSetRef(
        set_id=scenario.scenario_set_id,
        content_sha256=scenario.scenario_set_content_sha256,
      ),
    ))
  policy = spec.valuation_policy_binding
  if policy is not None:
    bound.append((
      "valuation_policy",
      policy.valuation_policy_id,
      ValuationPolicyRef(
        policy_id=policy.valuation_policy_id,
        content_sha256=policy.valuation_policy_content_sha256,
      ),
    ))
  return tuple(bound)


def _history_view(
  request: Any,
  *,
  head: JudgmentArtifact | None,
  load: Callable[[Any], JudgmentArtifact | None],
  views: QueryViewFactories[_QueryView],
) -> Any:
  if head is None:
    return views.history_view(artifact=request.artifact, subject=request.subject)
  rows: list[Any] = []
  for entry in history(request.subject, head=head, load_predecessor=load):
    rows.append(
      views.history_row(
        artifact_id=model_queries.artifact_ref_id(entry.artifact_ref),
        created_at=entry.created_at,
        row=None if entry.row is None else entry.row.model_dump(mode="json"),
        revision=entry.supersession,
      )
    )
  return views.history_view(
    artifact=request.artifact,
    subject=request.subject,
    entries=tuple(rows),
  )

def _query_model_version(
  version: VerifiedModelVersionArtifacts,
  *,
  selection: Any,
  loaders: Mapping[JudgmentArtifactKind, Callable[[Any], JudgmentArtifact | None]],
  views: QueryViewFactories[_QueryView],
  history_subjects: tuple[Any, ...] = (),
) -> _QueryView:
  """Trace, enumerate and read judgment history on acquired artifacts."""
  heads: dict[JudgmentArtifactKind, JudgmentArtifact] = {}
  bound: list[Any] = []
  for kind, artifact_id, ref in _bound_refs(version.model_spec):
    artifact = loaders[kind](ref)
    if artifact is not None:
      heads[kind] = artifact
    bound.append(
      views.bound_artifact(
        artifact=kind,
        artifact_id=artifact_id,
        held=artifact is not None,
      )
    )
  traced = (
    None
    if selection.item is None or selection.period is None
    else model_queries.trace(
      version.semantic_document,
      version.model_spec,
      item_id=selection.item,
      period=selection.period,
    )
  )
  return views.query_view(
    selection=selection,
    trace=traced,
    enumeration=model_queries.enumerate(version.semantic_document, heads.values()),
    bound_artifacts=tuple(bound),
    history=tuple(
      _history_view(
        request,
        head=heads.get(request.artifact),
        load=loaders[request.artifact],
        views=views,
      )
      for request in history_subjects
    ),
  )

@dataclass(frozen=True, slots=True)
class ModelSemanticsRead:
  pointer: CurrentModelPointer
  record: ModelRecord
  sections: dict[str, Any]
  identity: dict[str, Any]

@overload
def read_model_semantics(
  artifacts: VerifiedModelVersionArtifacts,
  request: CurrentModelPointer,
) -> ModelSemanticsRead: ...


@overload
def read_model_semantics(
  artifacts: VerifiedModelVersionArtifacts,
  request: ModelQueryRead[_QueryView],
) -> _QueryView: ...


def read_model_semantics(
  artifacts: VerifiedModelVersionArtifacts,
  request: CurrentModelPointer | ModelQueryRead[_QueryView],
) -> ModelSemanticsRead | _QueryView:
  """Project current compiler sections or a selected version's query view."""
  if isinstance(request, ModelQueryRead):
    return _query_model_version(
      artifacts, selection=request.selection, loaders=request.loaders,
      views=request.views, history_subjects=request.history_subjects,
    )
  pointer = request
  version = artifacts.model_version
  ticker = version.ticker
  sections = _canonical_semantics_sections(
    artifacts.model_spec,
  )
  full_semantics_hash = _canonical_model_spec_semantics_hash(
    ticker=ticker,
    semantics=sections,
  )
  has_semantics = bool(
    sections["forecast"]["entries"]
    or sections["scenarios"]["entries"]
    or sections["valuation"]["entries"]
  )
  record = ModelRecord(
    ref=ModelVersionRef(model_id=version.model_id, version=version.version),
    user_id=version.user_id,
    research_file_id=version.research_file_id,
    ticker=version.ticker,
    status=version.status,
    committed_at=version.committed_at,
    workbook=ModelRecordArtifact(
      location=version.workbook.location,
      sha256=version.workbook.sha256,
    ),
  )
  return ModelSemanticsRead(
    pointer=pointer,
    record=record,
    sections=sections,
    identity={
      "research_file_id": version.research_file_id,
      "model_id": version.model_id,
      "model_version": version.version,
      "pointer_epoch": pointer.epoch,
      "workbook_xlsx_sha256": version.workbook.sha256,
      "semantic_sidecar_sha256": version.semantic_sidecar.sha256,
      "model_spec_sha256": version.model_spec_digest,
      "semantics_source": "current_model.model_spec",
      "model_semantics_status": "success" if has_semantics else "missing",
      "model_semantics_hash": full_semantics_hash,
    },
  )

_WACC_IDS = {
    "adjusted_beta": "tpl.v.cost_of_equity.beta",
    "after_tax_cost_of_debt": "tpl.v.wacc.after_tax_cost_of_debt",
    "cost_of_equity": "tpl.v.cost_of_equity.cost_of_equity",
    "credit_spread": "tpl.v.wacc.credit_spread",
    "debt_value": "tpl.v.wacc.debt_value",
    "debt_weight": "tpl.v.wacc.weight_debt",
    "equity_value": "tpl.v.wacc.equity_value",
    "equity_weight": "tpl.v.wacc.weight_equity",
    "pretax_cost_of_debt": "tpl.v.wacc.pretax_cost_of_debt",
    "risk_free_rate": "tpl.v.cost_of_equity.risk_free_rate",
    "sofr_rate": "tpl.v.wacc.sofr_rate",
    "tax_shield": "tpl.v.wacc.tax_shield",
    "total_capital": "tpl.v.wacc.total_capital",
    "wacc": "tpl.v.wacc.wacc",
}


_ZERO_DEBT_INAPPLICABLE_WACC_KEYS = frozenset(
    {
        "after_tax_cost_of_debt",
        "credit_spread",
        "pretax_cost_of_debt",
    }
)


_WACC_UNITS = {
    "adjusted_beta": "ratio",
    "after_tax_cost_of_debt": "ratio",
    "cost_of_equity": "ratio",
    "credit_spread": "ratio",
    "debt_value": "usd_millions",
    "debt_weight": "ratio",
    "equity_value": "usd_millions",
    "equity_weight": "ratio",
    "pretax_cost_of_debt": "ratio",
    "risk_free_rate": "ratio",
    "sofr_rate": "ratio",
    "tax_shield": "ratio",
    "total_capital": "usd_millions",
    "wacc": "ratio",
}


_SHARE_IDS = (
    "tpl.fm.income_statement.diluted_shares_outstanding_m",
    "tpl.fm.income_statement.diluted_shares",
)


_PRICE_QUANTUM = Decimal("0.000001")

_ALWAYS_DISCLOSED_LIMITATIONS: dict[str, tuple[str, ...]] = {
    "ev_adjusted_ebitda": (CONSENSUS_NON_GAAP_EBITDA_DENOMINATOR_LIMITATION,),
}

def _exit_earnings_measure(policy: ValuationPolicyArtifact) -> ExitEarningsMeasure:
    terminal = policy.terminal_policy
    if terminal is None:
        return "terminal_fcf"
    return terminal.exit_earnings_measure


def _model_version_content_sha256(value: ModelVersion) -> str:
    return hashlib.sha256(
        canonical_json_bytes(value.model_dump(mode="json"))
    ).hexdigest()


def _exact_model_version_binding(
    value: ModelVersion,
    *,
    source_bundle_id: str,
    source_bundle_identity_digest: str,
) -> ExactModelVersionBinding:
    record = ModelVersion.model_validate_json(value.model_dump_json())
    return ExactModelVersionBinding(
        model_ref=ModelVersionRef(
            model_id=record.model_id,
            version=record.version,
        ),
        source_bundle_id=source_bundle_id,
        source_bundle_identity_digest=source_bundle_identity_digest,
        model_version_content_sha256=_model_version_content_sha256(record),
        request_digest=record.request_digest,
        source_bundle_digest=record.source_bundle_digest,
        model_spec_digest=record.model_spec_digest,
        workbook_content_sha256=record.workbook.sha256,
        semantic_sidecar_content_sha256=record.semantic_sidecar.sha256,
        provenance_map_content_sha256=record.provenance_map.sha256,
        validation_report_content_sha256=record.validation_report.sha256,
        committed_at=record.committed_at,
    )


def _decimal(value: object, *, field_name: str, error_type: type[Exception]) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise error_type(
            "compiled_valuation_output_missing", f"{field_name} is not a known number"
        )
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise error_type(
            "compiled_valuation_output_invalid", f"{field_name} is not numeric"
        ) from exc
    if not result.is_finite():
        raise error_type(
            "compiled_valuation_output_invalid", f"{field_name} is not finite"
        )
    return result


class _SemanticIndex:
    """The version's compiled values by ``(semantic id, period)``.

    A duplicated or non-numeric row is recorded against its key and raised
    only when that key is read, so one bad output fails the methods that read
    it and nothing else.
    """

    def __init__(self, artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts, *, error_type: type[Exception]) -> None:
        self._error_type = error_type
        self._values: dict[tuple[str, int], Decimal] = {}
        self._refused: dict[tuple[str, int], Exception] = {}
        for row in artifacts.semantic_document.semantic_values:
            key = (row.semantic_item_id, int(row.period))
            if key in self._values:
                del self._values[key]
                self._refused[key] = error_type(
                    "compiled_valuation_output_ambiguous",
                    f"semantic valuation output {key!r} is duplicated",
                )
            elif row.state == "known" and key not in self._refused:
                try:
                    self._values[key] = _decimal(
                        row.value, field_name=row.semantic_item_id, error_type=error_type
                    )
                except error_type as exc:
                    self._refused[key] = exc

    def known(self, semantic_id: str, period: int) -> Decimal | None:
        return self._values.get((semantic_id, int(period)))

    def value(self, semantic_id: str, period: int) -> Decimal:
        key = (semantic_id, int(period))
        if key in self._refused:
            raise self._refused[key]
        try:
            return self._values[key]
        except KeyError as exc:
            raise self._error_type(
                "compiled_valuation_output_missing",
                f"compiled valuation output {semantic_id!r} is missing for FY{period}",
            ) from exc

    def first_available(self, semantic_ids: tuple[str, ...], period: int) -> Decimal:
        matches = [
            self._values[(semantic_id, int(period))]
            for semantic_id in semantic_ids
            if (semantic_id, int(period)) in self._values
        ]
        if len(matches) != 1:
            raise self._error_type(
                "compiled_valuation_output_ambiguous",
                f"exactly one of {semantic_ids!r} must exist for FY{period}",
            )
        return matches[0]


def _component(
    key: str,
    value: Decimal,
    unit: str,
    *,
    fiscal_year: int | None = None,
) -> CompiledValuationComponent:
    return CompiledValuationComponent(
        key=key,
        value=value,
        unit=unit,  # type: ignore[arg-type]
        fiscal_year=fiscal_year,
    )


def _method_limitations(method: str, *reasons: str) -> tuple[str, ...]:
    return tuple(
        sorted(set(_ALWAYS_DISCLOSED_LIMITATIONS.get(method, ()) + reasons))
    )


def _method_output(
    method: str,
    calculate: Callable[
        [], tuple[CompiledPriceCases, tuple[CompiledValuationComponent, ...]]
    ],
    *,
    unavailable_reason: str | None = None,
    error_type: type[Exception],
) -> CompiledValuationMethodOutput:
    if unavailable_reason is not None:
        return CompiledValuationMethodOutput(
            method=method,  # type: ignore[arg-type]
            status="unavailable",
            limitations=_method_limitations(method, unavailable_reason),
        )
    try:
        prices, components = calculate()
    except (error_type, ValueError, ArithmeticError) as exc:
        return CompiledValuationMethodOutput(
            method=method,  # type: ignore[arg-type]
            status="invalid",
            limitations=_method_limitations(method, f"{type(exc).__name__}: {exc}"),
        )
    return CompiledValuationMethodOutput(
        method=method,  # type: ignore[arg-type]
        status="available",
        prices=prices,
        components=tuple(sorted(components, key=lambda row: row.key)),
        limitations=_method_limitations(method),
    )


def _dcf_prices(
    *,
    method: DcfMethod,
    cases: Mapping[str, Decimal],
    pv_cash_flows: Decimal,
    terminal_fcf: Decimal,
    terminal_discount_period: Decimal,
    wacc: Decimal,
    net_debt: Decimal,
    shares: Decimal,
    terminal_year: int,
    exit_earnings: Decimal | None = None,
    exit_earnings_measure: ExitEarningsMeasure = "terminal_fcf",
) -> tuple[CompiledPriceCases, tuple[CompiledValuationComponent, ...]]:
    prices = dcf_case_prices(
        method=method,
        cases={case: cases[case] for case in ("bull", "base", "bear")},
        pv_cash_flows=pv_cash_flows,
        terminal_fcf=terminal_fcf,
        terminal_discount_period=terminal_discount_period,
        wacc=wacc,
        net_debt=net_debt,
        shares=shares,
        exit_earnings=exit_earnings,
        exit_earnings_measure=exit_earnings_measure,
    )
    uses_declared_exit = (
        method == "dcf_exit_multiple" and exit_earnings_measure != "terminal_fcf"
    )
    earnings_key = (
        f"exit_earnings_{exit_earnings_measure}"
        if uses_declared_exit
        else "terminal_free_cash_flow"
    )
    earnings_value = exit_earnings if uses_declared_exit else terminal_fcf
    components = (
        _component("corporate_net_debt", net_debt, "usd_millions"),
        _component("diluted_shares", shares, "count"),
        _component("pv_explicit_cash_flows", pv_cash_flows, "usd_millions"),
        _component("terminal_discount_period", terminal_discount_period, "count"),
        _component(
            earnings_key,
            earnings_value if earnings_value is not None else terminal_fcf,
            "usd_millions",
            fiscal_year=terminal_year,
        ),
        _component("wacc", wacc, "ratio"),
    )
    return CompiledPriceCases(
        bull=prices["bull"],
        base=prices["base"],
        bear=prices["bear"],
    ), components


def _driver_sensitivities(
    artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts,
) -> tuple[CompiledDriverSensitivity, ...]:
    """Rank the judgment rows the compiled cells cite by the DCF price's response."""

    model = artifacts.semantic_document.model.model_copy(deep=True)
    drivers = judgment_drivers(
        model,
        artifacts.provenance,
        forecast_assumption_drivers={
            str(row.assumption_id): row.driver
            for row in artifacts.model_spec.forecast_assumptions
            if row.assumption_id
        },
    )
    return tuple(
        CompiledDriverSensitivity(
            driver_key=row.driver_key,
            subject=row.subject,
            item_ids=row.item_ids,
            delta_per_pct=Decimal(repr(row.delta_per_pct)),
            rank=row.rank,
        )
        for row in sensitize(model, drivers)
        if row.rank is not None and row.delta_per_pct is not None
    )


def _require_compiled_price_match(
    *, method: str, derived: Decimal, compiled: Decimal, error_type: type[Exception]
) -> None:
    canonical_compiled = Decimal(compiled).quantize(
        _PRICE_QUANTUM, rounding=ROUND_HALF_EVEN
    )
    if Decimal(derived) != canonical_compiled:
        raise error_type(
            "compiled_valuation_reconciliation_mismatch",
            f"derived {method} base price differs from exact compiled output",
        )


def _stock_observation(policy: ValuationPolicyArtifact):
    return {row.input_name: row for row in policy.observed_inputs}.get("stock_price")


def _known_components(
    index: _SemanticIndex,
    rows: tuple[tuple[str, str, str], ...],
    *,
    period: int,
    fiscal_year: int | None,
) -> tuple[CompiledValuationComponent, ...]:
    """The components whose compiled output the version holds, by key."""

    components = []
    for key, semantic_id, unit in rows:
        value = index.known(semantic_id, period)
        if value is not None:
            components.append(_component(key, value, unit, fiscal_year=fiscal_year))
    return tuple(sorted(components, key=lambda row: row.key))


def _compiled_valuation_findings(
    *,
    artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts,
    valuation_policy: ValuationPolicyArtifact,
    snapshot: CompiledValuationSnapshot,
    error_type: type[Exception],
) -> tuple[Finding, ...]:
    """What the analyst sees beside a compiled valuation, never a refusal.

    The priced version is not a publishable build; the priced policy is not
    accepted; the compiled current price differs from the policy's price
    Reference; a method's compiled cases do not run bull > base > bear
    (``scenario_case_order_inverted``). Each is a declinable warning on the
    postcompile context.
    """

    policy = ValuationPolicyArtifact.model_validate_json(
        valuation_policy.model_dump_json()
    )
    version = artifacts.model_version
    findings: list[Finding] = []
    for method in snapshot.methods:
        prices = method.prices
        if prices is not None and not prices.bull > prices.base > prices.bear:
            findings.append(
                make_finding(
                    "scenario_case_order_inverted",
                    message=(
                        f"{method.method} compiles bull {prices.bull}, base "
                        f"{prices.base}, bear {prices.bear}; the cases do not run "
                        "bull > base > bear."
                    ),
                    item_id=method.method,
                    observed={
                        "bull": str(prices.bull),
                        "base": str(prices.base),
                        "bear": str(prices.bear),
                    },
                )
            )
    if version.status not in PUBLISHABLE_MODEL_STATUSES:
        findings.append(
            make_finding(
                "postcompile_model_version_not_ready",
                message=(
                    f"the priced ModelVersion {version.model_id} v{version.version} "
                    f"is {version.status}, not a publishable build"
                ),
                item_id=f"{version.model_id}:{version.version}",
                observed={"status": version.status},
            )
        )
    if policy.status != "accepted" or policy.stage_receipt.status != "accepted":
        findings.append(
            make_finding(
                "postcompile_valuation_policy_not_accepted",
                message=f"the priced ValuationPolicy {policy.ref.policy_id} is not accepted",
                item_id=policy.ref.policy_id,
                observed={
                    "status": policy.status,
                    "stage_receipt_status": policy.stage_receipt.status,
                },
            )
        )
    stock = _stock_observation(policy)
    forecast_periods = artifacts.model_spec.fiscal_axis.forecast_periods
    if stock is not None and forecast_periods:
        first_year = forecast_periods[0].fiscal_year
        compiled = _SemanticIndex(artifacts, error_type=error_type).known(
            "tpl.v.current_valuation.stock_price", first_year
        )
        if compiled is not None and compiled != Decimal(stock.value):
            findings.append(
                make_finding(
                    "postcompile_current_price_mismatch",
                    message=(
                        f"the compiled current price {compiled} differs from the "
                        f"ValuationPolicy's price Reference {stock.value}"
                    ),
                    item_id="tpl.v.current_valuation.stock_price",
                    periods=(first_year,),
                    observed={"compiled": str(compiled)},
                    expected={"policy": str(stock.value)},
                )
            )
    return tuple(findings)


def _derive_compiled_valuation_snapshot(
    *,
    artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts,
    valuation_policy: ValuationPolicyArtifact,
    error_type: type[Exception],
) -> CompiledValuationSnapshot:
    if type(artifacts) not in (ExactModelVersionArtifacts, VerifiedModelVersionArtifacts):
        raise TypeError("artifacts must be verified ModelVersion artifacts")
    policy = ValuationPolicyArtifact.model_validate_json(
        valuation_policy.model_dump_json()
    )
    version = artifacts.model_version
    spec = artifacts.model_spec
    # Readiness, acceptance and the price Reference are the analyst's to see
    # (``compiled_valuation_findings``); stored statuses that disagree with
    # each other are corruption. The policy is the one given: the context door
    # loads it by the version's own ``valuation_policy_binding``.
    if (
        artifacts.validation_report.status != version.status
        or artifacts.semantic_document.validation_status != version.status
    ):
        raise error_type(
            "postcompile_model_version_integrity_failed",
            "the ModelVersion's stored validation statuses disagree",
        )

    years = tuple(period.fiscal_year for period in spec.fiscal_axis.forecast_periods)
    first_year, terminal_year = years[0], years[-1]
    index = _SemanticIndex(artifacts, error_type=error_type)
    stock = _stock_observation(policy)
    if stock is None:
        raise error_type(
            "postcompile_current_price_missing",
            "the ValuationPolicy has no exact stock-price observation",
        )
    authorizations = {row.method: row for row in spec.valuation_family_authorizations}
    ev_auth = authorizations.get("ev_adjusted_ebitda")

    # Every compiled output is read where a method needs it: a missing,
    # invalid or ambiguous output fails the methods that read it, with its
    # limitation, and the snapshot still issues; the component lists carry
    # the outputs the version holds.
    exact_zero_debt = (
        index.known(_WACC_IDS["debt_value"], first_year) == 0
        and index.known(_WACC_IDS["debt_weight"], first_year) == 0
    )
    wacc_components = _known_components(
        index,
        tuple(
            (key, semantic_id, _WACC_UNITS[key])
            for key, semantic_id in _WACC_IDS.items()
            if not (exact_zero_debt and key in _ZERO_DEBT_INAPPLICABLE_WACC_KEYS)
        ),
        period=first_year,
        fiscal_year=first_year,
    )
    bridge = _known_components(
        index,
        (
            ("corporate_net_debt", "tpl.v.dcf.less_net_debt", "usd_millions"),
            ("dcf_diluted_shares", "tpl.v.dcf.shares_outstanding", "count"),
            *(
                ()
                if ev_auth is None
                else (
                    (
                        "relative_corporate_net_debt",
                        "tpl.v.forward_ev_ebitda.method_corporate_net_debt",
                        "usd_millions",
                    ),
                )
            ),
        ),
        period=first_year,
        fiscal_year=None,
    )

    def dcf_output(
        method: DcfMethod, case_prefix: str
    ) -> tuple[CompiledPriceCases, tuple[CompiledValuationComponent, ...]]:
        measure = _exit_earnings_measure(policy) if method == "dcf_exit_multiple" else "terminal_fcf"
        exit_earnings = (
            None
            if measure == "terminal_fcf"
            else index.value(EXIT_EARNINGS_ITEMS[measure], terminal_year)
        )
        return _dcf_prices(
            method=method,
            cases={
                case: index.value(f"tpl.v.dcf.{case_prefix}_{case}", first_year)
                for case in ("bull", "base", "bear")
            },
            pv_cash_flows=index.value("tpl.v.dcf.pv_cash_flows", first_year),
            terminal_fcf=index.value("tpl.v.dcf.terminal_year_fcf", first_year),
            terminal_discount_period=index.value(
                "tpl.v.dcf.discount_period", terminal_year
            ),
            wacc=index.value(_WACC_IDS["wacc"], first_year),
            net_debt=index.value("tpl.v.dcf.less_net_debt", first_year),
            shares=index.value("tpl.v.dcf.shares_outstanding", first_year),
            terminal_year=terminal_year,
            exit_earnings=exit_earnings,
            exit_earnings_measure=measure,
        )

    def terminal_growth_output() -> tuple[
        CompiledPriceCases, tuple[CompiledValuationComponent, ...]
    ]:
        prices, components = dcf_output("dcf_terminal_growth", "terminal_growth")
        _require_compiled_price_match(
            method="dcf_terminal_growth",
            error_type=error_type,
            derived=prices.base,
            compiled=index.value("tpl.v.dcf.equity_value_per_share", first_year),
        )
        return prices, components

    methods: list[CompiledValuationMethodOutput] = [
        _method_output(
            "dcf_exit_multiple",
            lambda: dcf_output("dcf_exit_multiple", "exit_multiple"),
            error_type=error_type,
        ),
        _method_output("dcf_terminal_growth", terminal_growth_output, error_type=error_type),
    ]

    def ev_output() -> tuple[
        CompiledPriceCases, tuple[CompiledValuationComponent, ...]
    ]:
        assert ev_auth is not None
        relative_net_debt = index.value(
            "tpl.v.forward_ev_ebitda.method_corporate_net_debt", first_year
        )
        ebitda = index.value("tpl.fm.adjusted_earnings.adjusted_ebitda", first_year)
        relative_shares = index.first_available(_SHARE_IDS, first_year)
        prices = {
            case: (Decimal(getattr(ev_auth, case)) * ebitda - relative_net_debt)
            / relative_shares
            for case in ("bull", "base", "bear")
        }
        price_cases = CompiledPriceCases(
            bull=prices["bull"],
            base=prices["base"],
            bear=prices["bear"],
        )
        _require_compiled_price_match(
            method="ev_adjusted_ebitda",
            error_type=error_type,
            derived=price_cases.base,
            compiled=index.value(
                "tpl.v.forward_ev_ebitda.method_price", first_year
            ),
        )
        return price_cases, (
            _component(
                "adjusted_ebitda", ebitda, "usd_millions", fiscal_year=first_year
            ),
            _component("corporate_net_debt", relative_net_debt, "usd_millions"),
            _component("diluted_shares", relative_shares, "count"),
        )

    methods.append(
        _method_output(
            "ev_adjusted_ebitda",
            ev_output,
            error_type=error_type,
            unavailable_reason=(
                "ValuationPolicy did not authorize EV/adjusted EBITDA"
                if ev_auth is None
                else None
            ),
        )
    )
    pe_auth = authorizations.get("forward_pe")

    def pe_output() -> tuple[
        CompiledPriceCases, tuple[CompiledValuationComponent, ...]
    ]:
        assert pe_auth is not None
        prices = {
            case: index.value(
                f"tpl.s.valuation_scenarios.{case}_pe_valuation", first_year
            )
            for case in ("bull", "base", "bear")
        }
        return CompiledPriceCases(
            bull=prices["bull"],
            base=prices["base"],
            bear=prices["bear"],
        ), tuple(
            _component(
                f"{case}_selected_multiple",
                Decimal(getattr(pe_auth, case)),
                "multiple",
                fiscal_year=first_year,
            )
            for case in ("bear", "base", "bull")
        )

    methods.append(
        _method_output(
            "forward_pe",
            pe_output,
            error_type=error_type,
            unavailable_reason=(
                "ValuationPolicy did not authorize forward P/E"
                if pe_auth is None
                else None
            ),
        )
    )
    methods.sort(key=lambda row: row.method)

    payload: dict[str, object] = {
        "schema_version": "compiled-valuation-snapshot.v1",
        "model_version": _exact_model_version_binding(
            version,
            source_bundle_id=artifacts.source_bundle_lineage.bundle_id,
            source_bundle_identity_digest=(
                artifacts.source_bundle_lineage.identity_digest
            ),
        ),
        "valuation_policy_ref": policy.ref,
        "valuation_policy_stage_receipt_digest": policy.stage_receipt.receipt_digest,
        "scenario_set_ref": policy.scenario_set_ref,
        "current_price": CompiledCurrentPrice(
            value=stock.value,
            source_id=stock.source_id,
            provider=stock.provider,
            as_of_date=stock.as_of_date,
            retrieved_at=stock.retrieved_at,
            observation_id=stock.observation_id,
            observation_content_sha256=stock.content_sha256,
        ),
        "first_forecast_year": first_year,
        "terminal_forecast_year": terminal_year,
        "wacc": wacc_components,
        "enterprise_to_equity_bridge": bridge,
        "methods": tuple(methods),
    }
    sensitivities = _driver_sensitivities(artifacts)
    if sensitivities:
        payload["driver_sensitivities"] = sensitivities
    return CompiledValuationSnapshot.model_validate(
        {**payload, "snapshot_sha256": compiled_valuation_snapshot_digest(payload)}
    )


@dataclass(frozen=True)
class PostcompileBindingRead(Generic[_Commit]):
    model_record: ModelRecord
    commit: _Commit
    snapshot: CompiledValuationSnapshot | None
    valuation_policy: ValuationPolicyArtifact | None
    findings: tuple[Finding, ...] = ()

def read_postcompile_binding(
    *,
    commit: _Commit,
    artifacts: ExactModelVersionArtifacts | VerifiedModelVersionArtifacts,
    valuation_policy: ValuationPolicyArtifact | None,
    error_type: type[Exception],
) -> PostcompileBindingRead[_Commit]:
    """Derive exact compiled authority from one acquired artifact/policy snapshot."""
    if valuation_policy is None:
        return PostcompileBindingRead(commit.record, commit, None, None)
    snapshot = _derive_compiled_valuation_snapshot(
        artifacts=artifacts, valuation_policy=valuation_policy, error_type=error_type,
    )
    return PostcompileBindingRead(
        model_record=commit.record, commit=commit, snapshot=snapshot,
        valuation_policy=valuation_policy,
        findings=_compiled_valuation_findings(
            artifacts=artifacts, valuation_policy=valuation_policy,
            snapshot=snapshot, error_type=error_type,
        ),
    )
