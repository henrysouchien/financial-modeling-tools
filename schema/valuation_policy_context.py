"""Bounded exact-ref context for precompile ValuationPolicy judgment."""

from __future__ import annotations

import base64
from datetime import date
from decimal import Decimal
import hashlib
import hmac
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .build_model_product_contracts import RelativeSelectionResolution
from .finding_vocabulary import Finding
from .forecast_assumption_set import ForecastAssumptionSetRef
from .model_spec_authoring import (
  ModelValuationPolicyAuthoring,
  ValuationFamilyAuthorizationAuthoring,
)
from .scenario_set import ScenarioSetRef
from .valuation_policy import (
  ValuationNetShareDilution,
  ValuationPolicyGap,
  ValuationPolicyInput,
  ValuationPolicyRef,
  ValuationPolicyTerminalSummary,
  ValuationTerminalPolicy,
)
from .stable_control_identity import FiscalAxis
from .versioned_artifact import canonical_json_bytes


EXTERNAL_SOFR_HANDLE_ID_PATTERN = r"^h_[0-9a-f]{12,64}$"
MAX_VALUATION_OBSERVATIONS = 256
MAX_SCENARIO_CURVES = 512


class _Contract(BaseModel):
  model_config = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    str_strip_whitespace=True,
  )


def valuation_policy_context_digest(
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
  forecast_assumption_set_ref: ForecastAssumptionSetRef | None,
  forecast_assumption_stage_receipt_digest: str | None,
  scenario_set_ref: ScenarioSetRef | None,
  scenario_stage_receipt_digest: str | None,
  valuation_evidence_snapshot_sha256: str | None,
  external_sofr_handle_id: str | None,
) -> str:
  payload = {
    "schema_version": "valuation-policy-context.v1",
    "user_id": user_id,
    "research_file_id": research_file_id,
    "ticker": ticker,
    "forecast_assumption_set_ref": forecast_assumption_set_ref,
    "forecast_assumption_stage_receipt_digest": (
      forecast_assumption_stage_receipt_digest
    ),
    "scenario_set_ref": scenario_set_ref,
    "scenario_stage_receipt_digest": scenario_stage_receipt_digest,
    "valuation_evidence_snapshot_sha256": valuation_evidence_snapshot_sha256,
    "external_sofr_handle_id": external_sofr_handle_id,
  }
  return hashlib.sha256(
    b"valuation-policy-context.v1\0" + canonical_json_bytes(payload)
  ).hexdigest()


class ValuationPolicyContextBinding(_Contract):
  """What one issued context is bound to. An upstream the context could not
  resolve (no accepted ForecastAssumptionSet or ScenarioSet, an evidence
  snapshot that did not derive) is ``None`` here and a
  ``valuation_context_upstream_unresolved`` Finding on the response."""

  schema_version: Literal["valuation-policy-context-binding.v1"] = (
    "valuation-policy-context-binding.v1"
  )
  user_id: str = Field(min_length=1)
  research_file_id: int = Field(gt=0)
  ticker: str = Field(min_length=1, max_length=32)
  forecast_assumption_set_ref: ForecastAssumptionSetRef | None = None
  forecast_assumption_stage_receipt_digest: str | None = Field(
    default=None, pattern=r"^[0-9a-f]{64}$"
  )
  scenario_set_ref: ScenarioSetRef | None = None
  scenario_stage_receipt_digest: str | None = Field(
    default=None, pattern=r"^[0-9a-f]{64}$"
  )
  valuation_evidence_snapshot_sha256: str | None = Field(
    default=None, pattern=r"^[0-9a-f]{64}$"
  )
  external_sofr_handle_id: str | None = Field(
    default=None, pattern=EXTERNAL_SOFR_HANDLE_ID_PATTERN
  )
  context_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

  @model_validator(mode="after")
  def _digest(self) -> "ValuationPolicyContextBinding":
    expected = valuation_policy_context_digest(
      user_id=self.user_id,
      research_file_id=self.research_file_id,
      ticker=self.ticker,
      forecast_assumption_set_ref=self.forecast_assumption_set_ref,
      forecast_assumption_stage_receipt_digest=(
        self.forecast_assumption_stage_receipt_digest
      ),
      scenario_set_ref=self.scenario_set_ref,
      scenario_stage_receipt_digest=self.scenario_stage_receipt_digest,
      valuation_evidence_snapshot_sha256=(
        self.valuation_evidence_snapshot_sha256
      ),
      external_sofr_handle_id=self.external_sofr_handle_id,
    )
    if self.context_content_sha256 != expected:
      raise ValueError("valuation-policy context binding digest mismatch")
    return self


def encode_valuation_policy_context_ref(
  binding: ValuationPolicyContextBinding,
) -> str:
  payload = canonical_json_bytes(binding.model_dump(mode="json"))
  encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
  checksum = hashlib.sha256(
    b"valuation-policy-context-ref.v1\0" + payload
  ).hexdigest()
  return f"vpcx1.{encoded}.{checksum}"


def decode_valuation_policy_context_ref(
  value: str,
) -> ValuationPolicyContextBinding:
  try:
    prefix, encoded, checksum = value.split(".", 2)
    if prefix != "vpcx1" or len(checksum) != 64:
      raise ValueError
    payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    expected = hashlib.sha256(
      b"valuation-policy-context-ref.v1\0" + payload
    ).hexdigest()
    if not hmac.compare_digest(checksum, expected):
      raise ValueError
    return ValuationPolicyContextBinding.model_validate_json(payload)
  except (TypeError, ValueError) as exc:
    raise ValueError("valuation-policy context ref is invalid") from exc


class ValuationObservedInputProjection(_Contract):
  input_name: Literal[
    "stock_price",
    "raw_beta",
    "risk_free_rate",
    "equity_risk_premium",
    "sofr_rate",
  ]
  value: Decimal
  unit: Literal["usd_per_share", "ratio"]
  as_of_date: date
  provider: str = Field(min_length=1, max_length=128)
  evidence_key: str = Field(min_length=1, max_length=512)


class ValuationScenarioCurveProjection(_Contract):
  driver_key: str = Field(min_length=1, max_length=256)
  unit: str = Field(min_length=1, max_length=64)
  terminal_period_id: str = Field(min_length=1, max_length=64)
  bull: Decimal
  base: Decimal
  bear: Decimal


class ValuationObservationProjection(_Contract):
  evidence_key: str = Field(min_length=1, max_length=512)
  issuer: str | None = Field(default=None, max_length=32)
  metric: str = Field(min_length=1, max_length=128)
  fiscal_basis: str = Field(min_length=1, max_length=64)
  value: Decimal
  unit: str = Field(min_length=1, max_length=64)
  as_of_date: date
  provider: str = Field(min_length=1, max_length=128)
  locator: str = Field(min_length=1, max_length=2048)


class ValuationObservationIndexEntry(_Contract):
  """Compact natural locator for one bounded observation detail read."""

  evidence_key: str = Field(min_length=1, max_length=512)
  issuer: str | None = Field(default=None, max_length=32)
  metric: str = Field(min_length=1, max_length=128)
  fiscal_basis: str = Field(min_length=1, max_length=64)
  unit: str = Field(min_length=1, max_length=64)
  as_of_date: date
  selection_eligible: bool
  rejection_reasons: tuple[str, ...] = ()

  @field_validator("rejection_reasons")
  @classmethod
  def _rejection_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
    if value != tuple(sorted(set(value))) or any(not item for item in value):
      raise ValueError("valuation observation rejection reasons must be canonical")
    return value

  @model_validator(mode="after")
  def _eligibility_shape(self) -> "ValuationObservationIndexEntry":
    if self.selection_eligible == bool(self.rejection_reasons):
      raise ValueError(
        "eligible valuation observations must exclusively omit rejection reasons"
      )
    return self


class ValuationCoverageShortfallProjection(_Contract):
  scope_key: str = Field(min_length=1, max_length=64)
  reason: str = Field(min_length=1, max_length=64)


class ValuationRequirementCoverageProjection(_Contract):
  """Code-owned coverage verdict for one immutable requirement."""

  requirement_id: str = Field(min_length=1, max_length=256)
  metric: str = Field(min_length=1, max_length=128)
  fiscal_basis: str = Field(min_length=1, max_length=64)
  status: Literal["covered", "uncovered"]
  evidence_keys: tuple[str, ...] = ()
  shortfall: tuple[ValuationCoverageShortfallProjection, ...] = ()

  @model_validator(mode="after")
  def _coverage_shape(self) -> "ValuationRequirementCoverageProjection":
    if self.evidence_keys != tuple(sorted(set(self.evidence_keys))):
      raise ValueError("valuation coverage evidence keys must be canonical")
    shortfall_keys = tuple((row.scope_key, row.reason) for row in self.shortfall)
    if shortfall_keys != tuple(sorted(set(shortfall_keys))):
      raise ValueError("valuation coverage shortfall must be canonical")
    if (self.status == "covered") != (not self.shortfall):
      raise ValueError("valuation coverage shortfall must be empty iff covered")
    return self


class ValuationRelativeSelectionProjection(_Contract):
  """The peer selection the ValuationPolicy door verifies one relative method against.

  `resolution` is `derive_relative_selection`'s own result on the door's
  selected fiscal basis; `evidence_keys` are its contributing ids as the
  observation keys this context issues.
  """

  resolution: RelativeSelectionResolution
  evidence_keys: tuple[str, ...] = ()

  @model_validator(mode="after")
  def _evidence_keys(self) -> "ValuationRelativeSelectionProjection":
    if self.evidence_keys != tuple(
      f"observation:{item}" for item in self.resolution.contributing_ids
    ):
      raise ValueError(
        "relative selection evidence keys must be its contributing observations"
      )
    return self


class ValuationHouseDefaultProjection(_Contract):
  """The house value an unauthored ValuationPolicy parameter compiles.

  `value` is a decimal rate; `basis_period_id` is the reported year it is read
  from and `convention` names the house rule, as `judgment_absent_default_used`
  names it when the compile writes the value `default`.
  """

  value: Decimal
  basis_period_id: str = Field(min_length=1, max_length=64)
  convention: str = Field(min_length=1, max_length=128)


class ValuationPolicyContextHeader(_Contract):
  schema_version: Literal["valuation-policy-context.v1"] = (
    "valuation-policy-context.v1"
  )
  research_file_id: int = Field(gt=0)
  ticker: str = Field(min_length=1, max_length=32)
  context_ref: str = Field(min_length=1)
  # Measured, never bounded: the issued token's length, derived below.
  context_ref_length: int = 0
  forecast_assumption_set_ref: ForecastAssumptionSetRef | None = None
  scenario_set_ref: ScenarioSetRef | None = None
  valuation_evidence_snapshot_sha256: str | None = Field(
    default=None, pattern=r"^[0-9a-f]{64}$"
  )
  fiscal_axis: FiscalAxis | None = None

  @model_validator(mode="after")
  def _measure_context_ref(self) -> "ValuationPolicyContextHeader":
    object.__setattr__(self, "context_ref_length", len(self.context_ref))
    return self


class ValuationPolicyContextPredecessor(_Contract):
  """The accepted ValuationPolicy a new write on this context succeeds (the
  current accepted policy on this FAS and ScenarioSet, else the one on the
  ScenarioSet this one succeeds): its authored judgment, read to carry it
  forward on a re-accept."""

  valuation_policy_ref: ValuationPolicyRef
  forecast_assumption_set_ref: ForecastAssumptionSetRef
  scenario_set_ref: ScenarioSetRef
  policy_inputs: tuple[ValuationPolicyInput, ...]
  terminal_policy: ValuationTerminalPolicy | None
  net_share_dilution: ValuationNetShareDilution | None
  model_policy: ModelValuationPolicyAuthoring | None
  family_authorizations: tuple[ValuationFamilyAuthorizationAuthoring, ...]
  gaps: tuple[ValuationPolicyGap, ...]
  terminal_summary: ValuationPolicyTerminalSummary


class ValuationPolicyContextSummary(_Contract):
  kind: Literal["summary"] = "summary"
  observed_inputs: tuple[ValuationObservedInputProjection, ...]
  unresolved_observed_input_names: tuple[str, ...]
  scenario_curves: tuple[ValuationScenarioCurveProjection, ...] = Field(
    max_length=MAX_SCENARIO_CURVES
  )
  peer_tickers: tuple[str, ...]
  available_observation_count: int = Field(ge=0, le=MAX_VALUATION_OBSERVATIONS)
  observation_index: tuple[ValuationObservationIndexEntry, ...] = Field(
    max_length=MAX_VALUATION_OBSERVATIONS
  )
  requirement_coverage: tuple[
    ValuationRequirementCoverageProjection, ...
  ] = Field(max_length=MAX_VALUATION_OBSERVATIONS)
  relative_selections: tuple[ValuationRelativeSelectionProjection, ...] = ()
  net_share_dilution_house_default: ValuationHouseDefaultProjection | None = None
  predecessor: ValuationPolicyContextPredecessor | None = Field(
    default=None, exclude_if=lambda value: value is None
  )

  @model_validator(mode="after")
  def _canonical(self) -> "ValuationPolicyContextSummary":
    if tuple(row.input_name for row in self.observed_inputs) != tuple(
      sorted({row.input_name for row in self.observed_inputs})
    ):
      raise ValueError("observed input projections must be sorted and unique")
    if self.unresolved_observed_input_names != tuple(
      sorted(set(self.unresolved_observed_input_names))
    ):
      raise ValueError("unresolved observed inputs must be canonical")
    if tuple(row.driver_key for row in self.scenario_curves) != tuple(
      sorted({row.driver_key for row in self.scenario_curves})
    ):
      raise ValueError("scenario curve projections must be sorted and unique")
    if self.peer_tickers != tuple(sorted(set(self.peer_tickers))):
      raise ValueError("peer tickers must be sorted and unique")
    observation_keys = tuple(row.evidence_key for row in self.observation_index)
    if observation_keys != tuple(sorted(set(observation_keys))):
      raise ValueError("valuation observation index must be sorted and unique")
    requirement_ids = tuple(row.requirement_id for row in self.requirement_coverage)
    if requirement_ids != tuple(sorted(set(requirement_ids))):
      raise ValueError("valuation requirement coverage must be sorted and unique")
    if self.available_observation_count != len(self.observation_index):
      raise ValueError("valuation observation count must match its exact index")
    return self


class ValuationPolicyObservationDetail(_Contract):
  kind: Literal["observation"] = "observation"
  observation: ValuationObservationProjection


ValuationPolicyContextProjection = Annotated[
  ValuationPolicyContextSummary | ValuationPolicyObservationDetail,
  Field(discriminator="kind"),
]


class ValuationPolicyContextResponse(_Contract):
  """One issued context. ``findings`` carries ``valuation_context_reissued``
  when a carried context_ref's authority moved and this is the current one,
  ``valuation_context_upstream_unresolved`` for each upstream the context
  could not resolve (its header field is then ``None``), and what the
  ScenarioSet shows against its FAS. ``next_call`` produces the first missing
  upstream."""

  header: ValuationPolicyContextHeader
  context_ref: str | None = Field(default=None, exclude_if=lambda value: value is None)
  judgment_skeleton: dict[str, Any] | None = Field(
    default=None, exclude_if=lambda value: value is None
  )
  projection: ValuationPolicyContextProjection
  findings: tuple[Finding, ...] = ()
  next_call: dict[str, Any] | None = Field(
    default=None, exclude_if=lambda value: value is None
  )

  @model_validator(mode="after")
  def _top_level_context_ref(self) -> "ValuationPolicyContextResponse":
    if self.context_ref is None:
      object.__setattr__(self, "context_ref", self.header.context_ref)
    return self


__all__ = [
  "EXTERNAL_SOFR_HANDLE_ID_PATTERN",
  "ValuationHouseDefaultProjection",
  "ValuationObservationProjection",
  "ValuationObservationIndexEntry",
  "ValuationObservedInputProjection",
  "ValuationCoverageShortfallProjection",
  "ValuationPolicyContextBinding",
  "ValuationPolicyContextHeader",
  "ValuationPolicyContextPredecessor",
  "ValuationPolicyContextProjection",
  "ValuationPolicyContextResponse",
  "ValuationPolicyContextSummary",
  "ValuationPolicyObservationDetail",
  "ValuationRequirementCoverageProjection",
  "ValuationRelativeSelectionProjection",
  "ValuationScenarioCurveProjection",
  "decode_valuation_policy_context_ref",
  "encode_valuation_policy_context_ref",
  "valuation_policy_context_digest",
]
