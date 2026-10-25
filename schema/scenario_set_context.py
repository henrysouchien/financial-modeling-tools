"""Bounded exact-ref context for precompile ScenarioSet judgment.

Consumed by ``api/research/scenario_set_context_service.py``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .forecast_assumption_set import ForecastAssumptionSetRef
from .scenario_set import (
  CriticalFactorsArtifactRef,
  ScenarioCaseProbabilities,
  ScenarioCriticalFactor,
  ScenarioDriverCurve,
  ScenarioGap,
  ScenarioMateriality,
  ScenarioSetRef,
  ScenarioTerminalSummary,
)
from .stable_control_identity import FiscalAxis
from .finding_vocabulary import Finding
from .versioned_artifact import canonical_json_bytes


MAX_SCENARIO_DRIVERS = 512
MAX_CRITICAL_FACTORS = 2


class _Contract(BaseModel):
  model_config = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    str_strip_whitespace=True,
  )


def scenario_set_context_digest(
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
  forecast_assumption_set_ref: ForecastAssumptionSetRef | None,
  forecast_assumption_stage_receipt_digest: str | None,
  critical_factors_ref: CriticalFactorsArtifactRef | None,
) -> str:
  payload = {
    "schema_version": "scenario-set-context.v1",
    "user_id": user_id,
    "research_file_id": research_file_id,
    "ticker": ticker,
    "forecast_assumption_set_ref": forecast_assumption_set_ref,
    "forecast_assumption_stage_receipt_digest": (
      forecast_assumption_stage_receipt_digest
    ),
    "critical_factors_ref": critical_factors_ref,
  }
  return hashlib.sha256(
    b"scenario-set-context.v1\0" + canonical_json_bytes(payload)
  ).hexdigest()


class ScenarioSetContextBinding(_Contract):
  schema_version: Literal["scenario-set-context-binding.v1"] = (
    "scenario-set-context-binding.v1"
  )
  user_id: str = Field(min_length=1)
  research_file_id: int = Field(gt=0)
  ticker: str = Field(min_length=1, max_length=32)
  # ``None`` when the file has no accepted ForecastAssumptionSet / no
  # critical-factors artifact yet: the context is issued with that gap.
  forecast_assumption_set_ref: ForecastAssumptionSetRef | None
  forecast_assumption_stage_receipt_digest: str | None = Field(
    pattern=r"^[0-9a-f]{64}$"
  )
  critical_factors_ref: CriticalFactorsArtifactRef | None
  context_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

  @model_validator(mode="after")
  def _digest(self) -> "ScenarioSetContextBinding":
    expected = scenario_set_context_digest(
      user_id=self.user_id,
      research_file_id=self.research_file_id,
      ticker=self.ticker,
      forecast_assumption_set_ref=self.forecast_assumption_set_ref,
      forecast_assumption_stage_receipt_digest=(
        self.forecast_assumption_stage_receipt_digest
      ),
      critical_factors_ref=self.critical_factors_ref,
    )
    if self.context_content_sha256 != expected:
      raise ValueError("scenario context binding digest mismatch")
    return self


def encode_scenario_set_context_ref(binding: ScenarioSetContextBinding) -> str:
  payload = canonical_json_bytes(binding.model_dump(mode="json"))
  encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
  checksum = hashlib.sha256(b"scenario-set-context-ref.v1\0" + payload).hexdigest()
  return f"scx1.{encoded}.{checksum}"


def decode_scenario_set_context_ref(value: str) -> ScenarioSetContextBinding:
  try:
    prefix, encoded, checksum = value.split(".", 2)
    if prefix != "scx1" or len(checksum) != 64:
      raise ValueError
    payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    expected = hashlib.sha256(
      b"scenario-set-context-ref.v1\0" + payload
    ).hexdigest()
    if not hmac.compare_digest(checksum, expected):
      raise ValueError
    return ScenarioSetContextBinding.model_validate_json(payload)
  except (TypeError, ValueError) as exc:
    raise ValueError("scenario context ref is invalid") from exc


class ScenarioContextFactor(_Contract):
  factor_key: str = Field(min_length=1, max_length=256)
  label: str = Field(min_length=1, max_length=512)
  mechanism: str = Field(min_length=1, max_length=4096)
  # Below three is reported (``critical_factors_unqualified``), not refused.
  forecastability_score: int = Field(ge=1, le=5)


class ScenarioContextDriver(_Contract):
  driver_key: str = Field(min_length=1, max_length=256)
  base_values: dict[int, float]
  unit: str = Field(min_length=1, max_length=64)
  percentage_value_space: Literal["fraction", "percentage_points"] | None = None
  base_evidence_count: int = Field(ge=0)
  classification: Literal["critical", "supporting", "held_at_base"]

  @field_validator("base_values")
  @classmethod
  def _values(cls, value: dict[int, float]) -> dict[int, float]:
    if not value or any(year <= 0 for year in value):
      raise ValueError("scenario base values require positive forecast years")
    return value


class ScenarioContextHeader(_Contract):
  schema_version: Literal["scenario-set-context.v1"] = "scenario-set-context.v1"
  research_file_id: int = Field(gt=0)
  ticker: str = Field(min_length=1, max_length=32)
  context_ref: str = Field(min_length=1)
  # Measured, never bounded: the issued token's length, derived below.
  context_ref_length: int = 0
  forecast_assumption_set_ref: ForecastAssumptionSetRef | None
  critical_factors_ref: CriticalFactorsArtifactRef | None
  fiscal_axis: FiscalAxis | None

  @model_validator(mode="after")
  def _measure_context_ref(self) -> "ScenarioContextHeader":
    object.__setattr__(self, "context_ref_length", len(self.context_ref))
    return self


class ScenarioContextPredecessorCurve(_Contract):
  """One driver's accepted bull/bear curve in the predecessor set, values
  only; the driver lookup returns the whole curve as ``predecessor``."""

  driver_key: str = Field(min_length=1, max_length=256)
  critical_factor_key: str | None = None
  role: Literal["selected_factor", "amplifier_channel", "held_at_base"]
  direction: Literal["higher_is_bull", "lower_is_bull", "unchanged"]
  bull_values: dict[int, float] | None
  bear_values: dict[int, float] | None


class ScenarioContextPredecessor(_Contract):
  """The accepted ScenarioSet a new write on this context succeeds (the
  current accepted set on this ForecastAssumptionSet, else the one on the set
  it succeeds). Read to carry its judgment forward on a re-accept."""

  scenario_set_ref: ScenarioSetRef
  forecast_assumption_set_ref: ForecastAssumptionSetRef
  critical_factors: tuple[ScenarioCriticalFactor, ...]
  materiality: ScenarioMateriality
  case_probabilities: ScenarioCaseProbabilities | None
  curves: tuple[ScenarioContextPredecessorCurve, ...] = Field(
    max_length=MAX_SCENARIO_DRIVERS
  )
  gaps: tuple[ScenarioGap, ...]
  terminal_summary: ScenarioTerminalSummary


class ScenarioContextSummary(_Contract):
  kind: Literal["summary"] = "summary"
  # Complete factors remain visible while incomplete judgment is reported in
  # Findings; materiality is absent when its threshold or rationale is unresolved.
  critical_factors: tuple[ScenarioContextFactor, ...] = Field(
    max_length=MAX_CRITICAL_FACTORS,
  )
  materiality: ScenarioMateriality | None
  drivers: tuple[ScenarioContextDriver, ...] = Field(
    max_length=MAX_SCENARIO_DRIVERS,
  )
  predecessor: ScenarioContextPredecessor | None = Field(
    default=None, exclude_if=lambda value: value is None
  )

  @model_validator(mode="after")
  def _canonical(self) -> "ScenarioContextSummary":
    factor_keys = tuple(row.factor_key for row in self.critical_factors)
    driver_keys = tuple(row.driver_key for row in self.drivers)
    if factor_keys != tuple(sorted(set(factor_keys))):
      raise ValueError("scenario factors must be sorted and unique")
    if driver_keys != tuple(sorted(set(driver_keys))):
      raise ValueError("scenario drivers must be sorted and unique")
    return self


class ScenarioContextDriverDetail(_Contract):
  kind: Literal["driver"] = "driver"
  driver: ScenarioContextDriver
  candidate_factor_keys: tuple[str, ...]
  mechanism: str = Field(min_length=1, max_length=4096)
  base_rate_anchor: str = Field(min_length=1, max_length=4096)
  revision_trigger: str = Field(min_length=1, max_length=4096)
  #: The predecessor set's accepted curve for this driver, when it has one.
  predecessor: ScenarioDriverCurve | None = Field(
    default=None, exclude_if=lambda value: value is None
  )

  @field_validator("candidate_factor_keys")
  @classmethod
  def _factors(cls, value: tuple[str, ...]) -> tuple[str, ...]:
    if value != tuple(sorted(set(value))):
      raise ValueError("candidate factor keys must be sorted and unique")
    return value


ScenarioContextProjection = Annotated[
  ScenarioContextSummary | ScenarioContextDriverDetail,
  Field(discriminator="kind"),
]


class ScenarioSetContextResponse(_Contract):
  header: ScenarioContextHeader
  context_ref: str | None = Field(default=None, exclude_if=lambda value: value is None)
  judgment_skeleton: dict[str, Any] | None = Field(
    default=None, exclude_if=lambda value: value is None
  )
  projection: ScenarioContextProjection
  #: Requested driver details in item_keys order; absent on summary/singular reads.
  items: tuple[ScenarioContextDriverDetail, ...] | None = Field(
    default=None, exclude_if=lambda value: value is None
  )
  #: What the door observed instead of refusing: an input not resolved yet,
  #: the FAS not accepted, the critical-factors judgment below the
  #: methodology, what the accepted set shows against its FAS.
  findings: tuple[Finding, ...] = ()
  #: The producer of the first unresolved input or critical-factors judgment.
  next_call: dict[str, Any] | None = Field(
    default=None, exclude_if=lambda value: value is None
  )

  @model_validator(mode="after")
  def _top_level_context_ref(self) -> "ScenarioSetContextResponse":
    if self.context_ref is None:
      object.__setattr__(self, "context_ref", self.header.context_ref)
    return self


__all__ = [
  "ScenarioContextDriver",
  "ScenarioContextDriverDetail",
  "ScenarioContextFactor",
  "ScenarioContextHeader",
  "ScenarioContextPredecessor",
  "ScenarioContextPredecessorCurve",
  "ScenarioContextProjection",
  "ScenarioContextSummary",
  "ScenarioSetContextBinding",
  "ScenarioSetContextResponse",
  "decode_scenario_set_context_ref",
  "encode_scenario_set_context_ref",
  "scenario_set_context_digest",
]
