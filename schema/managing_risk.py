"""The stored typed outputs of a managing-risk sidecar.

The managing-risk door (``api/fms/modules/managing_risk``) constructs this model
from the admitted plan and persists its ``model_dump``; the agent never submits
it. A construction that does not validate is the door's defect, raised with the
field named, never a demand on the agent.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExpectedValueCheckpoint(_StoredContract):
    """The latest ``expected-value-decision`` artifact the door read, projected to what sizing consumes."""

    artifact_id: str = Field(min_length=1)
    expected_return_pct: float
    artifact_ref: str | None = None
    expected_price: float | None = None
    return_to_risk: float | None = None
    scenario_target_horizon: str | None = None
    materiality_verdict: str | None = None


class ManagingRiskOutputs(_StoredContract):
    recommendation_package: dict[str, JsonValue]
    inputs_loaded: tuple[JsonValue, ...] = ()
    expected_value_checkpoint: ExpectedValueCheckpoint | None = None
    sizing: dict[str, JsonValue]
    position_size_preview: dict[str, JsonValue]
    portfolio_fit: dict[str, JsonValue]
    sell_criteria: dict[str, JsonValue]
    state_discrepancies: tuple[JsonValue, ...] = ()
    sources: tuple[dict[str, JsonValue], ...] = ()
    data_gaps: tuple[dict[str, JsonValue], ...] = ()
    withheld_triggers: tuple[dict[str, JsonValue], ...] = ()
    citation_degradation_warnings: tuple[dict[str, JsonValue], ...] = ()
    #: What the door noticed admitting and constructing the plan: ``{index?, field, code, message, next_call}``.
    findings: tuple[dict[str, JsonValue], ...] = ()


__all__ = ["ExpectedValueCheckpoint", "ManagingRiskOutputs"]
