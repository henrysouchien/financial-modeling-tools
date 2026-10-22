"""The stored typed outputs of a thesis-articulation investment-memo report.

The report door (``fms_report_thesis_articulation``, built by
``api/fms/modules/thesis_articulation``) constructs this model from the admitted
judgment and persists its ``model_dump``; the agent never submits it. A
construction that does not validate is the door's defect, raised with the field
named, never a demand on the agent. ``api/research/workflow_output_compose.py``
composes the circulating HTML memo from the persisted dump.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


ThesisArticulationVerdict = Literal["PITCH_READY", "THESIS_INSUFFICIENT", "INSUFFICIENT_DATA"]
WritebackState = Literal["staged", "not_requested"]


class MemoLifecycle(_StoredContract):
    """What the report door recorded about this memo: its door, writeback and circulation."""

    fms_door: Literal["report_thesis_articulation"]
    writeback_state: WritebackState
    compose_eligible: bool
    verdict: ThesisArticulationVerdict
    #: The active thesis-articulation proposal the door verified, when ``writeback_state`` is staged.
    proposal_id: str | None = Field(default=None, min_length=1)


class InvestmentMemoOutputs(_StoredContract):
    source_date: str | None = None
    thesis_statement: dict[str, JsonValue] | None = None
    differentiated_view: tuple[dict[str, JsonValue], ...] = ()
    asymmetry_ratio: int | float | None = None
    asymmetry_diagnostics: JsonValue = None
    expectations_channels: JsonValue = None
    catalysts: tuple[dict[str, JsonValue], ...] = ()
    case_type: str | None = None
    case_type_failure_mode: str | None = None
    case_type_valuation_methodology: str | None = None
    ten_minute_outline: dict[str, JsonValue] | None = None
    elevator_90_seconds: str | None = None
    elevator_word_count: int | float | None = None
    ninety_second_test_passes: bool | None = None
    audience_emphasis: str | None = None
    data_gaps: tuple[dict[str, JsonValue], ...] = ()
    unmatched_claims: tuple[str, ...] = ()
    diligence_stack_audit: JsonValue = None
    recommended_next_action: str | None = None
    #: The Thesis source rows the memo cites, in first-reference order; absent when it cites none.
    sources: tuple[dict[str, JsonValue], ...] | None = None
    memo_lifecycle: MemoLifecycle


__all__ = ["InvestmentMemoOutputs", "MemoLifecycle", "ThesisArticulationVerdict", "WritebackState"]
