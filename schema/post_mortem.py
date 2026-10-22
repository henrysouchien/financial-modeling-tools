from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict, Field, field_validator, model_validator

from ._outcome_shared import OutcomeComponent
from .outcome_contract import PredictionTarget
from .outcome_realized import ComponentObservation, OutcomeContract, RealizedReturn
from .thesis import ComponentScorecardEntry, ScorecardEntryStatus
from .thesis_shared_slice import SourceId, _ContractModel


PostMortemFindingKind = Literal["went_right", "went_wrong"]
PostMortemSourceKind = Literal[
    "outcome_contract",
    "prediction_snapshot",
    "realization",
    "scorecard",
    "thesis_pre_mortem",
    "decision_log",
]


class _PostMortemModel(_ContractModel):
    model_config = ConfigDict(frozen=True)


class PostMortemSourceRef(_PostMortemModel):
    kind: PostMortemSourceKind
    ref_id: str = Field(..., min_length=1)
    artifact_path: str | None = Field(default=None, min_length=1)


class PostMortemMethodologyAction(_PostMortemModel):
    action: str = Field(..., min_length=1)
    rationale: str = Field(..., min_length=1)
    based_on_prediction_target_ids: tuple[str, ...] = Field(..., min_length=1)


class PostMortemCompileRequest(_PostMortemModel):
    schema_version: Literal["1.0"] = "1.0"
    outcome_contract: OutcomeContract
    scorecard_entries: tuple[ComponentScorecardEntry, ...] = Field(..., min_length=1)
    thesis_pre_mortem_ref: PostMortemSourceRef | None = None
    decision_log_refs: tuple[PostMortemSourceRef, ...] = ()
    methodology_actions: tuple[PostMortemMethodologyAction, ...] = ()

    @model_validator(mode="after")
    def _optional_refs_have_expected_kinds(self) -> "PostMortemCompileRequest":
        if (
            self.thesis_pre_mortem_ref is not None
            and self.thesis_pre_mortem_ref.kind != "thesis_pre_mortem"
        ):
            raise ValueError("thesis_pre_mortem_ref must use kind='thesis_pre_mortem'")
        if any(ref.kind != "decision_log" for ref in self.decision_log_refs):
            raise ValueError("decision_log_refs must use kind='decision_log'")
        return self


class PostMortemBottomLine(_PostMortemModel):
    action: Literal["EXIT REVIEW COMPLETE"] = "EXIT REVIEW COMPLETE"
    summary: str = Field(..., min_length=1)
    confirmed_count: int = Field(..., ge=0)
    challenged_count: int = Field(..., ge=0)
    disconfirmed_count: int = Field(..., ge=0)
    source_refs: tuple[str, ...] = Field(..., min_length=1)


class PostMortemThesisAtEntry(_PostMortemModel):
    prediction_snapshot_id: str = Field(..., min_length=1)
    as_of: str = Field(..., min_length=1)
    targets: tuple[PredictionTarget, ...] = Field(..., min_length=1)


class PostMortemRealizedOutcome(_PostMortemModel):
    realization_id: str = Field(..., min_length=1)
    as_of: str = Field(..., min_length=1)
    realized_return: RealizedReturn
    observations: tuple[ComponentObservation, ...] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _return_is_one_of_the_observations(self) -> "PostMortemRealizedOutcome":
        return_rows = [
            observation
            for observation in self.observations
            if observation.component == "return"
        ]
        if len(return_rows) != 1 or return_rows[0].realized != self.realized_return:
            raise ValueError("realized outcome requires exactly one matching return observation")
        return self


class PostMortemScoreRow(_PostMortemModel):
    prediction_target_id: str = Field(..., min_length=1)
    component: OutcomeComponent
    metric: str = Field(..., min_length=1)
    status: ScorecardEntryStatus
    predicted: float | None = Field(default=None, allow_inf_nan=False)
    realized: float | None = Field(default=None, allow_inf_nan=False)
    score: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    scored_at: str = Field(..., min_length=1)
    source_refs: tuple[SourceId, ...] = ()


class PostMortemFinding(_PostMortemModel):
    kind: PostMortemFindingKind
    prediction_target_id: str = Field(..., min_length=1)
    component: OutcomeComponent
    metric: str = Field(..., min_length=1)
    status: Literal["confirmed", "challenged", "disconfirmed"]
    predicted: float | None = Field(default=None, allow_inf_nan=False)
    realized: float | None = Field(default=None, allow_inf_nan=False)
    score: float = Field(..., ge=0, le=1, allow_inf_nan=False)
    evidence_refs: tuple[str, ...] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _kind_matches_status(self) -> "PostMortemFinding":
        expected = "went_right" if self.status == "confirmed" else "went_wrong"
        if self.kind != expected:
            raise ValueError(f"{self.status} finding must use kind={expected!r}")
        return self


class PostMortemDataGap(_PostMortemModel):
    prediction_target_id: str = Field(..., min_length=1)
    component: OutcomeComponent
    metric: str = Field(..., min_length=1)
    status: Literal["tracking", "unresolvable"]
    reason: str = Field(..., min_length=1)


class PostMortemPack(_PostMortemModel):
    schema_version: Literal["1.0"] = "1.0"
    genre: Literal["post_mortem"] = "post_mortem"
    ticker: str = Field(..., min_length=1)
    verdict: Literal["POST_MORTEM_COMPILED"] = "POST_MORTEM_COMPILED"
    as_of: str = Field(..., min_length=1)
    subtitle: str = Field(..., min_length=1)
    bottom_line: PostMortemBottomLine
    thesis_at_entry: PostMortemThesisAtEntry
    realized_outcome: PostMortemRealizedOutcome
    scorecard: tuple[PostMortemScoreRow, ...] = Field(..., min_length=1)
    what_went_right: tuple[PostMortemFinding, ...] = ()
    what_went_wrong: tuple[PostMortemFinding, ...] = ()
    lessons: tuple[PostMortemMethodologyAction, ...] = ()
    data_gaps: tuple[PostMortemDataGap, ...] = ()
    source_refs: tuple[PostMortemSourceRef, ...] = Field(..., min_length=4)
    outcome_contract_id: str = Field(..., min_length=1)
    prediction_snapshot_id: str = Field(..., min_length=1)
    realization_id: str = Field(..., min_length=1)
    scorecard_id: str = Field(..., min_length=1)
    scorer_version: str = Field(..., min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _lineage_and_sections_are_coherent(self) -> "PostMortemPack":
        if self.thesis_at_entry.prediction_snapshot_id != self.prediction_snapshot_id:
            raise ValueError("thesis_at_entry snapshot id must match pack lineage")
        if self.realized_outcome.realization_id != self.realization_id:
            raise ValueError("realized_outcome realization id must match pack lineage")

        targets = {
            target.prediction_target_id: target
            for target in self.thesis_at_entry.targets
        }
        score_rows = {row.prediction_target_id: row for row in self.scorecard}
        if len(score_rows) != len(self.scorecard):
            raise ValueError("scorecard prediction_target_ids must be unique")
        if set(score_rows) != set(targets):
            raise ValueError("scorecard must contain exactly one row per prediction target")
        for target_id, row in score_rows.items():
            target = targets[target_id]
            if row.component != target.component or row.metric != target.metric:
                raise ValueError("scorecard row component and metric must match target")

        finding_ids: set[str] = set()
        for finding in (*self.what_went_right, *self.what_went_wrong):
            if finding.prediction_target_id in finding_ids:
                raise ValueError("prediction target may appear in only one finding")
            finding_ids.add(finding.prediction_target_id)
            row = score_rows.get(finding.prediction_target_id)
            if row is None or row.status != finding.status or row.score != finding.score:
                raise ValueError("finding must match its scorecard row")
        expected_finding_ids = {
            row.prediction_target_id
            for row in self.scorecard
            if row.status in {"confirmed", "challenged", "disconfirmed"}
        }
        if finding_ids != expected_finding_ids:
            raise ValueError("findings must cover all resolved scorecard rows exactly")

        gap_ids = {gap.prediction_target_id for gap in self.data_gaps}
        expected_gap_ids = {
            row.prediction_target_id
            for row in self.scorecard
            if row.status in {"tracking", "unresolvable"}
        }
        if gap_ids != expected_gap_ids:
            raise ValueError("data_gaps must cover tracking and unresolvable rows exactly")

        counts = {
            status: sum(row.status == status for row in self.scorecard)
            for status in ("confirmed", "challenged", "disconfirmed")
        }
        if (
            self.bottom_line.confirmed_count != counts["confirmed"]
            or self.bottom_line.challenged_count != counts["challenged"]
            or self.bottom_line.disconfirmed_count != counts["disconfirmed"]
        ):
            raise ValueError("bottom_line counts must match scorecard statuses")
        for lesson in self.lessons:
            if not set(lesson.based_on_prediction_target_ids) <= set(targets):
                raise ValueError("lesson target ids must reference prediction targets")

        core_refs = {
            "outcome_contract": self.outcome_contract_id,
            "prediction_snapshot": self.prediction_snapshot_id,
            "realization": self.realization_id,
            "scorecard": self.scorecard_id,
        }
        for kind, ref_id in core_refs.items():
            matches = [
                ref for ref in self.source_refs if ref.kind == kind and ref.ref_id == ref_id
            ]
            if len(matches) != 1:
                raise ValueError(f"source_refs requires exactly one matching {kind} ref")
        return self


__all__ = [
    "PostMortemBottomLine",
    "PostMortemCompileRequest",
    "PostMortemDataGap",
    "PostMortemFinding",
    "PostMortemFindingKind",
    "PostMortemMethodologyAction",
    "PostMortemPack",
    "PostMortemRealizedOutcome",
    "PostMortemScoreRow",
    "PostMortemSourceKind",
    "PostMortemSourceRef",
    "PostMortemThesisAtEntry",
]
