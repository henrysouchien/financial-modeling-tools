"""The one owner of the stage list.

Every enumeration of the build stages derives from ``STAGE_REGISTRY`` here:
``root_causes``' stage owners, upstream stages and row predicate,
``evaluated_view.RootOwner``, and the finding repair-owner map in the door
layer. A stage's producer tool, persist door, row prefixes, upstream rank
fact and finding repair-owner alias are recorded once, per stage, in
``STAGES`` order.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Mapping, TypeAlias, get_args

Stage: TypeAlias = Literal[
    "correspondence",
    "business_model",
    "historical_foundation",
    "forecast_assumption_set",
    "scenario_set",
    "valuation_policy",
]

STAGES: tuple[Stage, ...] = get_args(Stage)


@dataclass(frozen=True)
class StageRecord:
    stage: Stage
    producer: str
    persist_tool: str
    row_prefixes: tuple[str, ...]
    scenario_rows: Literal["only", "exclude", "any"]
    upstream: bool
    repair_owner: str | None


_STAGE_RECORDS: tuple[StageRecord, ...] = (
    StageRecord(
        stage="correspondence",
        producer="corpus_state",
        persist_tool="fms_persist_correspondence",
        row_prefixes=("tpl.fm.",),
        scenario_rows="any",
        upstream=True,
        repair_owner="binding",
    ),
    StageRecord(
        stage="business_model",
        producer="get_business_model_context",
        persist_tool="fms_persist_business_model",
        row_prefixes=("bm.",),
        scenario_rows="any",
        upstream=True,
        repair_owner=None,
    ),
    StageRecord(
        stage="historical_foundation",
        producer="get_historical_foundation_context",
        persist_tool="fms_persist_historical_foundation",
        row_prefixes=("bm.", "tpl.fm."),
        scenario_rows="any",
        upstream=False,
        repair_owner=None,
    ),
    StageRecord(
        stage="forecast_assumption_set",
        producer="get_forecast_assumptions_context",
        persist_tool="fms_persist_forecast_assumption_set",
        row_prefixes=("tpl.a.",),
        scenario_rows="exclude",
        upstream=False,
        repair_owner="driver",
    ),
    StageRecord(
        stage="scenario_set",
        producer="get_scenario_set_context",
        persist_tool="fms_persist_scenario_set",
        row_prefixes=(),
        scenario_rows="only",
        upstream=False,
        repair_owner=None,
    ),
    StageRecord(
        stage="valuation_policy",
        producer="get_valuation_policy_context",
        persist_tool="fms_persist_valuation_policy",
        row_prefixes=("tpl.v.",),
        scenario_rows="exclude",
        upstream=False,
        repair_owner=None,
    ),
)

STAGE_REGISTRY: Mapping[Stage, StageRecord] = MappingProxyType(
    {record.stage: record for record in _STAGE_RECORDS}
)

# The Literal and the records are one list spelled twice (once for the type
# checker, once with data); a stage added to one without the other is a
# construction error here, never a silently unmatched stage downstream.
assert tuple(STAGE_REGISTRY) == STAGES, "stage_registry: Stage literal and records disagree"

__all__ = ["STAGE_REGISTRY", "STAGES", "Stage", "StageRecord"]
