from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal

from pydantic import Field

from industry_slice_core._contract import _ContractModel


ModelingWorkflowStage = Literal[
    "idea",
    "research",
    "diligence",
    "bm",
    "build",
    "forecast",
    "scenarios",
    "valuation",
    "review",
    "finalized",
]
ModelingWorkflowFinalizeGrade = Literal["build", "full"]
PendingActionSeverity = Literal["info", "warning", "blocking"]
RemediationInputKind = Literal["deterministic", "judgment"]


class GateTarget(_ContractModel):
    skill: str = Field(min_length=1)
    predicate: str = Field(min_length=1)


class LoopPolicy(_ContractModel):
    max_total_remediations: int = Field(ge=1)
    max_per_blocker: int = Field(ge=1)


class GoalSpec(_ContractModel):
    goal_id: str = Field(min_length=1)
    target: GateTarget
    loop_policy: LoopPolicy


class RemediationDirectiveInput(_ContractModel):
    input_id: str = Field(min_length=1)
    kind: RemediationInputKind
    args: dict[str, Any] = Field(default_factory=dict)


class RemediationDirective(_ContractModel):
    blocker_id: str = Field(min_length=1)
    recommended_skill: str = Field(min_length=1)
    inputs: list[RemediationDirectiveInput] = Field(min_length=1)
    message: str = Field(min_length=1)


PENDING_ACTION_REMEDIATION_ADAPTERS: dict[str, dict[str, Any]] = {
    "persist_valuation_policy": {
        "directive": "missing_wacc",
        "recommended_skill": "valuation-policy",
        # The precompile stage re-observes risk-free rate, ERP and raw beta
        # from the research file's valuation evidence and SOFR from the run's
        # observation, ratifies the beta floor, and seals a successor
        # ValuationPolicy; the compile that follows derives the credit spread.
        "inputs": {
            "tpl.v.cost_of_equity.risk_free_rate": "deterministic",
            "tpl.v.wacc.sofr_rate": "deterministic",
            "tpl.v.cost_of_equity.equity_risk_premium": "deterministic",
            "tpl.v.wacc.credit_spread": "deterministic",
            "tpl.v.cost_of_equity.beta_floor": "deterministic",
        },
    },
    "repair_model_calibration": {
        "directive": "model_calibration",
        "recommended_skill": "model-update",
        "input_kind": "judgment",
    },
}

_WACC_INPUT_LABELS = {
    "tpl.v.cost_of_equity.risk_free_rate": "risk_free_rate",
    "tpl.v.wacc.sofr_rate": "sofr_rate",
    "tpl.v.cost_of_equity.equity_risk_premium": "equity_risk_premium",
    "tpl.v.wacc.credit_spread": "credit_spread",
    "tpl.v.cost_of_equity.beta_floor": "beta_floor",
}
_WACC_LABEL_TO_INPUT_ID = {label: input_id for input_id, label in _WACC_INPUT_LABELS.items()}


def remediation_directives_from_pending_actions(
    pending_actions: Iterable[PendingAction | dict[str, Any]],
    *,
    message_by_code: dict[str, str | None] | None = None,
) -> list[RemediationDirective]:
    actions = [_coerce_pending_action(action) for action in pending_actions]
    actions = [action for action in actions if action.code in PENDING_ACTION_REMEDIATION_ADAPTERS]
    if not actions:
        return []

    directives: list[RemediationDirective] = []
    wacc_directive = _missing_wacc_directive(actions, message_by_code=message_by_code or {})
    if wacc_directive is not None:
        directives.append(wacc_directive)
    model_calibration_directive = _model_calibration_directive(actions, message_by_code=message_by_code or {})
    if model_calibration_directive is not None:
        directives.append(model_calibration_directive)
    return directives


def remediation_directives_payload(
    pending_actions: Iterable[PendingAction | dict[str, Any]],
    *,
    message_by_code: dict[str, str | None] | None = None,
) -> list[dict[str, Any]]:
    return [
        directive.model_dump(mode="json")
        for directive in remediation_directives_from_pending_actions(
            pending_actions,
            message_by_code=message_by_code,
        )
    ]


def _coerce_pending_action(action: PendingAction | dict[str, Any]) -> PendingAction:
    if isinstance(action, PendingAction):
        return action
    return PendingAction.model_validate(action)


def _missing_wacc_directive(
    actions: list[PendingAction],
    *,
    message_by_code: dict[str, str | None],
) -> RemediationDirective | None:
    relevant = [
        action
        for action in actions
        if PENDING_ACTION_REMEDIATION_ADAPTERS[action.code]["directive"] == "missing_wacc"
    ]
    if not relevant:
        return None
    adapter = PENDING_ACTION_REMEDIATION_ADAPTERS["persist_valuation_policy"]
    input_map = dict(adapter["inputs"])
    requested = sorted({input_id for action in relevant for input_id in _valuation_input_ids(action)})
    if not requested:
        requested = sorted(input_map)
    requested = [input_id for input_id in requested if input_id in input_map]
    if not requested:
        return None
    labels = sorted(_WACC_INPUT_LABELS[input_id] for input_id in requested)
    message = next(
        (
            message
            for action in relevant
            for message in (message_by_code.get(action.code), action.message)
            if message
        ),
        "Refresh the ValuationPolicy's observed market inputs through "
        "valuation-policy, then rebuild.",
    )
    return RemediationDirective(
        blocker_id=f"missing_wacc:{','.join(labels)}",
        recommended_skill=str(adapter["recommended_skill"]),
        inputs=[
            RemediationDirectiveInput(input_id=input_id, kind=input_map[input_id])
            for input_id in requested
        ],
        message=message,
    )


def _valuation_input_ids(action: PendingAction) -> list[str]:
    metadata = action.metadata if isinstance(action.metadata, dict) else {}
    raw_values: list[Any] = [metadata.get("input_id"), metadata.get("valuation_input_id")]
    for key in ("input_ids", "missing_input_ids", "valuation_input_ids", "related_item_ids"):
        raw = metadata.get(key)
        if isinstance(raw, list):
            raw_values.extend(raw)
    values: list[str] = []
    for raw in raw_values:
        token = str(raw or "").strip()
        if not token:
            continue
        values.append(_WACC_LABEL_TO_INPUT_ID.get(token, token))
    return sorted({value for value in values if value in _WACC_INPUT_LABELS})


def _model_calibration_directive(
    actions: list[PendingAction],
    *,
    message_by_code: dict[str, str | None],
) -> RemediationDirective | None:
    relevant = [
        action
        for action in actions
        if PENDING_ACTION_REMEDIATION_ADAPTERS[action.code]["directive"] == "model_calibration"
    ]
    if not relevant:
        return None
    adapter = PENDING_ACTION_REMEDIATION_ADAPTERS["repair_model_calibration"]
    input_ids = _model_calibration_input_ids(relevant)
    if not input_ids:
        return None
    blocker_key = _model_calibration_blocker_key(relevant)
    message = next(
        (
            message
            for action in relevant
            for message in (message_by_code.get(action.code), action.message)
            if message
        ),
        "Reconcile the model EPS/FCF basis against consensus before valuation.",
    )
    return RemediationDirective(
        blocker_id=f"model_calibration:{blocker_key}",
        recommended_skill=str(adapter["recommended_skill"]),
        inputs=[
            RemediationDirectiveInput(
                input_id=input_id,
                kind=adapter["input_kind"],
                args=_model_calibration_args(input_id, relevant),
            )
            for input_id in input_ids
        ],
        message=message,
    )


def _model_calibration_blocker_key(actions: list[PendingAction]) -> str:
    for action in actions:
        metadata = action.metadata if isinstance(action.metadata, dict) else {}
        for key in ("gap_id", "calibration_key", "basis_key"):
            value = str(metadata.get(key) or "").strip()
            if value:
                return value
    return "eps_consensus_basis"


def _model_calibration_input_ids(actions: list[PendingAction]) -> list[str]:
    raw_values: list[Any] = ["eps_consensus_basis"]
    for action in actions:
        metadata = action.metadata if isinstance(action.metadata, dict) else {}
        raw_values.extend(
            [
                metadata.get("model_metric_id"),
                metadata.get("consensus_metric_id"),
                metadata.get("output_item_id"),
            ]
        )
        for key in (
            "candidate_driver_item_ids",
            "candidate_model_update_item_ids",
            "candidate_writer_item_ids",
            "related_item_ids",
            "missing_input_ids",
        ):
            raw = metadata.get(key)
            if isinstance(raw, list):
                raw_values.extend(raw)
    input_ids = sorted({str(value).strip() for value in raw_values if str(value or "").strip()})
    return input_ids


def _model_calibration_args(input_id: str, actions: list[PendingAction]) -> dict[str, Any]:
    args: dict[str, Any] = {
        "basis_reconciliation_required": True,
        "do_not_write_output_rows": True,
        "required_sequence": ["model-update", "build-model", "model-review"],
        "calibration_input_id": input_id,
    }
    for action in actions:
        metadata = action.metadata if isinstance(action.metadata, dict) else {}
        if metadata:
            args.setdefault("pending_action_metadata", {}).update(metadata)
        for key, value in metadata.items():
            args.setdefault(key, value)
        if action.source:
            args.setdefault("source", action.source)
        if action.target:
            args.setdefault("target", action.target)
    return args


class ModelingWorkflowGateSnapshot(_ContractModel):
    status: Literal["passed", "failed", "unknown"] = "unknown"
    purpose: Literal["handoff", "build"] = "handoff"
    template_id: str | None = None
    failed_gates: dict[str, Any] = Field(default_factory=dict)
    gates_hash: str


class ModelingWorkflowDiligenceCompletionRef(_ContractModel):
    handoff_id: int | None = Field(default=None, ge=1)
    handoff_version: int | None = Field(default=None, ge=1)
    diligence_completion_hash: str


class PendingAction(_ContractModel):
    code: str = Field(min_length=1)
    stage: ModelingWorkflowStage
    message: str = Field(min_length=1)
    severity: PendingActionSeverity = "blocking"
    target: str | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "GateTarget",
    "GoalSpec",
    "LoopPolicy",
    "ModelingWorkflowDiligenceCompletionRef",
    "ModelingWorkflowFinalizeGrade",
    "ModelingWorkflowGateSnapshot",
    "ModelingWorkflowStage",
    "PENDING_ACTION_REMEDIATION_ADAPTERS",
    "PendingAction",
    "PendingActionSeverity",
    "RemediationDirective",
    "RemediationDirectiveInput",
    "RemediationInputKind",
    "remediation_directives_from_pending_actions",
    "remediation_directives_payload",
]
