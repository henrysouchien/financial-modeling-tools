from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal

from pydantic import Field

from .handoff import CurrentModelRef
from .thesis_shared_slice import _ContractModel


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
    "persist_forecast_assumption_driver": {
        "directive": "driver_plan_gap",
        "recommended_skill": "business-model-construction",
        "input_kind": "judgment",
    },
    "persist_forecast_assumptions": {
        "directive": "driver_plan_gap",
        "recommended_skill": "forecast-assumptions",
        "input_kind": "judgment",
    },
    "persist_valuation_inputs": {
        "directive": "missing_wacc",
        "recommended_skill": "valuation-inputs",
        "inputs": {
            "tpl.v.cost_of_equity.risk_free_rate": "deterministic",
            "tpl.v.wacc.sofr_rate": "deterministic",
            # ERP and beta-floor rows are dispatched through valuation-inputs,
            # but their args below mark the explicit source/carry-forward
            # requirement. valuation-inputs must not invent them.
            "tpl.v.cost_of_equity.equity_risk_premium": "deterministic",
            # Credit spread is computed when empirical debt/SOFR guards pass;
            # otherwise valuation-inputs requires an explicit sourced override.
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
_VALUATION_INPUT_REMEDIATION_ARGS: dict[str, dict[str, Any]] = {
    "tpl.v.cost_of_equity.risk_free_rate": {
        "source_mode": "fred",
        "series_id": "DGS10",
    },
    "tpl.v.wacc.sofr_rate": {
        "source_mode": "fred",
        "series_id": "SOFR",
    },
    "tpl.v.cost_of_equity.equity_risk_premium": {
        "requires_source": True,
        "value_key": "equity_risk_premium_override_decimal",
        "source_key": "equity_risk_premium_source",
        "rationale_key": "equity_risk_premium_rationale",
        "carry_forward_skill": "valuation-inputs",
    },
    "tpl.v.wacc.credit_spread": {
        "source_mode": "derive_or_source",
        "derivation": "effective_yield_minus_sofr",
        "override_value_key": "credit_spread_override_decimal",
        "override_source_key": "credit_spread_source",
        "override_rationale_key": "credit_spread_rationale",
        "carry_forward_skill": "valuation-inputs",
    },
    "tpl.v.cost_of_equity.beta_floor": {
        "requires_source": True,
        "value_key": "beta_floor_override_decimal",
        "source_key": "beta_floor_source",
        "rationale_key": "beta_floor_rationale",
        "carry_forward_skill": "valuation-inputs",
    },
}


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
    driver_directive = _driver_plan_gap_directive(actions, message_by_code=message_by_code or {})
    if driver_directive is not None:
        directives.append(driver_directive)
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


def _driver_plan_gap_directive(
    actions: list[PendingAction],
    *,
    message_by_code: dict[str, str | None],
) -> RemediationDirective | None:
    relevant = [
        action
        for action in actions
        if PENDING_ACTION_REMEDIATION_ADAPTERS[action.code]["directive"] == "driver_plan_gap"
    ]
    if not relevant:
        return None
    keys = sorted({key for action in relevant for key in _driver_gap_keys(action)})
    if not keys:
        return None
    adapter = PENDING_ACTION_REMEDIATION_ADAPTERS[relevant[0].code]
    messages = [
        message
        for action in relevant
        for message in (message_by_code.get(action.code), action.message)
        if message
    ]
    return RemediationDirective(
        blocker_id=f"driver_plan_gap:{','.join(keys)}",
        recommended_skill=str(adapter["recommended_skill"]),
        inputs=[
            RemediationDirectiveInput(
                input_id=key,
                kind=adapter["input_kind"],
                args=_driver_gap_args(action),
            )
            for key, action in _driver_gap_action_pairs(keys, relevant)
        ],
        message=messages[0] if messages else "Complete the missing BusinessModel driver plan before forecast assumptions can proceed.",
    )


def _driver_gap_keys(action: PendingAction) -> list[str]:
    metadata = action.metadata if isinstance(action.metadata, dict) else {}
    raw_values: list[Any] = [
        metadata.get("driver_key"),
        metadata.get("item_id"),
        metadata.get("projection_key"),
    ]
    for key in ("related_item_ids", "projection_keys", "missing_driver_keys"):
        raw = metadata.get(key)
        if isinstance(raw, list):
            raw_values.extend(raw)
    return sorted({str(value).strip() for value in raw_values if str(value or "").strip()})


def _driver_gap_action_pairs(keys: list[str], actions: list[PendingAction]) -> list[tuple[str, PendingAction]]:
    pairs: list[tuple[str, PendingAction]] = []
    for key in keys:
        owner = next((action for action in actions if key in _driver_gap_keys(action)), actions[0])
        pairs.append((key, owner))
    return pairs


def _driver_gap_args(action: PendingAction) -> dict[str, Any]:
    args = dict(action.metadata)
    if action.source:
        args.setdefault("source", action.source)
    if action.target:
        args.setdefault("target", action.target)
    return args


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
    adapter = PENDING_ACTION_REMEDIATION_ADAPTERS["persist_valuation_inputs"]
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
        "Refresh sourced WACC inputs through valuation-inputs.",
    )
    return RemediationDirective(
        blocker_id=f"missing_wacc:{','.join(labels)}",
        recommended_skill=str(adapter["recommended_skill"]),
        inputs=[
            RemediationDirectiveInput(
                input_id=input_id,
                kind=input_map[input_id],
                args=_valuation_input_args(input_id, relevant),
            )
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


def _valuation_input_args(input_id: str, actions: list[PendingAction]) -> dict[str, Any]:
    args: dict[str, Any] = dict(_VALUATION_INPUT_REMEDIATION_ARGS.get(input_id, {}))
    for action in actions:
        metadata = action.metadata if isinstance(action.metadata, dict) else {}
        if input_id in _valuation_input_ids(action) or not _valuation_input_ids(action):
            if metadata:
                args.setdefault("pending_action_metadata", {}).update(metadata)
            for key, value in metadata.items():
                args.setdefault(key, value)
            if action.source:
                args.setdefault("source", action.source)
    args.setdefault("input_label", _WACC_INPUT_LABELS[input_id])
    return args


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


class ModelingWorkflowStateCacheKey(_ContractModel):
    research_file_id: int = Field(ge=1)
    thesis_id: str | None = None
    thesis_version: int | None = Field(default=None, ge=1)
    handoff_id: int | None = Field(default=None, ge=1)
    handoff_version: int | None = Field(default=None, ge=1)
    model_id: str | None = None
    model_version: int | None = Field(default=None, ge=1)
    model_build_context_id: str | None = None
    model_build_context_version: int | None = Field(default=None, ge=1)
    diligence_completion_hash: str
    gates_hash: str
    model_semantics_hash: str | None = None


class ModelingWorkflowState(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    research_file_id: int = Field(ge=1)
    stage: ModelingWorkflowStage
    finalize_grade: ModelingWorkflowFinalizeGrade | None = None
    thesis_id: str | None = None
    thesis_version: int | None = Field(default=None, ge=1)
    handoff_id: int | None = Field(default=None, ge=1)
    handoff_version: int | None = Field(default=None, ge=1)
    handoff_status: str | None = None
    diligence_completion_ref: ModelingWorkflowDiligenceCompletionRef
    current_model_ref: CurrentModelRef | None = None
    gates_passed: ModelingWorkflowGateSnapshot
    pending_actions: list[PendingAction] = Field(default_factory=list)
    cache_key: ModelingWorkflowStateCacheKey


__all__ = [
    "GateTarget",
    "GoalSpec",
    "LoopPolicy",
    "ModelingWorkflowDiligenceCompletionRef",
    "ModelingWorkflowFinalizeGrade",
    "ModelingWorkflowGateSnapshot",
    "ModelingWorkflowStage",
    "ModelingWorkflowState",
    "ModelingWorkflowStateCacheKey",
    "PENDING_ACTION_REMEDIATION_ADAPTERS",
    "PendingAction",
    "PendingActionSeverity",
    "RemediationDirective",
    "RemediationDirectiveInput",
    "RemediationInputKind",
    "remediation_directives_from_pending_actions",
    "remediation_directives_payload",
]
