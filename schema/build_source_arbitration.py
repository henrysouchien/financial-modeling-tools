"""Pure source-arbitration decisions for model-build historical values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from .models import DataSourceMapping, ValueProvenance

SourceArbitrationMode = Literal["off", "shadow", "apply"]
SourceArbitrationPolicy = Literal[
    "diagnostic_only",
    "edgar_authoritative",
    "preferred_source_authoritative",
]
SourceArbitrationStatus = Literal[
    "confirmed",
    "single_source",
    "incomparable",
    "gap",
    "material_gap",
]
SourceArbitrationAction = Literal[
    "keep_served_value",
    "keep_authoritative_value",
    "apply_authoritative_value",
    "surface_diagnostic_only",
    "fail_closed_missing_provenance",
    "skip_superseded_cell",
]
SourceArbitrationReason = Literal[
    "mode_off",
    "diagnostic_only_policy",
    "exact_match",
    "within_tolerance",
    "gap",
    "material_gap",
    "single_source_authority",
    "authority_value_present",
    "authority_unavailable",
    "missing_authority_provenance",
    "current_cell_changed_since_population",
]
SourceObservationStatus = Literal["ok", "missing", "failed"]
SourceName = Literal["fmp", "edgar"]

_VALID_MODES: set[str] = {"off", "shadow", "apply"}
_VALID_SOURCES: set[str] = {"fmp", "edgar"}
_DEFAULT_POLICY: SourceArbitrationPolicy = "diagnostic_only"
_EQUALITY_EPSILON = 1e-9


@dataclass(frozen=True)
class SourceArbitrationInput:
    concept_id: str
    year: int
    original_served_source: SourceName | None = None
    original_cell_value: float | None = None
    original_cell_provenance: ValueProvenance | str | None = None
    fmp_value: float | None = None
    fmp_status: SourceObservationStatus = "missing"
    fmp_provenance: Any | None = None
    edgar_value: float | None = None
    edgar_status: SourceObservationStatus = "missing"
    edgar_provenance: Any | None = None
    current_cell_value: float | None = None
    current_cell_provenance: ValueProvenance | str | None = None


@dataclass(frozen=True)
class SourceArbitrationDecision:
    concept_id: str
    year: int
    enabled: bool
    mode: SourceArbitrationMode
    status: SourceArbitrationStatus
    policy: SourceArbitrationPolicy
    original_served_source: SourceName | None
    chosen_source: SourceName | None
    action: SourceArbitrationAction
    reason: SourceArbitrationReason
    fmp_value: float | None
    edgar_value: float | None
    delta: float | None = None
    delta_pct: float | None = None
    abs_delta_pct: float | None = None
    would_apply: bool = False


def validate_source_arbitration_mode(mode: str) -> SourceArbitrationMode:
    normalized = str(mode).lower()
    if normalized not in _VALID_MODES:
        raise ValueError(
            "source_arbitration_mode must be one of: apply, off, shadow"
        )
    return cast(SourceArbitrationMode, normalized)


def decide_source_arbitration(
    arbitration_input: SourceArbitrationInput,
    *,
    mapping: DataSourceMapping | None = None,
    mode: SourceArbitrationMode | str = "shadow",
    materiality_pct: float = 0.05,
    equality_epsilon: float = _EQUALITY_EPSILON,
) -> SourceArbitrationDecision:
    """Return the deterministic source-arbitration decision for one cell."""

    normalized_mode = validate_source_arbitration_mode(str(mode))
    policy = _mapping_policy(mapping)
    status, reason, delta, delta_pct, abs_delta_pct = _comparison_decision(
        arbitration_input,
        tolerance_pct=_mapping_tolerance_pct(mapping),
        materiality_pct=materiality_pct,
        equality_epsilon=equality_epsilon,
    )

    if normalized_mode == "off":
        return _decision(
            arbitration_input,
            enabled=False,
            mode=normalized_mode,
            status=status,
            policy=policy,
            chosen_source=None,
            action="keep_served_value",
            reason="mode_off",
            delta=delta,
            delta_pct=delta_pct,
            abs_delta_pct=abs_delta_pct,
        )

    if policy == "diagnostic_only":
        return _decision(
            arbitration_input,
            enabled=True,
            mode=normalized_mode,
            status=status,
            policy=policy,
            chosen_source=None,
            action="surface_diagnostic_only",
            reason="diagnostic_only_policy",
            delta=delta,
            delta_pct=delta_pct,
            abs_delta_pct=abs_delta_pct,
        )

    authority_source = _authority_source(policy, mapping)
    if authority_source is None:
        return _decision(
            arbitration_input,
            enabled=True,
            mode=normalized_mode,
            status=status,
            policy=policy,
            chosen_source=None,
            action="keep_served_value",
            reason="authority_unavailable",
            delta=delta,
            delta_pct=delta_pct,
            abs_delta_pct=abs_delta_pct,
        )

    authority_value = _source_value(arbitration_input, authority_source)
    authority_status = _source_status(arbitration_input, authority_source)
    authority_provenance = _source_provenance(arbitration_input, authority_source)
    if authority_status != "ok" or authority_value is None:
        return _decision(
            arbitration_input,
            enabled=True,
            mode=normalized_mode,
            status=status,
            policy=policy,
            chosen_source=authority_source,
            action="keep_served_value",
            reason="authority_unavailable",
            delta=delta,
            delta_pct=delta_pct,
            abs_delta_pct=abs_delta_pct,
        )

    if authority_provenance is None:
        return _decision(
            arbitration_input,
            enabled=True,
            mode=normalized_mode,
            status=status,
            policy=policy,
            chosen_source=authority_source,
            action="fail_closed_missing_provenance",
            reason="missing_authority_provenance",
            delta=delta,
            delta_pct=delta_pct,
            abs_delta_pct=abs_delta_pct,
        )

    if _cell_was_superseded(arbitration_input, equality_epsilon=equality_epsilon):
        return _decision(
            arbitration_input,
            enabled=True,
            mode=normalized_mode,
            status=status,
            policy=policy,
            chosen_source=authority_source,
            action="skip_superseded_cell",
            reason="current_cell_changed_since_population",
            delta=delta,
            delta_pct=delta_pct,
            abs_delta_pct=abs_delta_pct,
        )

    if arbitration_input.original_served_source == authority_source or _values_equal(
        arbitration_input.original_cell_value,
        authority_value,
        equality_epsilon=equality_epsilon,
    ):
        action: SourceArbitrationAction = "keep_authoritative_value"
        would_apply = False
    else:
        action = "apply_authoritative_value"
        would_apply = True

    return _decision(
        arbitration_input,
        enabled=True,
        mode=normalized_mode,
        status=status,
        policy=policy,
        chosen_source=authority_source,
        action=action,
        reason=(
            "single_source_authority"
            if status == "single_source"
            else "authority_value_present"
        ),
        delta=delta,
        delta_pct=delta_pct,
        abs_delta_pct=abs_delta_pct,
        would_apply=would_apply,
    )


def _decision(
    arbitration_input: SourceArbitrationInput,
    *,
    enabled: bool,
    mode: SourceArbitrationMode,
    status: SourceArbitrationStatus,
    policy: SourceArbitrationPolicy,
    chosen_source: SourceName | None,
    action: SourceArbitrationAction,
    reason: SourceArbitrationReason,
    delta: float | None,
    delta_pct: float | None,
    abs_delta_pct: float | None,
    would_apply: bool = False,
) -> SourceArbitrationDecision:
    return SourceArbitrationDecision(
        concept_id=arbitration_input.concept_id,
        year=arbitration_input.year,
        enabled=enabled,
        mode=mode,
        status=status,
        policy=policy,
        original_served_source=arbitration_input.original_served_source,
        chosen_source=chosen_source,
        action=action,
        reason=reason,
        fmp_value=arbitration_input.fmp_value,
        edgar_value=arbitration_input.edgar_value,
        delta=delta,
        delta_pct=delta_pct,
        abs_delta_pct=abs_delta_pct,
        would_apply=would_apply,
    )


def _mapping_policy(mapping: DataSourceMapping | None) -> SourceArbitrationPolicy:
    if mapping is None or mapping.source_arbitration_policy is None:
        return _DEFAULT_POLICY
    return mapping.source_arbitration_policy


def _mapping_tolerance_pct(mapping: DataSourceMapping | None) -> float:
    if mapping is None or mapping.validation_tolerance_pct is None:
        return 0.0
    return float(mapping.validation_tolerance_pct)


def _comparison_decision(
    arbitration_input: SourceArbitrationInput,
    *,
    tolerance_pct: float,
    materiality_pct: float,
    equality_epsilon: float,
) -> tuple[
    SourceArbitrationStatus,
    SourceArbitrationReason,
    float | None,
    float | None,
    float | None,
]:
    fmp_present = (
        arbitration_input.fmp_status == "ok" and arbitration_input.fmp_value is not None
    )
    edgar_present = (
        arbitration_input.edgar_status == "ok"
        and arbitration_input.edgar_value is not None
    )

    if not fmp_present and not edgar_present:
        return "incomparable", "authority_unavailable", None, None, None
    if not fmp_present or not edgar_present:
        return "single_source", "single_source_authority", None, None, None

    fmp_value = float(arbitration_input.fmp_value)
    edgar_value = float(arbitration_input.edgar_value)
    delta = edgar_value - fmp_value
    denominator = max(abs(edgar_value), abs(fmp_value))
    if denominator > 0:
        delta_pct = delta / denominator
        abs_delta_pct = abs(delta) / denominator
    else:
        delta_pct = 0.0
        abs_delta_pct = 0.0

    if abs(delta) <= equality_epsilon:
        return "confirmed", "exact_match", delta, delta_pct, abs_delta_pct
    if abs_delta_pct <= tolerance_pct:
        return "confirmed", "within_tolerance", delta, delta_pct, abs_delta_pct
    if abs_delta_pct >= materiality_pct:
        return "material_gap", "material_gap", delta, delta_pct, abs_delta_pct
    return "gap", "gap", delta, delta_pct, abs_delta_pct


def _authority_source(
    policy: SourceArbitrationPolicy,
    mapping: DataSourceMapping | None,
) -> SourceName | None:
    if policy == "edgar_authoritative":
        return "edgar"
    if policy != "preferred_source_authoritative" or mapping is None:
        return None
    source = str(mapping.preferred_source or "").lower()
    if source not in _VALID_SOURCES:
        return None
    return cast(SourceName, source)


def _source_value(
    arbitration_input: SourceArbitrationInput,
    source: SourceName,
) -> float | None:
    if source == "edgar":
        return arbitration_input.edgar_value
    return arbitration_input.fmp_value


def _source_status(
    arbitration_input: SourceArbitrationInput,
    source: SourceName,
) -> SourceObservationStatus:
    if source == "edgar":
        return arbitration_input.edgar_status
    return arbitration_input.fmp_status


def _source_provenance(
    arbitration_input: SourceArbitrationInput,
    source: SourceName,
) -> Any | None:
    if source == "edgar":
        return arbitration_input.edgar_provenance
    return arbitration_input.fmp_provenance


def _cell_was_superseded(
    arbitration_input: SourceArbitrationInput,
    *,
    equality_epsilon: float,
) -> bool:
    has_current = (
        arbitration_input.current_cell_value is not None
        or arbitration_input.current_cell_provenance is not None
    )
    if not has_current:
        return False
    if not _values_equal(
        arbitration_input.current_cell_value,
        arbitration_input.original_cell_value,
        equality_epsilon=equality_epsilon,
    ):
        return True
    return _provenance_key(arbitration_input.current_cell_provenance) != _provenance_key(
        arbitration_input.original_cell_provenance
    )


def _values_equal(
    left: float | None,
    right: float | None,
    *,
    equality_epsilon: float,
) -> bool:
    if left is None or right is None:
        return left is right
    return abs(float(left) - float(right)) <= equality_epsilon


def _provenance_key(value: ValueProvenance | str | None) -> str | None:
    if isinstance(value, ValueProvenance):
        return value.value
    if value is None:
        return None
    return str(value)


__all__ = [
    "SourceArbitrationAction",
    "SourceArbitrationDecision",
    "SourceArbitrationInput",
    "SourceArbitrationMode",
    "SourceArbitrationPolicy",
    "SourceArbitrationReason",
    "SourceArbitrationStatus",
    "SourceName",
    "SourceObservationStatus",
    "decide_source_arbitration",
    "validate_source_arbitration_mode",
]
