"""Bridge thesis scenario payloads into workbook scenario rows."""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from functools import cache
import re
from typing import Any, Literal, Optional, TypeGuard

from schema.scenario_rows import find_scenario_value_row
from schema.dependency_graph import DependencyGraph
from schema.formatter import SIA_FORMATTER
from schema.models import FinancialModel, FormulaType, LineItem
from industry_slice_core.industry_types import Unit
from schema.modify import (
    FixedCellValueOperation,
    Operation,
    OperationType,
    PeriodValueOperation,
)
from schema.refs import line_item_ref_from_obj
from schema.scenario_ordering import scenario_ordering_issues


@dataclass(frozen=True)
class AnchorResolution:
    factor: str
    owner_id: Optional[str]
    anchor_id: Optional[str]
    bull_id: Optional[str]
    base_id: Optional[str]
    bear_id: Optional[str]
    match_reason: Literal[
        "explicit_hint",
        "label_match",
        "label_match_low_confidence",
        "unresolved",
        "invalid_hint",
    ]
    score: Optional[float] = None
    candidates: list[str] = dataclass_field(default_factory=list)


@dataclass(frozen=True)
class BridgeWarning:
    kind: Literal[
        "unresolved_factor",
        "low_confidence_factor",
        "invalid_hint",
        "non_numeric_factor_value",
        "missing_factor_curve",
        "period_coverage_gap",
        "unit_shape_mismatch",
        "scenario_ordering_violation",
        "inert_scenario_anchor",
    ]
    factor: Optional[str]
    field: Optional[str]
    detail: str
    candidates: list[str] = dataclass_field(default_factory=list)


_STOPWORDS = {"pct", "yoy", "chg", "change"}
_ID_NOISE_TOKENS = {
    "tpl",
    "a",
    "s",
    "fm",
    "scenario",
    "scenarios",
    "tables",
    "table",
    "label",
    "drivers",
    "driver",
}
_BPS_FACTOR_PATTERNS = (
    re.compile(r"(?:^|_)bps(?:_|$)"),
    re.compile(r"(?:^|_)bp_per(?:_|$)"),
    re.compile(r"(?:^|_)basis_points?(?:_|$)"),
)
_INTENTIONAL_FORMULA_OVERRIDE_PREFIXES = (
    "tpl.a.scenario_tables.",
)
_SCENARIO_ORDERING_EPS = 1e-12


def _is_intentional_formula_override(item_id: str) -> bool:
    return str(item_id).startswith(_INTENTIONAL_FORMULA_OVERRIDE_PREFIXES)


def _bridge_fixed_value_operation(
    *,
    item_id: str,
    value: float,
) -> FixedCellValueOperation:
    return FixedCellValueOperation(
        type=OperationType.set_value,
        item_id=item_id,
        value=value,
        force_set_value_on_derived=_is_intentional_formula_override(item_id),
    )


def _bridge_period_value_operation(
    *,
    item_id: str,
    values: dict[int, float],
) -> PeriodValueOperation:
    return PeriodValueOperation(
        type=OperationType.set_value,
        item_id=item_id,
        values=values,
        force_set_value_on_derived=_is_intentional_formula_override(item_id),
    )


def _period_key(value: Any) -> int | None:
    text = str(value or "").strip()
    if text.isdigit() and len(text) == 4:
        return int(text)
    match = re.search(r"(?:FY)?(\d{4})", text, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _tokens(value: str) -> set[str]:
    normalized = str(value or "").lower().replace("%", " percent ")
    raw_tokens = re.split(r"[^a-z]+|\d+", normalized)
    return {token for token in raw_tokens if token and token not in _STOPWORDS}


def _id_tokens(value: str) -> set[str]:
    return {token for token in _tokens(value) if token not in _ID_NOISE_TOKENS}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _is_offset_scenario_owner(item: LineItem) -> bool:
    spec = item.projected
    return (
        spec is not None
        and spec.type == FormulaType.valuation
        and spec.subtype == "offset_scenario"
    )


def _anchor_id_for_owner(owner: LineItem) -> Optional[str]:
    spec = owner.projected
    if spec is None:
        return None
    anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
    return anchor_ref.id if anchor_ref is not None else None


def _scenario_owners(model: FinancialModel) -> list[LineItem]:
    if not model._index:
        model.build_index()
    assumptions = model.sheets.get("Assumptions")
    if assumptions is None:
        return []
    owners: list[LineItem] = []
    for section in assumptions.sections:
        for item in section.line_items:
            if _is_offset_scenario_owner(item):
                owners.append(item)
    return owners


def _score_candidate(
    factor_tokens: set[str], owner: LineItem, anchor: LineItem
) -> float:
    return (
        1.0 * _jaccard(_tokens(anchor.label), factor_tokens)
        + 0.5 * _jaccard(_id_tokens(anchor.id), factor_tokens)
        + 0.3 * _jaccard(_id_tokens(owner.id), factor_tokens)
    )


def _candidate_scores(
    model: FinancialModel, factor: str
) -> list[tuple[LineItem, str, float]]:
    factor_tokens = _tokens(factor)
    candidates: list[tuple[LineItem, str, float]] = []
    for owner in _scenario_owners(model):
        anchor_id = _anchor_id_for_owner(owner)
        if anchor_id is None:
            continue
        try:
            anchor = model.get_item(anchor_id)
        except KeyError:
            continue
        candidates.append(
            (owner, anchor_id, _score_candidate(factor_tokens, owner, anchor))
        )
    candidates.sort(key=lambda entry: (-entry[2], entry[0].id))
    return candidates


def _top_candidate_owner_ids(
    model: FinancialModel, factor: str, limit: int = 5
) -> list[str]:
    return [
        owner.id
        for owner, _anchor_id, _score in _candidate_scores(model, factor)[:limit]
    ]


def _owner_from_hint(model: FinancialModel, hint: str) -> LineItem | None:
    try:
        item = model.get_item(hint)
    except KeyError:
        return None
    if _is_offset_scenario_owner(item):
        return item
    for owner in _scenario_owners(model):
        anchor_id = _anchor_id_for_owner(owner)
        if anchor_id is None:
            continue
        if hint == anchor_id:
            return owner
        if hint in {
            row_id for row_id in _case_row_ids(model, anchor_id) if row_id is not None
        }:
            return owner
    return None


def _case_row_ids(
    model: FinancialModel, anchor_id: str
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    return (
        find_scenario_value_row(model, anchor_id, "bull"),
        find_scenario_value_row(model, anchor_id, "base"),
        find_scenario_value_row(model, anchor_id, "bear"),
    )


def find_scenario_anchor(
    model: FinancialModel,
    factor: str,
    hint: Optional[str] = None,
) -> AnchorResolution:
    """Resolve one thesis factor to one workbook scenario anchor."""

    if not model._index:
        model.build_index()
    factor_name = str(factor or "").strip()
    if hint:
        owner = _owner_from_hint(model, str(hint))
        if owner is None or (anchor_id := _anchor_id_for_owner(owner)) is None:
            return AnchorResolution(
                factor=factor_name,
                owner_id=str(hint),
                anchor_id=None,
                bull_id=None,
                base_id=None,
                bear_id=None,
                match_reason="invalid_hint",
                candidates=_top_candidate_owner_ids(model, factor_name),
            )
        bull_id, base_id, bear_id = _case_row_ids(model, anchor_id)
        if not all((bull_id, base_id, bear_id)):
            return AnchorResolution(
                factor=factor_name,
                owner_id=str(hint),
                anchor_id=None,
                bull_id=None,
                base_id=None,
                bear_id=None,
                match_reason="invalid_hint",
                candidates=_top_candidate_owner_ids(model, factor_name),
            )
        return AnchorResolution(
            factor=factor_name,
            owner_id=owner.id,
            anchor_id=anchor_id,
            bull_id=bull_id,
            base_id=base_id,
            bear_id=bear_id,
            match_reason="explicit_hint",
            score=None,
        )

    candidates = _candidate_scores(model, factor_name)
    if not candidates:
        return AnchorResolution(
            factor=factor_name,
            owner_id=None,
            anchor_id=None,
            bull_id=None,
            base_id=None,
            bear_id=None,
            match_reason="unresolved",
            candidates=[],
        )
    owner, anchor_id, score = candidates[0]
    if score < 0.30:
        return AnchorResolution(
            factor=factor_name,
            owner_id=None,
            anchor_id=None,
            bull_id=None,
            base_id=None,
            bear_id=None,
            match_reason="unresolved",
            score=score,
            candidates=[
                candidate_owner.id
                for candidate_owner, _candidate_anchor, _candidate_score in candidates[
                    :5
                ]
            ],
        )

    bull_id, base_id, bear_id = _case_row_ids(model, anchor_id)
    if not all((bull_id, base_id, bear_id)):
        return AnchorResolution(
            factor=factor_name,
            owner_id=None,
            anchor_id=None,
            bull_id=None,
            base_id=None,
            bear_id=None,
            match_reason="unresolved",
            score=score,
            candidates=[
                candidate_owner.id
                for candidate_owner, _candidate_anchor, _candidate_score in candidates[
                    :5
                ]
            ],
        )
    return AnchorResolution(
        factor=factor_name,
        owner_id=owner.id,
        anchor_id=anchor_id,
        bull_id=bull_id,
        base_id=base_id,
        bear_id=bear_id,
        match_reason="label_match_low_confidence",
        score=score,
        candidates=[
            candidate_owner.id
            for candidate_owner, _candidate_anchor, _candidate_score in candidates[:5]
        ],
    )


@cache
def _cash_flow_magnitude_inputs() -> frozenset[str]:
    """Schedule amounts whose compiled cash-flow consumer negates them."""

    from .build_financial_schedules import _complete_financial_schedules
    from .templates import load_sia_generic_template

    # FAS lowering precedes schedule construction on the document path. Read
    # the canonical completed formulas, not that still-uncompiled document.
    template = load_sia_generic_template()
    _complete_financial_schedules(template)
    targets: set[str] = set()
    for consumer in template._index.values():
        if not (consumer.required_role or "").startswith("statement.cf."):
            continue
        spec = consumer.projected
        if spec is None or spec.type is not FormulaType.ref:
            continue
        if spec.params.get("negate") is not True:
            continue
        source = line_item_ref_from_obj(spec.params.get("source"))
        if source is not None and source.t == 0:
            targets.add(source.id)
    return frozenset(targets)


def normalize_control_magnitude(target_item_id: str, value: float) -> float:
    """Keep filed/authored outflows in their compiled schedule's magnitude space."""

    numeric = float(value)
    return abs(numeric) if target_item_id in _cash_flow_magnitude_inputs() else numeric


def normalize_value_for_model(
    model: FinancialModel,
    target_item_id: str,
    value: float,
    *,
    value_scale: Literal["display", "model"],
    value_scale_explicit: bool,
    pct_decimal_passthrough: bool = False,
) -> float:
    """Normalize an authored value into model storage scale and sign."""

    target = model.get_item(target_item_id)
    numeric = normalize_control_magnitude(target_item_id, value)
    if value_scale == "model":
        return numeric

    fmt = SIA_FORMATTER.number_format_for(target)
    if fmt in (SIA_FORMATTER.percentage_format, SIA_FORMATTER.ratio_format):
        if pct_decimal_passthrough and abs(numeric) <= 1.0:
            return numeric
        return numeric / 100.0
    if (
        value_scale == "display"
        and value_scale_explicit
        and target.unit is Unit.dollars
        and getattr(target, "model_scale", "units") == "millions"
    ):
        return numeric / 1_000_000.0
    return numeric


def percent_normalize_via_formatter(
    model: FinancialModel,
    target_item_id: str,
    value: float,
    *,
    decimal_passthrough: bool = False,
) -> float:
    """Normalize whole-pct values for rows the renderer formats as pct/ratio."""

    return normalize_value_for_model(
        model,
        target_item_id,
        value,
        value_scale="display",
        value_scale_explicit=False,
        pct_decimal_passthrough=decimal_passthrough,
    )


def _is_pct_or_ratio_target(model: FinancialModel, target_item_id: str) -> bool:
    target = model.get_item(target_item_id)
    fmt = SIA_FORMATTER.number_format_for(target)
    return fmt in (SIA_FORMATTER.percentage_format, SIA_FORMATTER.ratio_format)


def _is_bps_shaped_factor_name(factor_name: str) -> bool:
    normalized = str(factor_name or "").lower()
    return any(pattern.search(normalized) for pattern in _BPS_FACTOR_PATTERNS)


def _is_numeric(value: object) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _base_values_by_period(
    model: FinancialModel,
    factor_payload: dict,
    base_id: str | None,
    projection_periods: list[int],
) -> dict[int, float]:
    if base_id is None:
        return {}
    periods = [int(period) for period in projection_periods]
    base_curve = factor_values_by_period(
        factor_payload,
        "base",
        projection_periods,
        allow_scalar=True,
    )
    explicit_base = (
        {
            int(period): percent_normalize_via_formatter(
                model,
                base_id,
                value,
                decimal_passthrough=True,
            )
            for period, value in base_curve.items()
        }
        if base_curve
        else {}
    )
    if explicit_base and all(period in explicit_base for period in periods):
        return explicit_base

    graph = DependencyGraph()
    graph.build(model)
    results = graph.compute({})
    graph_base = {
        int(period): float(value)
        for period in periods
        if (value := results.get(base_id, {}).get(int(period))) is not None
    }
    graph_base.update(explicit_base)
    return graph_base


def _scenario_ordering_issues(
    *,
    bull_values: dict[int, float],
    base_values: dict[int, float],
    bear_values: dict[int, float],
    strict_through_period: int | None = None,
) -> list[str]:
    return scenario_ordering_issues(
        bull_values=bull_values,
        base_values=base_values,
        bear_values=bear_values,
        strict_through_period=strict_through_period,
        eps=_SCENARIO_ORDERING_EPS,
    )


def factor_values_by_period(
    factor_payload: dict,
    case: str,
    projection_periods: list[int],
    *,
    allow_scalar: bool = False,
) -> dict[int, float] | None:
    """Read an explicit per-period scenario curve from a factor payload."""

    candidates = [
        factor_payload.get(f"{case}_values"),
        factor_payload.get(f"{case}_curve"),
        factor_payload.get(f"{case}_flex_curve"),
    ]
    for container_key in ("values", "curves", "flex_curve", "flex_curves"):
        container = factor_payload.get(container_key)
        if isinstance(container, dict):
            candidates.append(container.get(case))

    periods = [int(period) for period in projection_periods]
    period_set = set(periods)
    for candidate in candidates:
        if isinstance(candidate, dict):
            values: dict[int, float] = {}
            for raw_period, raw_value in candidate.items():
                period_key = _period_key(raw_period)
                if (
                    period_key is None
                    or period_key not in period_set
                    or not _is_numeric(raw_value)
                ):
                    continue
                values[period_key] = float(raw_value)
            if values:
                return values
        if isinstance(candidate, list):
            values = {
                int(period): float(raw_value)
                for period, raw_value in zip(periods, candidate)
                if _is_numeric(raw_value)
            }
            if values:
                return values

    raw_value = factor_payload.get(case)
    if allow_scalar and _is_numeric(raw_value):
        return {period: float(raw_value) for period in periods}
    return None


def build_bridge_operations(
    model: FinancialModel,
    assumptions_by_factor: list[dict],
    terminal_year: int,
    projection_periods: list[int],
    factor_anchor_hints: Optional[dict[str, str]] = None,
) -> tuple[list[Operation], list[AnchorResolution], list[BridgeWarning]]:
    """Build in-memory workbook operations for scenario flex inputs."""

    ops: list[Operation] = []
    resolutions: list[AnchorResolution] = []
    warnings: list[BridgeWarning] = []
    factor_anchor_hints = factor_anchor_hints or {}
    periods = [int(period) for period in projection_periods]
    if not periods:
        warnings.append(
            BridgeWarning(
                kind="period_coverage_gap",
                factor=None,
                field=None,
                detail="projection_periods_empty",
            )
        )

    for factor_payload in assumptions_by_factor:
        factor_name = str(factor_payload.get("factor") or "").strip()
        hint = factor_anchor_hints.get(factor_name)
        resolution = find_scenario_anchor(model, factor_name, hint=hint)
        if resolution.match_reason == "invalid_hint":
            warnings.append(
                BridgeWarning(
                    kind="invalid_hint",
                    factor=factor_name,
                    field=None,
                    detail=str(hint or ""),
                    candidates=list(resolution.candidates),
                )
            )
            resolutions.append(resolution)
            continue
        resolutions.append(resolution)

        if resolution.match_reason == "label_match_low_confidence":
            warnings.append(
                BridgeWarning(
                    kind="low_confidence_factor",
                    factor=factor_name,
                    field=None,
                    detail=f"score={resolution.score:.4f}"
                    if resolution.score is not None
                    else "score=None",
                    candidates=list(resolution.candidates),
                )
            )
        if resolution.match_reason == "unresolved":
            warnings.append(
                BridgeWarning(
                    kind="unresolved_factor",
                    factor=factor_name,
                    field=None,
                    detail="no_scenario_anchor_match",
                    candidates=list(resolution.candidates),
                )
            )
            continue
        if not periods:
            continue

        case_row_ids = {
            "bull": resolution.bull_id,
            "bear": resolution.bear_id,
        }
        if _is_bps_shaped_factor_name(factor_name) and any(
            target_item_id is not None
            and _is_pct_or_ratio_target(model, target_item_id)
            for target_item_id in case_row_ids.values()
        ):
            warnings.append(
                BridgeWarning(
                    kind="unit_shape_mismatch",
                    factor=factor_name,
                    field=None,
                    detail="bps_factor_targets_pct_or_ratio_row",
                )
            )
            continue
        candidate_ops: dict[str, Operation] = {}
        normalized_case_values: dict[str, dict[int, float]] = {}
        for case, target_item_id in case_row_ids.items():
            if target_item_id is None:
                continue
            values_by_period = factor_values_by_period(factor_payload, case, periods)
            if not values_by_period:
                scalar_value = factor_payload.get(case)
                detail = (
                    "scalar_requires_period_curve"
                    if _is_numeric(scalar_value)
                    else "missing_or_non_numeric_curve"
                )
                warnings.append(
                    BridgeWarning(
                        kind="missing_factor_curve",
                        factor=factor_name,
                        field=case,
                        detail=detail,
                    )
                )
                continue
            missing_periods = [
                period for period in periods if period not in values_by_period
            ]
            if missing_periods:
                warnings.append(
                    BridgeWarning(
                        kind="period_coverage_gap",
                        factor=factor_name,
                        field=case,
                        detail=f"curve_missing_periods:{','.join(str(period) for period in missing_periods)}",
                    )
                )
                continue
            normalized_values = {
                period: percent_normalize_via_formatter(
                    model,
                    target_item_id,
                    value,
                    decimal_passthrough=True,
                )
                for period, value in values_by_period.items()
            }
            normalized_case_values[case] = normalized_values
            candidate_ops[case] = _bridge_period_value_operation(
                item_id=target_item_id,
                values=normalized_values,
            )
        if {"bull", "bear"} <= set(normalized_case_values):
            base_values = _base_values_by_period(
                model, factor_payload, resolution.base_id, periods
            )
            ordering_issues = _scenario_ordering_issues(
                bull_values=normalized_case_values["bull"],
                base_values=base_values,
                bear_values=normalized_case_values["bear"],
                strict_through_period=int(terminal_year),
            )
            if ordering_issues:
                warnings.append(
                    BridgeWarning(
                        kind="scenario_ordering_violation",
                        factor=factor_name,
                        field=None,
                        detail=";".join(ordering_issues[:5]),
                    )
                )
                continue
        ops.extend(
            candidate_ops[case] for case in ("bull", "bear") if case in candidate_ops
        )
    return ops, resolutions, warnings
