"""Canonical rendering projection curves and target resolution.

The projection result is not a persistence format and never decodes or emits
the retired ``TickerOverrides.projections`` representation.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Literal, Mapping

from .models import FinancialModel, FormulaSpec, FormulaType
from .refs import line_item_ref_from_obj


CanonicalProjectionScenario = Literal["base", "bull", "bear"]
AUTHENTICATED_SCENARIO_PROJECTION_NOTE = (
    "authenticated_control_snapshot_scenario_projection"
)

# This is the template contract for scenario rendering.  It is deliberately a
# closed semantic topology rather than a label, row-offset, or suffix search.
# Every offset-scenario owner must name one of these exact anchors and every
# rendered case resolves to one exact template item.
CANONICAL_SCENARIO_RENDER_TARGETS: Mapping[
    str,
    Mapping[CanonicalProjectionScenario, str],
] = MappingProxyType(
    {
        "tpl.a.scenario_tables.scenario_volume_growth_label": MappingProxyType(
            {
                "bull": "tpl.a.scenario_tables.volume_bull",
                "base": "tpl.a.scenario_tables.volume_base",
                "bear": "tpl.a.scenario_tables.volume_bear",
            }
        ),
        "tpl.a.scenario_tables.scenario_margin_label": MappingProxyType(
            {
                "bull": "tpl.a.scenario_tables.margin_bull",
                "base": "tpl.a.scenario_tables.margin_base",
                "bear": "tpl.a.scenario_tables.margin_bear",
            }
        ),
        "tpl.v.dcf.exit_multiple_scenario_table_anchor": MappingProxyType(
            {
                "bull": "tpl.v.dcf.exit_multiple_bull",
                "base": "tpl.v.dcf.exit_multiple_base",
                "bear": "tpl.v.dcf.exit_multiple_bear",
            }
        ),
        "tpl.v.dcf.terminal_growth_scenario_table_anchor": MappingProxyType(
            {
                "bull": "tpl.v.dcf.terminal_growth_bull",
                "base": "tpl.v.dcf.terminal_growth_base",
                "bear": "tpl.v.dcf.terminal_growth_bear",
            }
        ),
    }
)


@dataclass(frozen=True, slots=True)
class CanonicalProjectionCurve:
    """One renderer-facing curve keyed by stable identities."""

    domain: Literal["forecast", "scenario"]
    driver_id: str | None
    control_id: str
    binding_id: str
    template_item_id: str
    scenario: CanonicalProjectionScenario
    values: tuple[tuple[int, float], ...]
    rationale: str
    confidence: Literal["low", "medium", "high"]
    held_at_base: bool
    disclosed_guidance_horizon: int | None
    extension_basis_by_period: tuple[tuple[int, str | None], ...]

    def __post_init__(self) -> None:
        if self.domain == "forecast" and (
            self.scenario != "base" or self.driver_id is None
        ):
            raise ValueError("forecast curve requires base scenario and driver_id")
        if self.domain == "scenario" and self.driver_id is not None:
            raise ValueError("scenario curve forbids driver_id")
        if not self.values:
            raise ValueError("canonical projection curve requires valued periods")
        years = tuple(year for year, _value in self.values)
        if years != tuple(sorted(set(years))):
            raise ValueError("canonical projection years must be sorted and unique")
        if any(not math.isfinite(value) for _year, value in self.values):
            raise ValueError("canonical projection values must be finite")
        extension_years = tuple(year for year, _basis in self.extension_basis_by_period)
        if extension_years != years:
            raise ValueError("extension basis must cover the exact valued periods")

    def to_dict(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "driver_id": self.driver_id,
            "control_id": self.control_id,
            "binding_id": self.binding_id,
            "template_item_id": self.template_item_id,
            "scenario": self.scenario,
            "values": [
                {"fiscal_year": year, "value": value}
                for year, value in self.values
            ],
            "rationale": self.rationale,
            "confidence": self.confidence,
            "held_at_base": self.held_at_base,
            "disclosed_guidance_horizon": self.disclosed_guidance_horizon,
            "extension_basis_by_period": [
                {"fiscal_year": year, "basis": basis}
                for year, basis in self.extension_basis_by_period
            ],
        }


def _authenticated_scenario_target_set(
    spec: FormulaSpec,
) -> tuple[str, Mapping[CanonicalProjectionScenario, str]]:
    """Resolve an exact registered topology from its currently resolved cases."""

    case_params = {
        name: value for name, value in (spec.params or {}).items()
        if name != "selector"
    }
    actual_targets = {
        case: ref.id
        for case, value in case_params.items()
        if (ref := line_item_ref_from_obj(value)) is not None
    }
    matching_target_sets = tuple(
        (anchor_id, targets)
        for anchor_id, targets in CANONICAL_SCENARIO_RENDER_TARGETS.items()
        if actual_targets
        and len(actual_targets) == len(case_params)
        and actual_targets == {
            case: target for case, target in targets.items() if case in actual_targets
        }
    )
    if len(matching_target_sets) != 1:
        raise ValueError(
            "authenticated scenario owner does not retain one exact target set"
        )
    return matching_target_sets[0]


def canonical_projection_render_target(
    model: FinancialModel,
    curve: CanonicalProjectionCurve,
) -> str:
    """Resolve one exact renderer target without labels or coordinate inference."""

    try:
        owner = model.get_item(curve.template_item_id)
    except KeyError as exc:
        raise ValueError(
            "canonical compile binding targets an absent template item: "
            f"{curve.template_item_id}"
        ) from exc
    if curve.domain == "forecast":
        if curve.scenario != "base":
            raise ValueError("forecast curve must render to its base owner")
        return owner.id

    spec = owner.projected
    if (
        spec is not None
        and spec.type is FormulaType.valuation
        and spec.subtype == "scenario_select"
        and spec.note == AUTHENTICATED_SCENARIO_PROJECTION_NOTE
    ):
        _anchor_id, targets = _authenticated_scenario_target_set(spec)
        return targets[curve.scenario]
    if (
        spec is None
        or spec.type is not FormulaType.valuation
        or spec.subtype != "offset_scenario"
    ):
        raise ValueError(
            "scenario compile binding must target an exact offset-scenario owner"
        )
    anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
    if anchor_ref is None:
        raise ValueError("scenario compile owner has no exact semantic anchor")
    case_targets = CANONICAL_SCENARIO_RENDER_TARGETS.get(anchor_ref.id)
    if case_targets is None:
        raise ValueError(
            "scenario compile owner references an unregistered semantic anchor"
        )
    target_id = case_targets[curve.scenario]
    try:
        target = model.get_item(target_id)
    except KeyError as exc:
        raise ValueError(
            "registered scenario render target is absent from the selected template"
        ) from exc
    if target.id != target_id:
        raise ValueError("scenario render target identity drifted")
    return target_id


def canonical_scenario_projection_owner(
    model: FinancialModel,
    base_target_item_id: str,
) -> str:
    """Resolve a forecast base target to its unique scenario-select owner.

    Forecast assumptions may write a registered Base-case leaf while scenario
    curves must bind the exact offset-scenario owner.  The reverse lookup is
    structural: one registered semantic anchor must own the supplied Base leaf
    and exactly one model row must reference that anchor.
    """

    owner = canonical_scenario_projection_owner_or_none(
        model,
        base_target_item_id,
    )
    if owner is None:
        raise ValueError(
            "forecast target does not identify one registered scenario base leaf: "
            f"{base_target_item_id}"
        )
    return owner


def canonical_scenario_projection_owner_or_none(
    model: FinancialModel,
    base_target_item_id: str,
) -> str | None:
    """Return the exact scenario-table owner, or ``None`` for a direct input.

    A ForecastAssumptionSet may flex controls that are valid compiler inputs but
    do not own one of the template's bounded Bull/Base/Bear tables.  Those
    controls are evaluated through exact per-scenario direct inputs; malformed or
    ambiguous scenario-table ownership still fails closed here.
    """

    try:
        target = model.get_item(base_target_item_id)
    except KeyError as exc:
        raise ValueError(
            "scenario forecast target is absent from the selected template: "
            f"{base_target_item_id}"
        ) from exc
    projected = target.projected
    if (
        projected is not None
        and projected.type is FormulaType.valuation
        and projected.subtype == "offset_scenario"
    ):
        anchor = line_item_ref_from_obj((projected.params or {}).get("anchor"))
        if anchor is None or anchor.id not in CANONICAL_SCENARIO_RENDER_TARGETS:
            raise ValueError(
                "scenario projection owner references an unregistered semantic anchor"
            )
        return target.id

    anchors = tuple(
        anchor_id
        for anchor_id, case_targets in CANONICAL_SCENARIO_RENDER_TARGETS.items()
        if case_targets["base"] == base_target_item_id
    )
    if not anchors:
        return None
    if len(anchors) != 1:
        raise ValueError(
            "forecast target identifies multiple registered scenario base leaves: "
            f"{base_target_item_id}"
        )
    anchor_id = anchors[0]
    owners: list[str] = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                spec = item.projected
                if spec is None or spec.type is not FormulaType.valuation:
                    continue
                if (
                    spec.subtype == "scenario_select"
                    and spec.note == AUTHENTICATED_SCENARIO_PROJECTION_NOTE
                ):
                    owner_anchor, _targets = _authenticated_scenario_target_set(spec)
                    if owner_anchor == anchor_id:
                        owners.append(item.id)
                elif spec.subtype == "offset_scenario":
                    anchor = line_item_ref_from_obj((spec.params or {}).get("anchor"))
                    if anchor is not None and anchor.id == anchor_id:
                        owners.append(item.id)
    if len(owners) != 1:
        raise ValueError(
            "registered scenario base leaf does not have one exact owner: "
            f"{base_target_item_id}"
        )
    return owners[0]


def validate_canonical_projection_target_uniqueness(
    model: FinancialModel,
    curves: tuple[CanonicalProjectionCurve, ...],
) -> None:
    """Reject curves that would overwrite the same renderer cell."""

    owner_by_write_key: dict[
        tuple[str, CanonicalProjectionScenario, int],
        CanonicalProjectionCurve,
    ] = {}
    for curve in curves:
        target_id = canonical_projection_render_target(model, curve)
        for fiscal_year, _value in curve.values:
            write_key = (target_id, curve.scenario, fiscal_year)
            previous = owner_by_write_key.get(write_key)
            if previous is not None:
                raise ValueError(
                    "canonical projection curves collide on the same renderer "
                    "target, scenario, and fiscal period: "
                    f"{target_id}/{curve.scenario}/{fiscal_year} "
                    f"({previous.control_id}, {curve.control_id})"
                )
            owner_by_write_key[write_key] = curve


__all__ = [
    "AUTHENTICATED_SCENARIO_PROJECTION_NOTE",
    "CANONICAL_SCENARIO_RENDER_TARGETS",
    "CanonicalProjectionCurve",
    "CanonicalProjectionScenario",
    "canonical_projection_render_target",
    "canonical_scenario_projection_owner",
    "canonical_scenario_projection_owner_or_none",
    "validate_canonical_projection_target_uniqueness",
]
