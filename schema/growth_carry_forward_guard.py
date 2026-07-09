"""Shared BusinessModel growth-driver carry-forward guard."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

from .model_readiness_common import _is_present
from .models import FinancialModel, FormulaSpec, LineItem, ValueProvenance
from .segment_formula_helpers import _is_carry_forward_formula


GROWTH_CARRY_FORWARD_MAX = 0.06
MIN_UNOWNED_PERIODS = 3
GROWTH_CARRY_FORWARD_FIX = (
    "write a full-horizon override series (decaying toward <= bound), wire the segment "
    "into the scenario table, or raise GROWTH_CARRY_FORWARD_MAX via TickerOverrides with rationale"
)


@dataclass(frozen=True)
class GrowthCarryForwardFinding:
    segment_id: str
    driver_item_id: str
    effective_growth_item_id: str
    unowned_periods: list[int]
    carried_rate: float
    bound: float
    min_unowned_periods: int
    scenario_row_item_id: str | None = None
    fix: str = GROWTH_CARRY_FORWARD_FIX


def growth_carry_forward_findings(
    model: FinancialModel,
    *,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
    guard_config: Any | None = None,
) -> list[GrowthCarryForwardFinding]:
    """Return blocking carry-forward findings for partially authored segment growth drivers."""

    if not projection_periods:
        return []

    bound = _guard_bound(guard_config)
    min_unowned_periods = _guard_min_unowned_periods(guard_config)
    normalized_values = _normalize_values(values)
    findings: list[GrowthCarryForwardFinding] = []

    for segment_id, driver_item_id in _segment_revenue_growth_driver_item_ids(model):
        effective_growth_item_id = f"bm.{segment_id}.__growth"
        if effective_growth_item_id not in model._index:
            continue
        effective_growth = model.get_item(effective_growth_item_id)

        driver_item = model.get_item(driver_item_id)
        if _is_carry_forward_formula(effective_growth.projected, effective_growth_item_id):
            owned_periods = _owned_projection_periods(driver_item, projection_periods)
            if owned_periods:
                unowned_periods = [period for period in projection_periods if period not in owned_periods]
                if len(unowned_periods) >= min_unowned_periods:
                    carried_rate = _carried_rate(
                        normalized_values.get(effective_growth_item_id, {}),
                        unowned_periods,
                    )
                    if carried_rate is not None and abs(carried_rate) > bound:
                        findings.append(
                            GrowthCarryForwardFinding(
                                segment_id=segment_id,
                                driver_item_id=driver_item_id,
                                effective_growth_item_id=effective_growth_item_id,
                                unowned_periods=unowned_periods,
                                carried_rate=carried_rate,
                                bound=bound,
                                min_unowned_periods=min_unowned_periods,
                            )
                        )
                        continue

        scenario_finding = _scenario_row_carry_forward_finding(
            model,
            segment_id=segment_id,
            driver_item=driver_item,
            effective_growth=effective_growth,
            effective_growth_item_id=effective_growth_item_id,
            values=normalized_values,
            projection_periods=projection_periods,
            bound=bound,
            min_unowned_periods=min_unowned_periods,
        )
        if scenario_finding is not None:
            findings.append(scenario_finding)

    return findings


def _guard_bound(guard_config: Any | None) -> float:
    value = getattr(guard_config, "growth_carry_forward_max", None)
    if value is None and isinstance(guard_config, dict):
        value = guard_config.get("growth_carry_forward_max")
    if value is None:
        return GROWTH_CARRY_FORWARD_MAX
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return GROWTH_CARRY_FORWARD_MAX
    return numeric if math.isfinite(numeric) and numeric >= 0.0 else GROWTH_CARRY_FORWARD_MAX


def _guard_min_unowned_periods(guard_config: Any | None) -> int:
    value = getattr(guard_config, "growth_carry_forward_min_periods", None)
    if value is None and isinstance(guard_config, dict):
        value = guard_config.get("growth_carry_forward_min_periods")
    if value is None:
        return MIN_UNOWNED_PERIODS
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return MIN_UNOWNED_PERIODS
    return numeric if numeric > 0 else MIN_UNOWNED_PERIODS


def _segment_revenue_growth_driver_item_ids(model: FinancialModel) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    suffix = ".segment_revenue__revenue_growth"
    for item_id in sorted(model._index):
        if not item_id.startswith("bm.") or not item_id.endswith(suffix):
            continue
        segment_id = item_id[len("bm.") : -len(suffix)]
        if segment_id:
            candidates.append((segment_id, item_id))
    return candidates


def _owned_projection_periods(item: LineItem, projection_periods: list[int]) -> set[int]:
    projection_set = {int(period) for period in projection_periods}
    owned: set[int] = set()

    for period, spec in (item.overrides or {}).items():
        period_int = int(period)
        if period_int not in projection_set or _is_synthetic_override(spec):
            continue
        owned.add(period_int)

    if item.projected is not None:
        formula_periods = item.formula_periods
        if formula_periods is None:
            owned.update(projection_set)
        else:
            owned.update(int(period) for period in formula_periods if int(period) in projection_set)

    if item.values is not None:
        for period, cell in item.values.values.items():
            period_int = int(period)
            if period_int not in projection_set or not _is_present(cell.value):
                continue
            if cell.provenance == ValueProvenance.input:
                owned.add(period_int)

    return owned


def _scenario_row_carry_forward_finding(
    model: FinancialModel,
    *,
    segment_id: str,
    driver_item: LineItem,
    effective_growth: LineItem,
    effective_growth_item_id: str,
    values: dict[str, dict[int, float]],
    projection_periods: list[int],
    bound: float,
    min_unowned_periods: int,
) -> GrowthCarryForwardFinding | None:
    for anchor_id in _scenario_anchor_ids(driver_item, effective_growth):
        scenario_row_item_id = _scenario_base_row_item_id(model, anchor_id)
        if scenario_row_item_id is None or scenario_row_item_id not in model._index:
            continue

        scenario_row = model.get_item(scenario_row_item_id)
        owned_periods = _scenario_row_owned_periods(scenario_row, projection_periods)
        if not owned_periods:
            continue
        unowned_periods = [period for period in projection_periods if period not in owned_periods]
        if len(unowned_periods) < min_unowned_periods:
            continue

        carried_rate = _scenario_row_carried_rate(
            values,
            effective_growth_item_id=effective_growth_item_id,
            scenario_row_item_id=scenario_row_item_id,
            unowned_periods=unowned_periods,
        )
        if carried_rate is None or abs(carried_rate) <= bound:
            continue

        return GrowthCarryForwardFinding(
            segment_id=segment_id,
            driver_item_id=driver_item.id,
            effective_growth_item_id=effective_growth_item_id,
            unowned_periods=unowned_periods,
            carried_rate=carried_rate,
            bound=bound,
            min_unowned_periods=min_unowned_periods,
            scenario_row_item_id=scenario_row_item_id,
            fix=_scenario_row_fix(segment_id, scenario_row_item_id),
        )

    return None


def _scenario_anchor_ids(driver_item: LineItem, effective_growth: LineItem) -> list[str]:
    from .scenario_bridge import _anchor_id_for_owner, _is_offset_scenario_owner

    anchor_ids: list[str] = []
    seen: set[str] = set()
    for owner in (driver_item, effective_growth):
        if not _is_offset_scenario_owner(owner):
            continue
        anchor_id = _anchor_id_for_owner(owner)
        if anchor_id is None or anchor_id in seen:
            continue
        anchor_ids.append(anchor_id)
        seen.add(anchor_id)
    return anchor_ids


def _scenario_base_row_item_id(model: FinancialModel, anchor_id: str) -> str | None:
    from .build_scenarios import _find_scenario_value_row

    return _find_scenario_value_row(model, anchor_id, "base")


def _scenario_row_owned_periods(row: LineItem, projection_periods: list[int]) -> set[int]:
    projection_set = {int(period) for period in projection_periods}
    owned: set[int] = set()

    for period, spec in (row.overrides or {}).items():
        period_int = int(period)
        if period_int not in projection_set or _is_synthetic_override(spec):
            continue
        owned.add(period_int)

    if row.projected is not None and not _is_carry_forward_formula(row.projected, row.id):
        formula_periods = row.formula_periods
        if formula_periods is None:
            owned.update(projection_set)
        else:
            owned.update(int(period) for period in formula_periods if int(period) in projection_set)

    if row.values is not None:
        for period, cell in row.values.values.items():
            period_int = int(period)
            if period_int not in projection_set or not _is_present(cell.value):
                continue
            if cell.provenance == ValueProvenance.input:
                owned.add(period_int)

    return owned


def _scenario_row_carried_rate(
    values: dict[str, dict[int, float]],
    *,
    effective_growth_item_id: str,
    scenario_row_item_id: str,
    unowned_periods: list[int],
) -> float | None:
    if effective_growth_item_id in values:
        return _carried_rate(values[effective_growth_item_id], unowned_periods)
    return _carried_rate(values.get(scenario_row_item_id, {}), unowned_periods)


def _scenario_row_fix(segment_id: str, scenario_row_item_id: str) -> str:
    return (
        f"author a full-horizon or explicitly-decaying series on {scenario_row_item_id}; "
        f"the {segment_id} driver reads it via offset_scenario"
    )


def _is_synthetic_override(spec: FormulaSpec) -> bool:
    return getattr(spec, "note", None) == "synthetic"


def _carried_rate(values: dict[int, Any], unowned_periods: list[int]) -> float | None:
    rates: list[float] = []
    for period in unowned_periods:
        value = values.get(int(period))
        if not _is_present(value):
            continue
        rates.append(float(value))
    if not rates:
        return None
    return max(rates, key=lambda value: abs(value))


def _normalize_values(values: dict[str, dict[int, float]] | None) -> dict[str, dict[int, float]]:
    normalized: dict[str, dict[int, float]] = {}
    for item_id, item_values in (values or {}).items():
        normalized[str(item_id)] = {
            int(period): value
            for period, value in (item_values or {}).items()
        }
    return normalized


__all__ = [
    "GROWTH_CARRY_FORWARD_MAX",
    "GROWTH_CARRY_FORWARD_FIX",
    "GrowthCarryForwardFinding",
    "MIN_UNOWNED_PERIODS",
    "growth_carry_forward_findings",
]
