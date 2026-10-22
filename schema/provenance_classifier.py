"""Frozen BM-6 slice-7 originless-cell classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

from .models import LineItemRef
from .presentation_captions import WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS


OriginlessClassification = Literal[
    "N2(i)",
    "N2(ii).discount_convention",
    "N2(ii).scenario_control",
    "N2(ii).mechanical_constant",
    "N2(ii).presentation_scaffold_value",
    "N2(iii)",
]

REGISTRY_DISCOUNT = frozenset({"tpl.v.dcf.discount_period"})
REGISTRY_SCENARIO = frozenset(
    {
        "tpl.a.header.scenario_value",
        "tpl.a.scenario_tables.margin_base",
        "tpl.a.scenario_tables.margin_bear",
        "tpl.a.scenario_tables.margin_bull",
        "tpl.a.scenario_tables.volume_base",
        "tpl.a.scenario_tables.volume_bear",
        "tpl.a.scenario_tables.volume_bull",
        "tpl.v.dcf.exit_multiple_base",
        "tpl.v.dcf.exit_multiple_bear",
        "tpl.v.dcf.exit_multiple_bull",
        "tpl.v.dcf.terminal_growth_base",
        "tpl.v.dcf.terminal_growth_bear",
        "tpl.v.dcf.terminal_growth_bull",
    }
)
REGISTRY_MECHANICAL = frozenset(
    {
        "tpl.s.earnings_scenarios.year_1",
        "tpl.s.earnings_scenarios.year_2",
        "tpl.s.earnings_scenarios.year_3",
        "tpl.s.earnings_scenarios.year_4",
    }
)
REGISTRY_SCAFFOLD = frozenset(WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS)

COMPILER_ORIGIN_REGISTRIES: Mapping[str, frozenset[str]] = {
    "discount_convention": REGISTRY_DISCOUNT,
    "scenario_control": REGISTRY_SCENARIO,
    "mechanical_constant": REGISTRY_MECHANICAL,
    "presentation_scaffold_value": REGISTRY_SCAFFOLD,
}


def assert_disjoint_compiler_origin_registries(
    registries: Mapping[str, frozenset[str]] = COMPILER_ORIGIN_REGISTRIES,
) -> None:
    names = tuple(registries)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            overlap = registries[left] & registries[right]
            if overlap:
                raise ValueError(
                    f"compiler-origin registries {left!r}/{right!r} overlap: "
                    f"{sorted(overlap)!r}"
                )


assert_disjoint_compiler_origin_registries()




@dataclass(frozen=True)
class OriginlessClassifierInput:
    state: str
    item_id: str
    consumes_financial_value: bool


def classify_originless_cell(value: OriginlessClassifierInput) -> OriginlessClassification:
    """Apply the frozen ordered R1 total classifier; first match wins."""

    if value.state != "known":
        return "N2(iii)"
    if value.consumes_financial_value:
        return "N2(i)"
    if value.item_id in REGISTRY_DISCOUNT:
        return "N2(ii).discount_convention"
    if value.item_id in REGISTRY_SCENARIO:
        return "N2(ii).scenario_control"
    if value.item_id in REGISTRY_MECHANICAL:
        return "N2(ii).mechanical_constant"
    if value.item_id in REGISTRY_SCAFFOLD:
        return "N2(ii).presentation_scaffold_value"
    return "N2(i)"


_FINANCIAL_PREFIXES = ("tpl.fm.", "tpl.a.", "bm.", "fm.semantic.")
_RECONSTRUCTIONS = frozenset(
    {
        (
            "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period",
            2021,
        )
    }
)


def financial_dependency_flags(
    model: Any,
    *,
    specs: Mapping[tuple[str, int], Any],
) -> Mapping[tuple[str, int], bool]:
    """Return immutable transitive financial-input flags for classifier step 2."""

    periods = tuple(
        int(period)
        for period in (
            *model.time_structure.historical_periods,
            *model.time_structure.projection_periods,
        )
    )
    last_period = periods[-1] if periods else None
    presentation_ids = {
        item.id
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
        if section.id == "header" or item.id in WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS
    }

    def pairs(value: Any, period: int) -> tuple[tuple[str, int], ...]:
        found: list[tuple[str, int]] = []
        if isinstance(value, LineItemRef):
            if value.period_anchor == "last":
                if last_period is not None:
                    target_index = periods.index(last_period) + int(value.t)
                else:
                    target_index = -1
            else:
                try:
                    target_index = periods.index(period) + int(value.t)
                except ValueError:
                    target_index = -1
            if 0 <= target_index < len(periods):
                found.append((value.id, periods[target_index]))
        elif isinstance(value, dict):
            for nested in value.values():
                found.extend(pairs(nested, period))
        elif isinstance(value, (list, tuple)):
            for nested in value:
                found.extend(pairs(nested, period))
        return tuple(dict.fromkeys(found))

    memo: dict[tuple[str, int], bool] = {}

    def consumes(identity: tuple[str, int], visiting: frozenset[tuple[str, int]]) -> bool:
        if identity in _RECONSTRUCTIONS:
            return True
        if identity in memo:
            return memo[identity]
        if identity in visiting:
            return False
        spec = specs.get(identity)
        if spec is None:
            memo[identity] = False
            return False
        for dependency in pairs(spec.params, identity[1]):
            dependency_id = dependency[0]
            if dependency_id.startswith(_FINANCIAL_PREFIXES) and dependency_id not in presentation_ids:
                memo[identity] = True
                return True
            if consumes(dependency, visiting | {identity}):
                memo[identity] = True
                return True
        memo[identity] = False
        return False

    return {
        identity: consumes(identity, frozenset())
        for identity in specs
    } | {identity: True for identity in _RECONSTRUCTIONS}


__all__ = [
    "COMPILER_ORIGIN_REGISTRIES",
    "OriginlessClassifierInput",
    "OriginlessClassification",
    "REGISTRY_DISCOUNT",
    "REGISTRY_MECHANICAL",
    "REGISTRY_SCAFFOLD",
    "REGISTRY_SCENARIO",
    "assert_disjoint_compiler_origin_registries",
    "classify_originless_cell",
    "financial_dependency_flags",
]
