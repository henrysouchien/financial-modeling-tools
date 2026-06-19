"""Build XBRL presentation trees from EDGAR financials payloads."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from typing import Dict, Mapping

from .source_values import (
    NORMALIZED_UNIT_MILLIONS,
    SourceValue,
    choose_preferred_source_value,
    normalize_edgar_source_value,
)

_BS_ROLE_PATTERNS = (
    "consolidatedbalancesheets",
    "statementbalancesheets",
    "statementconsolidatedbalancesheets",
    "balancesheets",
    "financialcondition",
    "financialposition",
)
_BS_ROLE_REJECT_PATTERNS = ("parenthetical", "details", "schedule")

@dataclass(frozen=True)
class PresentationObservation:
    """Per-year value twins plus presentation sign metadata for a child."""

    current_value: float | None
    current_period_value: float | None
    preferred_label_role: str | None
    calculation_weight: float | None


@dataclass(frozen=True)
class PresentationChild:
    tag: str
    order: float
    value_by_year: Mapping[int, float | None]
    scale_precedence_by_year: Mapping[int, tuple[int, int]] = field(default_factory=dict)
    source_value_by_year: Mapping[int, SourceValue] = field(default_factory=dict)
    observation_by_year: Mapping[int, PresentationObservation] = field(default_factory=dict)


@dataclass
class PresentationTree:
    role: str = "consolidatedbalancesheets"
    immediate_children_by_parent: Mapping[str, tuple[PresentationChild, ...]] = field(default_factory=dict)

    def immediate_children_of(self, parent_tag: str) -> tuple[PresentationChild, ...]:
        return self.immediate_children_by_parent.get(parent_tag, ())


def _discover_bs_role(facts: list[dict]) -> str | None:
    """Find the role label this filing uses for the balance-sheet face."""

    role_scores: dict[str, int] = {}
    bs_parent_substrings = ("Assets", "Liabilities", "StockholdersEquity")
    for fact in facts:
        if fact.get("axis_key") != "__NONE__":
            continue
        for entry in fact.get("presentation_hierarchy", []):
            role = entry.get("role", "")
            if not role:
                continue
            role_norm = role.lower().replace("role_", "")
            if any(pattern in role_norm for pattern in _BS_ROLE_REJECT_PATTERNS):
                continue
            if not any(pattern in role_norm for pattern in _BS_ROLE_PATTERNS):
                continue
            parent = entry.get("parent", "")
            if any(substring in parent for substring in bs_parent_substrings):
                role_scores[role] = role_scores.get(role, 0) + 1

    if not role_scores:
        return None
    return max(role_scores.items(), key=lambda kv: kv[1])[0]


def build_presentation_tree(
    facts: list[dict],
    *,
    year: int,
    role: str | None = None,
    existing_tree: PresentationTree | None = None,
) -> PresentationTree:
    """Build or merge a presentation tree from one filing year's EDGAR facts."""

    selected_role = role or _discover_bs_role(facts)
    if selected_role is None:
        return existing_tree or PresentationTree(role="", immediate_children_by_parent={})

    children_by_parent: dict[str, dict[str, PresentationChild]] = defaultdict(dict)
    if existing_tree is not None:
        for parent, children in existing_tree.immediate_children_by_parent.items():
            for child in children:
                children_by_parent[parent][child.tag] = child

    for fact in facts:
        if fact.get("axis_key") != "__NONE__":
            continue
        for entry in fact.get("presentation_hierarchy", []):
            if entry.get("role") != selected_role:
                continue
            parent = entry.get("parent")
            if parent is None:
                continue
            tag = fact["tag"]
            order = float(entry.get("order") or 0.0)
            raw_value = fact.get("current_value")
            if raw_value is None:
                raw_value = fact.get("current_period_value")
            scale = fact.get("scale")
            incoming_source_value = normalize_edgar_source_value(
                raw_value,
                scale,
                concept_id="",
                source="edgar",
                tag=tag,
                year=year,
                source_ref=fact.get("source_ref"),
            )
            incoming_precedence = incoming_source_value.scale_precedence
            incoming_value = incoming_source_value.normalized_value
            current_source_value = normalize_edgar_source_value(
                fact.get("current_value"),
                scale,
                concept_id="",
                source="edgar",
                tag=tag,
                year=year,
                source_ref=fact.get("source_ref"),
            )
            current_period_source_value = normalize_edgar_source_value(
                fact.get("current_period_value"),
                scale,
                concept_id="",
                source="edgar",
                tag=tag,
                year=year,
                source_ref=fact.get("source_ref"),
            )
            incoming_observation = None
            calculation_weight = entry.get("calculation_weight")
            if (
                current_source_value.normalized_value is not None
                or current_period_source_value.normalized_value is not None
            ):
                incoming_observation = PresentationObservation(
                    current_value=current_source_value.normalized_value,
                    current_period_value=current_period_source_value.normalized_value,
                    preferred_label_role=entry.get("preferred_label_role"),
                    calculation_weight=(
                        None if calculation_weight is None else float(calculation_weight)
                    ),
                )

            existing = children_by_parent[parent].get(tag)
            if existing is None:
                observation_by_year = (
                    {year: incoming_observation}
                    if incoming_observation is not None
                    else {}
                )
                children_by_parent[parent][tag] = PresentationChild(
                    tag=tag,
                    order=order,
                    value_by_year={year: incoming_value},
                    scale_precedence_by_year={year: incoming_precedence},
                    source_value_by_year={year: incoming_source_value},
                    observation_by_year=observation_by_year,
                )
            else:
                existing_source_value = existing.source_value_by_year.get(year)
                if existing_source_value is None and year in existing.value_by_year:
                    existing_source_value = SourceValue(
                        normalized_value=existing.value_by_year.get(year),
                        normalized_unit=NORMALIZED_UNIT_MILLIONS,
                        raw_value=None,
                        raw_scale=None,
                        scale_precedence=existing.scale_precedence_by_year.get(year, (1, 0)),
                        source="edgar",
                        tag=tag,
                        year=year,
                    )
                chosen_source_value = choose_preferred_source_value(
                    existing_source_value,
                    incoming_source_value,
                )

                if chosen_source_value is incoming_source_value:
                    merged_values = {**existing.value_by_year, year: incoming_value}
                    merged_precedence = {**existing.scale_precedence_by_year, year: incoming_precedence}
                    merged_source_values = {
                        **existing.source_value_by_year,
                        year: incoming_source_value,
                    }
                    merged_observations = dict(existing.observation_by_year)
                    if incoming_observation is not None:
                        merged_observations[year] = incoming_observation
                    else:
                        merged_observations.pop(year, None)
                else:
                    merged_values = dict(existing.value_by_year)
                    merged_precedence = dict(existing.scale_precedence_by_year)
                    merged_source_values = dict(existing.source_value_by_year)
                    merged_observations = dict(existing.observation_by_year)
                    if existing_source_value is not None and year not in merged_source_values:
                        merged_source_values[year] = existing_source_value
                children_by_parent[parent][tag] = replace(
                    existing,
                    value_by_year=merged_values,
                    scale_precedence_by_year=merged_precedence,
                    source_value_by_year=merged_source_values,
                    observation_by_year=merged_observations,
                )
            break

    return PresentationTree(
        role=selected_role,
        immediate_children_by_parent={
            parent: tuple(sorted(children.values(), key=lambda child: child.order))
            for parent, children in children_by_parent.items()
        },
    )


def _accumulate_tree(payloads_by_year: Dict[int, dict]) -> PresentationTree | None:
    """Build a PresentationTree by merging year-keyed /api/financials payloads."""

    tree = None
    for year in sorted(payloads_by_year):
        payload = payloads_by_year.get(year)
        if not payload:
            continue
        facts = payload.get("facts") or []
        if not facts:
            continue
        tree = build_presentation_tree(facts, year=year, existing_tree=tree)
    return tree
