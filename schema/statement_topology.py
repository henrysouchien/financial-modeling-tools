"""Immutable statement topology derived from financials or occurrence payloads."""

from __future__ import annotations

from collections import defaultdict, deque
import copy
from dataclasses import dataclass, replace
import hashlib
import json
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Literal, Mapping

from .source_values import (
    SourceValue,
    choose_preferred_source_value,
    normalize_edgar_source_value,
)


TopologyEdgeKind = Literal["calculation", "presentation"]


def normalize_topology_role(role: Any) -> str:
    text = str(role or "").strip()
    if not text:
        return ""
    text = text.rsplit("/", 1)[-1]
    if text.lower().startswith("role_"):
        text = text[5:]
    return "".join(character.lower() for character in text if character.isalnum())


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(child) for key, child in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(child) for child in value)
    return copy.deepcopy(value)


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(child) for child in value]
    return copy.deepcopy(value)


def _source_value_to_dict(source_value: SourceValue) -> dict[str, Any]:
    return {
        "normalized_value": source_value.normalized_value,
        "normalized_unit": source_value.normalized_unit,
        "raw_value": _thaw_json(source_value.raw_value),
        "raw_scale": source_value.raw_scale,
        "scale_precedence": list(source_value.scale_precedence),
        "source": source_value.source,
        "tag": source_value.tag,
        "year": source_value.year,
        "source_ref": _thaw_json(source_value.source_ref or {}),
    }


def freeze_payloads_by_year(
    payloads_by_year: Mapping[int, Mapping[str, Any]] | None,
) -> Mapping[int, Mapping[str, Any]]:
    return MappingProxyType(
        {
            int(year): _freeze_json(payload)
            for year, payload in sorted((payloads_by_year or {}).items())
        }
    )


def financials_payloads_fingerprint(
    payloads_by_year: Mapping[int, Mapping[str, Any]] | None,
) -> str:
    payload = {
        str(int(year)): _thaw_json(year_payload)
        for year, year_payload in sorted((payloads_by_year or {}).items())
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def thaw_financials_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a mutable JSON-compatible copy of one frozen payload snapshot."""

    return _thaw_json(payload)


@dataclass(frozen=True)
class TopologyEdge:
    year: int
    role: str
    edge_kind: TopologyEdgeKind
    parent_tag: str
    child_tag: str
    order: float
    weight: float | None
    source_value: SourceValue
    source_ref: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "year": self.year,
            "role": self.role,
            "edge_kind": self.edge_kind,
            "parent_tag": self.parent_tag,
            "child_tag": self.child_tag,
            "order": self.order,
            "weight": self.weight,
        }
        payload.update(_source_value_to_dict(self.source_value))
        payload["source_ref"] = _thaw_json(self.source_ref or {})
        return payload


@dataclass(frozen=True)
class DiscardedTopologyObservation:
    year: int
    role: str
    edge_kind: TopologyEdgeKind
    parent_tag: str
    child_tag: str
    kept_scale: str | None
    discarded_scale: str | None
    kept_value: float | None
    discarded_value: float | None
    kept_source_ref: Mapping[str, Any] | None = None
    discarded_source_ref: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "year": self.year,
            "role": self.role,
            "edge_kind": self.edge_kind,
            "parent_tag": self.parent_tag,
            "child_tag": self.child_tag,
            "kept_scale": self.kept_scale,
            "discarded_scale": self.discarded_scale,
            "kept_value": self.kept_value,
            "discarded_value": self.discarded_value,
            "kept_source_ref": _thaw_json(self.kept_source_ref or {}),
            "discarded_source_ref": _thaw_json(self.discarded_source_ref or {}),
        }


@dataclass(frozen=True)
class PeriodFact:
    """The period coordinates the producer declares for one (year, tag) fact.

    Both fields arrive on the ``/api/financials`` wire and are recorded
    verbatim--normalized only by case/whitespace--so a consumer that needs the
    distinction fails closed on an undeclared or unrecognized token rather
    than assuming one.
    ``PresentationLineRef.period_type`` (``schema/correspondence.py``) is
    constructor-filled from here, which is what makes ``period_type_mismatch``
    rejectable at proposal closure.
    """

    period_type: str | None = None
    date_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"period_type": self.period_type, "date_type": self.date_type}


@dataclass(frozen=True)
class StatementTopology:
    edges_by_year: Mapping[int, tuple[TopologyEdge, ...]]
    source_values_by_year: Mapping[int, Mapping[str, SourceValue]]
    discarded_duplicate_observations: tuple[DiscardedTopologyObservation, ...] = ()
    source_payloads_fingerprint: str = ""
    period_facts_by_year: Mapping[int, Mapping[str, PeriodFact]] = MappingProxyType({})

    def _roles(self, roles: list[str] | tuple[str, ...] | set[str]) -> set[str]:
        return {normalize_topology_role(role) for role in roles}

    def period_fact(self, tag: str, year: int) -> PeriodFact | None:
        """The declared period coordinates of the observation kept for (year, tag)."""

        return self.period_facts_by_year.get(int(year), {}).get(tag)

    def children_of(
        self,
        parent_tag: str,
        year: int,
        *,
        edge_kind: TopologyEdgeKind,
        roles: list[str] | tuple[str, ...] | set[str],
    ) -> tuple[TopologyEdge, ...]:
        accepted_roles = self._roles(roles)
        return tuple(
            edge
            for edge in self.edges_by_year.get(int(year), ())
            if edge.edge_kind == edge_kind
            and edge.parent_tag == parent_tag
            and edge.role in accepted_roles
        )

    def descendants_of(
        self,
        parent_tag: str,
        year: int,
        *,
        edge_kind: TopologyEdgeKind,
        roles: list[str] | tuple[str, ...] | set[str],
        allow_role_transitions: bool = False,
    ) -> tuple[TopologyEdge, ...]:
        accepted_roles = self._roles(roles)
        queue: deque[tuple[str, str | None]] = deque([(parent_tag, None)])
        seen_states: set[tuple[str, str | None]] = set()
        found: list[TopologyEdge] = []
        seen_edges: set[tuple[str, str, str]] = set()
        while queue:
            current_tag, path_role = queue.popleft()
            state = (current_tag, path_role)
            if state in seen_states:
                continue
            seen_states.add(state)
            for edge in self.edges_by_year.get(int(year), ()):
                if edge.edge_kind != edge_kind or edge.parent_tag != current_tag:
                    continue
                if edge.role not in accepted_roles:
                    continue
                if (
                    path_role is not None
                    and not allow_role_transitions
                    and edge.role != path_role
                ):
                    continue
                edge_key = (edge.role, edge.parent_tag, edge.child_tag)
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    found.append(edge)
                queue.append(
                    (
                        edge.child_tag,
                        edge.role if path_role is None else path_role,
                    )
                )
        return tuple(found)

    def path_between(
        self,
        ancestor_tag: str,
        descendant_tag: str,
        year: int,
        *,
        edge_kind: TopologyEdgeKind,
        roles: list[str] | tuple[str, ...] | set[str],
        allow_role_transitions: bool = False,
    ) -> tuple[TopologyEdge, ...] | None:
        accepted_roles = self._roles(roles)
        queue: deque[tuple[str, str | None, tuple[TopologyEdge, ...]]] = deque(
            [(ancestor_tag, None, ())]
        )
        seen: set[tuple[str, str | None]] = set()
        while queue:
            current_tag, path_role, path = queue.popleft()
            state = (current_tag, path_role)
            if state in seen:
                continue
            seen.add(state)
            for edge in self.edges_by_year.get(int(year), ()):
                if edge.edge_kind != edge_kind or edge.parent_tag != current_tag:
                    continue
                if edge.role not in accepted_roles:
                    continue
                if (
                    path_role is not None
                    and not allow_role_transitions
                    and edge.role != path_role
                ):
                    continue
                next_path = (*path, edge)
                if edge.child_tag == descendant_tag:
                    return next_path
                queue.append(
                    (
                        edge.child_tag,
                        edge.role if path_role is None else path_role,
                        next_path,
                    )
                )
        return None

    def relationship(
        self,
        parent_tag: str,
        child_tag: str,
        year: int,
        *,
        edge_kind: TopologyEdgeKind,
        role: str,
    ) -> TopologyEdge | None:
        normalized_role = normalize_topology_role(role)
        return next(
            (
                edge
                for edge in self.edges_by_year.get(int(year), ())
                if edge.edge_kind == edge_kind
                and edge.parent_tag == parent_tag
                and edge.child_tag == child_tag
                and edge.role == normalized_role
            ),
            None,
        )

    def additive_children_of(
        self,
        total_tag: str,
        year: int,
        *,
        calculation_roles: list[str] | tuple[str, ...] | set[str],
    ) -> tuple[TopologyEdge, ...]:
        return self.children_of(
            total_tag,
            year,
            edge_kind="calculation",
            roles=calculation_roles,
        )

    def presentation_children_of(
        self,
        section_abstract_tag: str,
        year: int,
        *,
        presentation_roles: list[str] | tuple[str, ...] | set[str],
    ) -> tuple[TopologyEdge, ...]:
        return self.children_of(
            section_abstract_tag,
            year,
            edge_kind="presentation",
            roles=presentation_roles,
        )

    def source_value(self, tag: str, year: int) -> SourceValue | None:
        return self.source_values_by_year.get(int(year), {}).get(tag)

    def to_dict(self) -> dict[str, Any]:
        return {
            "edges_by_year": {
                str(year): [edge.to_dict() for edge in edges]
                for year, edges in sorted(self.edges_by_year.items())
            },
            "source_values_by_year": {
                str(year): {
                    tag: _source_value_to_dict(source_value)
                    for tag, source_value in sorted(values.items())
                }
                for year, values in sorted(self.source_values_by_year.items())
            },
            "discarded_duplicate_observations": [
                observation.to_dict()
                for observation in self.discarded_duplicate_observations
            ],
            "source_payloads_fingerprint": self.source_payloads_fingerprint,
            "period_facts_by_year": {
                str(year): {
                    tag: period_fact.to_dict()
                    for tag, period_fact in sorted(period_facts.items())
                }
                for year, period_facts in sorted(self.period_facts_by_year.items())
            },
        }


def _relationship_entries(container: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Read occurrence arcs or legacy complete/flattened financials relationships."""

    if "calculation" in container:
        calculation = container["calculation"]
        if not isinstance(calculation, Mapping):
            return []
        return [{
            "parent": calculation.get("parent_qname"),
            "role": calculation.get("role_uri"),
            "weight": calculation.get("weight"),
            "order": calculation.get("order"),
        }]

    raw = container.get("calculation_relationships")
    if isinstance(raw, Mapping):
        entries = [raw]
    elif isinstance(raw, (list, tuple)):
        entries = [entry for entry in raw if isinstance(entry, Mapping)]
    else:
        entries = []

    flattened_parent = container.get("calculation_parent")
    if flattened_parent:
        entries.append(
            {
                "parent": flattened_parent,
                "role": container.get("calculation_role") or container.get("role"),
                "weight": container.get("calculation_weight"),
                "order": container.get("calculation_order") or container.get("order"),
            }
        )
    return entries


def occurrence_namespace_prefixes(
    qnames: Iterable[tuple[str, str]],
) -> dict[str, str]:
    """Resolve Clark names using only prefixes declared in this filing's QNames.

    A taxonomy-year namespace versions a concept, not its identity: an
    ``AssetsAbstract`` parent filed under successive us-gaap taxonomy years
    resolves to one ``us-gaap:AssetsAbstract`` across every vintage.
    Unknown or ambiguous prefixes remain namespace-qualified, never guessed.
    """

    prefixes: dict[str, set[str]] = defaultdict(set)
    for namespace, lexical in qnames:
        if not lexical.startswith("{") and ":" in lexical:
            prefixes[namespace].add(lexical.partition(":")[0])
    return {
        namespace: next(iter(names))
        for namespace, names in prefixes.items()
        if len(names) == 1
    }


def occurrence_qname(lexical: str, prefixes: Mapping[str, str]) -> str:
    if lexical.startswith("{"):
        namespace, separator, local = lexical[1:].partition("}")
        prefix = prefixes.get(namespace)
        if separator and prefix is not None:
            return f"{prefix}:{local}"
    return lexical


def _occurrence_facts(
    payload: Mapping[str, Any], year: int,
) -> Iterator[Mapping[str, Any]]:
    """Project native occurrence coordinates onto the existing topology input."""

    results = payload.get("results", (payload,))
    cells = [
        cell
        for result in results
        for cell in result.get("occurrences", ())
    ]
    prefixes = occurrence_namespace_prefixes(
        (cell["line_qname"]["namespace_uri"], cell["line_qname"]["lexical_qname"])
        for cell in cells
    )
    for cell in cells:
        binding = cell.get("binding")
        if not isinstance(binding, Mapping) or binding.get("kind") != "bound":
            continue
        fact = binding["fact"]
        if fact["dimensions"]:
            continue
        if cell["printed_column"]["period_ref"]["fiscal_year"] != year:
            continue
        parent = cell["tree"]["parent_qname"]
        quantity = fact["native_quantity"]
        number = quantity.get("native_number")
        yield {
            "tag": occurrence_qname(cell["line_qname"]["lexical_qname"], prefixes),
            "current_period_value": (
                number["value"]
                if number is not None and number["kind"] == "decimal"
                else None
            ),
            "scale": "units",
            "period_type": fact["period"]["kind"],
            "source_ref": {
                "current": {"accession": cell["accession"]},
                "cell_locator": cell["source"]["cell_locator"],
            },
            "presentation_hierarchy": {
                "parent": (
                    occurrence_qname(parent["lexical_qname"], prefixes)
                    if parent is not None else None
                ),
                "role": cell["presentation"]["presentation_role_uri"],
                "order": cell["tree"]["order"],
            },
            "calculation": (
                {
                    **cell["calculation"],
                    "parent_qname": occurrence_qname(
                        cell["calculation"]["parent_qname"], prefixes,
                    ) if cell["calculation"].get("parent_qname") else None,
                }
                if cell.get("calculation") is not None else None
            ),
        }


def _edge_from_entry(
    *,
    fact: Mapping[str, Any],
    entry: Mapping[str, Any],
    year: int,
    edge_kind: TopologyEdgeKind,
    source_value: SourceValue,
) -> TopologyEdge | None:
    child_tag = str(fact.get("tag") or "").strip()
    parent_tag = str(
        entry.get("parent")
        or entry.get("calculation_parent")
        or entry.get("parent_tag")
        or ""
    ).strip()
    role = normalize_topology_role(
        entry.get("role")
        or entry.get("calculation_role")
        or entry.get("presentation_role")
    )
    if not child_tag or not parent_tag or not role:
        return None
    raw_weight = entry.get("weight")
    if raw_weight is None:
        raw_weight = entry.get("calculation_weight")
    raw_order = entry.get("order")
    try:
        order = float(raw_order or 0.0)
    except (TypeError, ValueError):
        order = 0.0
    try:
        weight = None if raw_weight is None else float(raw_weight)
    except (TypeError, ValueError):
        weight = None
    source_ref = fact.get("source_ref")
    return TopologyEdge(
        year=int(year),
        role=role,
        edge_kind=edge_kind,
        parent_tag=parent_tag,
        child_tag=child_tag,
        order=order,
        weight=weight,
        source_value=source_value,
        source_ref=(
            _freeze_json(source_ref) if isinstance(source_ref, Mapping) else None
        ),
    )


def _period_fact_from_payload_fact(fact: Mapping[str, Any]) -> PeriodFact:
    """Read the declared period coordinates off one payload fact.

    Recorded verbatim (case/whitespace normalized only); an absent field stays
    ``None`` so downstream constructors refuse rather than assume — mechanism
    §7.1, and the Stage C port contract naming ``current_period_value`` as the
    canonical value field (the ``current_value``-first probe below survives as
    a read fallback, not as the contract).
    """

    def _token(key: str) -> str | None:
        raw = fact.get(key)
        if raw is None:
            return None
        text = str(raw).strip().lower()
        return text or None

    return PeriodFact(period_type=_token("period_type"), date_type=_token("date_type"))


def build_statement_topology(
    payloads_by_year: Mapping[int, Mapping[str, Any]] | None,
) -> StatementTopology:
    """Build the same topology from legacy facts or native occurrence envelopes.

    Occurrence inputs may be the full ``results`` envelope or an ``occurrences``
    mapping. The year selects the printed column; dimensioned cells do not enter
    this consolidated topology. Native values are already scaled to units.
    """

    selected_edges: dict[
        tuple[int, str, TopologyEdgeKind, str, str], TopologyEdge
    ] = {}
    selected_values: dict[int, dict[str, SourceValue]] = defaultdict(dict)
    selected_period_facts: dict[int, dict[str, PeriodFact]] = defaultdict(dict)
    discarded: list[DiscardedTopologyObservation] = []

    for year, payload in sorted((payloads_by_year or {}).items()):
        if not isinstance(payload, Mapping):
            continue
        facts = payload.get("facts")
        if not isinstance(facts, (list, tuple)):
            facts = _occurrence_facts(payload, int(year))
        for fact in facts:
            if not isinstance(fact, Mapping):
                continue
            if fact.get("axis_key") not in (None, "", "__NONE__"):
                continue
            tag = str(fact.get("tag") or "").strip()
            if not tag:
                continue
            # Port contract (Stage C, mechanism §7.1): `current_period_value` is
            # the CANONICAL wire field the producer emits. The `current_value`
            # probe below survives as a read fallback for older captured
            # payloads and in-repo fixtures — it is not the contract, and it is
            # not a deletion this stage takes.
            raw_value = fact.get("current_value")
            if raw_value is None:
                raw_value = fact.get("current_period_value")
            source_ref = fact.get("source_ref")
            source_value = normalize_edgar_source_value(
                raw_value,
                fact.get("scale"),
                series_key="",
                source="edgar_financials",
                tag=tag,
                year=int(year),
                source_ref=(
                    _freeze_json(source_ref)
                    if isinstance(source_ref, Mapping)
                    else None
                ),
            )
            existing_value = selected_values[int(year)].get(tag)
            chosen_source_value = (
                choose_preferred_source_value(existing_value, source_value)
                or source_value
            )
            selected_values[int(year)][tag] = chosen_source_value
            # The period coordinates travel with the observation whose value was
            # kept: one arbitration, one winner, no second copy that could
            # disagree with the value it describes.
            if existing_value is None or chosen_source_value is source_value:
                selected_period_facts[int(year)][tag] = _period_fact_from_payload_fact(
                    fact
                )

            entries: list[tuple[TopologyEdgeKind, Mapping[str, Any]]] = []
            presentation = fact.get("presentation_hierarchy")
            if isinstance(presentation, Mapping):
                presentation_entries = [presentation]
            elif isinstance(presentation, (list, tuple)):
                presentation_entries = [
                    entry for entry in presentation if isinstance(entry, Mapping)
                ]
            else:
                presentation_entries = []
            for presentation_entry in presentation_entries:
                entries.append(("presentation", presentation_entry))
                entries.extend(
                    ("calculation", entry)
                    for entry in _relationship_entries(presentation_entry)
                )
            entries.extend(
                ("calculation", entry) for entry in _relationship_entries(fact)
            )

            for edge_kind, entry in entries:
                incoming = _edge_from_entry(
                    fact=fact,
                    entry=entry,
                    year=int(year),
                    edge_kind=edge_kind,
                    source_value=source_value,
                )
                if incoming is None:
                    continue
                key = (
                    int(year),
                    incoming.role,
                    incoming.edge_kind,
                    incoming.parent_tag,
                    incoming.child_tag,
                )
                existing = selected_edges.get(key)
                if existing is None:
                    selected_edges[key] = incoming
                    continue
                chosen_value = choose_preferred_source_value(
                    existing.source_value,
                    incoming.source_value,
                )
                incoming_wins = chosen_value is incoming.source_value
                kept = incoming if incoming_wins else existing
                rejected = existing if incoming_wins else incoming
                selected_edges[key] = (
                    kept
                    if kept.source_value is chosen_value
                    else replace(kept, source_value=chosen_value)
                )
                discarded.append(
                    DiscardedTopologyObservation(
                        year=int(year),
                        role=incoming.role,
                        edge_kind=incoming.edge_kind,
                        parent_tag=incoming.parent_tag,
                        child_tag=incoming.child_tag,
                        kept_scale=kept.source_value.raw_scale,
                        discarded_scale=rejected.source_value.raw_scale,
                        kept_value=kept.source_value.normalized_value,
                        discarded_value=rejected.source_value.normalized_value,
                        kept_source_ref=kept.source_ref,
                        discarded_source_ref=rejected.source_ref,
                    )
                )

    edges_by_year: dict[int, tuple[TopologyEdge, ...]] = {}
    for year in sorted({key[0] for key in selected_edges}):
        edges_by_year[year] = tuple(
            sorted(
                (
                    edge
                    for key, edge in selected_edges.items()
                    if key[0] == year
                ),
                key=lambda edge: (
                    edge.edge_kind,
                    edge.role,
                    edge.parent_tag,
                    edge.order,
                    edge.child_tag,
                ),
            )
        )

    return StatementTopology(
        edges_by_year=MappingProxyType(edges_by_year),
        source_values_by_year=MappingProxyType(
            {
                year: MappingProxyType(dict(sorted(values.items())))
                for year, values in sorted(selected_values.items())
            }
        ),
        discarded_duplicate_observations=tuple(discarded),
        source_payloads_fingerprint=financials_payloads_fingerprint(
            payloads_by_year
        ),
        period_facts_by_year=MappingProxyType(
            {
                year: MappingProxyType(dict(sorted(period_facts.items())))
                for year, period_facts in sorted(selected_period_facts.items())
            }
        ),
    )


__all__ = [
    "DiscardedTopologyObservation",
    "PeriodFact",
    "StatementTopology",
    "TopologyEdge",
    "TopologyEdgeKind",
    "build_statement_topology",
    "financials_payloads_fingerprint",
    "freeze_payloads_by_year",
    "normalize_topology_role",
    "occurrence_namespace_prefixes",
    "occurrence_qname",
    "thaw_financials_payload",
]
