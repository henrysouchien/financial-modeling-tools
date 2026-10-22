"""Dated-EV components checked against the one selection that decides their concepts.

The concepts an issuer's dated-EV roles read are decided once, by
``api/research/build_model_source_bundle.py:_enterprise_value_selection`` (the focal
issuer's sealed bindings of ``statement.bs.long_term_debt`` /
``statement.bs.cash_and_equivalents`` in the EV filing's window, else the generic
capture policy), and consumed only by the roster projection. The stored-bundle validator
(``schema/valuation_evidence_validation.py:validate_valuation_evidence_source_bindings``)
binds a component's value to whatever fact its ref addresses and never consults that
selection. ``ev_check`` reads the stored SourceBundle's EV component observations, resolves
each issuer's selection through the same owner, and reports per debt/cash component (and per
declared-adjustment leg) whether it is bound to the selection:

- ``selection_match`` — every observed concept is selected, every assumed-zero concept is
  selected, and together they cover the selection. A summed role's observation records only
  its first contributing ref; when that ref heads the selection's contributing refs and the
  value is the selection's sum, every stated selected concept counts as observed.
- ``selection_mismatch:<observed> vs <selected>`` — an observed concept outside the selection,
  or a selected concept the observation neither binds nor reads as zero (``<observed>`` is
  ``none`` for a zero bound to no concept, the ref itself when it addresses no fact).
- ``assumed_zero_outside_selection`` — an assumed-zero concept the selection does not name.
- ``no_selection`` — the owner names no concept for the role.

Beside each verdict, the selection-bound value (the producer's own
``_resolve_financials_role`` over the selected concept groups) and the EV derived
(``derive_observation_dated_enterprise_value``) from the stored components vs from
selection-bound ones. A role's selection is ordered alternative concept groups; the producer
reads the first group with an undimensioned fact, and the verdict compares a component against the
group the producer read (the one whose concepts equal the stated ones plus the assumed-zero
ones), else against every group's concepts.
The producer's reads arrive as ``EnterpriseValueReads`` (this root imports no product); the
bench's adapter is ``scripts/research/bench/context.py`` ``EnterpriseValueReads``.
Plan: docs/design/financial-model-integrated-architecture-2026-10-05.md §4 Part C.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
import json
import re
from typing import TYPE_CHECKING, Any, Protocol

from schema.build_model_product_contracts import edgar_api_financials_fact_is_undimensioned
from schema.valuation_evidence_derivations import derive_observation_dated_enterprise_value

if TYPE_CHECKING:
  from schema.build_model_product_contracts import SourceBundle, ValuationObservation


class ResolvedRole(Protocol):
  """One dated-EV role resolved by the producer: native-unit ``value``, its contributing ``refs``
  in response order, and the selected concepts read as zero."""

  @property
  def value(self) -> Decimal: ...

  @property
  def refs(self) -> Sequence[str]: ...

  @property
  def assumed_zero_qnames(self) -> Sequence[str]: ...


class SelectedConcept(Protocol):
  """One concept a role reads and the direction (``1`` add, ``-1`` subtract) it enters the sum."""

  @property
  def qname(self) -> str: ...

  @property
  def sign(self) -> int: ...


class EnterpriseValueReads(Protocol):
  """The producer's own dated-EV reads this check compares against (the selection owner and the
  ``/api/financials`` resolver), over one research file's corpus."""

  @property
  def corpus_id(self) -> str: ...

  @property
  def focal(self) -> str: ...

  def selection(
    self, *, issuer: str, accession: str,
  ) -> Mapping[str, Sequence[Sequence[SelectedConcept]]] | None:
    """The ordered alternative concept groups each of ``issuer``'s dated-EV roles reads, or ``None``
    when none is decided. The producer picks the first group with any tagged fact."""
    ...

  def response_facts(self, response: Mapping[str, Any], *, label: str) -> Sequence[Any]: ...

  def report_date(self, response: Mapping[str, Any]) -> date: ...

  def accession(self, response: Mapping[str, Any]) -> str: ...

  def fact_millions(self, fact: Mapping[str, Any], *, label: str) -> Decimal:
    """A financials fact's ``current_period_value`` at its scale, in millions."""
    ...

  def millions(self, value: Decimal) -> Decimal: ...

  def resolve_role(
    self, *, issuer: str, role: str, groups: tuple[tuple[SelectedConcept, ...], ...], facts: Sequence[Any],
    locator: str, report_date: date,
  ) -> ResolvedRole:
    """The role read from ``groups`` (first group with a tagged fact, signed sum) by the producer's
    resolver; ``ValueError`` when it cannot."""
    ...


#: The dated-EV selection role each core monetary component reads.
COMPONENT_ROLES: Mapping[str, str] = {"debt": "long_term_debt", "cash": "corporate_cash"}

#: The product owner lines that bind a component's concept without consulting the selection.
OWNER_LINES: tuple[dict[str, str], ...] = (
  {
    "site": "schema/valuation_evidence_validation.py:validate_valuation_evidence_source_bindings",
    "lines": "debt/cash branch (`elif observation.component_type in {\"debt\", \"cash\"}`: "
    "`financials_ev_fact(observation.raw_ref, ...)` then value/date/currency only)",
    "needs": "compare `bound_qnames` with the role's selected concepts",
  },
  {
    "site": "schema/valuation_evidence_validation.py:validate_valuation_evidence_source_bindings",
    "lines": "declared-adjustment branch (`for leg in adjustment.legs: financials_ev_fact(leg.ref, ...)`)",
    "needs": "compare each leg's bound concept with `selection[leg.role_label]`",
  },
  {
    "site": "schema/valuation_evidence_validation.py:require_assumed_zero_absent",
    "lines": "`for qname in observation.assumed_zero_qnames` (absence checked, membership not)",
    "needs": "require every assumed-zero concept to be selected for the component's role",
  },
  {
    "site": "api/research/build_model_source_bundle.py:_enterprise_value_selection",
    "lines": "the selection owner; reads the corpus (policy table or sealed statement bindings)",
    "needs": "a form the schema validator can consult (the policy table, or the bundle's pinned "
    "statement.bs.long_term_debt / statement.bs.cash_and_equivalents column)",
  },
)

_FACT_REF = re.compile(r"response\.facts\[(\d+)\]\.current_period_value")


def _dec(value: Decimal | None) -> str | None:
  return None if value is None else format(value.normalize(), "f")


def _observed(ref: str, *, facts: Sequence[Any], locator: str, reads: EnterpriseValueReads) -> dict[str, Any]:
  """The concept(s) one component ref addresses in the financials response."""
  prefix = f"{locator}#"
  key = ref.removeprefix(prefix) if ref.startswith(prefix) else None
  if key == "response.facts":
    return {"ref": ref, "qnames": [], "fact_value": "0", "resolves": True, "undimensioned": None}
  match = _FACT_REF.fullmatch(key or "")
  if match is None or int(match.group(1)) >= len(facts) or not isinstance(facts[int(match.group(1))], Mapping):
    return {"ref": ref, "qnames": [], "fact_value": None, "resolves": False, "undimensioned": None}
  fact = facts[int(match.group(1))]
  value = reads.fact_millions(fact, label=f"ev_check fact {ref}")
  return {
    "ref": ref,
    "qnames": [str(fact.get("tag"))],
    "fact_value": _dec(value),
    "resolves": True,
    "undimensioned": edgar_api_financials_fact_is_undimensioned(fact),
  }


def component_verdict(
  *,
  observed_qnames: Sequence[str],
  assumed_zero_qnames: Sequence[str],
  selected: Sequence[str] | None,
  unresolved_ref: str | None = None,
) -> str:
  """The typed verdict for one component's observed and assumed-zero concepts vs its selection."""
  if not selected:
    return "no_selection"
  selected_text = ",".join(selected)
  if unresolved_ref is not None:
    return f"selection_mismatch:{unresolved_ref} vs {selected_text}"
  outside = [qname for qname in observed_qnames if qname not in selected]
  if outside:
    return f"selection_mismatch:{outside[0]} vs {selected_text}"
  if any(qname not in selected for qname in assumed_zero_qnames):
    return "assumed_zero_outside_selection"
  if set(observed_qnames) | set(assumed_zero_qnames) != set(selected):
    return f"selection_mismatch:{','.join(observed_qnames) or 'none'} vs {selected_text}"
  return "selection_match"


def _selected_read(
  *,
  issuer: str,
  role: str,
  groups: Sequence[Sequence[SelectedConcept]] | None,
  facts: Sequence[Any],
  locator: str,
  report_date: date,
  reads: EnterpriseValueReads,
) -> tuple[ResolvedRole | None, str | None]:
  """The role read from its selected concept groups by the producer's own resolver."""
  if not groups or not any(groups):
    return None, None
  try:
    resolved = reads.resolve_role(
      issuer=issuer, role=role, groups=tuple(tuple(group) for group in groups), facts=facts, locator=locator,
      report_date=report_date,
    )
  except ValueError as exc:
    return None, str(exc)
  return resolved, None


def _observed_qnames(
  observed: Mapping[str, Any],
  *,
  recorded_value: Decimal,
  selected: Sequence[str] | None,
  selected_read: ResolvedRole | None,
  reads: EnterpriseValueReads,
) -> list[str]:
  """The concepts a component's value reads.

  The producer records a summed role by its first contributing ref (``refs[0]``) alone. A
  component whose ref is the head of the selection's contributing refs and whose value is
  the selection's sum read every stated selected concept; any other component read only
  the concept its ref addresses.
  """
  if (
    selected and selected_read is not None
    and observed["ref"] == selected_read.refs[0]
    and recorded_value == reads.millions(selected_read.value)
  ):
    return [qname for qname in selected if qname not in selected_read.assumed_zero_qnames]
  return list(observed["qnames"])


def _row(
  *,
  component: str,
  role: str,
  observed: Mapping[str, Any],
  recorded_value: Decimal,
  assumed_zero_qnames: Sequence[str],
  selected: Sequence[str] | None,
  selected_read: ResolvedRole | None,
  selected_error: str | None,
  reads: EnterpriseValueReads,
) -> dict[str, Any]:
  selected_value = None if selected_read is None else reads.millions(selected_read.value)
  observed_qnames = _observed_qnames(
    observed, recorded_value=recorded_value, selected=selected, selected_read=selected_read, reads=reads,
  )
  verdict = component_verdict(
    observed_qnames=observed_qnames,
    assumed_zero_qnames=assumed_zero_qnames,
    selected=selected,
    unresolved_ref=None if observed["resolves"] else str(observed["ref"]),
  )
  return {
    "component": component,
    "role": role,
    "observed": {
      **observed,
      "qnames": observed_qnames,
      "value": _dec(recorded_value),
      "assumed_zero_qnames": list(assumed_zero_qnames),
    },
    "selected": {
      "qnames": None if selected is None else list(selected),
      "value": _dec(selected_value),
      **({"error": selected_error} if selected_error else {}),
    },
    "value_equals_selection": None if selected_value is None else recorded_value == selected_value,
    "bound_to_selection": verdict == "selection_match",
    "verdict": verdict,
  }


def check_issuer(
  *,
  issuer: str,
  selection: Mapping[str, Sequence[Sequence[SelectedConcept]]] | None,
  observations: Sequence[ValuationObservation],
  response: Mapping[str, Any],
  locator: str,
  reads: EnterpriseValueReads,
) -> dict[str, Any]:
  """One issuer's debt/cash components and declared legs vs its selection, and EV both ways.

  ``observations`` are the issuer's dated-EV component observations; ``response`` the
  ``/api/financials`` capture their debt/cash refs address under ``locator``; ``reads`` the
  producer's resolver they are checked against.
  """
  facts = reads.response_facts(response, label=f"ev_check {issuer} financials")
  report_date = reads.report_date(response)
  components: list[dict[str, Any]] = []

  def selected_for(role: str) -> tuple[Sequence[str] | None, ResolvedRole | None, str | None]:
    """The concepts the role reads: the group the producer chose, else every group's concepts."""
    groups = None if selection is None else selection.get(role)
    read, error = _selected_read(
      issuer=issuer, role=role, groups=groups, facts=facts, locator=locator, report_date=report_date, reads=reads,
    )
    if groups is None:
      return None, read, error
    flattened = [concept.qname for group in groups for concept in group]
    if read is None:
      return flattened, read, error
    # The producer reports the concepts it read, not the group: stated = the concepts its refs
    # address, plus those it read as zero; the chosen group is the one with exactly that set.
    stated = {
      qname for ref in read.refs
      for qname in _observed(ref, facts=facts, locator=locator, reads=reads)["qnames"]
    }
    read_set = stated | set(read.assumed_zero_qnames)
    chosen = next(
      ([concept.qname for concept in group] for group in groups if {concept.qname for concept in group} == read_set),
      flattened,
    )
    return chosen, read, error

  replaced: dict[str, Decimal | None] = {}
  for observation in observations:
    component_type = observation.component_type
    if component_type is not None and component_type in COMPONENT_ROLES:
      role = COMPONENT_ROLES[component_type]
      selected, read, error = selected_for(role)
      components.append(_row(
        component=component_type,
        role=role,
        observed=_observed(observation.raw_ref, facts=facts, locator=locator, reads=reads),
        recorded_value=observation.value,
        assumed_zero_qnames=observation.assumed_zero_qnames,
        selected=selected,
        selected_read=read,
        selected_error=error,
        reads=reads,
      ))
      replaced[observation.observation_id] = None if read is None else reads.millions(read.value)
    elif component_type == "declared_adjustment" and observation.declared_adjustment is not None:
      adjustment = observation.declared_adjustment
      # The row records one assumed-zero set, the union over its legs: each leg is read with
      # the concepts of that set its own role selects, plus any no leg's role selects.
      legs_selected = {
        concept.qname
        for leg in adjustment.legs
        for group in ((selection or {}).get(leg.role_label) or ())
        for concept in group
      }
      net: Decimal | None = Decimal(0)
      for leg in adjustment.legs:
        selected, read, error = selected_for(leg.role_label)
        value = None if read is None else reads.millions(read.value)
        observed = _observed(leg.ref, facts=facts, locator=locator, reads=reads)
        leg_zero = [
          qname for qname in observation.assumed_zero_qnames
          if qname in (selected or ()) or qname not in legs_selected
        ]
        components.append(_row(
          component=f"declared_adjustment.{adjustment.role_label}.{leg.role_label}",
          role=leg.role_label,
          observed=observed,
          recorded_value=leg.value,
          assumed_zero_qnames=leg_zero,
          selected=selected,
          selected_read=read,
          selected_error=error,
          reads=reads,
        ))
        net = None if net is None or value is None else net + (value if leg.sign == "add" else -value)
      replaced[observation.observation_id] = net

  as_of_closes = {observation.as_of_close for observation in observations}
  ev: dict[str, Any] = {"as_of_close": None, "observed": None, "selection_bound": None, "difference": None}
  if len(as_of_closes) == 1:
    as_of_close = next(iter(as_of_closes))
    observed_ev = derive_observation_dated_enterprise_value(observations, issuer=issuer, as_of_close=as_of_close)
    bound_ev = None
    if all(value is not None for value in replaced.values()):
      bound_rows = [
        observation.model_copy(update={"value": replaced[observation.observation_id]})
        if observation.observation_id in replaced else observation
        for observation in observations
      ]
      bound_ev = derive_observation_dated_enterprise_value(bound_rows, issuer=issuer, as_of_close=as_of_close)
    observed_value = getattr(observed_ev, "enterprise_value", None)
    bound_value = getattr(bound_ev, "enterprise_value", None)
    ev = {
      "as_of_close": as_of_close.isoformat(),
      "observed": _dec(observed_value),
      "selection_bound": _dec(bound_value),
      "difference": None if observed_value is None or bound_value is None else _dec(observed_value - bound_value),
    }
  return {
    "issuer": issuer,
    "has_selection": selection is not None,
    "components": components,
    "verdicts": {row["component"]: row["verdict"] for row in components},
    "ev": ev,
  }


def ev_check_bundle(
  bundle: SourceBundle,
  reads: EnterpriseValueReads,
  *,
  accession: str | None = None,
) -> dict[str, Any]:
  """Every issuer's dated-EV components in ``bundle`` vs the producer's selection (``reads``).

  ``accession`` keeps only issuers whose financials capture is that filing.
  """
  sources = {str(source.id): source for source in bundle.sources}
  by_issuer: dict[str, list[ValuationObservation]] = {}
  for observation in bundle.valuation_observations:
    if observation.component_type is not None and observation.issuer is not None:
      by_issuer.setdefault(str(observation.issuer), []).append(observation)
  issuers: list[dict[str, Any]] = []
  accessions: dict[str, str] = {}
  for issuer in sorted(by_issuer):
    rows = by_issuer[issuer]
    monetary = next((row for row in rows if row.component_type in COMPONENT_ROLES), None)
    if monetary is None:
      continue
    key_fields = sources[str(monetary.source_id)].key_fields or {}
    response = json.loads(str(key_fields["response"]))
    filing = reads.accession(response)
    accessions[issuer] = filing
    if accession is not None and filing != accession:
      continue
    result = check_issuer(
      issuer=issuer,
      selection=reads.selection(issuer=issuer, accession=filing),
      observations=rows,
      response=response,
      locator=str(key_fields["logical_locator"]),
      reads=reads,
    )
    issuers.append({"accession": filing, **result})
  return {
    "corpus_id": reads.corpus_id,
    "focal": reads.focal,
    "accession": accession,
    "accessions_available": accessions,
    "issuers": issuers,
    "unbound": [
      {"issuer": item["issuer"], "component": row["component"], "verdict": row["verdict"]}
      for item in issuers for row in item["components"] if not row["bound_to_selection"]
    ],
    "owner_lines": list(OWNER_LINES),
  }
