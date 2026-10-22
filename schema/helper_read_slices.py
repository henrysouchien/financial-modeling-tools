"""What each derivation helper reads, as one stable digest per helper.

``construct_document`` records a derivation helper's slice (``DERIVATION_HELPERS``) as its
``read_slice`` input and ``rerun_derivations`` re-runs the helper when the recorded slice differs
from the current one. The SourceBundle's ``bundle_id`` and the ModelSpec's whole-content sha are
provenance: they move whenever the bundle is prepared again (every correspondence persist moves the
corpus manifest) and say nothing about what a helper read, so they are recorded and never compared.

Each function digests exactly what its helper reads, read off the helper's owner function and the
``construct_document`` closures that feed it (named per function below). A read set errs toward
too wide: a field no helper is known to read stays in the slice unless it is provenance
(``retrieved_at`` stamps, the bundle's own sha). A missed read silently stops a re-run; an extra
one only runs a helper whose output comes out unchanged. Every function returns ``None`` when the
input it digests is absent (the helper does not run then).

Digests are cached per bundle / ModelSpec object (a refresh digests the same bundle for every
helper); a bundle or spec is immutable, so the cache holds a weak reference and dies with it.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import weakref
from collections.abc import Callable, Iterable
from typing import Any

from .build_model_product_contracts import ValuationLineageReference

#: ``SourceEvidence`` / ``HistoricalBasisFact`` stamp the instant the bundle was prepared; a
#: bundle's validators order them against each other (they hold in every bundle that loads) and
#: no helper reads the instant.
_PREPARED_AT = frozenset({"retrieved_at"})

#: The ModelSpec fields that are the bundle's content or a spec's own identity, not judgment: the sha of
#: the bundle it was assembled over, the spec's version identity, the gaps and degradations assembly
#: recorded, and the two projections of the bundle's historical facts (the HFF helper reads those).
#: What the scenario and valuation helpers read is the rest of the spec: the scenario curves and inputs
#: (``scenarios``, ``scenario_reviews``, ``forecast_assumptions``, ``fiscal_axis``), the valuation controls,
#: policy, family authorizations and bindings, ``_write_scenario_outputs``' ``scenario_case_gaps`` — and,
#: because ``scenario_outputs`` lowers cells ``valuation_inputs`` writes, they move together.
_BUNDLE_AND_IDENTITY_SPEC = frozenset({
    "source_bundle_sha256", "idempotency_key", "predecessor", "declared_gaps", "degraded_choices",
    "historical_foundation_fact_projection", "historical_driver_seed_projection",
})

#: ``apply_model_spec_historical_foundation_facts``; the segment-revenue series rule it applies
#: reads the driver-seed projection as well.
_HFF_SPEC = frozenset({
    "ticker", "historical_foundation_fact_projection", "historical_driver_seed_projection",
    "forecast_assumption_set_binding",
})
#: The ``CoveredMarketInputs`` fields the valuation helper reads a pinned observation for.
_MARKET_FIELDS = ("stock_price", "raw_beta", "risk_free_rate", "equity_risk_premium")

_PARTS: dict[int, tuple[weakref.ref[Any], dict[str, str]]] = {}


def _digest(label: str, *parts: str) -> str:
    digest = hashlib.sha256(label.encode())
    for part in parts:
        digest.update(b"\0")
        digest.update(part.encode())
    return digest.hexdigest()


def _default(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=str)
    return str(value)


def _text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_default)


def _part(owner: Any, name: str, compute: Callable[[], str]) -> str:
    """``compute()`` once per (``owner``, ``name``) while ``owner`` lives."""
    entry = _PARTS.get(id(owner))
    if entry is None or entry[0]() is not owner:
        key = id(owner)

        def forget(ref: weakref.ref[Any]) -> None:
            held = _PARTS.get(key)
            if held is not None and held[0] is ref:
                del _PARTS[key]

        entry = (weakref.ref(owner, forget), {})
        _PARTS[key] = entry
    parts = entry[1]
    if name not in parts:
        parts[name] = compute()
    return parts[name]


def _rows(label: str, rows: Iterable[Any], *, exclude: frozenset[str] | None = None) -> str:
    return _digest(label, *(row.model_dump_json(exclude=exclude) for row in rows))


def _requirements(bundle: Any) -> str:
    return _part(bundle, "requirements", lambda: _rows("requirements", bundle.requirements))


def _coverage(bundle: Any) -> str:
    return _part(bundle, "coverage", lambda: _rows("coverage", bundle.coverage))


def _evidence(bundle: Any) -> str:
    return _part(bundle, "evidence", lambda: _rows("evidence", bundle.evidence, exclude=_PREPARED_AT))


def _facts(bundle: Any) -> str:
    return _part(bundle, "facts", lambda: _rows("facts", bundle.historical_basis_facts, exclude=_PREPARED_AT))


def _dispositions(bundle: Any) -> str:
    """The profile's derived (role, period) dispositions: what the schedule policies resolve against."""
    return _part(
        bundle, "dispositions", lambda: _digest("dispositions", bundle.coverage_profile.role_dispositions.model_dump_json()),
    )


def _spec_judgment(spec: Any) -> str:
    """Every ModelSpec field but the bundle's content and the spec's own identity (``_BUNDLE_AND_IDENTITY_SPEC``)."""
    return _part(spec, "judgment", lambda: _digest("spec", spec.model_dump_json(exclude=set(_BUNDLE_AND_IDENTITY_SPEC))))


def _spec_hff(spec: Any) -> str:
    return _part(spec, "hff", lambda: _digest("spec", spec.model_dump_json(include=set(_HFF_SPEC))))


def statement_residual_histories() -> str:
    """Reads the model and the static role catalog only (``_install_residual_histories(model)``)."""
    return _digest("statement_residual_histories")


def business_model_node_history(bundle: Any) -> str | None:
    """``apply_business_model_node_history(model, bundle, required_set=derive_required_set(bundle.requirements, ...),
    opening_balances=opening_balances(bundle))``: every requirement row (identity series, pins, period cells), every
    coverage row and every evidence row — ``opening_balances`` reads the period before the build window, which
    ``derive_required_set`` scopes out."""
    if bundle is None:
        return None
    return _digest("business_model_node_history", _requirements(bundle), _coverage(bundle), _evidence(bundle))


def company_grounded_schedules(bundle: Any, *, opening: Any, grounded_rows: Any) -> str | None:
    """``apply_company_grounded_schedule_policies(model, historical_basis_facts=..., grounded_semantic_rows=...,
    opening_balances=..., role_dispositions=resolve_build_dispositions(bundle.coverage_profile.role_dispositions, ...))``
    and ``schedule_fact_source`` (facts and opening evidence by id). ``opening`` is ``opening_balances(bundle)``'s
    result (the evidence it names, by role), ``grounded_rows`` the declared grounded-row map."""
    if bundle is None:
        return None
    held = None if opening is None else {
        role: (balance.requirement_id, balance.evidence.model_dump(mode="json", exclude=_PREPARED_AT))
        for role, balance in opening.items()
    }
    return _digest("company_grounded_schedules", _facts(bundle), _text(held), _dispositions(bundle), _text(grounded_rows))


def scenario_targets(spec: Any) -> str | None:
    """``partition_model_spec_scenario_workbook_targets`` / ``lower_model_spec_scenario_curves`` /
    ``lower_model_spec_direct_scenario_inputs`` over the spec."""
    if spec is None:
        return None
    return _digest("scenario_targets", _spec_judgment(spec))


def historical_foundation_facts(spec: Any) -> str | None:
    """``apply_model_spec_historical_foundation_facts`` and the ``hff_fact_source`` closure."""
    if spec is None:
        return None
    return _digest("historical_foundation_facts", _spec_hff(spec))


def valuation_inputs(spec: Any, bundle: Any, *, market: Any) -> str | None:
    """The spec's valuation controls, policy and family authorizations; from the bundle only what
    ``_market_inputs(bundle)`` derived (``market``: the four covered inputs, their observation ids,
    values and source ids, or ``None`` when the derivation failed) and the lineage reference of each
    pinned observation selected (the helper stamps it on the cell it landed, ``snapshot_manifest_sha256``
    — the corpus manifest the observation was prepared against — included: a reader of the cell's
    ``valuation_refs`` validates it as part of ``ValuationLineageReference``)."""
    if spec is None or bundle is None:
        return None
    selected = {
        observation_id
        for name in _MARKET_FIELDS
        if (observation_id := getattr(getattr(market, name, None), "observation_id", None)) is not None
    }
    fields = set(ValuationLineageReference.model_fields)
    references = sorted(
        observation.model_dump_json(include=fields)
        for observation in bundle.valuation_observations
        if observation.observation_id in selected
    )
    return _digest("valuation_inputs", _spec_judgment(spec), _text(market), *references)


def control_curves(spec: Any) -> str | None:
    """``compile_candidate_model(model, None, curves_of(model), ...)``: the curves ``scenario_targets`` lowered."""
    if spec is None:
        return None
    return _digest("control_curves", _spec_judgment(spec))


def scenario_outputs(spec: Any, *, eps_publication_basis: Any) -> str | None:
    """``_populate_scenario_inputs`` over the lowered scenarios and curves, and ``_write_scenario_outputs``
    (``scenario_case_gaps(spec)``) with the kind's declared EPS publication basis."""
    if spec is None:
        return None
    return _digest("scenario_outputs", _spec_judgment(spec), _text(eps_publication_basis))


def share_conversion(bundle: Any, *, market: Any, declaration: Any) -> str | None:
    """``_materialize_declared_share_conversion(model, market, share_conversion)``: the covered market
    inputs ``_market_inputs(bundle)`` derived and the kind's declared conversion. Not a derivation
    helper (``DERIVATION_HELPERS``) but it records the bundle as an input, so a refresh compares it by
    what it read as well."""
    if bundle is None:
        return None
    return _digest("share_conversion", _text(market), _text(declaration))
