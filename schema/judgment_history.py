"""``history(subject)``: one judgment subject's revisions, oldest first.

Engine (B), a pure reader. It walks a judgment artifact's ``predecessor_ref``
chain (``schema/judgment_revision.py``) from the head the caller holds back to
the first artifact, through a ``load_predecessor`` callable the caller supplies,
so this module reads no store. Each step that carries the subject's row, that
supersedes it, or that drops a row its predecessor held is one
:class:`JudgmentHistoryEntry`. The chain is the five D4 artifacts as they are:
a FAS chain yields a driver's history, an HFF chain a normalization decision's
or a judged driver seed's, a ValuationPolicy chain a policy row's.

``judgment_rows`` is the one key scheme for a judgment row: a Supersession's
``prior_row``, a ``history`` subject, an ``enumerate`` row and a ``trace``
origin's ``row`` all name a row by it. An HFF holds two row families in one
key space: :func:`seed_row_key` names a seed under the ``seed:`` namespace and
:func:`normalization_row_key` keeps a decision's label, escaping the one label
shape that could reach that namespace, so no two rows share a key.
:func:`valuation_policy_row_key` translates the control ids compiled lineage
carries.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, TypeAlias

from pydantic import BaseModel

from .business_model import DriverAssumptionPlanEntry
from .business_model_revision import BusinessModelRevision
from .business_model_revision_ref import BusinessModelRevisionRef
from .forecast_assumption_set import (
    ForecastAssumptionSet,
    ForecastAssumptionSetRef,
    ForecastDriverAssumption,
)
from .historical_driver_seeds import HistoricalDriverSeed
from .historical_foundation import HistoricalFoundation, HistoricalFoundationRef
from .judgment_revision import Supersession
from .model_spec_authoring import HistoricalNormalizationDecisionAuthoring
from .scenario_set import ScenarioSetArtifact, ScenarioSetRef
from .valuation_policy import ValuationPolicyArtifact, ValuationPolicyRef


JudgmentArtifact: TypeAlias = (
    BusinessModelRevision
    | HistoricalFoundation
    | ForecastAssumptionSet
    | ScenarioSetArtifact
    | ValuationPolicyArtifact
)

#: The ref each judgment artifact names itself by.
JudgmentArtifactRef: TypeAlias = (
    BusinessModelRevisionRef
    | HistoricalFoundationRef
    | ForecastAssumptionSetRef
    | ScenarioSetRef
    | ValuationPolicyRef
)


@dataclass(frozen=True, slots=True)
class JudgmentHistoryEntry:
    """One artifact of the chain as it bears on the subject."""

    #: The artifact's own ref.
    artifact_ref: JudgmentArtifactRef
    created_at: datetime
    #: The subject's row on this artifact; ``None`` when this artifact dropped it.
    row: BaseModel | None
    #: This artifact's supersession of the predecessor's row, if it recorded one.
    supersession: Supersession | None


#: The namespace of an HFF driver seed's key, and the escape a normalization
#: label starting with either prefix takes so it never meets a seed's key.
_SEED_KEY_PREFIX: Final = "seed:"
_LABEL_KEY_PREFIX: Final = "label:"


def seed_row_key(driver_key: str, period_id: str) -> str:
    """The key of one HistoricalFoundation driver seed: ``seed:<driver>@<period>``."""

    return f"{_SEED_KEY_PREFIX}{driver_key}@{period_id}"


def normalization_row_key(label: str) -> str:
    """The key of one normalization decision: its label.

    A label that itself starts with ``seed:`` or ``label:`` is keyed
    ``label:<label>``; every other label is its own key, so a decision's key
    never starts with ``seed:`` and two labels never share one.
    """

    if label.startswith((_SEED_KEY_PREFIX, _LABEL_KEY_PREFIX)):
        return f"{_LABEL_KEY_PREFIX}{label}"
    return label


#: The prefixes ``schema/build_provenance.py`` mints for the authored
#: ValuationPolicy controls it writes into cell lineage, and the policy row each
#: names: a family authorization by its method, the DCF valuation control (its
#: rationale is the terminal policy's, ``project_valuation_policy``), and the
#: net share dilution rates.
_POLICY_CONTROL_PREFIXES: Final = (
    ("valuation_family_authorization.", None),
    ("valuation_control.", "terminal_policy"),
    ("valuation_policy.", None),
)


def valuation_policy_row_key(control_id: str) -> str | None:
    """The ValuationPolicy row a compiled lineage control id names, if any."""

    for prefix, row in _POLICY_CONTROL_PREFIXES:
        if control_id.startswith(prefix):
            return row or control_id.removeprefix(prefix)
    return None


def judgment_rows(artifact: JudgmentArtifact) -> Mapping[str, BaseModel]:
    """Every judgment row of one artifact, by the key a Supersession names.

    An HFF's judgments are its normalization decisions (by label) and every
    driver seed whose disposition is not ``observed`` (by :func:`seed_row_key`):
    an observed seed is the reported fact itself. A ValuationPolicy's are its
    policy inputs (by name), family authorizations (by method), the terminal
    policy (growth and exit-multiple cases), the net share dilution and the
    model policy.
    """

    if isinstance(artifact, ForecastAssumptionSet):
        return {row.assumption.driver: row for row in artifact.driver_assumptions}
    if isinstance(artifact, HistoricalFoundation):
        foundation_rows: dict[str, BaseModel] = {
            normalization_row_key(row.label): row
            for row in artifact.historical_normalization_decisions
        }
        foundation_rows.update(
            (seed_row_key(seed.driver_key, seed.period_id), seed)
            for seed in artifact.historical_driver_seeds
            if seed.disposition != "observed"
        )
        return foundation_rows
    if isinstance(artifact, BusinessModelRevision):
        # ``driver_id`` is the identity a successor plan carries forward;
        # ``driver_key`` is minted per revision.
        return {
            entry.driver_id: entry
            for entry in artifact.driver_assumption_plan.entries
        }
    if isinstance(artifact, ScenarioSetArtifact):
        return {row.driver_key: row for row in artifact.driver_curves}
    rows: dict[str, BaseModel] = {
        row.input_name: row for row in artifact.policy_inputs
    }
    rows.update((row.method, row) for row in artifact.family_authorizations)
    if artifact.terminal_policy is not None:
        rows["terminal_policy"] = artifact.terminal_policy
    if artifact.net_share_dilution is not None:
        rows["net_share_dilution"] = artifact.net_share_dilution
    if artifact.model_policy is not None:
        rows["model_policy"] = artifact.model_policy
    return rows


#: Fields of a DriverAssumptionPlanEntry minted per revision: they differ on
#: every successor without the decomposition changing.
_REVISION_SCOPED_ENTRY_FIELDS: Final = frozenset({"revision", "driver_key", "aliases"})
#: Lineage of a FAS row: the citations a re-accept re-issues. The judged
#: content (values, unit and value space, rationale, extension basis,
#: confidence, classification, mechanism, base-rate anchor, evidence-quality
#: assessment, revision trigger) is everything else.
_FORECAST_ROW_LINEAGE_FIELDS: Final = frozenset({"evidence_ids_by_period"})
_FORECAST_ASSUMPTION_LINEAGE_FIELDS: Final = frozenset(
    {
        "evidence_ids",
        "inherited_baseline_ids",
        "source_refs",
        "source_refs_by_period",
        "source_notes_by_period",
    }
)
#: Lineage of an HFF normalization decision.
_NORMALIZATION_LINEAGE_FIELDS: Final = frozenset({"evidence_ids"})
#: Lineage of an HFF driver seed: the evidence and fact ids it cites, on the
#: seed and on each derivation operand.
_SEED_LINEAGE_FIELDS: Final = frozenset(
    {"evidence_ids", "supporting_evidence_ids", "evidence_value_ids", "fact_ids"}
)
_SEED_OPERAND_LINEAGE_FIELDS: Final = frozenset(
    {"fact_id", "evidence_id", "evidence_value_id"}
)


def _without(values: dict[str, Any], excluded: frozenset[str]) -> dict[str, Any]:
    return {name: value for name, value in values.items() if name not in excluded}


def _judged_content(row: BaseModel) -> object:
    """What a row judges, without the ids its lineage re-issues."""

    if isinstance(row, DriverAssumptionPlanEntry):
        return _without(row.model_dump(), _REVISION_SCOPED_ENTRY_FIELDS)
    if isinstance(row, ForecastDriverAssumption):
        content = _without(row.model_dump(), _FORECAST_ROW_LINEAGE_FIELDS)
        content["assumption"] = _without(
            row.assumption.model_dump(), _FORECAST_ASSUMPTION_LINEAGE_FIELDS
        )
        return content
    if isinstance(row, HistoricalNormalizationDecisionAuthoring):
        return _without(row.model_dump(), _NORMALIZATION_LINEAGE_FIELDS)
    if isinstance(row, HistoricalDriverSeed):
        content = _without(row.model_dump(), _SEED_LINEAGE_FIELDS)
        content["derivation_operands"] = [
            _without(operand.model_dump(), _SEED_OPERAND_LINEAGE_FIELDS)
            for operand in row.derivation_operands
        ]
        return content
    return row


def same_judgment(prior: BaseModel, row: BaseModel | None) -> bool:
    """Whether a successor's row carries the predecessor row's judgment unchanged.

    Re-issued citations alone are not a change of judgment; a successor that
    keeps the judged content needs no supersession.
    """

    if row is None:
        return False
    return _judged_content(prior) == _judged_content(row)


def artifact_supersessions(artifact: JudgmentArtifact) -> Mapping[str, Supersession]:
    """The artifact's supersessions by prior row; none on ScenarioSet / policy."""

    rows: tuple[Supersession, ...] = getattr(artifact, "supersessions", ())
    return {row.prior_row: row for row in rows}


def history(
    subject: str,
    *,
    head: JudgmentArtifact,
    load_predecessor: Callable[[Any], JudgmentArtifact | None],
) -> tuple[JudgmentHistoryEntry, ...]:
    """The subject's entries along ``head``'s predecessor chain, oldest first.

    The walk stops at an artifact with no ``predecessor_ref``, at a
    predecessor ``load_predecessor`` cannot return, and at a ref already seen.
    """

    chain: list[JudgmentArtifact] = []
    seen: set[str] = set()
    artifact: JudgmentArtifact | None = head
    while artifact is not None:
        key = artifact.ref.model_dump_json()
        if key in seen:
            break
        seen.add(key)
        chain.append(artifact)
        predecessor_ref = artifact.predecessor_ref
        artifact = None if predecessor_ref is None else load_predecessor(predecessor_ref)

    entries: list[JudgmentHistoryEntry] = []
    prior_held = False
    for artifact in reversed(chain):
        row = judgment_rows(artifact).get(subject)
        supersession = artifact_supersessions(artifact).get(subject)
        if row is not None or supersession is not None or prior_held:
            entries.append(
                JudgmentHistoryEntry(
                    artifact_ref=artifact.ref,
                    created_at=artifact.created_at,
                    row=row,
                    supersession=supersession,
                )
            )
        prior_held = row is not None
    return tuple(entries)


__all__ = [
    "JudgmentArtifact",
    "JudgmentArtifactRef",
    "JudgmentHistoryEntry",
    "artifact_supersessions",
    "history",
    "judgment_rows",
    "normalization_row_key",
    "same_judgment",
    "seed_row_key",
    "valuation_policy_row_key",
]
