"""Annual BusinessModel capability and sufficiency contracts.

This is company-economic state owned by ``BusinessModel``.  It deliberately
does not own fiscal periods, source selections, normalized values, scenario
overrides, valuation choices, or workbook coordinates; those remain
``ModelSpec`` or compiler concerns under DEC-04.

This module is generic over a declared semantic-id vocabulary. Issuer-specific
requirements belong in authored model-kind data, never in issuer-named
mechanism branches. See ``schema/README.md`` and
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``.

Two things used to be issuer-shaped here and are now declared:

* **The id vocabulary.** Five regexes hard-coded one issuer's namespace. The
  patterns below carry the *shape* — a dotted lowercase namespace, then the
  mechanism's own ``capability``/``equation``/``schedule``/``binding`` segment
  — and the profile declares which namespace is its own. Every id it carries
  must live in that namespace, so the check is exactly the old one when the
  declared namespace is the reference issuer's, and refuses strictly more: a
  profile mixing two namespaces was previously unrepresentable only by
  accident of which prefix the regex named.
* **The closed capability denominator.** Which capabilities a model kind must
  declare to be sufficient, and which concept each gap points at for repair,
  was a module constant and a hand table. Both are now
  :class:`BusinessModelSufficiencyDenominator`, authored per model kind under
  ``schema/model_kinds/`` and read by :func:`load_sufficiency_denominator`.
"""

from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, ClassVar, Final, Literal, Mapping, NoReturn

from pydantic import BaseModel, ConfigDict, Field, model_validator


#: The *shape* of each id class. The leading segment is the model kind's
#: declared semantic namespace; the middle segment, where there is one, is
#: mechanism vocabulary.
_SEMANTIC_ID_PATTERN = r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*){2,}$"
_CAPABILITY_ID_PATTERN = r"^[a-z][a-z0-9_]*\.capability\.[a-z][a-z0-9_]*$"
_EQUATION_ID_PATTERN = r"^[a-z][a-z0-9_]*\.equation\.[a-z][a-z0-9_]*$"
_SCHEDULE_ID_PATTERN = r"^[a-z][a-z0-9_]*\.schedule\.[a-z][a-z0-9_]*$"
_BINDING_ID_PATTERN = r"^[a-z][a-z0-9_]*\.binding\.[a-z][a-z0-9_]*$"
_NAMESPACE_PATTERN = r"^[a-z][a-z0-9_]*$"

#: The checked-in model-kind data directory the authored denominators live in,
#: beside the IG-3 registry table.
DEFAULT_MODEL_KIND_DIR: Final = Path(__file__).resolve().parent / "model_kinds"

#: The denominator artifact's schema identity.
SUFFICIENCY_SCHEMA_VERSION: Final = "business-model-sufficiency.v1"

#: §14 addition at IG-5 (the IG-3 `model_kind_registry_invalid` precedent): the
#: two failure modes of an authored denominator, each with its own repair lane.
BUSINESS_MODEL_SUFFICIENCY_UNDECLARED: Final = "business_model_sufficiency_undeclared"
BUSINESS_MODEL_SUFFICIENCY_INVALID: Final = "business_model_sufficiency_invalid"

SUFFICIENCY_LANE: Final = (
    "author the model kind's sufficiency denominator in the model-kind data lane"
)
SUFFICIENCY_REPAIR_LANE: Final = (
    "repair the sufficiency denominator artifact in the model-kind data lane"
)


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


SemanticKind = Literal[
    "observation",
    "driver",
    "control",
    "mapping",
    "output",
    "policy",
]


class BusinessModelSemanticConcept(_Contract):
    """One durable economic identity; labels and aliases are metadata."""

    semantic_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)
    kind: SemanticKind
    label: str = Field(min_length=1)
    aliases: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _aliases_are_canonical(self) -> "BusinessModelSemanticConcept":
        if self.aliases != tuple(sorted(set(self.aliases))):
            raise ValueError("semantic aliases must be sorted and unique")
        return self


class BusinessModelSemanticBinding(_Contract):
    """Semantic mapping only; no label or workbook coordinate is identity."""

    binding_id: str = Field(pattern=_BINDING_ID_PATTERN)
    semantic_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)
    mapping_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)


EquationOperator = Literal[
    "identity",
    "sum",
    "difference",
    "product",
    "average",
    "ratio",
    "roll_forward",
]


class BusinessModelEquation(_Contract):
    equation_id: str = Field(pattern=_EQUATION_ID_PATTERN)
    output_semantic_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)
    operator: EquationOperator
    operand_semantic_ids: tuple[str, ...] = Field(min_length=1)
    roll_forward_operand_roles: tuple[
        Literal["beginning", "addition", "subtraction"], ...
    ] = ()

    @model_validator(mode="after")
    def _shape(self) -> "BusinessModelEquation":
        if len(self.operand_semantic_ids) != len(set(self.operand_semantic_ids)):
            raise ValueError("equation operands must be unique")
        if self.operator == "identity" and len(self.operand_semantic_ids) != 1:
            raise ValueError("identity equation requires exactly one operand")
        if self.operator == "difference" and len(self.operand_semantic_ids) != 2:
            raise ValueError("difference equation requires exactly two operands")
        if self.operator in {"average", "ratio"} and len(
            self.operand_semantic_ids
        ) != 2:
            raise ValueError(
                f"{self.operator} equation requires exactly two operands"
            )
        if self.operator == "roll_forward":
            if len(self.roll_forward_operand_roles) != len(
                self.operand_semantic_ids
            ):
                raise ValueError(
                    "roll_forward requires one role per ordered operand"
                )
            if "beginning" not in self.roll_forward_operand_roles:
                raise ValueError("roll_forward requires a beginning operand")
        elif self.roll_forward_operand_roles:
            raise ValueError(
                "roll_forward_operand_roles require roll_forward operator"
            )
        return self


class BusinessModelSchedule(_Contract):
    schedule_id: str = Field(pattern=_SCHEDULE_ID_PATTERN)
    observation_semantic_ids: tuple[str, ...] = Field(min_length=1)
    driver_semantic_ids: tuple[str, ...] = Field(min_length=1)
    policy_semantic_ids: tuple[str, ...] = ()
    mapping_semantic_ids: tuple[str, ...] = Field(min_length=1)
    equation_ids: tuple[str, ...] = Field(min_length=1)
    reconciliation_output_semantic_ids: tuple[str, ...] = Field(min_length=1)


class BusinessModelCapability(_Contract):
    capability_id: str = Field(pattern=_CAPABILITY_ID_PATTERN)
    observation_semantic_ids: tuple[str, ...] = Field(min_length=1)
    driver_semantic_ids: tuple[str, ...] = Field(min_length=1)
    control_semantic_ids: tuple[str, ...] = ()
    mapping_semantic_ids: tuple[str, ...] = Field(min_length=1)
    schedule_ids: tuple[str, ...] = Field(min_length=1)
    equation_ids: tuple[str, ...] = Field(min_length=1)
    source_evidence_ids: tuple[str, ...] = Field(min_length=1)
    projection_coverage: Literal["all_model_spec_projection_periods"] = (
        "all_model_spec_projection_periods"
    )

    @model_validator(mode="after")
    def _canonical_sets(self) -> "BusinessModelCapability":
        for field_name in (
            "observation_semantic_ids",
            "driver_semantic_ids",
            "control_semantic_ids",
            "mapping_semantic_ids",
            "schedule_ids",
            "equation_ids",
            "source_evidence_ids",
        ):
            values = getattr(self, field_name)
            if values != tuple(sorted(set(values))):
                raise ValueError(f"{field_name} must be sorted and unique")
        return self


class DirectRevenueGrowthFallback(_Contract):
    """The degraded control a model kind falls back to, and what it cannot do.

    ``satisfies_required_denominator`` is pinned ``False`` by the type: a
    fallback that could satisfy the closed denominator would not be a fallback,
    and the constant is the mechanism's, not any model kind's.
    """

    semantic_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)
    disposition: Literal["degraded_draft_only"]
    satisfies_required_denominator: Literal[False]
    repair_capability_id: str = Field(pattern=_CAPABILITY_ID_PATTERN)


class BusinessModelScenarioDirectionRule(_Contract):
    """Direction is local to one heterogeneous control/output (DEC-10)."""

    semantic_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)
    bull_direction: Literal["increase", "decrease", "context_specific"]
    bear_direction: Literal["increase", "decrease", "context_specific"]

    @model_validator(mode="after")
    def _directions(self) -> "BusinessModelScenarioDirectionRule":
        if (
            self.bull_direction != "context_specific"
            and self.bull_direction == self.bear_direction
        ):
            raise ValueError("bull and bear direction must differ per control")
        return self


class BusinessModelCapabilityProfile(_Contract):
    """One model kind's closed annual capability profile.

    ``semantic_namespace`` is the model kind's declared id namespace: every
    semantic, capability, equation, schedule and binding id this profile
    carries must live inside it (plan §3 row S1). The mechanism owns the id
    *shape*; the model kind owns the namespace.
    """

    profile_id: str = Field(min_length=1)
    business_type: str = Field(min_length=1)
    semantic_namespace: str = Field(pattern=_NAMESPACE_PATTERN)
    semantic_concepts: tuple[BusinessModelSemanticConcept, ...] = Field(
        min_length=1
    )
    semantic_bindings: tuple[BusinessModelSemanticBinding, ...] = Field(
        min_length=1
    )
    equations: tuple[BusinessModelEquation, ...] = Field(min_length=1)
    schedules: tuple[BusinessModelSchedule, ...] = Field(min_length=1)
    capabilities: tuple[BusinessModelCapability, ...] = Field(min_length=1)
    scenario_direction_rules: tuple[
        BusinessModelScenarioDirectionRule, ...
    ] = Field(min_length=1)
    direct_revenue_growth_fallback: DirectRevenueGrowthFallback

    @model_validator(mode="after")
    def _references(self) -> "BusinessModelCapabilityProfile":
        namespace = f"{self.semantic_namespace}."
        declared_ids = (
            *(item.semantic_id for item in self.semantic_concepts),
            *(item.binding_id for item in self.semantic_bindings),
            *(item.equation_id for item in self.equations),
            *(item.schedule_id for item in self.schedules),
            *(item.capability_id for item in self.capabilities),
            *(item.semantic_id for item in self.scenario_direction_rules),
            self.direct_revenue_growth_fallback.semantic_id,
            self.direct_revenue_growth_fallback.repair_capability_id,
        )
        outside = sorted(
            {
                declared_id
                for declared_id in declared_ids
                if not declared_id.startswith(namespace)
            }
        )
        if outside:
            raise ValueError(
                "every declared id must live in this model kind's semantic "
                f"namespace {self.semantic_namespace!r}: {outside}"
            )
        concepts = {item.semantic_id: item for item in self.semantic_concepts}
        if len(concepts) != len(self.semantic_concepts):
            raise ValueError("semantic concept IDs must be unique")
        bindings = {item.binding_id: item for item in self.semantic_bindings}
        if len(bindings) != len(self.semantic_bindings):
            raise ValueError("semantic binding IDs must be unique")
        equations = {item.equation_id: item for item in self.equations}
        if len(equations) != len(self.equations):
            raise ValueError("equation IDs must be unique")
        schedules = {item.schedule_id: item for item in self.schedules}
        if len(schedules) != len(self.schedules):
            raise ValueError("schedule IDs must be unique")
        capabilities = {item.capability_id: item for item in self.capabilities}
        if len(capabilities) != len(self.capabilities):
            raise ValueError("capability IDs must be unique")
        directions = {
            item.semantic_id: item for item in self.scenario_direction_rules
        }
        if len(directions) != len(self.scenario_direction_rules):
            raise ValueError("scenario direction semantic IDs must be unique")
        unknown_directions = directions.keys() - concepts.keys()
        if unknown_directions:
            raise ValueError(
                "scenario directions reference unknown semantic concepts: "
                f"{sorted(unknown_directions)}"
            )
        invalid_directions = sorted(
            semantic_id
            for semantic_id in directions
            if concepts[semantic_id].kind not in {"driver", "control", "output"}
        )
        if invalid_directions:
            raise ValueError(
                "scenario directions require driver/control/output concepts: "
                f"{invalid_directions}"
            )
        fallback = concepts.get(
            self.direct_revenue_growth_fallback.semantic_id
        )
        if fallback is None or fallback.kind != "control":
            raise ValueError(
                "direct revenue growth fallback must reference a control concept"
            )
        # The fallback's *other* id — ``repair_capability_id`` — was a
        # ``Literal`` too, and its resolution check cannot live here: the
        # pointer names the capability whose *absence* put the model kind on
        # the fallback, so it is legitimately not among ``self.capabilities``
        # in exactly the degraded state the field exists to describe. The
        # vocabulary that can resolve it is the model kind's authored
        # denominator, so the check lives with the pair in
        # :func:`assess_business_model_sufficiency`.

        def require_kind(
            semantic_ids: tuple[str, ...],
            expected: SemanticKind,
            *,
            owner: str,
        ) -> None:
            wrong = sorted(
                semantic_id
                for semantic_id in semantic_ids
                if concepts.get(semantic_id) is not None
                and concepts[semantic_id].kind != expected
            )
            if wrong:
                raise ValueError(
                    f"{owner} requires {expected} semantic concepts: {wrong}"
                )

        output_ids: set[str] = set()
        for equation in self.equations:
            unknown = (
                {equation.output_semantic_id, *equation.operand_semantic_ids}
                - concepts.keys()
            )
            if unknown:
                raise ValueError(
                    f"equation {equation.equation_id} references unknown semantic "
                    f"concepts: {sorted(unknown)}"
                )
            if equation.output_semantic_id in output_ids:
                raise ValueError("one semantic output may have only one equation")
            output_ids.add(equation.output_semantic_id)
        for binding in self.semantic_bindings:
            unknown = {binding.semantic_id, binding.mapping_id} - concepts.keys()
            if unknown:
                raise ValueError(
                    f"binding {binding.binding_id} references unknown semantic "
                    f"concepts: {sorted(unknown)}"
                )
            if concepts[binding.mapping_id].kind != "mapping":
                raise ValueError(
                    f"binding {binding.binding_id} mapping_id must be a mapping"
                )
        for schedule in self.schedules:
            unknown_concepts = (
                set(schedule.observation_semantic_ids)
                | set(schedule.driver_semantic_ids)
                | set(schedule.policy_semantic_ids)
                | set(schedule.mapping_semantic_ids)
                | set(schedule.reconciliation_output_semantic_ids)
            ) - concepts.keys()
            unknown_equations = set(schedule.equation_ids) - equations.keys()
            if unknown_concepts or unknown_equations:
                raise ValueError(
                    f"schedule {schedule.schedule_id} has unknown references: "
                    f"{sorted(unknown_concepts | unknown_equations)}"
                )
            require_kind(
                schedule.observation_semantic_ids,
                "observation",
                owner=schedule.schedule_id,
            )
            require_kind(
                schedule.driver_semantic_ids,
                "driver",
                owner=schedule.schedule_id,
            )
            require_kind(
                schedule.policy_semantic_ids,
                "policy",
                owner=schedule.schedule_id,
            )
            require_kind(
                schedule.mapping_semantic_ids,
                "mapping",
                owner=schedule.schedule_id,
            )
            require_kind(
                schedule.reconciliation_output_semantic_ids,
                "output",
                owner=schedule.schedule_id,
            )
        for capability in self.capabilities:
            unknown_concepts = (
                set(capability.observation_semantic_ids)
                | set(capability.driver_semantic_ids)
                | set(capability.control_semantic_ids)
                | set(capability.mapping_semantic_ids)
            ) - concepts.keys()
            unknown_schedules = set(capability.schedule_ids) - schedules.keys()
            unknown_equations = set(capability.equation_ids) - equations.keys()
            if unknown_concepts or unknown_schedules or unknown_equations:
                raise ValueError(
                    f"capability {capability.capability_id} has unknown references: "
                    f"{sorted(unknown_concepts | unknown_schedules | unknown_equations)}"
                )
            require_kind(
                capability.observation_semantic_ids,
                "observation",
                owner=capability.capability_id,
            )
            require_kind(
                capability.driver_semantic_ids,
                "driver",
                owner=capability.capability_id,
            )
            for semantic_id in capability.control_semantic_ids:
                if concepts[semantic_id].kind not in {"control", "policy"}:
                    raise ValueError(
                        f"{capability.capability_id} controls must be control/policy "
                        f"semantic concepts: {semantic_id}"
                    )
            require_kind(
                capability.mapping_semantic_ids,
                "mapping",
                owner=capability.capability_id,
            )
        return self


SuccessorActionKind = Literal["rename", "rebind"]


class BusinessModelSemanticSuccessorAction(_Contract):
    """The complete BM-3 successor vocabulary (DEC-11); no split/merge."""

    action: SuccessorActionKind
    semantic_id: str = Field(pattern=_SEMANTIC_ID_PATTERN)
    binding_id: str | None = Field(default=None, pattern=_BINDING_ID_PATTERN)
    predecessor_value: str = Field(min_length=1)
    successor_value: str = Field(min_length=1)

    @model_validator(mode="after")
    def _shape(self) -> "BusinessModelSemanticSuccessorAction":
        if self.predecessor_value == self.successor_value:
            raise ValueError("successor action must describe an actual change")
        if self.action == "rename" and self.binding_id is not None:
            raise ValueError("rename must not name a binding")
        if self.action == "rebind" and self.binding_id is None:
            raise ValueError("rebind requires binding_id")
        return self


class BusinessModelSufficiencyGap(_Contract):
    capability_id: str
    missing_observation: str
    missing_driver: str
    missing_mapping: str
    missing_schedule: str
    message: str


class BusinessModelSufficiencyAssessment(_Contract):
    profile_id: str = Field(min_length=1)
    schema_valid: bool
    compiler_seedable: bool
    sufficient: bool
    topological_equation_ids: tuple[str, ...]
    gaps: tuple[BusinessModelSufficiencyGap, ...]


def _topological_equations(
    profile: BusinessModelCapabilityProfile,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    producer = {
        equation.output_semantic_id: equation.equation_id
        for equation in profile.equations
    }
    dependencies: dict[str, set[str]] = {}
    consumers: dict[str, set[str]] = defaultdict(set)
    for equation in profile.equations:
        required = {
            producer[operand]
            for operand in equation.operand_semantic_ids
            if operand in producer
        }
        dependencies[equation.equation_id] = required
        for dependency in required:
            consumers[dependency].add(equation.equation_id)
    ready = sorted(
        equation_id
        for equation_id, required in dependencies.items()
        if not required
    )
    ordered: list[str] = []
    while ready:
        equation_id = ready.pop(0)
        ordered.append(equation_id)
        for consumer in sorted(consumers[equation_id]):
            dependencies[consumer].discard(equation_id)
            if not dependencies[consumer] and consumer not in ordered + ready:
                ready.append(consumer)
                ready.sort()
    cyclic = tuple(sorted(set(dependencies) - set(ordered)))
    return tuple(ordered), cyclic


class BusinessModelSufficiencyDenominatorError(ValueError):
    """Root of the sufficiency-denominator refusal vocabulary (plan §6)."""

    code: ClassVar[str] = "business_model_sufficiency_refused"

    def __init__(self, message: str, *, repair: str) -> None:
        self.repair = repair
        super().__init__(f"{self.code}: {message} — {repair}")


class BusinessModelSufficiencyUndeclared(BusinessModelSufficiencyDenominatorError):
    """A model kind has no authored sufficiency denominator."""

    code: ClassVar[str] = BUSINESS_MODEL_SUFFICIENCY_UNDECLARED

    def __init__(self, *, profile_id: str, declared: tuple[str, ...]) -> None:
        self.profile_id = profile_id
        self.declared = declared
        super().__init__(
            f"model kind {profile_id!r} declares no sufficiency denominator; "
            f"the model-kind data lane holds {list(declared)}",
            repair=SUFFICIENCY_LANE,
        )


class BusinessModelSufficiencyInvalid(BusinessModelSufficiencyDenominatorError):
    """An authored sufficiency denominator is malformed."""

    code: ClassVar[str] = BUSINESS_MODEL_SUFFICIENCY_INVALID

    def __init__(self, *, artifact: str, field: str, detail: str) -> None:
        self.artifact = artifact
        self.field = field
        self.detail = detail
        super().__init__(
            f"sufficiency denominator {artifact!r} is invalid at {field!r}: {detail}",
            repair=SUFFICIENCY_REPAIR_LANE,
        )


def _denominator_invalid(artifact: str, field: str, detail: str) -> NoReturn:
    raise BusinessModelSufficiencyInvalid(
        artifact=artifact, field=field, detail=detail
    )


class RequiredCapability(_Contract):
    """One required capability and the concepts a gap in it points at."""

    capability_id: str = Field(pattern=_CAPABILITY_ID_PATTERN)
    missing_observation: str = Field(min_length=1)
    missing_driver: str = Field(min_length=1)
    missing_mapping: str = Field(min_length=1)
    missing_schedule: str = Field(min_length=1)


class BusinessModelSufficiencyDenominator(_Contract):
    """A model kind's closed capability denominator, as authored data (row S1).

    ``required_capabilities`` is the closed set a profile must declare to be
    sufficient, each carrying the repair targets its gap message names.
    ``cycle_gap_capability_id`` is the capability an equation-cycle gap is
    reported under — model-kind content too, since the cycle blocks *its*
    reconciliation.
    """

    profile_id: str = Field(min_length=1)
    semantic_namespace: str = Field(pattern=_NAMESPACE_PATTERN)
    cycle_gap_capability_id: str = Field(pattern=_CAPABILITY_ID_PATTERN)
    required_capabilities: tuple[RequiredCapability, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _shape(self) -> "BusinessModelSufficiencyDenominator":
        namespace = f"{self.semantic_namespace}."
        ids = tuple(row.capability_id for row in self.required_capabilities)
        if len(ids) != len(set(ids)):
            raise ValueError("required capability ids must be unique")
        if ids != tuple(sorted(ids)):
            raise ValueError("required capability ids must be sorted")
        outside = sorted(
            declared_id
            for declared_id in (*ids, self.cycle_gap_capability_id)
            if not declared_id.startswith(namespace)
        )
        if outside:
            raise ValueError(
                "every declared id must live in this model kind's semantic "
                f"namespace {self.semantic_namespace!r}: {outside}"
            )
        if self.cycle_gap_capability_id not in set(ids):
            raise ValueError(
                "the cycle gap capability must be one of the required capabilities"
            )
        return self

    def required_capability_ids(self) -> tuple[str, ...]:
        return tuple(row.capability_id for row in self.required_capabilities)

    def repair_targets(self) -> Mapping[str, RequiredCapability]:
        return {row.capability_id: row for row in self.required_capabilities}


def decode_sufficiency_denominator(
    payload: Any, *, artifact: str = "<payload>"
) -> BusinessModelSufficiencyDenominator:
    """Decode one authored denominator, fail-closed on every malformation."""

    if not isinstance(payload, Mapping):
        _denominator_invalid(artifact, "<artifact>", "must be an object")
    unknown = sorted(
        set(payload)
        - {
            "schema_version",
            "provenance",
            "profile_id",
            "semantic_namespace",
            "cycle_gap_capability_id",
            "required_capabilities",
        }
    )
    if unknown:
        _denominator_invalid(artifact, unknown[0], "unknown key")
    version = payload.get("schema_version")
    if version != SUFFICIENCY_SCHEMA_VERSION:
        _denominator_invalid(
            artifact,
            "schema_version",
            f"expected {SUFFICIENCY_SCHEMA_VERSION!r}, read {version!r}",
        )
    try:
        return BusinessModelSufficiencyDenominator.model_validate(
            {
                key: value
                for key, value in payload.items()
                if key not in {"schema_version", "provenance"}
            }
        )
    except ValueError as exc:
        _denominator_invalid(artifact, "<denominator>", str(exc))


@lru_cache(maxsize=None)
def _denominators(
    directory: str,
) -> Mapping[str, BusinessModelSufficiencyDenominator]:
    root = Path(directory)
    if not root.is_dir():
        _denominator_invalid(
            directory, "<library>", "the model-kind data directory does not exist"
        )
    decoded: dict[str, BusinessModelSufficiencyDenominator] = {}
    for path in sorted(root.glob("*_sufficiency.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            _denominator_invalid(path.name, "<artifact>", f"unreadable: {exc}")
        denominator = decode_sufficiency_denominator(payload, artifact=path.name)
        if denominator.profile_id in decoded:
            _denominator_invalid(
                path.name, "profile_id", f"declared twice: {denominator.profile_id!r}"
            )
        decoded[denominator.profile_id] = denominator
    return decoded


def load_sufficiency_denominator(
    profile_id: str, *, directory: Path | str = DEFAULT_MODEL_KIND_DIR
) -> BusinessModelSufficiencyDenominator:
    """Resolve a model kind's authored denominator, fail-closed on absence."""

    wanted = str(profile_id or "").strip()
    declared = _denominators(str(directory))
    denominator = declared.get(wanted)
    if denominator is None:
        raise BusinessModelSufficiencyUndeclared(
            profile_id=wanted, declared=tuple(sorted(declared))
        )
    return denominator


def assess_business_model_sufficiency(
    profile: BusinessModelCapabilityProfile | None,
    *,
    denominator: BusinessModelSufficiencyDenominator,
) -> BusinessModelSufficiencyAssessment:
    """Assess schema, seed topology, and annual sufficiency independently.

    ``denominator`` is the model kind's authored closed capability set (row
    S1). It is required rather than derived, because the absent-profile arm has
    to name every capability a profile *would* have to declare, and an absent
    profile declares nothing to derive it from.
    """

    repair_targets = denominator.repair_targets()
    required_ids = denominator.required_capability_ids()
    gaps: list[BusinessModelSufficiencyGap] = []
    if profile is None:
        for capability_id in required_ids:
            target = repair_targets[capability_id]
            gaps.append(
                BusinessModelSufficiencyGap(
                    capability_id=capability_id,
                    missing_observation=target.missing_observation,
                    missing_driver=target.missing_driver,
                    missing_mapping=target.missing_mapping,
                    missing_schedule=target.missing_schedule,
                    message="Annual capability is not declared.",
                )
            )
        return BusinessModelSufficiencyAssessment(
            profile_id=denominator.profile_id,
            schema_valid=True,
            compiler_seedable=False,
            sufficient=False,
            topological_equation_ids=(),
            gaps=tuple(gaps),
        )
    if profile.profile_id != denominator.profile_id:
        raise BusinessModelSufficiencyInvalid(
            artifact=denominator.profile_id,
            field="profile_id",
            detail=(
                "the denominator declares a different model kind than the "
                f"profile assessed ({profile.profile_id!r})"
            ),
        )
    # The degraded fallback's repair pointer names the capability an operator
    # must declare to escape it, so it has to be a capability this model kind's
    # closed denominator actually requires — the vocabulary the pre-IG-5
    # ``Literal`` on ``repair_capability_id`` pinned by hand, one issuer's
    # recurring-revenue capability spelled into the type. It deliberately need
    # not be *declared on the profile*: its absence is what
    # put the model kind on the fallback in the first place, so a profile-side
    # rule would refuse the state the field exists to describe. Checked here
    # because this is the one place both artifacts are present.
    repair_pointer = profile.direct_revenue_growth_fallback.repair_capability_id
    if repair_pointer not in required_ids:
        raise BusinessModelSufficiencyInvalid(
            artifact=denominator.profile_id,
            field="direct_revenue_growth_fallback.repair_capability_id",
            detail=(
                f"the profile's degraded-fallback repair pointer "
                f"({repair_pointer!r}) names no capability this model kind's "
                f"denominator requires: {list(required_ids)}"
            ),
        )

    ordered, cyclic = _topological_equations(profile)
    declared = {item.capability_id: item for item in profile.capabilities}
    for capability_id in required_ids:
        if capability_id in declared:
            continue
        target = repair_targets[capability_id]
        gaps.append(
            BusinessModelSufficiencyGap(
                capability_id=capability_id,
                missing_observation=target.missing_observation,
                missing_driver=target.missing_driver,
                missing_mapping=target.missing_mapping,
                missing_schedule=target.missing_schedule,
                message="Required capability is missing from the closed profile.",
            )
        )
    for equation_id in cyclic:
        equation = next(
            item for item in profile.equations if item.equation_id == equation_id
        )
        gaps.append(
            BusinessModelSufficiencyGap(
                capability_id=denominator.cycle_gap_capability_id,
                missing_observation=equation.output_semantic_id,
                missing_driver="acyclic economic dependency",
                missing_mapping="topological equation mapping",
                missing_schedule=equation_id,
                message="Economic equation cycle blocks compilation (DEC-12).",
            )
        )
    compiler_seedable = not cyclic
    sufficient = (
        compiler_seedable
        and not gaps
        and not profile.direct_revenue_growth_fallback.satisfies_required_denominator
    )
    return BusinessModelSufficiencyAssessment(
        profile_id=denominator.profile_id,
        schema_valid=True,
        compiler_seedable=compiler_seedable,
        sufficient=sufficient,
        topological_equation_ids=ordered,
        gaps=tuple(gaps),
    )


def validate_semantic_successor(
    predecessor: BusinessModelCapabilityProfile,
    successor: BusinessModelCapabilityProfile,
    actions: tuple[BusinessModelSemanticSuccessorAction, ...],
) -> None:
    """Require explicit, identity-preserving rename/rebind successor changes."""

    before_concepts = {
        concept.semantic_id: concept for concept in predecessor.semantic_concepts
    }
    after_concepts = {
        concept.semantic_id: concept for concept in successor.semantic_concepts
    }
    if before_concepts.keys() != after_concepts.keys():
        raise ValueError(
            "annual semantic identity set changed; BM-3 supports rename/rebind only"
        )
    before_bindings = {
        binding.binding_id: binding for binding in predecessor.semantic_bindings
    }
    after_bindings = {
        binding.binding_id: binding for binding in successor.semantic_bindings
    }
    if before_bindings.keys() != after_bindings.keys():
        raise ValueError(
            "annual binding identity set changed; BM-3 supports rename/rebind only"
        )

    required: set[tuple[str, str, str | None, str, str]] = set()
    for semantic_id, before in before_concepts.items():
        after = after_concepts[semantic_id]
        if before.label != after.label:
            required.add(
                ("rename", semantic_id, None, before.label, after.label)
            )
    for binding_id, before in before_bindings.items():
        after = after_bindings[binding_id]
        if before.semantic_id != after.semantic_id:
            raise ValueError("rebind cannot change the bound semantic identity")
        if before.mapping_id != after.mapping_id:
            required.add(
                (
                    "rebind",
                    before.semantic_id,
                    binding_id,
                    before.mapping_id,
                    after.mapping_id,
                )
            )
    supplied = {
        (
            action.action,
            action.semantic_id,
            action.binding_id,
            action.predecessor_value,
            action.successor_value,
        )
        for action in actions
    }
    if supplied != required:
        raise ValueError(
            "semantic_successor_actions must exactly describe rename/rebind changes"
        )


__all__ = [
    "BusinessModelCapability",
    "BusinessModelEquation",
    "BusinessModelSchedule",
    "BusinessModelScenarioDirectionRule",
    "BusinessModelSemanticBinding",
    "BusinessModelSemanticConcept",
    "BusinessModelSemanticSuccessorAction",
    "BusinessModelCapabilityProfile",
    "BusinessModelSufficiencyAssessment",
    "BusinessModelSufficiencyDenominator",
    "BusinessModelSufficiencyDenominatorError",
    "BusinessModelSufficiencyGap",
    "BusinessModelSufficiencyInvalid",
    "BusinessModelSufficiencyUndeclared",
    "BUSINESS_MODEL_SUFFICIENCY_INVALID",
    "BUSINESS_MODEL_SUFFICIENCY_UNDECLARED",
    "DirectRevenueGrowthFallback",
    "RequiredCapability",
    "SUFFICIENCY_SCHEMA_VERSION",
    "assess_business_model_sufficiency",
    "decode_sufficiency_denominator",
    "load_sufficiency_denominator",
    "validate_semantic_successor",
]
