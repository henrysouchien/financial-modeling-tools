"""Represent a pin's historical window as ordered basis segments.

The retired single-period representation could express only one basis for a
whole window and could not represent an issuer whose presentation changed
inside it. A ``MultiBasisWindow`` is:

* **ordered and exactly contiguous** — ``through_fy[i] + 1 == from_fy[i+1]``.
  A fiscal year inside the window always has exactly one basis segment;
  overlapping or gapped segments are unwritable. (A year whose filing is
  missing from the corpus is a *pin* problem — the source-unavailable
  machinery — never a window gap.)
* **total over the section's required roles** — every role in every segment is
  ``bound`` or typed ``not_presented``. The third state, silently missing from
  the map, is a parse error.
* **explicit about shortening** — the window's ``from_fy`` is a declared fact.
  A build requesting earlier years fails typed at compile; an undeclared
  shortening is unwritable as success. Shortening a ratified profile's window
  is a DEC-07-class re-ratification in the corpus/profile lane, never an
  admission-door decision.

**What the window deliberately does not store: identity continuations.**
Whether a role bound in adjacent bound spans continues by identity is derived
(``presentation_identity_unchanged``) and is recomputed every compile. Only
drift needs an artifact (a link) and only refusal needs an artifact (a break).
Storing "continues by identity" rows would be the second copy P1 forbids —
precisely the desynchronization the deleted merge-by-tag tree had.

This contract is issuer-generic by construction: no issuer identifier appears
here. Current pipeline context lives in
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``;
the owner map is ``schema/README.md``.

Two fields make validator inputs declared rather than ambient:

* ``MultiBasisWindow.required_roles`` — the section's required role names, so
  "``rows`` is total over the section's required roles" is checkable at
  construction;
* ``MultiBasisWindow.period_type`` — so "window/segment validation requires
  uniform ``period_type`` per section" (mechanism §7.2) has something to be
  uniform against.
"""

from __future__ import annotations

from typing import Annotated, Literal, Mapping

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import CoreSchema, core_schema

from .models import FinancialStatement
from industry_slice_core.industry_types import Unit
from .provider_port.registry import (
    ProviderId,
    ProviderRegistry,
    require_registered_provider_id,
)


FactDurationType = Literal["instant", "duration"]

#: Which period type each statement's face lines carry (mechanism §7.2:
#: balance-sheet lines are instants; income-statement and cash-flow lines are
#: durations). Data, not a heuristic — and the reason a window declares one.
_PERIOD_TYPE_BY_STATEMENT: Mapping[FinancialStatement, FactDurationType] = {
    FinancialStatement.balance_sheet: "instant",
    FinancialStatement.income_statement: "duration",
    FinancialStatement.cash_flow: "duration",
}


class _WindowContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _PositiveFormulaSignSchema:
    """Preserve the runtime ``Literal[1.0]`` contract with a valid type form."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: object,
        _handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        return core_schema.literal_schema([1.0])


_ExpectedFormulaSign = Annotated[float, _PositiveFormulaSignSchema]


def _require_non_blank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _normalize_unique_strings(values: list[str], *, field_name: str) -> list[str]:
    normalized = [_require_non_blank(value, field_name=field_name) for value in values]
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} values must be unique")
    return normalized


# ---------------------------------------------------------------------------
# Carried from the retired v1 authored contract (plan §3.1/§3.2 REPR rows).
#
# Step C-4 deletes `schema/statement_mappings.py` whole — the v1 window
# (`EffectivePeriod`), the v1 section/row bindings, the `FactBinding`
# `equivalence_group` arm, the balance-sheet `Literal`s, the `RollupPolicy`
# constants and the v1 compiler. What survives is orthogonal to correspondence
# and is re-homed here, on the artifact that now owns it: the evidence block,
# the projection applicability, the reviewed role allowlists, the provider
# route policy, and the insert-row machinery. Nothing is re-declared twice:
# these are moves, not copies.
#
# What deliberately did NOT come across: `FactBinding`. A segment row binding
# names one exact tag and carries no `match_policy` and no `registry_group_id`,
# so there is no field anywhere in this plane through which a tag-equivalence
# group could widen a binding (mechanism §2.7; plan §6.3 mutation family 2).
# ---------------------------------------------------------------------------


class ProjectionApplicability(_WindowContract):
    mode: Literal["latest_historical_basis"] = "latest_historical_basis"
    first_projection_fy: int = Field(ge=1900, le=2201)


class MappingEvidence(_WindowContract):
    """The reviewed filings behind a window: every effective year, pinned."""

    fiscal_years_reviewed: tuple[int, ...] = Field(min_length=1)
    filing_accessions: tuple[str, ...] = Field(min_length=1)
    rationale: str

    @field_validator("fiscal_years_reviewed")
    @classmethod
    def _reviewed_years_unique(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if len(value) != len(set(value)):
            raise ValueError("fiscal_years_reviewed values must be unique")
        return tuple(sorted(value))

    @field_validator("filing_accessions")
    @classmethod
    def _accessions_non_blank(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            _normalize_unique_strings(list(value), field_name="filing_accessions")
        )

    @field_validator("rationale")
    @classmethod
    def _rationale_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="rationale")


class TopologyPolicy(_WindowContract):
    """The reviewed role allowlists a segment reads its topology through."""

    presentation_parent_tags: tuple[str, ...] = Field(min_length=1)
    calculation_parent_tag: str
    basis_calculation_roles: tuple[str, ...] = Field(min_length=1)
    coverage_calculation_roles: tuple[str, ...] = ()
    presentation_roles: tuple[str, ...] = Field(min_length=1)
    basis_preference: Literal["calculation_then_presentation"] = (
        "calculation_then_presentation"
    )

    @field_validator(
        "presentation_parent_tags",
        "basis_calculation_roles",
        "coverage_calculation_roles",
        "presentation_roles",
    )
    @classmethod
    def _normalize_allowlists(cls, value: tuple[str, ...], info) -> tuple[str, ...]:
        return tuple(
            _normalize_unique_strings(list(value), field_name=info.field_name)
        )

    @field_validator("calculation_parent_tag")
    @classmethod
    def _calculation_parent_tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="calculation_parent_tag")


#: The reviewed role-allowlist vocabulary, carried from v1 unchanged
#: (mechanism §7.1: role scoping is reviewed data, never a heuristic — no
#: role-*discovery* heuristic is built for any statement).
TopologyRoleAllowlists = TopologyPolicy


class RoutePolicy(_WindowContract):
    """Which provider serves a bound role (plan §9 flag F2, carried to Stage D)."""

    preferred: ProviderId
    fallback_order: tuple[ProviderId, ...] = Field(min_length=1)

    @field_validator("preferred", mode="before")
    @classmethod
    def _normalize_preferred_provider(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> ProviderId:
        registry = (
            info.context.get("provider_registry")
            if isinstance(info.context, dict)
            else None
        )
        return require_registered_provider_id(
            value,
            provider_registry=(
                registry if isinstance(registry, ProviderRegistry) else None
            ),
            entry_point="statement route preferred provider",
        )

    @field_validator("fallback_order", mode="before")
    @classmethod
    def _normalize_fallback_providers(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        registry = (
            info.context.get("provider_registry")
            if isinstance(info.context, dict)
            else None
        )
        active_registry = registry if isinstance(registry, ProviderRegistry) else None
        return tuple(
            require_registered_provider_id(
                provider_id,
                provider_registry=active_registry,
                entry_point="statement route fallback provider",
            )
            for provider_id in value
        )

    @model_validator(mode="after")
    def _validate_route(self) -> "RoutePolicy":
        if self.fallback_order[0] != self.preferred:
            raise ValueError("fallback_order[0] must equal preferred")
        if len(self.fallback_order) != len(set(self.fallback_order)):
            raise ValueError("fallback_order sources must be unique")
        return self


class ExistingConceptTarget(_WindowContract):
    kind: Literal["existing_concept"]
    concept_id: str

    @field_validator("concept_id")
    @classmethod
    def _concept_id_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="concept_id")


class InsertRowPolicy(_WindowContract):
    mode: Literal["insert_or_bind_same_semantic"] = "insert_or_bind_same_semantic"
    insert_after_item_id: str
    formula_wiring_mode: Literal["external"] = "external"

    @field_validator("insert_after_item_id")
    @classmethod
    def _insert_after_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="insert_after_item_id")


class FormulaMembership(_WindowContract):
    parent_item_id: str
    insert_after_item_id: str
    expected_formula_sign: _ExpectedFormulaSign = 1.0

    @field_validator("parent_item_id", "insert_after_item_id")
    @classmethod
    def _item_ids_non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


class ForecastPolicy(_WindowContract):
    type: Literal["carry_forward", "prior_period", "flat", "zero"]
    rationale: str

    @field_validator("rationale")
    @classmethod
    def _rationale_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="rationale")


class InsertTarget(_WindowContract):
    kind: Literal["insert"]
    stable_item_id: str
    label: str
    unit: Unit
    row_policy: InsertRowPolicy
    formula_membership: FormulaMembership
    forecast_policy: ForecastPolicy

    @field_validator("stable_item_id", "label")
    @classmethod
    def _required_strings_non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


StatementRowTarget = Annotated[
    ExistingConceptTarget | InsertTarget,
    Field(discriminator="kind"),
]


class SectionTotalPolicy(_WindowContract):
    """The section total's model target and route.

    The total's *tag* is a per-segment fact (``BasisSegment.section_total_tag``)
    because a section total can be re-tagged mid-window exactly as a row can.
    The concept it writes and the provider that serves it are facts of the
    window: a section total that changed which model row it fills would be a
    different section, which is what a ``SectionRebasis`` break is for.
    """

    target_concept_id: str
    route: RoutePolicy

    @field_validator("target_concept_id")
    @classmethod
    def _target_concept_id_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="target_concept_id")


class SegmentRowBinding(_WindowContract):
    """A role bound to an exact tag inside one segment's filings.

    ``tag`` is an exact tag. There is no ``match_policy`` and no
    ``registry_group_id``: tag-equivalence is producer-side synonym resolution
    and cannot reach a correspondence decision through this plane.
    """

    disposition: Literal["bound"] = "bound"
    tag: str
    presence: Literal["presented_all_years", "optional_if_not_presented"]
    coverage: Literal["self", "subtree"] = "self"
    expected_weight: float = 1.0
    #: Which model row this role fills, and who serves it. Carried from the v1
    #: row binding (plan §3.2 REPR rows, §9 flag F2) onto the segment binding.
    #: The window validator requires every bound span of one role to declare
    #: the *same* target and route — a role that changed model row or provider
    #: mid-window is a different row, and the artifact for that is a break.
    target: StatementRowTarget
    route: RoutePolicy

    @field_validator("tag")
    @classmethod
    def _tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="tag")

    @property
    def concept_id(self) -> str | None:
        """The existing model concept this binding populates, if any."""

        target = self.target
        return (
            target.concept_id
            if isinstance(target, ExistingConceptTarget)
            else None
        )


class SegmentRowAbsence(_WindowContract):
    """A DECLARED absence: no line for this role in this segment's filings.

    Distinct from ``optional_if_not_presented`` on a bound row, which is a
    per-year presence outcome inside a segment where the role *is* bound.
    """

    disposition: Literal["not_presented"] = "not_presented"


SegmentRowDisposition = Annotated[
    SegmentRowBinding | SegmentRowAbsence,
    Field(discriminator="disposition"),
]


class DerivedBasis(_WindowContract):
    """The segment's basis is derived per filing from the calculation linkbase."""

    kind: Literal["derived"] = "derived"
    accession_by_year: Mapping[int, str]

    @field_validator("accession_by_year")
    @classmethod
    def _accessions_non_blank(cls, value: Mapping[int, str]) -> Mapping[int, str]:
        if not value:
            raise ValueError("accession_by_year must cover at least one year")
        return {
            int(year): _require_non_blank(accession, field_name="accession")
            for year, accession in value.items()
        }


class AdmittedBasis(_WindowContract):
    """The segment's basis is an admitted ``BasisDeclaration`` per filing.

    Admissible only where the derivation is genuinely ambiguous within a
    filing; the declaration door refuses ``declaration_contradicts_pin``
    otherwise (``ambiguity`` is min 1, and a derivable basis exhibits none).
    """

    kind: Literal["admitted"] = "admitted"
    declaration_id_by_year: Mapping[int, str]

    @field_validator("declaration_id_by_year")
    @classmethod
    def _ids_non_blank(cls, value: Mapping[int, str]) -> Mapping[int, str]:
        if not value:
            raise ValueError("declaration_id_by_year must cover at least one year")
        return {
            int(year): _require_non_blank(
                declaration_id, field_name="declaration_id"
            )
            for year, declaration_id in value.items()
        }


SegmentBasis = Annotated[DerivedBasis | AdmittedBasis, Field(discriminator="kind")]


class BasisSegment(_WindowContract):
    """One contiguous span of fiscal years sharing one presentation basis."""

    from_fy: int = Field(ge=1900, le=2200)
    through_fy: int = Field(ge=1900, le=2200)
    basis: SegmentBasis
    section_total_tag: str
    topology_roles: TopologyRoleAllowlists
    rows: Mapping[str, SegmentRowDisposition]

    @field_validator("section_total_tag")
    @classmethod
    def _total_tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="section_total_tag")

    @property
    def fiscal_years(self) -> tuple[int, ...]:
        return tuple(range(self.from_fy, self.through_fy + 1))

    @model_validator(mode="after")
    def _validate_segment(self) -> "BasisSegment":
        if self.through_fy < self.from_fy:
            raise ValueError("segment through_fy must be >= from_fy")
        if not self.rows:
            raise ValueError("segment rows must declare at least one role")
        for role in self.rows:
            _require_non_blank(role, field_name="segment row role")
        covered = set(self.fiscal_years)
        basis_years = set(
            self.basis.accession_by_year
            if isinstance(self.basis, DerivedBasis)
            else self.basis.declaration_id_by_year
        )
        missing = sorted(covered - basis_years)
        if missing:
            raise ValueError(f"segment basis does not cover fiscal years {missing!r}")
        extra = sorted(basis_years - covered)
        if extra:
            raise ValueError(
                f"segment basis names fiscal years outside the segment: {extra!r}"
            )
        bound_tags = [
            row.tag
            for row in self.rows.values()
            if isinstance(row, SegmentRowBinding)
        ]
        if len(bound_tags) != len(set(bound_tags)):
            raise ValueError("bound tags must be unique within a segment")
        if self.section_total_tag in bound_tags:
            raise ValueError("section_total_tag may not also be a bound row tag")
        return self


class MultiBasisWindow(_WindowContract):
    """The ticker/section's historical window: ordered, exactly contiguous segments."""

    #: The ``"{statement}.{section}"`` identity (mechanism §2.5). Validated to
    #: equal it, so C-3's repository keys and lineage folds cannot key on a
    #: self-declared name that disagrees with the artifact it names.
    window_id: str
    ticker: str
    statement: FinancialStatement
    section: str
    period_type: FactDurationType
    required_roles: tuple[str, ...] = Field(min_length=1)
    section_total: SectionTotalPolicy
    segments: tuple[BasisSegment, ...] = Field(min_length=1)
    declared_breaks: tuple[str, ...] = ()
    projection_applicability: ProjectionApplicability
    evidence: MappingEvidence
    tolerance_profile_id: str

    @field_validator("window_id", "ticker", "section", "tolerance_profile_id")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @field_validator("required_roles")
    @classmethod
    def _roles_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            _require_non_blank(role, field_name="required_roles") for role in value
        )
        if len(normalized) != len(set(normalized)):
            raise ValueError("required_roles values must be unique")
        return normalized

    @field_validator("declared_breaks")
    @classmethod
    def _breaks_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            _require_non_blank(break_id, field_name="declared_breaks")
            for break_id in value
        )
        if len(normalized) != len(set(normalized)):
            raise ValueError("declared_breaks values must be unique")
        return normalized

    @property
    def from_fy(self) -> int:
        return self.segments[0].from_fy

    @property
    def through_fy(self) -> int:
        return self.segments[-1].through_fy

    @property
    def fiscal_years(self) -> tuple[int, ...]:
        return tuple(range(self.from_fy, self.through_fy + 1))

    def bound_tag_for(self, row_role: str, fiscal_year: int) -> str | None:
        """The ONE tag this window binds ``row_role`` to in ``fiscal_year``.

        ``None`` where the year is outside the window, or where the covering
        segment declares the role ``not_presented``. There is no second answer
        and no fallback: a role has exactly one bound tag per year, which is
        what makes "the value came from the tag the window declares" checkable
        rather than a claim.
        """

        segment = self.segment_for(fiscal_year)
        if segment is None:
            return None
        disposition = segment.rows.get(str(row_role))
        return (
            disposition.tag
            if isinstance(disposition, SegmentRowBinding)
            else None
        )

    def bound_tags_for(self, row_role: str) -> tuple[str, ...]:
        """Every tag the window declares for ``row_role``, ordered by segment."""

        seen: list[str] = []
        for segment in self.segments:
            disposition = segment.rows.get(str(row_role))
            if isinstance(disposition, SegmentRowBinding) and disposition.tag not in seen:
                seen.append(disposition.tag)
        return tuple(seen)

    def binding_for(self, row_role: str) -> SegmentRowBinding | None:
        """The role's target/route carrier — identical across every bound span."""

        for segment in self.segments:
            disposition = segment.rows.get(str(row_role))
            if isinstance(disposition, SegmentRowBinding):
                return disposition
        return None

    @property
    def bound_roles(self) -> tuple[str, ...]:
        """Roles bound in at least one segment (the rest are declared absences)."""

        return tuple(
            role
            for role in self.required_roles
            if self.binding_for(role) is not None
        )

    def segment_for(self, fiscal_year: int) -> BasisSegment | None:
        """The one segment covering ``fiscal_year``, or None outside the window."""

        year = int(fiscal_year)
        return next(
            (
                segment
                for segment in self.segments
                if segment.from_fy <= year <= segment.through_fy
            ),
            None,
        )

    def uncovered_years(self, requested: tuple[int, ...]) -> tuple[int, ...]:
        """Requested years the segment union does not cover.

        The carried data of ``multi_basis_window_incomplete``, whose repair
        target is the profile lane: extend the window with new segments, or
        re-ratify a shorter model window (DEC-07-class). The refusal itself
        fires at compile, step C-4.
        """

        return tuple(
            year for year in sorted(set(requested)) if self.segment_for(year) is None
        )

    @model_validator(mode="after")
    def _validate_window(self) -> "MultiBasisWindow":
        expected_window_id = f"{self.statement.value}.{self.section}"
        if self.window_id != expected_window_id:
            raise ValueError(
                f"window_id must be the {'{statement}.{section}'} identity "
                f"{expected_window_id!r}; got {self.window_id!r}"
            )

        expected_period_type = _PERIOD_TYPE_BY_STATEMENT[self.statement]
        if self.period_type != expected_period_type:
            raise ValueError(
                f"{self.statement.value} lines are {expected_period_type} facts; "
                f"window declares {self.period_type!r}"
            )

        ordered = self.segments
        for earlier, later in zip(ordered, ordered[1:]):
            if later.from_fy == earlier.from_fy:
                raise ValueError("segments must be ordered and non-overlapping")
            if later.from_fy < earlier.through_fy + 1:
                raise ValueError(
                    f"segments overlap at FY{later.from_fy}: a fiscal year has "
                    "exactly one basis segment"
                )
            if later.from_fy > earlier.through_fy + 1:
                raise ValueError(
                    f"segments leave a gap between FY{earlier.through_fy} and "
                    f"FY{later.from_fy}: windows are exactly contiguous"
                )

        required = set(self.required_roles)
        for segment in ordered:
            declared = set(segment.rows)
            missing = sorted(required - declared)
            if missing:
                raise ValueError(
                    f"segment FY{segment.from_fy}-FY{segment.through_fy} does not "
                    f"declare required roles {missing!r}: every role is bound or "
                    "not_presented"
                )
            undeclared = sorted(declared - required)
            if undeclared:
                raise ValueError(
                    f"segment FY{segment.from_fy}-FY{segment.through_fy} declares "
                    f"roles outside the section's required set: {undeclared!r}"
                )

        expected_first_projection = self.through_fy + 1
        if self.projection_applicability.first_projection_fy != expected_first_projection:
            raise ValueError(
                "projection_applicability.first_projection_fy must equal the last "
                "segment's through_fy + 1"
            )

        reviewed = set(self.evidence.fiscal_years_reviewed)
        unreviewed = sorted(set(self.fiscal_years) - reviewed)
        if unreviewed:
            raise ValueError(
                f"evidence does not cover window fiscal years {unreviewed!r}"
            )

        # One role, one model target and one route across every bound span.
        # Without this the window could carry two answers to "which row does
        # this fill" and the compile would have to pick one — the second-copy
        # failure P1 forbids, in the shape that silently repoints a cell.
        for role in required:
            carriers = [
                disposition
                for segment in ordered
                if isinstance(
                    disposition := segment.rows[role],
                    SegmentRowBinding,
                )
            ]
            targets = {carrier.target.model_dump_json() for carrier in carriers}
            if len(targets) > 1:
                raise ValueError(
                    f"role {role!r} declares more than one row target across its "
                    "bound spans: a role that changes model row mid-window is a "
                    "different row (declare a break)"
                )
            routes = {carrier.route.model_dump_json() for carrier in carriers}
            if len(routes) > 1:
                raise ValueError(
                    f"role {role!r} declares more than one route across its bound "
                    "spans"
                )
            for carrier in carriers:
                target = carrier.target
                if (
                    isinstance(target, ExistingConceptTarget)
                    and target.concept_id != role
                ):
                    raise ValueError(
                        f"existing-concept target {target.concept_id!r} must equal "
                        f"row role {role!r}"
                    )

        total_concept = self.section_total.target_concept_id
        if total_concept in required:
            raise ValueError(
                "section_total target may not also be a required row role"
            )
        for segment in ordered:
            bound_tags = {
                row.tag
                for row in segment.rows.values()
                if isinstance(row, SegmentRowBinding)
            }
            if segment.section_total_tag in bound_tags:
                raise ValueError(
                    "section_total_tag may not also be a bound row tag"
                )

        stable_item_ids = [
            carrier.target.stable_item_id
            for segment in ordered
            for carrier in segment.rows.values()
            if isinstance(carrier, SegmentRowBinding)
            and isinstance(carrier.target, InsertTarget)
        ]
        if len(set(stable_item_ids)) != len(
            {
                role
                for segment in ordered
                for role, carrier in segment.rows.items()
                if isinstance(carrier, SegmentRowBinding)
                and isinstance(carrier.target, InsertTarget)
            }
        ):
            raise ValueError("insert target stable_item_id values must be unique")
        return self


__all__ = [
    "AdmittedBasis",
    "BasisSegment",
    "DerivedBasis",
    "ExistingConceptTarget",
    "ForecastPolicy",
    "FormulaMembership",
    "InsertRowPolicy",
    "InsertTarget",
    "MappingEvidence",
    "FactDurationType",
    "MultiBasisWindow",
    "ProjectionApplicability",
    "RoutePolicy",
    "SectionTotalPolicy",
    "SegmentBasis",
    "SegmentRowAbsence",
    "SegmentRowBinding",
    "SegmentRowDisposition",
    "StatementRowTarget",
    "TopologyPolicy",
    "TopologyRoleAllowlists",
]
