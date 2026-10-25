"""Closed historical value seeds for BusinessModel nodes.

Two requirement scopes share one seed shape. An ``opening`` requirement is the
terminal historical level of a ``self[-1]`` growth node. A ``segment_revenue``
requirement is the filed history of a segment's revenue node that no
disclosed-series claim covers: its accepted seed names the terminal-period
value as a linear combination of filed facts, and that combination is the
node's series identity, so the compiler applies the same coefficients to the
same required roles in every earlier historical period.
"""

from __future__ import annotations

from decimal import Decimal
import hashlib
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .models import ModelScale
from industry_slice_core.industry_types import Unit
from .model_state_identity import validate_sha256_hex
from .versioned_artifact import canonical_json_bytes


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


HistoricalDriverSeedDerivationKind = Literal[
    "direct",
    "arithmetic_mean",
    "allocation",
    "ratio",
]

HistoricalDriverSeedScope = Literal["opening", "segment_revenue"]

#: The limitation an analyst-literal seed carries when the analyst stated none.
_ANALYST_LITERAL_LIMITATION = "analyst literal from cited filing passage"


class HistoricalDriverSeedRequirement(_Contract):
    """One exact historical value required by an accepted BMR node."""

    driver_id: str = Field(min_length=1, max_length=256)
    driver_key: str = Field(min_length=1, max_length=512)
    period_id: str = Field(min_length=1, max_length=64)
    unit: Unit
    model_scale: ModelScale
    observation_basis: Literal["period_flow", "period_end", "period_average"]
    predecessor_period_id: str | None = Field(default=None, min_length=1)
    allowed_derivation_kinds: tuple[HistoricalDriverSeedDerivationKind, ...] = Field(
        min_length=1
    )
    history_scope: HistoricalDriverSeedScope = Field(
        default="opening",
        exclude_if=lambda value: value == "opening",
    )

    @model_validator(mode="after")
    def _scale_matches_unit(self) -> "HistoricalDriverSeedRequirement":
        if self.unit is not Unit.dollars and self.model_scale != "units":
            raise ValueError("non-dollar historical driver seeds require units scale")
        if self.observation_basis == "period_average":
            if self.predecessor_period_id is not None:
                if self.allowed_derivation_kinds != ("arithmetic_mean", "direct"):
                    raise ValueError(
                        "allowed seed derivations differ from observation basis"
                    )
            elif self.allowed_derivation_kinds not in {
                ("arithmetic_mean", "direct"),
                ("direct",),
                ("allocation", "direct"),
                ("direct", "allocation"),
            }:
                raise ValueError(
                    "allowed seed derivations differ from observation basis"
                )
        else:
            if self.allowed_derivation_kinds != ("allocation", "direct"):
                raise ValueError(
                    "allowed seed derivations differ from observation basis"
                )
        if self.history_scope == "segment_revenue" and (
            self.unit is not Unit.dollars
            or self.observation_basis != "period_flow"
        ):
            raise ValueError("segment revenue history is a dollar period flow")
        if self.observation_basis == "period_average":
            if (
                self.predecessor_period_id is None
                and self.allowed_derivation_kinds == ("arithmetic_mean", "direct")
            ):
                raise ValueError(
                    "period-average seed requires exact predecessor period"
                )
        elif self.predecessor_period_id is not None:
            raise ValueError("period-average seed requires exact predecessor period")
        return self


HistoricalDriverSeedDisposition = Literal[
    "observed",
    "derived",
    "proxy",
    "analyst_normalized",
]


class HistoricalDriverSeedOperand(_Contract):
    """One typed, value-bearing input to a closed seed derivation."""

    kind: Literal[
        "historical_fact",
        "business_model_evidence_value",
        "analyst_literal",
        "captured_table_cell",
    ]
    fact_id: str | None = Field(default=None, min_length=1)
    evidence_id: str | None = Field(default=None, min_length=1)
    evidence_value_id: str | None = Field(default=None, min_length=1)
    #: ``captured_table_cell`` only: the captured KPI row (``capture_id``, ``row``,
    #: ``span``) the value was read from.
    capture_id: str | None = Field(
        default=None, min_length=1, exclude_if=lambda value: value is None
    )
    row: int | None = Field(default=None, ge=0, exclude_if=lambda value: value is None)
    span: str | None = Field(
        default=None, min_length=1, exclude_if=lambda value: value is None
    )
    period_id: str = Field(min_length=1, max_length=64)
    observation_basis: Literal["period_flow", "period_end", "period_average"]
    value: Decimal
    unit: Unit
    model_scale: ModelScale
    coefficient: Decimal | None = None

    @field_validator("value", "coefficient")
    @classmethod
    def _finite_decimal(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("historical driver seed operand must be finite")
        return value

    @model_validator(mode="after")
    def _shape(self) -> "HistoricalDriverSeedOperand":
        if self.unit is not Unit.dollars and self.model_scale != "units":
            raise ValueError("non-dollar historical driver seed operands require units scale")
        identifiers = (self.fact_id, self.evidence_id, self.evidence_value_id)
        if self.kind == "historical_fact" and (
            self.fact_id is None or any(identifiers[1:])
        ):
            raise ValueError("historical-fact operand requires only fact_id")
        if self.kind == "business_model_evidence_value" and (
            self.fact_id is not None
            or self.evidence_id is None
            or self.evidence_value_id is None
        ):
            raise ValueError(
                "BusinessModel evidence operand requires evidence_id and value_id"
            )
        if self.kind == "analyst_literal" and any(identifiers):
            raise ValueError("analyst literal operand cannot claim source identifiers")
        cell = (self.capture_id, self.row, self.span)
        if self.kind == "captured_table_cell":
            if any(value is None for value in cell):
                raise ValueError("captured-table-cell operand requires capture_id, row and span")
            if self.fact_id is not None or self.evidence_value_id is not None:
                raise ValueError(
                    "captured-table-cell operand names its cell, not a fact or evidence value"
                )
        elif any(value is not None for value in cell):
            raise ValueError("capture_id, row and span belong to captured-table-cell operands")
        return self


class HistoricalDriverSeed(_Contract):
    """One accepted historical value bound to an exact BMR seed requirement."""

    driver_id: str = Field(min_length=1, max_length=256)
    driver_key: str = Field(min_length=1, max_length=512)
    period_id: str = Field(min_length=1, max_length=64)
    value: Decimal
    unit: Unit
    model_scale: ModelScale
    disposition: HistoricalDriverSeedDisposition
    evidence_ids: tuple[str, ...] = ()
    supporting_evidence_ids: tuple[str, ...] = ()
    evidence_value_ids: tuple[str, ...] = ()
    fact_ids: tuple[str, ...] = ()
    derivation_kind: HistoricalDriverSeedDerivationKind
    derivation_operands: tuple[HistoricalDriverSeedOperand, ...] = Field(min_length=1)
    derivation_rationale: str = Field(min_length=1, max_length=4000)
    confidence: Literal["high", "medium", "low"]
    limitations: tuple[str, ...] = ()

    @field_validator(
        "evidence_ids",
        "supporting_evidence_ids",
        "evidence_value_ids",
        "fact_ids",
        "limitations",
    )
    @classmethod
    def _sorted_unique(cls, value: tuple[str, ...], info) -> tuple[str, ...]:
        if any(not item for item in value):
            raise ValueError(f"{info.field_name} must be non-empty")
        return tuple(sorted(set(value)))

    @field_validator("value")
    @classmethod
    def _finite_value(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("historical driver seed value must be finite")
        return value

    @model_validator(mode="after")
    def _analyst_literal_disposition(self) -> "HistoricalDriverSeed":
        """An analyst literal is an analyst-normalized seed by construction.

        An ``observed`` or ``derived`` seed whose operand is an
        ``analyst_literal`` is recorded as ``analyst_normalized`` and, when the
        analyst stated none, carries the limitation that says so: the reading
        is the analyst's, not a filed fact the platform bound. This is the
        schema's normalization, never a refusal; the confidence the analyst
        claims is recorded as claimed. It runs before ``_coherent``, which
        sees the normalized seed. The seed is still under construction (pydantic
        supports only ``self`` from an after-validator), so the two fields are
        set on it directly.
        """

        if not any(row.kind == "analyst_literal" for row in self.derivation_operands):
            return self
        if self.disposition in {"observed", "derived"}:
            object.__setattr__(self, "disposition", "analyst_normalized")
        if (
            self.disposition in {"proxy", "analyst_normalized"}
            and not self.limitations
        ):
            object.__setattr__(self, "limitations", (_ANALYST_LITERAL_LIMITATION,))
            self.__pydantic_fields_set__.add("limitations")
        return self

    @model_validator(mode="after")
    def _coherent(self) -> "HistoricalDriverSeed":
        if self.unit is not Unit.dollars and self.model_scale != "units":
            raise ValueError("non-dollar historical driver seeds require units scale")
        operand_evidence_ids = tuple(
            sorted(
                {
                    operand.evidence_id
                    for operand in self.derivation_operands
                    if operand.evidence_id is not None
                }
            )
        )
        operand_value_ids = tuple(
            sorted(
                {
                    operand.evidence_value_id
                    for operand in self.derivation_operands
                    if operand.evidence_value_id is not None
                }
            )
        )
        operand_fact_ids = tuple(
            sorted(
                operand.fact_id
                for operand in self.derivation_operands
                if operand.fact_id is not None
            )
        )
        if self.evidence_ids != operand_evidence_ids:
            raise ValueError("seed numeric evidence IDs must equal typed operands")
        if self.evidence_value_ids != operand_value_ids or self.fact_ids != operand_fact_ids:
            raise ValueError("seed value/fact IDs must equal typed operands")
        evidence_operand_pairs = tuple(
            (operand.evidence_id, operand.evidence_value_id)
            for operand in self.derivation_operands
            if operand.evidence_id is not None
        )
        if len(evidence_operand_pairs) != len(set(evidence_operand_pairs)) or len(
            operand_fact_ids
        ) != len(set(operand_fact_ids)):
            raise ValueError("historical driver seed operands repeat source identity")
        if self.derivation_kind != "ratio" and any(
            (operand.unit, operand.model_scale) != (self.unit, self.model_scale)
            for operand in self.derivation_operands
        ):
            raise ValueError("historical driver seed operands differ in unit or scale")
        analyst_operands = tuple(
            row for row in self.derivation_operands if row.kind == "analyst_literal"
        )
        if set(self.evidence_ids) & set(self.supporting_evidence_ids):
            raise ValueError("numeric and supporting evidence IDs cannot overlap")
        if analyst_operands and not self.supporting_evidence_ids:
            raise ValueError(
                "an analyst_literal needs at least one supporting_evidence_keys entry "
                "the context issued"
            )
        if not (
            self.evidence_ids or self.supporting_evidence_ids or self.fact_ids
        ):
            raise ValueError("historical driver seed requires durable authority")
        if self.disposition in {"proxy", "analyst_normalized"} and not self.limitations:
            raise ValueError("proxy and analyst-normalized seeds require limitations")
        if self.disposition == "observed" and self.derivation_kind != "direct":
            raise ValueError("observed historical driver seeds must be direct")
        if self.derivation_kind == "direct":
            if len(self.derivation_operands) != 1:
                raise ValueError("direct seed requires one operand")
            if self.derivation_operands[0].period_id != self.period_id:
                raise ValueError("direct seed operand must match output period")
            # ``None`` and ``1`` are the identity; any other coefficient is a unit
            # conversion the seed's derivation_rationale states.
            direct = self.derivation_operands[0]
            expected = direct.value * (
                Decimal(1) if direct.coefficient is None else direct.coefficient
            )
        elif self.derivation_kind == "ratio":
            if self.disposition != "derived":
                raise ValueError("ratio seeds are derived")
            if len(self.derivation_operands) != 2 or any(
                operand.period_id != self.period_id for operand in self.derivation_operands
            ):
                raise ValueError(
                    "ratio seed requires a numerator and a denominator in the output period"
                )
            numerator, denominator = self.derivation_operands
            if denominator.coefficient is not None or denominator.value == 0:
                raise ValueError("ratio seed denominator is an unweighted nonzero operand")
            # The numerator's coefficient, when given, is the unit conversion between
            # the operands' measure and the seed's.
            expected = (
                numerator.value
                * (Decimal(1) if numerator.coefficient is None else numerator.coefficient)
                / denominator.value
            )
        elif self.derivation_kind == "arithmetic_mean":
            if len(self.derivation_operands) != 2 or any(
                operand.coefficient is not None for operand in self.derivation_operands
            ):
                raise ValueError("arithmetic mean requires exactly two unweighted operands")
            expected = sum(
                (operand.value for operand in self.derivation_operands),
                Decimal(0),
            ) / Decimal(len(self.derivation_operands))
        else:
            if any(
                operand.coefficient is None for operand in self.derivation_operands
            ):
                raise ValueError("allocation requires a coefficient for every operand")
            expected = sum(
                (
                    operand.value * operand.coefficient
                    for operand in self.derivation_operands
                    if operand.coefficient is not None
                ),
                Decimal(0),
            )
        if self.value != expected:
            raise ValueError("historical driver seed value differs from typed derivation")
        return self


def historical_driver_seed_denominator_sha256(
    value: Sequence[HistoricalDriverSeedRequirement | Mapping[str, Any]],
) -> str:
    requirements = tuple(
        row
        if isinstance(row, HistoricalDriverSeedRequirement)
        else HistoricalDriverSeedRequirement.model_validate(row)
        for row in value
    )
    if not requirements:
        raise ValueError("historical driver seed denominator is empty")
    keys = tuple(row.driver_key for row in requirements)
    if keys != tuple(sorted(set(keys))):
        raise ValueError("historical driver seed requirements must be sorted and unique")
    driver_ids = tuple(row.driver_id for row in requirements)
    if len(driver_ids) != len(set(driver_ids)):
        raise ValueError("historical driver seed requirements repeat driver_id")
    return hashlib.sha256(
        b"historical-driver-seed-denominator.v1\0"
        + canonical_json_bytes(
            [row.model_dump(mode="json") for row in requirements]
        )
    ).hexdigest()


def validate_historical_driver_seed_denominator_sha256(value: str) -> str:
    return validate_sha256_hex(
        value,
        field_name="historical_driver_seed_denominator_sha256",
    )


__all__ = [
    "HistoricalDriverSeed",
    "HistoricalDriverSeedDerivationKind",
    "HistoricalDriverSeedDisposition",
    "HistoricalDriverSeedOperand",
    "HistoricalDriverSeedRequirement",
    "HistoricalDriverSeedScope",
    "historical_driver_seed_denominator_sha256",
    "validate_historical_driver_seed_denominator_sha256",
]
