"""Version 2 durable contract for a complete company report section."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
import re
from typing import ClassVar, Literal

from pydantic import Field, model_validator

from .initiation_evidence import (
    AttributedClaim,
    AttributedValue,
    EstimateAlternative,
    EstimateValue,
)
from .thesis_shared_slice import DataGap, _ContractModel


_DECIMAL_WIRE_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_LEGACY_ESTIMATE_FIELDS = frozenset({"currency", "unit"})


def _require_decimal_wire_string(value: object, *, path: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{path} must be a nonempty JSON string")
    if _DECIMAL_WIRE_RE.fullmatch(value) is None:
        raise ValueError(f"{path} must use canonical decimal notation")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{path} must be a decimal") from exc
    if not number.is_finite() or str(number) != value:
        raise ValueError(f"{path} must use canonical decimal notation")


def _validate_raw_estimate(
    value: object,
    *,
    path: str,
    typed_model: type[EstimateValue] | type[EstimateAlternative] = EstimateValue,
) -> None:
    if value is None or isinstance(value, typed_model):
        return
    if not isinstance(value, Mapping):
        return

    legacy_fields = sorted(_LEGACY_ESTIMATE_FIELDS.intersection(value))
    if legacy_fields:
        joined = ", ".join(legacy_fields)
        raise ValueError(f"{path} contains forbidden legacy fields: {joined}")

    for field_name in ("point", "low", "high"):
        if field_name in value:
            _require_decimal_wire_string(
                value[field_name],
                path=f"{path}.{field_name}",
            )

    alternatives = value.get("alternatives")
    if isinstance(alternatives, Sequence) and not isinstance(
        alternatives, (str, bytes, bytearray)
    ):
        for index, alternative in enumerate(alternatives):
            _validate_raw_estimate(
                alternative,
                path=f"{path}.alternatives[{index}]",
                typed_model=EstimateAlternative,
            )


class CompanyBusinessProfile(_ContractModel):
    non_quantity_fields: ClassVar[tuple[str, ...]] = (
        "completed_research_steps",
        "total_applicable_steps",
    )

    description: AttributedValue[str]
    business_model_framing: AttributedValue[str]
    research_date: str = Field(min_length=1)
    completed_research_steps: int = Field(ge=0)
    total_applicable_steps: int = Field(ge=1)

    @model_validator(mode="after")
    def _validate_steps(self) -> "CompanyBusinessProfile":
        if self.completed_research_steps > self.total_applicable_steps:
            raise ValueError("completed research steps cannot exceed applicable steps")
        return self


class CompanyQualityAssessment(_ContractModel):
    assessment_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)
    judgment: AttributedClaim
    metric: EstimateValue | None = None


class ManagementProfile(_ContractModel):
    profile_id: str = Field(min_length=1)
    name: AttributedValue[str]
    title: AttributedValue[str]
    role_since: AttributedValue[str] | None = None
    company_tenure_since: AttributedValue[str] | None = None
    relevant_experience: AttributedClaim
    assessment: AttributedClaim


class GovernanceAssessment(_ContractModel):
    board_independence: EstimateValue | None = None
    independent_chair: AttributedValue[bool] | None = None
    incentive_alignment: AttributedClaim | None = None
    succession: AttributedClaim | None = None
    governance_risks: list[AttributedClaim] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_content(self) -> "GovernanceAssessment":
        if not any(
            (
                self.board_independence,
                self.independent_chair,
                self.incentive_alignment,
                self.succession,
                self.governance_risks,
            )
        ):
            raise ValueError("governance assessment requires at least one observation")
        return self


class ProductEconomicsObservation(_ContractModel):
    observation_id: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    denominator: str | None = Field(default=None, min_length=1)
    value: EstimateValue


class CompanyDiligence(_ContractModel):
    schema_version: Literal["2.0"] = "2.0"
    business_profile: CompanyBusinessProfile
    quality_assessments: list[CompanyQualityAssessment] = Field(default_factory=list)
    management_profiles: list[ManagementProfile] = Field(default_factory=list)
    governance: GovernanceAssessment | None = None
    product_economics: list[ProductEconomicsObservation] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _validate_quantity_wire_values(cls, value: object) -> object:
        if isinstance(value, cls) or not isinstance(value, Mapping):
            return value
        if "schema_version" not in value:
            raise ValueError("company_diligence.schema_version is required")

        quality_assessments = value.get("quality_assessments")
        if isinstance(quality_assessments, Sequence) and not isinstance(
            quality_assessments, (str, bytes, bytearray)
        ):
            for index, assessment in enumerate(quality_assessments):
                if isinstance(assessment, CompanyQualityAssessment):
                    continue
                if isinstance(assessment, Mapping):
                    _validate_raw_estimate(
                        assessment.get("metric"),
                        path=f"quality_assessments[{index}].metric",
                    )

        governance = value.get("governance")
        if isinstance(governance, Mapping):
            _validate_raw_estimate(
                governance.get("board_independence"),
                path="governance.board_independence",
            )

        product_economics = value.get("product_economics")
        if isinstance(product_economics, Sequence) and not isinstance(
            product_economics, (str, bytes, bytearray)
        ):
            for index, observation in enumerate(product_economics):
                if isinstance(observation, ProductEconomicsObservation):
                    continue
                if isinstance(observation, Mapping):
                    _validate_raw_estimate(
                        observation.get("value"),
                        path=f"product_economics[{index}].value",
                    )
        return value

    @model_validator(mode="after")
    def _unique_ids(self) -> "CompanyDiligence":
        assessment_ids = [row.assessment_id for row in self.quality_assessments]
        profile_ids = [row.profile_id for row in self.management_profiles]
        observation_ids = [row.observation_id for row in self.product_economics]
        for label, values in (
            ("quality assessment", assessment_ids),
            ("management profile", profile_ids),
            ("product economics observation", observation_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"{label} ids must be unique")
        return self


__all__ = [
    "CompanyBusinessProfile",
    "CompanyDiligence",
    "CompanyQualityAssessment",
    "GovernanceAssessment",
    "ManagementProfile",
    "ProductEconomicsObservation",
]
