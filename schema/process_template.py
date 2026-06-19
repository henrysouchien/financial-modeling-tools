from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from ._shared_slice import DILIGENCE_SECTION_KEYS
from .enum_canonicalizers import (
    canonicalize_optional_strategy,
    canonicalize_optional_timeframe,
)
from .strategy_categories.registry import validate_strategy_category_id


_DILIGENCE_KEYS = frozenset(DILIGENCE_SECTION_KEYS)
_FROZEN_CONTRACT = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
    populate_by_name=True,
    frozen=True,
)
_CONFIDENCE_LEVELS = frozenset({"low", "medium", "high"})

# Must match _shared_slice.DILIGENCE_SECTION_KEYS — parity enforced by tests/schema/test_process_template.py:312.
SectionKey = Literal[
    "business_overview",
    "thesis",
    "catalysts",
    "valuation",
    "assumptions",
    "risks",
    "peers",
    "ownership",
    "monitoring",
    "industry_analysis",
]
CompletionState = Literal["empty", "draft", "confirmed"]
StrategyCategoryId = Annotated[str, AfterValidator(validate_strategy_category_id)]
HoldingPeriodBias = Literal["near_term", "medium", "long_term"]


class InvestorProfile(BaseModel):
    strategy_bias: StrategyCategoryId | None = None
    holding_period_bias: HoldingPeriodBias | None = None
    style_notes: str | None = None

    @field_validator("strategy_bias", mode="before")
    @classmethod
    def _canonicalize_strategy_bias(cls, value: object | None) -> str | None:
        return canonicalize_optional_strategy(value)

    @field_validator("holding_period_bias", mode="before")
    @classmethod
    def _canonicalize_holding_period_bias(cls, value: object | None) -> str | None:
        return canonicalize_optional_timeframe(value)

    model_config = _FROZEN_CONTRACT


class SectionConfig(BaseModel):
    required: list[SectionKey] = Field(default_factory=list)
    order: list[SectionKey] = Field(default_factory=list)
    min_completion: dict[SectionKey, CompletionState] = Field(default_factory=dict)
    build_min_completion: dict[SectionKey, CompletionState] | None = None

    @model_validator(mode="after")
    def _validate_structure(self) -> SectionConfig:
        required_keys = set(self.required)
        order_keys = set(self.order)

        if len(required_keys) != len(self.required):
            raise ValueError("required contains duplicate section keys")
        if len(order_keys) != len(self.order):
            raise ValueError("order contains duplicate section keys")
        if self.order and order_keys != required_keys:
            raise ValueError(
                "order must be a permutation of required (same set, any order)"
            )
        if not set(self.min_completion).issubset(required_keys):
            unknown = sorted(set(self.min_completion) - required_keys)
            raise ValueError(f"min_completion keys not in required: {unknown}")
        if self.build_min_completion is not None and not set(
            self.build_min_completion
        ).issubset(required_keys):
            unknown = sorted(set(self.build_min_completion) - required_keys)
            raise ValueError(f"build_min_completion keys not in required: {unknown}")
        return self

    model_config = _FROZEN_CONTRACT


class SeedQualitativeFactor(BaseModel):
    category: str = Field(min_length=1)
    label: str = Field(min_length=1)
    guidance: str = Field(min_length=1)
    default_rating: Literal["low", "medium", "high"] | None = None
    default_data_shape: dict[str, Any] | None = None

    @field_validator("default_rating", mode="before")
    @classmethod
    def _canonicalize_default_rating(cls, value: object | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if not normalized:
            return None
        if normalized not in _CONFIDENCE_LEVELS:
            raise ValueError("default_rating must be one of low, medium, high")
        return normalized

    model_config = _FROZEN_CONTRACT


class RequiredSourceCoverage(BaseModel):
    min_filings: int | None = Field(default=None, ge=0)
    min_transcripts: int | None = Field(default=None, ge=0)
    min_investor_decks: int | None = Field(default=None, ge=0)
    min_other: int | None = Field(default=None, ge=0)

    model_config = _FROZEN_CONTRACT


class ProcessTemplate(BaseModel):
    template_id: str = Field(
        min_length=3, max_length=63, pattern=r"^[a-z][a-z0-9_]{2,62}$"
    )
    name: str = Field(min_length=1)
    description: str | None = None
    investor_profile: InvestorProfile | None = None
    section_config: SectionConfig = Field(default_factory=SectionConfig)
    seed_qualitative_factors: list[SeedQualitativeFactor] = Field(default_factory=list)
    valuation_methods_allowed: list[str] = Field(default_factory=list)
    methodology_tags: list[str] = Field(default_factory=list)
    required_source_coverage: RequiredSourceCoverage | None = None
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("seed_qualitative_factors")
    @classmethod
    def _reject_duplicate_seeds(
        cls,
        value: list[SeedQualitativeFactor],
    ) -> list[SeedQualitativeFactor]:
        seen: set[tuple[str, str]] = set()
        for seed in value:
            key = (seed.category, seed.label)
            if key in seen:
                raise ValueError(
                    "duplicate seed_qualitative_factors entry: "
                    f"category={seed.category!r}, label={seed.label!r}"
                )
            seen.add(key)
        return value

    @field_validator("valuation_methods_allowed")
    @classmethod
    def _reject_duplicate_valuation_methods(cls, value: list[str]) -> list[str]:
        stripped = [entry.strip() for entry in value]
        if any(not entry for entry in stripped):
            raise ValueError(
                "valuation_methods_allowed entries must be non-empty strings"
            )
        if len(set(stripped)) != len(stripped):
            raise ValueError("valuation_methods_allowed contains duplicates")
        return stripped

    @field_validator("methodology_tags")
    @classmethod
    def _reject_duplicate_methodology_tags(cls, value: list[str]) -> list[str]:
        stripped = [entry.strip() for entry in value]
        if any(not entry for entry in stripped):
            raise ValueError("methodology_tags entries must be non-empty strings")
        if len(set(stripped)) != len(stripped):
            raise ValueError("methodology_tags contains duplicates")
        return stripped

    model_config = _FROZEN_CONTRACT


__all__ = [
    "CompletionState",
    "HoldingPeriodBias",
    "InvestorProfile",
    "ProcessTemplate",
    "RequiredSourceCoverage",
    "SectionConfig",
    "SectionKey",
    "SeedQualitativeFactor",
    "StrategyCategoryId",
]
