from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .process_template import StrategyCategoryId


_FROZEN_CONTRACT = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
    populate_by_name=True,
    frozen=True,
)

SiaPhase = Literal[
    "foundation",
    "fundamental-analysis",
    "strategic-evaluation",
    "decision-making",
    "risk-management",
    "valuation-modeling",
    "pitch",
]
WikiArticleType = Literal["concept", "case", "framework", "pattern"]
WikiFamily = Literal[
    "structural-advantage",
    "financial-signature",
    "setup/decision",
    "cycle",
    "idea-sourcing",
]
WikiTimeHorizon = Literal[
    "near_term",
    "medium",
    "long_term",
    "near_term_medium",
    "medium_long_term",
    "near_term_long_term",
    "variable",
    "inherits",
    "per_case_type",
]
Difficulty = Literal["beginner", "intermediate", "advanced"]
MAX_WIKI_SOURCE_MODULE = 8


class WikiArticle(BaseModel):
    title: str = Field(min_length=1)
    type: WikiArticleType
    source_modules: list[int] = Field(default_factory=list)
    related: list[str] = Field(default_factory=list)
    cases: list[str] = Field(default_factory=list)
    family: WikiFamily | None = None
    time_horizon: WikiTimeHorizon | None = None

    # Computed at load time, not in frontmatter.
    slug: str = Field(min_length=1)
    body_md: str = Field(default="")

    @field_validator("source_modules")
    @classmethod
    def _validate_modules(cls, value: list[int]) -> list[int]:
        for module in value:
            if not 1 <= module <= MAX_WIKI_SOURCE_MODULE:
                raise ValueError(
                    f"source_module must be in 1..{MAX_WIKI_SOURCE_MODULE}; got {module}"
                )
        return value

    schema_version: str = Field(default="1.0")
    model_config = _FROZEN_CONTRACT


class MethodologyUnit(BaseModel):
    name: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9-]*$")
    description: str = Field(min_length=1)
    sia_module: int = Field(ge=1, le=7)
    sia_phase: SiaPhase
    concepts: list[str] = Field(default_factory=list)
    tools: list[str] = Field(default_factory=list)
    prerequisites: list[str] = Field(default_factory=list)
    case_studies: list[str] = Field(default_factory=list)
    difficulty: Difficulty
    version: str = Field(min_length=1)

    # v1.0 two-field categorization.
    process_template_categories: list[StrategyCategoryId] = Field(default_factory=list)
    methodology_tags: list[str] = Field(default_factory=list)

    # Computed at load time.
    body_md: str = Field(default="")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if value.endswith("-"):
            raise ValueError("name must not end with '-'")
        return value

    schema_version: str = Field(default="1.0")
    model_config = _FROZEN_CONTRACT


__all__ = [
    "Difficulty",
    "MAX_WIKI_SOURCE_MODULE",
    "MethodologyUnit",
    "SiaPhase",
    "WikiArticle",
    "WikiArticleType",
    "WikiFamily",
    "WikiTimeHorizon",
]
