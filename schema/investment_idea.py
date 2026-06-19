from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from api.memory.ticker_utils import TICKER_RE, normalize_ticker

from .enum_canonicalizers import (
    canonicalize_direction,
    canonicalize_strategy,
    canonicalize_timeframe,
)
from .process_template import StrategyCategoryId
from .research_labels import canonicalize_optional_research_label

if TYPE_CHECKING:
    from .handoff import IdeaProvenance


_IDEA_NAMESPACE = uuid.UUID("21befb5d-e5c9-4800-ae8c-e3dcc595b17e")


class _IdeaContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, populate_by_name=True, frozen=True)


def _require_non_empty(value: object, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _normalize_optional_string(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


class SourceRef(_IdeaContractModel):
    type: Literal["screen", "finding", "manual", "newsletter", "research_note", "external"]
    source_id: str
    source_repo: Literal["investment_tools", "ai_excel_addin", "manual", "external"]
    source_payload: dict[str, Any] | None = None

    @field_validator("source_id", mode="before")
    @classmethod
    def _validate_source_id(cls, value: object) -> str:
        return _require_non_empty(value, field_name="source_id")


class InvestmentIdeaRelated(_IdeaContractModel):
    findings: list[str] = Field(default_factory=list)
    screen_hits: list[str] = Field(default_factory=list)
    annotations: list[str] = Field(default_factory=list)


class InvestmentIdea(_IdeaContractModel):
    ticker: str
    thesis: str
    source: str
    source_date: date

    company_name: str | None = None
    strategy: StrategyCategoryId | None = None
    direction: Literal["long", "short", "hedge", "pair"] = "long"
    catalyst: str | None = None
    timeframe: Literal["near_term", "medium", "long_term"] | None = None
    conviction: int | None = Field(default=None, ge=1, le=5)
    tags: list[str] = Field(default_factory=list)

    idea_id: str
    surfaced_at: datetime
    source_ref: SourceRef

    related: InvestmentIdeaRelated = Field(default_factory=InvestmentIdeaRelated)

    suggested_process_template_id: str | None = None
    label: str | None = None

    metadata: dict[str, Any] = Field(default_factory=dict)

    schema_version: Literal["1.0"] = "1.0"

    @field_validator("ticker", mode="before")
    @classmethod
    def _normalize_ticker(cls, value: object) -> str:
        cleaned = normalize_ticker(str(value or ""))
        if not TICKER_RE.match(cleaned):
            raise ValueError(f"invalid ticker after normalization: {value!r} -> {cleaned!r}")
        return cleaned

    @field_validator("thesis", "source", "idea_id", mode="before")
    @classmethod
    def _validate_required_strings(cls, value: object, info: ValidationInfo) -> str:
        return _require_non_empty(value, field_name=info.field_name)

    @field_validator("company_name", "catalyst", "suggested_process_template_id", mode="before")
    @classmethod
    def _validate_optional_strings(cls, value: object | None) -> str | None:
        return _normalize_optional_string(value)

    @field_validator("label", mode="before")
    @classmethod
    def _validate_optional_label(cls, value: object | None) -> str | None:
        return canonicalize_optional_research_label(value)

    @field_validator("strategy", mode="before")
    @classmethod
    def _canonicalize_strategy(cls, value: Any) -> Any:
        return canonicalize_strategy(value) if value is not None else None

    @field_validator("direction", mode="before")
    @classmethod
    def _canonicalize_direction(cls, value: Any) -> Any:
        return canonicalize_direction(value) if value is not None else "long"

    @field_validator("timeframe", mode="before")
    @classmethod
    def _canonicalize_timeframe(cls, value: Any) -> Any:
        return canonicalize_timeframe(value) if value is not None else None

    @field_validator("conviction", mode="before")
    @classmethod
    def _validate_conviction(cls, value: object | None) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("conviction must be an int between 1 and 5")
        if value < 1 or value > 5:
            raise ValueError("conviction must be between 1 and 5")
        return value

    @field_validator("tags")
    @classmethod
    def _normalize_tags(cls, value: list[str]) -> list[str]:
        return [tag.strip() for tag in value if tag.strip()]

    @model_validator(mode="after")
    def _surfaced_at_ge_source_date(self) -> InvestmentIdea:
        if self.surfaced_at.date() < self.source_date:
            raise ValueError("surfaced_at must be on or after source_date")
        return self

    @classmethod
    def mint_idea_id(cls, source_repo: str, source_id: str) -> str:
        name = f"{source_repo.strip().lower()}|{source_id.strip()}"
        return str(uuid.uuid5(_IDEA_NAMESPACE, name))

def project_to_handoff_idea_provenance(idea: InvestmentIdea) -> IdeaProvenance:
    """Project InvestmentIdea -> 2-field IdeaProvenance for HandoffArtifact."""

    from .handoff import IdeaProvenance

    validated = InvestmentIdea.model_validate(idea)
    return IdeaProvenance(
        idea_id=validated.idea_id,
        source_summary=(
            f"{validated.source_ref.type}:"
            f"{validated.source_ref.source_repo}:"
            f"{validated.source_ref.source_id}"
        ),
    )


__all__ = [
    "InvestmentIdea",
    "InvestmentIdeaRelated",
    "SourceRef",
    "project_to_handoff_idea_provenance",
]
