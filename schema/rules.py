from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


SCHEMA_VERSION = "1.0"

EvidenceType = Literal[
    "vals",
    "dogfooding",
    "user_feedback",
    "manual_review",
    "post_incident",
]

Category = Literal[
    "numeric-precision",
    "basis-discipline",
    "source-routing",
    "source-fidelity",
    "arithmetic-verification",
    "final-answer-check",
    "framing",
]

_STRICT_CONFIG = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
    frozen=True,
)


class RuleEvidence(BaseModel):
    model_config = _STRICT_CONFIG

    type: EvidenceType
    q: str | None = None
    ref: str | None = None
    verified: date | None = None

    @model_validator(mode="after")
    def _type_specific_required_fields(self) -> "RuleEvidence":
        if self.type == "vals" and not self.q:
            raise ValueError("evidence.type='vals' requires `q` (vals question id)")
        if self.type != "vals" and not (self.ref or self.verified):
            raise ValueError(
                f"evidence.type='{self.type}' requires at least one of `ref` or `verified`"
            )
        return self


class Rule(BaseModel):
    model_config = _STRICT_CONFIG

    id: str = Field(
        pattern=r"^[a-z][a-z0-9_]*$",
        description="snake_case, starts with letter, unique across registry",
    )
    source_file: str
    created: date
    commit: str | None = None
    origin_note: str | None = None
    categories: list[Category] = Field(min_length=1)
    last_reviewed: date
    evidence: list[RuleEvidence] = Field(default_factory=list)
    description_summary: str

    @model_validator(mode="after")
    def _created_before_or_equal_last_reviewed(self) -> "Rule":
        if self.created > self.last_reviewed:
            raise ValueError(
                f"rule '{self.id}': created ({self.created}) must be <= "
                f"last_reviewed ({self.last_reviewed})"
            )
        return self


class RuleRegistry(BaseModel):
    model_config = _STRICT_CONFIG

    schema_version: Literal["1.0"] = "1.0"
    rules: list[Rule]

    @model_validator(mode="after")
    def _unique_rule_ids(self) -> "RuleRegistry":
        seen: set[str] = set()
        duplicates: list[str] = []
        for rule in self.rules:
            if rule.id in seen:
                duplicates.append(rule.id)
            seen.add(rule.id)
        if duplicates:
            raise ValueError(f"Duplicate rule ids in registry: {duplicates}")
        return self


__all__ = [
    "Category",
    "EvidenceType",
    "Rule",
    "RuleEvidence",
    "RuleRegistry",
    "SCHEMA_VERSION",
]
