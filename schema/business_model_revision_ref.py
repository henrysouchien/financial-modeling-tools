"""Cycle-free identity contract for one immutable BusinessModel revision."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .model_state_identity import validate_sha256_hex


class BusinessModelRevisionRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    business_model_id: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


__all__ = ["BusinessModelRevisionRef"]
