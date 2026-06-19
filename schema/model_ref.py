from __future__ import annotations

from pydantic import BaseModel, Field

from ._insights_shared import _FROZEN_CONTRACT


class ModelVersionRef(BaseModel):
    model_id: str = Field(min_length=1)
    version: int = Field(ge=1)

    model_config = _FROZEN_CONTRACT


__all__ = [
    "ModelVersionRef",
]
