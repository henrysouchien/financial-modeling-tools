"""Seal and verify the ``CompiledModelIntermediate``.

``compile_model_intermediate`` (``schema/build.py``) returns a sealed copy:
every field revalidated and the canonical projection digested into
``content_digest``. ``finalize_compiled_model`` verifies that digest before
it renders.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import TYPE_CHECKING

from pydantic import BaseModel

from .models import FinancialModel

if TYPE_CHECKING:
    from .build import CompiledModelIntermediate


def _canonical_intermediate_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, BaseModel):
        return {
            "__type__": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": value.model_dump(mode="json"),
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("intermediate datetime must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "__type__": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": {
                field.name: _canonical_intermediate_value(
                    getattr(value, field.name)
                )
                for field in fields(value)
            },
        }
    if isinstance(value, Mapping):
        pairs = [
            (
                _canonical_intermediate_value(key),
                _canonical_intermediate_value(item),
            )
            for key, item in value.items()
        ]
        pairs.sort(
            key=lambda pair: (
                pair[0]
                if isinstance(pair[0], str)
                else json.dumps(
                    pair[0], sort_keys=True, separators=(",", ":"), ensure_ascii=False
                )
            )
        )
        return {"__mapping__": [[key, item] for key, item in pairs]}
    if isinstance(value, (list, tuple)):
        return [_canonical_intermediate_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_canonical_intermediate_value(item) for item in value]
        items.sort(
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
        )
        return {"__set__": items}
    if isinstance(value, bytes):
        return {"__bytes_sha256__": hashlib.sha256(value).hexdigest()}
    if callable(value):
        return {
            "__callable__": (
                f"{getattr(value, '__module__', type(value).__module__)}."
                f"{getattr(value, '__qualname__', type(value).__qualname__)}"
            )
        }
    state = getattr(value, "__dict__", None)
    if isinstance(state, dict):
        return {
            "__type__": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": _canonical_intermediate_value(state),
        }
    raise TypeError(
        "unsupported intermediate state type: "
        f"{type(value).__module__}.{type(value).__qualname__}"
    )


def _compiled_intermediate_digest(
    intermediate: CompiledModelIntermediate,
) -> str:
    projection = {
        field.name: _canonical_intermediate_value(
            getattr(intermediate, field.name)
        )
        for field in fields(intermediate)
        if field.name != "content_digest"
    }
    encoded = json.dumps(
        projection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _deep_revalidate(value: object) -> object:
    if isinstance(value, BaseModel):
        restored = type(value).model_validate(value.model_dump(mode="python"))
        if isinstance(restored, FinancialModel):
            restored.build_index()
        return restored
    return deepcopy(value)


def _seal_compiled_intermediate(
    intermediate: CompiledModelIntermediate,
) -> CompiledModelIntermediate:
    cloned = replace(
        intermediate,
        **{
            field.name: _deep_revalidate(getattr(intermediate, field.name))
            for field in fields(intermediate)
            if field.name != "content_digest"
        },
    )
    return replace(cloned, content_digest=_compiled_intermediate_digest(cloned))


def _verified_compiled_intermediate(
    intermediate: CompiledModelIntermediate,
) -> CompiledModelIntermediate:
    observed = _compiled_intermediate_digest(intermediate)
    if observed != intermediate.content_digest:
        raise ValueError("compiled_model_intermediate_mutated")
    return intermediate
