"""Load adapter-owned FMP mappings onto filing-line targets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal, TypeAlias, cast


FmpBindingKind: TypeAlias = Literal[
    "filing_line",
    "aggregate_derivation",
    "vendor_convenience",
    "market_observation",
]
FmpFilingLineKey: TypeAlias = tuple[str, str]


@dataclass(frozen=True)
class ConceptMapping:
    """One FMP wire field and the filing definition it asserts, if any."""

    tag: str | None
    statement_role: str | None
    binding_kind: FmpBindingKind
    native_endpoint: str
    native_field: str
    required_role: str | None = None
    fallback_native_field: str | None = None
    negate: bool = False
    treat_zero_as_missing: bool = False

    def __post_init__(self) -> None:
        has_line = self.tag is not None
        if self.binding_kind == "filing_line":
            if not has_line or self.statement_role is None:
                raise ValueError("filing_line mapping requires tag and statement_role")
        elif has_line:
            raise ValueError(
                f"{self.binding_kind} mapping cannot make a single-tag claim"
            )
        if self.required_role is not None:
            required_role = self.required_role.strip()
            if required_role != self.required_role or not required_role:
                raise ValueError("required_role must be null or a non-empty string")
            plane_by_statement_role: dict[str | None, str] = {
                "income_statement": "is",
                "balance_sheet": "bs",
                "cash_flow": "cf",
            }
            expected_plane = plane_by_statement_role.get(self.statement_role)
            if (
                expected_plane is None
                or not required_role.startswith(f"statement.{expected_plane}.")
            ):
                raise ValueError(
                    "required_role must agree with the mapping statement role"
                )
        if not self.native_endpoint.strip() or not self.native_field.strip():
            raise ValueError("FMP native endpoint and field must be non-empty")

    @property
    def filing_line_key(self) -> FmpFilingLineKey | None:
        if self.tag is None or self.statement_role is None:
            return None
        return self.statement_role, self.tag


_CONFIG_PATH = Path(__file__).with_name("mapping_config.json")


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None

def _row_to_mapping(row: dict[str, object]) -> ConceptMapping:
    binding_kind = str(row["binding_kind"])
    if binding_kind not in {
        "filing_line",
        "aggregate_derivation",
        "vendor_convenience",
        "market_observation",
    }:
        raise ValueError(f"unsupported FMP binding kind: {binding_kind!r}")
    return ConceptMapping(
        tag=_optional_text(row.get("tag")),
        statement_role=_optional_text(row.get("statement_role")),
        binding_kind=cast(FmpBindingKind, binding_kind),
        required_role=_optional_text(row.get("required_role")),
        native_endpoint=str(row["native_endpoint"]),
        native_field=str(row["native_field"]),
        fallback_native_field=_optional_text(row.get("fallback_native_field")),
        negate=bool(row.get("negate")),
        treat_zero_as_missing=bool(row.get("treat_zero_as_missing")),
    )


@lru_cache(maxsize=1)
def load_concept_mappings() -> tuple[ConceptMapping, ...]:
    payload = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("mapping config must be a JSON array")
    return tuple(_row_to_mapping(row) for row in payload)


def mapping_by_filing_line() -> dict[FmpFilingLineKey, ConceptMapping]:
    mappings: dict[FmpFilingLineKey, ConceptMapping] = {}
    for mapping in load_concept_mappings():
        key = mapping.filing_line_key
        if key is None:
            continue
        previous = mappings.get(key)
        if previous is not None:
            if (
                previous.binding_kind,
                previous.native_endpoint,
                previous.native_field,
                previous.fallback_native_field,
                previous.negate,
                previous.treat_zero_as_missing,
            ) != (
                mapping.binding_kind,
                mapping.native_endpoint,
                mapping.native_field,
                mapping.fallback_native_field,
                mapping.negate,
                mapping.treat_zero_as_missing,
            ):
                raise ValueError(f"conflicting FMP mappings for filing line {key!r}")
            continue
        mappings[key] = mapping
    return mappings


def mapping_by_required_role() -> dict[str, ConceptMapping]:
    """Adapter-authored statement mappings keyed by build-model structure role."""

    mappings: dict[str, ConceptMapping] = {}
    for mapping in load_concept_mappings():
        required_role = mapping.required_role
        if required_role is None:
            continue
        previous = mappings.get(required_role)
        if previous is not None and previous != mapping:
            raise ValueError(f"conflicting FMP mappings for required role {required_role!r}")
        mappings[required_role] = mapping
    return mappings
