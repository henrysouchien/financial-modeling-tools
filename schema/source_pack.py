from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import Field

from industry_slice_core._contract import _ContractModel


_MANIFEST_PATH = Path(__file__).with_name("source_pack_manifest.json")
_MANIFEST = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
_CANONICAL_INTENTS: Final[tuple[str, ...]] = tuple(
    sorted(_MANIFEST["canonical_intents"])
)

INTENTS = Literal[
    "acquisition_strategy",
    "concentration_risk",
    "deal_status",
    "debt_terms",
    "generic_filing_question",
    "guidance_actuals",
    "non_gaap_reconciliation",
    "proxy_governance",
    "regulatory_risk",
    "revenue_disaggregation",
    "risk_factor",
    "security_offering_terms",
]
INTENT_ALIASES: dict[str, str] = dict(_MANIFEST["aliases"])

_PROVENANCE_FIELDS = frozenset(
    {"ticker", "fiscal_period", "form_type", "matched_intent_alias"}
)


def _canonical_intent(intent: str) -> str:
    normalized = str(intent).strip()
    return INTENT_ALIASES.get(normalized, normalized)


class SourceRef(_ContractModel):
    section: str
    subsections: list[str] = Field(default_factory=list)
    rationale: str


class SourcePack(_ContractModel):
    intent: INTENTS
    source_pack_origin: Literal["planner", "manual"]
    source_pack_version: str | None = None
    required_reads: list[SourceRef] = Field(..., min_length=1)
    optional_reads: list[SourceRef] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    rationale: str
    matched_intent_alias: str | None = None
    ticker: str | None = None
    fiscal_period: str | None = None
    form_type: str | None = None

    @classmethod
    def from_planner_result(
        cls, planner_result: Any, **provenance: Any
    ) -> "SourcePack":
        unknown = set(provenance) - _PROVENANCE_FIELDS
        if unknown:
            raise TypeError(
                f"Unsupported SourcePack provenance fields: {sorted(unknown)}"
            )

        matched_intent = str(getattr(planner_result, "matched_intent")).strip()
        intent = _canonical_intent(matched_intent)
        payload = {
            "intent": intent,
            "source_pack_origin": "planner",
            "source_pack_version": getattr(planner_result, "source_pack_version"),
            "required_reads": [
                _coerce_source_ref(ref)
                for ref in getattr(planner_result, "required_reads")
            ],
            "optional_reads": [
                _coerce_source_ref(ref)
                for ref in (getattr(planner_result, "optional_reads", []) or [])
            ],
            "warnings": list(getattr(planner_result, "warnings", []) or []),
            "rationale": getattr(planner_result, "rationale"),
            "matched_intent_alias": (
                matched_intent if matched_intent != intent else None
            ),
        }
        payload.update(provenance)
        return cls(**payload)

    @classmethod
    def manual(
        cls,
        intent: str,
        required_reads: list[SourceRef],
        rationale: str,
        **kwargs: Any,
    ) -> "SourcePack":
        canonical = _canonical_intent(intent)
        payload = dict(kwargs)
        if canonical != intent and "matched_intent_alias" not in payload:
            payload["matched_intent_alias"] = intent
        payload.update(
            {
                "intent": canonical,
                "source_pack_origin": "manual",
                "required_reads": required_reads,
                "rationale": rationale,
            }
        )
        return cls(**payload)

def _coerce_source_ref(value: Any) -> SourceRef:
    if isinstance(value, SourceRef):
        return value
    if isinstance(value, Mapping):
        return SourceRef.model_validate(value)
    return SourceRef(
        section=getattr(value, "section"),
        subsections=list(getattr(value, "subsections", []) or []),
        rationale=getattr(value, "rationale"),
    )
