from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import Field, model_validator

from schema.thesis_shared_slice import _ContractModel


_MANIFEST_PATH = Path(__file__).with_name("source_pack_manifest.json")
_MANIFEST = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
_CANONICAL_INTENTS: Final[tuple[str, ...]] = tuple(
    sorted(_MANIFEST["canonical_intents"])
)

INTENTS = Literal[_CANONICAL_INTENTS]  # type: ignore[valid-type]
INTENT_ALIASES: dict[str, str] = dict(_MANIFEST["aliases"])

_PROVENANCE_FIELDS = frozenset(
    {"ticker", "fiscal_period", "form_type", "matched_intent_alias"}
)


def _canonical_intent(intent: str) -> str:
    normalized = str(intent).strip()
    return INTENT_ALIASES.get(normalized, normalized)


def _looks_like_investment_idea_source_ref(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    return "section" not in value and {"type", "source_id", "source_repo"} <= set(value)


class SourceRef(_ContractModel):
    section: str
    subsections: list[str] = Field(default_factory=list)
    rationale: str

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # schema.SourceRef was already public for InvestmentIdea. Keep that
        # construction path working while exporting this filing SourceRef there.
        if cls is SourceRef:
            candidate = args[0] if len(args) == 1 and not kwargs else kwargs
            if _looks_like_investment_idea_source_ref(candidate):
                from schema.investment_idea import SourceRef as IdeaSourceRef

                if args and not kwargs:
                    return IdeaSourceRef.model_validate(args[0])
                return IdeaSourceRef(**kwargs)
        return super().__new__(cls)

    @classmethod
    def model_validate(cls, obj: Any, *args: Any, **kwargs: Any) -> Any:
        if cls is SourceRef and _looks_like_investment_idea_source_ref(obj):
            from schema.investment_idea import SourceRef as IdeaSourceRef

            return IdeaSourceRef.model_validate(obj, *args, **kwargs)
        return super().model_validate(obj, *args, **kwargs)


class SourcePack(_ContractModel):
    intent: INTENTS
    source_pack_origin: Literal["planner", "manual"]
    source_pack_version: str | None = None
    source_pack_sha256: str | None = None
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
            raise TypeError(f"Unsupported SourcePack provenance fields: {sorted(unknown)}")

        matched_intent = str(getattr(planner_result, "matched_intent")).strip()
        intent = _canonical_intent(matched_intent)
        payload = {
            "intent": intent,
            "source_pack_origin": "planner",
            "source_pack_version": getattr(planner_result, "source_pack_version"),
            "source_pack_sha256": getattr(planner_result, "source_pack_sha256"),
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
                "source_pack_sha256": None,
                "required_reads": required_reads,
                "rationale": rationale,
            }
        )
        return cls(**payload)

    @model_validator(mode="after")
    def _enforce_origin_invariants(self) -> "SourcePack":
        if self.source_pack_origin == "planner" and self.source_pack_sha256 is None:
            raise ValueError("planner-origin SourcePack must carry source_pack_sha256")
        if self.source_pack_origin == "manual" and self.source_pack_sha256 is not None:
            raise ValueError("manual-origin SourcePack must NOT claim source_pack_sha256")
        return self


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
