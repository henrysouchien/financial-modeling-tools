"""Bounded exact-authority context for postcompile valuation judgment."""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Annotated, Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .finding_vocabulary import Finding
from .postcompile_valuation import (
    CompiledCurrentPrice,
    CompiledPriceCases,
    CompiledValuationComponent,
    ExactModelVersionBinding,
)
from .scenario_set import ScenarioSetRef
from .valuation_policy import ValuationPolicyRef
from .versioned_artifact import canonical_json_bytes


MAX_POSTCOMPILE_CONTEXT_REF_LENGTH = 8192
PostcompileMethod = Literal[
    "dcf_exit_multiple",
    "dcf_terminal_growth",
    "ev_adjusted_ebitda",
    "forward_pe",
]
POSTCOMPILE_METHODS: tuple[PostcompileMethod, ...] = get_args(PostcompileMethod)


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


def postcompile_valuation_context_digest(
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    model_version: ExactModelVersionBinding | None = None,
    valuation_policy_ref: ValuationPolicyRef | None = None,
    valuation_policy_stage_receipt_digest: str | None = None,
    scenario_set_ref: ScenarioSetRef | None = None,
    compiled_valuation_snapshot_sha256: str | None = None,
) -> str:
    payload = {
        "schema_version": "postcompile-valuation-context.v1",
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "model_version": model_version,
        "valuation_policy_ref": valuation_policy_ref,
        "valuation_policy_stage_receipt_digest": (
            valuation_policy_stage_receipt_digest
        ),
        "scenario_set_ref": scenario_set_ref,
        "compiled_valuation_snapshot_sha256": (
            compiled_valuation_snapshot_sha256
        ),
    }
    return hashlib.sha256(
        b"postcompile-valuation-context.v1\0" + canonical_json_bytes(payload)
    ).hexdigest()

class PostcompileValuationContextBinding(_Contract):
    """What one issued context is bound to. An upstream the context could not
    resolve (no current ModelVersion, or a version that binds no
    ValuationPolicy) is ``None`` here and a
    ``valuation_context_upstream_unresolved`` Finding on the response."""

    schema_version: Literal["postcompile-valuation-context-binding.v1"] = (
        "postcompile-valuation-context-binding.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1, max_length=32)
    model_version: ExactModelVersionBinding | None = None
    valuation_policy_ref: ValuationPolicyRef | None = None
    valuation_policy_stage_receipt_digest: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    scenario_set_ref: ScenarioSetRef | None = None
    compiled_valuation_snapshot_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    context_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _digest(self) -> "PostcompileValuationContextBinding":
        expected = postcompile_valuation_context_digest(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
            model_version=self.model_version,
            valuation_policy_ref=self.valuation_policy_ref,
            valuation_policy_stage_receipt_digest=(
                self.valuation_policy_stage_receipt_digest
            ),
            scenario_set_ref=self.scenario_set_ref,
            compiled_valuation_snapshot_sha256=(
                self.compiled_valuation_snapshot_sha256
            ),
        )
        if self.context_content_sha256 != expected:
            raise ValueError("postcompile valuation context digest mismatch")
        return self


def encode_postcompile_valuation_context_ref(
    binding: PostcompileValuationContextBinding,
) -> str:
    payload = canonical_json_bytes(binding.model_dump(mode="json"))
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    checksum = hashlib.sha256(
        b"postcompile-valuation-context-ref.v1\0" + payload
    ).hexdigest()
    value = f"pcvcx1.{encoded}.{checksum}"
    if len(value) > MAX_POSTCOMPILE_CONTEXT_REF_LENGTH:
        raise ValueError("postcompile valuation context ref exceeds its wire bound")
    return value


def decode_postcompile_valuation_context_ref(
    value: str,
) -> PostcompileValuationContextBinding:
    try:
        prefix, encoded, checksum = value.split(".", 2)
        if prefix != "pcvcx1" or len(checksum) != 64:
            raise ValueError
        payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        expected = hashlib.sha256(
            b"postcompile-valuation-context-ref.v1\0" + payload
        ).hexdigest()
        if not hmac.compare_digest(checksum, expected):
            raise ValueError
        return PostcompileValuationContextBinding.model_validate_json(payload)
    except (TypeError, ValueError) as exc:
        raise ValueError("postcompile valuation context ref is malformed") from exc


class PostcompileValuationMethodProjection(_Contract):
    schema_version: Literal["postcompile-valuation-method-projection.v1"] = (
        "postcompile-valuation-method-projection.v1"
    )
    method: PostcompileMethod
    status: Literal["available", "unavailable", "invalid"]
    prices: CompiledPriceCases | None = None
    output_keys: tuple[str, ...]
    limitations: tuple[str, ...]

    @field_validator("output_keys", "limitations")
    @classmethod
    def _canonical_strings(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))) or any(not item for item in value):
            raise ValueError("postcompile method strings must be sorted and unique")
        return value


class PostcompileValuationContextHeader(_Contract):
    schema_version: Literal["postcompile-valuation-context-header.v1"] = (
        "postcompile-valuation-context-header.v1"
    )
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    context_ref: str = Field(min_length=1, max_length=MAX_POSTCOMPILE_CONTEXT_REF_LENGTH)
    model_version: ExactModelVersionBinding | None = None
    valuation_policy_ref: ValuationPolicyRef | None = None
    scenario_set_ref: ScenarioSetRef | None = None
    compiled_valuation_snapshot_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )

class PostcompileValuationContextSummary(_Contract):
    kind: Literal["summary"] = "summary"
    current_price: CompiledCurrentPrice
    first_forecast_year: int
    terminal_forecast_year: int
    wacc: tuple[CompiledValuationComponent, ...]
    enterprise_to_equity_bridge: tuple[CompiledValuationComponent, ...]
    methods: tuple[PostcompileValuationMethodProjection, ...]


class PostcompileValuationMethodDetail(_Contract):
    kind: Literal["method"] = "method"
    method: PostcompileValuationMethodProjection
    components: tuple[CompiledValuationComponent, ...]


PostcompileValuationContextProjection = Annotated[
    PostcompileValuationContextSummary | PostcompileValuationMethodDetail,
    Field(discriminator="kind"),
]


class PostcompileValuationContextResponse(_Contract):
    """One issued context. ``findings`` carries ``valuation_context_reissued``
    when a carried context_ref's authority moved and this is the current one,
    and ``valuation_context_upstream_unresolved`` for each upstream the context
    could not resolve (its header field is then ``None``). ``projection`` is
    absent until a compiled snapshot resolves. ``next_call`` produces the first
    missing upstream."""

    header: PostcompileValuationContextHeader
    projection: PostcompileValuationContextProjection | None = None
    findings: tuple[Finding, ...] = ()
    next_call: dict[str, Any] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )


__all__ = [
    "MAX_POSTCOMPILE_CONTEXT_REF_LENGTH",
    "POSTCOMPILE_METHODS",
    "PostcompileMethod",
    "PostcompileValuationContextBinding",
    "PostcompileValuationContextHeader",
    "PostcompileValuationContextProjection",
    "PostcompileValuationContextResponse",
    "PostcompileValuationContextSummary",
    "PostcompileValuationMethodDetail",
    "PostcompileValuationMethodProjection",
    "decode_postcompile_valuation_context_ref",
    "encode_postcompile_valuation_context_ref",
    "postcompile_valuation_context_digest",
]
