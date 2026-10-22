from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator, model_validator

from industry_slice_core._contract import _ContractModel


PtChangeVerdict = Literal[
    "PT_CHANGE_COMPILED",
    "INSUFFICIENT_ARTIFACTS",
    "INSUFFICIENT_DATA",
    "REFRESH_ONLY",
]
PtChangeConfidence = Literal["HIGH", "MEDIUM", "LOW"]
PtChangeCirculationMode = Literal["company_note", "refresh_audit"]
PtChangeFundamentalKind = Literal["operating", "estimate", "thesis", "none"]
PtChangeStatus = Literal["PASS", "FAIL"]

REQUIRED_SOURCE_SKILLS = frozenset(
    {"dcf-relative-valuation", "expected-value-decision"}
)


class PtChangeSourceArtifact(_ContractModel):
    skill: str = Field(..., min_length=1)
    artifact_id: str = Field(..., min_length=1)
    ticker: str = Field(..., min_length=1)
    content_sha256: str = Field(..., min_length=16)
    schema_version: str = Field(..., min_length=1)
    created_at: str | None = None

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()


class PtChangeBottomLine(_ContractModel):
    action: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)


class PtChangePriceTarget(_ContractModel):
    spot: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    mid: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    implied_return_pct: float = Field(..., ge=-1, le=10)
    method: str = Field(..., min_length=1)
    horizon: str = Field(..., min_length=1)
    valuation_horizon: str | None = None
    caveat: str | None = None
    model_id: str | None = None
    model_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _ordered_range(self) -> "PtChangePriceTarget":
        if not self.low <= self.mid <= self.high:
            raise ValueError("price-target range must satisfy low <= mid <= high")
        return self


class PtChangeEvMateriality(_ContractModel):
    status: PtChangeStatus
    threshold_pct: float = Field(..., ge=0, le=1)
    expected_return_pct: float = Field(..., ge=-1, le=10)
    horizon: str = Field(..., min_length=1)


class PtChangeFundamentalChange(_ContractModel):
    kind: PtChangeFundamentalKind
    summary: str = Field(..., min_length=1)


class PtChangeAnalysisChange(_ContractModel):
    action: str = Field(..., min_length=1)
    what_changed: str = Field(..., min_length=1)
    fundamental_change: PtChangeFundamentalChange
    fundamental_source_refs: list[str] = Field(default_factory=list)
    dcf_vs_ev: str = Field(..., min_length=1)
    rating_implication: str = Field(..., min_length=1)
    rationale: str = Field(..., min_length=1)


class PtChangeMethodRow(_ContractModel):
    name: str = Field(..., min_length=1)
    target: float = Field(..., gt=0)
    multiple_display: str | None = None
    note: str | None = None


class PtChangeScenarioRow(_ContractModel):
    scenario: Literal["bull", "base", "bear"]
    synthesized_target: float = Field(..., gt=0)
    dcf_target: float | None = Field(default=None, gt=0)
    terminal_growth: float | None = Field(default=None, ge=-1, le=1)
    terminal_multiple: float | None = Field(default=None, gt=0)
    rationale: str | None = None


class PtChangeEvScenarioRow(_ContractModel):
    scenario: Literal["bull", "base", "bear"]
    probability: float = Field(..., ge=0, le=1)
    target: float = Field(..., gt=0)
    return_pct: float = Field(..., ge=-1, le=10)


class PtChangeEvCheckpoint(_ContractModel):
    as_of: str = Field(..., min_length=1)
    spot: float = Field(..., gt=0)
    expected_price: float = Field(..., gt=0)
    expected_return_pct: float = Field(..., ge=-1, le=10)
    return_to_risk: float = Field(..., ge=0)
    horizon: str = Field(..., min_length=1)
    rationale: str = Field(..., min_length=1)
    materiality_note: str = Field(..., min_length=1)
    scenarios: list[PtChangeEvScenarioRow] = Field(..., min_length=3, max_length=3)

    @model_validator(mode="after")
    def _validate_scenarios(self) -> "PtChangeEvCheckpoint":
        labels = [row.scenario for row in self.scenarios]
        if set(labels) != {"bull", "base", "bear"} or len(labels) != len(set(labels)):
            raise ValueError("EV scenarios must contain bull, base, and bear exactly once")
        if abs(sum(row.probability for row in self.scenarios) - 1.0) > 1e-6:
            raise ValueError("EV scenario probabilities must sum to 1")
        return self


class PtChangeSensitivity(_ContractModel):
    driver: str = Field(..., min_length=1)
    rank: int = Field(..., ge=1)
    delta_per_pct: float


class PtChangePack(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    ticker: str = Field(..., min_length=1)
    company_name: str = Field(..., min_length=1)
    verdict: PtChangeVerdict
    confidence: PtChangeConfidence
    as_of: str = Field(..., min_length=1)
    subtitle: str = Field(..., min_length=1)
    circulation_mode: PtChangeCirculationMode | None = None
    bottom_line: PtChangeBottomLine | None = None
    price_target: PtChangePriceTarget | None = None
    dcf_verdict_echo: str | None = None
    method_spread: str | None = None
    ev_materiality: PtChangeEvMateriality | None = None
    analysis_change: PtChangeAnalysisChange | None = None
    methods: list[PtChangeMethodRow] = Field(default_factory=list)
    scenarios: list[PtChangeScenarioRow] = Field(default_factory=list)
    ev_checkpoint: PtChangeEvCheckpoint | None = None
    sensitivities: list[PtChangeSensitivity] = Field(default_factory=list)
    data_gaps: list[str] = Field(default_factory=list)
    source_manifest: list[PtChangeSourceArtifact] = Field(default_factory=list)
    recommended_next_action: str = Field(..., min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_pack(self) -> "PtChangePack":
        if self.verdict != "PT_CHANGE_COMPILED":
            if not self.data_gaps:
                raise ValueError("non-compiled PT change requires data_gaps")
            return self

        required = {
            "circulation_mode": self.circulation_mode,
            "bottom_line": self.bottom_line,
            "price_target": self.price_target,
            "dcf_verdict_echo": self.dcf_verdict_echo,
            "method_spread": self.method_spread,
            "ev_materiality": self.ev_materiality,
            "analysis_change": self.analysis_change,
            "ev_checkpoint": self.ev_checkpoint,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise ValueError("compiled PT change missing sections: " + ", ".join(missing))
        if self.dcf_verdict_echo != "PRICE_TARGET_SET":
            raise ValueError("compiled PT change requires PRICE_TARGET_SET DCF verdict")
        if not self.methods:
            raise ValueError("compiled PT change requires method rows")
        scenario_labels = [row.scenario for row in self.scenarios]
        if set(scenario_labels) != {"bull", "base", "bear"} or len(scenario_labels) != len(set(scenario_labels)):
            raise ValueError("PT scenarios must contain bull, base, and bear exactly once")

        artifact_ids = [source.artifact_id for source in self.source_manifest]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("source_manifest artifact_ids must be unique")
        for required_skill in REQUIRED_SOURCE_SKILLS:
            if sum(source.skill == required_skill for source in self.source_manifest) != 1:
                raise ValueError(f"source_manifest requires exactly one {required_skill} artifact")
        if any(source.ticker != self.ticker for source in self.source_manifest):
            raise ValueError("source_manifest ticker must match pack ticker")

        assert self.analysis_change is not None
        fundamental = self.analysis_change.fundamental_change
        refs = self.analysis_change.fundamental_source_refs
        optional_ids = {
            source.artifact_id
            for source in self.source_manifest
            if source.skill not in REQUIRED_SOURCE_SKILLS
        }
        if not set(refs) <= optional_ids:
            raise ValueError("fundamental_source_refs must reference optional manifest artifacts")
        if self.circulation_mode == "company_note":
            if fundamental.kind == "none" or not refs:
                raise ValueError("company_note requires a sourced fundamental change")
        elif fundamental.kind != "none" or refs:
            raise ValueError("refresh_audit requires fundamental_change.kind=none and no refs")
        return self


__all__ = [
    "PtChangeAnalysisChange",
    "PtChangeBottomLine",
    "PtChangeCirculationMode",
    "PtChangeConfidence",
    "PtChangeEvCheckpoint",
    "PtChangeEvMateriality",
    "PtChangeEvScenarioRow",
    "PtChangeFundamentalChange",
    "PtChangeFundamentalKind",
    "PtChangeMethodRow",
    "PtChangePack",
    "PtChangePriceTarget",
    "PtChangeScenarioRow",
    "PtChangeSensitivity",
    "PtChangeSourceArtifact",
    "PtChangeStatus",
    "PtChangeVerdict",
    "REQUIRED_SOURCE_SKILLS",
]
