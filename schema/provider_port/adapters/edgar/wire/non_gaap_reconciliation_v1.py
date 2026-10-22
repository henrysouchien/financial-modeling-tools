"""Strict consumer for Edgar-owned issuer non-GAAP reconciliation artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import re
from typing import Annotated, Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from value_semantics_core import (
    ReconciliationComponent,
    TwoStageReconciliationAvailable,
    reconcile_two_stage_bridge,
)


ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_KIND = (
    "canonical_json_of_issuer_non_gaap_reconciliation"
)
ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_CODEC = (
    "issuer-non-gaap-reconciliation.v1+canonical-json"
)
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_ACCESSION_PATTERN = r"^\d{10}-\d{2}-\d{6}$"
_DECIMAL_PATTERN = r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$"

# Numeric meaning of each declared ``value_scale`` token: multiply a
# reconciliation line value by its factor to express it in currency units.
# Owned here with the ``value_scale`` Literal so consumers derive scaling
# instead of pinning one token.
VALUE_SCALE_FACTORS: Mapping[str, Decimal] = {
    "units": Decimal(1),
    "thousands": Decimal(10) ** 3,
    "millions": Decimal(10) ** 6,
    "billions": Decimal(10) ** 9,
}

# ``currency_label`` is the issuer's own free-form label; these are the
# spellings that denote U.S. dollars. Owned beside the field declaration so
# consumers normalize instead of hand-copying label sets.
_USD_CURRENCY_LABELS = frozenset({"$", "US$", "USD", "U.S. DOLLARS", "US DOLLARS"})


def is_usd_currency_label(label: str) -> bool:
    """True when an issuer-declared currency label denotes U.S. dollars."""

    return label.strip().upper() in _USD_CURRENCY_LABELS


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("reconciliation values must be finite")
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"-0", ""} else rendered


class NonGaapCellLocator(_Contract):
    document_sha256: str = Field(pattern=_SHA256_PATTERN)
    table_sha256: str = Field(pattern=_SHA256_PATTERN)
    table_index: int = Field(ge=0)
    row_index: int = Field(ge=0)
    column_index: int = Field(ge=0)
    result_key: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def _canonical_result_key(self) -> "NonGaapCellLocator":
        expected = (
            f"tables[{self.table_index}].rows[{self.row_index}]"
            f".cells[{self.column_index}]"
        )
        if self.result_key != expected:
            raise ValueError("cell result_key must be the canonical table coordinate")
        return self


class NonGaapTextEvidence(_Contract):
    text: str = Field(min_length=1)
    content_norm: Literal["visible-document-text.v1"] = "visible-document-text.v1"
    content_sha256: str = Field(pattern=_SHA256_PATTERN)
    char_start: int = Field(ge=0)
    char_end: int = Field(gt=0)
    result_key: str = Field(min_length=1, max_length=200)
    footnote_key: str | None = Field(default=None, pattern=r"^[1-9]\d?$")

    @model_validator(mode="after")
    def _ordered_span(self) -> "NonGaapTextEvidence":
        if self.char_start >= self.char_end:
            raise ValueError("text evidence span must be ordered")
        expected = f"visible_document_text[{self.char_start}:{self.char_end}]"
        if self.result_key != expected:
            raise ValueError("text result_key must be the canonical character span")
        return self


class NonGaapAcceptedAtEvidence(_Contract):
    artifact_kind: str = Field(min_length=1, max_length=100)
    artifact_locator: str = Field(min_length=1, max_length=2048)
    content_encoding: Literal["identity", "gzip"] = "identity"
    document_sha256: str = Field(pattern=_SHA256_PATTERN)
    document_byte_count: int = Field(gt=0)
    decompressed_document_sha256: str = Field(pattern=_SHA256_PATTERN)
    decompressed_document_byte_count: int = Field(gt=0)
    result_key: str = Field(min_length=1, max_length=500)
    raw_value: str = Field(min_length=1, max_length=100)
    canonical_accepted_at: datetime

    @model_validator(mode="after")
    def _validate_evidence(self) -> "NonGaapAcceptedAtEvidence":
        if (
            self.canonical_accepted_at.tzinfo is None
            or self.canonical_accepted_at.utcoffset() != timedelta(0)
        ):
            raise ValueError("canonical_accepted_at must be aware UTC")
        if self.content_encoding == "identity" and (
            self.document_sha256 != self.decompressed_document_sha256
            or self.document_byte_count != self.decompressed_document_byte_count
        ):
            raise ValueError("identity-encoded accepted-at source hashes must match")
        return self


class NonGaapDocumentSource(_Contract):
    producer_id: Literal["edgar_updater"] = "edgar_updater"
    ticker: str = Field(min_length=1, max_length=20)
    cik: str = Field(pattern=r"^\d{1,10}$")
    form: str = Field(min_length=1, max_length=20)
    accession: str = Field(pattern=_ACCESSION_PATTERN)
    accepted_at: datetime
    filing_date: date
    report_date: date | None = None
    filing_url: str = Field(min_length=1, max_length=2048)
    document_url: str = Field(min_length=1, max_length=2048)
    document_name: str = Field(min_length=1, max_length=512)
    artifact_kind: str = Field(min_length=1, max_length=100)
    artifact_locator: str = Field(min_length=1, max_length=2048)
    document_content_type: Literal["html", "markdown"]
    content_encoding: Literal["identity", "gzip"] = "identity"
    document_sha256: str = Field(pattern=_SHA256_PATTERN)
    document_byte_count: int = Field(gt=0)
    decompressed_document_sha256: str = Field(pattern=_SHA256_PATTERN)
    decompressed_document_byte_count: int = Field(gt=0)
    accepted_at_evidence: NonGaapAcceptedAtEvidence
    amendment_status: Literal["original", "amendment", "not_determined"]
    supersedes_accession: str | None = Field(default=None, pattern=_ACCESSION_PATTERN)
    superseded_by_accession: str | None = Field(
        default=None, pattern=_ACCESSION_PATTERN
    )

    @model_validator(mode="after")
    def _validate_source_identity(self) -> "NonGaapDocumentSource":
        if self.accepted_at.tzinfo is None or self.accepted_at.utcoffset() != timedelta(0):
            raise ValueError("accepted_at must be aware UTC")
        if self.report_date is not None and self.report_date > self.filing_date:
            raise ValueError("report_date must not be after filing_date")
        if self.accepted_at.date() < self.filing_date:
            raise ValueError("accepted_at must not precede filing_date")
        normalized_form = self.form.strip().upper()
        if normalized_form.endswith("/A") and self.amendment_status != "amendment":
            raise ValueError("amended forms require amendment_status=amendment")
        if not normalized_form.endswith("/A") and self.amendment_status == "amendment":
            raise ValueError("amendment_status=amendment requires an amended form")
        if self.supersedes_accession is not None and self.amendment_status != "amendment":
            raise ValueError("supersedes_accession requires an amended filing")
        if self.supersedes_accession == self.accession:
            raise ValueError("a filing cannot supersede itself")
        if self.superseded_by_accession == self.accession:
            raise ValueError("a filing cannot be superseded by itself")
        if self.content_encoding == "identity" and (
            self.document_sha256 != self.decompressed_document_sha256
            or self.document_byte_count != self.decompressed_document_byte_count
        ):
            raise ValueError("identity-encoded source hashes must match")
        if self.accepted_at_evidence.canonical_accepted_at != self.accepted_at:
            raise ValueError("accepted_at must equal its exact source evidence")
        return self


class NonGaapReconciliationPeriod(_Contract):
    period_type: Literal["Q", "YTD", "FY"]
    fiscal_year: int = Field(ge=1994, le=2200)
    fiscal_quarter: int | None = Field(default=None, ge=1, le=4)
    period_start: date | None = None
    period_end: date
    printed_header: str = Field(min_length=1, max_length=1000)
    value_column_ordinal: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_period(self) -> "NonGaapReconciliationPeriod":
        if self.period_type in {"Q", "YTD"} and self.fiscal_quarter is None:
            raise ValueError("quarter and YTD periods require fiscal_quarter")
        if self.period_type == "FY" and self.fiscal_quarter not in {None, 4}:
            raise ValueError("FY fiscal_quarter must be absent or 4")
        if self.period_start is not None and self.period_start >= self.period_end:
            raise ValueError("period_start must be before period_end")
        return self

_PRINTED_NUMBER_TOKEN_RE = re.compile(r"\d+(?:,\d{3})*(?:\.(\d+))?|\.(\d+)")
_TRAILING_FOOTNOTE_PAREN_RE = re.compile(
    r"(?<=[0-9%\)])\s*\(\s*[1-9]\d?\s*\)\s*$|\*+"
)
_DASH_FOOTNOTE_ONLY_RE = re.compile(
    r"^[\s\$\(—–\-]*(?:\(\s*[1-9]\d?\s*\))?[\s\)%]*$"
)


def printed_decimal_places(value_raw: str) -> int | None:
    """Number of fractional digits in a verbatim printed cell, or ``None`` for a blank/dash."""
    raw = (value_raw or "").strip()
    if not raw:
        return None
    cleaned = _TRAILING_FOOTNOTE_PAREN_RE.sub(" ", raw)
    if any(dash in cleaned for dash in ("—", "–", "--")) and _DASH_FOOTNOTE_ONLY_RE.match(cleaned):
        return None
    match = _PRINTED_NUMBER_TOKEN_RE.search(cleaned)
    if match is None:
        return None
    frac = match.group(1) or match.group(2) or ""
    return len(frac)

class NonGaapReconciliationLine(_Contract):
    issuer_label: str = Field(min_length=1, max_length=1000)
    value: str = Field(pattern=_DECIMAL_PATTERN)
    value_raw: str = Field(min_length=1, max_length=200)
    locator: NonGaapCellLocator
    footnote_keys: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _canonical_value(self) -> "NonGaapReconciliationLine":
        if _canonical_decimal(Decimal(self.value)) != self.value:
            raise ValueError("reconciliation values must be canonical Decimal strings")
        if tuple(dict.fromkeys(self.footnote_keys)) != self.footnote_keys:
            raise ValueError("footnote_keys must be unique and ordered")
        if any(re.fullmatch(r"[1-9]\d?", key) is None for key in self.footnote_keys):
            raise ValueError("footnote_keys must be numeric source markers")
        return self

    @property
    def decimal_places(self) -> int | None:
        """Printed decimal places of the verbatim source cell (``None`` for dash placeholders)."""
        return printed_decimal_places(self.value_raw)

    @property
    def half_ulp(self) -> Decimal:
        """Half a unit in the last printed place of this cell (``0`` for dash placeholders)."""
        places = self.decimal_places
        if places is None:
            return Decimal(0)
        return Decimal("0.5") * (Decimal(10) ** (-places))


class NonGaapReconciliationComponent(NonGaapReconciliationLine):
    stage: Literal["gaap_to_ebitda", "ebitda_to_adjusted_ebitda"]
    component_role: Literal[
        "interest_expense",
        "income_tax_expense_benefit",
        "depreciation_and_amortization",
        "stock_based_compensation_and_payroll_taxes",
        "other_items",
    ]


class IssuerNonGaapReconciliationV1(_Contract):
    contract_version: Literal["issuer-non-gaap-reconciliation.v1"] = (
        "issuer-non-gaap-reconciliation.v1"
    )
    non_gaap_metric: Literal["adjusted_ebitda"] = "adjusted_ebitda"
    issuer_metric_label: str = Field(min_length=1, max_length=1000)
    gaap_metric: Literal["net_income"] = "net_income"
    currency_label: str = Field(min_length=1, max_length=40)
    value_scale: Literal["units", "thousands", "millions", "billions"]
    source: NonGaapDocumentSource
    period: NonGaapReconciliationPeriod
    definition: NonGaapTextEvidence
    footnotes: tuple[NonGaapTextEvidence, ...] = ()
    gaap_base: NonGaapReconciliationLine
    components: tuple[NonGaapReconciliationComponent, ...]
    ebitda_subtotal: NonGaapReconciliationLine
    disclosed_adjusted_ebitda: NonGaapReconciliationLine
    recomputed_ebitda: str = Field(pattern=_DECIMAL_PATTERN)
    ebitda_residual: str = Field(pattern=_DECIMAL_PATTERN)
    recomputed_adjusted_ebitda: str = Field(pattern=_DECIMAL_PATTERN)
    adjusted_ebitda_residual: str = Field(pattern=_DECIMAL_PATTERN)
    arithmetic_closed: bool

    @model_validator(mode="after")
    def _validate_arithmetic_and_bindings(self) -> "IssuerNonGaapReconciliationV1":
        lines: tuple[NonGaapReconciliationLine, ...] = (
            self.gaap_base,
            *self.components,
            self.ebitda_subtotal,
            self.disclosed_adjusted_ebitda,
        )
        if any(
            line.locator.document_sha256 != self.source.decompressed_document_sha256
            for line in lines
        ):
            raise ValueError("every reconciliation line must bind the source document hash")
        table_identities = {
            (line.locator.table_index, line.locator.table_sha256) for line in lines
        }
        if len(table_identities) != 1:
            raise ValueError("every reconciliation line must bind one exact table")
        expected_roles = (
            "interest_expense",
            "income_tax_expense_benefit",
            "depreciation_and_amortization",
            "stock_based_compensation_and_payroll_taxes",
            "other_items",
        )
        expected_stages = (
            "gaap_to_ebitda",
            "gaap_to_ebitda",
            "gaap_to_ebitda",
            "ebitda_to_adjusted_ebitda",
            "ebitda_to_adjusted_ebitda",
        )
        if tuple(row.component_role for row in self.components) != expected_roles:
            raise ValueError(
                "complete Adjusted EBITDA bridge requires each canonical component role once"
            )
        if tuple(row.stage for row in self.components) != expected_stages:
            raise ValueError("canonical component roles must use their defined bridge stage")
        result = reconcile_two_stage_bridge(
            starting_value=Decimal(self.gaap_base.value),
            first_stage_components=tuple(
                ReconciliationComponent(row.component_role, Decimal(row.value))
                for row in self.components[:3]
            ),
            reported_subtotal=Decimal(self.ebitda_subtotal.value),
            second_stage_components=tuple(
                ReconciliationComponent(row.component_role, Decimal(row.value))
                for row in self.components[3:]
            ),
            reported_output=Decimal(self.disclosed_adjusted_ebitda.value),
        )
        actual = (
            self.recomputed_ebitda,
            self.ebitda_residual,
            self.recomputed_adjusted_ebitda,
            self.adjusted_ebitda_residual,
        )
        expected = (
            _canonical_decimal(result.recomputed_subtotal),
            _canonical_decimal(result.subtotal_residual),
            _canonical_decimal(result.recomputed_output),
            _canonical_decimal(result.output_residual),
        )
        if actual != expected:
            raise ValueError("published recomputation fields must match the disclosed bridge")
        if self.arithmetic_closed != isinstance(result, TwoStageReconciliationAvailable):
            raise ValueError("arithmetic_closed must match both exact residuals")
        required_footnotes = {key for line in lines for key in line.footnote_keys}
        published_footnotes = {row.footnote_key for row in self.footnotes}
        if None in published_footnotes:
            raise ValueError("footnote evidence requires a footnote_key")
        if required_footnotes - published_footnotes:
            raise ValueError("every referenced reconciliation footnote must be published")
        return self


class NonGaapNetIncomeComponent(NonGaapReconciliationLine):
    """One signed row of the GAAP-to-non-GAAP net income bridge.

    The stage is the row's place in the issuer's table: every row between the
    GAAP base and the disclosed total is a pre-tax adjustment except the last,
    the income tax effect on those adjustments.
    """

    stage: Literal["pre_tax_adjustment", "income_tax_effect_on_adjustments"]


class IssuerNonGaapNetIncomeReconciliationV1(_Contract):
    contract_version: Literal["issuer-non-gaap-reconciliation.v1"] = (
        "issuer-non-gaap-reconciliation.v1"
    )
    non_gaap_metric: Literal["non_gaap_net_income"] = "non_gaap_net_income"
    issuer_metric_label: str = Field(min_length=1, max_length=1000)
    gaap_metric: Literal["net_income"] = "net_income"
    currency_label: str = Field(min_length=1, max_length=40)
    value_scale: Literal["units", "thousands", "millions", "billions"]
    source: NonGaapDocumentSource
    period: NonGaapReconciliationPeriod
    definition: NonGaapTextEvidence
    footnotes: tuple[NonGaapTextEvidence, ...] = ()
    gaap_base: NonGaapReconciliationLine
    components: tuple[NonGaapNetIncomeComponent, ...]
    disclosed_non_gaap_net_income: NonGaapReconciliationLine
    recomputed_non_gaap_net_income: str = Field(pattern=_DECIMAL_PATTERN)
    non_gaap_net_income_residual: str = Field(pattern=_DECIMAL_PATTERN)
    arithmetic_closed: bool

    @model_validator(mode="after")
    def _validate_arithmetic_and_bindings(
        self,
    ) -> "IssuerNonGaapNetIncomeReconciliationV1":
        lines: tuple[NonGaapReconciliationLine, ...] = (
            self.gaap_base,
            *self.components,
            self.disclosed_non_gaap_net_income,
        )
        if any(
            line.locator.document_sha256 != self.source.decompressed_document_sha256
            for line in lines
        ):
            raise ValueError("every reconciliation line must bind the source document hash")
        table_identities = {
            (line.locator.table_index, line.locator.table_sha256) for line in lines
        }
        if len(table_identities) != 1:
            raise ValueError("every reconciliation line must bind one exact table")
        stages = tuple(row.stage for row in self.components)
        if len(stages) < 2 or stages != (
            *("pre_tax_adjustment",) * (len(stages) - 1),
            "income_tax_effect_on_adjustments",
        ):
            raise ValueError(
                "non-GAAP net income bridge requires pre-tax adjustments followed "
                "by one income tax effect on adjustments"
            )
        recomputed = Decimal(self.gaap_base.value) + sum(
            (Decimal(row.value) for row in self.components), Decimal(0)
        )
        residual = Decimal(self.disclosed_non_gaap_net_income.value) - recomputed
        if (
            self.recomputed_non_gaap_net_income,
            self.non_gaap_net_income_residual,
        ) != (_canonical_decimal(recomputed), _canonical_decimal(residual)):
            raise ValueError("published recomputation fields must match the disclosed bridge")
        if self.arithmetic_closed != (residual == 0):
            raise ValueError("arithmetic_closed must match the exact residual")
        required_footnotes = {key for line in lines for key in line.footnote_keys}
        published_footnotes = {row.footnote_key for row in self.footnotes}
        if None in published_footnotes:
            raise ValueError("footnote evidence requires a footnote_key")
        if required_footnotes - published_footnotes:
            raise ValueError("every referenced reconciliation footnote must be published")
        return self

    @property
    def income_tax_effect_on_adjustments(self) -> NonGaapNetIncomeComponent:
        """The bridge's final row, the tax effect on its pre-tax adjustments."""

        return self.components[-1]


#: One Edgar contract version, one bridge shape per declared ``non_gaap_metric``.
IssuerNonGaapReconciliation = Annotated[
    IssuerNonGaapReconciliationV1 | IssuerNonGaapNetIncomeReconciliationV1,
    Field(discriminator="non_gaap_metric"),
]
_RECONCILIATION_ADAPTER: TypeAdapter[
    IssuerNonGaapReconciliationV1 | IssuerNonGaapNetIncomeReconciliationV1
] = TypeAdapter(IssuerNonGaapReconciliation)
_Reconciliation = TypeVar(
    "_Reconciliation",
    bound=IssuerNonGaapReconciliationV1 | IssuerNonGaapNetIncomeReconciliationV1,
)


def canonical_issuer_non_gaap_reconciliation_bytes(
    value: IssuerNonGaapReconciliationV1
    | IssuerNonGaapNetIncomeReconciliationV1
    | Mapping[str, Any],
) -> bytes:
    if isinstance(
        value, (IssuerNonGaapReconciliationV1, IssuerNonGaapNetIncomeReconciliationV1)
    ):
        validated = value
    else:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        validated = _RECONCILIATION_ADAPTER.validate_json(encoded)
    return json.dumps(
        validated.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _strict_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value: {value}")


def decode_issuer_non_gaap_reconciliation_bytes(
    payload: bytes,
) -> IssuerNonGaapReconciliationV1 | IssuerNonGaapNetIncomeReconciliationV1:
    """Decode one canonical artifact into the shape its ``non_gaap_metric`` declares."""

    _require_strict_json_object(payload)
    # JSON-mode validation deliberately permits the producer's ISO date/time
    # strings and arrays while the strict decode still rejects duplicate
    # keys, non-finite values, and non-canonical bytes.
    return _canonical_only(payload, _RECONCILIATION_ADAPTER.validate_json(payload))


def _canonical_only(payload: bytes, validated: _Reconciliation) -> _Reconciliation:
    if canonical_issuer_non_gaap_reconciliation_bytes(validated) != payload:
        raise ValueError("reconciliation artifact is not exact canonical JSON")
    return validated


def _require_strict_json_object(payload: bytes) -> None:
    try:
        decoded = json.loads(
            payload,
            object_pairs_hook=_strict_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("reconciliation artifact is not strict JSON") from exc
    if not isinstance(decoded, dict):
        raise ValueError("reconciliation artifact must be a JSON object")


def issuer_non_gaap_reconciliation_sha256(
    value: IssuerNonGaapReconciliationV1
    | IssuerNonGaapNetIncomeReconciliationV1
    | Mapping[str, Any],
) -> str:
    return hashlib.sha256(canonical_issuer_non_gaap_reconciliation_bytes(value)).hexdigest()


__all__ = [
    "ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_CODEC",
    "ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_KIND",
    "IssuerNonGaapNetIncomeReconciliationV1",
    "IssuerNonGaapReconciliation",
    "IssuerNonGaapReconciliationV1",
    "NonGaapAcceptedAtEvidence",
    "NonGaapCellLocator",
    "NonGaapDocumentSource",
    "NonGaapNetIncomeComponent",
    "NonGaapReconciliationComponent",
    "NonGaapReconciliationLine",
    "NonGaapReconciliationPeriod",
    "NonGaapTextEvidence",
    "VALUE_SCALE_FACTORS",
    "is_usd_currency_label",
    "canonical_issuer_non_gaap_reconciliation_bytes",
    "decode_issuer_non_gaap_reconciliation_bytes",
    "issuer_non_gaap_reconciliation_sha256",
    "printed_decimal_places",
]
