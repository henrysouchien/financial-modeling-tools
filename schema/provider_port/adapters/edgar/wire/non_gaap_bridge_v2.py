"""Strict consumer for Edgar-owned issuer non-GAAP bridges, any metric.

``IssuerNonGaapBridgeV2`` mirrors ``edgar_parser.non_gaap_reconciliation``'s
DTO of the same name: one issuer table from a GAAP row to an issuer-defined
non-GAAP output through the components the issuer lists, each with an open
role and its printed sign. A v1 reconciliation artifact (Adjusted EBITDA or
non-GAAP net income) is one instance of this shape; ``bridge_from_v1``
decodes it so every consumer reads one contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
import re
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from industry_slice_core.thesis_shared_sources import SourceRecord

from .non_gaap_reconciliation_v1 import (
    ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_CODEC,
    ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_KIND,
    IssuerNonGaapNetIncomeReconciliationV1,
    IssuerNonGaapReconciliationV1,
    NonGaapDocumentSource,
    NonGaapReconciliationLine,
    NonGaapReconciliationPeriod,
    NonGaapTextEvidence,
    _canonical_decimal,
    _Contract,
    _DECIMAL_PATTERN,
    _require_strict_json_object,
    decode_issuer_non_gaap_reconciliation_bytes,
)
from .non_gaap_bridge_v3 import (
    ISSUER_NON_GAAP_BRIDGE_V3_ARTIFACT_CODEC,
    BridgeOperand,
    EvidencedBlank,
    IssuerNonGaapBridgeV3,
    NonGaapOutputMetricId,
    canonical_issuer_non_gaap_bridge_v3_bytes,
    decode_issuer_non_gaap_bridge_v3_bytes,
)
from .non_gaap_bridge_v3 import NonGaapBridgeComponent as NonGaapBridgeComponentV3


ISSUER_NON_GAAP_BRIDGE_ARTIFACT_KIND = "canonical_json_of_issuer_non_gaap_bridge"
ISSUER_NON_GAAP_BRIDGE_ARTIFACT_CODEC = "issuer-non-gaap-bridge.v2+canonical-json"
#: Corpus file kinds carrying an issuer bridge: capture writes the v2 kind;
#: packaged v1 artifacts stay readable through ``bridge_from_v1``.
ISSUER_NON_GAAP_BRIDGE_CORPUS_KIND = "edgar_issuer_non_gaap_bridge_v2"
ISSUER_NON_GAAP_RECONCILIATION_V1_CORPUS_KIND = "edgar_issuer_non_gaap_reconciliation_v1"
ISSUER_NON_GAAP_BRIDGE_CORPUS_KINDS = frozenset(
    {ISSUER_NON_GAAP_BRIDGE_CORPUS_KIND, ISSUER_NON_GAAP_RECONCILIATION_V1_CORPUS_KIND}
)
#: An open snake_case metric or role identifier (``metric_identifier``'s output).
NON_GAAP_METRIC_ID_PATTERN = r"^[a-z0-9]+(?:_[a-z0-9]+)*$"

NonGaapMetricFamily = Literal[
    "income", "per_share", "margin_pct", "expense", "cash_flow", "other"
]


class NonGaapBridgeComponent(NonGaapReconciliationLine):
    """One issuer row between a bridge's GAAP start and its non-GAAP output.

    ``role`` comes from the producer's open keyword vocabulary (``other`` when
    no keyword matches); the issuer's label is always kept. The value is kept
    as printed and contributes ``sign * value``.
    """

    role: str = Field(min_length=1, max_length=100, pattern=NON_GAAP_METRIC_ID_PATTERN)
    sign: Literal[1, -1]


def reconcile_bridge(
    *,
    gaap_base: NonGaapReconciliationLine,
    components: Sequence[NonGaapBridgeComponent],
    subtotals: Sequence[NonGaapReconciliationLine],
    disclosed_output: NonGaapReconciliationLine,
) -> tuple[Decimal, Decimal, tuple[Decimal, ...]]:
    """Recompute a bridge; each residual is reported minus recomputed.

    The output is the GAAP base plus every signed component; a subtotal
    checks the base plus the signed components printed above it.
    """

    base = Decimal(gaap_base.value)

    def running_total(before_row: int | None) -> Decimal:
        return base + sum(
            (
                component.sign * Decimal(component.value)
                for component in components
                if before_row is None or component.locator.row_index < before_row
            ),
            Decimal(0),
        )

    recomputed = running_total(None)
    return (
        recomputed,
        Decimal(disclosed_output.value) - recomputed,
        tuple(
            Decimal(subtotal.value) - running_total(subtotal.locator.row_index)
            for subtotal in subtotals
        ),
    )

BridgeClosureKind = Literal["exact", "rounding_consistent", "unexplained"]


@dataclass(frozen=True, slots=True)
class BridgeClosureState:
    """Zero-parameter half-ulp rounding-consistency closure state (INTEGRITY-Q6 / §2.9)."""

    state: BridgeClosureKind
    residual: str
    half_ulp_budget: str
    decimal_places: tuple[int | None, ...]
    subtotal_residuals: tuple[str, ...]
    subtotal_half_ulp_budgets: tuple[str, ...]


def compute_bridge_closure(
    *,
    gaap_base: NonGaapReconciliationLine,
    components: Sequence[NonGaapBridgeComponent],
    subtotals: Sequence[NonGaapReconciliationLine],
    disclosed_output: NonGaapReconciliationLine,
) -> BridgeClosureState:
    """Compute the zero-parameter half-ulp rounding-consistency state from verbatim cells."""
    _recomputed, residual, subtotal_residuals = reconcile_bridge(
        gaap_base=gaap_base,
        components=components,
        subtotals=subtotals,
        disclosed_output=disclosed_output,
    )
    output_budget = (
        gaap_base.half_ulp
        + sum((c.half_ulp for c in components), Decimal(0))
        + disclosed_output.half_ulp
    )
    subtotal_budgets = tuple(
        gaap_base.half_ulp
        + sum(
            (
                c.half_ulp
                for c in components
                if c.locator.row_index < subtotal.locator.row_index
            ),
            Decimal(0),
        )
        + subtotal.half_ulp
        for subtotal in subtotals
    )
    if residual == 0 and all(sr == 0 for sr in subtotal_residuals):
        state: BridgeClosureKind = "exact"
    elif abs(residual) <= output_budget and all(
        abs(sr) <= sb
        for sr, sb in zip(subtotal_residuals, subtotal_budgets, strict=True)
    ):
        state = "rounding_consistent"
    else:
        state = "unexplained"
    all_lines = (gaap_base, *components, *subtotals, disclosed_output)
    return BridgeClosureState(
        state=state,
        residual=_canonical_decimal(residual),
        half_ulp_budget=_canonical_decimal(output_budget),
        decimal_places=tuple(ln.decimal_places for ln in all_lines),
        subtotal_residuals=tuple(_canonical_decimal(sr) for sr in subtotal_residuals),
        subtotal_half_ulp_budgets=tuple(
            _canonical_decimal(sb) for sb in subtotal_budgets
        ),
    )

class IssuerNonGaapBridgeV2(_Contract):
    """One source-bound issuer bridge from a GAAP row to a non-GAAP output."""

    contract_version: Literal["issuer-non-gaap-bridge.v2"] = (
        "issuer-non-gaap-bridge.v2"
    )
    output_metric_id: str = Field(max_length=200, pattern=NON_GAAP_METRIC_ID_PATTERN)
    output_metric_label: str = Field(min_length=1, max_length=1000)
    gaap_metric_id: str = Field(max_length=200, pattern=NON_GAAP_METRIC_ID_PATTERN)
    gaap_metric_label: str = Field(min_length=1, max_length=1000)
    metric_family: NonGaapMetricFamily
    currency_label: str = Field(min_length=1, max_length=40)
    value_scale: Literal["units", "thousands", "millions", "billions"]
    source: NonGaapDocumentSource
    period: NonGaapReconciliationPeriod
    definition: NonGaapTextEvidence | None = None
    footnotes: tuple[NonGaapTextEvidence, ...] = ()
    gaap_base: NonGaapReconciliationLine
    components: tuple[NonGaapBridgeComponent, ...] = Field(min_length=1)
    subtotals: tuple[NonGaapReconciliationLine, ...] = ()
    disclosed_output: NonGaapReconciliationLine
    recomputed_output: str = Field(pattern=_DECIMAL_PATTERN)
    output_residual: str = Field(pattern=_DECIMAL_PATTERN)
    subtotal_residuals: tuple[Annotated[str, Field(pattern=_DECIMAL_PATTERN)], ...] = ()
    arithmetic_closed: bool

    @model_validator(mode="after")
    def _validate_arithmetic_and_bindings(self) -> "IssuerNonGaapBridgeV2":
        lines: tuple[NonGaapReconciliationLine, ...] = (
            self.gaap_base,
            *self.components,
            *self.subtotals,
            self.disclosed_output,
        )
        if any(
            line.locator.document_sha256 != self.source.decompressed_document_sha256
            for line in lines
        ):
            raise ValueError("every bridge line must bind the source document hash")
        table_identities = {
            (line.locator.table_index, line.locator.table_sha256) for line in lines
        }
        if len(table_identities) != 1:
            raise ValueError("every bridge line must bind one exact table")
        published_footnotes = {footnote.footnote_key for footnote in self.footnotes}
        if None in published_footnotes:
            raise ValueError("footnote evidence requires a footnote_key")
        if {key for line in lines for key in line.footnote_keys} - published_footnotes:
            raise ValueError("every referenced bridge footnote must be published")
        recomputed, residual, subtotal_residuals = reconcile_bridge(
            gaap_base=self.gaap_base,
            components=self.components,
            subtotals=self.subtotals,
            disclosed_output=self.disclosed_output,
        )
        if (
            self.recomputed_output,
            self.output_residual,
            self.subtotal_residuals,
        ) != (
            _canonical_decimal(recomputed),
            _canonical_decimal(residual),
            tuple(_canonical_decimal(value) for value in subtotal_residuals),
        ):
            raise ValueError(
                "published recomputation fields must match the disclosed bridge"
            )
        if self.arithmetic_closed != (
            residual == 0 and all(value == 0 for value in subtotal_residuals)
        ):
            raise ValueError("arithmetic_closed must match every exact residual")
        return self

    @property
    def closure(self) -> BridgeClosureState:
        return compute_bridge_closure(
            gaap_base=self.gaap_base,
            components=self.components,
            subtotals=self.subtotals,
            disclosed_output=self.disclosed_output,
        )

    @property
    def closure_state(self) -> BridgeClosureKind:
        return self.closure.state

    @property
    def half_ulp_budget(self) -> str:
        return self.closure.half_ulp_budget

    @property
    def subtotal_half_ulp_budgets(self) -> tuple[str, ...]:
        return self.closure.subtotal_half_ulp_budgets


def canonical_issuer_non_gaap_bridge_bytes(
    value: IssuerNonGaapBridgeV2 | Mapping[str, Any],
) -> bytes:
    validated = (
        value
        if isinstance(value, IssuerNonGaapBridgeV2)
        else IssuerNonGaapBridgeV2.model_validate_json(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    )
    return json.dumps(
        validated.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


SUPPORTED_BRIDGE_CONTRACT_VERSIONS: tuple[str, ...] = (
    "issuer-non-gaap-reconciliation.v1",
    "issuer-non-gaap-bridge.v2",
    "issuer-non-gaap-bridge.v3",
)


def validate_bridge_contract_version(
    version: object,
    *,
    allowed: Sequence[str] = SUPPORTED_BRIDGE_CONTRACT_VERSIONS,
) -> str:
    if not isinstance(version, str) or version not in allowed:
        raise ValueError(
            f"version_refusal: unsupported non-GAAP bridge contract_version "
            f"(expected one of {list(allowed)}, received {version!r})"
        )
    return version


def decode_issuer_non_gaap_bridge_v2_bytes(payload: bytes) -> IssuerNonGaapBridgeV2:
    """Decode one canonical v2 bridge artifact."""

    _require_strict_json_object(payload)
    parsed = json.loads(payload)
    validate_bridge_contract_version(
        parsed.get("contract_version"), allowed=("issuer-non-gaap-bridge.v2",)
    )
    bridge = IssuerNonGaapBridgeV2.model_validate_json(payload)
    if canonical_issuer_non_gaap_bridge_bytes(bridge) != payload:
        raise ValueError("bridge artifact is not exact canonical JSON")
    return bridge


def decode_issuer_non_gaap_bridge_bytes(payload: bytes) -> IssuerNonGaapBridgeV2:
    """Decode a canonical v2 bridge or v1 reconciliation artifact as a v2 bridge."""

    _require_strict_json_object(payload)
    parsed = json.loads(payload)
    version = validate_bridge_contract_version(
        parsed.get("contract_version"),
        allowed=("issuer-non-gaap-reconciliation.v1", "issuer-non-gaap-bridge.v2"),
    )
    if version == "issuer-non-gaap-bridge.v2":
        return decode_issuer_non_gaap_bridge_v2_bytes(payload)
    return bridge_from_v1(decode_issuer_non_gaap_reconciliation_bytes(payload))


def decode_issuer_non_gaap_bridge_any_bytes(
    payload: bytes,
) -> IssuerNonGaapBridgeV2 | IssuerNonGaapBridgeV3:
    """Decode a canonical v1/v2 bridge or a v3 bridge, keeping v3 typed as v3."""

    _require_strict_json_object(payload)
    parsed = json.loads(payload)
    version = validate_bridge_contract_version(
        parsed.get("contract_version"), allowed=SUPPORTED_BRIDGE_CONTRACT_VERSIONS
    )
    if version == "issuer-non-gaap-bridge.v3":
        return decode_issuer_non_gaap_bridge_v3_bytes(payload)
    return decode_issuer_non_gaap_bridge_bytes(payload)

# ``edgar_parser.non_gaap_reconciliation._COMPONENT_ROLES``, mirrored so a v1
# row decodes to the role the producer's v2 walker assigns the same label.
# First match wins; an unmatched label is ``other`` and keeps its label.
_COMPONENT_ROLES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "stock_based_compensation",
        re.compile(
            r"\b(?:stock|share|equity) based compensation\b"
            r"|\b(?:stock|equity) compensation\b"
            r"|\bpayroll tax(?:es)?\b.*\b(?:stock|equity|share)\b"
        ),
    ),
    (
        "acquired_intangible_amortization",
        re.compile(r"\bamortization\b.*\b(?:acquired|purchased)\b.*\bintangible"),
    ),
    (
        "acquisition_related",
        re.compile(
            r"\b(?:acquisitions?|mergers?|transactions?|dispositions?|divestitures?)\b"
        ),
    ),
    ("restructuring", re.compile(r"\brestructuring\b")),
    ("impairment", re.compile(r"\bimpairments?\b|\bwrite (?:downs?|offs?)\b")),
    ("litigation", re.compile(r"\blitigation\b|\blegal settlements?\b")),
    (
        "depreciation_and_amortization",
        re.compile(
            r"\bdepreciation\b|\bd a\b"
            r"|^(?!.*\b(?:debt|issuance|acquired|purchased)\b).*\bamortization\b"
        ),
    ),
    ("interest", re.compile(r"\binterest\b")),
    ("income_tax", re.compile(r"\btax(?:es)?\b")),
    (
        "investment_gain_loss",
        re.compile(
            r"\b(?:gains?|loss(?:es)?)\b.*\binvestments?\b"
            r"|\binvestments?\b.*\b(?:gains?|loss(?:es)?)\b|\bfair value\b"
        ),
    ),
    ("fx", re.compile(r"\bforeign (?:exchange|currency)\b|\bcurrency\b|\bfx\b")),
    (
        "capex",
        re.compile(
            r"\bpurchases? of property\b|\bcapital expenditures?\b"
            r"|\bcapitali[sz](?:ed|ation of)\b.*\bsoftware\b"
        ),
    ),
)
_FOOTNOTE_MARKER_RE = re.compile(r"\(\s*[1-9]\d?\s*\)|\*+")
_LINE_BREAK_HYPHEN_RE = re.compile(r"(?<=[a-z])-\s+(?=[a-z])")
_SIGN_QUALIFIER_RE = re.compile(
    r"\(\s*(?:loss|losses|income|expense|expenses|benefit|gain|deficit)\s*\)"
)
_SCALE_WORDS_RE = re.compile(
    r"\(\s*(?:\$|usd)?\s*in\s+(?:thousands|millions|billions)[^)]*\)"
    r"|\b(?:in\s+)?(?:thousands|millions|billions)\b|\$|\busd\b|\bdollars\b"
)


def _normalized_label(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _component_role(issuer_label: str) -> str:
    label = _normalized_label(issuer_label)
    for role, pattern in _COMPONENT_ROLES:
        if pattern.search(label):
            return role
    return "other"


def metric_identifier(label: str) -> str:
    """The producer's open snake_case identifier of an issuer label."""

    text = _LINE_BREAK_HYPHEN_RE.sub("", label.lower())
    for pattern in (_FOOTNOTE_MARKER_RE, _SIGN_QUALIFIER_RE, _SCALE_WORDS_RE):
        text = pattern.sub(" ", text)
    identifier = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return re.sub(r"^gaap_", "", identifier)


def _bridge_component(
    row: NonGaapReconciliationLine, *, role: str
) -> NonGaapBridgeComponent:
    return NonGaapBridgeComponent(
        issuer_label=row.issuer_label,
        value=row.value,
        value_raw=row.value_raw,
        locator=row.locator,
        footnote_keys=row.footnote_keys,
        role=role,
        sign=1,
    )


def bridge_from_v1(
    reconciliation: IssuerNonGaapReconciliationV1
    | IssuerNonGaapNetIncomeReconciliationV1,
) -> IssuerNonGaapBridgeV2:
    """The v2 bridge a v1 reconciliation is one instance of.

    Every v1 row takes the producer's keyword role from its issuer label, as
    the v2 walker assigns it. Adjusted EBITDA runs from net income through
    one EBITDA subtotal. Non-GAAP net income's final row is the
    ``income_tax`` effect on adjustments. v1 rows are additive (sign 1).
    """

    if isinstance(reconciliation, IssuerNonGaapNetIncomeReconciliationV1):
        components = tuple(
            _bridge_component(
                row,
                role=(
                    "income_tax"
                    if row.stage == "income_tax_effect_on_adjustments"
                    else _component_role(row.issuer_label)
                ),
            )
            for row in reconciliation.components
        )
        subtotals: tuple[NonGaapReconciliationLine, ...] = ()
        subtotal_residuals: tuple[str, ...] = ()
        disclosed_output = reconciliation.disclosed_non_gaap_net_income
        recomputed_output = reconciliation.recomputed_non_gaap_net_income
        output_residual = reconciliation.non_gaap_net_income_residual
    else:
        components = tuple(
            _bridge_component(row, role=_component_role(row.issuer_label))
            for row in reconciliation.components
        )
        subtotals = (reconciliation.ebitda_subtotal,)
        subtotal_residuals = (reconciliation.ebitda_residual,)
        disclosed_output = reconciliation.disclosed_adjusted_ebitda
        recomputed_output = reconciliation.recomputed_adjusted_ebitda
        output_residual = reconciliation.adjusted_ebitda_residual
    return IssuerNonGaapBridgeV2(
        output_metric_id=reconciliation.non_gaap_metric,
        output_metric_label=reconciliation.issuer_metric_label,
        gaap_metric_id=reconciliation.gaap_metric,
        gaap_metric_label=reconciliation.gaap_base.issuer_label,
        metric_family="income",
        currency_label=reconciliation.currency_label,
        value_scale=reconciliation.value_scale,
        source=reconciliation.source,
        period=reconciliation.period,
        definition=reconciliation.definition,
        footnotes=reconciliation.footnotes,
        gaap_base=reconciliation.gaap_base,
        components=components,
        subtotals=subtotals,
        disclosed_output=disclosed_output,
        recomputed_output=recomputed_output,
        output_residual=output_residual,
        subtotal_residuals=subtotal_residuals,
        arithmetic_closed=reconciliation.arithmetic_closed,
    )


# The historical-rail series role of each bridge row: the output is its
# ``output_metric_id``, the base its ``gaap_metric_id``, a component
# ``<output>__<role>`` and a subtotal ``<output>__subtotal_<label id>``, with
# ``__<n>`` on the n-th repeat of one role.
_OCCURRENCE_SUFFIX_RE = re.compile(r"__([2-9]|[1-9]\d+)$")


def _row_series_role(output_metric_id: str, role: str, occurrence: int) -> str:
    series_role = f"{output_metric_id}__{role}"
    return series_role if occurrence == 1 else f"{series_role}__{occurrence}"


def bridge_rows(
    bridge: IssuerNonGaapBridgeV2,
) -> tuple[tuple[str, str | None, NonGaapReconciliationLine], ...]:
    """Every bridge row in table order as ``(series_role, role, line)``.

    ``role`` is the component role, ``subtotal_<label id>`` for a subtotal,
    and ``None`` for the base and the output.
    """

    seen: dict[str, int] = {}
    body: list[tuple[str, str | None, NonGaapReconciliationLine]] = []
    keyed: list[tuple[str, NonGaapReconciliationLine]] = [
        *((component.role, component) for component in bridge.components),
        *(
            (f"subtotal_{metric_identifier(line.issuer_label)}", line)
            for line in bridge.subtotals
        ),
    ]
    for role, line in sorted(keyed, key=lambda item: item[1].locator.row_index):
        seen[role] = seen.get(role, 0) + 1
        body.append(
            (_row_series_role(bridge.output_metric_id, role, seen[role]), role, line)
        )
    has_non_ebitda_subtotal = any(
        metric_identifier(line.issuer_label)
        not in {"ebitda", "ebita", "ebit", "income_from_operations", "operating_income"}
        for line in bridge.subtotals
    )
    is_scoped_base = has_non_ebitda_subtotal or bridge.output_metric_id in {
        "ebitda",
        "ebita",
        "ebit",
    }
    base_series_role = (
        f"{bridge.output_metric_id}_base_{bridge.gaap_metric_id}"
        if is_scoped_base
        else bridge.gaap_metric_id
    )
    return (
        (base_series_role, None, bridge.gaap_base),
        *body,
        (bridge.output_metric_id, None, bridge.disclosed_output),
    )


def v3_output_metric_id(bridge: IssuerNonGaapBridgeV3) -> str:
    if bridge.canonical_output_metric is NonGaapOutputMetricId.OTHER:
        return bridge.output_metric_slug
    return bridge.canonical_output_metric.value


def v3_gaap_metric_id(bridge: IssuerNonGaapBridgeV3) -> str:
    return metric_identifier(bridge.gaap_metric_label)


_V3_TO_V2_ROLE: dict[str, str] = {
    "interest_expense": "interest",
    "interest_income": "interest",
    "income_tax_effect": "income_tax",
    "pre_tax_adjustments_total": "other",
}


def bridge_rows_v3(
    bridge: IssuerNonGaapBridgeV3,
) -> tuple[tuple[str, str | None, BridgeOperand | NonGaapBridgeComponentV3], ...]:
    """Every numeric v3 bridge row in table order as ``(series_role, role, line)``.

    ``role`` is the component role, ``subtotal_<label id>`` for a subtotal,
    and ``None`` for the base and the output. Evidenced blanks carry no row;
    a blank base or output leaves the bridge without numeric rows.
    """

    if isinstance(bridge.gaap_base.value, EvidencedBlank) or isinstance(
        bridge.disclosed_output.value, EvidencedBlank
    ):
        return ()

    output_metric_id = v3_output_metric_id(bridge)
    gaap_metric_id = v3_gaap_metric_id(bridge)
    seen: dict[str, int] = {}
    body: list[tuple[str, str | None, BridgeOperand | NonGaapBridgeComponentV3]] = []
    keyed: list[tuple[str, BridgeOperand | NonGaapBridgeComponentV3]] = []
    for component in bridge.components:
        if not isinstance(component.value, EvidencedBlank):
            raw_role = component.role.value
            comp_role = _V3_TO_V2_ROLE.get(raw_role, raw_role)
            keyed.append((comp_role, component))
    keyed.extend(
        (f"subtotal_{metric_identifier(line.issuer_label)}", line)
        for line in bridge.subtotals
        if not isinstance(line.value, EvidencedBlank)
    )
    for role, line in sorted(keyed, key=lambda item: item[1].cell.row_index):
        seen[role] = seen.get(role, 0) + 1
        body.append(
            (_row_series_role(output_metric_id, role, seen[role]), role, line)
        )
    has_non_ebitda_subtotal = any(
        metric_identifier(line.issuer_label)
        not in {"ebitda", "ebita", "ebit", "income_from_operations", "operating_income"}
        for line in bridge.subtotals
        if not isinstance(line.value, EvidencedBlank)
    )
    is_scoped_base = has_non_ebitda_subtotal or output_metric_id in {
        "ebitda",
        "ebita",
        "ebit",
    }
    base_series_role = (
        f"{output_metric_id}_base_{gaap_metric_id}"
        if is_scoped_base
        else gaap_metric_id
    )
    return (
        (base_series_role, None, bridge.gaap_base),
        *body,
        (output_metric_id, None, bridge.disclosed_output),
    )


def bridge_row_key(series_role: str) -> tuple[str, str] | None:
    """``(output_metric_id, role)`` of a component or subtotal series role.

    ``None`` for a base or output series role, which carries no role.
    """

    stem = _OCCURRENCE_SUFFIX_RE.sub("", series_role)
    parts = stem.split("__")
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def validate_edgar_non_gaap_bridge_source_record(
    source: SourceRecord,
) -> tuple[IssuerNonGaapBridgeV2 | IssuerNonGaapBridgeV3, str, tuple[str, str]]:
    """Rebuild one derived bridge source (v2, or a v1 reconciliation) and its
    raw-parent binding."""

    if (
        source.provider != "edgar"
        or source.endpoint_or_filing_id != "issuer_non_gaap_reconciliation"
    ):
        raise ValueError("non-GAAP reconciliation source provider endpoint is invalid")
    key_fields = source.key_fields or {}
    codec = (
        key_fields.get("artifact_kind"),
        key_fields.get("artifact_codec"),
        key_fields.get("contract_version"),
    )
    if codec not in {
        (
            ISSUER_NON_GAAP_BRIDGE_ARTIFACT_KIND,
            ISSUER_NON_GAAP_BRIDGE_ARTIFACT_CODEC,
            "issuer-non-gaap-bridge.v2",
        ),
        (
            ISSUER_NON_GAAP_BRIDGE_ARTIFACT_KIND,
            ISSUER_NON_GAAP_BRIDGE_V3_ARTIFACT_CODEC,
            "issuer-non-gaap-bridge.v3",
        ),
        (
            ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_KIND,
            ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_CODEC,
            "issuer-non-gaap-reconciliation.v1",
        ),
    }:
        raise ValueError("non-GAAP reconciliation artifact codec is invalid")
    locator = key_fields.get("logical_locator")
    if not isinstance(locator, str) or not locator.strip():
        raise ValueError("non-GAAP reconciliation source requires logical_locator")
    payload = source.text.encode("utf-8")
    if codec[2] == "issuer-non-gaap-bridge.v3":
        bridge: IssuerNonGaapBridgeV2 | IssuerNonGaapBridgeV3 = (
            decode_issuer_non_gaap_bridge_v3_bytes(payload)
        )
    elif codec[0] == ISSUER_NON_GAAP_BRIDGE_ARTIFACT_KIND:
        bridge = decode_issuer_non_gaap_bridge_v2_bytes(payload)
    else:
        bridge = bridge_from_v1(decode_issuer_non_gaap_reconciliation_bytes(payload))
    artifact_sha256 = hashlib.sha256(payload).hexdigest()
    if key_fields.get("artifact_sha256") != artifact_sha256:
        raise ValueError("non-GAAP reconciliation artifact SHA is invalid")
    if (
        key_fields.get("ticker"),
        key_fields.get("accession"),
        key_fields.get("form"),
        key_fields.get("period_end"),
    ) != (
        bridge.source.ticker,
        bridge.source.accession,
        bridge.source.form,
        bridge.period.period_end.isoformat(),
    ):
        raise ValueError("non-GAAP reconciliation source identity is invalid")
    filing_parent = key_fields.get("filing_parent_source_id")
    accepted_parent = key_fields.get("accepted_at_parent_source_id")
    if not isinstance(filing_parent, str) or not isinstance(accepted_parent, str):
        raise ValueError("non-GAAP reconciliation requires exact raw parent source IDs")
    if filing_parent == accepted_parent:
        raise ValueError("non-GAAP reconciliation raw parent source IDs must differ")
    if (
        key_fields.get("filing_parent_artifact_sha256")
        != bridge.source.document_sha256
        or key_fields.get("accepted_at_parent_artifact_sha256")
        != bridge.source.accepted_at_evidence.document_sha256
        or key_fields.get("accepted_at_result_key")
        != bridge.source.accepted_at_evidence.result_key
    ):
        raise ValueError("non-GAAP reconciliation raw parent hash binding is invalid")
    return bridge, locator, (filing_parent, accepted_parent)


__all__ = [
    "BridgeClosureKind",
    "BridgeClosureState",
    "ISSUER_NON_GAAP_BRIDGE_ARTIFACT_CODEC",
    "ISSUER_NON_GAAP_BRIDGE_ARTIFACT_KIND",
    "ISSUER_NON_GAAP_BRIDGE_CORPUS_KIND",
    "ISSUER_NON_GAAP_BRIDGE_CORPUS_KINDS",
    "ISSUER_NON_GAAP_BRIDGE_V3_ARTIFACT_CODEC",
    "ISSUER_NON_GAAP_RECONCILIATION_V1_CORPUS_KIND",
    "IssuerNonGaapBridgeV2",
    "IssuerNonGaapBridgeV3",
    "NON_GAAP_METRIC_ID_PATTERN",
    "NonGaapBridgeComponent",
    "NonGaapMetricFamily",
    "SUPPORTED_BRIDGE_CONTRACT_VERSIONS",
    "bridge_from_v1",
    "bridge_row_key",
    "bridge_rows",
    "bridge_rows_v3",
    "canonical_issuer_non_gaap_bridge_bytes",
    "canonical_issuer_non_gaap_bridge_v3_bytes",
    "compute_bridge_closure",
    "decode_issuer_non_gaap_bridge_any_bytes",
    "decode_issuer_non_gaap_bridge_bytes",
    "decode_issuer_non_gaap_bridge_v2_bytes",
    "decode_issuer_non_gaap_bridge_v3_bytes",
    "metric_identifier",
    "reconcile_bridge",
    "v3_gaap_metric_id",
    "v3_output_metric_id",
    "validate_bridge_contract_version",
    "validate_edgar_non_gaap_bridge_source_record",
]
