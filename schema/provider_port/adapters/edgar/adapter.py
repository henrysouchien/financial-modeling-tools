"""Edgar STATEMENT_FACTS adapter over sealed filing-presentation occurrences.

Native numbers are projected into the producer-declared presentation scale
without losing decimal precision.  The one presentation-sign projection happens
here: when the filing's preferred-label role declares ``negates_value``, the
emitted :class:`Quantity` negates the native number so population sees the sign
printed in the statement.  Both the native sign and the directive remain in
observation detail and the occurrence locator.

A cash-flow statement presents beginning and ending cash with the same tag,
role, and printed fiscal-year column identity. ``FilingLineRef`` intentionally
addresses only the ending balance: on that collision the adapter chooses the
instant equal to ``printed_column.period_end``. The beginning balance is the
prior column's ending balance and is not separately addressable by this identity.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
import json
import logging
from types import MappingProxyType
from typing import Literal

from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import (
    AmbiguousBindingV1,
    BoundBindingV1,
    EdgarFilingFactsContractError,
    EdgarFilingFactsEnvelopeV1,
    ExplicitDimensionV1,
    FilingPresentationCellV1,
    GapBindingV1,
    NumericQuantityV1,
    PresentationDisplayKnownV1,
    validate_edgar_filing_facts_payload,
)
from schema.edgar_metric_series_v1 import validated_metric_series_entry
from schema.provider_port.adapters.edgar.line_bindings import (
    EdgarLineBinding,
    load_edgar_line_bindings,
)
from industry_slice_core.profile_normalization import ProviderCapability
from schema.provider_port.contracts import (
    FilingDimensionRef,
    FilingLineFact,
    FilingLineObservation,
    FilingLineRef,
    FiscalWindow,
    ReportedPeriod,
    ReportedPeriodType,
    VendorProvenance,
    mint_source_fact_ref,
)
from schema.provider_port.registry import EDGAR_PROVIDER_ID
from schema.source_values import SourceValue, source_value_from_edgar_quantity
from schema.statement_topology import normalize_topology_role
from value_semantics_core import (
    CONVENTIONS_IR_V1,
    FiscalYearPeriod,
    NumericDisplaySpec,
    Quantity,
    normalize_decimal_v1,
)


EdgarOccurrenceArtifacts = Mapping[
    str, Mapping[str, object] | EdgarFilingFactsEnvelopeV1
]
_UnresolvedDimensions = Literal[
    "typed_dimension_unrepresentable", "ambiguous_dimensions",
]
_OccurrenceIdentity = tuple[
    str, str, str, str, tuple[FilingDimensionRef, ...] | _UnresolvedDimensions
]

_FACE_ROLES = frozenset(
    {
        "balance_sheet",
        "income_statement",
        "comprehensive_income",
        "equity",
        "cash_flow",
    }
)
_REPORTED_QUARTERS: tuple[ReportedPeriodType, ...] = ("Q1", "Q2", "Q3", "Q4")

_SCALE_FACTORS = {
    row.numeric_scale: Decimal(row.multiplier)
    for row in CONVENTIONS_IR_V1.scale_factors
}


class EdgarAdapter:
    """Expose replayable filing-native facts from accession-keyed artifacts."""

    provider_name = EDGAR_PROVIDER_ID

    def __init__(
        self,
        *,
        occurrence_artifacts: EdgarOccurrenceArtifacts | None = None,
        line_bindings: Sequence[EdgarLineBinding] | None = None,
    ) -> None:
        self._line_bindings = tuple(
            line_bindings
            if line_bindings is not None
            else load_edgar_line_bindings()
        )
        self._envelopes: dict[str, EdgarFilingFactsEnvelopeV1] = {}
        for accession, payload in (occurrence_artifacts or {}).items():
            if isinstance(payload, EdgarFilingFactsEnvelopeV1):
                if payload.filing.accession != accession:
                    raise EdgarFilingFactsContractError(
                        "filing_facts.v1 response accession does not match its artifact key"
                    )
                envelope = payload
            else:
                envelope = validate_edgar_filing_facts_payload(
                    payload,
                    expected_accession=accession,
                )
            self._envelopes[accession] = envelope
        self._filing_accepted_at_by_accession = MappingProxyType({
            accession: (
                datetime.fromisoformat(envelope.filing.accepted_at.replace("Z", "+00:00"))
                if envelope.filing.accepted_at is not None
                else None
            )
            for accession, envelope in self._envelopes.items()
        })
        self._occurrences = _index_occurrences(tuple(self._envelopes.values()))

    @property
    def filing_accepted_at_by_accession(self) -> Mapping[str, datetime | None]:
        """Acceptance of each decoded filing, including explicitly undated filings."""
        return self._filing_accepted_at_by_accession

    def advertised_capabilities(self) -> frozenset[ProviderCapability]:
        return frozenset({ProviderCapability.STATEMENT_FACTS})

    def advertised_filing_line_bindings_by_capability(
        self,
    ) -> dict[ProviderCapability, frozenset[tuple[str | None, str]]]:
        return {
            ProviderCapability.STATEMENT_FACTS: frozenset(
                (binding.statement_role, binding.tag)
                for binding in self._line_bindings
            )
        }

    def fetch_filing_line_facts(
        self,
        ticker: str,
        *,
        lines: Sequence[FilingLineRef],
        window: FiscalWindow,
        cited_arcs: Mapping[FilingLineRef, str] | None = None,
    ) -> tuple[tuple[FilingLineObservation, ...], tuple[ReportedPeriod, ...]]:
        """Each requested line's observation and the window's reported periods.

        ``cited_arcs`` is line → the presentation arc (``source.cell_locator``)
        the binding row that pinned the line cites in that filing. It rides
        beside the line, never inside its identity: a tag printed on several
        arcs of one role yields the cited arc's fact; a line with no cited arc,
        or whose cited arc is not among its occurrences, keeps every candidate.
        """
        arcs = cited_arcs or {}
        requested = tuple(
            sorted(
                set(lines),
                key=lambda line: (
                    line.tag,
                    line.period.canonical_label(),
                    line.statement_role,
                    line.accession,
                    tuple(
                        (dimension.axis_qname, dimension.member_qname)
                        for dimension in line.dimensions
                    ),
                ),
            )
        )
        start_year = window.end_fiscal_year - window.n_years + 1
        observations: list[FilingLineObservation] = []
        for line in requested:
            identity = _line_identity(line)
            candidates = self._occurrences.get(identity, ())
            if not candidates:
                observations.append(
                    FilingLineObservation(
                        line=line,
                        status="missing",
                        fact=None,
                        provider="edgar",
                        detail=next(
                            (
                                reason for reason in (
                                    "ambiguous_dimensions",
                                    "typed_dimension_unrepresentable",
                                )
                                if (*identity[:4], reason) in self._occurrences
                            ),
                            "occurrence_not_present",
                        ),
                    )
                )
                continue
            fiscal_years = {
                occurrence.printed_column.period_ref.fiscal_year
                for occurrence in candidates
            }
            if any(
                not start_year <= fiscal_year <= window.end_fiscal_year
                for fiscal_year in fiscal_years
            ):
                raise ValueError(
                    f"requested filing line is outside FY{start_year}-"
                    f"FY{window.end_fiscal_year}"
                )
            observations.append(
                _observation_for_candidates(line, candidates, cited_arc=arcs.get(line))
            )
        periods = _reported_periods_from_columns(
            tuple(self._envelopes.values()),
            start_year=start_year,
            filing_accepted_at_by_accession=self.filing_accepted_at_by_accession,
            end_year=window.end_fiscal_year,
        )
        return tuple(observations), periods


def _index_occurrences(
    envelopes: Sequence[EdgarFilingFactsEnvelopeV1],
) -> dict[
    _OccurrenceIdentity,
    tuple[FilingPresentationCellV1, ...],
]:
    indexed: dict[
        _OccurrenceIdentity,
        list[FilingPresentationCellV1],
    ] = {}
    for envelope in envelopes:
        for occurrence in envelope.occurrences:
            if occurrence.line_kind == "abstract":
                continue
            dimensions = _occurrence_dimensions(occurrence)
            key = (
                occurrence.accession,
                _filing_line_period(occurrence).canonical_label(),
                normalize_topology_role(
                    occurrence.presentation.presentation_role_name
                ),
                occurrence.line_qname.lexical_qname,
                dimensions,
            )
            indexed.setdefault(key, []).append(occurrence)
    return {
        key: tuple(
            sorted(
                values,
                key=lambda row: (
                    row.source.table_locator,
                    row.source.row_locator,
                    row.source.cell_locator,
                ),
            )
        )
        for key, values in indexed.items()
    }



def _line_identity(line: FilingLineRef) -> _OccurrenceIdentity:
    return (
        line.accession,
        line.period.canonical_label(),
        line.statement_role,
        line.tag,
        line.dimensions,
    )




def _occurrence_dimensions(
    occurrence: FilingPresentationCellV1,
) -> tuple[FilingDimensionRef, ...] | _UnresolvedDimensions:
    binding = occurrence.binding
    if isinstance(binding, BoundBindingV1):
        facts = (binding.fact,)
    elif isinstance(binding, AmbiguousBindingV1):
        facts = binding.candidates
    else:
        return ()
    coordinates: set[tuple[FilingDimensionRef, ...]] = set()
    for fact in facts:
        dimensions: set[FilingDimensionRef] = set()
        for dimension in fact.dimensions:
            if not isinstance(dimension, ExplicitDimensionV1):
                return "typed_dimension_unrepresentable"
            dimensions.add(
                FilingDimensionRef(
                    axis_qname=dimension.axis.lexical_qname,
                    member_qname=dimension.member.lexical_qname,
                )
            )
        coordinates.add(tuple(
            sorted(
                dimensions,
                key=lambda dimension: (
                    dimension.axis_qname, dimension.member_qname
                ),
            )
        ))
    # Conflicting candidates remain indexed, but cannot impersonate a coordinate.
    if len(coordinates) != 1:
        return "ambiguous_dimensions"
    return next(iter(coordinates))


def _dimension_key(occurrence: FilingPresentationCellV1) -> str:
    binding = occurrence.binding
    if isinstance(binding, BoundBindingV1):
        dimensions = [
            dimension.model_dump(mode="json")
            for dimension in binding.fact.dimensions
        ]
    elif isinstance(binding, AmbiguousBindingV1):
        dimensions = [
            [
                dimension.model_dump(mode="json")
                for dimension in candidate.dimensions
            ]
            for candidate in binding.candidates
        ]
    else:
        dimensions = []
    return json.dumps(
        dimensions,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _filing_line_period(occurrence: FilingPresentationCellV1) -> FiscalYearPeriod:
    """Project a printed annual column onto the annual requirement identity."""

    return FiscalYearPeriod(
        kind="fiscal_year",
        fiscal_year=occurrence.printed_column.period_ref.fiscal_year,
    )


def _minted_line(occurrence: FilingPresentationCellV1) -> FilingLineRef:
    dimensions = _occurrence_dimensions(occurrence)
    assert isinstance(dimensions, tuple)
    return FilingLineRef(
        accession=occurrence.accession,
        period=_filing_line_period(occurrence),
        statement_role=normalize_topology_role(
            occurrence.presentation.presentation_role_name
        ),
        tag=occurrence.line_qname.lexical_qname,
        dimensions=dimensions,
    )


def _ending_balance_candidates(
    candidates: Sequence[FilingPresentationCellV1],
) -> Sequence[FilingPresentationCellV1]:
    """Disambiguate beginning/end instant rows sharing one printed FY identity."""

    if len(candidates) <= 1:
        return candidates
    if not all(
        isinstance(occurrence.binding, BoundBindingV1)
        and occurrence.binding.fact.period.kind == "instant"
        for occurrence in candidates
    ):
        return candidates
    ending = tuple(
        occurrence
        for occurrence in candidates
        if isinstance(occurrence.binding, BoundBindingV1)
        and getattr(occurrence.binding.fact.period, "instant", None)
        == occurrence.printed_column.period_end
    )
    return ending if len(ending) == 1 else candidates


def _rendering_identity(occurrence: FilingPresentationCellV1) -> str:
    """Project an occurrence onto everything except where the arc sits.

    A filing may hang one bound fact off two presentation arcs of the same
    link — one filing prints "Intangible assets, net" in the intangibles
    roll-forward and again above the future amortization schedule. Those are
    renderings of one fact: they differ only in the arc that locates them, in
    the tree position that arc gives them, and in the producer's
    ``occurrence_id``, which content-addresses the printing arc (EDGAR-OCC-ID).
    Everything else the emitted fact is built from stays in the projection, so
    candidates that disagree about value, label, display, printed lexical or
    binding remain rivals the adapter must not choose between.
    """

    payload = occurrence.model_dump(mode="json")
    payload.pop("tree")
    payload.pop("occurrence_id")
    for locator in ("table_locator", "row_locator", "cell_locator"):
        payload["source"].pop(locator)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _cited_candidates(
    candidates: Sequence[FilingPresentationCellV1],
    cited_arc: str | None,
) -> Sequence[FilingPresentationCellV1]:
    """The cited arc's occurrences first, then other renderings of that fact.

    A binding row cites one printed arc. When the tag prints on several arcs
    of the role (an end-of-period row and a total row), the cited arc is the
    value; another arc stays only as a consistent rendering of the same fact.
    An arc no candidate prints selects nothing, so undisambiguated rivals
    still fail as ``multiple_presentation_occurrences``.
    """

    if cited_arc is None:
        return candidates
    cited = tuple(
        occurrence
        for occurrence in candidates
        if occurrence.source.cell_locator == cited_arc
    )
    if not cited:
        return candidates
    renderings = {_rendering_identity(occurrence) for occurrence in cited}
    return (
        *cited,
        *(
            occurrence
            for occurrence in candidates
            if occurrence.source.cell_locator != cited_arc
            and _rendering_identity(occurrence) in renderings
        ),
    )


def _observation_for_candidates(
    requested: FilingLineRef,
    candidates: Sequence[FilingPresentationCellV1],
    *,
    cited_arc: str | None,
) -> FilingLineObservation:
    candidates = _ending_balance_candidates(_cited_candidates(candidates, cited_arc))
    minted = tuple(_minted_line(occurrence) for occurrence in candidates)
    if any(line != requested for line in minted):
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail="occurrence_minted_identity_mismatch",
        )
    if len({_rendering_identity(occurrence) for occurrence in candidates}) != 1:
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail=_candidate_detail("multiple_presentation_occurrences", candidates),
        )
    occurrence = candidates[0]
    binding = occurrence.binding
    if isinstance(binding, GapBindingV1):
        return FilingLineObservation(
            line=requested,
            status="missing",
            fact=None,
            provider="edgar",
            detail=binding.reason,
        )
    if binding is None:
        return FilingLineObservation(
            line=requested,
            status="missing",
            fact=None,
            provider="edgar",
            detail=occurrence.presentation_state or "unbound_occurrence",
        )
    if isinstance(binding, AmbiguousBindingV1):
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail=_ambiguous_binding_detail(occurrence),
        )
    if not isinstance(binding, BoundBindingV1):
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail="unsupported_binding",
        )
    if occurrence.presentation_state in {
        "reported_nil",
        "unbound_dash",
        "unbound_blank",
    }:
        return FilingLineObservation(
            line=requested,
            status="missing",
            fact=None,
            provider="edgar",
            detail=occurrence.presentation_state,
        )
    display = occurrence.presentation_display
    if not isinstance(display, PresentationDisplayKnownV1):
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail=f"presentation_display_unknown:{display.reason}",
        )
    native_quantity = binding.fact.native_quantity
    if not isinstance(native_quantity, NumericQuantityV1):
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail=f"native_quantity_{native_quantity.state}",
        )
    number = native_quantity.native_number
    if number.kind != "decimal":
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail="native_rational_not_representable_as_quantity",
        )
    try:
        native_value = Decimal(number.value)
    except InvalidOperation:
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail="native_decimal_invalid",
        )
    if not native_value.is_finite():
        return FilingLineObservation(
            line=requested,
            status="failed",
            fact=None,
            provider="edgar",
            detail="native_decimal_non_finite",
        )
    with localcontext() as context:
        # Dividing by a power-of-ten scale and flipping the sign are exact once
        # the context holds every native digit; the ambient default would round.
        context.prec = max(context.prec, len(native_value.as_tuple().digits))
        value = (
            Decimal(0)
            if occurrence.presentation_state == "reported_fixed_zero_dash"
            else native_value / _SCALE_FACTORS[display.scale]
        )
        if occurrence.preferred_label.negates_value:
            value = -value
    display_payload = display.model_dump(
        mode="python",
        exclude={"kind"},
        exclude_none=True,
    )
    quantity = Quantity.model_validate(
        {
            "value": normalize_decimal_v1(value),
            "display": display_payload,
        },
        strict=True,
    )
    detail = _semantic_detail(occurrence, number.value)
    provenance = VendorProvenance(
        provider=EDGAR_PROVIDER_ID,
        native_endpoint="filing_facts",
        native_field=_native_field(
            occurrence, minted[0], number.value, candidates
        ),
        preferred_label_text=occurrence.preferred_label.text,
        preferred_label_role_uri=occurrence.preferred_label.role_uri,
        preferred_label_negates_value=occurrence.preferred_label.negates_value,
        decimals=native_quantity.source.decimals,
    )
    fact = FilingLineFact(
        line=minted[0],
        value=quantity,
        grade="filing_native",
        provenance=provenance,
        label=occurrence.preferred_label.text,
        source_ref=mint_source_fact_ref(minted[0], "statement"),
    )
    return FilingLineObservation(
        line=minted[0],
        status="ok",
        fact=fact,
        detail=detail,
    )


def _candidate_detail(
    reason: str,
    candidates: Sequence[FilingPresentationCellV1],
) -> str:
    return json.dumps(
        {
            "reason": reason,
            "candidates": [
                {
                    "dimensions": json.loads(_dimension_key(row)),
                    "label": row.preferred_label.text,
                    "locator": row.source.cell_locator,
                    "presentation_state": row.presentation_state,
                }
                for row in candidates
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

def _ambiguous_binding_detail(occurrence: FilingPresentationCellV1) -> str:
    binding = occurrence.binding
    assert isinstance(binding, AmbiguousBindingV1)
    return json.dumps(
        {
            "reason": "ambiguous_binding",
            "candidates": [
                {
                    "dimensions": [
                        dimension.model_dump(mode="json")
                        for dimension in candidate.dimensions
                    ],
                    "native_quantity": candidate.native_quantity.model_dump(
                        mode="json"
                    ),
                    "qname": candidate.qname.lexical_qname,
                    "source_locators": [
                        locator.model_dump(mode="json")
                        for locator in candidate.provenance.locators
                    ],
                }
                for candidate in binding.candidates
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )



def _semantic_detail(
    occurrence: FilingPresentationCellV1,
    native_number: str,
) -> str:
    return json.dumps(
        {
            "native_number": native_number,
            "preferred_label_negates_value": occurrence.preferred_label.negates_value,
            "presentation_state": occurrence.presentation_state,
            "printed_lexical": occurrence.printed_lexical,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _native_field(
    occurrence: FilingPresentationCellV1,
    line: FilingLineRef,
    native_number: str,
    renderings: Sequence[FilingPresentationCellV1],
) -> str:
    other_cells = [
        rendering.source.cell_locator
        for rendering in renderings
        if rendering is not occurrence
    ]
    return json.dumps(
        {
            "accession": occurrence.accession,
            "cell_locator": occurrence.source.cell_locator,
            # The bundle builder embeds this string verbatim in sealed
            # SourceBundle bytes (evidence locators and statement-fact
            # provenance), which the content digest covers. Absent means
            # absent: only a fact rendered more than once names its other
            # presentation cells.
            **(
                {"consistent_presentation_cells": other_cells}
                if other_cells
                else {}
            ),
            "dimensions": [
                {
                    "axis_qname": dimension.axis_qname,
                    "member_qname": dimension.member_qname,
                }
                for dimension in line.dimensions
            ],
            "line_qname": occurrence.line_qname.lexical_qname,
            "native_number": native_number,
            "preferred_label_negates_value": occurrence.preferred_label.negates_value,
            "preferred_label_role_uri": occurrence.preferred_label.role_uri,
            "preferred_label_text": occurrence.preferred_label.text,
            "presentation_role_uri": occurrence.presentation.presentation_role_uri,
            "printed_column_position": occurrence.printed_column.position,
            "consistent_duplicates": [
                locator.model_dump(mode="json")
                for locator in (
                    occurrence.binding.fact.consistent_duplicates
                    if isinstance(occurrence.binding, BoundBindingV1)
                    else ()
                )
            ],
            "xbrl_locators": [
                locator.model_dump(mode="json")
                for locator in occurrence.source.xbrl_locators
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _reported_periods_from_columns(
    envelopes: Sequence[EdgarFilingFactsEnvelopeV1],
    *,
    filing_accepted_at_by_accession: Mapping[str, datetime | None],
    start_year: int,
    end_year: int,
) -> tuple[ReportedPeriod, ...]:
    columns_by_year: dict[
        int,
        list[tuple[FilingPresentationCellV1, EdgarFilingFactsEnvelopeV1]],
    ] = {}
    for envelope in envelopes:
        for occurrence in envelope.occurrences:
            fiscal_year = occurrence.printed_column.period_ref.fiscal_year
            if (
                start_year <= fiscal_year <= end_year
                and occurrence.presentation.presentation_kind != "parenthetical"
                and frozenset(occurrence.presentation.statement_roles) & _FACE_ROLES
            ):
                columns_by_year.setdefault(fiscal_year, []).append(
                    (occurrence, envelope)
                )
    periods: list[ReportedPeriod] = []
    for fiscal_year, rows in columns_by_year.items():
        period_ends = {
            date.fromisoformat(row.printed_column.period_end)
            for row, _envelope in rows
        }
        if len(period_ends) != 1:
            logging.warning(
                "edgar occurrence columns disagree on FY%s period_end; omitting period",
                fiscal_year,
            )
            continue
        period_end = period_ends.pop()
        sourced = [
            (row, envelope)
            for row, envelope in rows
            if row.printed_column.period_start is not None
        ]
        starts = {
            date.fromisoformat(row.printed_column.period_start or "")
            for row, _envelope in sourced
        }
        sources = {
            (
                envelope.filing.accession,
                envelope.filing.filing_date,
                envelope.filing.primary_document_url,
            )
            for _row, envelope in sourced
        }
        if len(starts) == 1 and len(sources) == 1:
            accession, filing_date, source_url = sources.pop()
            periods.append(
                ReportedPeriod(
                    fiscal_year=fiscal_year,
                    period_type="FY",
                    period_end=period_end,
                    period_start=starts.pop(),
                    source_provider=EDGAR_PROVIDER_ID,
                    native_source_id=accession,
                    source_available_on=date.fromisoformat(filing_date),
                    source_locator=source_url,
                    source_accepted_at=filing_accepted_at_by_accession[accession],
                )
            )
        else:
            periods.append(
                ReportedPeriod(
                    fiscal_year=fiscal_year,
                    period_type="FY",
                    period_end=period_end,
                )
            )
    return tuple(
        sorted(periods, key=lambda period: period.period_end, reverse=True)
    )

def reported_period_candidates_from_validated_payload(
    validated_payload: Mapping[str, object],
) -> tuple[tuple[int, ReportedPeriod], ...]:
    """Project every dated producer row without discarding imprecise candidates."""

    raw_series = validated_payload.get("series")
    if not isinstance(raw_series, list):
        raise ValueError("validated EDGAR metric-series payload requires series")
    candidates: list[tuple[int, ReportedPeriod]] = []
    for row_index, raw_entry in enumerate(raw_series):
        if not isinstance(raw_entry, dict):
            raise ValueError("validated EDGAR metric-series rows must be objects")
        typed_entry = validated_metric_series_entry(raw_entry)
        if typed_entry is None:
            raise ValueError("EDGAR metric-series row lacks strict DTO validation")
        if typed_entry.period_end is None:
            continue
        period_end = _canonical_date(typed_entry.period_end)
        assert period_end is not None
        period_type: ReportedPeriodType = (
            "FY"
            if typed_entry.period_ref.kind == "fiscal_year"
            else _REPORTED_QUARTERS[typed_entry.quarter - 1]
        )
        legacy = ReportedPeriod(
            typed_entry.year,
            period_type,
            period_end,
        )
        if typed_entry.quantity is None or typed_entry.metric_tag is None:
            candidates.append((row_index, legacy))
            continue
        source_value = source_value_from_edgar_quantity(
            typed_entry.quantity,
            tag=typed_entry.metric_tag,
            year=typed_entry.year,
            source_ref=typed_entry.source_ref,
            reported_period_start=typed_entry.reported_period_start,
            reported_duration_period_type=typed_entry.reported_duration_period_type,
            source_accepted_at=(
                typed_entry.source_accepted_at
                if typed_entry.reported_duration_period_type is not None
                else None
            ),
        )
        sourced = _sourced_reported_period(
            typed_entry.year,
            period_type,
            period_end,
            source_value,
        )
        candidates.append((row_index, sourced or legacy))
    return tuple(candidates)


def _canonical_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    raw = str(value)
    parsed = date.fromisoformat(raw)
    if parsed.isoformat() != raw:
        raise ValueError("EDGAR period_end must be a canonical ISO date")
    return parsed


def _sourced_reported_period(
    fiscal_year: int,
    period_type: ReportedPeriodType,
    period_end: date | None,
    source_value: SourceValue | None,
) -> ReportedPeriod | None:
    if (
        period_end is None
        or source_value is None
        or source_value.reported_duration_period_type != period_type
        or source_value.reported_period_start is None
        or not isinstance(source_value.source_ref, Mapping)
    ):
        return None
    current = source_value.source_ref.get("current")
    if not isinstance(current, Mapping):
        return None
    accession = current.get("accession")
    filing_date = current.get("filing_date")
    filing_url = current.get("filing_url")
    if not all(isinstance(value, str) for value in (accession, filing_date, filing_url)):
        return None
    source_available_on = _canonical_date(filing_date)
    if source_available_on is None:
        return None
    return ReportedPeriod(
        fiscal_year=fiscal_year,
        period_type=period_type,
        period_end=period_end,
        period_start=source_value.reported_period_start,
        source_provider=EDGAR_PROVIDER_ID,
        native_source_id=accession,
        source_available_on=source_available_on,
        source_locator=filing_url,
        source_accepted_at=source_value.source_accepted_at,
    )


def latest_reported_period_accepted_by_cutoff(
    periods: Sequence[ReportedPeriod],
    *,
    cutoff_at: datetime,
) -> ReportedPeriod | None:
    """Choose the latest unambiguous sourced period known accepted by cutoff."""

    if cutoff_at.tzinfo is None or cutoff_at.utcoffset() != timedelta(0):
        raise ValueError("cutoff_at must be aware UTC")
    if not periods:
        return None
    eligible = tuple(
        period
        for period in periods
        if period.source_accepted_at is not None
        and period.source_accepted_at <= cutoff_at
    )
    potentially_available_imprecise = tuple(
        period
        for period in periods
        if period.source_accepted_at is None
        and period.period_end <= cutoff_at.date()
        and (
            period.source_available_on is None
            or period.source_available_on <= cutoff_at.date()
        )
    )
    if not eligible:
        if potentially_available_imprecise:
            raise ValueError(
                "reported periods do not contain precise source acceptance by cutoff"
            )
        return None
    latest_period_end = max(period.period_end for period in eligible)
    for period in potentially_available_imprecise:
        if period.period_end < latest_period_end:
            continue
        if not period.has_sourced_duration and period.period_end == latest_period_end:
            # An end-only row carries no filing identity at all, so it asserts
            # nothing about the period it shadows. When a fully sourced period
            # shares that exact end, the sourced provenance already establishes
            # the anchor and the shadow row must not defeat it. Any other
            # imprecise row at or after the latest end leaves the anchor
            # under-evidenced and stays fail-closed.
            continue
        raise ValueError("latest reported period has incomplete source provenance")
    latest = {
        period for period in eligible if period.period_end == latest_period_end
    }
    if len(latest) != 1:
        raise ValueError("latest reported period source identity is ambiguous")
    return latest.pop()
