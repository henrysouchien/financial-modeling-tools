"""Valuation-evidence derivations over the SourceBundle's typed observations.

Engine (B) procedures that read the Store types of
``schema/build_model_product_contracts.py``: the observation projections onto
the shared ``value_semantics_core`` arithmetic, the per-observation coverage and
eligibility verdicts, valuation coverage, covered market inputs, relative
selection and the valuation-evidence status with its public projection. They
derive; they never construct or store a contract the Store does not already
type. ``schema/valuation_evidence_validation.py`` owns the source-binding check
these derivations consult.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Final, Literal, Sequence

from industry_slice_core.thesis_shared_sources import SourceRecord
from value_semantics_core import (
    DatedEnterpriseValueAvailable,
    DatedEnterpriseValueResult,
    EnterpriseValueComponent,
    ForwardEvAdjustedEbitdaAvailable,
    ForwardEvAdjustedEbitdaResult,
    NtmForwardEvAdjustedEbitdaAvailable,
    NtmForwardEvAdjustedEbitdaResult,
    NtmForwardPeAvailable,
    NtmForwardPeResult,
    QuarterlyForwardAdjustedEbitdaEstimate,
    QuarterlyForwardEpsEstimate,
    derive_dated_enterprise_value,
    derive_forward_ev_adjusted_ebitda,
    derive_ntm_forward_ev_adjusted_ebitda,
    derive_ntm_forward_pe,
)

from .build_model_product_contracts import (
    VALUATION_MULTIPLE_QUANTUM,
    CoveredMarketInput,
    CoveredMarketInputs,
    FiscalPeriodFacts,
    ForwardEvAdjustedEbitdaOperands,
    MarketCalendarFacts,
    NtmForwardEvAdjustedEbitdaOperands,
    RelativeSelectionEligibilityShortfall,
    RelativeSelectionRejected,
    RelativeSelectionResolution,
    SourceBundle,
    ValuationCoverage,
    ValuationEligibilityReason,
    ValuationEstimateBasis,
    ValuationEvidenceStatus,
    ValuationEvidenceStatusArtifact,
    ValuationEvidenceStatusNotRecorded,
    ValuationMethodEvidenceStatus,
    ValuationObservation,
    ValuationObservationVerdict,
    ValuationPeerPolicy,
    ValuationShortfall,
    ValuationShortfallReason,
    _DenominatorVerification,
    _SHORTFALL_REASON_ORDER,
)
from .coverage_profile import ValuationRequirement
from .ticker_contract import normalize_contract_ticker


def derive_observation_dated_enterprise_value(
    observations: Sequence[ValuationObservation],
    *,
    issuer: str,
    as_of_close: date,
) -> DatedEnterpriseValueResult | None:
    """Project one exact dated component observation set into shared EV arithmetic.

    The arithmetic core is closed and universal: market capitalisation from price
    times split-adjusted shares, plus debt, less cash. Any further balance an
    issuer presents arrives as a declared adjustment row that states its own
    arithmetic effect, and this bridge honours exactly what each row declares —
    a ``disclosure_only`` row contributes no term, and an arithmetic row
    contributes exactly one signed term. A declared row this bridge cannot
    honour fails the whole projection closed; nothing is silently ignored.
    """

    rows = tuple(
        row
        for row in observations
        if row.issuer == issuer
        and row.as_of_close == as_of_close
        and row.component_type is not None
    )
    declared = tuple(row for row in rows if row.component_type == "declared_adjustment")
    core = {
        row.component_type: row
        for row in rows
        if row.component_type != "declared_adjustment"
    }
    if len(core) != len(rows) - len(declared):
        return None
    if set(core) != {"price", "shares", "debt", "cash"}:
        return None
    price = core["price"]
    shares = core["shares"]
    debt = core["debt"]
    cash = core["cash"]
    if (
        price.unit != "usd_per_share"
        or shares.unit != "shares_millions"
        or debt.unit != "usd_millions"
        or cash.unit != "usd_millions"
        or {price.currency, debt.currency, cash.currency} != {"USD"}
        or shares.split_events_between
    ):
        return None
    adjustments: list[EnterpriseValueComponent] = []
    role_labels: set[str] = set()
    for row in declared:
        adjustment = row.declared_adjustment
        if adjustment is None or adjustment.role_label in role_labels:
            return None
        role_labels.add(adjustment.role_label)
        if row.unit != "usd_millions" or row.currency != "USD":
            return None
        if adjustment.effect == "disclosure_only":
            continue
        signed = (
            row.value if adjustment.effect == "adds_to_enterprise_value" else -row.value
        )
        adjustments.append(EnterpriseValueComponent(row.observation_id, signed))
    return derive_dated_enterprise_value(
        price_per_share=price.value,
        split_adjusted_shares=shares.value,
        components=(
            EnterpriseValueComponent(debt.observation_id, debt.value),
            EnterpriseValueComponent(cash.observation_id, -cash.value),
            *adjustments,
        ),
    )


def derive_observation_ntm_forward_pe(
    observation: ValuationObservation,
) -> NtmForwardPeResult | None:
    """Project one observation into the shared exact-NTM calculation owner."""

    operands = observation.ntm_operands
    if operands is None:
        return None
    estimates = tuple(
        QuarterlyForwardEpsEstimate(
            fact_id=f"{observation.source_id}::{item.eps_ref}",
            period_start=item.period_start,
            period_end=item.period_end,
            eps=item.eps,
            analyst_count=item.analyst_count,
        )
        for item in operands.quarterly_eps
    )
    return derive_ntm_forward_pe(
        price=observation.raw_close,
        close_date=observation.raw_market_date,
        estimates=estimates,
        last_reported_period_end=operands.reported_anchor.period_end,
        quantum=VALUATION_MULTIPLE_QUANTUM,
    )


_ForwardEvOperands = (
    ForwardEvAdjustedEbitdaOperands | NtmForwardEvAdjustedEbitdaOperands
)


def derive_observation_forward_ev_adjusted_ebitda(
    observation: ValuationObservation,
    observations: Sequence[ValuationObservation],
) -> ForwardEvAdjustedEbitdaResult | NtmForwardEvAdjustedEbitdaResult | None:
    """Project one typed observation into the shared exact ratio owner."""

    operands: _ForwardEvOperands | None = (
        observation.forward_adjusted_ebitda_operands
        if observation.forward_adjusted_ebitda_operands is not None
        else observation.ntm_forward_adjusted_ebitda_operands
    )
    if operands is None or observation.issuer is None:
        return None
    component_by_id = {
        row.observation_id: row
        for row in observations
        if row.observation_id in operands.enterprise_value_component_ids
    }
    if set(component_by_id) != set(operands.enterprise_value_component_ids):
        return None
    components = tuple(
        component_by_id[value] for value in operands.enterprise_value_component_ids
    )
    if any(
        row.issuer != observation.issuer or row.as_of_close != observation.as_of_close
        for row in components
    ):
        return None
    enterprise_value = derive_observation_dated_enterprise_value(
        components,
        issuer=observation.issuer,
        as_of_close=observation.as_of_close,
    )
    if not isinstance(enterprise_value, DatedEnterpriseValueAvailable):
        return None
    if isinstance(operands, NtmForwardEvAdjustedEbitdaOperands):
        return derive_ntm_forward_ev_adjusted_ebitda(
            enterprise_value=enterprise_value.enterprise_value,
            close_date=observation.raw_market_date,
            estimates=tuple(
                QuarterlyForwardAdjustedEbitdaEstimate(
                    fact_id=f"{observation.source_id}::{item.adjusted_ebitda_ref}",
                    period_start=item.period_start,
                    period_end=item.period_end,
                    adjusted_ebitda=item.adjusted_ebitda,
                    analyst_count=item.analyst_count,
                )
                for item in operands.quarterly_adjusted_ebitda
            ),
            last_reported_period_end=operands.reported_anchor.period_end,
            quantum=VALUATION_MULTIPLE_QUANTUM,
        )
    return derive_forward_ev_adjusted_ebitda(
        enterprise_value=enterprise_value.enterprise_value,
        adjusted_ebitda=operands.adjusted_ebitda,
        quantum=VALUATION_MULTIPLE_QUANTUM,
    )


_FMP_PROVIDER_LINEAGE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "validated_params",
        "normalized_records",
        "lineage_descriptor",
        "observed_at",
        "normalized_response_sha256",
    }
)


def _has_fmp_provider_lineage(source: SourceRecord) -> bool:
    """Identify sources that claim any part of the provider-owned FMP contract."""

    return bool(_FMP_PROVIDER_LINEAGE_FIELDS & set(source.key_fields or {}))


def _is_exact_fmp_official_close_source(source: SourceRecord) -> bool:
    """Classify the exact source shape owned by the binding validator below."""

    key_fields = source.key_fields or {}
    return (
        source.provider == "fmp"
        and source.endpoint_or_filing_id == "historical_price_eod"
        and key_fields.get("native_field") == "close"
        and _FMP_PROVIDER_LINEAGE_FIELDS <= set(key_fields)
    )


_VALUATION_REASON_PUBLIC_PHRASES: Final[dict[str, str]] = {
    "missing": "required observation absent",
    "stale_close": "market close is stale",
    "wrong_close_date": "dated official close is unavailable",
    "definition_nonconforming": "provider estimate definition evidence absent",
    "mapping_nonconforming": "issuer-fiscal mapping evidence absent",
    "cutoff_violation": "observation arrived after the evidence cutoff",
    "currency_nonconforming": "currency basis is not USD",
    "basis_unreconciled": "share or valuation basis is unreconciled",
    "not_separable": "required corporate component is not separable",
    "horizon_nonconforming": "observation is not an exact next-four-quarter horizon",
    "unit_nonconforming": "observation unit is not authorized",
    "value_nonconforming": "observation value is outside its authorized domain",
    "source_nonconforming": "observation source is not authorized for model use",
    "duplicate_conforming_observation": "multiple conforming observations prevent deterministic selection",
    "peer_roster_empty": "no peer roster is named",
}

_CONSENSUS_NON_GAAP_DETAIL_SUFFIX: Final[str] = (
    "; consensus non-GAAP EBITDA denominator, not issuer-reconciled"
)


def valuation_evidence_status_public_projection(
    status: ValuationEvidenceStatusArtifact,
) -> dict[str, Any]:
    if isinstance(status, ValuationEvidenceStatusNotRecorded):
        return {
            "kind": status.kind,
            "summary": "Pre-slice-6 valuation evidence status was not recorded",
        }
    rows: list[dict[str, Any]] = []
    for method in status.methods:
        if method.status == "resolved":
            detail = (
                f"Median of {len(method.contributing_ids)} covered observations"
                + (
                    f" as of {method.as_of_close.isoformat()}"
                    if method.as_of_close is not None
                    else ""
                )
                + (
                    _CONSENSUS_NON_GAAP_DETAIL_SUFFIX
                    if method.method == "ev_adjusted_ebitda"
                    else ""
                )
            )
        else:
            reasons = tuple(
                dict.fromkeys(
                    [item.reason for item in method.shortfall]
                    + [item.reason for item in method.eligibility_shortfall]
                    + [reason for item in method.rejected for reason in item.reasons]
                )
            )
            phrases = tuple(_VALUATION_REASON_PUBLIC_PHRASES[item] for item in reasons)
            detail = "Blocked" + (f" — {'; '.join(phrases)}" if phrases else "")
        rows.append(
            {
                "method": method.method,
                "status": method.status,
                "fiscal_basis": method.fiscal_basis,
                "selected_value": (
                    None
                    if method.selected_value is None
                    else format(method.selected_value, "f")
                ),
                "contributing_ids": list(method.contributing_ids),
                "denominator_verification": method.denominator_verification,
                "detail": detail,
            }
        )
    return {
        "kind": status.kind,
        "bundle_digest": status.bundle_digest,
        "methods": rows,
    }


def _period_mapping_conforms(
    observation: ValuationObservation,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
) -> bool:
    if (
        observation.ntm_operands is not None
        or observation.ntm_forward_adjusted_ebitda_operands is not None
    ):
        return True
    evidence = observation.estimate_period_evidence
    if evidence is None or observation.issuer is None:
        return False
    if evidence.mapping_rule != "provider_annual_period_end_exact_match":
        return False
    try:
        raw_period_end = date.fromisoformat(evidence.raw_provider_period)
    except ValueError:
        return False
    matches = tuple(
        fact
        for fact in fiscal_calendar
        if fact.issuer == observation.issuer and fact.basis == observation.fiscal_basis
    )
    return (
        len(matches) == 1
        and matches[0].has_exact_annual_source_evidence
        and raw_period_end == matches[0].period_end
    )


def _valuation_coverage_reasons(
    item: ValuationObservation,
    *,
    cutoff_at: datetime,
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
) -> tuple[ValuationShortfallReason, ...]:
    """Return conformance reasons using the observation's market-session facts."""

    failed: list[ValuationShortfallReason] = []
    sessions = market_calendar.completed_session_dates
    if item.retrieved_at > cutoff_at:
        failed.append("cutoff_violation")
    expected_market_date: date | None = item.as_of_close
    last_bond = next(
        (session for session in reversed(market_calendar.bond_session_dates)
         if session <= item.as_of_close),
        None,
    )
    if (
        item.metric == "risk_free_10y"
        and item.fiscal_basis == "point_in_time"
        and item.as_of_close not in market_calendar.bond_session_dates
        and last_bond is not None
    ):
        expected_market_date = last_bond
    if item.raw_market_date is not None and item.raw_market_date != expected_market_date:
        failed.append("wrong_close_date")
    if item.metric in {"fwd_pe", "fwd_ev_adj_ebitda"} and item.raw_close is None:
        failed.append("wrong_close_date")
    if item.as_of_close not in sessions or (
        sessions
        and len([d for d in sessions if item.as_of_close <= d <= sessions[-1]]) > 5
    ):
        failed.append("stale_close")
    if item.metric in {"fwd_pe", "fwd_ev_adj_ebitda"}:
        definition = item.definition_evidence
        if definition is None or definition.stated_basis not in {
            "non_gaap",
            "adjusted",
        }:
            failed.append("definition_nonconforming")
        if not _period_mapping_conforms(item, fiscal_calendar):
            failed.append("mapping_nonconforming")
    if (item.metric == "fwd_ev_adj_ebitda" and item.currency != "USD") or (
        item.currency is not None and item.currency != "USD"
    ):
        failed.append("currency_nonconforming")
    if item.split_events_between:
        failed.append("basis_unreconciled")
    if item.separability == "not_separable":
        failed.append("not_separable")
    return tuple(dict.fromkeys(failed))


def _valuation_eligibility_reasons(
    item: ValuationObservation,
    observations: Sequence[ValuationObservation],
) -> tuple[ValuationEligibilityReason, ...]:
    """Apply the stricter slice-6 activation trust boundary."""

    expected_units = {
        "quote": "usd_per_share",
        "beta": "ratio",
        "risk_free_10y": "ratio",
        "erp": "ratio",
        "fwd_pe": "multiple",
        "fwd_ev_adj_ebitda": "multiple",
    }
    reasons: list[ValuationEligibilityReason] = []
    if item.metric == "fwd_pe":
        ntm_result = derive_observation_ntm_forward_pe(item)
        if ntm_result is None or not isinstance(ntm_result, NtmForwardPeAvailable):
            reasons.append("horizon_nonconforming")
        elif ntm_result.multiple != item.value:
            reasons.append("value_nonconforming")
        if ntm_result is not None:
            if (
                item.currency != "USD"
                or item.currency_source_id is None
                or item.currency_ref is None
            ):
                reasons.append("currency_nonconforming")
            if item.split_history_ref is None or item.split_events_between:
                reasons.append("basis_unreconciled")
    elif item.metric == "fwd_ev_adj_ebitda":
        result = derive_observation_forward_ev_adjusted_ebitda(item, observations)
        if not isinstance(
            result,
            ForwardEvAdjustedEbitdaAvailable | NtmForwardEvAdjustedEbitdaAvailable,
        ):
            reasons.append("basis_unreconciled")
        elif result.multiple != item.value:
            reasons.append("value_nonconforming")
    expected_unit = expected_units.get(item.metric)
    if expected_unit is not None and item.unit != expected_unit:
        reasons.append("unit_nonconforming")
    value = item.value
    if not value.is_finite():
        reasons.append("value_nonconforming")
    elif item.metric in {"quote", "fwd_pe", "fwd_ev_adj_ebitda"} and value <= 0:
        reasons.append("value_nonconforming")
    elif item.metric == "beta" and not (Decimal("0") < value <= Decimal("4")):
        reasons.append("value_nonconforming")
    elif item.metric in {"risk_free_10y", "erp"} and not (
        Decimal("0") < value <= Decimal("0.25")
    ):
        reasons.append("value_nonconforming")
    if item.metric == "quote" and (
        item.raw_market_date != item.as_of_close
        or item.raw_close is None
        or item.raw_close != item.value
    ):
        reasons.append("wrong_close_date")
    return tuple(dict.fromkeys(reasons))


def derive_valuation_observation_verdicts(
    observations: tuple[ValuationObservation, ...],
    cutoff_at: datetime,
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
) -> tuple[ValuationObservationVerdict, ...]:
    return tuple(
        ValuationObservationVerdict(
            observation_id=item.observation_id,
            coverage_reasons=_valuation_coverage_reasons(
                item,
                cutoff_at=cutoff_at,
                market_calendar=market_calendar,
                fiscal_calendar=fiscal_calendar,
            ),
            eligibility_reasons=_valuation_eligibility_reasons(item, observations),
        )
        for item in observations
    )


def derive_source_aware_valuation_observation_verdicts(
    observations: tuple[ValuationObservation, ...],
    cutoff_at: datetime,
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
    *,
    sources: tuple[SourceRecord, ...],
    retrieved_at: datetime,
) -> tuple[ValuationObservationVerdict, ...]:
    """Project source authority without changing stored evidence or display."""

    # The validator imports this module's observation projections at module
    # level, so this one call back into it is resolved at call time.
    from .valuation_evidence_validation import (
        validate_valuation_evidence_source_bindings,
    )

    source_by_id = {source.id: source for source in sources}
    if len(source_by_id) != len(sources):
        raise ValueError("valuation evidence source ids must be unique")
    verdicts = derive_valuation_observation_verdicts(
        observations,
        cutoff_at,
        market_calendar,
        fiscal_calendar,
    )
    results: list[ValuationObservationVerdict] = []
    for observation, verdict in zip(observations, verdicts, strict=True):
        reasons = list(verdict.eligibility_reasons)
        if observation.metric == "quote":
            source = source_by_id.get(observation.source_id)
            if source is None:
                raise ValueError("valuation observation source_id must resolve")
            if _has_fmp_provider_lineage(source):
                # Reuse the exact provider-owned scalar/date/time binder without
                # revalidating unrelated observations or fiscal facts.
                validate_valuation_evidence_source_bindings(
                    observations=(observation,),
                    fiscal_calendar=(),
                    sources=sources,
                    cutoff_at=cutoff_at,
                    retrieved_at=retrieved_at,
                )
            if not _is_exact_fmp_official_close_source(source):
                reasons.append("source_nonconforming")
        results.append(
            verdict.model_copy(
                update={"eligibility_reasons": tuple(dict.fromkeys(reasons))}
            )
        )
    return tuple(results)


def derive_valuation_coverage(
    observations: tuple[ValuationObservation, ...],
    cutoff_at: datetime,
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
    *,
    requirements: tuple[ValuationRequirement, ...],
    focal_ticker: str,
    peer_tickers: tuple[str, ...],
    minimum_covered_peer_count: int,
) -> tuple[ValuationCoverage, ...]:
    """Derive a resolved profile denominator from normalized payload facts."""

    focal = normalize_contract_ticker(focal_ticker)
    peers = tuple(normalize_contract_ticker(item) for item in peer_tickers)
    if peers != tuple(sorted(set(peers))) or focal in peers:
        raise ValueError("valuation peer roster must be sorted, unique, and non-focal")
    if minimum_covered_peer_count < 0 or minimum_covered_peer_count > len(peers):
        raise ValueError("minimum covered peer count is outside the profile roster")
    verdicts = {
        item.observation_id: item
        for item in derive_valuation_observation_verdicts(
            observations,
            cutoff_at,
            market_calendar,
            fiscal_calendar,
        )
    }

    def reasons(item: ValuationObservation) -> tuple[ValuationShortfallReason, ...]:
        return verdicts[item.observation_id].coverage_reasons

    results: list[ValuationCoverage] = []
    for requirement in requirements:
        candidates = tuple(
            item
            for item in observations
            if (item.metric, item.fiscal_basis)
            == (requirement.metric, requirement.fiscal_basis)
        )
        observation_ids = tuple(sorted(item.observation_id for item in candidates))
        scopes = (
            (focal, *peers)
            if requirement.roster_rule == "focal_and_peers"
            else (focal if requirement.scope == "focal" else requirement.metric,)
        )
        by_scope: dict[str, tuple[ValuationObservation, ...]] = {}
        for scope in scopes:
            by_scope[scope] = tuple(
                item
                for item in candidates
                if item.issuer == (scope if scope in {focal, *peers} else None)
            )
        eligible = {
            scope: any(not reasons(item) for item in scoped)
            for scope, scoped in by_scope.items()
        }
        status = (
            "covered"
            if (
                eligible[focal]
                and sum(eligible[p] for p in peers) >= minimum_covered_peer_count
                if requirement.roster_rule == "focal_and_peers"
                else eligible[scopes[0]]
            )
            else "uncovered"
        )
        shortfall: list[ValuationShortfall] = []
        if status == "uncovered":
            for scope in scopes:
                if eligible[scope]:
                    continue
                scoped = by_scope[scope]
                scope_reasons: set[ValuationShortfallReason] = {
                    reason for item in scoped for reason in reasons(item)
                }
                if not scoped:
                    scope_reasons = {"missing"}
                for reason in sorted(
                    scope_reasons, key=_SHORTFALL_REASON_ORDER.__getitem__
                ):
                    shortfall.append(ValuationShortfall(scope_key=scope, reason=reason))
            shortfall.sort(
                key=lambda item: (item.scope_key, _SHORTFALL_REASON_ORDER[item.reason])
            )
        results.append(
            ValuationCoverage(
                requirement_id=requirement.requirement_id,
                status=status,
                observation_ids=observation_ids,
                shortfall=tuple(shortfall),
            )
        )
    return tuple(results)


def derive_covered_market_inputs(
    observations: tuple[ValuationObservation, ...],
    cutoff_at: datetime,
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
    *,
    requirements: tuple[ValuationRequirement, ...],
    focal_ticker: str,
    sources: tuple[SourceRecord, ...],
    retrieved_at: datetime,
) -> CoveredMarketInputs:
    """Resolve exactly one L2-eligible observation for each market input."""

    verdicts = {
        item.observation_id: verdict
        for item, verdict in zip(
            observations,
            derive_source_aware_valuation_observation_verdicts(
                observations,
                cutoff_at,
                market_calendar,
                fiscal_calendar,
                sources=sources,
                retrieved_at=retrieved_at,
            ),
            strict=True,
        )
    }
    requirement_by_metric = {
        requirement.metric: requirement
        for requirement in requirements
        if requirement.metric in {"quote", "beta", "risk_free_10y", "erp"}
    }
    if set(requirement_by_metric) != {"quote", "beta", "risk_free_10y", "erp"}:
        raise ValueError("profile must define one requirement for every market input")
    resolved: dict[str, CoveredMarketInput] = {}
    field_by_metric: dict[
        str,
        Literal[
            "stock_price",
            "raw_beta",
            "risk_free_rate",
            "equity_risk_premium",
        ],
    ] = {
        "quote": "stock_price",
        "beta": "raw_beta",
        "risk_free_10y": "risk_free_rate",
        "erp": "equity_risk_premium",
    }
    focal = normalize_contract_ticker(focal_ticker)
    for metric, field_name in field_by_metric.items():
        requirement = requirement_by_metric[metric]
        requirement_id = requirement.requirement_id
        candidates = tuple(
            item
            for item in observations
            if (item.metric, item.fiscal_basis)
            == (requirement.metric, requirement.fiscal_basis)
            and item.issuer == (focal if requirement.scope == "focal" else None)
        )
        eligible = tuple(
            item
            for item in candidates
            if verdicts[item.observation_id].selection_eligible
        )
        if len(eligible) == 1:
            item = eligible[0]
            result = CoveredMarketInput(
                requirement_id=requirement_id,
                field_name=field_name,
                status="resolved",
                value=item.value,
                observation_id=item.observation_id,
                source_id=item.source_id,
            )
        else:
            candidate_reasons: tuple[ValuationEligibilityReason, ...] = tuple(
                reason
                for item in candidates
                for reason in (
                    *verdicts[item.observation_id].coverage_reasons,
                    *verdicts[item.observation_id].eligibility_reasons,
                )
            )
            reasons: tuple[ValuationEligibilityReason, ...]
            if len(eligible) > 1:
                reasons = ("duplicate_conforming_observation",)
            elif candidate_reasons:
                reasons = tuple(dict.fromkeys(candidate_reasons))
            else:
                reasons = ("missing",)
            result = CoveredMarketInput(
                requirement_id=requirement_id,
                field_name=field_name,
                status="blocked",
                reasons=reasons,
            )
        resolved[field_name] = result
    return CoveredMarketInputs(**resolved)


def derive_focal_common_close_candidates(
    candidates: Sequence[ValuationObservation],
    *,
    focal_ticker: str,
) -> tuple[date | None, tuple[str, ...]]:
    """Return the one focal close and candidate IDs priced on that exact close."""

    focal = normalize_contract_ticker(focal_ticker)
    focal_candidates = tuple(item for item in candidates if item.issuer == focal)
    if len(focal_candidates) != 1:
        return None, ()
    focal_close = focal_candidates[0].as_of_close
    return focal_close, tuple(
        sorted(
            item.observation_id
            for item in candidates
            if item.as_of_close == focal_close
        )
    )


def derive_relative_selection(
    observations: tuple[ValuationObservation, ...],
    cutoff_at: datetime,
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: tuple[FiscalPeriodFacts, ...],
    peer_policy: ValuationPeerPolicy,
    *,
    focal_ticker: str,
    valuation_requirements: tuple[ValuationRequirement, ...],
    selected_fiscal_basis: str,
) -> tuple[RelativeSelectionResolution, ...]:
    """Derive the profile-selected peer-only median for both methods."""

    policy = ValuationPeerPolicy.model_validate(peer_policy)
    verdicts = {
        item.observation_id: verdict
        for item, verdict in zip(
            observations,
            derive_valuation_observation_verdicts(
                observations,
                cutoff_at,
                market_calendar,
                fiscal_calendar,
            ),
            strict=True,
        )
    }
    coverage = {
        item.requirement_id: item
        for item in derive_valuation_coverage(
            observations,
            cutoff_at,
            market_calendar,
            fiscal_calendar,
            requirements=valuation_requirements,
            focal_ticker=focal_ticker,
            peer_tickers=policy.peer_tickers,
            minimum_covered_peer_count=policy.minimum_covered_peer_count,
        )
    }
    metric_by_method: dict[
        Literal["forward_pe", "ev_adjusted_ebitda"], str
    ] = {
        "forward_pe": "fwd_pe",
        "ev_adjusted_ebitda": "fwd_ev_adj_ebitda",
    }
    results: list[RelativeSelectionResolution] = []
    for method, metric in metric_by_method.items():
        matching_requirements = tuple(
            requirement
            for requirement in valuation_requirements
            if requirement.metric == metric
            and requirement.fiscal_basis == selected_fiscal_basis
        )
        if len(matching_requirements) != 1:
            raise ValueError(
                "profile requires exactly one selected relative-valuation requirement"
            )
        requirement_coverage = coverage[matching_requirements[0].requirement_id]
        candidates = tuple(
            item
            for item in observations
            if item.metric == metric and item.fiscal_basis == selected_fiscal_basis
        )
        base_reasons_by_id = {
            item.observation_id: tuple(
                dict.fromkeys(
                    (
                        *verdicts[item.observation_id].coverage_reasons,
                        *verdicts[item.observation_id].eligibility_reasons,
                    )
                )
            )
            for item in candidates
        }
        common_close_scope = tuple(
            item
            for item in candidates
            if set(base_reasons_by_id[item.observation_id])
            <= {"currency_nonconforming", "basis_unreconciled"}
        )
        focal_close, common_close_candidate_ids = derive_focal_common_close_candidates(
            common_close_scope,
            focal_ticker=focal_ticker,
        )
        common_close_candidate_id_set = set(common_close_candidate_ids)
        reasons_by_id = {
            item.observation_id: tuple(
                dict.fromkeys(
                    (
                        *base_reasons_by_id[item.observation_id],
                        *(
                            ("wrong_close_date",)
                            if focal_close is not None
                            and item.issuer != focal_ticker
                            and item in common_close_scope
                            and item.observation_id not in common_close_candidate_id_set
                            else ()
                        ),
                    )
                )
            )
            for item in candidates
        }
        rejected = tuple(
            RelativeSelectionRejected(
                observation_id=item.observation_id,
                reasons=reasons_by_id[item.observation_id],
            )
            for item in candidates
            if reasons_by_id[item.observation_id]
        )
        by_issuer = {
            issuer: tuple(
                item
                for item in candidates
                if item.issuer == issuer and not reasons_by_id[item.observation_id]
            )
            for issuer in (focal_ticker, *policy.peer_tickers)
        }
        eligibility_shortfall: list[RelativeSelectionEligibilityShortfall] = []
        for issuer, items in by_issuer.items():
            if len(items) > 1:
                eligibility_shortfall.append(
                    RelativeSelectionEligibilityShortfall(
                        scope_key=issuer,
                        reason="duplicate_conforming_observation",
                    )
                )
        eligible_peer_issuers = tuple(
            issuer for issuer in policy.peer_tickers if len(by_issuer[issuer]) == 1
        )
        focal_ids = tuple(item.observation_id for item in by_issuer[focal_ticker])
        if not policy.peer_tickers:
            eligibility_shortfall.append(
                RelativeSelectionEligibilityShortfall(
                    scope_key=method,
                    reason="peer_roster_empty",
                )
            )
        # A median needs one contributor, whatever minimum the policy states.
        if (
            requirement_coverage.status == "uncovered"
            or len(by_issuer[focal_ticker]) != 1
            or len(eligible_peer_issuers)
            < max(1, policy.minimum_covered_peer_count)
            or eligibility_shortfall
        ):
            if requirement_coverage.status == "covered":
                for issuer in policy.peer_tickers:
                    if not by_issuer[issuer]:
                        issuer_candidates = tuple(
                            item for item in candidates if item.issuer == issuer
                        )
                        reason: ValuationEligibilityReason = (
                            "wrong_close_date"
                            if any(
                                "wrong_close_date" in reasons_by_id[item.observation_id]
                                for item in issuer_candidates
                            )
                            else "missing"
                        )
                        eligibility_shortfall.append(
                            RelativeSelectionEligibilityShortfall(
                                scope_key=issuer,
                                reason=reason,
                            )
                        )
            results.append(
                RelativeSelectionResolution(
                    method=method,
                    fiscal_basis=selected_fiscal_basis,
                    status="blocked",
                    focal_observation_ids=focal_ids,
                    rejected=rejected,
                    shortfall=requirement_coverage.shortfall,
                    eligibility_shortfall=tuple(
                        sorted(
                            eligibility_shortfall,
                            key=lambda item: (item.scope_key, item.reason),
                        )
                    ),
                )
            )
            continue
        contributors = tuple(by_issuer[issuer][0] for issuer in eligible_peer_issuers)
        verification_classes: set[_DenominatorVerification | None] = {
            item.denominator_component_verification for item in contributors
        }
        if len(verification_classes) != 1:
            results.append(
                RelativeSelectionResolution(
                    method=method,
                    fiscal_basis=selected_fiscal_basis,
                    status="blocked",
                    focal_observation_ids=focal_ids,
                    rejected=rejected,
                    eligibility_shortfall=(
                        RelativeSelectionEligibilityShortfall(
                            scope_key=method,
                            reason="basis_unreconciled",
                        ),
                    ),
                )
            )
            continue
        denominator_verification = verification_classes.pop()
        ordered_values = sorted(item.value for item in contributors)
        midpoint = len(ordered_values) // 2
        if len(ordered_values) % 2:
            median = ordered_values[midpoint]
        else:
            median = (
                ordered_values[midpoint - 1] + ordered_values[midpoint]
            ) / Decimal("2")
        results.append(
            RelativeSelectionResolution(
                method=method,
                fiscal_basis=selected_fiscal_basis,
                status="resolved",
                selected_value=median.quantize(
                    VALUATION_MULTIPLE_QUANTUM,
                    rounding=ROUND_HALF_EVEN,
                ),
                as_of_close=focal_close,
                focal_observation_ids=focal_ids,
                contributing_ids=tuple(
                    sorted(item.observation_id for item in contributors)
                ),
                rejected=rejected,
                denominator_verification=denominator_verification,
            )
        )
    return tuple(results)


def derive_valuation_evidence_status(
    bundle: "SourceBundle",
    peer_policy: ValuationPeerPolicy | None = None,
) -> ValuationEvidenceStatus:
    from .render_state import compiler_input_content_hash

    if peer_policy is None:
        selected_basis = bundle.coverage_profile.selected_relative_fiscal_basis
        if selected_basis == "next_four_quarters":
            period_ids: tuple[str, ...] = ("next_four_quarters",)
            estimate_basis: ValuationEstimateBasis = "exact_next_four_quarters"
        else:
            period_ids = tuple(
                item.period_id
                for item in bundle.coverage_profile.valuation_period_labels
            )
            estimate_basis = "issuer_fiscal_year"
        peer_policy = ValuationPeerPolicy(
            peer_tickers=bundle.coverage_profile.peer_tickers,
            minimum_covered_peer_count=(
                bundle.coverage_profile.minimum_covered_peer_count
            ),
            metric_ids=("ev_adjusted_ebitda", "forward_pe"),
            fiscal_basis_period_ids=period_ids,
            estimate_basis=estimate_basis,
            as_of_policy="common_market_close_at_or_before_cutoff_max_five_business_days",
            aggregation="median",
            replacement_policy="no_automatic_replacements",
        )
    resolutions = derive_relative_selection(
        bundle.valuation_observations,
        bundle.cutoff_at,
        bundle.market_calendar,
        bundle.fiscal_calendar,
        peer_policy,
        focal_ticker=bundle.coverage_profile.focal_ticker,
        valuation_requirements=bundle.valuation_requirements,
        selected_fiscal_basis=(bundle.coverage_profile.selected_relative_fiscal_basis),
    )
    return ValuationEvidenceStatus(
        bundle_digest=compiler_input_content_hash(bundle),
        methods=tuple(
            ValuationMethodEvidenceStatus(
                method=item.method,
                status=item.status,
                fiscal_basis=item.fiscal_basis,
                selected_value=item.selected_value,
                as_of_close=item.as_of_close,
                contributing_ids=item.contributing_ids,
                rejected=item.rejected,
                shortfall=item.shortfall,
                eligibility_shortfall=item.eligibility_shortfall,
                denominator_verification=item.denominator_verification,
            )
            for item in resolutions
        ),
    )
