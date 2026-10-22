"""Historical population on the required-role to pinned-column join."""

from __future__ import annotations


from collections.abc import Callable, Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Literal

from .build_model_product_contracts import (
    HistoricalBasisFact,
    SourceBundle,
    historical_fact_id,
    source_measure,
)
from .build_model_product_contracts import SourceEvidence
from .build_concept_buffers import _build_statement_fact_buffer
from .build_model_items import _iter_items
from .build_reported_periods import (
    _collect_reported_period_ends,
    _refresh_period_metadata,
)
from .build_findings import make_finding
from .build_required_lines import RequiredLinePlan
from .finding_vocabulary import Finding
from .build_value_writers import (
    _set_constant_override,
    _set_imported_value,
)
from .models import FinancialModel, LineItem, ValueProvenance
from .provider_port.contracts import (
    FilingLineFact,
    FilingLineRef,
    ReportedPeriod,
)
from value_semantics_core import CONVENTIONS_IR_V1, Quantity
from .provider_port.registry import EDGAR_PROVIDER_ID, ProviderId
from .required_role_catalog import RequiredRoleUnknown, load_required_role_catalog
from schema.provider_port.adapters.edgar.wire.non_gaap_bridge_v2 import bridge_row_key

from .build_types import PopulateStats, ServedByBreakdown, SourceResolutionEntry









class QuantityConversionGap(ValueError):
    """A filing quantity cannot populate a declared workbook requirement."""

    code = "quantity_conversion_gap"

    def __init__(self, message: str) -> None:
        super().__init__(f"{self.code}: {message}")


_SCALE_FACTORS = {
    row.numeric_scale: Decimal(row.multiplier)
    for row in CONVENTIONS_IR_V1.scale_factors
}


def _target_quantity_semantics(unit: str) -> tuple[str, str | None]:
    measure = source_measure(unit)
    if measure is None:
        raise QuantityConversionGap(
            f"requirement unit {unit!r} names no source measure"
        )
    return measure.numeric_kind, measure.currency


def quantity_native_value(
    value: Quantity,
    *,
    preferred_label_negates_value: bool = False,
) -> Decimal:
    """Recover the unscaled native value for economic typing and its provenance."""
    source_factor = _SCALE_FACTORS.get(value.display.scale)
    if source_factor is None:
        raise QuantityConversionGap(f"unsupported source scale {value.display.scale!r}")
    return Decimal(value.value) * source_factor * (
        -1 if preferred_label_negates_value else 1
    )


def requirement_value_polarity(
    *,
    role_polarity: str | None,
    concept_balances: Mapping[tuple[str, str], str | None] | None = None,
    concept_key: tuple[str, str] | None = None,
) -> Literal[1, -1, "unmapped"]:
    """Map a sealed concept's XBRL balance to the catalog role's polarity."""
    balance = (
        concept_balances.get(concept_key)
        if concept_balances is not None and concept_key is not None
        else None
    )
    concept_polarity = {
        "credit": "income_positive",
        "debit": "expense_positive",
    }.get(balance) if balance is not None else None
    if concept_polarity is None or role_polarity is None:
        return "unmapped"
    return 1 if concept_polarity == role_polarity else -1


def quantity_to_requirement_value(
    value: object,
    *,
    unit: str,
    scale: str,
    statement: str | None = None,
    preferred_label_negates_value: bool = False,
    role_polarity: str | None = None,
    concept_balances: Mapping[tuple[str, str], str | None] | None = None,
    concept_key: tuple[str, str] | None = None,
) -> Decimal:
    """Convert a filing quantity to the role's economic workbook value.

    Income-statement roles map native XBRL signs through the concept's balance
    and the catalog role's declared polarity. Presentation negation is undone
    once; an unmapped concept keeps its native sign. Other statements retain
    the presented sign. Quantity is the numeric authority; provenance native
    numbers are lineage only. Untyped Decimal values are already economic,
    requirement-scaled derivations, not reoriented.
    """

    if not isinstance(value, Quantity):
        if isinstance(value, Decimal):
            return value
        raise QuantityConversionGap(
            "filing observation reached population without a Quantity"
        )
    expected_kind, expected_currency = _target_quantity_semantics(unit)
    display = value.display
    if (
        display.numeric_kind != expected_kind
        or display.currency != expected_currency
    ):
        raise QuantityConversionGap(
            "incompatible quantity semantics: "
            f"source={display.numeric_kind}/{display.currency or '-'} "
            f"target={expected_kind}/{expected_currency or '-'}"
        )
    source_scale = display.scale
    target_scale = "unit" if scale == "units" else scale
    source_factor = _SCALE_FACTORS.get(source_scale)
    target_factor = _SCALE_FACTORS.get(target_scale)
    if source_factor is None or target_factor is None:
        raise QuantityConversionGap(
            f"unsupported scale conversion {source_scale!r} -> {scale!r}"
        )
    if statement == "income_statement":
        native_value = quantity_native_value(
            value,
            preferred_label_negates_value=preferred_label_negates_value,
        ) / target_factor
        polarity = requirement_value_polarity(
            role_polarity=role_polarity,
            concept_balances=concept_balances,
            concept_key=concept_key,
        )
        return native_value * (-1 if polarity == -1 else 1)
    return Decimal(value.value) * source_factor / target_factor


def _fact_value(
    fact: FilingLineFact | HistoricalBasisFact,
) -> Quantity | Decimal:
    return fact.value


def _value_provenance(
    fact: FilingLineFact | HistoricalBasisFact,
) -> ValueProvenance:
    if fact.grade == "filing_native":
        return ValueProvenance.imported_edgar
    if fact.grade == "adapter_asserted":
        return ValueProvenance.imported_vendor
    return ValueProvenance.imported_other


def _required_role_fact_buffer(
    observations: Sequence[object],
) -> dict[tuple[str, int], HistoricalBasisFact]:
    component_signs = {
        fact_id: sign
        for fact in observations
        if isinstance(fact, HistoricalBasisFact) and fact.reconciliation is not None
        for fact_id, sign in zip(
            fact.reconciliation.component_fact_ids,
            fact.reconciliation.component_signs,
            strict=True,
        )
    }
    buffer: dict[tuple[str, int], HistoricalBasisFact] = {}
    for fact in observations:
        if (
            not isinstance(fact, HistoricalBasisFact)
            or fact.required_role_correspondence is None
        ):
            continue
        period_id = fact.period_id
        if not period_id.startswith("FY") or not period_id[2:].isdigit():
            continue
        key = (fact.required_role_correspondence, int(period_id[2:]))
        if key in buffer:
            existing = buffer[key]
            existing_key = bridge_row_key(
                existing.line.tag.removeprefix("issuer:")
            )
            fact_key = bridge_row_key(fact.line.tag.removeprefix("issuer:"))
            if (
                existing.line.statement_role == "nongaapreconciliation"
                and fact.line.statement_role == "nongaapreconciliation"
                and existing.source_id == fact.source_id
                and existing_key is not None
                and existing_key == fact_key
            ):
                buffer[key] = existing.model_copy(update={
                    "value": existing.value + component_signs.get(fact.fact_id, 1) * fact.value,
                })
                continue
            raise ValueError(
                "multiple selected filing-native facts correspond to "
                f"{key[0]} / FY{key[1]}"
            )
        if fact.fact_id in component_signs:
            buffer[key] = fact.model_copy(update={
                "value": component_signs[fact.fact_id] * fact.value,
            })
        else:
            buffer[key] = fact
    return buffer


def _targets_by_role(
    model: FinancialModel,
    plan: RequiredLinePlan,
    *,
    ticker: str,
) -> dict[str, tuple[LineItem, ...]]:
    """Resolve catalog or reviewed-window ownership onto required roles."""

    catalog = load_required_role_catalog()
    stamped: dict[str, list[LineItem]] = {}
    for item in _iter_items(model):
        required_role = item.required_role
        if required_role is not None:
            stamped.setdefault(required_role, []).append(item)

    targets: dict[str, tuple[LineItem, ...]] = {}
    for row in plan.rows:
        stamped_for_role = stamped.get(row.required_role, ())
        try:
            catalog.role(row.required_role)
        except RequiredRoleUnknown:
            window_target = tuple(
                item for item in stamped_for_role if item.id == row.model_item
            )
            if len(window_target) != 1:
                raise
            targets[row.required_role] = window_target
            continue

        by_id = {item.id: item for item in stamped_for_role}
        for item_id in catalog.population_item_ids(row.required_role, ticker=ticker):
            if item_id in model._index:
                item = model.get_item(item_id)
                by_id[item.id] = item
        if by_id:
            targets[row.required_role] = tuple(by_id.values())
    return targets


def _write_observations(
    model: FinancialModel,
    *,
    ticker: str,
    observations: Sequence[Any],
    reported_periods: Sequence[ReportedPeriod],
    required_line_plan: RequiredLinePlan | None,
    source: ProviderId | None,
    evidence_by_line: SourceEvidenceByLine,
) -> PopulateStats:

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    if required_line_plan is None:
        return PopulateStats(
            source=source,
            items_populated=0,
            items_skipped=0,
            periods_populated=0,
            missing_roles=[],
        )

    targets = _targets_by_role(model, required_line_plan, ticker=ticker)
    requested_lines = tuple(
        line
        for row in required_line_plan.rows
        for line in row.pinned_columns
    )
    buffer = _build_statement_fact_buffer(
        observations,
        requested_lines,
        historical_periods,
    )
    corresponding_facts = _required_role_fact_buffer(observations)
    set_imported_value = _set_imported_value
    set_constant_override = _set_constant_override

    missing_roles: set[str] = set()
    failed_roles: set[str] = set()
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    served_source_by_role_year: dict[str, dict[int, ProviderId]] = {}

    for row in required_line_plan.rows:
        role_targets = targets.get(row.required_role, ())
        if not role_targets:
            continue
        populated_target_ids: set[str] = set()
        for year in historical_periods:
            line = required_line_plan.pinned_line(row.required_role, year)
            corresponding_fact = corresponding_facts.get((row.required_role, year))
            observation = None if line is None else buffer.get(line)
            fact = (
                corresponding_fact
                if corresponding_fact is not None
                else None if observation is None else observation.fact
            )
            if fact is None or (
                corresponding_fact is None
                and (observation is None or observation.status != "ok")
            ):
                missing_roles.add(row.required_role)
                if observation is not None and observation.status == "failed":
                    failed_roles.add(row.required_role)
                continue

            vendor_provenance = (
                fact.provenance if isinstance(fact, FilingLineFact) else None
            )
            provider = fact.provider
            try:
                value = evidenced_requirement_value(
                    evidence_by_line, fact, unit=row.unit, scale=row.scale,
                )
            except QuantityConversionGap as exc:
                raise QuantityConversionGap(
                    f"{row.required_role}/FY{year}: {exc}"
                ) from exc
            workbook_value = float(value)
            for item in role_targets:
                # The selected filing fact is this cell's one value. The graph
                # seeds from the value cell, which also carries the filing line
                # (its lineage); a formula-backed target also takes the period
                # override so the renderer emits the fact, not the template
                # formula (as `_apply_filing_schedule_disclosures` does).
                set_imported_value(
                    item,
                    year,
                    workbook_value,
                    provenance=_value_provenance(fact),
                    vendor_provenance=vendor_provenance,
                    line_ref=fact.line,
                )
                if item.historical is not None:
                    set_constant_override(
                        item,
                        year,
                        workbook_value,
                        vendor_provenance=vendor_provenance,
                    )
                populated_target_ids.add(item.id)
                periods_populated += 1
            # Owner PopulateStats: registered ids only; absence = no served source.
            if provider is not None:
                served_source_by_role_year.setdefault(row.required_role, {})[
                    year
                ] = provider
        items_populated += len(populated_target_ids)
        items_skipped += len(role_targets) - len(populated_target_ids)

    _refresh_period_metadata(
        model,
        _collect_reported_period_ends(reported_periods, historical_periods),
    )

    served_by_role = {}
    source_resolution = []
    for row in required_line_plan.rows:
        served_by_year = served_source_by_role_year.get(row.required_role, {})
        producer_by_year = dict(sorted(served_by_year.items()))
        source_resolution.append(
            SourceResolutionEntry(
                required_role=row.required_role,
                producer_by_year=producer_by_year,
            )
        )
        if producer_by_year:
            served_by_role[row.required_role] = ServedByBreakdown(
                producer_by_year=producer_by_year,
                years_unserved=sorted(row.expected_years - set(served_by_year)),
            )
    return PopulateStats(
        source=source,
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_roles=sorted(missing_roles),
        edgar_errors=sorted(failed_roles) if source == EDGAR_PROVIDER_ID else [],
        served_source_by_role_year=served_source_by_role_year,
        source_resolution=source_resolution,
        served_by_role=served_by_role,
    )






#: The SourceBundle's evidence row of each filing line and provider: the one place
#: the converter's economic orientation (catalog role statement and polarity, the
#: sealed concept balance) was applied. Population reads it, never re-converts.
SourceEvidenceByLine = Mapping[tuple[FilingLineRef, str | None], SourceEvidence]


def source_evidence_by_line(
    evidence: Iterable[SourceEvidence],
) -> SourceEvidenceByLine:
    return {(row.line, row.provider): row for row in evidence}


def evidenced_requirement_value(
    evidence_by_line: SourceEvidenceByLine,
    fact: FilingLineFact | HistoricalBasisFact,
    *,
    unit: str,
    scale: str,
) -> Decimal:
    """A fact's economic value in the requirement's unit and scale.

    A correspondence fact is already an economic requirement-scaled Decimal. A
    filing observation takes the value of its SourceBundle evidence row, which
    ``corpus_facts.statement_source_evidence`` minted through the one converter.
    """

    if isinstance(fact, HistoricalBasisFact):
        return fact.value
    provider = None if fact.provenance is None else fact.provenance.provider
    row = evidence_by_line.get((fact.line, provider))
    if row is None:
        raise QuantityConversionGap(
            f"no SourceBundle evidence row for filing line {fact.line.tag!r}"
        )
    if (row.unit, row.scale) != (unit, scale):
        raise QuantityConversionGap(
            f"evidence unit/scale {row.unit}/{row.scale} differs from the "
            f"requirement's {unit}/{scale}"
        )
    return row.value


def populate_historicals(
    model: FinancialModel,
    source: ProviderId | None,
    ticker: str,
    *,
    statement_facts: Sequence[object],
    reported_periods: Sequence[ReportedPeriod] = (),
    required_line_plan: RequiredLinePlan | None = None,
    evidence_by_line: SourceEvidenceByLine,
) -> PopulateStats:
    """Populate provider-neutral filing observations by pinned identity."""

    return _write_observations(
        model,
        ticker=ticker,
        observations=statement_facts,
        reported_periods=reported_periods,
        required_line_plan=required_line_plan,
        source=source,
        evidence_by_line=evidence_by_line,
    )


def _selected_statement_population_inputs(
    source_bundle: SourceBundle,
) -> tuple[object, ...]:
    """Return exact pinned observations plus selected declared correspondences."""

    requirement_by_id = {
        requirement.requirement_id: requirement
        for requirement in source_bundle.requirements
        if requirement.family == "statement"
    }
    evidence_by_id = {
        evidence.evidence_id: evidence for evidence in source_bundle.evidence
    }
    fact_by_correspondence = {
        (
            fact.required_role_correspondence,
            fact.period_id,
            fact.line,
            str(fact.source_id),
        ): fact
        for fact in source_bundle.historical_basis_facts
        if fact.required_role_correspondence is not None
    }
    selected_facts: list[object] = []
    displaced_lines = set()
    for coverage in source_bundle.coverage:
        requirement = requirement_by_id.get(coverage.requirement_id)
        if requirement is None:
            continue
        pinned_by_period = {
            line.period.canonical_label(): line
            for line in requirement.pinned_columns
        }
        for period in coverage.periods:
            for evidence_id in period.evidence_ids:
                evidence = evidence_by_id.get(evidence_id)
                if evidence is None:
                    continue
                fact = fact_by_correspondence.get(
                    (
                        requirement.required_role,
                        period.period_id,
                        evidence.line,
                        str(evidence.source_id),
                    )
                )
                if fact is None:
                    if (
                        period.disposition == "derived"
                        and evidence.derivation_expression == "closure_corroborated_zero"
                        and not any(
                            observation.status == "ok"
                            and observation.line == evidence.line
                            for observation in source_bundle.statement_facts
                        )
                    ):
                        fact_id = (
                            "fact." + evidence.evidence_id[len("evidence."):]
                            if evidence.evidence_id.startswith("evidence.")
                            else evidence.evidence_id
                            if evidence.evidence_id.startswith("fact.")
                            else historical_fact_id(
                                evidence.line, period.period_id, "closure_corroborated_zero"
                            )
                        )
                        fact = HistoricalBasisFact(
                            fact_id=fact_id,
                            line=evidence.line,
                            grade=evidence.grade,
                            period_id=period.period_id,
                            value=evidence.value,
                            unit=evidence.unit,
                            scale=evidence.scale,
                            sign=evidence.sign,
                            source_id=evidence.source_id,
                            provider=evidence.provider,
                            retrieved_at=evidence.retrieved_at,
                            available_at=evidence.available_at,
                            as_of_date=evidence.as_of_date,
                            locator=evidence.locator,
                            fiscal_basis=evidence.fiscal_basis,
                            period_length_months=evidence.period_length_months,
                            accounting_definition=evidence.accounting_definition,
                            confidence=evidence.confidence,
                            source_quality=evidence.source_quality,
                            required_role_correspondence=requirement.required_role,
                        )
                        selected_facts.append(fact)
                        pinned_line = pinned_by_period.get(period.period_id)
                        if pinned_line is not None:
                            displaced_lines.add(pinned_line)
                    continue
                selected_facts.append(fact)
                pinned_line = pinned_by_period.get(period.period_id)
                if pinned_line is not None:
                    displaced_lines.add(pinned_line)
    return (
        *(
            observation
            for observation in source_bundle.statement_facts
            if observation.line not in displaced_lines
        ),
        *selected_facts,
    )


class StatementPopulationSourceAmbiguous(ValueError):
    """The selected statement facts name more than one provider."""

    code = "statement_population_source_ambiguous"

    def __init__(self, providers: Iterable[str]) -> None:
        super().__init__(
            f"{self.code}: selected statement facts declare providers "
            f"{sorted(providers)!r}"
        )


def _statement_population_source(
    observations: Sequence[object],
) -> tuple[ProviderId | None, tuple[Finding, ...]]:
    """Derive the compile source stamp from the selected facts alone.

    One provider is the stamp. None (a pin that selects no statement fact
    yet) is no provider, recorded as one ``statement_population_source_absent``
    Finding naming the statement binding that supplies one. The bundle's
    declared source records are never the answer: they name every provider
    the corpus read, not the facts the pin selected.
    """

    providers: set[ProviderId] = set()
    for observation in observations:
        fact = getattr(observation, "fact", observation)
        provider = getattr(fact, "provider", None) if fact is not None else None
        if provider is None:
            provider = getattr(observation, "provider", None)
        if provider is not None:
            providers.add(provider)
    if len(providers) > 1:
        raise StatementPopulationSourceAmbiguous(providers)
    if providers:
        return providers.pop(), ()
    return None, (
        make_finding(
            "statement_population_source_absent",
            message=(
                "The pin selects no statement fact, so historical population "
                "has no provider. Read each filing through corpus_state "
                "(research_file_id, accession) and persist a statement binding "
                "for its window through fms_persist_correspondence; the facts "
                "it selects supply the provider."
            ),
            observed={
                "selected_statement_facts": len(observations),
                "next_call": "corpus_state",
                "repair_door": "fms_persist_correspondence",
            },
        ),
    )


__all__ = [
    "QuantityConversionGap",
    "StatementPopulationSourceAmbiguous",
    "populate_historicals",
    "quantity_to_requirement_value",
    "quantity_native_value",
    "requirement_value_polarity",
]
