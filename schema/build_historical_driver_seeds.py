"""Apply accepted historical-foundation driver seeds to compiled model state.

An ``opening`` seed sets the terminal historical level of a growth node. A
direct segment-revenue node also replays its filed history across the historical
window, regardless of its forecast driver or seed scope. Cited BusinessModel
observations supply historical cells not covered by the HFF series.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterable, Mapping, NoReturn

from .business_model import (
    DriverNode,
    SumParams,
    business_model_observation_claim_key,
    node_measure,
)
from .business_model_revision import BusinessModelRevision
from .build_model_product_contracts import (
    ModelSpec,
    ModelSpecHistoricalDriverSeedProjection,
    ModelSpecHistoricalFoundationObservation,
)
from .coverage_profile import SUPPORTED_COVERAGE_DISPOSITIONS
from .historical_driver_seeds import (
    HistoricalDriverSeed,
    HistoricalDriverSeedRequirement,
)
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItemRef,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from industry_slice_core.industry_types import Unit

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry


class HistoricalDriverSeedCompileError(ValueError):
    """The compiler cannot safely apply the exact accepted-HFF seed projection."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> NoReturn:
    raise HistoricalDriverSeedCompileError(code, message)


def _iter_nodes(nodes: list[DriverNode]):
    for node in nodes:
        yield node
        yield from _iter_nodes(node.children or [])


def _nodes_by_key(revision: BusinessModelRevision) -> dict[str, DriverNode]:
    return {
        f"{segment.id}.{node.id}": node
        for segment in revision.business_model.segments
        for node in _iter_nodes(segment.revenue_model.decomposition)
    }


def _has_required_self_growth_node(revision: BusinessModelRevision) -> bool:
    nodes = _nodes_by_key(revision)
    for entry in revision.driver_assumption_plan.entries:
        if not entry.base_case_required or entry.compile_target_type != "assumption_row":
            continue
        node = nodes.get(f"{entry.segment_id}.{entry.driver_node_id}")
        expression = None if node is None else node.driver
        if (
            expression is not None
            and expression.type == "growth"
            and expression.params.base.node_id == "self"
            and expression.params.base.t == -1
        ):
            return True
    return False


def _formula_node_ids(expression: Any) -> tuple[str, ...]:
    if expression is None:
        return ()
    params = expression.params
    refs: list[Any] = []
    if expression.type in {"product", "sum"}:
        refs.extend(params.operands)
    elif expression.type == "derived":
        refs.extend((params.numerator, params.denominator))
    elif expression.type == "roll_forward":
        refs.append(params.beginning)
        refs.extend(params.additions)
        refs.extend(params.subtractions)
    return tuple(ref.node_id for ref in refs if ref.node_id != "self")


def validate_historical_driver_seed_revenue_topology(
    revision: BusinessModelRevision,
) -> bool:
    """Validate per-unit revenue scope and single-count structural topology.

    Returns whether the accepted BMR contains a per-unit revenue branch.  The
    determination uses canonical node measures: a dollar output product of one
    count operand and one per-count price/yield operand. It does not depend on
    ticker strings or node-name conventions.
    """

    multi_stream = len(revision.business_model.segments) > 1
    found_per_unit_branch = False
    for segment in revision.business_model.segments:
        nodes = {
            node.id: node
            for node in _iter_nodes(segment.revenue_model.decomposition)
        }
        formulas = [segment.revenue_model.consolidation_formula]
        formulas.extend(
            expression
            for node in nodes.values()
            for expression in (node.driver, node.children_formula)
            if expression is not None
        )
        for expression in formulas:
            node_ids = _formula_node_ids(expression)
            if len(node_ids) != len(set(node_ids)):
                _fail(
                    "historical_driver_seed_revenue_branch_duplicate",
                    f"segment {segment.id!r} formula counts one revenue branch more than once",
                )
        for output in nodes.values():
            expression = output.driver or output.children_formula
            if (
                expression is None
                or expression.type != "product"
                or output.unit is not Unit.dollars
            ):
                continue
            operands = tuple(nodes.get(node_id) for node_id in _formula_node_ids(expression))
            if any(node is None for node in operands):
                continue
            count_operands = tuple(
                node for node in operands if node is not None and node.unit is Unit.item_count
            )
            price_operands = tuple(
                node
                for node in operands
                if node is not None
                and node_measure(node).unit is Unit.per_count
                and "price" in node.factors
            )
            if len(count_operands) != 1 or len(price_operands) != 1:
                continue
            found_per_unit_branch = True
            price = price_operands[0]
            if multi_stream and price.revenue_scope != "own_revenue_stream_only":
                _fail(
                    "historical_driver_seed_rpc_revenue_scope_invalid",
                    f"multi-stream per-unit price driver {segment.id}.{price.id!s} "
                    "must be scoped to its own revenue stream",
                )
    return found_per_unit_branch


def _validate_seed_derivation_periods(
    requirement: HistoricalDriverSeedRequirement,
    seed: HistoricalDriverSeed,
) -> None:
    if requirement.observation_basis == "period_average":
        if seed.derivation_kind == "direct":
            if seed.derivation_operands[0].period_id != requirement.period_id:
                _fail(
                    "historical_driver_seed_average_period_invalid",
                    f"direct average seed {seed.driver_key!r} is not terminal-period evidence",
                )
            return
        if seed.derivation_kind != "arithmetic_mean":
            _fail(
                "historical_driver_seed_average_derivation_invalid",
                f"average seed {seed.driver_key!r} must be direct or adjacent-period mean",
            )
        periods = tuple(row.period_id for row in seed.derivation_operands)
        if periods != (
            requirement.predecessor_period_id,
            requirement.period_id,
        ):
            _fail(
                "historical_driver_seed_average_periods_invalid",
                f"average seed {seed.driver_key!r} does not use the exact adjacent periods",
            )
    elif seed.derivation_kind == "arithmetic_mean":
        _fail(
            "historical_driver_seed_flow_derivation_invalid",
            f"non-average seed {seed.driver_key!r} cannot use an arithmetic mean",
        )


def _line_item_ref(value: Any) -> LineItemRef | None:
    if isinstance(value, LineItemRef):
        return value
    if isinstance(value, dict) and isinstance(value.get("id"), str):
        return LineItemRef.model_validate(value)
    return None


def _validate_compiled_growth_target(
    *,
    item: Any,
    natural_key: str,
    expected_unit: Unit,
    expected_scale: str,
) -> str:
    if (
        item.item_type is not ItemType.input
        or item.unit is not expected_unit
        or item.model_scale != expected_scale
        or item.projected is None
        or item.projected.type is not FormulaType.growth
    ):
        _fail(
            "historical_driver_seed_compiler_target_invalid",
            f"compiled target for {natural_key!r} is not the exact typed growth level",
        )
    base = _line_item_ref(item.projected.params.get("base"))
    rate = _line_item_ref(item.projected.params.get("rate"))
    if (
        base is None
        or base.id != item.id
        or base.t != -1
        or rate is None
        or rate.t != 0
    ):
        _fail(
            "historical_driver_seed_compiler_formula_invalid",
            f"compiled target for {natural_key!r} is not an exact self[-1] growth formula",
        )
    return rate.id


def _coverage_row(
    *,
    projection: ModelSpecHistoricalDriverSeedProjection,
    requirement: HistoricalDriverSeedRequirement,
    seed: HistoricalDriverSeed,
    item_id: str,
    period: int,
    legacy_anchor_reconciled: bool,
) -> dict[str, object]:
    return {
        "source_kind": "historical_driver_seed",
        "historical_foundation_artifact_id": (
            projection.historical_foundation_artifact_id
        ),
        "historical_foundation_content_sha256": (
            projection.historical_foundation_content_sha256
        ),
        "historical_driver_seed_denominator_sha256": projection.denominator_sha256,
        "driver_id": seed.driver_id,
        "driver_key": seed.driver_key,
        "resolved_item_id": item_id,
        "periods": [period],
        "period_id": seed.period_id,
        "unit": seed.unit.value,
        "model_scale": seed.model_scale,
        "observation_basis": requirement.observation_basis,
        "value": str(seed.value),
        "disposition": seed.disposition,
        "derivation_kind": seed.derivation_kind,
        "derivation_rationale": seed.derivation_rationale,
        "confidence": seed.confidence,
        "limitations": list(seed.limitations),
        "evidence_ids": list(seed.evidence_ids),
        "supporting_evidence_ids": list(seed.supporting_evidence_ids),
        "evidence_value_ids": list(seed.evidence_value_ids),
        "fact_ids": list(seed.fact_ids),
        "legacy_anchor_reconciled": legacy_anchor_reconciled,
    }


def segment_revenue_series(
    seeds: Iterable[HistoricalDriverSeed],
    requirements: Iterable[HistoricalDriverSeedRequirement],
    requirement_by_fact_id: Mapping[str, str],
) -> dict[str, tuple[tuple[Decimal, str], ...]]:
    """Each segment-revenue seed's series: (coefficient, requirement_id) terms.

    The one rule for which requirements a seed's history replays.  A seed
    whose every operand is a fact of a requirement names a series; one with a
    literal, BusinessModel-evidence or member-revenue operand names only its
    own period and is absent here.  The HFF fact projection
    (`api/research/model_spec_historical_foundation_facts.py`) carries exactly
    these requirements beyond the role catalog's plane, and the HFF fact pass
    leaves exactly these observations to the driver-seed pass; each side
    supplies its own fact-to-requirement resolution.
    """

    scopes = {row.driver_key: row.history_scope for row in requirements}
    series: dict[str, tuple[tuple[Decimal, str], ...]] = {}
    for seed in seeds:
        scope = scopes.get(seed.driver_key)
        if (
            scope not in {"segment_revenue", "opening"}
            or seed.unit is not Unit.dollars
            or not all(
                operand.observation_basis == "period_flow"
                and operand.fact_id is not None
                and operand.fact_id in requirement_by_fact_id
                for operand in seed.derivation_operands
            )
        ):
            continue
        if scope == "opening" and not any(
            requirement_by_fact_id[operand.fact_id].endswith(".statement.is.revenue")
            or operand.coefficient is not None
            for operand in seed.derivation_operands
            if operand.fact_id is not None
        ):
            continue
        terms = tuple(
            (
                Decimal(1) if operand.coefficient is None else operand.coefficient,
                requirement_by_fact_id[operand.fact_id],
            )
            for operand in seed.derivation_operands
            if operand.fact_id is not None and operand.fact_id in requirement_by_fact_id
        )
        if len(terms) == len(seed.derivation_operands):
            series[seed.driver_key] = terms
    return series


def model_spec_segment_revenue_series(
    model_spec: ModelSpec,
) -> dict[str, tuple[tuple[Decimal, str], ...]]:
    """`segment_revenue_series` over a ModelSpec's projected observations."""

    seeds = model_spec.historical_driver_seed_projection
    facts = model_spec.historical_foundation_fact_projection
    if seeds is None or facts is None:
        return {}
    return segment_revenue_series(
        seeds.seeds,
        seeds.requirements,
        {row.fact.fact_id: row.requirement_id for row in facts.observations},
    )


def _filing_line_observation_id(line: Any) -> str:
    period = getattr(line, "period", None)
    canonical_label = getattr(period, "canonical_label", None)
    label = (
        str(canonical_label())
        if callable(canonical_label)
        else str(period or "")
    )
    raw_dims: tuple[Any, ...] = tuple(getattr(line, "dimensions", None) or ())
    dims = ";".join(
        f"{dim.axis_qname}={dim.member_qname}"
        for dim in raw_dims
    )
    return f"filing-line:{line.accession}:{label}:{line.statement_role}:{line.tag}:{dims}"


def hff_segment_revenue_series_values(
    hff: Any, seed: Any, historical_years: Iterable[int]
) -> dict[int, tuple[Decimal, tuple[str, ...], str]]:
    """Replay a segment-revenue seed's series over an HFF's earlier years.

    Returns ``{year: (value, fact_ids, disposition)}`` for each historical
    year other than the seed's own period where every term's requirement is
    supported at that period and the pinned line carries an HFF fact, or
    where the HFF carries the driver's member-reported segment revenue fact.
    A member fact is ``observed``; in a single-segment year (an explicit
    ``AllOtherSegmentsMember`` fact reported as 0, and no other sibling) the
    supported consolidated revenue fact is the primary segment's value by
    identity, so it is ``derived`` with the consolidated fact as its input.
    A year with no fact of any kind is never written.
    """
    if hff is None or seed is None:
        return {}
    seeds: tuple[Any, ...] = tuple(getattr(hff, "historical_driver_seeds", ()) or ())
    seed_requirements: tuple[Any, ...] = tuple(
        getattr(hff, "historical_driver_seed_requirements", ()) or ()
    )
    if not seeds or not seed_requirements:
        return {}
    driver_key = str(getattr(seed, "driver_key", "") or "")
    if not driver_key or not any(
        getattr(row, "driver_key", None) == driver_key
        for row in seed_requirements
    ):
        return {}
    requirements: tuple[Any, ...] = tuple(getattr(hff, "requirement_denominator", ()) or ())
    facts: tuple[Any, ...] = tuple(getattr(hff, "historical_basis_facts", ()) or ())
    requirement_by_line: dict[Any, str] = {}
    requirement_by_id: dict[str, Any] = {}
    for requirement in requirements:
        requirement_id = getattr(requirement, "requirement_id", None)
        if requirement_id is None:
            continue
        requirement_by_id[str(requirement_id)] = requirement
        for line in getattr(requirement, "pinned_columns", ()) or ():
            requirement_by_line[line] = str(requirement_id)
    requirement_by_fact_id: dict[str, str] = {
        str(fact.fact_id): requirement_by_line[fact.line]
        for fact in facts
        if getattr(fact, "line", None) in requirement_by_line
        and getattr(fact, "fact_id", None) is not None
    }
    series_by_key = segment_revenue_series(
        seeds, seed_requirements, requirement_by_fact_id
    )
    terms = series_by_key.get(driver_key)
    coverage_by_id = {
        getattr(row, "requirement_id", None): row
        for row in (getattr(hff, "coverage", ()) or ())
    }
    fact_by_line_period: dict[tuple[Any, str], Any] = {}
    for fact in sorted(facts, key=lambda row: str(getattr(row, "fact_id", ""))):
        fact_by_line_period.setdefault((fact.line, fact.period_id), fact)
    if not terms:
        operands = tuple(getattr(seed, "derivation_operands", ()) or ())
        if (
            getattr(seed, "derivation_kind", None) != "direct"
            or len(operands) != 1
            or getattr(operands[0], "kind", None) != "historical_fact"
        ):
            return {}
        facts_by_id = {
            str(fact.fact_id): fact
            for fact in facts
            if getattr(fact, "fact_id", None) is not None
        }
        terminal_fact = facts_by_id.get(str(getattr(operands[0], "fact_id", "") or ""))
        terminal_dims: tuple[Any, ...] = tuple(
            getattr(getattr(terminal_fact, "line", None), "dimensions", None) or ()
        )
        if terminal_fact is None or not terminal_dims:
            return {}
        terminal_def = str(getattr(terminal_fact, "accounting_definition", "") or "")
        member_prefix = (
            "Exact member-reported segment revenue bound by accepted BusinessModel driver "
        )
        driver_marker = f"driver {driver_key};"
        if member_prefix not in terminal_def or driver_marker not in terminal_def:
            return {}
        member_fact_by_period: dict[str, Any] = {}
        other_driver_facts_by_period: dict[str, list[Any]] = {}
        other_all_other_drivers: set[str] = set()
        other_regular_drivers: set[str] = set()
        for fact in sorted(facts, key=lambda row: str(getattr(row, "fact_id", ""))):
            definition = str(getattr(fact, "accounting_definition", "") or "")
            f_period = str(getattr(fact, "period_id", "") or "")
            if driver_marker in definition:
                member_fact_by_period.setdefault(f_period, fact)
            elif member_prefix in definition:
                other_driver_facts_by_period.setdefault(f_period, []).append(fact)
                after = definition.split(member_prefix, 1)[1]
                other_dkey = after.split(";", 1)[0].strip()
                f_dims = tuple(
                    getattr(getattr(fact, "line", None), "dimensions", None) or ()
                )
                if any(
                    str(getattr(dim, "member_qname", "")).endswith(
                        ":AllOtherSegmentsMember"
                    )
                    for dim in f_dims
                ):
                    other_all_other_drivers.add(other_dkey)
                else:
                    other_regular_drivers.add(other_dkey)
        is_all_other = any(
            str(getattr(dim, "member_qname", "")).endswith(":AllOtherSegmentsMember")
            for dim in terminal_dims
        )

        revenue_req = next(
            (
                req
                for req in requirements
                if getattr(req, "required_role", None) == "statement.is.revenue"
            ),
            None,
        )
        revenue_cov = (
            None
            if revenue_req is None
            else coverage_by_id.get(getattr(revenue_req, "requirement_id", None))
        )
        result_member: dict[int, tuple[Decimal, tuple[str, ...], str]] = {}
        for year in historical_years:
            period_id = f"FY{int(year)}"
            if period_id == getattr(seed, "period_id", None):
                continue
            matched_fact = member_fact_by_period.get(period_id)
            derived_fact = None
            other_facts = tuple(other_driver_facts_by_period.get(period_id, ()))
            single_segment_year = bool(
                other_all_other_drivers
                and not other_regular_drivers
                and bool(other_facts)
                and all(
                    getattr(other, "value", None) == Decimal(0)
                    for other in other_facts
                )
            )
            if (
                matched_fact is None
                and not is_all_other
                and single_segment_year
                and revenue_req is not None
                and revenue_cov is not None
            ):
                period_cov = next(
                    (
                        row
                        for row in (getattr(revenue_cov, "periods", ()) or ())
                        if getattr(row, "period_id", None) == period_id
                    ),
                    None,
                )
                if (
                    period_cov is not None
                    and getattr(period_cov, "disposition", None)
                    in SUPPORTED_COVERAGE_DISPOSITIONS
                ):
                    pinned_line = next(
                        (
                            line
                            for line in (getattr(revenue_req, "pinned_columns", ()) or ())
                            if line.period.canonical_label() == period_id
                        ),
                        None,
                    )
                    if pinned_line is not None:
                        matched_fact = fact_by_line_period.get((pinned_line, period_id))
                        derived_fact = matched_fact
            if matched_fact is None:
                continue
            if matched_fact is derived_fact:
                result_member[int(year)] = (
                    matched_fact.value,
                    (str(matched_fact.fact_id),),
                    "derived",
                )
                continue
            obs_id = _filing_line_observation_id(matched_fact.line)
            result_member[int(year)] = (
                matched_fact.value,
                (str(matched_fact.fact_id), obs_id),
                "observed",
            )
        return result_member
    result: dict[int, tuple[Decimal, tuple[str, ...], str]] = {}
    for year in historical_years:
        period_id = f"FY{int(year)}"
        if period_id == getattr(seed, "period_id", None):
            continue
        total = Decimal(0)
        fact_ids: list[str] = []
        missing = False
        for coefficient, requirement_id in terms:
            requirement = requirement_by_id.get(requirement_id)
            coverage = coverage_by_id.get(requirement_id)
            if requirement is None or coverage is None:
                missing = True
                break
            period_coverage = next(
                (
                    row
                    for row in (getattr(coverage, "periods", ()) or ())
                    if getattr(row, "period_id", None) == period_id
                ),
                None,
            )
            if (
                period_coverage is None
                or getattr(period_coverage, "disposition", None)
                not in SUPPORTED_COVERAGE_DISPOSITIONS
            ):
                missing = True
                break
            pinned_line = next(
                (
                    line
                    for line in (getattr(requirement, "pinned_columns", ()) or ())
                    if line.period.canonical_label() == period_id
                ),
                None,
            )
            fact = (
                None
                if pinned_line is None
                else fact_by_line_period.get((pinned_line, period_id))
            )
            if fact is None:
                missing = True
                break
            total += coefficient * fact.value
            fact_ids.append(fact.fact_id)
        if missing:
            continue
        result[int(year)] = (total, tuple(sorted(fact_ids)), "observed")
    return result

def _direct_segment_revenue_item_id(
    revision: BusinessModelRevision,
    compiled_registry: "CompiledDriverRegistry",
    driver_key: str,
) -> str | None:
    """The income-statement segment line a one-node segment consolidation feeds."""

    segment_id, _, node_id = driver_key.partition(".")
    for segment in revision.business_model.segments:
        if segment.id != segment_id or segment.id not in compiled_registry.segment_mapping:
            continue
        params = segment.revenue_model.consolidation_formula.params
        if not isinstance(params, SumParams):
            continue
        operands = tuple(params.operands)
        if (
            len(operands) == 1
            and operands[0].node_id == node_id
            and operands[0].t == 0
            and operands[0].sign == 1
        ):
            return (
                "tpl.fm.income_statement."
                f"business_segment_{compiled_registry.segment_mapping[segment.id]}_revenue"
            )
    return None


def _extend_segment_revenue_periods(
    model: FinancialModel, segment_id: str, seeded_years: set[int]
) -> None:
    revenue = model._index.get(f"bm.{segment_id}.__rev")
    if revenue is not None and revenue.formula_periods is not None:
        revenue.formula_periods = sorted(set(revenue.formula_periods) | seeded_years)
    growth = model._index.get(f"bm.{segment_id}.__growth")
    if growth is not None and growth.formula_periods is not None:
        growth.formula_periods = sorted(
            set(growth.formula_periods)
            | {year for year in seeded_years if year - 1 in seeded_years}
        )


def _apply_segment_revenue_seed(
    model: FinancialModel,
    *,
    projection: ModelSpecHistoricalDriverSeedProjection,
    requirement: HistoricalDriverSeedRequirement,
    seed: HistoricalDriverSeed,
    revision: BusinessModelRevision,
    compiled_registry: "CompiledDriverRegistry",
    fiscal_years: dict[str, int],
    series: tuple[tuple[Decimal, str], ...] | None,
    observations: dict[tuple[str, str], ModelSpecHistoricalFoundationObservation],
    resolved_cells: set[tuple[str, int]],
) -> list[dict[str, object]]:
    item_id = compiled_registry.node_items.get(seed.driver_key)
    if item_id is None:
        _fail(
            "historical_driver_seed_compiler_binding_missing",
            f"compiled registry lacks natural seed key {seed.driver_key!r}",
        )
    target_ids = [item_id]
    segment_item_id = _direct_segment_revenue_item_id(
        revision, compiled_registry, seed.driver_key
    )
    if segment_item_id is not None and segment_item_id in model._index:
        target_ids.append(segment_item_id)
    targets = [model.get_item(target_id) for target_id in target_ids]
    for target in targets:
        if target.unit is not seed.unit or target.model_scale != seed.model_scale:
            _fail(
                "historical_driver_seed_compiler_target_invalid",
                f"compiled target {target.id!r} for {seed.driver_key!r} differs "
                "in unit or scale",
            )

    values: dict[str, tuple[Decimal, tuple[str, ...]]] = {
        seed.period_id: (seed.value, seed.fact_ids)
    }
    observation_evidence: dict[str, tuple[str, ...]] = {}
    for period_id in fiscal_years:
        if series is None or period_id == seed.period_id:
            continue
        period_observations = [
            observations.get((requirement_id, period_id))
            for _, requirement_id in series
        ]
        if any(row is None for row in period_observations):
            continue  # the filed series has no supported value this period
        values[period_id] = (
            sum(
                (
                    coefficient * row.fact.value
                    for (coefficient, _), row in zip(series, period_observations)
                    if row is not None
                ),
                Decimal(0),
            ),
            tuple(
                sorted(row.fact.fact_id for row in period_observations if row is not None)
            ),
        )
    if revision.stage_metadata is not None:
        evidence_by_claim = {
            binding.claim_key: binding.evidence_ids
            for binding in revision.stage_metadata.claim_evidence_bindings
        }
        node = _nodes_by_key(revision).get(seed.driver_key)
        for observation in (() if node is None else node.historical_observations):
            evidence_ids = evidence_by_claim.get(
                business_model_observation_claim_key(
                    seed.driver_key, observation.period_id
                )
            )
            if (
                observation.period_id in fiscal_years
                and observation.period_id not in values
                and evidence_ids
            ):
                values[observation.period_id] = (
                    observation.value, tuple(evidence_ids)
                )
                observation_evidence[observation.period_id] = tuple(evidence_ids)

    note = (
        "accepted_historical_foundation_segment_revenue:"
        f"{projection.historical_foundation_artifact_id}:{seed.driver_id}"
    )
    coverage: list[dict[str, object]] = []
    for period_id, (value, fact_ids) in sorted(values.items()):
        year = fiscal_years[period_id]
        for target in targets:
            cell_key = (target.id, year)
            if cell_key in resolved_cells:
                _fail(
                    "historical_driver_seed_compiler_binding_duplicate",
                    f"multiple seeds resolve to {target.id!r}/FY{year}",
                )
            resolved_cells.add(cell_key)
            existing = None if target.values is None else target.values.values.get(year)
            if existing is not None and (
                existing.value is None or Decimal(str(existing.value)) != value
            ):
                _fail(
                    "historical_driver_seed_legacy_anchor_conflict",
                    f"prior value of {target.id!r}/FY{year} conflicts with exact "
                    f"HFF segment revenue seed {seed.driver_key!r}",
                )
            if target.values is None:
                target.values = ValueSeries()
            target.values.values[year] = ValueCell(
                period=year,
                value=float(value),
                provenance=ValueProvenance.historical_foundation,
                note=note,
            )
            if target.overrides is None:
                target.overrides = {}
            target.overrides[year] = FormulaSpec(
                type=FormulaType.constant,
                params={"value": float(value)},
                note=note,
            )
            row = _coverage_row(
                projection=projection,
                requirement=requirement,
                seed=seed,
                item_id=target.id,
                period=year,
                legacy_anchor_reconciled=existing is not None,
            )
            row.update(
                {"period_id": period_id, "value": str(value), "fact_ids": list(fact_ids)}
            )
            if period_id in observation_evidence:
                row.update({
                    "fact_ids": [],
                    "evidence_ids": list(observation_evidence[period_id]),
                    "evidence_value_ids": [],
                    "supporting_evidence_ids": [],
                    "disposition": "observed",
                    "derivation_kind": "direct",
                    "derivation_rationale": "Cited BusinessModel historical observation.",
                })
            coverage.append(row)
    if segment_item_id is not None:
        segment_id, _, _ = seed.driver_key.partition(".")
        seeded_years = {fiscal_years[period_id] for period_id in values}
        _extend_segment_revenue_periods(model, segment_id, seeded_years)
    return coverage


def apply_model_spec_historical_driver_seeds(
    model: FinancialModel,
    model_spec: ModelSpec,
    *,
    business_model_revision: BusinessModelRevision,
    compiled_registry: "CompiledDriverRegistry",
) -> list[dict[str, object]]:
    """Seed exact historical cells of BMR nodes and return lineage coverage.

    The projection contains no workbook IDs.  Natural BMR node keys are resolved
    only after the exact revision has been compiled.  Existing legacy anchors may
    agree byte-for-value, but an authority conflict is always fatal.
    """

    spec = ModelSpec.model_validate(model_spec)
    revision = BusinessModelRevision.model_validate(business_model_revision)
    projection = spec.historical_driver_seed_projection
    validate_historical_driver_seed_revenue_topology(revision)
    if projection is None:
        if revision.stage_receipt is not None and _has_required_self_growth_node(revision):
            _fail(
                "historical_driver_seed_rebuild_required",
                "accepted BusinessModel self-growth nodes require an exact HFF seed projection",
            )
        return []

    period_by_id = {
        period.period_id: period
        for period in spec.fiscal_axis.historical_periods
    }
    requirements = {row.driver_key: row for row in projection.requirements}
    nodes = _nodes_by_key(revision)
    plan_entries = {
        f"{entry.segment_id}.{entry.driver_node_id}": entry
        for entry in revision.driver_assumption_plan.entries
    }
    compiled_years = set(model.time_structure.historical_periods)
    fiscal_years = {
        period.period_id: period.fiscal_year
        for period in spec.fiscal_axis.historical_periods
        if period.period_type == "annual"
        and not period.is_estimate
        and period.fiscal_year in compiled_years
    }
    series_by_key = model_spec_segment_revenue_series(spec)
    observations = {
        (row.requirement_id, row.period_id): row
        for row in (
            ()
            if spec.historical_foundation_fact_projection is None
            else spec.historical_foundation_fact_projection.observations
        )
    }
    coverage: list[dict[str, object]] = []
    resolved_cells: set[tuple[str, int]] = set()
    for seed in projection.seeds:
        requirement = requirements[seed.driver_key]
        node = nodes.get(seed.driver_key)
        entry = plan_entries.get(seed.driver_key)
        fiscal_period = period_by_id.get(seed.period_id)
        if requirement.history_scope == "segment_revenue" or (
            _direct_segment_revenue_item_id(
                revision, compiled_registry, seed.driver_key
            ) is not None
            and node is not None
            and node.unit is Unit.dollars
            and requirement.observation_basis == "period_flow"
        ):
            if (
                node is None
                or entry is None
                or entry.driver_id != seed.driver_id
                or node.unit is not requirement.unit
                or seed.period_id not in fiscal_years
            ):
                _fail(
                    "historical_driver_seed_business_model_mismatch",
                    f"seed {seed.driver_key!r} differs from the exact accepted "
                    "BMR segment revenue node",
                )
            coverage.extend(
                _apply_segment_revenue_seed(
                    model,
                    projection=projection,
                    requirement=requirement,
                    seed=seed,
                    revision=revision,
                    compiled_registry=compiled_registry,
                    fiscal_years=fiscal_years,
                    series=series_by_key.get(seed.driver_key),
                    observations=observations,
                    resolved_cells=resolved_cells,
                )
            )
            continue
        expression = None if node is None else node.driver
        if (
            node is None
            or entry is None
            or entry.driver_id != seed.driver_id
            or not entry.base_case_required
            or entry.compile_target_type != "assumption_row"
            or expression is None
            or expression.type != "growth"
            or expression.params.base.node_id != "self"
            or expression.params.base.t != -1
            or node.observation_basis != requirement.observation_basis
            or node.unit is not requirement.unit
            or node_measure(node).model_scale != requirement.model_scale
        ):
            _fail(
                "historical_driver_seed_business_model_mismatch",
                f"seed {seed.driver_key!r} differs from the exact accepted BMR growth node",
            )
        if fiscal_period is None or fiscal_period.fiscal_year not in set(
            model.time_structure.historical_periods
        ):
            _fail(
                "historical_driver_seed_period_mismatch",
                f"seed {seed.driver_key!r} does not resolve to a compiled historical period",
            )
        _validate_seed_derivation_periods(requirement, seed)

        item_id = compiled_registry.node_items.get(seed.driver_key)
        if item_id is None:
            _fail(
                "historical_driver_seed_compiler_binding_missing",
                f"compiled registry lacks natural seed key {seed.driver_key!r}",
            )
        item = model.get_item(item_id)
        measure = node_measure(node)
        _validate_compiled_growth_target(
            item=item,
            natural_key=seed.driver_key,
            expected_unit=measure.unit,
            expected_scale=measure.model_scale,
        )
        cell_key = (item_id, fiscal_period.fiscal_year)
        if cell_key in resolved_cells:
            _fail(
                "historical_driver_seed_compiler_binding_duplicate",
                f"multiple seeds resolve to {item_id!r}/FY{fiscal_period.fiscal_year}",
            )
        resolved_cells.add(cell_key)

        existing = None if item.values is None else item.values.values.get(
            fiscal_period.fiscal_year
        )
        reconciled = existing is not None
        if existing is not None and (
            existing.value is None
            or Decimal(str(existing.value)) != seed.value
        ):
            _fail(
                "historical_driver_seed_legacy_anchor_conflict",
                f"legacy anchor conflicts with exact HFF seed {seed.driver_key!r}",
            )
        if item.values is None:
            item.values = ValueSeries()
        item.values.values[fiscal_period.fiscal_year] = ValueCell(
            period=fiscal_period.fiscal_year,
            value=float(seed.value),
            provenance=ValueProvenance.historical_foundation,
            note=(
                "accepted_historical_foundation_driver_seed:"
                f"{projection.historical_foundation_artifact_id}:"
                f"{seed.driver_id}"
            ),
        )
        # The accepted HFF seed owns this exact historical opening cell even
        # when the generic BusinessModel compiler installed a historical
        # fallback formula. Materialize an exact-period constant override so
        # forced typed recomputation validates the accepted seed rather than
        # recursively requiring the superseded fallback source.
        if item.overrides is None:
            item.overrides = {}
        item.overrides[fiscal_period.fiscal_year] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": float(seed.value)},
            note=(
                "accepted_historical_foundation_driver_seed:"
                f"{projection.historical_foundation_artifact_id}:"
                f"{seed.driver_id}"
            ),
        )
        coverage.append(
            _coverage_row(
                projection=projection,
                requirement=requirement,
                seed=seed,
                item_id=item_id,
                period=fiscal_period.fiscal_year,
                legacy_anchor_reconciled=reconciled,
            )
        )
    if revision.stage_metadata is not None:
        evidence_by_claim = {
            binding.claim_key: binding.evidence_ids
            for binding in revision.stage_metadata.claim_evidence_bindings
        }
        for segment in revision.business_model.segments:
            segment_years: set[int] = set()
            for node in _iter_nodes(segment.revenue_model.decomposition):
                node_key = f"{segment.id}.{node.id}"
                item_id = compiled_registry.node_items.get(node_key)
                if item_id is None:
                    continue
                target_ids = [item_id]
                direct_segment_id = _direct_segment_revenue_item_id(
                    revision, compiled_registry, node_key
                )
                if direct_segment_id is not None and direct_segment_id in model._index:
                    target_ids.append(direct_segment_id)
                for observation in node.historical_observations:
                    evidence_ids = evidence_by_claim.get(
                        business_model_observation_claim_key(
                            node_key, observation.period_id
                        )
                    )
                    if observation.period_id not in fiscal_years or not evidence_ids:
                        continue
                    year = fiscal_years[observation.period_id]
                    for target_id in target_ids:
                        if (target_id, year) in resolved_cells:
                            if target_id == direct_segment_id:
                                segment_years.add(year)
                            continue
                        target = model.get_item(target_id)
                        existing = (
                            None if target.values is None
                            else target.values.values.get(year)
                        )
                        if existing is not None and (
                            existing.value is None
                            or Decimal(str(existing.value)) != observation.value
                        ):
                            continue
                        note = (
                            "accepted_business_model_observation:"
                            f"{revision.ref.content_sha256}:{node_key}:"
                            f"{observation.period_id}"
                        )
                        if target.values is None:
                            target.values = ValueSeries()
                        target.values.values[year] = ValueCell(
                            period=year,
                            value=float(observation.value),
                            provenance=ValueProvenance.historical_foundation,
                            note=note,
                        )
                        if target.overrides is None:
                            target.overrides = {}
                        target.overrides[year] = FormulaSpec(
                            type=FormulaType.constant,
                            params={"value": float(observation.value)},
                            note=note,
                        )
                        resolved_cells.add((target_id, year))
                        if target_id == direct_segment_id:
                            segment_years.add(year)
                        coverage.append({
                            "driver_key": node_key,
                            "resolved_item_id": target_id,
                            "periods": [year],
                            "period_id": observation.period_id,
                            "value": str(observation.value),
                            "unit": observation.unit.value,
                            "model_scale": observation.model_scale,
                            "observation_basis": node.observation_basis,
                            "disposition": "observed",
                            "evidence_ids": list(evidence_ids),
                            "fact_ids": [],
                            "confidence": observation.confidence,
                        })
            if segment_years:
                _extend_segment_revenue_periods(model, segment.id, segment_years)
    return coverage


__all__ = [
    "HistoricalDriverSeedCompileError",
    "apply_model_spec_historical_driver_seeds",
    "hff_segment_revenue_series_values",
    "validate_historical_driver_seed_revenue_topology",
]
