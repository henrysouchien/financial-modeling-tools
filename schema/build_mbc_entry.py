"""ModelBuildContext entrypoint helpers for schema build orchestration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from .model_build_context import Driver, HistoricalSources, ModelBuildContext, SegmentConfig
from .models import (
    FinancialModel,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .source_routing import (
    ConceptSourceRoute,
    validate_route_eligibility as validate_route_eligibility_fallback,
)
from .valuation_schema_invariant import assert_valuation_template_schema

if TYPE_CHECKING:
    from .business_model import BusinessModel
    from .business_model_compiler import CompiledDriverRegistry
    from .build import BuildResult, ValuationCompsPayload
    from .segments import SegmentProfile


def _parent_attr(name: str, default: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        return default
    return getattr(parent, name, default)


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build helper '{name}' is unavailable")
    return helper


def _segment_profile_from_snapshot(
    ticker: str,
    segment_config: SegmentConfig,
) -> SegmentProfile:
    segment_info_type = _required_parent_attr("SegmentInfo")
    segment_profile_type = _required_parent_attr("SegmentProfile")
    observations_from_snapshot = _required_parent_attr(
        "segment_revenue_observations_from_snapshot"
    )

    snapshot = segment_config.segment_profile_snapshot
    sorted_snapshot_segments = sorted(
        snapshot.segments,
        key=lambda segment: segment.segment_index,
    )
    return segment_profile_type(
        ticker=ticker,
        segments=[
            segment_info_type(
                name=segment.name,
                edgar_member=segment.edgar_member,
                revenue_observations=observations_from_snapshot(segment),
                volume_label=segment.volume_label,
                price_label=segment.price_label,
            )
            for segment in sorted_snapshot_segments
        ],
        source=snapshot.source,
        axis_used=snapshot.axis_used,
        total_revenue_check=(
            dict(snapshot.total_revenue_check)
            if snapshot.total_revenue_check is not None
            else None
        ),
    )


def build_model_from_mbc(
    mbc: ModelBuildContext,
    *,
    output_path: str | None = None,
    formatter: Any | None = None,
    edgar_fetcher: Callable | None = None,
    edgar_financials_fetcher: Any | None = None,
    fmp_data: dict | None = None,
    business_model: "BusinessModel | None" = None,
    valuation_comps: "ValuationCompsPayload | dict[str, Any] | None" = None,
    validation_mode: bool = False,
    run_diagnostics: bool = False,
    overrides_dir: Path | None = None,
) -> BuildResult:
    """Build a model using ModelBuildContext as the authoritative scalar input source."""

    build_model = _required_parent_attr("build_model")
    is_default_historical_sources = _parent_attr(
        "_is_default_historical_sources",
        _is_default_historical_sources,
    )
    load_data_taxonomy = _required_parent_attr("load_data_taxonomy")
    concept_source_route = _parent_attr("ConceptSourceRoute", ConceptSourceRoute)
    validate_route_eligibility = _parent_attr(
        "validate_route_eligibility",
        validate_route_eligibility_fallback,
    )

    is_default_hs = (
        "historical_sources" not in mbc.model_fields_set
        or is_default_historical_sources(mbc.historical_sources)
    )
    historical_sources = None
    if not is_default_hs:
        taxonomy = load_data_taxonomy()
        for override in mbc.historical_sources.overrides:
            route = concept_source_route(
                concept_id=override.concept_id,
                primary=override.preferred,
                fallback_order=list(override.fallback_order),
                layer_decided="mbc_override",
            )
            validate_route_eligibility(
                route,
                taxonomy.get(override.concept_id),
                is_explicit_override=True,
            )
        historical_sources = mbc.historical_sources

    result = build_model(
        ticker=mbc.company.ticker,
        company_name=mbc.company.name,
        fiscal_year_end=mbc.company.fiscal_year_end,
        most_recent_fy=mbc.company.most_recent_fy,
        output_path=output_path,
        source=mbc.source,
        fmp_data=fmp_data,
        sector=mbc.sector,
        n_historical=mbc.n_historical,
        n_projection=mbc.n_projection,
        formatter=formatter,
        edgar_fetcher=edgar_fetcher,
        segment_mapping=None,
        edgar_financials_fetcher=edgar_financials_fetcher,
        axis=None,
        formula_first=mbc.formula_first,
        mbc_segment_config=mbc.segment_config,
        business_model=business_model,
        mbc_drivers=mbc.drivers if business_model else None,
        historical_sources=historical_sources,
        validation_mode=validation_mode,
        run_diagnostics=run_diagnostics,
        equity_risk_premium=mbc.valuation.inputs.equity_risk_premium,
        valuation_comps=valuation_comps,
        overrides_dir=overrides_dir,
    )
    result_model = getattr(result, "model", None)
    if isinstance(result_model, FinancialModel):
        assert_valuation_template_schema(
            result_model,
            origin="build_model_from_mbc",
            workbook_path=output_path,
            module_path=__file__,
        )
    return result


def _is_default_historical_sources(historical_sources: HistoricalSources) -> bool:
    # Persisted MBC JSON includes schema defaults; after reload Pydantic marks
    # them as supplied even though callers intended the legacy source path.
    return (
        historical_sources.default_source == "fmp"
        and not historical_sources.default_fallback_enabled
        and not historical_sources.overrides
    )


def _apply_mbc_seeds(
    model: FinancialModel,
    mbc_drivers: dict[str, Driver],
    compiled_registry: "CompiledDriverRegistry",
) -> None:
    """Write MBC driver values into ValueSeries before formula-first derivation."""

    is_business_model_rate_driver_key = _parent_attr(
        "_is_business_model_rate_driver_key",
        _is_business_model_rate_driver_key,
    )
    value_cell_type = _parent_attr("ValueCell", ValueCell)
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    value_series_type = _parent_attr("ValueSeries", ValueSeries)

    historical_periods = (
        model.time_structure.historical_periods
        or model.time_structure.historical_years
        or []
    )
    projection_periods = (
        model.time_structure.projection_periods
        or model.time_structure.projection_years
        or []
    )
    historical_set = {int(period) for period in historical_periods}

    for driver_key, driver in mbc_drivers.items():
        normalized_driver_key = str(driver_key or "").strip()
        item_id = compiled_registry.driver_keys.get(normalized_driver_key)
        if item_id is None:
            continue
        try:
            item = model.get_item(item_id)
        except KeyError:
            continue

        if item.values is None:
            item.values = value_series_type()

        has_formula = item.projected is not None
        is_bm_rate_row = is_business_model_rate_driver_key(
            normalized_driver_key,
            item_id,
        )
        requested = [int(period) for period in (driver.periods or [])]
        if not requested:
            if has_formula and not is_bm_rate_row:
                historical_years = sorted(historical_set)
                requested = [historical_years[-1]] if historical_years else []
            else:
                requested = [int(period) for period in projection_periods]

        if has_formula and not is_bm_rate_row:
            requested = [period for period in requested if period in historical_set]

        for period in requested:
            item.values.values[period] = value_cell_type(
                period=period,
                value=driver.value,
                provenance=value_provenance.input,
            )


def _is_business_model_rate_driver_key(driver_key: str, item_id: str) -> bool:
    if not item_id.startswith("bm.") or "__" not in item_id:
        return False
    rate_key = item_id.rsplit("__", 1)[-1]
    normalized = str(driver_key or "").strip()
    return normalized.endswith(f".{rate_key}") or normalized.endswith(f"__{rate_key}")


__all__ = [
    "_apply_mbc_seeds",
    "_is_business_model_rate_driver_key",
    "_is_default_historical_sources",
    "_segment_profile_from_snapshot",
    "build_model_from_mbc",
]
