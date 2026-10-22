"""Transform a published model at selected scenario multiples without product state."""

from __future__ import annotations

from collections.abc import Mapping

from schema.build import load_template
from schema.models import FinancialModel, ValueCell, ValueProvenance, ValueSeries


def model_at_selected_multiples(
    model: FinancialModel,
    selected_multiples: Mapping[str, float],
    *,
    selected_pe_ids: Mapping[str, str],
    pe_valuation_ids: Mapping[str, str],
) -> FinancialModel:
    """Copy the published model at each selected P/E, restoring authorized formulas.

    A predecessor built under a ValuationPolicy without forward_pe authorization
    was published with its relative-valuation formula removed. The successor
    carries that authorization, so restore the template formula in the copy.
    Item identities are supplied by the caller alongside its selected cases.
    """
    priced = model.model_copy(deep=True)
    priced.build_index()
    template: FinancialModel | None = None
    for case, multiple in selected_multiples.items():
        item = priced.get_item(selected_pe_ids[case])
        periods = {
            *priced.time_structure.projection_periods,
            *(item.values.values if item.values is not None else ()),
        }
        item.values = ValueSeries(
            values={
                int(period): ValueCell(
                    period=int(period),
                    value=float(multiple),
                    provenance=ValueProvenance.input,
                )
                for period in periods
            }
        )
        valuation = priced.get_item(pe_valuation_ids[case])
        if valuation.projected is None:
            template = template or load_template()
            source = template.get_item(pe_valuation_ids[case])
            for field in ("historical", "projected", "overrides", "values", "formula_periods"):
                setattr(valuation, field, getattr(source, field))
    return priced
