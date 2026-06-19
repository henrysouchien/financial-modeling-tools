from __future__ import annotations


SHARED_SLICE_FIELD_PATHS = (
    "company",
    "thesis",
    "consensus_view",
    "differentiated_view",
    "invalidation_triggers",
    "business_overview",
    "catalysts",
    "risks",
    "valuation",
    "peers",
    "assumptions",
    "qualitative_factors",
    "ownership",
    "monitoring",
    "sources",
    "industry_analysis",
    "materiality",
    "historical_coincidences",
    "data_gaps",
    "quantitative_framing",
)

SHARED_SLICE_FIELD_SET = frozenset(SHARED_SLICE_FIELD_PATHS)
DILIGENCE_SECTION_KEYS = (
    "business_overview",
    "thesis",
    "catalysts",
    "valuation",
    "assumptions",
    "risks",
    "peers",
    "ownership",
    "monitoring",
    "industry_analysis",
)


__all__ = [
    "DILIGENCE_SECTION_KEYS",
    "SHARED_SLICE_FIELD_PATHS",
    "SHARED_SLICE_FIELD_SET",
]
