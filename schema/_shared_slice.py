from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .process_template import SectionKey


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
DILIGENCE_SECTION_KEYS: tuple[SectionKey, ...] = (
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
