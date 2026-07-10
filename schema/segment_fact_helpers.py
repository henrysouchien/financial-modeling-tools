"""Revenue segment fact parsing helpers."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence

from .model_build_context import SegmentRevenueObservation
from .source_values import normalize_edgar_value

REVENUE_TAGS_PRIORITY = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "Revenues",
]

SEGMENT_AXES_PRIORITY = [
    "StatementBusinessSegmentsAxis",
    "ProductOrServiceAxis",
    "StatementGeographicalAxis",
]

_REVENUE_TAGS = set(REVENUE_TAGS_PRIORITY)


def _normalize_qname(qname: str) -> str:
    if not qname:
        return ""
    text = str(qname)
    if ":" in text:
        return text.split(":", 1)[1]
    return text


def _normalize_scale(value: float, scale: Optional[str]) -> float:
    return normalize_edgar_value(value, scale, concept_id="")


def _filter_revenue_facts(facts: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    consolidated: List[Dict] = []
    dimensional: List[Dict] = []

    for fact in facts:
        tag = _normalize_qname(str(fact.get("tag") or fact.get("metric_tag") or ""))
        if tag not in _REVENUE_TAGS:
            continue
        value = _fact_value(fact)
        if value is None:
            continue
        axis_key = str(fact.get("axis_key") or "__NONE__")
        if axis_key == "__NONE__":
            consolidated.append(fact)
        else:
            dimensional.append(fact)

    return consolidated, dimensional


def _extract_fact_rows(payload: Dict) -> List[Dict]:
    rows: List[Dict] = []

    def walk(obj) -> None:
        if isinstance(obj, dict):
            if "tag" in obj and (
                "current_period_value" in obj or "visual_current_value" in obj or "value" in obj
            ):
                rows.append(obj)
                return
            for value in obj.values():
                walk(value)
            return
        if isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(payload)
    return rows


def _dedupe_member_facts(
    facts: List[Dict],
    tag_priority: List[str],
) -> Dict[tuple[str, int], Dict]:
    priority_index = {tag: index for index, tag in enumerate(tag_priority)}
    deduped: Dict[tuple[str, int], Dict] = {}

    for fact in facts:
        dimension = _single_segment_dimension(fact)
        if dimension is None:
            continue

        member = str(dimension["member"])
        year = int(fact["_segment_year"])
        key = (member, year)
        existing = deduped.get(key)
        current_priority = priority_index.get(_normalize_qname(str(fact.get("tag") or "")), len(tag_priority))
        if existing is None:
            deduped[key] = fact
            continue

        existing_priority = priority_index.get(_normalize_qname(str(existing.get("tag") or "")), len(tag_priority))
        if current_priority < existing_priority:
            deduped[key] = fact

    return deduped


def _consolidated_values(facts: Sequence[Dict]) -> Dict[int, float]:
    priority_index = {tag: index for index, tag in enumerate(REVENUE_TAGS_PRIORITY)}
    best_by_year: Dict[int, Dict] = {}

    for fact in facts:
        year = int(fact["_segment_year"])
        existing = best_by_year.get(year)
        if existing is None:
            best_by_year[year] = fact
            continue
        current_priority = priority_index.get(_normalize_qname(str(fact.get("tag") or "")), len(REVENUE_TAGS_PRIORITY))
        existing_priority = priority_index.get(_normalize_qname(str(existing.get("tag") or "")), len(REVENUE_TAGS_PRIORITY))
        if current_priority < existing_priority:
            best_by_year[year] = fact

    values: Dict[int, float] = {}
    for year, fact in best_by_year.items():
        value = _fact_value(fact)
        if value is None:
            continue
        values[int(year)] = _normalize_scale(value, fact.get("scale"))
    return dict(sorted(values.items()))


def _largest_consolidated_values(facts: Sequence[Dict]) -> Dict[int, float]:
    """Return the broadest consolidated revenue fact available per year.

    Segment axes can legitimately reconcile to contract revenue while a separate
    non-contract stream (for example client-funds interest) makes GAAP total
    revenue larger.  Axis validation therefore keeps using tag-priority values;
    build diagnostics use this broader, magnitude-selected ground truth.
    """

    priority_index = {tag: index for index, tag in enumerate(REVENUE_TAGS_PRIORITY)}
    best_by_year: Dict[int, tuple[float, int]] = {}

    for fact in facts:
        raw_value = _fact_value(fact)
        if raw_value is None:
            continue
        year = int(fact["_segment_year"])
        normalized_value = _normalize_scale(raw_value, fact.get("scale"))
        tag = _normalize_qname(str(fact.get("tag") or ""))
        priority = priority_index.get(tag, len(REVENUE_TAGS_PRIORITY))
        existing = best_by_year.get(year)
        if existing is None or abs(normalized_value) > abs(existing[0]) or (
            abs(normalized_value) == abs(existing[0]) and priority < existing[1]
        ):
            best_by_year[year] = (normalized_value, priority)

    return {
        int(year): float(value)
        for year, (value, _priority) in sorted(best_by_year.items())
    }


def _single_segment_dimension(fact: Dict) -> Optional[Dict[str, str]]:
    dimensions = _fact_dimensions(fact)
    relevant = [
        dimension
        for dimension in dimensions
        if _normalize_qname(dimension["axis"]) in SEGMENT_AXES_PRIORITY
    ]
    if len(relevant) != 1:
        return None
    return relevant[0]


def _fact_dimensions(fact: Dict) -> List[Dict[str, str]]:
    dimensions: List[Dict[str, str]] = []
    raw_dimensions = fact.get("dimensions")
    if isinstance(raw_dimensions, list):
        for entry in raw_dimensions:
            if not isinstance(entry, dict):
                continue
            axis = _normalize_qname(str(entry.get("axis") or entry.get("axis_key") or ""))
            member = str(entry.get("member") or "")
            if not axis or not member:
                continue
            dimensions.append(
                {
                    "axis": axis,
                    "member": member,
                    "member_label": str(entry.get("member_label") or _pretty_member_label(member)),
                }
            )
        if dimensions:
            return dimensions

    axis_key = str(fact.get("axis_key") or "")
    if not axis_key or axis_key == "__NONE__":
        return dimensions

    for part in re.split(r"[|;]", axis_key):
        if "=" not in part:
            continue
        axis_name, member = part.split("=", 1)
        axis = _normalize_qname(axis_name.strip())
        member = member.strip()
        if not axis or not member:
            continue
        dimensions.append(
            {
                "axis": axis,
                "member": member,
                "member_label": _pretty_member_label(member),
            }
        )
    return dimensions


def _observation_from_fact(
    fact: Dict,
    *,
    dimension: Dict[str, str],
    year: int,
    value: float,
) -> SegmentRevenueObservation:
    return SegmentRevenueObservation(
        fiscal_year=int(year),
        value=float(value),
        source="edgar_fact",
        tag=_clean_optional_string(fact.get("tag") or fact.get("metric_tag")),
        scale=_clean_optional_string(fact.get("scale")),
        axis=_clean_optional_string(dimension.get("axis")),
        member=_clean_optional_string(dimension.get("member")),
        member_label=_clean_optional_string(dimension.get("member_label")),
        source_filing_accession=_clean_optional_string(
            fact.get("source_filing_accession")
            or fact.get("filing_accession")
            or fact.get("accession")
            or fact.get("adsh")
        ),
        source_form=_clean_optional_string(fact.get("source_form") or fact.get("form")),
        filed_at=_clean_optional_string(fact.get("filed_at") or fact.get("filed")),
        period_end=_clean_optional_string(
            fact.get("period_end")
            or fact.get("reported_period_end")
            or fact.get("end_date")
            or fact.get("end")
        ),
        basis_key=_clean_optional_string(
            fact.get("segment_basis_key")
            or fact.get("basis_key")
            or fact.get("recast_basis")
            or fact.get("presentation_basis")
            or fact.get("statement_basis")
        ),
    )


def _annotate_revenue_comparability(
    observations: Dict[int, SegmentRevenueObservation],
) -> Dict[int, SegmentRevenueObservation]:
    annotated: Dict[int, SegmentRevenueObservation] = {}
    prior: SegmentRevenueObservation | None = None
    for year, observation in sorted((int(year), obs) for year, obs in observations.items()):
        if prior is None:
            comparable = "not_applicable"
            note = observation.comparability_note
        elif prior.basis_key and observation.basis_key:
            comparable = "comparable" if prior.basis_key == observation.basis_key else "not_comparable"
            note = (
                observation.comparability_note
                if comparable == "comparable"
                else f"segment basis changed from {prior.basis_key!r} to {observation.basis_key!r}"
            )
        else:
            comparable = "unknown"
            note = observation.comparability_note or "segment basis provenance is missing for adjacent-year comparability"
        updated = observation.model_copy(
            update={
                "comparable_with_prior": comparable,
                "comparability_note": note,
            }
        )
        annotated[int(year)] = updated
        prior = updated
    return annotated


def _clean_optional_string(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _materiality_share(values: Dict[int, float], consolidated_values: Dict[int, float]) -> float:
    numerator = sum(float(values.get(year, 0.0)) for year in consolidated_values)
    denominator = sum(abs(float(value)) for value in consolidated_values.values())
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _segment_label(dimension: Dict[str, str]) -> str:
    label = str(dimension.get("member_label") or "").strip()
    if label:
        return label
    return _pretty_member_label(str(dimension.get("member") or ""))


def _pretty_member_label(member: str) -> str:
    label = _normalize_qname(member)
    label = re.sub(r"Member$", "", label)
    label = re.sub(r"(?<!^)([A-Z])", r" \1", label)
    return label.strip() or member


def _fact_value(fact: Dict):
    value = fact.get("current_period_value")
    if value is None:
        value = fact.get("visual_current_value")
    if value is None:
        value = fact.get("value")
    return value


__all__ = [
    "REVENUE_TAGS_PRIORITY",
    "SEGMENT_AXES_PRIORITY",
    "_REVENUE_TAGS",
    "_annotate_revenue_comparability",
    "_clean_optional_string",
    "_consolidated_values",
    "_dedupe_member_facts",
    "_extract_fact_rows",
    "_fact_dimensions",
    "_fact_value",
    "_filter_revenue_facts",
    "_materiality_share",
    "_normalize_qname",
    "_normalize_scale",
    "_observation_from_fact",
    "_pretty_member_label",
    "_segment_label",
    "_single_segment_dimension",
]
