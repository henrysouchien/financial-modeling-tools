"""Custom concept population helpers for schema build orchestration."""

from __future__ import annotations

import logging
import re
import sys
from typing import TYPE_CHECKING, Any

from .build_fmp_values import (
    _build_fmp_lookup as _fmp_build_fmp_lookup,
    _scale_fmp_value as _fmp_scale_fmp_value,
)
from .build_semantic_rows import (
    _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES,
)
from .build_value_writers import (
    _set_constant_override as _value_writer_set_constant_override,
    _set_imported_value as _value_writer_set_imported_value,
)
from .model_build_context import HistoricalSources
from .models import (
    DataSourceMapping,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
    Unit,
    ValueProvenance,
)
from .overrides import TickerOverrides

if TYPE_CHECKING:
    from .build import EdgarFetcher
    from .business_model import BusinessModel


_DSM_EXCLUDED_FROM_OVERRIDE_ENTRY = {"notes"}
_DSM_FIELD_NAMES = set(DataSourceMapping.model_fields.keys()) - _DSM_EXCLUDED_FROM_OVERRIDE_ENTRY
_AXIS_QNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z][A-Za-z0-9_-]*$")
_EDGAR_AXIS_FAMILY_LABELS = frozenset({"business_segment", "product", "geography"})


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build helper '{name}' is unavailable")
    return helper


def _validate_axis_key(axis_key: str, concept_id: str) -> None:
    if not isinstance(axis_key, str):
        raise ValueError(f"custom_concept {concept_id!r} axis_key must be a string")
    if "|" in axis_key:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must contain exactly one axis/member "
            "pair; multi-axis '|' keys are not supported"
        )
    if axis_key.count("=") != 1:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must have shape "
            "'axis_qname=member_qname'"
        )

    axis_qname, member_qname = axis_key.split("=", 1)
    if not axis_qname or not member_qname:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must have non-empty axis and member"
        )
    edgar_axis_family_labels = _parent_attr(
        "_EDGAR_AXIS_FAMILY_LABELS",
        _EDGAR_AXIS_FAMILY_LABELS,
    )
    axis_qname_re = _parent_attr("_AXIS_QNAME_RE", _AXIS_QNAME_RE)
    if axis_qname in edgar_axis_family_labels:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key axis {axis_qname!r} is a family "
            "label; expected an XBRL axis QName"
        )
    if not axis_qname_re.match(axis_qname):
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key axis {axis_qname!r} must be an "
            "XBRL QName"
        )
    if not axis_qname_re.match(member_qname):
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key member {member_qname!r} must be an "
            "XBRL QName"
        )


def _validate_inline_values(inline_values: dict, concept_id: str) -> None:
    if not isinstance(inline_values, dict):
        raise ValueError(f"custom_concept {concept_id!r} inline_values must be a dict")
    for year, value in inline_values.items():
        if not isinstance(year, str):
            raise ValueError(
                f"custom_concept {concept_id!r} inline_values keys must be fiscal-year strings"
            )
        try:
            int(year)
        except ValueError as exc:
            raise ValueError(
                f"custom_concept {concept_id!r} inline_values key {year!r} must be a "
                "fiscal-year string"
            ) from exc
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(
                f"custom_concept {concept_id!r} inline_values[{year!r}] must be numeric"
            )


def _apply_custom_concept_target_metadata(
    item: LineItem,
    concept_id: str,
    entry: dict[str, Any],
) -> None:
    existing_concept_id = item.data_concept_id
    allow_replace = (
        entry.get("replace_existing") is True
        or (entry.get("_meta") or {}).get("replace_existing") is True
    )
    if (
        item.id.startswith("tpl.fm.")
        and existing_concept_id
        and existing_concept_id != concept_id
        and not allow_replace
    ):
        raise ValueError(
            f"custom_concept {concept_id!r} cannot overwrite occupied financial-model "
            f"row {item.id!r} already mapped to {existing_concept_id!r}; use "
            "semantic_rows row_policy to bind or insert a reviewed semantic row"
        )
    item.data_concept_id = concept_id

    label = entry.get("target_label") or entry.get("label")
    if isinstance(label, str) and label.strip():
        item.label = label.strip()

    unit = entry.get("unit")
    if unit is not None:
        unit_cls = _parent_attr("Unit", Unit)
        item.unit = unit_cls(str(unit))

    notes = entry.get("analyst_notes") or entry.get("notes")
    if isinstance(notes, str) and notes.strip():
        item.build_notes = notes.strip()

    strategy = entry.get("projection_strategy")
    carry_forward_strategies = _parent_attr(
        "_CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES",
        _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES,
    )
    if strategy is not None and strategy not in carry_forward_strategies:
        raise ValueError(
            f"custom_concept {concept_id!r} projection_strategy {strategy!r} is not supported"
        )
    if strategy in carry_forward_strategies:
        formula_spec_cls = _parent_attr("FormulaSpec", FormulaSpec)
        formula_type = _parent_attr("FormulaType", FormulaType)
        line_item_ref_cls = _parent_attr("LineItemRef", LineItemRef)
        item.projected = formula_spec_cls(
            type=formula_type.ref,
            params={"source": line_item_ref_cls(id=item.id, t=-1)},
        )


def _populate_custom_concepts(
    model: FinancialModel,
    ticker: str,
    overrides: TickerOverrides,
    source: str,
    edgar_fetcher: "EdgarFetcher | None",
    fmp_data: dict | None,
    historical_sources: HistoricalSources | None,
    business_model: "BusinessModel | None",
    most_recent_fy: int,
    n_historical: int,
) -> int:
    """Fetch + populate custom_concepts into their target rows."""

    if (
        historical_sources is not None
        and historical_sources.overrides
        and overrides.custom_concepts
    ):
        raise NotImplementedError(
            "custom_concepts under routed builds with per-concept overrides "
            "(historical_sources.overrides) not supported in v1"
        )

    data_source_mapping_cls = _parent_attr("DataSourceMapping", DataSourceMapping)
    dsm_field_names = _parent_attr("_DSM_FIELD_NAMES", _DSM_FIELD_NAMES)
    validate_axis_key = _parent_attr("_validate_axis_key", _validate_axis_key)
    validate_inline_values = _parent_attr(
        "_validate_inline_values",
        _validate_inline_values,
    )
    apply_target_metadata = _parent_attr(
        "_apply_custom_concept_target_metadata",
        _apply_custom_concept_target_metadata,
    )
    fetch_dimensional_edgar_concept = _required_parent_attr(
        "_fetch_dimensional_edgar_concept"
    )
    fetch_legacy_edgar_concept = _required_parent_attr("_fetch_legacy_edgar_concept")
    build_fmp_lookup = _parent_attr("_build_fmp_lookup", _fmp_build_fmp_lookup)
    scale_fmp_value = _parent_attr("_scale_fmp_value", _fmp_scale_fmp_value)
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    set_imported_value = _parent_attr(
        "_set_imported_value",
        _value_writer_set_imported_value,
    )
    set_constant_override = _parent_attr(
        "_set_constant_override",
        _value_writer_set_constant_override,
    )

    base_effective_source = (
        historical_sources.default_source if historical_sources is not None else source
    )
    base_effective_source = str(base_effective_source).lower()
    populated = 0
    historical_periods = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))

    for concept_id, entry in sorted(overrides.custom_concepts.items()):
        meta = entry.get("_meta") or {}
        if meta.get("disabled") is True:
            continue

        target_id = entry.get("target_item_id")
        if not target_id:
            logging.warning("custom_concept %r missing target_item_id; skipping", concept_id)
            continue

        try:
            item = model.get_item(target_id)
        except KeyError:
            if target_id.startswith("bm.") and business_model is not None:
                raise KeyError(
                    f"custom_concept {concept_id!r} targets BM row {target_id!r} "
                    "which the compiler did not emit"
                )
            logging.warning(
                "custom_concept %r targets missing item %r; skipping",
                concept_id,
                target_id,
            )
            continue

        dsm_fields = {key: value for key, value in entry.items() if key in dsm_field_names}
        dsm_fields["concept_id"] = concept_id
        mapping = data_source_mapping_cls.model_validate(dsm_fields)

        if mapping.registry_group_id:
            logging.warning(
                "custom_concept %r uses registry_group_id; not supported in v1, skipping",
                concept_id,
            )
            continue

        target_id = str(target_id)
        effective_source = base_effective_source
        is_bm_generated_concept = (
            business_model is not None
            and target_id.startswith("bm.")
            and meta.get("source") == "f2h"
            and bool(mapping.edgar_tags)
        )
        if is_bm_generated_concept:
            effective_source = "edgar"
        elif mapping.preferred_source:
            effective_source = str(mapping.preferred_source).lower()

        axis_key = entry.get("axis_key")
        inline_values = entry.get("inline_values")
        if axis_key is not None:
            validate_axis_key(axis_key, concept_id)
        if inline_values is not None:
            validate_inline_values(inline_values, concept_id)

        apply_target_metadata(item, concept_id, entry)

        values_by_year: dict[int, float] = {}
        path_taken: str | None = None
        path_a_eligible = (
            effective_source == "edgar"
            and edgar_fetcher is not None
            and axis_key is not None
            and getattr(edgar_fetcher, "supports_axis_filter", False)
            and bool(mapping.edgar_tags)
        )

        if path_a_eligible:
            fetch_result = fetch_dimensional_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=mapping,
                axis_key=axis_key,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
                include_equivalents=is_bm_generated_concept,
                allow_equivalent_tags=is_bm_generated_concept,
                include_local_tag_candidates=is_bm_generated_concept,
            )
            values_by_year = dict(fetch_result.values_dict)
            path_taken = "path_a"
            if not values_by_year and inline_values is not None:
                logging.warning(
                    "custom_concept %r Path A (axis_key=%r) returned no values "
                    "(status=%s, periods_failed=%d); falling back to inline_values",
                    concept_id,
                    axis_key,
                    fetch_result.status,
                    fetch_result.periods_failed,
                )
                path_taken = None

        if path_taken is None and effective_source == "edgar" and inline_values is not None:
            for year_str, value in inline_values.items():
                year = int(year_str)
                if year in historical_periods:
                    values_by_year[year] = float(value)
            path_taken = "path_b"
        elif (
            path_taken is None
            and effective_source == "edgar"
            and edgar_fetcher is not None
            and mapping.edgar_tags
        ):
            if meta.get("dimensional_intent") is True or axis_key is not None:
                logging.warning(
                    "custom_concept %r has dimensional intent (marker=%s, axis_key=%r) "
                    "but no Path A/B values available; refusing v1 unfiltered EDGAR "
                    "fallback (would write aggregate values). Re-bridge or supply "
                    "inline_values.",
                    concept_id,
                    meta.get("dimensional_intent"),
                    axis_key,
                )
                continue
            blended_members = _absorbed_claim_members_for_target(business_model, target_id)
            if blended_members:
                logging.warning(
                    "custom_concept %r targets blended segment members %s, but current "
                    "custom concept format cannot filter to multiple members; falling "
                    "back to unfiltered EDGAR fetch.",
                    concept_id,
                    blended_members,
                )
            fetch_result = fetch_legacy_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=mapping,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
                include_equivalents=is_bm_generated_concept,
                allow_equivalent_tags=is_bm_generated_concept,
                include_local_tag_candidates=is_bm_generated_concept,
            )
            values_by_year = dict(fetch_result.values_dict)
            path_taken = "v1_edgar"
        elif (
            path_taken is None
            and effective_source == "fmp"
            and fmp_data is not None
            and mapping.fmp_field
            and mapping.fmp_endpoint
        ):
            fmp_lookup = build_fmp_lookup(fmp_data)
            for year in historical_periods:
                record = fmp_lookup.get(mapping.fmp_endpoint, {}).get(year)
                raw = record.get(mapping.fmp_field) if record else None
                if raw is not None:
                    values_by_year[year] = scale_fmp_value(
                        concept_id,
                        raw,
                        concept=mapping,
                    )
            path_taken = "v1_fmp"

        if path_taken in ("path_a", "path_b", "v1_edgar"):
            provenance = value_provenance.imported_edgar
        elif path_taken == "v1_fmp":
            provenance = value_provenance.imported_fmp
        else:
            continue
        for year in historical_periods:
            if year not in values_by_year:
                continue
            value = values_by_year[year]
            if item.historical is None:
                set_imported_value(item, year, value, provenance=provenance)
            else:
                set_constant_override(item, year, value)
            populated += 1

    return populated


def _absorbed_claim_members_for_target(
    business_model: "BusinessModel | None",
    target_id: str,
) -> list[str]:
    if business_model is None:
        return []
    parts = str(target_id or "").split(".")
    if len(parts) < 3 or parts[0] != "bm":
        return []
    segment_id = parts[1]
    for segment in business_model.segments:
        if str(segment.id) != segment_id:
            continue
        members: list[str] = []
        for claim in segment.absorbs or []:
            member = str(getattr(claim, "member", "") or "").strip()
            name = str(getattr(claim, "name", "") or "").strip()
            members.append(member or name)
        return [member for member in members if member]
    return []
