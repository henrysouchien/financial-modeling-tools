"""Template metadata helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict

from ..models import DataSourceMapping


_DATA_TAXONOMY_PATH = Path(__file__).resolve().parent / "data_taxonomy.json"


def _validate_taxonomy_mapping(concept_id: str, mapping: DataSourceMapping) -> None:
    has_registry_group = mapping.registry_group_id is not None
    has_nonadmissible_reason = mapping.nonadmissible_reason_code is not None
    has_edgar_tags = bool(mapping.edgar_tags)

    if has_registry_group and mapping.canonical_tag is None:
        raise ValueError(
            f"Taxonomy concept '{concept_id}' sets registry_group_id without canonical_tag"
        )
    if has_nonadmissible_reason:
        if has_registry_group or mapping.canonical_tag is not None:
            raise ValueError(
                f"Taxonomy concept '{concept_id}' sets nonadmissible_reason_code with registry_group_id/canonical_tag"
            )
        if not has_edgar_tags:
            raise ValueError(
                f"Taxonomy concept '{concept_id}' sets nonadmissible_reason_code without edgar_tags"
            )
    if has_edgar_tags and not (has_registry_group or has_nonadmissible_reason):
        raise ValueError(
            f"Taxonomy concept '{concept_id}' has edgar_tags but no registry_group_id or nonadmissible_reason_code"
        )


def load_data_taxonomy() -> Dict[str, DataSourceMapping]:
    """Load bundled template data taxonomy mappings."""

    payload = json.loads(_DATA_TAXONOMY_PATH.read_text(encoding="utf-8"))
    taxonomy: Dict[str, DataSourceMapping] = {}
    for concept_id, raw_mapping in payload.items():
        mapping = DataSourceMapping.model_validate(raw_mapping)
        if mapping.concept_id != concept_id:
            raise ValueError(
                f"Taxonomy key '{concept_id}' does not match concept_id '{mapping.concept_id}'"
            )
        _validate_taxonomy_mapping(concept_id, mapping)
        if mapping.registry_group_id and mapping.edgar_tags:
            logging.info(
                "Taxonomy concept '%s' keeps both registry_group_id and edgar_tags during rollout",
                concept_id,
            )
        if (
            mapping.fmp_field is None
            and mapping.fmp_endpoint is None
            and not mapping.registry_group_id
            and not (mapping.edgar_tags or [])
        ):
            raise ValueError(
                f"Taxonomy concept '{concept_id}' has no FMP or EDGAR fetch path"
            )
        taxonomy[concept_id] = mapping
    return taxonomy


def build_pcty_reference_template(*args, **kwargs):
    """Build the checked-in PCTY reference artifact from a parsed PCTY model."""

    from .template_builder import build_pcty_reference_template as _build_pcty_reference_template

    return _build_pcty_reference_template(*args, **kwargs)


def build_sia_generic_template(*args, **kwargs):
    """Build the checked-in generic SIA template artifact."""

    from .template_builder import build_sia_generic_template as _build_sia_generic_template

    return _build_sia_generic_template(*args, **kwargs)


def load_pcty_reference():
    """Load the checked-in PCTY reference artifact."""

    from .template_builder import load_pcty_reference as _load_pcty_reference

    return _load_pcty_reference()


def load_sia_generic_template():
    """Load the checked-in generic SIA template artifact."""

    from .template_builder import load_sia_generic_template as _load_sia_generic_template

    return _load_sia_generic_template()


__all__ = [
    "build_pcty_reference_template",
    "build_sia_generic_template",
    "load_data_taxonomy",
    "load_pcty_reference",
    "load_sia_generic_template",
]
