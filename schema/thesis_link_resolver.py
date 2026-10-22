"""Pure ThesisLink -> FinancialModel resolution helpers.

Known contract gap deferred to ModelBuildContext work:

``segment_edgar_member`` cannot resolve today because expanded ``LineItem``
instances do not persist EDGAR member identity on the model graph.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from .business_model_compiler import CompiledDriverRegistry
from .driver_resolver import _find_item, resolve_driver_key
from .model_input_errors import InvalidDriverKey, ModelBuildContextError
from .models import FinancialModel, LineItem
from .thesis import ResolutionAnchor, StructuralFingerprint, ThesisLink

logger = logging.getLogger(__name__)

ResolvedAnchor = Literal[
    "driver_key",
    "business_model_node",
    "required_role",
    "structural_fingerprint",
    "template_version_cache",
    "model_item_id",
    "none",
]

_STRUCTURAL_ANCHOR_WARNING_REPEAT_GROUP_ROLE_MISMATCH = "repeat_group_role_mismatch"
_STRUCTURAL_ANCHOR_WARNING_SEGMENT_EDGAR_MEMBER_UNAVAILABLE = "segment_edgar_member_anchor_unavailable"
_STRUCTURAL_ROLE_GENERIC_GROWTH = "growth"
_STRUCTURAL_ROLE_SPECIFIC_GROWTH = frozenset({"volume_growth", "price_growth"})
_SEMANTIC_DRIVER_KEY_ALIASES: dict[str, str] = {
    "consolidated_gross_margin_pct": "tpl.a.unit_economics.gross_margin",
    "gross_margin_pct": "tpl.a.unit_economics.gross_margin",
    "unit_economics.gross_margin": "tpl.a.unit_economics.gross_margin",
}
_ANCHOR_STRENGTH: dict[ResolvedAnchor, int] = {
    "driver_key": 6,
    "business_model_node": 5,
    "required_role": 4,
    "structural_fingerprint": 3,
    "template_version_cache": 2,
    "model_item_id": 1,
    "none": 0,
}


@dataclass(frozen=True)
class ResolvedLink:
    line_item: LineItem | None
    anchor: ResolutionAnchor
    resolution_strength: int
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _ItemContext:
    sheet_name: str
    section_id: str
    position_index: int
    segment_index: int | None
    line_item: LineItem


def _resolved(anchor: ResolvedAnchor, line_item: LineItem | None, warnings: list[str]) -> ResolvedLink:
    return ResolvedLink(
        line_item=line_item,
        anchor=anchor,
        resolution_strength=_ANCHOR_STRENGTH[anchor],
        warnings=list(warnings),
    )


def _append_warning(warnings: list[str], warning: str) -> None:
    if warning and warning not in warnings:
        warnings.append(warning)






def _iter_contexts(model: FinancialModel) -> list[_ItemContext]:
    contexts: list[_ItemContext] = []
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            segment_counters: dict[tuple[str, str], int] = {}
            for position_index, item in enumerate(section.line_items, start=1):
                segment_index: int | None = None
                if item.repeat_group_id and item.repeat_group_role:
                    key = (item.repeat_group_id, item.repeat_group_role)
                    segment_counters[key] = segment_counters.get(key, 0) + 1
                    segment_index = segment_counters[key]
                contexts.append(
                    _ItemContext(
                        sheet_name=sheet_name,
                        section_id=section.id,
                        position_index=position_index,
                        segment_index=segment_index,
                        line_item=item,
                    )
                )
    return contexts


def _section_contexts(contexts: list[_ItemContext], fingerprint: StructuralFingerprint) -> list[_ItemContext]:
    return [
        context
        for context in contexts
        if context.sheet_name == fingerprint.sheet and context.section_id == fingerprint.section_id
    ]


def _select_candidate(candidates: list[_ItemContext], position_index: int | None) -> LineItem | None:
    if len(candidates) == 1:
        return candidates[0].line_item
    if len(candidates) <= 1 or position_index is None:
        return None
    positioned = [candidate for candidate in candidates if candidate.position_index == int(position_index)]
    if len(positioned) == 1:
        return positioned[0].line_item
    return None


def _resolve_structural_fingerprint(
    link: ThesisLink,
    contexts: list[_ItemContext],
    warnings: list[str],
) -> LineItem | None:
    fingerprint = link.structural_fingerprint
    if fingerprint is None:
        return None

    base_candidates = _section_contexts(contexts, fingerprint)
    if not base_candidates:
        return None

    if (
        fingerprint.repeat_group_id
        and fingerprint.repeat_group_role
        and fingerprint.segment_index is not None
    ):
        segment_candidates = [
            candidate
            for candidate in base_candidates
            if candidate.line_item.repeat_group_id == fingerprint.repeat_group_id
            and candidate.segment_index == fingerprint.segment_index
        ]
        role_matches = [
            candidate
            for candidate in segment_candidates
            if candidate.line_item.repeat_group_role == fingerprint.repeat_group_role
        ]
        selected = _select_candidate(role_matches, fingerprint.position_index)
        if selected is not None:
            return selected

        if (
            fingerprint.repeat_group_role in _STRUCTURAL_ROLE_SPECIFIC_GROWTH
            and any(candidate.line_item.repeat_group_role == _STRUCTURAL_ROLE_GENERIC_GROWTH for candidate in segment_candidates)
        ):
            _append_warning(warnings, _STRUCTURAL_ANCHOR_WARNING_REPEAT_GROUP_ROLE_MISMATCH)

    if fingerprint.segment_edgar_member:
        # The current FinancialModel graph does not retain segment member
        # identity after expand_segments().
        _append_warning(warnings, _STRUCTURAL_ANCHOR_WARNING_SEGMENT_EDGAR_MEMBER_UNAVAILABLE)
    return None


def _resolve_required_role(
    required_role: str | None,
    contexts: list[_ItemContext],
) -> LineItem | None:
    if not required_role:
        return None
    matches = [
        context.line_item
        for context in contexts
        if context.line_item.required_role == required_role
    ]
    return matches[0] if len(matches) == 1 else None


def _resolve_template_version_cache(link: ThesisLink, model: FinancialModel) -> LineItem | None:
    if not link.template_version or not link.model_item_id:
        return None
    current_template_version = model.metadata.template_version
    if not current_template_version:
        return None
    if link.template_version != current_template_version:
        return None
    if not model._index:
        model.build_index()
    try:
        return model.get_item(link.model_item_id)
    except KeyError:
        logger.debug("template_version cache anchor dropped: model_item_id %r not in model", link.model_item_id)
        return None


def _resolve_model_item_id(model: FinancialModel, model_item_id: str | None) -> LineItem | None:
    if not model_item_id:
        return None
    if not model._index:
        model.build_index()
    try:
        return model.get_item(model_item_id)
    except KeyError:
        logger.debug("model_item_id anchor dropped: %r not in model", model_item_id)
        return None


def _try_driver_key(
    link: ThesisLink,
    model: FinancialModel,
    compiled_registry: CompiledDriverRegistry | None,
) -> ResolvedLink | None:
    if not link.driver_key:
        return None
    resolved_item_id: str | None
    try:
        resolved_item_id = resolve_driver_key(link.driver_key, compiled_registry=compiled_registry)
    except InvalidDriverKey:
        resolved_item_id = _SEMANTIC_DRIVER_KEY_ALIASES.get(str(link.driver_key or "").strip())
    except (KeyError, ValueError, ModelBuildContextError) as exc:
        logger.warning(
            "driver_key %r failed to resolve (%s: %s); anchor dropped",
            link.driver_key,
            type(exc).__name__,
            exc,
        )
        return None
    if resolved_item_id is None:
        return None
    try:
        _, item = _find_item(model, resolved_item_id)
    except KeyError:
        logger.debug("driver_key %r resolved to %r but item not in model", link.driver_key, resolved_item_id)
        return None
    return _resolved("driver_key", item, [])


def _try_business_model_node(
    link: ThesisLink,
    model: FinancialModel,
    compiled_registry: CompiledDriverRegistry | None,
    warnings: list[str],
) -> ResolvedLink | None:
    if not link.business_model_node_id or compiled_registry is None:
        return None
    item_id = compiled_registry.node_items.get(link.business_model_node_id)
    if item_id is None:
        _append_warning(
            warnings,
            f"business_model_node_id {link.business_model_node_id!r} not in compiled registry",
        )
        return None
    try:
        _, item = _find_item(model, item_id)
    except KeyError:
        _append_warning(
            warnings,
            f"BM node resolved to {item_id!r} but item not found in model",
        )
        return None
    return _resolved("business_model_node", item, warnings)


def resolve_link(
    link: ThesisLink,
    model: FinancialModel,
    *,
    compiled_registry: CompiledDriverRegistry | None = None,
) -> ResolvedLink:
    warnings: list[str] = []
    contexts = _iter_contexts(model)

    result = _try_driver_key(link, model, compiled_registry)
    if result is not None:
        return result

    result = _try_business_model_node(link, model, compiled_registry, warnings)
    if result is not None:
        return result

    item = _resolve_required_role(link.required_role, contexts)
    if item is not None:
        return _resolved("required_role", item, warnings)

    item = _resolve_structural_fingerprint(link, contexts, warnings)
    if item is not None:
        return _resolved("structural_fingerprint", item, warnings)

    item = _resolve_template_version_cache(link, model)
    if item is not None:
        return _resolved("template_version_cache", item, warnings)

    item = _resolve_model_item_id(model, link.model_item_id)
    if item is not None:
        _append_warning(warnings, "stale_model_item_id")
        return _resolved("model_item_id", item, warnings)

    return _resolved("none", None, warnings)


__all__ = ["ResolvedLink", "resolve_link"]
