"""Resolve diagnostic section roles from model metadata with legacy fallback."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .build_diagnostic_sections import BS_SECTIONS
from .build_diagnostic_values import _iter_items
from .models import FinancialModel, FinancialStatement, LineItem
from .required_role_catalog import RequiredRoleCatalog


@dataclass(frozen=True)
class RoleResolutionResult:
    sections: dict[str, dict[str, Any]]
    metadata_source: str
    fallback_reason: str | None
    role_derived_sections: int
    legacy_fallback_sections: int


def resolve_balance_sheet_sections(
    model: FinancialModel,
    *,
    base_sections: dict[str, dict[str, Any]] | None = None,
    role_catalog: RequiredRoleCatalog | None = None,
) -> RoleResolutionResult:
    """Return balance-sheet diagnostic sections, preferring complete row metadata.

    A section member is a declared template member present in the model: a
    slot the build removed (a source-empty, role-less placeholder) is not one.
    """

    base = _model_sections(model, base_sections or BS_SECTIONS)
    base_copy = _copy_sections(base)
    role_index = _balance_sheet_role_index(model)
    non_presentation_sections = [
        section
        for section, definition in base.items()
        if not definition.get("presentation_only")
    ]

    if not role_index:
        return _legacy_result(base_copy, "no_statement_section_roles")

    unknown_sections = sorted(set(role_index) - set(base))
    if unknown_sections:
        return _legacy_result(base_copy, f"unknown_section:{unknown_sections[0]}")

    for section in non_presentation_sections:
        definition = base[section]
        roles = role_index.get(section)
        if roles is None:
            return _legacy_result(base_copy, f"missing_section_roles:{section}")
        fallback_reason = _validate_required_roles(
            section,
            definition,
            roles,
        )
        if fallback_reason is not None:
            return _legacy_result(base_copy, fallback_reason)

    resolved = _copy_sections(base)

    return RoleResolutionResult(
        sections=resolved,
        metadata_source="model_metadata",
        fallback_reason=None,
        role_derived_sections=len(non_presentation_sections),
        legacy_fallback_sections=sum(
            1 for definition in base.values() if definition.get("presentation_only")
        ),
    )


def _copy_sections(sections: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {section: deepcopy(definition) for section, definition in sections.items()}


def _model_sections(
    model: FinancialModel,
    sections: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    present = {item.id for item in _iter_items(model)}
    resolved = _copy_sections(sections)
    for definition in resolved.values():
        definition["sub_lines"] = [
            member
            for member in definition["sub_lines"]
            if member.template_item_id in present
        ]
    return resolved


def _legacy_result(
    sections: dict[str, dict[str, Any]],
    fallback_reason: str,
) -> RoleResolutionResult:
    return RoleResolutionResult(
        sections=sections,
        metadata_source="legacy_bs_sections",
        fallback_reason=fallback_reason,
        role_derived_sections=0,
        legacy_fallback_sections=len(sections),
    )


def _balance_sheet_role_index(
    model: FinancialModel,
) -> dict[str, dict[str, list[LineItem]]]:
    role_index: dict[str, dict[str, list[LineItem]]] = {}
    for item in _iter_items(model):
        for role in item.statement_section_roles or []:
            if role.statement != FinancialStatement.balance_sheet:
                continue
            section_roles = role_index.setdefault(role.section, {})
            section_roles.setdefault(role.role, []).append(item)
    return role_index


def _validate_required_roles(
    section: str,
    definition: dict[str, Any],
    roles: dict[str, list[LineItem]],
) -> str | None:
    section_total_roles = roles.get("section_total", [])
    if len(section_total_roles) != 1:
        return f"missing_section_total:{section}"
    if section_total_roles[0].id != definition["total_item_id"]:
        return f"section_total_mismatch:{section}"

    structural_members = {
        item.id
        for structural_role in ("member", "catch_all")
        for item in roles.get(structural_role, ())
    }
    for member in definition["sub_lines"]:
        if member.template_item_id not in structural_members:
            return f"member_role_mismatch:{section}:{member.template_item_id}"


    included_subtotal_id = definition.get("also_includes_subtotal")
    included_subtotal_roles = roles.get("included_subtotal", [])
    if included_subtotal_id:
        if (
            len(included_subtotal_roles) != 1
            or included_subtotal_roles[0].id != included_subtotal_id
        ):
            return f"missing_included_subtotal:{section}:{included_subtotal_id}"
    elif included_subtotal_roles:
        return f"unexpected_included_subtotal:{section}:{included_subtotal_roles[0].id}"

    return None




__all__ = [
    "RoleResolutionResult",
    "resolve_balance_sheet_sections",
]
