"""Name unresolved template slots before the compiled intermediate is sealed.

``compile_model_intermediate`` (``schema/build.py``) calls
``_resolve_presentation_template_residue`` after population has written every
label it owns; whatever still reads as a slot noun is reported by the annual
presentation guard (``schema/annual_presentation_guard.py``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import re
from typing import Any

from .annual_presentation_guard import _is_visible_template_residue
from .models import FinancialModel, FormulaType, LineItem, LineItemRef
from .required_role_catalog import load_required_role_catalog


_PRESENTATION_LABEL_OVERRIDES = {
    "of_operating_line_adjust_link_as_neccesary": "Operating line ratio",
    "long_term_asset_2_pct_operating_line": "Long-term asset ratio",
    "long_term_asset_3_pct_operating_line": "Long-term asset ratio",
    "long_term_asset_1": "Long-term deferred contract costs",
    "long_term_asset_2": "Other non-current assets",
    "long_term_asset_3": "Marketable securities noncurrent",
    "scenario_margin_label": "Scenario margin",
}


def _node_driven_balance_labels() -> Mapping[str, str]:
    """Slot noun -> display name of each template slot only a declaring node's balance occupies."""

    return {
        balance.target_item.rsplit(".", 1)[-1]: balance.display_name
        for balance in load_required_role_catalog().balances.values()
        if balance.node_driven and balance.target_item is not None
    }


def _semantic_presentation_label(item_id: str) -> str:
    """Return a neutral semantic label for one unresolved template slot."""

    token = str(item_id).rsplit(".", 1)[-1]
    label = _PRESENTATION_LABEL_OVERRIDES.get(token) or _node_driven_balance_labels().get(token)
    if label is not None:
        return label
    token = re.sub(r"^comp_\d+(?:_|$)", "peer_", token)
    token = re.sub(r"_\d+$", "", token)
    token = token.replace("_if_applicable", "")
    token = token.replace("_adjust_link_as_neccesary", "")
    rendered = token.replace("_", " ").strip()
    return rendered[:1].upper() + rendered[1:] if rendered else "Other"


def _formula_refs(value: Any) -> Iterable[LineItemRef]:
    if isinstance(value, LineItemRef):
        yield value
    elif isinstance(value, Mapping):
        for nested in value.values():
            yield from _formula_refs(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _formula_refs(nested)


def _single_slot_transformed(item: LineItem) -> str | None:
    """The one item this row only references or takes the change of, if any."""

    specs = [
        spec
        for spec in (item.historical, item.projected, *(item.overrides or {}).values())
        if spec is not None and spec.type is not FormulaType.constant
    ]
    if not specs:
        return None
    slot_ids: set[str] = set()
    for spec in specs:
        refs = list(_formula_refs(spec.params))
        ids = {ref.id for ref in refs}
        if len(ids) != 1:
            return None
        if spec.type is FormulaType.ref:
            if len(refs) != 1:
                return None
        elif spec.type is FormulaType.arithmetic:
            if {ref.t for ref in refs} != {0, -1}:
                return None
        else:
            return None
        slot_ids |= ids
    return next(iter(slot_ids)) if len(slot_ids) == 1 else None


def _resolve_presentation_template_residue(
    model: FinancialModel,
    ticker: str,
    *,
    composition_slots: Mapping[str, str] | None = None,
) -> None:
    """Name every unresolved template slot before rendering a live workbook.

    Acts only on a label that is still template residue; a label population
    wrote stays. A slot label resolves, in order, to the display name of the
    single catalog role whose population or HFF plane names the item, else of
    its stamped ``required_role``; else to the operating leaf name of its
    composition slot; else to the label of the item on another sheet sharing
    its ``template_token``; else, for a row that only references or takes the
    change of one item, to that item's label. Anything still
    unresolved keeps the neutral semantic label, which the annual
    presentation guard reports when it is still a slot noun. Unresolved
    constant placeholders render blank.
    """

    from .required_role_catalog import load_required_role_catalog

    catalog = load_required_role_catalog()
    display_names = {
        row.required_role: row.display_name for row in catalog.required_roles
    }
    planted_roles: dict[str, set[str]] = {}
    for row in catalog.required_roles:
        # An alias plants its home's items; attribution names the home.
        if row.alias_of is not None:
            continue
        for item_id in (
            *catalog.population_item_ids(row.required_role, ticker=ticker),
            *row.hff_fact_items,
        ):
            planted_roles.setdefault(item_id, set()).add(row.required_role)

    composition_roles = {
        item_id.format(ticker=ticker.lower()): role
        for item_id, role in (composition_slots or {}).items()
    }
    ticker_label = str(ticker).strip().upper()
    items_by_id: dict[str, LineItem] = {}
    sheet_of: dict[str, str] = {}
    by_token: dict[str, list[LineItem]] = {}
    pending: list[LineItem] = []
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                items_by_id[item.id] = item
                sheet_of[item.id] = sheet_name
                token = str(item.template_token or "").strip()
                if token:
                    by_token.setdefault(token, []).append(item)
                if not _is_visible_template_residue(item.label):
                    continue
                if token.upper() == "[TICKER]":
                    item.label = ticker_label
                    continue
                roles = planted_roles.get(item.id, set())
                if len(roles) > 1:
                    item.label = _semantic_presentation_label(item.id)
                    continue
                role = next(iter(roles)) if roles else item.required_role
                if role in display_names:
                    item.label = display_names[role]
                elif not roles and item.id in composition_roles:
                    label = composition_roles[item.id].removeprefix("operating.")
                    label = label.replace("_", " ")
                    item.label = label[:1].upper() + label[1:]
                else:
                    pending.append(item)

    def _resolved_label(item_id: str | None) -> str | None:
        other = items_by_id.get(item_id) if item_id is not None else None
        if other is None or _is_visible_template_residue(other.label):
            return None
        return other.label

    def _sibling_label(item: LineItem) -> str | None:
        token = str(item.template_token or "").strip()
        siblings = [
            other
            for other in by_token.get(token, ())
            if other is not item and sheet_of[other.id] != sheet_of[item.id]
        ]
        return _resolved_label(siblings[0].id) if len(siblings) == 1 else None

    progressed = True
    while pending and progressed:
        progressed = False
        for item in list(pending):
            label = _sibling_label(item) or _resolved_label(
                _single_slot_transformed(item)
            )
            if label is not None:
                item.label = label
                pending.remove(item)
                progressed = True
    for item in pending:
        item.label = _semantic_presentation_label(item.id)

    for item in items_by_id.values():
        for spec in (item.historical, item.projected):
            if spec is None or spec.type is not FormulaType.constant:
                continue
            value = spec.params.get("value")
            if not _is_visible_template_residue(value):
                continue
            if str(value).strip().upper() == "[TICKER]":
                spec.params["value"] = ticker_label
            else:
                spec.params["value"] = ""
