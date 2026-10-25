"""Semantic row materialization helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import logging
import re
from typing import Any

from .build_findings import make_finding
from .build_formula_refs import (
    _extract_ref_ids as _formula_extract_ref_ids,
    saved_formula_slots,
)
from .build_model_items import _iter_items as _model_iter_items
from .build_real_data import _item_has_real_data as _real_data_item_has_real_data
from .finding_vocabulary import Finding
from .models import (
    FinancialStatement,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    StatementSectionRole,
)
from industry_slice_core.industry_types import Unit
from .multi_basis_window import InsertTarget, MultiBasisWindow
from .overrides import TickerOverrides
from .required_role_catalog import (
    _ROLE_RE,
    RequiredRoleUnknown,
    load_required_role_catalog,
)
from .statement_windows import CompiledStatementWindows

logger = logging.getLogger(__name__)


@dataclass
class SemanticRowsResult:
    materialized: list[dict[str, Any]] = field(default_factory=list)
    linkages: list[dict[str, Any]] = field(default_factory=list)
    collisions: list[dict[str, Any]] = field(default_factory=list)
    gaps: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialized": list(self.materialized),
            "linkages": list(self.linkages),
            "collisions": list(self.collisions),
            "gaps": list(self.gaps),
        }


_SEMANTIC_CARRY_FORWARD_STRATEGIES = frozenset(
    {"carry_forward", "prior_period", "flat"}
)
_SEMANTIC_FORECAST_ZERO_TYPE = "zero"

# Single owner of the semantic_rows row_policy.mode vocabulary (documented in
# schema/overrides/README.md). Bind-only modes must resolve against an existing row;
# insert-capable modes may fall through to inserting a new row.
_MODE_BIND_IF_EMPTY = "bind_if_empty"
_MODE_BIND_IF_SAME_SEMANTIC = "bind_if_same_semantic"
_MODE_BIND_IF_EMPTY_OR_INSERT = "bind_if_empty_or_insert"
_MODE_INSERT = "insert"
_MODE_INSERT_OR_BIND_SAME_SEMANTIC = "insert_or_bind_same_semantic"
_ROW_POLICY_BIND_ONLY_MODES = frozenset(
    {_MODE_BIND_IF_EMPTY, _MODE_BIND_IF_SAME_SEMANTIC}
)
_ROW_POLICY_INSERT_CAPABLE_MODES = frozenset(
    {_MODE_BIND_IF_EMPTY_OR_INSERT, _MODE_INSERT, _MODE_INSERT_OR_BIND_SAME_SEMANTIC}
)
# Modes permitted to bind an empty preferred target (spans both partitions).
_ROW_POLICY_BIND_EMPTY_MODES = frozenset(
    {_MODE_BIND_IF_EMPTY, _MODE_BIND_IF_EMPTY_OR_INSERT}
)

# The materialized-entry key that carries semantic_rows override values outside
# the vocabulary this module owns. Each element is ``{"site", "value",
# "outcome"}``; ``semantic_row_policy_findings`` turns a row's list into one
# ``semantic_row_policy_unrecognized`` warning, because Finding identity is
# ``(code, item_id, periods)`` (``collapse_findings``) and a second warning on
# the same row would be dropped there.
_UNRECOGNIZED_POLICY_KEY = "unrecognized_policy"


def _unrecognized_policy(site: str, value: str, outcome: str) -> dict[str, str]:
    return {"site": site, "value": value, "outcome": outcome}


def _materialized_entry(
    *,
    required_role: str,
    action: str,
    item_id: str,
    unrecognized: Iterable[dict[str, str] | None],
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "required_role": required_role,
        "action": action,
        "item_id": item_id,
    }
    carried = [value for value in unrecognized if value is not None]
    if carried:
        entry[_UNRECOGNIZED_POLICY_KEY] = carried
    return entry


def semantic_row_policy_findings(
    materialized: Iterable[Mapping[str, Any]],
) -> tuple[Finding, ...]:
    """One ``semantic_row_policy_unrecognized`` warning per row, naming every
    unrecognized site/value and what the compiler did with each."""

    findings: list[Finding] = []
    for row in materialized:
        carried = list(row.get(_UNRECOGNIZED_POLICY_KEY, ()))
        if not carried:
            continue
        clauses = "; ".join(
            f"{unrecognized['site']} {unrecognized['value']!r} is not a recognized "
            f"value, {unrecognized['outcome']}"
            for unrecognized in carried
        )
        observed: dict[str, str] = {"required_role": row["required_role"]}
        for unrecognized in carried:
            site = unrecognized["site"]
            observed[f"{site}.value"] = unrecognized["value"]
            observed[f"{site}.outcome"] = unrecognized["outcome"]
        findings.append(
            make_finding(
                "semantic_row_policy_unrecognized",
                message=f"semantic_row {row['required_role']!r}: {clauses}",
                item_id=row["item_id"],
                observed=observed,
            )
        )
    return tuple(findings)


_BS_TOTAL_FORMULA_BY_SECTION = {
    "current_assets": "tpl.fm.balance_sheet.total_current_assets",
    "current_liabilities": "tpl.fm.balance_sheet.total_current_liabilities",
}


def _semantic_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower())
    return slug.strip("_") or "row"


def _semantic_row_stable_id(
    ticker: str,
    required_role: str,
    row_policy: dict[str, Any],
) -> str:
    configured = row_policy.get("stable_item_id")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    return f"fm.semantic.{ticker.lower()}.{_semantic_slug(required_role)}"


def _locate_model_item(
    model: FinancialModel,
    item_id: str,
) -> tuple[str, Any, int, LineItem]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item in enumerate(section.line_items):
                if item.id == item_id:
                    return sheet_name, section, index, item
    raise KeyError(item_id)


def _item_has_any_real_historical_data(model: FinancialModel, item: LineItem) -> bool:
    for period in (
        model.time_structure.historical_periods
        or model.time_structure.historical_years
    ):
        if _real_data_item_has_real_data(item, int(period), model=model):
            return True
    return False


def _semantic_target_is_empty(model: FinancialModel, item: LineItem) -> bool:
    return not item.required_role and not _item_has_any_real_historical_data(model, item)


def _semantic_append_note(item: LineItem, note: str) -> None:
    note = note.strip()
    if not note:
        return
    if item.build_notes and note not in item.build_notes:
        item.build_notes = f"{item.build_notes}\n{note}"
    elif not item.build_notes:
        item.build_notes = note


def _semantic_forecast_type(entry: dict[str, Any]) -> str | None:
    policy = entry.get("forecast_policy")
    if isinstance(policy, dict):
        policy_type = policy.get("type")
        if isinstance(policy_type, str) and policy_type.strip():
            return policy_type.strip()
    strategy = entry.get("projection_strategy")
    if isinstance(strategy, str) and strategy.strip():
        return strategy.strip()
    return None


def _apply_semantic_row_metadata(
    item: LineItem,
    *,
    required_role: str,
    entry: dict[str, Any],
) -> dict[str, str] | None:
    """Apply the entry's metadata; return its unrecognized forecast type, if any."""

    item.required_role = required_role
    label = entry.get("target_label") or entry.get("label")
    if isinstance(label, str) and label.strip():
        item.label = label.strip()
    unit = entry.get("unit")
    if unit is not None:
        item.unit = Unit(str(unit))
    semantic_role = entry.get("semantic_role")
    forecast_policy = entry.get("forecast_policy")
    notes = entry.get("analyst_notes") or entry.get("notes")
    note_parts: list[str] = []
    if isinstance(notes, str) and notes.strip():
        note_parts.append(notes.strip())
    if isinstance(semantic_role, str) and semantic_role.strip():
        note_parts.append(f"Semantic role: {semantic_role.strip()}.")
    if isinstance(forecast_policy, dict):
        policy_type = forecast_policy.get("type")
        rationale = forecast_policy.get("rationale")
        if policy_type:
            note = f"Forecast policy: {policy_type}"
            if rationale:
                note += f" ({rationale})"
            note_parts.append(f"{note}.")
    for note in note_parts:
        _semantic_append_note(item, note)

    row_policy = entry.get("row_policy")
    if (
        isinstance(row_policy, dict)
        and row_policy.get("formula_wiring_mode") == "external"
    ):
        statement = FinancialStatement(str(entry.get("statement") or ""))
        section = str(entry.get("section") or "").strip()
        if not section:
            raise ValueError(
                f"semantic_row {required_role!r} external formula wiring "
                "requires section"
            )
        item.statement_section_roles = [
            StatementSectionRole(
                statement=statement,
                section=section,
                role="member",
            )
        ]

    forecast_type = _semantic_forecast_type(entry)
    if forecast_type in _SEMANTIC_CARRY_FORWARD_STRATEGIES:
        item.projected = FormulaSpec(
            type=FormulaType.ref,
            params={"source": LineItemRef(id=item.id, t=-1)},
        )
    elif forecast_type in {None, ""}:
        return None
    elif forecast_type == _SEMANTIC_FORECAST_ZERO_TYPE:
        item.projected = FormulaSpec(type=FormulaType.constant, params={"value": 0})
    else:
        return _unrecognized_policy(
            "forecast_policy.type",
            str(forecast_type),
            "the row's projection is left unset",
        )
    return None


def _insert_line_item_after(
    model: FinancialModel,
    *,
    anchor_item_id: str,
    new_item: LineItem,
) -> LineItem:
    """Insert ``new_item`` after its anchor; it joins the anchor's row block and, unless the
    caller declared one, its ``model_scale``. Compile-time insertion runs after the template's
    scale inference, so a row that said nothing took the ``LineItem`` default (``units``) in a
    schedule held in millions: the tax rows read an authored ``5`` as a cited-fact mismatch
    against ``5,000,000``, and the basic-shares row left the share curve mixing scales."""
    if any(item.id == new_item.id for item in _model_iter_items(model)):
        raise ValueError(f"semantic row item_id {new_item.id!r} already exists")

    sheet_name, section, anchor_index, anchor = _locate_model_item(model, anchor_item_id)
    if "model_scale" not in new_item.model_fields_set:
        new_item.model_scale = anchor.model_scale
    insert_row = int(anchor.row) + 1
    for sheet_item in (
        item
        for section_obj in model.sheets[sheet_name].sections
        for item in section_obj.line_items
    ):
        if int(sheet_item.row) >= insert_row:
            sheet_item.row = int(sheet_item.row) + 1
    new_item.row = insert_row
    section.line_items.insert(anchor_index + 1, new_item)
    model.build_index()
    return new_item


def _formula_contains_ref(obj: Any, item_id: str) -> bool:
    return item_id in _formula_extract_ref_ids(obj)


def _insert_ref_after_in_obj(obj: Any, *, after_id: str, new_id: str) -> bool:
    if isinstance(obj, list):
        inserted = False
        index = 0
        while index < len(obj):
            value = obj[index]
            if _insert_ref_after_in_obj(value, after_id=after_id, new_id=new_id):
                inserted = True
            if isinstance(value, LineItemRef) and value.id == after_id:
                obj.insert(index + 1, LineItemRef(id=new_id))
                inserted = True
                index += 1
            index += 1
        return inserted
    if isinstance(obj, dict):
        inserted = False
        for value in obj.values():
            if _insert_ref_after_in_obj(value, after_id=after_id, new_id=new_id):
                inserted = True
        return inserted
    return False


def _insert_ref_after_in_saved_formulas(
    params: dict[str, Any], *, after_id: str, new_id: str
) -> None:
    """Insert the ref into every formula ``params`` saved for restore, as in its live refs.

    A pass that lowered the spec kept the formula it replaced as JSON and a
    later pass restores it; a ref inserted only in the live spec is lost to
    that restore. ``_without_slot`` does the same for removal.
    """

    for container, keys in saved_formula_slots(params):
        for key in keys:
            saved = container.get(key)
            if not isinstance(saved, str) or not saved:
                continue
            held = FormulaSpec.model_validate_json(saved)
            _insert_ref_after_in_saved_formulas(
                held.params, after_id=after_id, new_id=new_id
            )
            if not _formula_contains_ref(held.params, new_id):
                _insert_ref_after_in_obj(held.params, after_id=after_id, new_id=new_id)
            container[key] = held.model_dump_json()


def _add_formula_ref_after(
    model: FinancialModel,
    *,
    formula_item_id: str,
    after_id: str,
    new_id: str,
) -> bool:
    item = model.get_item(formula_item_id)
    inserted_any = False
    for spec in (item.historical, item.projected):
        if spec is None:
            continue
        _insert_ref_after_in_saved_formulas(
            spec.params, after_id=after_id, new_id=new_id
        )
        if _formula_contains_ref(spec.params, new_id):
            inserted_any = True
            continue
        inserted_any = (
            _insert_ref_after_in_obj(spec.params, after_id=after_id, new_id=new_id)
            or inserted_any
        )
    return inserted_any


def _wire_semantic_bs_row(
    model: FinancialModel,
    *,
    section: str,
    row_policy: dict[str, Any],
    item_id: str,
) -> dict[str, str] | None:
    """Wire an inserted row into its section total.

    Returns the unrecognized ``formula_wiring_mode``, if any; that row is wired
    as ``auto``.
    """

    wiring_mode = str(row_policy.get("formula_wiring_mode") or "auto").strip()
    if wiring_mode == "external":
        return None
    unrecognized = (
        None
        if wiring_mode == "auto"
        else _unrecognized_policy(
            "row_policy.formula_wiring_mode",
            wiring_mode,
            "the row is wired as 'auto'",
        )
    )
    formula_item_id = _BS_TOTAL_FORMULA_BY_SECTION.get(section)
    if formula_item_id is None:
        return unrecognized
    after_id = row_policy.get("formula_insert_after_item_id") or row_policy.get(
        "insert_after_item_id"
    )
    if not isinstance(after_id, str) or not after_id.strip():
        return unrecognized
    if not _add_formula_ref_after(
        model,
        formula_item_id=formula_item_id,
        after_id=after_id.strip(),
        new_id=item_id,
    ):
        raise ValueError(
            f"semantic row {item_id!r} could not be wired into {formula_item_id!r} "
            f"after {after_id!r}"
        )
    return unrecognized


def _create_semantic_line_item(
    ticker: str,
    required_role: str,
    entry: dict[str, Any],
    row_policy: dict[str, Any],
) -> LineItem:
    return LineItem(
        id=_semantic_row_stable_id(ticker, required_role, row_policy),
        label=str(entry.get("target_label") or entry.get("label") or required_role),
        row=0,
        item_type=ItemType.derived,
        unit=Unit(str(entry.get("unit") or "dollars")),
        required_role=required_role,
    )




def _reviewed_window_insert_owner(
    compiled_statement_windows: CompiledStatementWindows | None,
    *,
    required_role: str,
    entry: dict[str, Any],
) -> tuple[MultiBasisWindow, InsertTarget] | None:
    if (
        compiled_statement_windows is None
        or compiled_statement_windows.semantic_rows.get(required_role) != entry
    ):
        return None
    owners: list[tuple[MultiBasisWindow, InsertTarget]] = []
    for section_plan in compiled_statement_windows.section_plans.values():
        if required_role not in section_plan.inserted_required_roles:
            continue
        binding = section_plan.window.binding_for(required_role)
        if binding is not None and isinstance(binding.target, InsertTarget):
            owners.append((section_plan.window, binding.target))
    if len(owners) != 1:
        return None
    return owners[0]


def _bind_or_insert_semantic_row(
    model: FinancialModel,
    *,
    ticker: str,
    required_role: str,
    entry: dict[str, Any],
    result: SemanticRowsResult,
    compiled_statement_windows: CompiledStatementWindows | None = None,
) -> LineItem | None:
    # Issuer operating leaves are validated by their declaring profile/binding.
    if _ROLE_RE.fullmatch(required_role):
        try:
            load_required_role_catalog().role(required_role)
        except RequiredRoleUnknown:
            owner = _reviewed_window_insert_owner(
                compiled_statement_windows,
                required_role=required_role,
                entry=entry,
            )
            if owner is None:
                raise
            _window, insert_target = owner
            if entry.get("row_policy", {}).get("stable_item_id") != (
                insert_target.stable_item_id
            ):
                raise ValueError(
                    f"semantic_row {required_role!r} stable item disagrees with its "
                    "reviewed window insert target"
                )
    row_policy = entry.get("row_policy")
    if not isinstance(row_policy, dict):
        raise ValueError(f"semantic_row {required_role!r} missing row_policy object")
    mode = str(row_policy.get("mode") or "").strip()
    if not mode:
        raise ValueError(f"semantic_row {required_role!r} missing row_policy.mode")

    semantic_row_stable_id = _semantic_row_stable_id
    apply_semantic_row_metadata = _apply_semantic_row_metadata
    stable_id = semantic_row_stable_id(ticker, required_role, row_policy)
    try:
        item = model.get_item(stable_id)
        unrecognized_forecast = apply_semantic_row_metadata(
            item,
            required_role=required_role,
            entry=entry,
        )
        result.materialized.append(
            _materialized_entry(
                required_role=required_role,
                action="bound_existing",
                item_id=item.id,
                unrecognized=(unrecognized_forecast,),
            )
        )
        return item
    except KeyError:
        pass

    iter_items = _model_iter_items
    for candidate in iter_items(model):
        if candidate.required_role == required_role:
            unrecognized_forecast = apply_semantic_row_metadata(
                candidate,
                required_role=required_role,
                entry=entry,
            )
            result.materialized.append(
                _materialized_entry(
                    required_role=required_role,
                    action="bound_same_semantic",
                    item_id=candidate.id,
                    unrecognized=(unrecognized_forecast,),
                )
            )
            return candidate

    preferred_target_id = row_policy.get("preferred_target_item_id")
    preferred_item: LineItem | None = None
    if isinstance(preferred_target_id, str) and preferred_target_id.strip():
        try:
            preferred_item = model.get_item(preferred_target_id.strip())
        except KeyError:
            result.gaps.append(
                {
                    "required_role": required_role,
                    "kind": "preferred_target_missing",
                    "item_id": preferred_target_id,
                }
            )

    if preferred_item is not None:
        if mode in _ROW_POLICY_BIND_EMPTY_MODES and _semantic_target_is_empty(
            model, preferred_item
        ):
            unrecognized_forecast = apply_semantic_row_metadata(
                preferred_item,
                required_role=required_role,
                entry=entry,
            )
            result.materialized.append(
                _materialized_entry(
                    required_role=required_role,
                    action="bound_empty",
                    item_id=preferred_item.id,
                    unrecognized=(unrecognized_forecast,),
                )
            )
            return preferred_item
        if (
            mode == _MODE_BIND_IF_SAME_SEMANTIC
            and preferred_item.required_role == required_role
        ):
            unrecognized_forecast = apply_semantic_row_metadata(
                preferred_item,
                required_role=required_role,
                entry=entry,
            )
            result.materialized.append(
                _materialized_entry(
                    required_role=required_role,
                    action="bound_same_semantic",
                    item_id=preferred_item.id,
                    unrecognized=(unrecognized_forecast,),
                )
            )
            return preferred_item
        if mode in _ROW_POLICY_BIND_ONLY_MODES:
            result.collisions.append(
                {
                    "required_role": required_role,
                    "item_id": preferred_item.id,
                    "existing_required_role": preferred_item.required_role,
                    "mode": mode,
                }
            )
            raise ValueError(
                f"semantic_row {required_role!r} cannot bind occupied target "
                f"{preferred_item.id!r} assigned to "
                f"{preferred_item.required_role!r}"
            )

    if mode in _ROW_POLICY_BIND_ONLY_MODES:
        result.gaps.append(
            {
                "required_role": required_role,
                "kind": "unsupported_row_policy",
                "mode": mode,
            }
        )
        raise ValueError(
            f"semantic_row {required_role!r} bind-only row_policy.mode {mode!r} "
            "found no target to bind"
        )
    unrecognized_mode = (
        None
        if mode in _ROW_POLICY_INSERT_CAPABLE_MODES
        else _unrecognized_policy("row_policy.mode", mode, "the row is inserted")
    )

    insert_after_id = row_policy.get("insert_after_item_id")
    if not isinstance(insert_after_id, str) or not insert_after_id.strip():
        raise ValueError(
            f"semantic_row {required_role!r} insert mode missing "
            "insert_after_item_id"
        )
    create_semantic_line_item = _create_semantic_line_item
    insert_line_item_after = _insert_line_item_after
    wire_semantic_bs_row = _wire_semantic_bs_row
    item = create_semantic_line_item(ticker, required_role, entry, row_policy)
    unrecognized_forecast = apply_semantic_row_metadata(
        item,
        required_role=required_role,
        entry=entry,
    )
    insert_line_item_after(model, anchor_item_id=insert_after_id.strip(), new_item=item)
    unrecognized_wiring = wire_semantic_bs_row(
        model,
        section=str(entry.get("section") or ""),
        row_policy=row_policy,
        item_id=item.id,
    )
    result.materialized.append(
        _materialized_entry(
            required_role=required_role,
            action="inserted",
            item_id=item.id,
            unrecognized=(unrecognized_mode, unrecognized_forecast, unrecognized_wiring),
        )
    )
    return item



def _materialize_semantic_rows(
    model: FinancialModel,
    ticker: str,
    overrides: TickerOverrides,
    *,
    compiled_statement_windows: CompiledStatementWindows | None = None,
) -> SemanticRowsResult:
    result = SemanticRowsResult()
    materialized_by_role: dict[str, str] = {}
    bind_or_insert_semantic_row = _bind_or_insert_semantic_row
    for required_role, entry in sorted((overrides.semantic_rows or {}).items()):
        if not isinstance(entry, dict):
            raise ValueError(f"semantic_row {required_role!r} must be an object")
        if (entry.get("_meta") or {}).get("disabled") is True:
            continue
        statement = str(entry.get("statement") or "")
        if statement != "balance_sheet":
            logger.warning(
                "semantic_row %r skipped as unsupported_statement: statement %r "
                "is not 'balance_sheet'; row recorded as gap",
                required_role,
                statement,
            )
            result.gaps.append(
                {
                    "required_role": required_role,
                    "kind": "unsupported_statement",
                    "statement": statement,
                }
            )
            continue
        item = bind_or_insert_semantic_row(
            model,
            ticker=ticker,
            required_role=required_role,
            entry=entry,
            result=result,
            compiled_statement_windows=compiled_statement_windows,
        )
        if item is None:
            continue
        materialized_by_role[required_role] = item.id

    from .build_semantic_rows_cash_flow import (
        _apply_semantic_cash_flow_linkages as apply_semantic_cash_flow_linkages,
    )
    from .build_semantic_rows_valuation import (
        _apply_semantic_valuation_linkages as apply_semantic_valuation_linkages,
    )
    apply_semantic_cash_flow_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_role=materialized_by_role,
        result=result,
    )
    apply_semantic_valuation_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_role=materialized_by_role,
        result=result,
    )
    return result
