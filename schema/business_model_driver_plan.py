from __future__ import annotations

from functools import lru_cache
import re
import sys
from typing import Any

from .business_model_driver_specs import (
    _capital_source_driver_specs,
    _working_capital_driver_specs,
)
from .models import Unit


_TEMPLATE_DRIVER_KEY_ALIASES = {
    "sm_pct": "sales_marketing_pct",
    "s_m_pct": "sales_marketing_pct",
    "sales_and_marketing_pct": "sales_marketing_pct",
    "sales_marketing_pct_revenue": "sales_marketing_pct",
    "rd_pct_revenue": "rd_pct",
    "r_d_pct": "rd_pct",
    "research_development_pct": "rd_pct",
    "research_and_development_pct": "rd_pct",
    "research_development_pct_revenue": "rd_pct",
    "ga_pct_revenue": "ga_pct",
    "g_a_pct": "ga_pct",
    "general_admin_pct": "ga_pct",
    "general_administrative_pct": "ga_pct",
    "general_and_administrative_pct": "ga_pct",
    "general_administrative_pct_revenue": "ga_pct",
}


def _parent_module() -> Any:
    parent = sys.modules.get("schema.business_model")
    if parent is None:
        parent = sys.modules.get("business_model")
    if parent is None:
        from . import business_model as parent
    return parent


def _parent_attr(name: str, fallback: Any) -> Any:
    return getattr(_parent_module(), name, fallback)


def driver_assumption_key(
    *,
    business_model_id: str,
    revision: str,
    segment_id: str,
    driver_node_id: str,
) -> str:
    return f"bm:{business_model_id}@{revision}:{segment_id}:{driver_node_id}"


def derive_driver_assumption_plan(
    business_model: Any,
    *,
    business_model_id: str | None = None,
) -> Any:
    business_model_id_fn = _parent_attr("_business_model_id", _business_model_id)
    iter_driver_nodes = _parent_attr("_iter_driver_nodes", _iter_driver_nodes)
    driver_assumption_entry = _parent_attr("_driver_assumption_entry", _driver_assumption_entry)
    consolidated_driver_assumption_entries = _parent_attr(
        "_consolidated_driver_assumption_entries",
        _consolidated_driver_assumption_entries,
    )
    driver_assumption_plan_cls = _parent_attr("DriverAssumptionPlan", None)
    if driver_assumption_plan_cls is None:
        raise RuntimeError("DriverAssumptionPlan is unavailable")

    normalized_business_model_id = business_model_id_fn(business_model, business_model_id)
    revision = str(business_model.metadata.revision or "").strip()
    if not revision:
        raise ValueError("BusinessModel metadata.revision is required to derive DriverAssumptionPlan")

    entries: list[Any] = []
    for segment in business_model.segments:
        segment_id = segment.id or "consolidated"
        for node in iter_driver_nodes(segment.revenue_model.decomposition):
            entries.append(
                driver_assumption_entry(
                    business_model_id=normalized_business_model_id,
                    revision=revision,
                    segment_id=segment_id,
                    node=node,
                )
            )

    entries.extend(
        consolidated_driver_assumption_entries(
            business_model,
            business_model_id=normalized_business_model_id,
            revision=revision,
        )
    )

    return driver_assumption_plan_cls(
        business_model_id=normalized_business_model_id,
        revision=revision,
        entries=entries,
    )


def _business_model_id(business_model: Any, business_model_id: str | None) -> str:
    normalized = str(business_model_id or "").strip()
    if normalized:
        return normalized
    return f"{business_model.company.ticker}_business_model"


def _iter_driver_nodes(nodes: list[Any]):
    for node in nodes:
        yield node
        if node.children:
            yield from _iter_driver_nodes(node.children)


def _driver_assumption_entry(
    *,
    business_model_id: str,
    revision: str,
    segment_id: str,
    node: Any,
) -> Any:
    driver_assumption_key_fn = _parent_attr("driver_assumption_key", driver_assumption_key)
    driver_assumption_plan_entry_cls = _parent_attr("DriverAssumptionPlanEntry", None)
    driver_assumption_aliases = _parent_attr("_driver_assumption_aliases", _driver_assumption_aliases)
    if driver_assumption_plan_entry_cls is None:
        raise RuntimeError("DriverAssumptionPlanEntry is unavailable")

    key = driver_assumption_key_fn(
        business_model_id=business_model_id,
        revision=revision,
        segment_id=segment_id,
        driver_node_id=node.id,
    )
    return driver_assumption_plan_entry_cls(
        driver_key=key,
        business_model_id=business_model_id,
        revision=revision,
        segment_id=segment_id,
        driver_node_id=node.id,
        label=node.label,
        unit=node.unit,
        factors=list(node.factors or []),
        behavior=node.behavior,
        management_target=node.management_target,
        compile_target_type=node.compile_to.target_type,
        existing_driver_key=node.compile_to.existing_driver_key,
        base_case_required=node.compile_to.target_type in {"assumption_row", "existing_row"},
        aliases=driver_assumption_aliases(segment_id, node),
    )


def _consolidated_driver_assumption_entries(
    business_model: Any,
    *,
    business_model_id: str,
    revision: str,
) -> list[Any]:
    plan_node_id = _parent_attr("_plan_node_id", _plan_node_id)
    existing_driver_key_resolves_to_input = _parent_attr(
        "_existing_driver_key_resolves_to_input",
        _existing_driver_key_resolves_to_input,
    )
    normal_existing_driver_key = _parent_attr("_normal_existing_driver_key", _normal_existing_driver_key)
    resolved_existing_driver_alias = _parent_attr(
        "_resolved_existing_driver_alias",
        _resolved_existing_driver_alias,
    )
    template_driver_assumption_entry = _parent_attr(
        "_template_driver_assumption_entry",
        _template_driver_assumption_entry,
    )
    working_capital_driver_specs = _parent_attr(
        "_working_capital_driver_specs",
        _working_capital_driver_specs,
    )
    capital_source_driver_specs = _parent_attr(
        "_capital_source_driver_specs",
        _capital_source_driver_specs,
    )

    entries: list[Any] = []
    seen_driver_node_ids: set[str] = set()

    def add_entry(
        driver_node_id: str,
        *,
        label: str,
        unit: Unit,
        factors: list[Any],
        existing_driver_key: str,
        aliases: list[str | None] | None = None,
        behavior: Any | None = None,
        management_target: str | None = None,
    ) -> None:
        normalized_node_id = plan_node_id(driver_node_id)
        if not normalized_node_id or normalized_node_id in seen_driver_node_ids:
            return
        if not existing_driver_key_resolves_to_input(existing_driver_key):
            return
        seen_driver_node_ids.add(normalized_node_id)
        entries.append(
            template_driver_assumption_entry(
                business_model_id=business_model_id,
                revision=revision,
                driver_node_id=normalized_node_id,
                label=label,
                unit=unit,
                factors=factors,
                existing_driver_key=existing_driver_key,
                aliases=aliases or [],
                behavior=behavior,
                management_target=management_target,
            )
        )

    consolidated = business_model.consolidated
    if consolidated is None:
        return entries

    for item in consolidated.cost_structure.items:
        submitted_driver_key = str(item.driver_key or "").strip()
        driver_key = normal_existing_driver_key(submitted_driver_key)
        if not driver_key:
            continue
        add_entry(
            item.id,
            label=item.label,
            unit=Unit.percentage,
            factors=list(item.factors or ["cost_structure"]),
            existing_driver_key=driver_key,
            aliases=[
                item.id,
                submitted_driver_key,
                driver_key,
                resolved_existing_driver_alias(driver_key),
            ],
        )

    reinvestment = consolidated.reinvestment
    if reinvestment is not None:
        if reinvestment.capex.model_as == "percent_of_revenue":
            add_entry(
                "capex_pct_revenue",
                label="Capital Expenditures % of Revenue",
                unit=Unit.percentage,
                factors=["reinvestment"],
                existing_driver_key="capex_pct",
                aliases=[
                    "capex_pct",
                    "capex_pct_revenue",
                    "capital_expenditures_pct_revenue",
                    "tpl.a.capital_investments.property_and_equipment_pct_revenue",
                ],
            )

        if reinvestment.working_capital is not None:
            for spec in working_capital_driver_specs(reinvestment.working_capital):
                add_entry(**spec)

    if consolidated.capital_sources is not None:
        for spec in capital_source_driver_specs(consolidated.capital_sources):
            add_entry(**spec)

    return entries


def _template_driver_assumption_entry(
    *,
    business_model_id: str,
    revision: str,
    driver_node_id: str,
    label: str,
    unit: Unit,
    factors: list[Any],
    existing_driver_key: str,
    aliases: list[str | None],
    behavior: Any | None = None,
    management_target: str | None = None,
) -> Any:
    driver_assumption_key_fn = _parent_attr("driver_assumption_key", driver_assumption_key)
    driver_assumption_plan_entry_cls = _parent_attr("DriverAssumptionPlanEntry", None)
    template_driver_aliases = _parent_attr("_template_driver_aliases", _template_driver_aliases)
    if driver_assumption_plan_entry_cls is None:
        raise RuntimeError("DriverAssumptionPlanEntry is unavailable")

    return driver_assumption_plan_entry_cls(
        driver_key=driver_assumption_key_fn(
            business_model_id=business_model_id,
            revision=revision,
            segment_id="consolidated",
            driver_node_id=driver_node_id,
        ),
        business_model_id=business_model_id,
        revision=revision,
        segment_id="consolidated",
        driver_node_id=driver_node_id,
        label=label,
        unit=unit,
        factors=factors,
        behavior=behavior,
        management_target=management_target,
        compile_target_type="existing_row",
        existing_driver_key=existing_driver_key,
        base_case_required=True,
        aliases=template_driver_aliases(driver_node_id, existing_driver_key, aliases),
    )


def _template_driver_aliases(
    driver_node_id: str,
    existing_driver_key: str,
    aliases: list[str | None],
) -> list[str]:
    resolved_existing_driver_alias = _parent_attr(
        "_resolved_existing_driver_alias",
        _resolved_existing_driver_alias,
    )
    unique_nonempty = _parent_attr("_unique_nonempty", _unique_nonempty)
    resolved = resolved_existing_driver_alias(existing_driver_key)
    return unique_nonempty(
        [
            f"consolidated.{driver_node_id}",
            f"bm.consolidated.{driver_node_id}",
            existing_driver_key,
            resolved,
            *aliases,
        ]
    )


def _plan_node_id(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if not normalized:
        return ""
    if normalized[0].isdigit():
        normalized = f"driver_{normalized}"
    return normalized


def _normal_existing_driver_key(value: str) -> str | None:
    aliases = _parent_attr("_TEMPLATE_DRIVER_KEY_ALIASES", _TEMPLATE_DRIVER_KEY_ALIASES)
    unique_nonempty = _parent_attr("_unique_nonempty", _unique_nonempty)
    existing_driver_key_resolves_to_input = _parent_attr(
        "_existing_driver_key_resolves_to_input",
        _existing_driver_key_resolves_to_input,
    )

    submitted = str(value or "").strip()
    if not submitted:
        return None
    candidates = unique_nonempty([submitted, aliases.get(submitted)])
    for candidate in candidates:
        if existing_driver_key_resolves_to_input(candidate):
            return candidate
    return None


def _existing_driver_key_resolves_to_input(existing_driver_key: str | None) -> bool:
    template_input_item_ids = _parent_attr("_template_input_item_ids", _template_input_item_ids)
    if not existing_driver_key:
        return False
    try:
        from .driver_resolver import resolve_driver_key

        item_id = resolve_driver_key(existing_driver_key)
        return item_id in template_input_item_ids()
    except Exception:
        return False


@lru_cache(maxsize=1)
def _template_input_item_ids() -> frozenset[str]:
    from .models import ItemType
    from .templates import load_sia_generic_template

    model = load_sia_generic_template()
    model.build_index()
    return frozenset(
        item_id
        for item_id, item in model._index.items()
        if item.item_type == ItemType.input
    )


def _driver_assumption_aliases(segment_id: str, node: Any) -> list[str]:
    resolved_existing_driver_alias = _parent_attr(
        "_resolved_existing_driver_alias",
        _resolved_existing_driver_alias,
    )
    unique_nonempty = _parent_attr("_unique_nonempty", _unique_nonempty)

    legacy_driver_key = f"{segment_id}.{node.id}"
    existing_driver_key = node.compile_to.existing_driver_key
    aliases = [
        legacy_driver_key,
        f"bm.{segment_id}.{node.id}",
        existing_driver_key,
        resolved_existing_driver_alias(existing_driver_key),
    ]
    if node.driver is not None and node.driver.type == "growth":
        rate_key = node.driver.params.rate_key
        aliases.extend(
            [
                f"{legacy_driver_key}.{rate_key}",
                f"bm.{segment_id}.{node.id}__{rate_key}",
            ]
        )
    return unique_nonempty(aliases)


def _resolved_existing_driver_alias(existing_driver_key: str | None) -> str | None:
    if not existing_driver_key:
        return None
    try:
        from .driver_resolver import resolve_driver_key

        resolved = resolve_driver_key(existing_driver_key)
    except Exception:
        return None
    return resolved if resolved != existing_driver_key else None


def _unique_nonempty(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


__all__ = [
    "_TEMPLATE_DRIVER_KEY_ALIASES",
    "_business_model_id",
    "_consolidated_driver_assumption_entries",
    "_driver_assumption_aliases",
    "_driver_assumption_entry",
    "_existing_driver_key_resolves_to_input",
    "_iter_driver_nodes",
    "_normal_existing_driver_key",
    "_plan_node_id",
    "_resolved_existing_driver_alias",
    "_template_driver_aliases",
    "_template_driver_assumption_entry",
    "_template_input_item_ids",
    "_unique_nonempty",
    "derive_driver_assumption_plan",
    "driver_assumption_key",
]
