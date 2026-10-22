from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import re
from typing import Any, TypeVar

from .business_model_driver_specs import (
    _capital_source_driver_specs,
    _working_capital_driver_specs,
)
from industry_slice_core.industry_types import Unit
from .stable_control_identity import (
    CompileBindingDefinition,
    ControlDefinition,
    DriverControlOwner,
    StableControlIdentityPlanInputs,
)
from .versioned_artifact import (
    VersionedArtifactRef,
    compute_artifact_content_sha256,
)


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


@dataclass(frozen=True, slots=True)
class _StableDriverPlanBinding:
    driver_id: str
    control_id: str
    binding_id: str
    template_item_id: str
    control_unit: Unit


class DriverAssumptionPlanIdentityUnavailable(ValueError):
    """The canonical stable-identity inputs cannot bind one plan entry."""


_Definition = TypeVar("_Definition")


def _current_definitions(
    definitions: tuple[_Definition, ...],
    *,
    identity_field: str,
) -> dict[str, _Definition]:
    current: dict[str, _Definition] = {}
    for definition in definitions:
        current[getattr(definition, identity_field)] = definition
    return current


def _stable_driver_plan_binding(
    *,
    registry: StableControlIdentityPlanInputs,
    segment_id: str,
    driver_node_id: str,
    expected_unit: Unit,
    write_eligible: bool,
) -> _StableDriverPlanBinding:
    """Resolve one closed BusinessModel node path through canonical identity inputs."""

    node_path = f"{segment_id}.{driver_node_id}"
    aliases = tuple(
        alias
        for alias in registry.aliases
        if alias.alias_kind == "business_model_node_path"
        and alias.raw_alias == node_path
        and alias.status == "active"
        and alias.valid_through_business_model_ref is None
        and alias.valid_through_business_model_hash is None
    )
    targets = tuple(
        sorted({(alias.identity_kind, alias.target_id) for alias in aliases})
    )
    if len(targets) != 1:
        raise DriverAssumptionPlanIdentityUnavailable(
            f"{node_path!r} requires exactly one active canonical identity alias"
        )

    drivers = _current_definitions(registry.drivers, identity_field="driver_id")
    controls = _current_definitions(registry.controls, identity_field="control_id")
    bindings = _current_definitions(registry.bindings, identity_field="binding_id")
    required_role = "primary_compile" if write_eligible else "display"

    target_kind, target_id = targets[0]
    selected_binding: CompileBindingDefinition | None = None
    selected_control: ControlDefinition | None = None
    if target_kind == "binding":
        candidate_binding = bindings.get(target_id)
        if candidate_binding is not None:
            selected_binding = candidate_binding
            selected_control = controls.get(candidate_binding.control_id)
    elif target_kind == "control":
        selected_control = controls.get(target_id)
    else:
        candidate_driver = drivers.get(target_id)
        matching_controls = tuple(
            control
            for control in controls.values()
            if control.lifecycle == "active"
            and control.domain == "forecast"
            and control.owner.kind == "driver"
            and control.owner.driver_id == getattr(
                candidate_driver,
                "driver_id",
                None,
            )
        )
        if len(matching_controls) == 1:
            selected_control = matching_controls[0]

    if (
        selected_control is None
        or selected_control.lifecycle != "active"
        or selected_control.domain != "forecast"
        or not isinstance(selected_control.owner, DriverControlOwner)
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            f"{node_path!r} does not resolve to one eligible forecast control"
        )
    control = selected_control
    driver = drivers.get(selected_control.owner.driver_id)
    if (
        driver is None
        or driver.lifecycle != "active"
        or driver.output_unit != expected_unit
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            f"{node_path!r} does not resolve to one eligible active driver"
        )

    if selected_binding is None:
        matching_bindings = tuple(
            binding
            for binding in bindings.values()
            if binding.lifecycle == "active"
            and binding.control_id == control.control_id
            and binding.role == required_role
            and binding.workbook_target is not None
        )
        if len(matching_bindings) != 1:
            raise DriverAssumptionPlanIdentityUnavailable(
                f"{node_path!r} requires exactly one active {required_role} binding"
            )
        selected_binding = matching_bindings[0]
    compile_binding = selected_binding
    if (
        compile_binding.lifecycle != "active"
        or compile_binding.control_id != control.control_id
        or compile_binding.role != required_role
        or compile_binding.workbook_target is None
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            f"{node_path!r} does not resolve to one eligible {required_role} binding"
        )
    assert compile_binding.workbook_target is not None
    return _StableDriverPlanBinding(
        driver_id=driver.driver_id,
        control_id=control.control_id,
        binding_id=compile_binding.binding_id,
        template_item_id=compile_binding.workbook_target.template_item_id,
        control_unit=control.unit,
    )


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
    business_model_ref: VersionedArtifactRef,
    identity_registry: StableControlIdentityPlanInputs,
    business_model_id: str | None = None,
) -> Any:
    from .business_model import DriverAssumptionPlan

    if (
        business_model_ref != identity_registry.business_model_ref
        or identity_registry.business_model_hash != business_model_ref.content_sha256
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            "identity inputs do not bind the exact BusinessModel artifact ref"
        )
    if (
        business_model_ref.artifact_kind != "business_model"
        or business_model_ref.payload_schema_version != "1.0"
        or business_model_ref.media_type != "application/json"
        or business_model_ref.digest_codec != "canonical_json_v1"
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            "BusinessModel artifact ref has the wrong canonical contract"
        )
    if (
        compute_artifact_content_sha256(
            business_model.model_dump(mode="json"),
            business_model_ref.digest_codec,
        )
        != business_model_ref.content_sha256
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            "BusinessModel bytes differ from the identity-bound artifact"
        )
    if (
        getattr(business_model_ref.scope, "ticker", None)
        != business_model.company.ticker
    ):
        raise DriverAssumptionPlanIdentityUnavailable(
            "BusinessModel artifact scope differs from the company ticker"
        )

    normalized_business_model_id = _business_model_id(business_model, business_model_id)
    revision = str(business_model.metadata.revision or "").strip()
    if not revision:
        raise ValueError("BusinessModel metadata.revision is required to derive DriverAssumptionPlan")

    entries: list[Any] = []
    for segment in business_model.segments:
        segment_id = segment.id or "consolidated"
        for node in _iter_driver_nodes(segment.revenue_model.decomposition):
            entries.append(
                _driver_assumption_entry(
                    identity_registry=identity_registry,
                    business_model_id=normalized_business_model_id,
                    revision=revision,
                    segment_id=segment_id,
                    node=node,
                )
            )

    entries.extend(
        _consolidated_driver_assumption_entries(
            business_model,
            identity_registry=identity_registry,
            business_model_id=normalized_business_model_id,
            revision=revision,
        )
    )

    return DriverAssumptionPlan(
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
    identity_registry: StableControlIdentityPlanInputs,
    business_model_id: str,
    revision: str,
    segment_id: str,
    node: Any,
) -> Any:
    from .business_model import DriverAssumptionPlanEntry

    key = driver_assumption_key(
        business_model_id=business_model_id,
        revision=revision,
        segment_id=segment_id,
        driver_node_id=node.id,
    )
    base_case_required = node.compile_to.target_type in {
        "assumption_row",
        "existing_row",
    }
    stable = _stable_driver_plan_binding(
        registry=identity_registry,
        segment_id=segment_id,
        driver_node_id=node.id,
        expected_unit=node.unit,
        write_eligible=base_case_required,
    )
    return DriverAssumptionPlanEntry(
        driver_id=stable.driver_id,
        control_id=stable.control_id,
        binding_id=stable.binding_id,
        template_item_id=stable.template_item_id,
        control_unit=stable.control_unit,
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
        base_case_required=base_case_required,
        aliases=_driver_assumption_aliases(segment_id, node),
    )


def _consolidated_driver_assumption_entries(
    business_model: Any,
    *,
    identity_registry: StableControlIdentityPlanInputs,
    business_model_id: str,
    revision: str,
) -> list[Any]:
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
        normalized_node_id = _plan_node_id(driver_node_id)
        if not normalized_node_id or normalized_node_id in seen_driver_node_ids:
            return
        if not _existing_driver_key_resolves_to_input(existing_driver_key):
            return
        seen_driver_node_ids.add(normalized_node_id)
        entries.append(
            _template_driver_assumption_entry(
                identity_registry=identity_registry,
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
        driver_key = _normal_existing_driver_key(submitted_driver_key)
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
                _resolved_existing_driver_alias(driver_key),
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
            for spec in _working_capital_driver_specs(reinvestment.working_capital):
                add_entry(**spec)

    if consolidated.capital_sources is not None:
        for spec in _capital_source_driver_specs(consolidated.capital_sources):
            add_entry(**spec)

    return entries


def _template_driver_assumption_entry(
    *,
    identity_registry: StableControlIdentityPlanInputs,
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
    from .business_model import DriverAssumptionPlanEntry

    stable = _stable_driver_plan_binding(
        registry=identity_registry,
        segment_id="consolidated",
        driver_node_id=driver_node_id,
        expected_unit=unit,
        write_eligible=True,
    )
    return DriverAssumptionPlanEntry(
        driver_id=stable.driver_id,
        control_id=stable.control_id,
        binding_id=stable.binding_id,
        template_item_id=stable.template_item_id,
        control_unit=stable.control_unit,
        driver_key=driver_assumption_key(
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
        aliases=_template_driver_aliases(driver_node_id, existing_driver_key, aliases),
    )


def _template_driver_aliases(
    driver_node_id: str,
    existing_driver_key: str,
    aliases: list[str | None],
) -> list[str]:
    resolved = _resolved_existing_driver_alias(existing_driver_key)
    return _unique_nonempty(
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
    submitted = str(value or "").strip()
    if not submitted:
        return None
    candidates = _unique_nonempty([submitted, _TEMPLATE_DRIVER_KEY_ALIASES.get(submitted)])
    for candidate in candidates:
        if _existing_driver_key_resolves_to_input(candidate):
            return candidate
    return None


def _existing_driver_key_resolves_to_input(existing_driver_key: str | None) -> bool:
    if not existing_driver_key:
        return False
    from .model_input_errors import ModelBuildContextError
    from .driver_resolver import resolve_driver_key

    try:
        item_id = resolve_driver_key(existing_driver_key)
    except (ModelBuildContextError, KeyError):
        # The resolver's typed answer for "this key does not resolve to a
        # template input"; anything else (broken driver_mapping.yaml, template
        # load faults) propagates as the owner's error.
        return False
    return item_id in _template_input_item_ids()


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
    legacy_driver_key = f"{segment_id}.{node.id}"
    existing_driver_key = node.compile_to.existing_driver_key
    aliases = [
        legacy_driver_key,
        f"bm.{segment_id}.{node.id}",
        existing_driver_key,
        _resolved_existing_driver_alias(existing_driver_key),
    ]
    if node.driver is not None and node.driver.type == "growth":
        rate_key = node.driver.params.rate_key
        aliases.extend(
            [
                f"{legacy_driver_key}.{rate_key}",
                f"bm.{segment_id}.{node.id}__{rate_key}",
            ]
        )
    return _unique_nonempty(aliases)


def _resolved_existing_driver_alias(existing_driver_key: str | None) -> str | None:
    if not existing_driver_key:
        return None
    from .model_input_errors import ModelBuildContextError
    from .driver_resolver import resolve_driver_key

    try:
        resolved = resolve_driver_key(existing_driver_key)
    except (ModelBuildContextError, KeyError):
        # Unresolvable key means "no canonical alias"; owner-typed faults from
        # a broken mapping or template load propagate instead of defaulting.
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
    "DriverAssumptionPlanIdentityUnavailable",
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
