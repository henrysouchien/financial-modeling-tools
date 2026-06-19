from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Iterator

from .driver_resolver import RAW_PREFIX, _find_validation_item, load_driver_mapping
from .model_build_context import ModelBuildContext
from .model_build_context_errors import InvalidDriverKey
from .models import ItemType

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry


def _iter_driver_keys(mbc: ModelBuildContext) -> Iterator[str]:
    for driver_key in mbc.drivers.keys():
        yield driver_key
    for scenario in mbc.scenarios.values():
        for driver_key in scenario.overrides.keys():
            yield driver_key


def _validate_template_input_item(driver_key: str, item_id: str, *, reason_prefix: str) -> None:
    try:
        _, item = _find_validation_item(item_id)
    except KeyError as exc:
        raise InvalidDriverKey(driver_key, f"{reason_prefix}: target missing from template ({item_id})") from exc
    if item.item_type != ItemType.input:
        raise InvalidDriverKey(
            driver_key,
            f"{reason_prefix}: target is not ItemType.input ({item.item_type.value})",
        )


def _validate_raw_driver_key(driver_key: str) -> None:
    literal = driver_key[len(RAW_PREFIX):].strip()
    if not literal:
        raise InvalidDriverKey(driver_key, "raw: target missing from template")
    _validate_template_input_item(driver_key, literal, reason_prefix="raw")


def _validate_mapped_driver_key(driver_key: str) -> None:
    try:
        mapping = load_driver_mapping()
    except ValueError as exc:
        raise InvalidDriverKey(driver_key, f"driver_mapping.yaml invalid: {exc}") from exc

    item_id = mapping.get(driver_key)
    if item_id is None:
        raise InvalidDriverKey(driver_key, "not in driver_mapping.yaml")
    _validate_template_input_item(driver_key, item_id, reason_prefix="mapping")


def _validate_compiled_input_item(
    driver_key: str,
    item_id: str,
    compiled_registry: "CompiledDriverRegistry",
) -> None:
    try:
        item = compiled_registry.validation_model.get_item(item_id)
    except KeyError as exc:
        raise InvalidDriverKey(
            driver_key,
            f"compiled business model: target missing from validation model ({item_id})",
        ) from exc
    if item.item_type != ItemType.input:
        raise InvalidDriverKey(
            driver_key,
            f"compiled business model: target is not ItemType.input ({item.item_type.value})",
        )


def validate_phase1(
    mbc: ModelBuildContext,
    *,
    compiled_registry: "CompiledDriverRegistry | None" = None,
) -> None:
    for driver_key in _iter_driver_keys(mbc):
        normalized = str(driver_key).strip()
        if normalized.startswith(RAW_PREFIX):
            _validate_raw_driver_key(normalized)
            continue
        if compiled_registry is not None and normalized in compiled_registry.driver_keys:
            _validate_compiled_input_item(
                normalized,
                compiled_registry.driver_keys[normalized],
                compiled_registry,
            )
            continue
        _validate_mapped_driver_key(normalized)


__all__ = ["validate_phase1"]
