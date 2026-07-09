"""Pure content-addressable hash helpers for FMS store coordination."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from schema.models import FinancialModel
from schema.overrides import overrides_payload


ABSENT_STORE = "absent"
_ROOT_META_VOLATILE_KEYS = {
    "last_updated",
    "created_at",
    "updated_at",
    "written_at",
    "timestamp",
}


def _strip_revision_exclusions(value):
    if isinstance(value, dict):
        return {
            key: _strip_revision_exclusions(child)
            for key, child in value.items()
            if key not in {"source_sha256", "base_results"}
        }
    if isinstance(value, list):
        return [_strip_revision_exclusions(child) for child in value]
    return value


def canonical_model_hash(model: FinancialModel) -> str:
    """Return a deterministic hash of model semantic content only."""

    payload = model.model_dump(mode="json")
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        metadata.pop("created_at", None)
    payload = _strip_revision_exclusions(payload)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def workbook_bundle_hash(model: FinancialModel) -> str:
    from schema.serialization import COMPUTE_ENGINE_VERSION

    return f"{canonical_model_hash(model)}:{COMPUTE_ENGINE_VERSION}"


def xlsx_tripwire_sha(path: str | Path) -> str | None:
    target = Path(path)
    if not target.exists():
        return None
    return hashlib.sha256(target.read_bytes()).hexdigest()


def _strip_overrides_payload_volatiles(payload):
    if isinstance(payload, dict) and isinstance(payload.get("_meta"), dict):
        payload = dict(payload)
        payload["_meta"] = {
            key: value
            for key, value in payload["_meta"].items()
            if key not in _ROOT_META_VOLATILE_KEYS
        }
    return payload


def _canonical_json_sha(payload) -> str:
    payload = _strip_overrides_payload_volatiles(payload)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def overrides_store_hash(path: str | Path) -> str:
    target = Path(path)
    if not target.exists():
        return ABSENT_STORE

    return _canonical_json_sha(json.loads(target.read_bytes()))


def overrides_payload_hash(overrides) -> str:
    return _canonical_json_sha(overrides_payload(overrides))


def cas_vector_members(
    subcommand: str,
    *,
    forecast_mode: str | None = None,
    projection_store_changed: bool | None = None,
) -> tuple[str, ...]:
    if subcommand in {
        "persist_dcf_relative_valuation",
        "persist_scenario_multiple_pricing",
    }:
        return ("workbook",)
    if subcommand == "persist_valuation_inputs":
        return ("workbook", "valuation_override")
    if subcommand == "persist_forecast_assumptions":
        if forecast_mode == "main_write":
            return ("workbook", "projection_override")
        if forecast_mode == "prune":
            if projection_store_changed is True:
                return ("projection_override",)
            if projection_store_changed is False:
                return ()
            raise ValueError(
                "projection_store_changed is required for persist_forecast_assumptions prune"
            )
        if forecast_mode in {"no_write", "noop_verify"}:
            return ()
        raise ValueError(
            "forecast_mode is required for persist_forecast_assumptions CAS membership"
        )
    if subcommand == "persist_earnings_scenarios":
        return ("workbook", "projection_override")
    if subcommand == "persist_model_update":
        if projection_store_changed is True:
            return ("workbook", "projection_override")
        return ("workbook",)
    raise ValueError(f"unknown subcommand for CAS membership: {subcommand!r}")


def journal_content_id(
    subcommand: str,
    *,
    target_store_hashes,
    target_keys=(),
    forecast_mode: str | None = None,
    projection_store_changed: bool | None = None,
    content_discriminator: str | None = None,
) -> str:
    members = cas_vector_members(
        subcommand,
        forecast_mode=forecast_mode,
        projection_store_changed=projection_store_changed,
    )
    if set(target_store_hashes) != set(members):
        raise ValueError(
            "target_store_hashes keys must match CAS vector members for the branch"
        )

    keys = sorted(str(key) for key in target_keys)
    payload = {
        "version": 1,
        "subcommand": subcommand,
        "stores": dict(sorted(target_store_hashes.items())),
        "keys": keys,
    }
    if content_discriminator is not None:
        discriminator = str(content_discriminator).strip()
        if not discriminator:
            raise ValueError("content_discriminator must be non-empty when provided")
        payload["content_discriminator"] = discriminator
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
