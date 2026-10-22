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


def overrides_payload_hash(overrides, *, provider_registry=None) -> str:
    return _canonical_json_sha(
        overrides_payload(
            overrides,
            provider_registry=provider_registry,
        )
    )


def cas_vector_members(subcommand: str) -> tuple[str, ...]:
    if subcommand == "diligence_s4_proposal_apply":
        return ("diligence-s4-application", "diligence-s4-handoff")
    if subcommand == "build_model_render_commit":
        return ("build_sidecar", "build_workbook")
    if subcommand in {
        "persist_dcf_relative_valuation",
        "persist_scenario_multiple_pricing",
    }:
        return (
            "build_sidecar",
            "workbook",
            "valuation_override",
        )
    if subcommand == "persist_model_update":
        return ("build_sidecar", "workbook")
    raise ValueError(f"unknown subcommand for CAS membership: {subcommand!r}")


def journal_content_id(
    subcommand: str,
    *,
    target_store_hashes,
    target_keys=(),
    content_discriminator: str | None = None,
    operation_kind: str | None = None,
) -> str:
    supplied_members = set(target_store_hashes)
    if operation_kind is None:
        members = cas_vector_members(subcommand)
        expected_members = set(members)
        valid_membership = supplied_members == expected_members
    elif operation_kind == "gateway_model_state_mutation":
        if subcommand == "bma112_offline_model_state_import":
            raise ValueError(
                "offline model-state import is not a gateway mutation subcommand"
            )
        members = set(cas_vector_members(subcommand))
        expected_members = members | {"canonical-model-state"}
        valid_membership = supplied_members == expected_members
    elif operation_kind == "bma112_offline_model_state_import":
        if subcommand != "bma112_offline_model_state_import":
            raise ValueError(
                "offline model-state operation requires its exact import subcommand"
            )
        expected_members = {"canonical-model-state"}
        valid_membership = supplied_members == expected_members
    elif operation_kind == "workspace_preview_model_state_mutation":
        if subcommand == "bma112_offline_model_state_import":
            raise ValueError(
                "offline model-state import is not a workspace-preview subcommand"
            )
        members = set(cas_vector_members(subcommand))
        expected_members = members | {"workspace-preview-model-state"}
        valid_membership = supplied_members == expected_members
    else:
        raise ValueError(f"unknown canonical operation kind: {operation_kind!r}")
    if not valid_membership:
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
