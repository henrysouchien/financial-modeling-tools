"""Authored coverage-profile loader — registry artifacts resolved fail-closed.

This is the contract-decoding companion of
:mod:`schema.model_kind_registry`. That module owns registry lookup and its
failure vocabulary; this module resolves the selected artifact through the one
typed ``AnnualCoverageProfile`` contract. See ``schema/README.md`` and
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``.

``artifact:<file name>`` is decoded fail-closed, refusing with
``model_kind_profile_artifact_invalid`` and naming the first failing field.
This module names no issuer (plan §11).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from .coverage_profile import AnnualCoverageProfile
from .model_kind_registry import (
    AUTHORED_ARTIFACT_ARM,
    ModelKindEntry,
    ModelKindProfileArtifactInvalid,
    ModelKindRegistry,
    ModelKindRegistryInvalid,
    artifact_path,
    load_model_kind_registry,
    resolve_model_kind,
)


def _first_failing_field(exc: ValidationError) -> tuple[str, str]:
    """The first error's location and message, for the typed refusal."""

    errors = exc.errors()
    if not errors:  # pragma: no cover - pydantic always reports at least one
        return "<payload>", str(exc)
    first = errors[0]
    location = ".".join(str(part) for part in first.get("loc", ())) or "<payload>"
    return location, str(first.get("msg", "invalid"))


def load_authored_coverage_profile(
    entry: ModelKindEntry, *, registry: ModelKindRegistry
) -> AnnualCoverageProfile:
    """Decode one ``artifact:`` row's authored profile, fail-closed.

    Refuses — never falls back — on a missing or unreadable artifact, bytes
    that are not JSON, a payload the ``AnnualCoverageProfile`` contract rejects
    (including an unknown key: the contract forbids extras), and a focal ticker
    that disagrees with the row that named the artifact. The last check is the
    artifact arm's own affinity rule: an authored profile claiming a different
    issuer than its registry row is authored data disagreeing with itself.

    A malformed capability section inside the payload raises the capability
    record's own ``capability_declaration_invalid`` refusal, which is more
    specific than this one and propagates unchanged (it is deliberately not a
    ``ValueError``, so no clause here can swallow it).
    """

    path = artifact_path(entry, registry=registry)
    ref = entry.profile_ref.ref
    try:
        raw = path.read_bytes()
    except OSError as exc:  # pragma: no cover - lstat already proved it readable
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=ref,
            field="<file>",
            detail=f"profile artifact is unreadable: {exc}",
        ) from exc
    try:
        payload: Any = json.loads(raw)
    except ValueError as exc:
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=ref,
            field="<file>",
            detail=f"profile artifact is not valid JSON: {exc}",
        ) from exc
    if not isinstance(payload, dict):
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=ref,
            field="<payload>",
            detail=f"expected an object, got {type(payload).__name__}",
        )
    try:
        profile = AnnualCoverageProfile.model_validate(payload)
    except ValidationError as exc:
        field, detail = _first_failing_field(exc)
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=ref, field=field, detail=detail
        ) from exc
    if profile.focal_ticker != entry.ticker:
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=ref,
            field="focal_ticker",
            detail=(
                f"{profile.focal_ticker!r} does not match the registry row's "
                f"ticker {entry.ticker!r}"
            ),
        )
    return profile


def resolve_model_kind_coverage_profile(
    *,
    ticker: str,
    corpus_id: str,
    registry: ModelKindRegistry | None = None,
) -> AnnualCoverageProfile:
    """Resolve one verified corpus to its model kind's coverage profile.

    Total over the registry: an unregistered ``(ticker, corpus_id)`` pair is
    ``model_kind_unregistered``, never a silent default and never a ride on
    another model kind's profile.
    """

    table = registry or load_model_kind_registry()
    entry = resolve_model_kind(ticker, corpus_id, registry=table)
    if entry.profile_ref.arm != AUTHORED_ARTIFACT_ARM:  # pragma: no cover - decoded
        raise ModelKindRegistryInvalid(
            source=table.source.name,
            field=f"{entry.kind}.profile_ref",
            detail=f"unknown profile-ref arm {entry.profile_ref.arm!r}",
        )
    return load_authored_coverage_profile(entry, registry=table)


__all__ = [
    "load_authored_coverage_profile",
    "resolve_model_kind_coverage_profile",
]
