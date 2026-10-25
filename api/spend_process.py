"""Attribute every API-spend ledger row written by this process to its gateway instance.

``configure_api_spend_process`` is called once at process entry (gateway
``api/main.py``, autonomous worker ``api/agent/autonomous/entry.py``). The ledger
and its field resolution belong to ``model_authority.spend_ledger``; this module
only derives ``instance_id`` and ``git_sha`` from the environment the local
gateway launchers already export and reuses ``code_freshness``'s boot identity.
Contract: ``docs/reference/api-spend-ledger.md``.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

import code_freshness
from model_authority.spend_ledger import (
  ApiCategory,
  AuthKind,
  SpendLedgerUnavailable,
  configure_spend_process,
  record_api_spend,
)
from model_authority.spend_settings import SpendCeilingExceeded, SpendSettingsError, enforce_spend_ceilings

SPEND_REPO = "ai-excel-addin"
_SHARED_PORT = 8000
_ISOLATED_ROOT_DIRNAME = "isolated-gateways"
# A paid call is refused (never treated as unlimited) when a ceiling is hit, the
# ledger cannot be read, or the operator settings file is malformed.
SPEND_REFUSALS = (SpendCeilingExceeded, SpendLedgerUnavailable, SpendSettingsError)


def _gateway_port(env: Mapping[str, str]) -> int | None:
  raw = str(env.get("RESEARCH_GATEWAY_PORT") or "").strip()
  if not raw:
    try:
      raw = str(urlsplit(str(env.get("GATEWAY_BASE_URL") or "")).port or "")
    except ValueError:
      raw = ""
  if not raw:
    # The shared pair's launcher defaults RESEARCH_GATEWAY_PORT to 8000 and
    # exports no port of its own.
    return _SHARED_PORT if str(env.get("LOCAL_GATEWAY_RUNTIME_ROOT") or "").strip() else None
  try:
    return int(raw)
  except ValueError:
    return None


def derive_instance_id(env: Mapping[str, str] | None = None, *, pid: int | None = None) -> str:
  """Name the serving instance: operator override, isolated:<port>, shared:8000 or pid:<pid>."""

  env = os.environ if env is None else env
  override = str(env.get("HANK_SPEND_INSTANCE_ID") or "").strip()
  if override:
    return override
  runtime_root = str(env.get("LOCAL_GATEWAY_RUNTIME_ROOT") or "").strip()
  if runtime_root:
    port = _gateway_port(env)
    if port is not None:
      private_root = _ISOLATED_ROOT_DIRNAME in Path(runtime_root).parts
      if private_root or port != _SHARED_PORT:
        return f"isolated:{port}"
      return f"shared:{_SHARED_PORT}"
  return f"pid:{os.getpid() if pid is None else pid}"


def derive_git_sha() -> str | None:
  """The commit this process booted, as ``code_freshness`` captured it."""

  boot = code_freshness.snapshot().get("boot_runtime")
  if not isinstance(boot, Mapping):
    return None
  sha = boot.get("source_commit") or boot.get("ai_revision")
  return str(sha) if sha else None


def configure_api_spend_process(*, service: str) -> None:
  """Stamp repo, service, instance and commit onto every ledger row this process writes."""

  configure_spend_process(
    repo=SPEND_REPO,
    service=service,
    instance_id=derive_instance_id(),
    git_sha=derive_git_sha(),
  )


def enforce_vendor_ceilings(
  *,
  api_category: ApiCategory,
  provider: str,
  model_or_endpoint: str,
  auth_kind: AuthKind,
  credential_fingerprint: str | None,
  call_site: str,
) -> None:
  """Refuse a paid upstream call over its ceiling, recording the refusal as a budget_blocked row.

  Raises one of ``SPEND_REFUSALS``: a ceiling breach, or a PAYG check that cannot
  read the ledger or settings, which is a stop, never unlimited.
  """

  try:
    enforce_spend_ceilings(
      provider=provider,
      auth_kind=auth_kind,
      credential_fingerprint=credential_fingerprint,
    )
  except SPEND_REFUSALS:
    record_api_spend(
      api_category=api_category,
      provider=provider,
      model_or_endpoint=model_or_endpoint,
      auth_kind=auth_kind,
      credential_fingerprint=credential_fingerprint,
      call_site=call_site,
      api_calls=0,
      status="budget_blocked",
    )
    raise
