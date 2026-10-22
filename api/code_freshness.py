"""Revision freshness for the immutable local gateway runtime.

The gateway captures the selected runtime's commit identity at import. Health
checks compare that boot identity with the runtime selected by the atomic
active-version pointer. Mutable checkout mtimes are intentionally not a
freshness signal: supervised processes never import from a working tree.

IMPORT-ORDER CONTRACT: ``api/main.py`` imports this module before any other
project module so the boot identity is captured before application imports.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeGuard


_BOOT_TS = time.time()
_REPO_ROOT = Path(__file__).resolve().parents[1]
_CACHE_TTL_SECONDS = 30.0
_DEPLOYED_SOURCE_COMMIT = os.getenv(
  "AGENT_GATEWAY_SOURCE_COMMIT", ""
).strip()
# The local-gateway bootstrap exports the selected runtime identity alongside
# AGENT_GATEWAY_SOURCE_COMMIT; deploy readiness verifies the ai revision and,
# when the snapshot is paired, the Risk revision.
_DEPLOYED_AI_REVISION = os.getenv("GATEWAY_RUNTIME_AI_REVISION", "").strip()
_DEPLOYED_RISK_REVISION = os.getenv("GATEWAY_RUNTIME_RISK_REVISION", "").strip()
# The annual-release publication graph (runtime_release_sha256 /
# verified_runtime_release_sha256 / boot_release_sha256) was retired with the
# model finalization graph (8e7d4488c). Freshness reads the active-version
# pointer and the manifest's selected revisions directly.

_cached_snapshot: dict[str, Any] | None = None
_cached_at = 0.0

def repo_root() -> Path:
  return _REPO_ROOT


def _iso(ts: float) -> str:
  return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def boot_manifest_path() -> Path:
  configured = os.getenv("LOCAL_GATEWAY_RUNTIME_MANIFEST", "").strip()
  if configured:
    return Path(configured).expanduser()
  return _REPO_ROOT.parent / "manifest.json"


def _active_version_dir(runtime_root: Path) -> Path:
  root = runtime_root.expanduser()
  active = root / "control" / "active"
  try:
    info = active.lstat()
  except OSError as exc:
    raise OSError(f"cannot resolve active runtime version: {active}: {exc}") from exc
  if not active.is_symlink():
    raise OSError(f"cannot resolve active runtime version: {active} is not a symlink")
  if info.st_uid != os.getuid():
    raise OSError(
      f"cannot resolve active runtime version: {active} is not owned by uid {os.getuid()}"
    )
  raw_target = Path(os.readlink(active))
  name = raw_target.name
  if (
    raw_target.is_absolute()
    or raw_target.parts != ("..", "versions", name)
    or not name
    or name.startswith(".")
  ):
    raise OSError(
      f"cannot resolve active runtime version: {active} has invalid target {raw_target}"
    )
  return (active.parent / raw_target).resolve()


def active_manifest_path() -> Path:
  runtime_root = os.getenv("LOCAL_GATEWAY_RUNTIME_ROOT", "").strip()
  if not runtime_root:
    return boot_manifest_path()
  return _active_version_dir(Path(runtime_root)) / "manifest.json"


def _is_commit(value: object) -> TypeGuard[str]:
  return bool(
    isinstance(value, str)
    and len(value) == 40
    and all(character in "0123456789abcdef" for character in value)
  )


def _manifest_revisions(payload: dict[str, Any]) -> tuple[str, str | None]:
  sources = payload.get("sources")
  if isinstance(sources, dict) and isinstance(sources.get("ai-excel-addin"), dict):
    ai_revision = sources["ai-excel-addin"].get("revision")
    risk_source = sources.get("risk_module")
    if risk_source is None:
      risk_revision = None
    elif isinstance(risk_source, dict):
      risk_revision = risk_source.get("revision")
    else:
      raise OSError("runtime manifest sources are invalid")
  else:
    ai_revision = payload.get("ai_revision")
    risk_revision = payload.get("risk_revision")
  if not _is_commit(ai_revision):
    raise OSError("runtime manifest has invalid ai revision")
  if risk_revision is not None and not _is_commit(risk_revision):
    raise OSError("runtime manifest has invalid risk revision")
  return ai_revision, risk_revision


def _runtime_identity(path: Path) -> dict[str, Any]:
  try:
    payload = json.loads(path.read_text(encoding="utf-8"))
  except (OSError, ValueError) as exc:
    raise OSError(f"cannot read runtime manifest {path}: {exc}") from exc
  if not isinstance(payload, dict):
    raise OSError(f"cannot read runtime manifest {path}: JSON contract must be an object")
  try:
    ai_revision, risk_revision = _manifest_revisions(payload)
  except OSError as exc:
    raise OSError(f"cannot read runtime manifest {path}: {exc}") from exc
  expected = (
    f"ai-{ai_revision}_risk-{risk_revision}" if risk_revision else f"ai-{ai_revision}"
  )
  if payload.get("version") != expected:
    raise OSError(
      f"cannot read runtime manifest {path}: version does not match source revisions"
    )
  return {
    "version": payload["version"],
    "ai_revision": ai_revision,
    "risk_revision": risk_revision,
  }

def _capture_boot_runtime() -> tuple[dict[str, str] | None, str | None]:
  path = boot_manifest_path()
  try:
    return _runtime_identity(path), None
  except OSError as exc:
    return None, str(exc)


_BOOT_RUNTIME, _BOOT_RUNTIME_ERROR = _capture_boot_runtime()


def deployment_identity() -> dict[str, Any]:
  """Return the commit selected by the production service configuration."""

  if _is_commit(_DEPLOYED_SOURCE_COMMIT):
    return {
      "status": "reported",
      "source_commit": _DEPLOYED_SOURCE_COMMIT,
      "source": "AGENT_GATEWAY_SOURCE_COMMIT",
    }
  return {
    "status": "local" if (_REPO_ROOT / ".git").exists() else "unavailable",
    "source_commit": None,
    "source": None,
  }


def snapshot(*, force: bool = False) -> dict[str, Any]:
  """Return revision-based boot-versus-active freshness."""

  global _cached_snapshot, _cached_at
  now = time.time()
  if not force and _cached_snapshot is not None and (now - _cached_at) < _CACHE_TTL_SECONDS:
    return _cached_snapshot

  if _is_commit(_DEPLOYED_SOURCE_COMMIT):
    identity: dict[str, Any] = {"source_commit": _DEPLOYED_SOURCE_COMMIT}
    # An unpaired runtime exports the ai revision and substitutes "live" (or
    # leaves the Risk revision empty). Readiness still has to see the ai
    # revision; only a real Risk commit is a Risk pin.
    if _is_commit(_DEPLOYED_AI_REVISION):
      identity["ai_revision"] = _DEPLOYED_AI_REVISION
      identity["risk_revision"] = (
        _DEPLOYED_RISK_REVISION if _is_commit(_DEPLOYED_RISK_REVISION) else None
      )
    result = {
      "stale": False,
      "source": "deployment_environment",
      "boot_ts": _iso(_BOOT_TS),
      "boot_manifest": None,
      "active_manifest": None,
      "boot_runtime": identity,
      "active_runtime": dict(identity),
      "checked_at": _iso(now),
      "error": None,
    }
    _cached_snapshot = result
    _cached_at = now
    return result

  try:
    current_path = active_manifest_path()
    selection_error = None
  except OSError as exc:
    current_path = Path("<unresolved-active-version>")
    selection_error = str(exc)
  result: dict[str, Any] = {
    "stale": None,
    "source": "active_runtime_revisions",
    "boot_ts": _iso(_BOOT_TS),
    "boot_manifest": str(boot_manifest_path()),
    "active_manifest": str(current_path),
    "boot_runtime": _BOOT_RUNTIME,
    "active_runtime": None,
    "checked_at": _iso(now),
    "error": _BOOT_RUNTIME_ERROR or selection_error,
  }
  if _BOOT_RUNTIME is not None and selection_error is None:
    try:
      active = _runtime_identity(current_path)
    except OSError as exc:
      result["error"] = str(exc)
    else:
      result["active_runtime"] = active
      result["stale"] = active != _BOOT_RUNTIME
      result["error"] = None

  _cached_snapshot = result
  _cached_at = now
  return result


__all__ = [
  "boot_manifest_path",
  "deployment_identity",
  "active_manifest_path",
  "repo_root",
  "snapshot",
]
