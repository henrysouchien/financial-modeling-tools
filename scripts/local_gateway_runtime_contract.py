#!/usr/bin/env python3
"""Canonical integrity contract for immutable local gateway runtimes.

This module is deliberately stdlib-only so the bootstrap and isolated Python
children can import it from the selected immutable version tree under ``-I -S``.
"""

from __future__ import annotations

import functools
import grp
import hashlib
import json
import os
import pwd
from pathlib import Path
import plistlib
import re
import stat
import struct
import sys
import sysconfig
from typing import Any, Mapping


MANIFEST_SCHEMA_VERSION = 2
COMMIT_RE = re.compile(r"^[0-9a-f]{40,64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
LAUNCH_PYTHON_ENV = "LOCAL_GATEWAY_PYTHON"
CONTROL_STATE_DIRECTORY_MODE = 0o700
CONTROL_LOG_FILE_MODE = 0o600
CONTROL_LOG_MAX_BYTES = 64 * 1024 * 1024
CONTROL_LOG_DIRECTORY = "logs"
APPLICATION_LOG_DIRECTORY = "application-logs"

LAUNCHD_CONTRACT_PATHS = {
  "local.f91.research_gateway": "ai-excel-addin/deploy/launchd/local.f91.research_gateway.plist",
}


class RuntimeContractError(RuntimeError):
  """An immutable runtime or active-version selection violated its contract."""


def scrub_python_isolation_env(base: Mapping[str, str]) -> dict[str, str]:
  """Copy a launch environment without inherited interpreter or source identity."""

  env = dict(base)
  for name in (
    "AI_EXCEL_ADDIN_GATEWAY_PYTHONPATH",
    "AI_EXCEL_ADDIN_ROOT",
    "LOCAL_GATEWAY_PYTHON",
    "RISK_MODULE_ROOT",
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONUSERBASE",
    "PYTHONSTARTUP",
    "PYTHONINSPECT",
    "PYTHONSAFEPATH",
    "VIRTUAL_ENV",
  ):
    env.pop(name, None)
  # Non-isolated descendants must not load the caller's user site either.
  env["PYTHONNOUSERSITE"] = "1"
  return env


def scrub_bootstrap_env(base: Mapping[str, str]) -> dict[str, str]:
  """Also discard source revision, validation mode, and log authority at bootstrap."""

  env = scrub_python_isolation_env(base)
  for name in (
    "AGENT_GATEWAY_SOURCE_COMMIT",
    "GATEWAY_LOG_DIR",
    "LOCAL_GATEWAY_RUNTIME_VALIDATION_MODE",
    "TOOL_TIMING_DB_PATH",
  ):
    env.pop(name, None)
  return env


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
  result: dict[str, Any] = {}
  for key, value in pairs:
    if key in result:
      raise ValueError(f"duplicate JSON key: {key}")
    result[key] = value
  return result


def read_json_object(path: Path) -> dict[str, Any]:
  try:
    payload = json.loads(
      path.read_text(encoding="utf-8"),
      object_pairs_hook=_reject_duplicate_keys,
    )
  except (OSError, ValueError) as exc:
    raise RuntimeContractError(f"invalid JSON contract {path}: {exc}") from exc
  if not isinstance(payload, dict):
    raise RuntimeContractError(f"JSON contract must be an object: {path}")
  return payload




def _require_owned_mode(
  path: Path,
  *,
  kind: str,
  immutable: bool,
) -> os.stat_result:
  try:
    info = path.lstat()
  except OSError as exc:
    raise RuntimeContractError(f"cannot stat {kind} {path}: {exc}") from exc
  if stat.S_ISLNK(info.st_mode):
    raise RuntimeContractError(f"{kind} must not be a symlink: {path}")
  if info.st_uid != os.getuid():
    raise RuntimeContractError(f"{kind} is not owned by uid {os.getuid()}: {path}")
  if info.st_mode & 0o022:
    raise RuntimeContractError(f"{kind} is group/other writable: {path}")
  if immutable and info.st_mode & 0o200:
    raise RuntimeContractError(f"{kind} is writable instead of immutable: {path}")
  return info


def require_regular_file(path: Path, *, immutable: bool) -> os.stat_result:
  info = _require_owned_mode(path, kind="file", immutable=immutable)
  if not stat.S_ISREG(info.st_mode):
    raise RuntimeContractError(f"path is not a regular file: {path}")
  if info.st_nlink != 1:
    raise RuntimeContractError(f"file must not have hard-link aliases: {path}")
  return info


def require_directory(path: Path, *, immutable: bool) -> os.stat_result:
  info = _require_owned_mode(path, kind="directory", immutable=immutable)
  if not stat.S_ISDIR(info.st_mode):
    raise RuntimeContractError(f"path is not a directory: {path}")
  return info


def _canonical_existing_directory(path: Path, *, kind: str) -> tuple[Path, os.stat_result]:
  if not path.is_absolute():
    raise RuntimeContractError(f"{kind} must be absolute: {path}")
  try:
    resolved = path.resolve(strict=True)
  except OSError as exc:
    raise RuntimeContractError(f"cannot resolve {kind} {path}: {exc}") from exc
  if path != resolved:
    raise RuntimeContractError(f"{kind} must already be canonical with no symlink components: {path}")
  info = _require_owned_mode(path, kind=kind, immutable=False)
  if not stat.S_ISDIR(info.st_mode):
    raise RuntimeContractError(f"{kind} must be a directory: {path}")
  return resolved, info


def _private_directory_details(
  path: Path,
  *,
  kind: str,
) -> tuple[Path, os.stat_result]:
  canonical, info = _canonical_existing_directory(path, kind=kind)
  if stat.S_IMODE(info.st_mode) != CONTROL_STATE_DIRECTORY_MODE:
    raise RuntimeContractError(
      f"{kind} has non-canonical mode (expected 0700): {path}"
    )
  return canonical, info


def _require_private_directory(path: Path, *, kind: str) -> Path:
  canonical, _info = _private_directory_details(path, kind=kind)
  return canonical


def _directory_open_flags() -> int:
  flags = os.O_RDONLY
  if hasattr(os, "O_DIRECTORY"):
    flags |= os.O_DIRECTORY
  if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
  if hasattr(os, "O_CLOEXEC"):
    flags |= os.O_CLOEXEC
  return flags


def _same_opened_path(path: Path, info: os.stat_result, *, kind: str) -> None:
  try:
    current = path.lstat()
  except OSError as exc:
    raise RuntimeContractError(f"cannot restat {kind} {path}: {exc}") from exc
  if stat.S_ISLNK(current.st_mode) or (current.st_dev, current.st_ino) != (
    info.st_dev,
    info.st_ino,
  ):
    raise RuntimeContractError(f"{kind} changed or became a symlink during validation: {path}")


def _ensure_private_child_directory(parent: Path, name: str, *, kind: str) -> Path:
  if not name or name in {".", ".."} or "/" in name or os.sep in name:
    raise RuntimeContractError(f"invalid {kind} name: {name!r}")
  canonical_parent, parent_info = _canonical_existing_directory(
    parent,
    kind=f"{kind} parent",
  )
  parent_fd = os.open(canonical_parent, _directory_open_flags())
  child_fd = -1
  try:
    opened_parent = os.fstat(parent_fd)
    if (opened_parent.st_dev, opened_parent.st_ino) != (
      parent_info.st_dev,
      parent_info.st_ino,
    ):
      raise RuntimeContractError(f"{kind} parent changed during validation: {parent}")
    try:
      os.mkdir(name, CONTROL_STATE_DIRECTORY_MODE, dir_fd=parent_fd)
    except FileExistsError:
      pass
    child = canonical_parent / name
    try:
      child_path_info = child.lstat()
    except OSError as exc:
      raise RuntimeContractError(f"cannot stat {kind} {child}: {exc}") from exc
    if stat.S_ISLNK(child_path_info.st_mode):
      raise RuntimeContractError(f"{kind} must not be a symlink: {child}")
    child_fd = os.open(name, _directory_open_flags(), dir_fd=parent_fd)
    info = os.fstat(child_fd)
    if not stat.S_ISDIR(info.st_mode):
      raise RuntimeContractError(f"{kind} must be a directory: {child}")
    if info.st_uid != os.getuid():
      raise RuntimeContractError(f"{kind} is not owned by uid {os.getuid()}: {child}")
    if stat.S_IMODE(info.st_mode) != CONTROL_STATE_DIRECTORY_MODE:
      raise RuntimeContractError(
        f"{kind} has non-canonical mode (expected 0700): {child}"
      )
    _same_opened_path(child, info, kind=kind)
    if child.resolve(strict=True) != child:
      raise RuntimeContractError(
        f"{kind} must already be canonical with no symlink components: {child}"
      )
    # Flush both the directory inode and its parent entry. This is harmless on
    # an existing directory and closes the creation crash window on first use.
    os.fsync(child_fd)
    os.fsync(parent_fd)
    return child
  except OSError as exc:
    raise RuntimeContractError(f"cannot prepare {kind} below {parent}: {exc}") from exc
  finally:
    if child_fd >= 0:
      os.close(child_fd)
    os.close(parent_fd)


def prepare_local_control_state(
  control_state_root: Path,
  *,
  include_application_logs: bool,
) -> dict[str, Path | None]:
  """Create and authenticate the private mutable local-gateway state tree.

  The root may be created only below an existing canonical caller-owned parent.
  Existing roots are never chmod-repaired: a symlink, owner mismatch, or mode
  drift is a hard failure. Every created/validated child is descriptor-bound
  and fsynced before the path is returned to a launcher.
  """

  raw = control_state_root.expanduser()
  if not raw.is_absolute():
    raise RuntimeContractError(f"control state root must be absolute: {raw}")
  if raw.name in {"", ".", ".."}:
    raise RuntimeContractError(f"control state root has an invalid name: {raw}")
  try:
    raw.lstat()
  except FileNotFoundError:
    _ensure_private_child_directory(
      raw.parent,
      raw.name,
      kind="control state root",
    )
  except OSError as exc:
    raise RuntimeContractError(f"cannot stat control state root {raw}: {exc}") from exc
  root = _require_private_directory(raw, kind="control state root")
  supervisor_logs = _ensure_private_child_directory(
    root,
    CONTROL_LOG_DIRECTORY,
    kind="control supervisor log directory",
  )
  application_logs = (
    _ensure_private_child_directory(
      root,
      APPLICATION_LOG_DIRECTORY,
      kind="gateway application log directory",
    )
    if include_application_logs
    else None
  )
  return {
    "root": root,
    "supervisor_logs": supervisor_logs,
    "application_logs": application_logs,
  }


def require_gateway_application_log_directory(
  control_state_root: Path,
  gateway_log_dir: Path,
) -> Path:
  """Authenticate an inherited application-log path against control state."""

  root = _require_private_directory(
    control_state_root.expanduser(),
    kind="control state root",
  )
  expected = root / APPLICATION_LOG_DIRECTORY
  configured = gateway_log_dir.expanduser()
  if configured != expected:
    raise RuntimeContractError(
      f"GATEWAY_LOG_DIR must be the canonical control-state child {expected}"
    )
  return _require_private_directory(
    configured,
    kind="gateway application log directory",
  )


def open_private_control_log(log_directory: Path, name: str) -> int:
  """Open one bounded append-only supervisor log without following aliases."""

  if not name or name in {".", ".."} or "/" in name or os.sep in name:
    raise RuntimeContractError(f"invalid control log file name: {name!r}")
  directory, validated_directory = _private_directory_details(
    log_directory,
    kind="control supervisor log directory",
  )
  directory_fd = os.open(directory, _directory_open_flags())
  flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
  if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
  if hasattr(os, "O_CLOEXEC"):
    flags |= os.O_CLOEXEC
  fd = -1
  try:
    opened_directory = os.fstat(directory_fd)
    if (opened_directory.st_dev, opened_directory.st_ino) != (
      validated_directory.st_dev,
      validated_directory.st_ino,
    ):
      raise RuntimeContractError(
        f"control supervisor log directory changed during validation: {directory}"
      )
    if (
      not stat.S_ISDIR(opened_directory.st_mode)
      or opened_directory.st_uid != os.getuid()
      or stat.S_IMODE(opened_directory.st_mode) != CONTROL_STATE_DIRECTORY_MODE
    ):
      raise RuntimeContractError(
        f"control supervisor log directory is not a caller-owned 0700 directory: {directory}"
      )
    _same_opened_path(
      directory,
      opened_directory,
      kind="control supervisor log directory",
    )
    fd = os.open(name, flags, CONTROL_LOG_FILE_MODE, dir_fd=directory_fd)
    info = os.fstat(fd)
    path = directory / name
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
      raise RuntimeContractError(f"control log must be one unaliased regular file: {path}")
    if info.st_uid != os.getuid():
      raise RuntimeContractError(f"control log is not owned by uid {os.getuid()}: {path}")
    if stat.S_IMODE(info.st_mode) != CONTROL_LOG_FILE_MODE:
      raise RuntimeContractError(
        f"control log has non-canonical mode (expected 0600): {path}"
      )
    _same_opened_path(path, info, kind="control log")
    # Keep crash loops from multiplying an already-large append-only log on
    # every restart.  Truncating the authenticated inode (rather than renaming
    # it) is safe for inherited O_APPEND descriptors and immediately releases
    # disk blocks.  The scheduled retention sweep applies the same ceiling to
    # a long-lived process that has not restarted.
    if info.st_size > CONTROL_LOG_MAX_BYTES:
      os.ftruncate(fd, 0)
    os.fsync(fd)
    os.fsync(directory_fd)
    opened = fd
    fd = -1
    return opened
  except OSError as exc:
    raise RuntimeContractError(f"cannot open private control log {name}: {exc}") from exc
  finally:
    if fd >= 0:
      os.close(fd)
    os.close(directory_fd)


def require_publication_root(path: Path) -> os.stat_result:
  """Require the canonical final-name publication-envelope mode.

  Darwin requires owner-write on the directory inode being renamed, so the
  envelope remains 0700 after publication.  Manifest-declared source trees
  beneath it are read-only and authenticated by their tree digests; unrelated
  top-level siblings are outside that authenticated product content.
  """

  info = require_directory(path, immutable=False)
  if stat.S_IMODE(info.st_mode) != 0o700:
    raise RuntimeContractError(f"publication root has non-canonical mode: {path}")
  return info


@functools.lru_cache(maxsize=None)
def _group_writers_are_trusted(gid: int) -> bool:
  """Report whether every principal writing via ``gid`` is root or this caller.

  A group-writable interpreter artifact is only dangerous when the group grants
  write access to some principal other than root or the caller. Membership is
  therefore read from the account database that owns it -- supplementary members
  plus every account holding ``gid`` as its primary group -- rather than inferred
  from the caller's own group list, which answers a different question ("am I in
  this group?") and gets the trust decision backwards in both directions.

  An empty writer set is NOT trust. macOS grants groups such as ``everyone`` (12)
  and ``localaccounts`` (61) implicitly: ``gr_mem`` is empty and no account holds
  them as a primary group, yet every local account can write through them. Since
  enumeration cannot prove such a group is narrow, it is refused. ``wheel``
  carries an explicit ``gr_mem`` of ``root`` and so remains trusted.

  Cached per gid: ``pwd.getpwall()`` costs ~5ms and this is reached on every
  child-process verification, while group membership cannot change inside a
  launch that immediately execs.
  """

  try:
    writers = set(grp.getgrgid(gid).gr_mem)
  except KeyError:
    return False
  try:
    accounts = pwd.getpwall()
  except OSError:
    return False
  writers |= {account.pw_name for account in accounts if account.pw_gid == gid}
  try:
    caller = pwd.getpwuid(os.getuid()).pw_name
  except KeyError:
    return False
  if not writers:
    return False
  return not (writers - {"root", caller})


def _require_trusted_python_directory(
  path: Path,
  *,
  kind: str,
  allow_group_write: bool = False,
) -> Path:
  """Validate one interpreter-owned stdlib/dependency allowlist entry.

  These directories are not part of the paired runtime inventory, but they
  are the only non-runtime import roots admitted after ``python -I -S``.
  Root ownership is accepted for system interpreters. Canonical interpreter
  trees may be group writable when every principal that group grants write
  access to is root or this caller; world writable locations, path aliases,
  and groups with any other writer are rejected.
  """

  if not path.is_absolute():
    raise RuntimeContractError(f"{kind} must be absolute: {path}")
  try:
    info = path.lstat()
    resolved = path.resolve(strict=True)
  except OSError as exc:
    raise RuntimeContractError(f"cannot resolve {kind} {path}: {exc}") from exc
  if path != resolved:
    raise RuntimeContractError(f"{kind} must already be canonical: {path}")
  if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
    raise RuntimeContractError(f"{kind} must be a real directory: {path}")
  if info.st_uid not in {0, os.getuid()}:
    raise RuntimeContractError(f"{kind} has an untrusted owner: {path}")
  if info.st_mode & 0o002 or (info.st_mode & 0o020 and not allow_group_write):
    raise RuntimeContractError(f"{kind} has unsafe write permissions: {path}")
  if info.st_mode & 0o020 and not _group_writers_are_trusted(info.st_gid):
    raise RuntimeContractError(f"{kind} is writable by an untrusted group: {path}")
  return resolved


def isolated_python_path(
  application_paths: list[Path] | tuple[Path, ...],
  *,
  require_immutable: bool,
) -> list[str]:
  """Return the complete import path for an authenticated ``-I -S`` child.

  Python's startup path is deliberately discarded by callers.  Every
  application path must exist in the selected runtime; the remaining entries
  are the canonical stdlib directories reported by this exact interpreter and
  the ``site-packages`` of the virtual environment that owns the launch
  Python.  ``-S`` never reads ``pyvenv.cfg``, so the environment is located
  from the launch executable itself.  Adding a site-packages directory
  directly does not execute its ``.pth`` startup hooks because ``site`` is
  never initialized.
  """

  if not sys.flags.isolated or not sys.flags.no_site:
    raise RuntimeContractError("internal Python requires isolated no-site mode (-I -S)")

  result: list[str] = []

  def add(path: Path) -> None:
    value = str(path)
    if value not in result:
      result.append(value)

  for raw in application_paths:
    if not raw.is_absolute():
      raise RuntimeContractError(f"runtime import path must be absolute: {raw}")
    require_directory(raw, immutable=require_immutable)
    resolved = raw.resolve(strict=True)
    if raw != resolved:
      raise RuntimeContractError(f"runtime import path must already be canonical: {raw}")
    add(resolved)

  paths = sysconfig.get_paths()
  stdlib_values = [paths.get("stdlib"), paths.get("platstdlib")]
  if not all(isinstance(value, str) and value for value in stdlib_values):
    raise RuntimeContractError("interpreter stdlib paths are incomplete")
  stdlib_roots: list[Path] = []
  for value in stdlib_values:
    assert isinstance(value, str)
    root = _require_trusted_python_directory(
      Path(value),
      kind="stdlib",
      allow_group_write=True,
    )
    if root not in stdlib_roots:
      stdlib_roots.append(root)
    add(root)
  dynamic = _require_trusted_python_directory(
    stdlib_roots[0] / "lib-dynload",
    kind="stdlib dynamic modules",
    allow_group_write=True,
  )
  add(dynamic)
  environment = Path(sys.executable).resolve(strict=True).parent.parent
  if not (environment / "pyvenv.cfg").is_file():
    raise RuntimeContractError(
      f"launch Python is not a virtual environment interpreter: {sys.executable}"
    )
  dependency_paths = sysconfig.get_paths(
    "venv",
    vars={"base": str(environment), "platbase": str(environment)},
  )
  for key in ("purelib", "platlib"):
    value = dependency_paths.get(key)
    if not isinstance(value, str) or not value:
      raise RuntimeContractError("interpreter dependency-environment paths are incomplete")
    add(_require_trusted_python_directory(Path(value), kind="dependency environment"))
  return result


def dependency_environment_paths() -> list[Path]:
  """Return the launch venv's ``site-packages`` roots for a grandchild PYTHONPATH.

  Fresh ``python3`` children (``code_execute`` host=subprocess) do not inherit
  this process's rebuilt ``sys.path``.  The venv's ``site-packages`` is where
  the installed distributions (``risk_client``, ``portfolio_math``, ``fmp``,
  ``brokerage``, ...) live; the default snapshot installs nothing into a
  version-local ``risk-dists``, so this is the whole travel set.  ``-S`` never
  reads ``pyvenv.cfg``, so the environment is located from the launch
  executable itself.
  """

  environment = Path(sys.executable).resolve(strict=True).parent.parent
  if not (environment / "pyvenv.cfg").is_file():
    raise RuntimeContractError(
      f"launch Python is not a virtual environment interpreter: {sys.executable}"
    )
  dependency_paths = sysconfig.get_paths(
    "venv",
    vars={"base": str(environment), "platbase": str(environment)},
  )
  roots: list[Path] = []
  for key in ("purelib", "platlib"):
    value = dependency_paths.get(key)
    if not isinstance(value, str) or not value:
      raise RuntimeContractError("interpreter dependency-environment paths are incomplete")
    root = _require_trusted_python_directory(Path(value), kind="dependency environment")
    if root not in roots:
      roots.append(root)
  return roots


def ai_source_import_paths(ai_root: Path) -> list[Path]:
  """Return same-revision add-in source roots, before environment packages."""

  return [
    ai_root,
    ai_root / "api",
    ai_root / "packages" / "agent-gateway",
    ai_root / "packages" / "agent-workflow-contracts",
    ai_root / "packages" / "model-authority",
    ai_root / "packages" / "excel-mcp" / "python",
    ai_root / "packages" / "ibkr-relay-client" / "python",
    ai_root / "packages" / "sheets-finance-mcp",
    ai_root / "packages" / "value-semantics-core",
    ai_root / "packages" / "industry-slice-core",
    ai_root / "packages" / "contracts",
  ]


def risk_side_import_paths(version_root: Path) -> list[Path]:
  """Return the import roots every risk-side process of a paired version needs.

  A paired version exists only for ``start --web``, which serves a private Risk
  API from the archived Risk tree.  Risk consumers import the shared value and
  industry contracts and the model authority (``model_authority``); risk
  providers (providers/completion.py, providers/model_authority.py) and the
  commercial usage contract import ``agent_gateway`` at module level, whose
  package surface reaches ``excel_mcp`` and ``model_authority``.  The launch
  environment carries those add-in distributions only as ``.pth`` editable
  installs, which an ``-I -S`` child never executes, so the roots must come
  from the selected version's own trees.  The generation's ``risk-dists``
  precedes the Risk tree and the launch environment's site-packages, so the
  selected Risk distributions win.  The default (unpaired) snapshot carries no
  Risk tree and no ``risk-dists``; the add-in gateway targets take their
  distributions from the launch venv's site-packages instead.
  """

  ai_root = version_root / "ai-excel-addin"
  risk_root = version_root / "risk_module"
  return [
    ai_root / "packages" / "agent-gateway",
    ai_root / "packages" / "agent-workflow-contracts",
    ai_root / "packages" / "model-authority",
    ai_root / "packages" / "excel-mcp" / "python",
    ai_root / "packages" / "value-semantics-core",
    ai_root / "packages" / "industry-slice-core",
    ai_root / "packages" / "contracts",
    ai_root / "risk-dists",
    risk_root / "brokerage-connect",
    risk_root,
  ]


def canonical_runtime_root(path: Path, *, must_exist: bool = True) -> Path:
  raw = path.expanduser()
  if not raw.is_absolute():
    raise RuntimeContractError(f"runtime root must be absolute: {raw}")
  if must_exist:
    require_directory(raw, immutable=False)
  try:
    resolved = raw.resolve(strict=must_exist)
  except OSError as exc:
    raise RuntimeContractError(f"cannot resolve runtime root {raw}: {exc}") from exc
  if must_exist:
    require_directory(resolved, immutable=False)
    if raw != resolved:
      raise RuntimeContractError(f"runtime root must already be canonical: {raw}")
  return resolved


def versions_root(runtime_root: Path) -> Path:
  return canonical_runtime_root(runtime_root) / "versions"


def canonical_version_path(
  runtime_root: Path,
  version: Path,
  *,
  allow_candidate: bool = False,
) -> Path:
  root = canonical_runtime_root(runtime_root)
  raw_parent = root / "versions"
  require_directory(raw_parent, immutable=False)
  expected_parent = raw_parent.resolve(strict=True)
  require_directory(expected_parent, immutable=False)
  if raw_parent != expected_parent:
    raise RuntimeContractError(f"runtime versions root must not be a symlink: {raw_parent}")
  try:
    raw_candidate = version.expanduser()
    require_directory(raw_candidate, immutable=False)
    candidate = raw_candidate.resolve(strict=True)
  except OSError as exc:
    raise RuntimeContractError(f"cannot resolve runtime version {version}: {exc}") from exc
  require_directory(candidate, immutable=False)
  if raw_candidate != candidate:
    raise RuntimeContractError(f"runtime version must already be canonical: {raw_candidate}")
  if candidate.parent != expected_parent:
    raise RuntimeContractError(f"runtime version must be a direct child of {expected_parent}")
  if candidate.name.startswith(".") and not (
    allow_candidate and candidate.name.startswith(".candidate-")
  ):
    raise RuntimeContractError(f"hidden runtime version is not launchable: {candidate}")
  return candidate


def tree_sha256(root: Path, *, require_immutable: bool) -> str:
  root_info = require_directory(root, immutable=require_immutable)
  if require_immutable and stat.S_IMODE(root_info.st_mode) != 0o555:
    raise RuntimeContractError(f"runtime directory has non-canonical mode: {root}")
  digest = hashlib.sha256()
  try:
    paths = sorted(root.rglob("*"), key=lambda item: item.as_posix())
  except OSError as exc:
    raise RuntimeContractError(f"cannot enumerate runtime tree {root}: {exc}") from exc
  digest.update(b"local-gateway-runtime-tree-v2\0")
  digest.update(struct.pack(">Q", len(paths)))
  empty_sha256 = hashlib.sha256(b"").digest()
  for path in paths:
    relative = path.relative_to(root).as_posix().encode("utf-8")
    info = _require_owned_mode(path, kind="runtime path", immutable=require_immutable)
    if stat.S_ISREG(info.st_mode):
      if info.st_nlink != 1:
        raise RuntimeContractError(f"file must not have hard-link aliases: {path}")
      prescribed_mode = 0o555 if info.st_mode & stat.S_IXUSR else 0o444
      if require_immutable and stat.S_IMODE(info.st_mode) != prescribed_mode:
        raise RuntimeContractError(f"runtime file has non-canonical mode: {path}")
      content_digest = hashlib.sha256()
      byte_length = 0
      try:
        with path.open("rb") as handle:
          for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            byte_length += len(chunk)
            content_digest.update(chunk)
      except OSError as exc:
        raise RuntimeContractError(f"cannot hash runtime file {path}: {exc}") from exc
      path_type = b"F"
      content_sha256 = content_digest.digest()
    elif stat.S_ISDIR(info.st_mode):
      prescribed_mode = 0o555
      if require_immutable and stat.S_IMODE(info.st_mode) != prescribed_mode:
        raise RuntimeContractError(f"runtime directory has non-canonical mode: {path}")
      path_type = b"D"
      byte_length = 0
      content_sha256 = empty_sha256
    else:
      raise RuntimeContractError(f"unsupported runtime path type: {path}")
    digest.update(struct.pack(">Q", len(relative)))
    digest.update(relative)
    digest.update(path_type)
    digest.update(struct.pack(">I", prescribed_mode))
    digest.update(struct.pack(">Q", byte_length))
    digest.update(content_sha256)
  return digest.hexdigest()


SOURCE_NAMES = ("ai-excel-addin", "risk_module")


def version_name_for(ai_revision: str, risk_revision: str | None) -> str:
  """Return the version name one or two committed revisions select.

  The default local start is one revision: its bytes are the ai commit alone, so
  its version name carries the ai revision only. ``start --web`` still serves a
  private Risk API from a pinned Risk revision, so a paired snapshot keeps the
  ``ai-<sha>_risk-<sha>`` pair form.
  """

  if risk_revision is None:
    return f"ai-{ai_revision}"
  return f"ai-{ai_revision}_risk-{risk_revision}"


def runtime_identity(payload: Mapping[str, Any]) -> tuple[str, str | None]:
  """Return the manifest's selected revisions: the ai commit and an optional Risk commit.

  ``risk_module`` is present only in a paired snapshot (``start --web``). The
  default snapshot selects the gateway bytes from the ai revision alone, so its
  manifest carries one source and the version name derives from it alone.
  """

  if set(payload) != {"schema_version", "version", "built_at", "sources", "tree_sha256"}:
    raise RuntimeContractError("runtime manifest has unknown or missing fields")
  if payload.get("schema_version") != MANIFEST_SCHEMA_VERSION:
    raise RuntimeContractError("unsupported runtime manifest schema")
  if not isinstance(payload.get("built_at"), str) or not payload["built_at"]:
    raise RuntimeContractError("runtime manifest built_at is invalid")
  sources = payload.get("sources")
  if not isinstance(sources, dict) or not sources:
    raise RuntimeContractError("runtime manifest sources are invalid")
  unknown = set(sources) - set(SOURCE_NAMES)
  if unknown or "ai-excel-addin" not in sources:
    raise RuntimeContractError("runtime manifest has unknown or missing source names")
  for name, source in sorted(sources.items()):
    if not isinstance(source, dict) or set(source) != {
      "revision",
      "worktree_dirty_at_build",
    }:
      raise RuntimeContractError(f"runtime manifest has invalid {name} source contract")
    revision = source.get("revision")
    if not isinstance(revision, str) or not COMMIT_RE.fullmatch(revision):
      raise RuntimeContractError(f"runtime manifest has invalid {name} revision")
    if not isinstance(source.get("worktree_dirty_at_build"), bool):
      raise RuntimeContractError(f"runtime manifest has invalid {name} dirty marker")
  ai_revision = sources["ai-excel-addin"]["revision"]
  risk_source = sources.get("risk_module")
  risk_revision = risk_source["revision"] if risk_source is not None else None
  expected = version_name_for(ai_revision, risk_revision)
  if payload.get("version") != expected:
    raise RuntimeContractError("runtime manifest version does not match source revisions")
  return ai_revision, risk_revision




def _trusted_python_executable(path: Path) -> Path:
  if not path.is_absolute():
    raise RuntimeContractError(f"launch Python must be absolute: {path}")
  try:
    info = path.lstat()
    resolved = path.resolve(strict=True)
  except OSError as exc:
    raise RuntimeContractError(f"cannot resolve launch Python {path}: {exc}") from exc
  if path != resolved:
    raise RuntimeContractError(f"launch Python must already be canonical: {path}")
  if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
    raise RuntimeContractError(f"launch Python must be a real file: {path}")
  if info.st_uid not in {0, os.getuid()}:
    raise RuntimeContractError(f"launch Python has an untrusted owner: {path}")
  if info.st_mode & 0o002:
    raise RuntimeContractError(f"launch Python is world-writable: {path}")
  if info.st_mode & 0o020 and not _group_writers_are_trusted(info.st_gid):
    raise RuntimeContractError(f"launch Python is writable by an untrusted group: {path}")
  if not os.access(path, os.X_OK):
    raise RuntimeContractError(f"launch Python is not executable: {path}")
  return resolved


def _launchd_python_executable(
  version_root: Path,
  *,
  require_immutable: bool,
) -> Path:
  interpreters: set[Path] = set()
  for label, relative in LAUNCHD_CONTRACT_PATHS.items():
    path = version_root / relative
    require_regular_file(path, immutable=require_immutable)
    try:
      with path.open("rb") as handle:
        payload = plistlib.load(handle)
    except (OSError, plistlib.InvalidFileException) as exc:
      raise RuntimeContractError(f"invalid launchd contract {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("Label") != label:
      raise RuntimeContractError(f"launchd contract label mismatch: {path}")
    environment = payload.get("EnvironmentVariables")
    launch_python = environment.get(LAUNCH_PYTHON_ENV) if isinstance(environment, dict) else None
    if not isinstance(launch_python, str) or not launch_python:
      raise RuntimeContractError(f"launchd contract has no pinned Python: {path}")
    program_arguments = payload.get("ProgramArguments")
    if (
      not isinstance(program_arguments, list)
      or len(program_arguments) != 3
      or program_arguments[:2] != ["/bin/sh", "-c"]
      or not isinstance(program_arguments[2], str)
      or f'exec "${LAUNCH_PYTHON_ENV}" -B -I -S ' not in program_arguments[2]
    ):
      raise RuntimeContractError(f"launchd contract does not exec pinned Python: {path}")
    interpreters.add(Path(launch_python))
  if len(interpreters) != 1:
    raise RuntimeContractError("launchd contracts do not pin one shared Python executable")
  return _trusted_python_executable(interpreters.pop())






def launchd_python_executable(version_root: Path, *, require_immutable: bool) -> Path:
  return _launchd_python_executable(
    version_root,
    require_immutable=require_immutable,
  )


def verify_runtime_version(
  runtime_root: Path,
  version: Path,
  *,
  require_immutable: bool = True,
  allow_candidate: bool = False,
  verify_tree_digests: bool = True,
) -> dict[str, Any]:
  candidate = canonical_version_path(runtime_root, version, allow_candidate=allow_candidate)
  if require_immutable:
    require_publication_root(candidate)
  else:
    require_directory(candidate, immutable=False)
  manifest_path = candidate / "manifest.json"
  require_regular_file(manifest_path, immutable=require_immutable)
  payload = read_json_object(manifest_path)
  ai_revision, risk_revision = runtime_identity(payload)
  expected_version = version_name_for(ai_revision, risk_revision)
  if candidate.name != expected_version and not (
    allow_candidate and candidate.name.startswith(".candidate-")
  ):
    raise RuntimeContractError("runtime directory name does not match manifest version")

  expected_digests = payload.get("tree_sha256")
  if not isinstance(expected_digests, dict) or set(expected_digests) != set(
    payload["sources"]
  ):
    raise RuntimeContractError("runtime manifest has invalid tree_sha256 contract")
  for name in sorted(expected_digests):
    require_directory(candidate / name, immutable=require_immutable)
    expected = expected_digests[name]
    if not isinstance(expected, str) or not SHA256_RE.fullmatch(expected):
      raise RuntimeContractError(f"runtime manifest has invalid {name} tree digest")
    if verify_tree_digests:
      actual = tree_sha256(candidate / name, require_immutable=require_immutable)
      if actual != expected:
        raise RuntimeContractError(f"runtime tree digest mismatch for {name}")
  _launchd_python_executable(
    candidate,
    require_immutable=require_immutable,
  )
  return payload


def _active_version_path(runtime_root: Path) -> tuple[Path, Path]:
  root = canonical_runtime_root(runtime_root)
  control = root / "control"
  require_directory(control, immutable=False)
  active = control / "active"
  try:
    info = active.lstat()
  except OSError as exc:
    raise RuntimeContractError(f"active runtime version is missing: {active}: {exc}") from exc
  if not stat.S_ISLNK(info.st_mode):
    raise RuntimeContractError(f"active runtime version must be a symlink: {active}")
  if info.st_uid != os.getuid():
    raise RuntimeContractError(f"active runtime version is not owned by uid {os.getuid()}: {active}")
  raw_target = Path(os.readlink(active))
  if (
    raw_target.is_absolute()
    or len(raw_target.parts) != 3
    or raw_target.parts[:2] != ("..", "versions")
    or not raw_target.parts[2]
    or raw_target.parts[2].startswith(".")
  ):
    raise RuntimeContractError(f"active runtime version has invalid target: {raw_target}")
  version = canonical_version_path(root, active.resolve(strict=True))
  return root, version


def resolve_active_version(
  runtime_root: Path,
  *,
  verify_tree_digests: bool = True,
) -> tuple[Path, dict[str, Any]]:
  root, version = _active_version_path(runtime_root)
  payload = verify_runtime_version(
    root,
    version,
    verify_tree_digests=verify_tree_digests,
  )
  return version, payload




__all__ = [
  "APPLICATION_LOG_DIRECTORY",
  "COMMIT_RE",
  "CONTROL_LOG_DIRECTORY",
  "CONTROL_LOG_FILE_MODE",
  "CONTROL_STATE_DIRECTORY_MODE",
  "LAUNCH_PYTHON_ENV",
  "LAUNCHD_CONTRACT_PATHS",
  "MANIFEST_SCHEMA_VERSION",
  "RuntimeContractError",
  "SOURCE_NAMES",
  "ai_source_import_paths",
  "canonical_runtime_root",
  "canonical_version_path",
  "dependency_environment_paths",
  "isolated_python_path",
  "launchd_python_executable",
  "open_private_control_log",
  "prepare_local_control_state",
  "read_json_object",
  "require_directory",
  "require_gateway_application_log_directory",
  "require_publication_root",
  "require_regular_file",
  "resolve_active_version",
  "risk_side_import_paths",
  "runtime_identity",
  "scrub_bootstrap_env",
  "scrub_python_isolation_env",
  "tree_sha256",
  "verify_runtime_version",
  "version_name_for",
]
