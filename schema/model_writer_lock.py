"""Cross-process flock for physical model-writer stores."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Iterator, TextIO


_LOCK_POLL_INTERVAL_SECONDS = 0.05
_MODEL_EXPORT_RE = re.compile(r"^model_\d+_v\d+\.xlsx$")
_FALLBACK_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")
_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ResolvedTarget:
    key: str
    lock_path: Path
    shape: str
    target_path: str
    scope_root: str | None
    ticker: str | None


@dataclass
class _HeldLock:
    handle: TextIO
    previous_content: str
    lock_path: Path
    refcount: int = 1


_REGISTRY_LOCK = threading.Lock()
_KEY_RLOCKS: dict[str, Any] = {}
_HELD_KEYS: dict[str, _HeldLock] = {}
_ORDER_LOCAL = threading.local()


class ModelWriterLockTimeout(TimeoutError):
    """Raised when a model-writer store lock cannot be acquired in time."""

    def __init__(
        self,
        message: str,
        *,
        lock_path: Path | None = None,
        timeout_seconds: float | None = None,
        holder: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.lock_name = lock_path.name if lock_path is not None else None
        self.lock_path = lock_path
        self.timeout_seconds = timeout_seconds
        self.holder = holder


def _flock_enabled() -> bool:
    return os.getenv("FMS_MODEL_WRITER_FLOCK", "1").strip().lower() not in {"0", "false", "no", "off"}


def _lock_order_assert_enabled() -> bool:
    return os.getenv("FMS_LOCK_ORDER_ASSERT", "0").strip().lower() in {"1", "true", "yes", "on"}


def _ticker_re() -> re.Pattern[str]:
    try:
        from research.artifact_paths import TICKER_RE

        return TICKER_RE
    except Exception:
        try:
            from api.research.artifact_paths import TICKER_RE

            return TICKER_RE
        except Exception:
            return _FALLBACK_TICKER_RE


def _normalize_timeout_seconds(timeout_seconds: float | None) -> float:
    if timeout_seconds is None:
        timeout_seconds = os.getenv("FMS_MODEL_WRITER_LOCK_TIMEOUT_SECONDS", "30").strip()
    normalized = float(timeout_seconds)
    if normalized < 0:
        raise ValueError("timeout_seconds must be >= 0")
    return normalized


def _resolved_path(path: str | os.PathLike[str]) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _run_scoped_target(path: Path) -> tuple[Path, str, str] | None:
    parts = path.parts
    try:
        index = parts.index("model_workspaces")
    except ValueError:
        return None
    if len(parts) <= index + 2:
        _log.warning("invalid run-scoped model-writer path shape: %s", path)
        return None
    ticker = str(parts[index + 2]).strip().upper()
    if not _ticker_re().match(ticker):
        _log.warning("invalid run-scoped model-writer ticker segment: %s", path)
        return None
    key = Path(*parts[: index + 3])
    scope_root = Path(*parts[: index + 2])
    return key, str(scope_root), ticker


def _lock_path_for_key(key: Path, shape: str) -> Path:
    if shape == "run_scoped":
        return key / ".locks" / "model_writer.lock"
    digest = hashlib.sha1(key.name.encode("utf-8")).hexdigest()[:16]
    return key.parent / ".locks" / f"model_writer_{digest}.lock"


def _resolve_target(path: str | os.PathLike[str]) -> _ResolvedTarget:
    target = _resolved_path(path)
    run_scoped = _run_scoped_target(target)
    if run_scoped is not None:
        key_path, scope_root, ticker = run_scoped
        shape = "run_scoped"
    elif "model_workspaces" not in target.parts and _MODEL_EXPORT_RE.match(target.name):
        key_path = target
        scope_root = str(target.parent)
        ticker = None
        shape = "export"
    else:
        key_path = target
        scope_root = str(target.parent)
        ticker = _ticker_from_filename(target)
        shape = "file"
    return _ResolvedTarget(
        key=str(key_path),
        lock_path=_lock_path_for_key(key_path, shape),
        shape=shape,
        target_path=str(target),
        scope_root=scope_root,
        ticker=ticker,
    )


def _ticker_from_filename(path: Path) -> str | None:
    if path.suffix.lower() != ".json":
        return None
    ticker = path.stem.strip().upper()
    return ticker if _ticker_re().match(ticker) else None


def resolve_store_key(path: str | os.PathLike[str]) -> str:
    """Return the shape-normalized physical store key for a target path."""

    return _resolve_target(path).key


def describe_lock(path_or_paths: str | os.PathLike[str] | list[str | os.PathLike[str]] | tuple[str | os.PathLike[str], ...]) -> dict[str, Any]:
    """Describe model-writer lock placement without acquiring it."""

    paths = path_or_paths if isinstance(path_or_paths, (list, tuple)) else (path_or_paths,)
    grouped = _group_targets([_resolve_target(path) for path in paths])
    descriptions = [_description_for_group(key, targets) for key, targets in grouped]
    if len(descriptions) == 1:
        return descriptions[0]
    return {
        "key": [item["key"] for item in descriptions],
        "lock_path": [item["lock_path"] for item in descriptions],
        "shape": "multi",
        "target_paths": [path for item in descriptions for path in item["target_paths"]],
        "locks": descriptions,
    }


@contextmanager
def model_writer_lock(
    *keys: str | os.PathLike[str],
    timeout_seconds: float | None = None,
    ticker: str | None = None,
) -> Iterator[None]:
    """Acquire model-writer store locks in deterministic order.

    The feature flag is checked before any path resolution or filesystem work.
    With the flag off, this is a pure no-op.
    """

    if not _flock_enabled():
        yield
        return

    normalized_timeout = _normalize_timeout_seconds(timeout_seconds)
    grouped = _group_targets([_resolve_target(key) for key in keys])
    acquired: list[str] = []
    try:
        for key, targets in grouped:
            _acquire_registry_key(key, targets, normalized_timeout, ticker=ticker)
            acquired.append(key)
        with _lock_order_scope():
            yield
    finally:
        for key in reversed(acquired):
            _release_registry_key(key)


def assert_model_writer_lock_held(kind: str) -> None:
    """Opt-in lock-order assertion for thesis flocks and SQLite write txns."""

    if not _lock_order_assert_enabled():
        return
    if int(getattr(_ORDER_LOCAL, "depth", 0) or 0) <= 0:
        return
    with _REGISTRY_LOCK:
        held = bool(_HELD_KEYS)
    if not held:
        raise AssertionError(f"model-writer lock must be held before {kind}")


def model_writer_lock_held_keys() -> tuple[str, ...]:
    with _REGISTRY_LOCK:
        return tuple(sorted(_HELD_KEYS))


@contextmanager
def _lock_order_scope() -> Iterator[None]:
    if not _lock_order_assert_enabled():
        yield
        return
    current = int(getattr(_ORDER_LOCAL, "depth", 0) or 0)
    _ORDER_LOCAL.depth = current + 1
    try:
        yield
    finally:
        _ORDER_LOCAL.depth = current


def _group_targets(targets: list[_ResolvedTarget]) -> list[tuple[str, list[_ResolvedTarget]]]:
    grouped: dict[str, list[_ResolvedTarget]] = {}
    for target in targets:
        grouped.setdefault(target.key, []).append(target)
    return [(key, grouped[key]) for key in sorted(grouped)]


def _description_for_group(key: str, targets: list[_ResolvedTarget]) -> dict[str, Any]:
    first = targets[0]
    return {
        "key": key,
        "lock_path": str(first.lock_path),
        "shape": first.shape,
        "target_paths": sorted({target.target_path for target in targets}),
    }


def _acquire_registry_key(
    key: str,
    targets: list[_ResolvedTarget],
    timeout_seconds: float,
    *,
    ticker: str | None,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    with _REGISTRY_LOCK:
        key_rlock = _KEY_RLOCKS.setdefault(key, threading.RLock())

    if not key_rlock.acquire(timeout=timeout_seconds):
        raise ModelWriterLockTimeout(
            f"timed out acquiring in-process model-writer lock: {targets[0].lock_path.name}",
            lock_path=targets[0].lock_path,
            timeout_seconds=timeout_seconds,
            holder=_read_lock_holder_from_path(targets[0].lock_path),
        )

    try:
        with _REGISTRY_LOCK:
            held = _HELD_KEYS.get(key)
            if held is not None:
                held.refcount += 1
                return

        remaining_timeout = max(0.0, deadline - time.monotonic())
        handle, previous_content = _acquire_lock(
            targets[0].lock_path,
            remaining_timeout,
            targets,
            ticker=ticker,
            reported_timeout_seconds=timeout_seconds,
        )
        with _REGISTRY_LOCK:
            _HELD_KEYS[key] = _HeldLock(
                handle=handle,
                previous_content=previous_content,
                lock_path=targets[0].lock_path,
            )
    except BaseException:
        key_rlock.release()
        raise


def _release_registry_key(key: str) -> None:
    key_rlock = None
    held_to_release: _HeldLock | None = None
    with _REGISTRY_LOCK:
        held = _HELD_KEYS.get(key)
        if held is None:
            raise RuntimeError(f"model-writer lock key not held: {key}")
        key_rlock = _KEY_RLOCKS.get(key)
        held.refcount -= 1
        if held.refcount == 0:
            _HELD_KEYS.pop(key, None)
            held_to_release = held

    try:
        if held_to_release is not None:
            try:
                _restore_lock_content(held_to_release.handle, held_to_release.previous_content)
            finally:
                try:
                    fcntl.flock(held_to_release.handle.fileno(), fcntl.LOCK_UN)
                finally:
                    held_to_release.handle.close()
    finally:
        if key_rlock is not None:
            key_rlock.release()


def _read_lock_holder_from_path(lock_path: Path) -> dict[str, Any] | None:
    try:
        with lock_path.open("r", encoding="utf-8") as lock_handle:
            return _read_lock_holder(lock_handle)
    except OSError:
        return None


def _acquire_lock(
    lock_path: Path,
    timeout_seconds: float,
    targets: list[_ResolvedTarget],
    *,
    ticker: str | None,
    reported_timeout_seconds: float | None = None,
) -> tuple[TextIO, str]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    lock_handle = lock_path.open("a+", encoding="utf-8")
    acquired = False
    try:
        while True:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if time.monotonic() >= deadline:
                    raise ModelWriterLockTimeout(
                        f"timed out acquiring model-writer lock: {lock_path.name}",
                        lock_path=lock_path,
                        timeout_seconds=timeout_seconds if reported_timeout_seconds is None else reported_timeout_seconds,
                        holder=_read_lock_holder(lock_handle),
                    ) from exc
                time.sleep(min(_LOCK_POLL_INTERVAL_SECONDS, max(0.0, deadline - time.monotonic())))
        previous_content = _write_lock_holder(lock_handle, lock_path, targets, ticker=ticker)
        return lock_handle, previous_content
    except BaseException:
        if acquired:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()
        raise


def _read_lock_content(lock_handle: TextIO) -> str:
    lock_handle.seek(0)
    return lock_handle.read()


def _read_lock_holder(lock_handle: TextIO) -> dict[str, Any] | None:
    raw = _read_lock_content(lock_handle).strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw[:500]}
    return payload if isinstance(payload, dict) else {"raw": raw[:500]}


def _write_lock_holder(
    lock_handle: TextIO,
    lock_path: Path,
    targets: list[_ResolvedTarget],
    *,
    ticker: str | None,
) -> str:
    previous_content = _read_lock_content(lock_handle)
    primary = targets[0]
    payload = {
        "pid": os.getpid(),
        "lock_name": lock_path.name,
        "scope_root": primary.scope_root,
        "ticker": str(ticker).strip().upper() if ticker else primary.ticker,
        "target_paths": sorted({target.target_path for target in targets}),
        "acquired_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    lock_handle.seek(0)
    lock_handle.truncate()
    json.dump(payload, lock_handle, sort_keys=True)
    lock_handle.write("\n")
    lock_handle.flush()
    return previous_content


def _restore_lock_content(lock_handle: TextIO, previous_content: str) -> None:
    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(previous_content)
    lock_handle.flush()


__all__ = [
    "ModelWriterLockTimeout",
    "assert_model_writer_lock_held",
    "describe_lock",
    "model_writer_lock",
    "model_writer_lock_held_keys",
    "resolve_store_key",
]
