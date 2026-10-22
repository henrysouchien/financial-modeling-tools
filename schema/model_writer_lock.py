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
from typing import Any, Iterable, Iterator, TextIO

from schema.model_state_identity import ModelStateWriteContext, encode_uri_segment


_LOCK_POLL_INTERVAL_SECONDS = 0.05
_MODEL_EXPORT_RE = re.compile(r"^model_\d+_v\d+\.xlsx$")
_FALLBACK_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.]{0,14}$")
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


def _reset_lock_state_after_fork_in_child() -> None:
    """Discard inherited bookkeeping without unlocking the parent's flocks."""

    global _HELD_KEYS
    global _KEY_RLOCKS
    global _REGISTRY_LOCK

    for held in tuple(_HELD_KEYS.values()):
        try:
            held.handle.close()
        except OSError:
            pass
    _REGISTRY_LOCK = threading.Lock()
    _KEY_RLOCKS = {}
    _HELD_KEYS = {}


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_lock_state_after_fork_in_child)


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


@dataclass(frozen=True)
class ModelWriterLockKeys:
    """One logical authority key plus the complete sorted physical lock set."""

    context: ModelStateWriteContext
    logical_target: Path
    physical_targets: tuple[Path, ...]

    def __post_init__(self) -> None:
        if type(self.context) is not ModelStateWriteContext:
            raise TypeError("model-writer lock keys require a typed write context")
        if not isinstance(self.logical_target, Path):
            raise TypeError("model-writer logical lock target must be a Path")
        if type(self.physical_targets) is not tuple or any(
            not isinstance(path, Path) for path in self.physical_targets
        ):
            raise TypeError("model-writer physical lock targets must be a Path tuple")
        expected = tuple(
            sorted(
                set(self.physical_targets),
                key=lambda path: (_resolve_target(path).key, str(path)),
            )
        )
        if self.physical_targets != expected:
            raise ValueError(
                "model-writer physical lock targets must be unique and canonical-order sorted"
            )
        if self.logical_target in self.physical_targets:
            raise ValueError("logical model-writer lock cannot also be a physical target")

    @property
    def targets(self) -> tuple[Path, ...]:
        return (self.logical_target, *self.physical_targets)


def _model_writer_logical_lock_target(
    context: ModelStateWriteContext,
    *,
    user_data_root: Path | None = None,
) -> Path:
    if user_data_root is not None:
        if not isinstance(user_data_root, Path) or not user_data_root.is_absolute():
            raise TypeError("model-writer user_data_root must be one absolute Path")
        data_root = user_data_root
    else:
        configured = os.getenv("USER_DATA_DIR", "").strip()
        if configured:
            data_root = Path(configured).expanduser()
        else:
            test_root = os.getenv("MODEL_WRITER_LOCK_TEST_ROOT", "").strip()
            if os.getenv("ENVIRONMENT", "").strip().lower() != "test" or not test_root:
                raise RuntimeError(
                    "model-writer logical locks require authoritative USER_DATA_DIR"
                )
            data_root = Path(test_root).expanduser()
    user_segment = encode_uri_segment(context.user_id, field_name="user_id")
    return (
        data_root
        / "users"
        / user_segment
        / ".locks"
        / "model_writer_logical"
        / f"{context.ticker}.lock-target"
    ).resolve(strict=False)


def resolve_model_writer_lock_keys(
    context: ModelStateWriteContext,
    prepared_effects: Iterable[str | os.PathLike[str]],
    *,
    user_data_root: Path | None = None,
) -> ModelWriterLockKeys:
    """Resolve the authority-scoped logical flock and complete physical lock set.

    ``prepared_effects`` is the already prepared set of physical workbook,
    store, and render targets.  Callers must pass the whole set at once; this
    resolver performs no discovery and supplies no path fallback.
    """

    if type(context) is not ModelStateWriteContext:
        raise TypeError("model-writer locks require a typed write context")
    # Re-validate a strict copy before deriving any filesystem identity.
    validated_context = ModelStateWriteContext.model_validate(
        context.model_dump(mode="python", round_trip=True),
        strict=True,
    )
    if validated_context != context:
        raise ValueError("model-writer lock context changed during validation")
    try:
        raw_targets = tuple(prepared_effects)
    except TypeError as exc:
        raise TypeError("prepared model-writer effects must be iterable") from exc
    if any(not isinstance(path, (str, os.PathLike)) for path in raw_targets):
        raise TypeError(
            "prepared model-writer effects must contain only exact str or Path targets"
        )
    physical_targets = tuple(
        sorted(
            {_resolved_path(path) for path in raw_targets},
            key=lambda path: (_resolve_target(path).key, str(path)),
        )
    )
    return ModelWriterLockKeys(
        context=context,
        logical_target=_model_writer_logical_lock_target(
            context,
            user_data_root=user_data_root,
        ),
        physical_targets=physical_targets,
    )


def _fallback_is_valid_ticker_component(value: str) -> bool:
    return (
        bool(_FALLBACK_TICKER_RE.fullmatch(value))
        and ".." not in value
        and not value.endswith(".")
    )


def _is_valid_ticker_component(value: str) -> bool:
    try:
        from research.artifact_paths import is_valid_ticker_component

        return is_valid_ticker_component(value)
    except Exception:
        try:
            from research.artifact_paths import is_valid_ticker_component

            return is_valid_ticker_component(value)
        except Exception:
            return _fallback_is_valid_ticker_component(value)


def _normalize_timeout_seconds(timeout_seconds: float | None) -> float:
    timeout_value: float | str
    if timeout_seconds is None:
        timeout_value = os.getenv(
            "FMS_MODEL_WRITER_LOCK_TIMEOUT_SECONDS", "30"
        ).strip()
    else:
        timeout_value = timeout_seconds
    normalized = float(timeout_value)
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
    if not _is_valid_ticker_component(ticker):
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
    return ticker if _is_valid_ticker_component(ticker) else None


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
    """Acquire model-writer store locks in deterministic order."""

    normalized_timeout = _normalize_timeout_seconds(timeout_seconds)
    acquisition_pid = os.getpid()
    acquired: list[str] = []
    try:
        grouped = _group_targets([_resolve_target(key) for key in keys])
        for key, targets in grouped:
            _acquire_registry_key(key, targets, normalized_timeout, ticker=ticker)
            acquired.append(key)
        yield
    finally:
        if acquisition_pid == os.getpid():
            for key in reversed(acquired):
                _release_registry_key(key)


def model_writer_lock_held_keys() -> tuple[str, ...]:
    with _REGISTRY_LOCK:
        return tuple(sorted(_HELD_KEYS))


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
    "describe_lock",
    "model_writer_lock",
    "model_writer_lock_held_keys",
    "resolve_store_key",
]
