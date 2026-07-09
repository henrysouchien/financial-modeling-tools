"""File persistence helpers for schema model modifications."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional


_MissingFile = object()


def file_sha256(path: str | Path) -> Optional[str]:
    """Return a file's SHA-256 hex digest, or None if the file does not exist."""

    file_path = Path(path)
    if not file_path.is_file():
        return None

    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256(path: str) -> Optional[str]:
    return file_sha256(path)


def _read_optional_bytes(path: Path) -> bytes | object:
    if not path.exists():
        return _MissingFile
    return path.read_bytes()


def _restore_optional_bytes(path: Path, payload: bytes | object) -> None:
    if payload is _MissingFile:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)  # type: ignore[arg-type]


__all__ = [
    "_MissingFile",
    "file_sha256",
    "_read_optional_bytes",
    "_restore_optional_bytes",
    "_sha256",
]
