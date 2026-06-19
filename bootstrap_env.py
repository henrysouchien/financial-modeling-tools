"""Minimal environment bootstrap for the standalone financial-modeling-tools mirror."""

from __future__ import annotations

import os
from pathlib import Path


_BOOTSTRAPPED = False


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def bootstrap(required: list[str] = ()) -> None:
    """Load a local `.env` if present and leave missing dev secrets non-fatal."""

    global _BOOTSTRAPPED
    if not _BOOTSTRAPPED:
        _load_env_file(Path(__file__).resolve().parent / ".env")
        _BOOTSTRAPPED = True

