"""Compatibility re-exports for comps sheet registry models.

Transferred to `packages/comps` (`comps.registry`) per Phase 1 target architecture
(docs/design/skills-as-code-libraries-implementation-plan.md §3.1).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure packages/comps is on sys.path if not installed as distribution
_COMPS_PKG_ROOT = Path(__file__).resolve().parents[1] / "packages" / "comps"
if _COMPS_PKG_ROOT.exists() and str(_COMPS_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_COMPS_PKG_ROOT))

from comps.registry import (
    CompsSheetEntry,
    CompsSheetProvenance,
    CompsSheetRegistry,
    CompsSheetTab,
)

__all__ = [
    "CompsSheetEntry",
    "CompsSheetProvenance",
    "CompsSheetRegistry",
    "CompsSheetTab",
]
