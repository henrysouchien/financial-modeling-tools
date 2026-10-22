"""Shared classification for platform-managed current model bundles."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


PLATFORM_MANAGED_MODEL_BUNDLE_WRITE_FORBIDDEN = (
    "platform_managed_model_bundle_write_forbidden"
)
PLATFORM_MANAGED_MODEL_BUNDLE_WRITE_ERROR_TYPE = "managed_model_bundle_path"


class PlatformManagedModelBundlePathKind(StrEnum):
    """Canonical platform-owned workbook path shapes."""

    REPOSITORY_EXPORT = "repository_export"
    RUN_SCOPED_PREVIEW = "run_scoped_preview"


@dataclass(frozen=True)
class PlatformManagedModelBundlePath:
    """Typed classification of one platform-owned workbook path."""

    path: Path
    kind: PlatformManagedModelBundlePathKind

    @property
    def write_rejection_message(self) -> str:
        return (
            f"{str(self.path)!r} is platform-managed {self.kind.value} current "
            "model bundle state; use the owning FMS/repository transaction"
        )


def classify_platform_managed_model_bundle_path(
    file_path: str | Path,
) -> PlatformManagedModelBundlePath | None:
    """Classify exact repository-export and run-scoped preview workbooks."""

    path = Path(file_path).expanduser().resolve(strict=False)
    parts = path.parts
    if (
        len(parts) >= 4
        and parts[-4] == "users"
        and parts[-2] == "exports"
        and path.suffix.lower() == ".xlsx"
    ):
        return PlatformManagedModelBundlePath(
            path=path,
            kind=PlatformManagedModelBundlePathKind.REPOSITORY_EXPORT,
        )

    if len(parts) >= 4 and parts[-4] == "model_workspaces":
        ticker = parts[-2]
        if path.name == f"{ticker}_preview_model.xlsx":
            return PlatformManagedModelBundlePath(
                path=path,
                kind=PlatformManagedModelBundlePathKind.RUN_SCOPED_PREVIEW,
            )

    return None


__all__ = [
    "PLATFORM_MANAGED_MODEL_BUNDLE_WRITE_ERROR_TYPE",
    "PLATFORM_MANAGED_MODEL_BUNDLE_WRITE_FORBIDDEN",
    "PlatformManagedModelBundlePath",
    "PlatformManagedModelBundlePathKind",
    "classify_platform_managed_model_bundle_path",
]
