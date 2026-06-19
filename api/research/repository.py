"""Fail-closed repository compatibility for standalone annotation imports."""

from __future__ import annotations


class StandaloneRepositoryUnavailable(RuntimeError):
    """Raised when AI-excel-addin repository storage is requested standalone."""


def get_repository_factory():
    raise StandaloneRepositoryUnavailable(
        "Research repository storage is not available in the standalone financial-modeling-tools mirror"
    )

