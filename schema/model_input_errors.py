from __future__ import annotations

from typing import Literal


UnsupportedCategory = Literal["B1", "B2"]


class ModelBuildContextError(Exception):
    """Base class for typed model-build-context failures."""


class InvalidDriverKey(ModelBuildContextError):
    def __init__(self, driver_key: str, reason: str) -> None:
        self.driver_key = str(driver_key)
        self.reason = str(reason)
        super().__init__(f"Invalid driver key '{self.driver_key}': {self.reason}")


class SegmentExpansionAmbiguity(ModelBuildContextError):
    def __init__(self, driver_key: str, reason: str, segment_index: int | None = None) -> None:
        self.driver_key = str(driver_key)
        self.segment_index = segment_index
        self.reason = str(reason)
        suffix = f" (segment_index={segment_index})" if segment_index is not None else ""
        super().__init__(f"Segment expansion ambiguity for '{self.driver_key}'{suffix}: {self.reason}")


class UnsupportedInSegmentMode(ModelBuildContextError):
    def __init__(self, driver_key: str, category: UnsupportedCategory, reason: str) -> None:
        if category not in {"B1", "B2"}:
            raise ValueError("category must be 'B1' or 'B2'")
        self.driver_key = str(driver_key)
        self.category = category
        self.reason = str(reason)
        super().__init__(
            f"Unsupported in segment mode [{self.category}] for '{self.driver_key}': {self.reason}"
        )


class MissingSegmentSnapshot(ModelBuildContextError):
    def __init__(self, reason: str) -> None:
        self.reason = str(reason)
        super().__init__(f"Missing segment snapshot: {self.reason}")


class SegmentProfileMismatch(ModelBuildContextError):
    def __init__(self, reason: str) -> None:
        self.reason = str(reason)
        super().__init__(f"Segment profile mismatch: {self.reason}")


class BusinessModelRevisionMismatch(ModelBuildContextError):
    def __init__(
        self,
        reason: str,
        *,
        expected_revision: str | None = None,
        actual_revision: str | None = None,
    ) -> None:
        self.reason = str(reason)
        self.expected_revision = None if expected_revision is None else str(expected_revision)
        self.actual_revision = None if actual_revision is None else str(actual_revision)
        super().__init__(f"Business model revision mismatch: {self.reason}")


__all__ = [
    "BusinessModelRevisionMismatch",
    "InvalidDriverKey",
    "MissingSegmentSnapshot",
    "ModelBuildContextError",
    "SegmentExpansionAmbiguity",
    "SegmentProfileMismatch",
    "UnsupportedCategory",
    "UnsupportedInSegmentMode",
]
