from __future__ import annotations

import uuid

_NS_MODEL_INSIGHTS = uuid.UUID("f626190e-4f05-46c3-be43-51b5ac158501")
_NS_PRICE_TARGET = uuid.UUID("d7e9b977-c67f-4f5e-a0db-11ad8e5ba8ea")
_NS_ASSUMPTION = uuid.UUID("d96546bb-85c3-408b-a8de-811c3e95b227")
_NS_RISK = uuid.UUID("ac16e43a-6963-472f-ba2c-6a94cac27d75")
_NS_CATALYST = uuid.UUID("38539b58-8205-4a06-a4a4-c97eaee48121")
_NS_TRIGGER = uuid.UUID("98c888ac-5819-4137-a8ac-2b092409cb48")
_NS_CLAIM = uuid.UUID("22d8a8f7-b0ad-4e34-aa7b-77001a867ef3")
_NS_OUTCOME = uuid.UUID("f3d6a3ee-4d02-4b2e-8f4e-35af0d97d3a3")


def _normalize_optional_part(value: object | None) -> str:
  return "" if value is None else str(value)


def model_insights_id(mbc_id: str, generated_at_iso: str) -> str:
  return str(uuid.uuid5(_NS_MODEL_INSIGHTS, f"{mbc_id}|{generated_at_iso}"))


def price_target_id(research_file_id: int, handoff_id: int) -> str:
  return str(uuid.uuid5(_NS_PRICE_TARGET, f"{research_file_id}|{handoff_id}"))


def assumption_id(driver: str, value: float, rationale: str) -> str:
  return str(uuid.uuid5(_NS_ASSUMPTION, f"{driver}|{value}|{rationale}"))


def risk_id(description: str, severity: str) -> str:
  return str(uuid.uuid5(_NS_RISK, f"{description}|{severity}"))


def catalyst_id(description: str, expected_date: str | None) -> str:
  return str(uuid.uuid5(_NS_CATALYST, f"{description}|{_normalize_optional_part(expected_date)}"))


def trigger_id(
  description: str,
  metric: str | None,
  threshold: float | None,
  threshold_direction: str | None,
) -> str:
  return str(
    uuid.uuid5(
      _NS_TRIGGER,
      (
        f"{description}|"
        f"{_normalize_optional_part(metric)}|"
        f"{_normalize_optional_part(threshold)}|"
        f"{_normalize_optional_part(threshold_direction)}"
      ),
    )
  )


def claim_id(claim: str, rationale: str) -> str:
  return str(uuid.uuid5(_NS_CLAIM, f"{claim}|{rationale}"))


__all__ = [
  "_NS_ASSUMPTION",
  "_NS_CATALYST",
  "_NS_CLAIM",
  "_NS_MODEL_INSIGHTS",
  "_NS_OUTCOME",
  "_NS_PRICE_TARGET",
  "_NS_RISK",
  "_NS_TRIGGER",
  "assumption_id",
  "catalyst_id",
  "claim_id",
  "model_insights_id",
  "price_target_id",
  "risk_id",
  "trigger_id",
]
