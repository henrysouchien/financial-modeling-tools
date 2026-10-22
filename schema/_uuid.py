from __future__ import annotations

import json
import unicodedata
import uuid

_NS_MODEL_INSIGHTS = uuid.UUID("f626190e-4f05-46c3-be43-51b5ac158501")
_NS_PRICE_TARGET = uuid.UUID("d7e9b977-c67f-4f5e-a0db-11ad8e5ba8ea")
_NS_ASSUMPTION = uuid.UUID("c62b16eb-6244-424d-a308-3556fee56772")
_NS_RISK = uuid.UUID("ac16e43a-6963-472f-ba2c-6a94cac27d75")
_NS_CATALYST = uuid.UUID("38539b58-8205-4a06-a4a4-c97eaee48121")
_NS_TRIGGER = uuid.UUID("98c888ac-5819-4137-a8ac-2b092409cb48")
_NS_CLAIM = uuid.UUID("22d8a8f7-b0ad-4e34-aa7b-77001a867ef3")
_NS_OUTCOME = uuid.UUID("f3d6a3ee-4d02-4b2e-8f4e-35af0d97d3a3")
_NS_DV_CLAIM_V2 = uuid.UUID("6d518d03-43b3-4647-bbec-1eac887488af")
_NS_SINGLETON_CLAIM_V2 = uuid.UUID("a729385b-09f0-46a6-a9d2-d426196d66a8")
_NS_GAAP_BRIDGE_V2 = uuid.UUID("7f85aeb2-47f5-4a61-bf75-26cf940a9b71")
GAP_NAMESPACE = uuid.UUID("1b9c56df-1f77-41e8-a602-913b761e1951")


def _normalize_optional_part(value: object | None) -> str:
  return "" if value is None else str(value)


def model_insights_id(
  model_id: str,
  model_version: int,
  generated_at_iso: str | None = None,
) -> str:
  identity = f"{str(model_id).strip()}|{int(model_version)}"
  if generated_at_iso is not None:
    # Keep the legacy three-part preimage byte-for-byte stable.  New immutable
    # ModelInsights writers omit the timestamp so one ModelVersion resolves to
    # one deterministic artifact identity without aliasing legacy rows.
    identity = f"{identity}|{generated_at_iso}"
  return str(
    uuid.uuid5(
      _NS_MODEL_INSIGHTS,
      identity,
    )
  )


def price_target_id(
  research_file_id: int,
  model_id: str,
  model_version: int,
) -> str:
  return str(
    uuid.uuid5(
      _NS_PRICE_TARGET,
      f"{int(research_file_id)}|{str(model_id).strip()}|{int(model_version)}",
    )
  )


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


def _nfc_text(value: str | None) -> str:
  return unicodedata.normalize("NFC", value or "")


def _nfc_scalar(value: object | None) -> object | None:
  if isinstance(value, str):
    return unicodedata.normalize("NFC", value)
  return value


def _nfc_json_value(value: object | None) -> object | None:
  if isinstance(value, str):
    return unicodedata.normalize("NFC", value)
  if isinstance(value, list):
    return [_nfc_json_value(item) for item in value]
  if isinstance(value, dict):
    return {
      unicodedata.normalize("NFC", str(key)): _nfc_json_value(nested)
      for key, nested in value.items()
    }
  return value


def _canonical_identity_json(parts: list[object | None]) -> str:
  return json.dumps(
    parts,
    ensure_ascii=False,
    separators=(",", ":"),
    allow_nan=True,
  )


def dv_claim_id_v2(claim: str | None, rationale: str | None) -> str:
  encoded = _canonical_identity_json(
    ["dv_claim", _nfc_text(claim), _nfc_text(rationale)]
  )
  return str(uuid.uuid5(_NS_DV_CLAIM_V2, encoded))


def differentiated_view_claim_id_v2(
  claim: str | None,
  rationale: str | None,
) -> str:
  return dv_claim_id_v2(claim, rationale)


def assumption_id(
  driver: str | None,
  value: object | None,
  rationale: str | None,
) -> str:
  encoded = _canonical_identity_json(
    ["assumption", _nfc_text(driver), _nfc_scalar(value), _nfc_text(rationale)]
  )
  return str(uuid.uuid5(_NS_ASSUMPTION, encoded))


def singleton_claim_id_v2(family: str, narrative_content: object | None) -> str:
  encoded = json.dumps(
    [_nfc_text(family), _nfc_json_value(narrative_content)],
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    allow_nan=True,
  )
  return str(uuid.uuid5(_NS_SINGLETON_CLAIM_V2, encoded))


def gaap_bridge_id_v2(
  metric: str | None,
  period: str | None,
  gaap_value: object | None,
  non_gaap_value: object | None,
) -> str:
  encoded = _canonical_identity_json(
    [
      "gaap_bridge",
      _nfc_text(metric),
      _nfc_text(period),
      _nfc_json_value(gaap_value),
      _nfc_json_value(non_gaap_value),
    ]
  )
  return str(uuid.uuid5(_NS_GAAP_BRIDGE_V2, encoded))


def gap_id(target_handle: str | None, description: str | None) -> str:
  encoded = _canonical_identity_json(
    [_nfc_text(target_handle), _nfc_text(description)]
  )
  return f"gap_{uuid.uuid5(GAP_NAMESPACE, encoded)}"


__all__ = [
  "_NS_ASSUMPTION",
  "_NS_CATALYST",
  "_NS_CLAIM",
  "_NS_DV_CLAIM_V2",
  "_NS_GAAP_BRIDGE_V2",
  "_NS_MODEL_INSIGHTS",
  "_NS_OUTCOME",
  "_NS_PRICE_TARGET",
  "_NS_RISK",
  "_NS_SINGLETON_CLAIM_V2",
  "_NS_TRIGGER",
  "GAP_NAMESPACE",
  "assumption_id",
  "catalyst_id",
  "claim_id",
  "differentiated_view_claim_id_v2",
  "dv_claim_id_v2",
  "gaap_bridge_id_v2",
  "gap_id",
  "model_insights_id",
  "price_target_id",
  "risk_id",
  "singleton_claim_id_v2",
  "trigger_id",
]
