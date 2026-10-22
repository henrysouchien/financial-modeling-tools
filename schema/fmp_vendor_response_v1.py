"""Offline validator for the provider-owned ``fms.vendor-response.v1`` wire.

The live FMP package owns creation of this descriptor.  Stored SourceBundle
decoding cannot import that optional pandas-backed package, so this module
freezes its small stdlib-only canonicalization contract for exact replay.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Literal, Mapping


@dataclass(frozen=True, slots=True)
class FmpVendorScalarV1:
    result_key: str
    row_index: int
    field: str
    value_kind: Literal["date", "decimal", "integer"]
    canonical_value: str
    period: str | None

    def as_date(self) -> date:
        if self.value_kind != "date":
            raise ValueError(f"{self.result_key} is not a date scalar")
        return date.fromisoformat(self.canonical_value)

    def period_as_date(self) -> date | None:
        return date.fromisoformat(self.period) if self.period is not None else None

    def as_decimal(self) -> Decimal:
        if self.value_kind not in {"decimal", "integer"}:
            raise ValueError(f"{self.result_key} is not a numeric scalar")
        try:
            parsed = Decimal(self.canonical_value)
        except InvalidOperation as exc:
            raise ValueError(f"{self.result_key} is not a numeric scalar") from exc
        if not parsed.is_finite():
            raise ValueError(f"{self.result_key} is not a finite numeric scalar")
        return parsed

    def as_integer(self) -> int:
        if self.value_kind not in {"decimal", "integer"}:
            raise ValueError(f"{self.result_key} is not an integer scalar")
        try:
            parsed = Decimal(self.canonical_value)
        except InvalidOperation as exc:
            raise ValueError(f"{self.result_key} is not an integer scalar") from exc
        if not parsed.is_finite() or parsed < 0 or parsed != parsed.to_integral_value():
            raise ValueError(f"{self.result_key} is not a nonnegative integer scalar")
        integer = int(parsed)
        if self.value_kind == "integer" and str(integer) != self.canonical_value:
            raise ValueError(f"{self.result_key} is not a canonical integer scalar")
        return integer


@dataclass(frozen=True, slots=True)
class ValidatedFmpVendorResponseV1:
    endpoint: str
    validated_params: Mapping[str, object]
    response_sha256: str
    scalars: tuple[FmpVendorScalarV1, ...]
    _scalars_by_key: Mapping[str, FmpVendorScalarV1]

    def scalar(self, result_key: str, *, expected_field: str) -> FmpVendorScalarV1:
        scalar = self._scalars_by_key.get(result_key)
        if scalar is None or scalar.field != expected_field:
            raise ValueError(
                f"{result_key!r} must resolve exact {expected_field} scalar"
            )
        return scalar


def _reject_duplicate_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate canonical JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite canonical JSON constant: {value}")


def canonical_fmp_vendor_json_v1(value: object) -> str:
    """Return the frozen v1 compact ASCII JSON representation."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _decode_canonical_json(
    raw_value: object,
    *,
    field_name: str,
    expected_type: type[dict] | type[list],
) -> dict[str, object] | list[object]:
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be canonical JSON")
    try:
        value = json.loads(
            raw_value,
            object_pairs_hook=_reject_duplicate_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be canonical JSON") from exc
    if (
        not isinstance(value, expected_type)
        or canonical_fmp_vendor_json_v1(value) != raw_value
    ):
        raise ValueError(f"{field_name} must be canonical JSON")
    return value


def decode_canonical_json_records(
    raw_value: object,
    *,
    field_name: str,
) -> list[object]:
    value = _decode_canonical_json(
        raw_value,
        field_name=field_name,
        expected_type=list,
    )
    assert isinstance(value, list)
    return value


def decode_canonical_json_mapping(
    raw_value: object,
    *,
    field_name: str,
) -> dict[str, object]:
    value = _decode_canonical_json(
        raw_value,
        field_name=field_name,
        expected_type=dict,
    )
    assert isinstance(value, dict)
    return value


def _sha256(value: object) -> str:
    canonical_bytes = canonical_fmp_vendor_json_v1(value).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def _canonical_scalar(value: object) -> tuple[str, str] | None:
    if type(value) is bool or value is None:
        return None
    if type(value) is int:
        return "integer", str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        parsed = Decimal(str(value))
        text = format(parsed, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return "decimal", "0" if text == "-0" else text
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            date.fromisoformat(text)
        except ValueError:
            return None
        return "date", text
    return None


def _period(record: Mapping[str, object]) -> str | None:
    for key in ("date", "period", "calendarYear", "fiscalYear"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _rebuild_descriptor(
    endpoint: str,
    params: Mapping[str, object],
    records: list[object],
) -> dict[str, object]:
    if any(not isinstance(record, dict) for record in records):
        raise ValueError("normalized FMP records must be JSON objects")
    values: list[dict[str, object]] = []
    for row_index, raw_record in enumerate(records):
        assert isinstance(raw_record, dict)
        record = {str(key): value for key, value in raw_record.items()}
        period = _period(record)
        for field in sorted(record):
            canonical = _canonical_scalar(record[field])
            if canonical is None:
                continue
            value_kind, canonical_value = canonical
            scalar: dict[str, object] = {
                "result_key": f"data[{row_index}].{field}",
                "row_index": row_index,
                "field": field,
                "value_kind": value_kind,
                "canonical_value": canonical_value,
                "concept": field,
            }
            if period is not None and field != "date":
                scalar["period"] = period
            values.append(scalar)
    return {
        "schema_version": "fms.vendor-response.v1",
        "issuer": "risk_module",
        "provider": "fmp",
        "endpoint": endpoint,
        "request_sha256": _sha256({"endpoint": endpoint, "params": dict(params)}),
        "response_sha256": _sha256(records),
        "values": values,
    }


def validate_fmp_vendor_response_v1(
    *,
    endpoint: str,
    validated_params_json: object,
    normalized_records_json: object,
    lineage_descriptor_json: object,
    artifact_sha256: object,
    retained_response_sha256: object | None = None,
) -> ValidatedFmpVendorResponseV1:
    """Rebuild and validate one exact provider-issued response descriptor."""

    params = decode_canonical_json_mapping(
        validated_params_json,
        field_name="validated_params",
    )
    records = decode_canonical_json_records(
        normalized_records_json,
        field_name="normalized_records",
    )
    descriptor = decode_canonical_json_mapping(
        lineage_descriptor_json,
        field_name="lineage_descriptor",
    )
    expected_descriptor = _rebuild_descriptor(endpoint, params, records)
    if descriptor != expected_descriptor:
        raise ValueError("lineage descriptor must exactly match normalized records")
    response_sha256 = expected_descriptor["response_sha256"]
    if (
        not isinstance(response_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", response_sha256)
        or artifact_sha256 != response_sha256
        or retained_response_sha256 is not None
        and retained_response_sha256 != response_sha256
    ):
        raise ValueError("FMP response_sha256 must match source artifact")
    raw_scalars = expected_descriptor["values"]
    assert isinstance(raw_scalars, list)
    scalars = tuple(
        FmpVendorScalarV1(
            result_key=str(raw["result_key"]),
            row_index=int(raw["row_index"]),
            field=str(raw["field"]),
            value_kind=raw["value_kind"],
            canonical_value=str(raw["canonical_value"]),
            period=(str(raw["period"]) if "period" in raw else None),
        )
        for raw in raw_scalars
        if isinstance(raw, dict)
    )
    scalars_by_key = {scalar.result_key: scalar for scalar in scalars}
    return ValidatedFmpVendorResponseV1(
        endpoint=endpoint,
        validated_params=MappingProxyType(dict(params)),
        response_sha256=response_sha256,
        scalars=scalars,
        _scalars_by_key=MappingProxyType(scalars_by_key),
    )


__all__ = [
    "FmpVendorScalarV1",
    "ValidatedFmpVendorResponseV1",
    "canonical_fmp_vendor_json_v1",
    "decode_canonical_json_mapping",
    "decode_canonical_json_records",
    "validate_fmp_vendor_response_v1",
]
