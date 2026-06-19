"""Per-ticker taxonomy override file support."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
from typing import Any

from .models import DataSourceMapping


DEFAULT_OVERRIDES_DIR = Path(__file__).with_name("overrides")
MODEL_ENGINE_OVERRIDES_DIR_ENV = "MODEL_ENGINE_OVERRIDES_DIR"
_DSM_FIELD_NAMES = set(DataSourceMapping.model_fields.keys())
_CUSTOM_CONCEPT_DSM_FIELD_NAMES = _DSM_FIELD_NAMES - {"notes"}
_CUSTOM_CONCEPT_EXTRA_FIELDS = frozenset({"axis_key", "inline_values"})


@dataclass
class TickerOverrides:
    ticker: str
    overrides: dict[str, dict]
    custom_concepts: dict[str, dict]
    file_meta: dict = field(default_factory=dict)
    projections: dict[str, dict] = field(default_factory=dict)
    semantic_rows: dict[str, dict] = field(default_factory=dict)
    valuation: dict[str, Any] = field(default_factory=dict)


_SCHEMA_VERSION_RANK = {"1": 1, "2": 2, "3": 3, "4": 4}


def _normalize_ticker(ticker: str) -> str:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        raise ValueError("ticker is required")
    return normalized


def configured_overrides_dir() -> Path | None:
    configured = os.environ.get(MODEL_ENGINE_OVERRIDES_DIR_ENV, "").strip()
    if not configured:
        return None
    return Path(configured).expanduser().resolve(strict=False)


def _override_dir(overrides_dir: Path | None) -> Path:
    if overrides_dir is not None:
        return Path(overrides_dir)
    return configured_overrides_dir() or DEFAULT_OVERRIDES_DIR


def resolve_overrides_dir(overrides_dir: Path | None = None) -> Path:
    """Return the shared override directory used by override readers/writers."""

    return _override_dir(overrides_dir)


def _matching_override_paths(overrides_dir: Path, ticker_upper: str) -> list[Path]:
    if not overrides_dir.exists():
        return []
    target_name = f"{ticker_upper}.json"
    target_lower = target_name.lower()
    return sorted(
        (
            path
            for path in overrides_dir.iterdir()
            if path.is_file() and path.name.lower() == target_lower
        ),
        key=lambda path: path.name,
    )


def _resolve_override_path(ticker_upper: str, overrides_dir: Path) -> Path | None:
    target_name = f"{ticker_upper}.json"
    matches = _matching_override_paths(overrides_dir, ticker_upper)
    if not matches:
        return None

    exact_matches = [path for path in matches if path.name == target_name]
    if len(exact_matches) == 1 and len(matches) == 1:
        return exact_matches[0]

    variants = ", ".join(path.name for path in matches)
    raise ValueError(
        f"Invalid override file casing for ticker {ticker_upper}: {variants}. "
        f"Use {target_name}."
    )


def _coerce_dict_section(data: Any, section: str) -> dict[str, dict]:
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"{section} must be an object")
    result: dict[str, dict] = {}
    for key, value in data.items():
        if not isinstance(value, dict):
            raise ValueError(f"{section}.{key} must be an object")
        result[str(key)] = dict(value)
    return result


def _coerce_meta(data: Any) -> dict:
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError("_meta must be an object")
    return dict(data)


def _coerce_valuation_section(data: Any) -> dict[str, Any]:
    if data is None or data == {}:
        return {}
    if not isinstance(data, dict):
        raise ValueError("valuation must be an object")
    from .model_semantics import ValuationArtifact

    return ValuationArtifact.model_validate(data).model_dump(mode="json")


def overrides_schema_version_rank(schema_version: Any) -> int:
    text = str(schema_version or "1")
    if text not in _SCHEMA_VERSION_RANK:
        raise ValueError(f"unsupported overrides schema_version: {text!r}")
    return _SCHEMA_VERSION_RANK[text]


def _schema_version_for_rank(rank: int) -> str:
    for schema_version, candidate_rank in _SCHEMA_VERSION_RANK.items():
        if candidate_rank == rank:
            return schema_version
    raise ValueError(f"unsupported overrides schema rank: {rank!r}")


def required_overrides_schema_version(
    *,
    projections: dict[str, Any] | None = None,
    semantic_rows: dict[str, Any] | None = None,
    valuation: dict[str, Any] | None = None,
) -> str:
    if valuation:
        return "4"
    if semantic_rows:
        return "3"
    if projections:
        return "2"
    return "1"


def derive_overrides_file_meta(
    file_meta: dict[str, Any] | None,
    *,
    ticker: str | None = None,
    projections: dict[str, Any] | None = None,
    semantic_rows: dict[str, Any] | None = None,
    valuation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    meta = dict(file_meta or {})
    if ticker:
        meta.setdefault("ticker", _normalize_ticker(ticker))
    existing_version = str(meta.get("schema_version", "1") or "1")
    required_version = required_overrides_schema_version(
        projections=projections,
        semantic_rows=semantic_rows,
        valuation=valuation,
    )
    existing_rank = overrides_schema_version_rank(existing_version)
    required_rank = overrides_schema_version_rank(required_version)
    target_rank = max(existing_rank, required_rank)
    meta["schema_version"] = _schema_version_for_rank(target_rank)
    if existing_rank < 2 <= target_rank:
        meta.setdefault("migrated_from_v1", True)
    return meta


def derive_ticker_overrides_schema_version(overrides: TickerOverrides) -> str:
    return derive_overrides_file_meta(
        overrides.file_meta,
        ticker=overrides.ticker,
        projections=overrides.projections,
        semantic_rows=overrides.semantic_rows,
        valuation=overrides.valuation,
    )["schema_version"]


def load_ticker_overrides(
    ticker: str,
    overrides_dir: Path | None = None,
) -> TickerOverrides | None:
    """Load side-effect-free per-ticker overrides, if present."""

    ticker_upper = _normalize_ticker(ticker)
    directory = _override_dir(overrides_dir)
    path = _resolve_override_path(ticker_upper, directory)
    if path is None:
        return None

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Override file {path} must contain a JSON object")

    file_meta = _coerce_meta(payload.get("_meta"))
    schema_version = str(file_meta.get("schema_version", "1"))
    if schema_version not in ("1", "2", "3", "4"):
        raise ValueError(f"unsupported overrides schema_version: {schema_version!r}")

    return TickerOverrides(
        ticker=ticker_upper,
        overrides=_coerce_dict_section(payload.get("overrides"), "overrides"),
        custom_concepts=_coerce_dict_section(payload.get("custom_concepts"), "custom_concepts"),
        file_meta=file_meta,
        projections=_coerce_dict_section(payload.get("projections"), "projections"),
        semantic_rows=_coerce_dict_section(payload.get("semantic_rows"), "semantic_rows"),
        valuation=_coerce_valuation_section(payload.get("valuation")),
    )


def _merge_mapping(
    concept_id: str,
    base: DataSourceMapping,
    override_fields: dict[str, Any],
) -> DataSourceMapping:
    update = {
        key: value
        for key, value in override_fields.items()
        if key in _DSM_FIELD_NAMES and key != "concept_id"
    }
    payload = base.model_dump()
    payload.update(update)
    payload["concept_id"] = concept_id
    return DataSourceMapping.model_validate(payload)


def merge_overrides(
    taxonomy: dict[str, DataSourceMapping],
    overrides: TickerOverrides,
) -> tuple[dict[str, DataSourceMapping], int]:
    """Return (merged_taxonomy, count_of_base_overrides_applied)."""

    merged = dict(taxonomy)
    applied_count = 0

    for concept_id, override_fields in sorted(overrides.overrides.items()):
        base = merged.get(concept_id)
        if base is None:
            logging.warning(
                "Override for unknown base taxonomy concept %r skipped",
                concept_id,
            )
            continue
        merged[concept_id] = _merge_mapping(concept_id, base, override_fields)
        applied_count += 1

    base_ids = set(taxonomy)
    override_ids = set(overrides.overrides)
    semantic_row_ids = set(overrides.semantic_rows or {})
    for concept_id, entry in sorted(overrides.custom_concepts.items()):
        if concept_id in base_ids or concept_id in override_ids or concept_id in semantic_row_ids:
            raise ValueError(
                f"custom_concept {concept_id!r} collides with base taxonomy "
                "or overrides/semantic_rows section"
            )
        dsm_fields = {
            key: value
            for key, value in entry.items()
            if key in _CUSTOM_CONCEPT_DSM_FIELD_NAMES and key != "concept_id"
        }
        dsm_fields["concept_id"] = concept_id
        merged[concept_id] = DataSourceMapping.model_validate(dsm_fields)

    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        if concept_id in base_ids or concept_id in override_ids:
            raise ValueError(
                f"semantic_row {concept_id!r} collides with base taxonomy "
                "or overrides section"
            )
        source = entry.get("source") if isinstance(entry.get("source"), dict) else {}
        dsm_fields = {
            key: value
            for key, value in entry.items()
            if key in _CUSTOM_CONCEPT_DSM_FIELD_NAMES and key != "concept_id"
        }
        if isinstance(source, dict):
            if "preferred_source" in source:
                dsm_fields["preferred_source"] = source["preferred_source"]
            tags = source.get("xbrl_tags") or source.get("edgar_tags")
            if tags is not None:
                dsm_fields["edgar_tags"] = tags
            if "registry_group_id" in source:
                dsm_fields["registry_group_id"] = source["registry_group_id"]
            if "canonical_tag" in source:
                dsm_fields["canonical_tag"] = source["canonical_tag"]
            if "fmp_endpoint" in source:
                dsm_fields["fmp_endpoint"] = source["fmp_endpoint"]
            if "fmp_field" in source:
                dsm_fields["fmp_field"] = source["fmp_field"]
        dsm_fields["concept_id"] = concept_id
        merged[concept_id] = DataSourceMapping.model_validate(dsm_fields)

    return merged, applied_count


def save_ticker_overrides(
    overrides: TickerOverrides,
    overrides_dir: Path | None = None,
) -> Path:
    """Write per-ticker overrides to the uppercase ticker JSON path."""

    ticker_upper = _normalize_ticker(overrides.ticker)
    directory = _override_dir(overrides_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{ticker_upper}.json"

    valuation = _coerce_valuation_section(overrides.valuation)
    file_meta = derive_overrides_file_meta(
        overrides.file_meta,
        ticker=ticker_upper,
        projections=overrides.projections,
        semantic_rows=overrides.semantic_rows,
        valuation=valuation,
    )
    schema_rank = overrides_schema_version_rank(file_meta.get("schema_version"))
    payload = {
        "_meta": file_meta,
        "overrides": dict(overrides.overrides),
        "custom_concepts": dict(overrides.custom_concepts),
    }
    if overrides.semantic_rows or schema_rank >= 3:
        payload["semantic_rows"] = dict(overrides.semantic_rows)
    if overrides.projections or schema_rank >= 2:
        payload["projections"] = dict(overrides.projections)
    if valuation or schema_rank >= 4:
        payload["valuation"] = valuation
    text = json.dumps(
        payload,
        sort_keys=True,
        indent=2,
        ensure_ascii=False,
        allow_nan=False,
    )
    tmp_path = directory / f"{ticker_upper}.json.tmp.{os.getpid()}"
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(f"{text}\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(tmp_path, path)
        dir_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            dir_flags |= os.O_DIRECTORY
        dir_fd = os.open(directory, dir_flags)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path


def list_ticker_overrides(overrides_dir: Path | None = None) -> list[str]:
    """List tickers with a correctly cased override JSON file."""

    directory = _override_dir(overrides_dir)
    if not directory.exists():
        return []
    tickers = {
        path.stem.upper()
        for path in directory.glob("*.json")
        if path.is_file() and path.name == f"{path.stem.upper()}.json"
    }
    return sorted(tickers)


__all__ = [
    "TickerOverrides",
    "MODEL_ENGINE_OVERRIDES_DIR_ENV",
    "configured_overrides_dir",
    "derive_overrides_file_meta",
    "derive_ticker_overrides_schema_version",
    "load_ticker_overrides",
    "merge_overrides",
    "overrides_schema_version_rank",
    "required_overrides_schema_version",
    "resolve_overrides_dir",
    "save_ticker_overrides",
    "list_ticker_overrides",
]
