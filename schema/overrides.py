"""Per-ticker taxonomy override file support."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field, replace
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, model_validator

from .model_writer_lock import model_writer_lock
from .multi_basis_window import MultiBasisWindow
from .statement_windows import (
    parse_statement_windows,
    serialize_statement_windows,
)
from .required_role_catalog import _ROLE_RE, load_required_role_catalog

if TYPE_CHECKING:
    from .provider_port.registry import ProviderRegistry


DEFAULT_OVERRIDES_DIR = Path(__file__).with_name("overrides")
MODEL_ENGINE_OVERRIDES_DIR_ENV = "MODEL_ENGINE_OVERRIDES_DIR"


class GuardConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    growth_carry_forward_max: float | None = None
    growth_carry_forward_min_periods: int | None = None
    rationale: str | None = None

    @model_validator(mode="after")
    def _rationale_required(self) -> "GuardConfig":
        if (
            self.growth_carry_forward_max is not None
            or self.growth_carry_forward_min_periods is not None
        ) and not (self.rationale or "").strip():
            raise ValueError("guards overrides require a rationale")
        return self




@dataclass
class TickerOverrides:
    ticker: str
    overrides: dict[str, dict]
    file_meta: dict = field(default_factory=dict)
    semantic_rows: dict[str, dict] = field(default_factory=dict)
    valuation: dict[str, Any] = field(default_factory=dict)
    guards: GuardConfig | None = None
    statement_windows: dict[str, MultiBasisWindow] = field(default_factory=dict)
    needs_re_emission: bool = False
    provider_registry: ProviderRegistry | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        self.statement_windows = parse_statement_windows(
            self.statement_windows,
            provider_registry=self.provider_registry,
        )


@dataclass(frozen=True)
class TickerOverridesSnapshot:
    ticker: str
    overrides: TickerOverrides | None
    store_hash: str
    payload_hash: str
    path: Path
    provider_registry: ProviderRegistry | None = field(
        default=None,
        repr=False,
        compare=False,
    )


def _runtime_provider_registry(
    instance: TickerOverrides | TickerOverridesSnapshot,
) -> ProviderRegistry | None:
    return instance.provider_registry


_RETIRED_SECTION_REPAIRS = {
    "projections": (
        "forecast authoring lane: remove the retired section and author forecast "
        "controls through the FMS model-update path"
    ),
    "statement_mappings": (
        "statement-window authoring lane: author a statement_windows section "
        "through the isolated hash-verified preview"
    ),
    "custom_concepts": (
        "required-role authoring lane: replace the house-id mint with a catalog "
        "required_role semantic row and a producer-side filing-line binding"
    ),
}
_RETIRED_SECTION_ERROR_CODES = {
    "projections": "projections_section_retired",
    "statement_mappings": "statement_mappings_section_retired",
    "custom_concepts": "custom_concepts_section_retired",
}


class RetiredOverridesSectionError(ValueError):
    """A stored override still carries a section retired by the clean cut."""

    def __init__(self, *, path: str, section: str) -> None:
        self.path = path
        self.section = section
        self.error_code = _RETIRED_SECTION_ERROR_CODES[section]
        self.repair_target = _RETIRED_SECTION_REPAIRS[section]
        super().__init__(
            f"{path}: the {section!r} overrides section is retired; "
            f"repair: {self.repair_target}"
        )


class UnknownOverrideRequiredRoleError(ValueError):
    """A binding patch names no admitted required role for this build."""

    error_code = "override_required_role_unknown"

    def __init__(
        self,
        *,
        ticker: str,
        required_role: str,
        repair_target: str,
    ) -> None:
        self.ticker = ticker
        self.required_role = required_role
        self.repair_target = repair_target
        super().__init__(
            f"override for ticker {ticker!r} names unknown required_role "
            f"{required_role!r}; repair: {repair_target}"
        )


class OverrideBindingPatchInvalid(ValueError):
    """A surviving producer binding patch is not filing-line keyed."""

    error_code = "override_binding_patch_invalid"

    def __init__(self, *, required_role: str, missing: set[str]) -> None:
        self.required_role = required_role
        self.missing = frozenset(missing)
        self.repair_target = (
            "adapter binding authoring lane: provide statement_role, tag, and "
            "provider_binding for the required role"
        )
        super().__init__(
            f"override binding patch {required_role!r} is missing "
            f"{sorted(missing)}; repair: {self.repair_target}"
        )


class TickerOverridesSnapshotConflictError(RuntimeError):
    error_code = "statement_window_override_snapshot_conflict"


def load_ticker_overrides_snapshot(
    ticker: str,
    overrides_dir: Path | None = None,
    *,
    max_attempts: int = 3,
    load_fn=None,
    provider_registry: ProviderRegistry | None = None,
) -> TickerOverridesSnapshot:
    """Load one stable override snapshot and its matching whole-store hash."""

    from .cas import ABSENT_STORE, overrides_payload_hash, overrides_store_hash

    ticker_upper = _normalize_ticker(ticker)
    path = overrides_lock_target(ticker_upper, overrides_dir)
    loader = load_ticker_overrides if load_fn is None else load_fn
    for _attempt in range(max(1, int(max_attempts))):
        before_hash = overrides_store_hash(path)
        load_kwargs: dict[str, Any] = {"overrides_dir": overrides_dir}
        if provider_registry is not None:
            load_kwargs["provider_registry"] = provider_registry
        overrides = loader(ticker_upper, **load_kwargs)
        after_hash = overrides_store_hash(path)
        if before_hash == after_hash:
            return TickerOverridesSnapshot(
                ticker=ticker_upper,
                overrides=overrides,
                store_hash=after_hash,
                payload_hash=(
                    ABSENT_STORE
                    if overrides is None
                    else overrides_payload_hash(
                        overrides,
                        provider_registry=provider_registry,
                    )
                ),
                path=path,
                provider_registry=provider_registry,
            )
    raise TickerOverridesSnapshotConflictError(
        f"Ticker override store changed while loading snapshot for {ticker_upper}"
    )


def assert_ticker_overrides_snapshot_current(
    snapshot: TickerOverridesSnapshot,
) -> None:
    from .cas import ABSENT_STORE, overrides_payload_hash, overrides_store_hash

    current_hash = overrides_store_hash(snapshot.path)
    if current_hash != snapshot.store_hash:
        raise TickerOverridesSnapshotConflictError(
            f"Ticker override store changed after preflight for {snapshot.ticker}: "
            f"expected {snapshot.store_hash}, got {current_hash}"
        )
    current_payload_hash = (
        ABSENT_STORE
        if snapshot.overrides is None
        else overrides_payload_hash(
            snapshot.overrides,
            provider_registry=_runtime_provider_registry(snapshot),
        )
    )
    if current_payload_hash != snapshot.payload_hash:
        raise TickerOverridesSnapshotConflictError(
            f"Ticker override snapshot mutated in memory after preflight for "
            f"{snapshot.ticker}: expected payload {snapshot.payload_hash}, got "
            f"{current_payload_hash}"
        )


_SCHEMA_VERSION_RANK = {
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
}


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


def overrides_lock_target(ticker: str, overrides_dir: Path | None = None) -> Path:
    """Return the model-writer lock target for a ticker override JSON file."""

    return resolve_overrides_dir(overrides_dir) / f"{_normalize_ticker(ticker)}.json"


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


def validate_ticker_override_path_case(
    ticker: str,
    overrides_dir: Path | None = None,
) -> None:
    """Validate ticker override filename casing without reading or writing files."""

    ticker_upper = _normalize_ticker(ticker)
    directory = _override_dir(overrides_dir)
    _resolve_override_path(ticker_upper, directory)


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


def _coerce_guards_section(data: Any) -> GuardConfig | None:
    if data is None:
        return None
    if not isinstance(data, dict):
        raise ValueError("guards must be an object")
    return GuardConfig.model_validate(data)


def _coerce_statement_windows_section(
    data: Any,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> dict[str, MultiBasisWindow]:
    return parse_statement_windows(
        data,
        provider_registry=provider_registry,
    )


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
    overrides: dict[str, Any] | None = None,
    semantic_rows: dict[str, Any] | None = None,
    valuation: dict[str, Any] | None = None,
    guards: GuardConfig | dict[str, Any] | None = None,
    statement_windows: dict[str, MultiBasisWindow] | dict[str, Any] | None = None,
) -> str:
    if statement_windows:
        return "8"
    if any(
        isinstance(entry, dict)
        and isinstance(entry.get("provider_binding"), dict)
        for entry in (overrides or {}).values()
    ):
        return "7"
    if guards is not None:
        return "5"
    if valuation:
        return "4"
    if semantic_rows:
        return "3"
    return "1"


def derive_overrides_file_meta(
    file_meta: dict[str, Any] | None,
    *,
    ticker: str | None = None,
    overrides: dict[str, Any] | None = None,
    semantic_rows: dict[str, Any] | None = None,
    valuation: dict[str, Any] | None = None,
    guards: GuardConfig | dict[str, Any] | None = None,
    statement_windows: dict[str, MultiBasisWindow] | dict[str, Any] | None = None,
) -> dict[str, Any]:
    meta = dict(file_meta or {})
    if ticker:
        meta.setdefault("ticker", _normalize_ticker(ticker))
    existing_version = str(meta.get("schema_version", "1") or "1")
    required_version = required_overrides_schema_version(
        overrides=overrides,
        semantic_rows=semantic_rows,
        valuation=valuation,
        guards=guards,
        statement_windows=statement_windows,
    )
    existing_rank = overrides_schema_version_rank(existing_version)
    required_rank = overrides_schema_version_rank(required_version)
    target_rank = max(existing_rank, required_rank)
    meta["schema_version"] = _schema_version_for_rank(target_rank)
    if existing_rank == 1 and target_rank > 1:
        meta.setdefault("migrated_from_v1", True)
    return meta


def derive_ticker_overrides_schema_version(overrides: TickerOverrides) -> str:
    return derive_overrides_file_meta(
        overrides.file_meta,
        ticker=overrides.ticker,
        overrides=overrides.overrides,
        semantic_rows=overrides.semantic_rows,
        valuation=overrides.valuation,
        guards=overrides.guards,
        statement_windows=overrides.statement_windows,
    )["schema_version"]


def load_ticker_overrides(
    ticker: str,
    overrides_dir: Path | None = None,
    *,
    provider_registry: ProviderRegistry | None = None,
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
    for retired_section in _RETIRED_SECTION_REPAIRS:
        if retired_section in payload:
            raise RetiredOverridesSectionError(
                path=str(path),
                section=retired_section,
            )

    from .provider_port.adapters.fmp.overlay import canonicalize_override_payload

    payload, needs_re_emission = canonicalize_override_payload(
        payload,
        provider_registry=provider_registry,
    )
    file_meta = _coerce_meta(payload.get("_meta"))
    schema_version = str(file_meta.get("schema_version", "1"))
    if schema_version not in _SCHEMA_VERSION_RANK:
        raise ValueError(f"unsupported overrides schema_version: {schema_version!r}")

    return TickerOverrides(
        ticker=ticker_upper,
        overrides=_coerce_dict_section(payload.get("overrides"), "overrides"),
        file_meta=file_meta,
        semantic_rows=_coerce_semantic_rows_section(payload.get("semantic_rows")),
        valuation=_coerce_valuation_section(payload.get("valuation")),
        guards=_coerce_guards_section(payload.get("guards")),
        statement_windows=_coerce_statement_windows_section(
            payload.get("statement_windows"),
            provider_registry=provider_registry,
        ),
        needs_re_emission=needs_re_emission,
        provider_registry=provider_registry,
    )


def overrides_payload(
    overrides: TickerOverrides,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> dict[str, Any]:
    """Return the JSON payload saved by ``save_ticker_overrides``."""

    from .provider_port.adapters.fmp.overlay import canonicalize_override_payload

    active_registry = (
        provider_registry
        if provider_registry is not None
        else _runtime_provider_registry(overrides)
    )
    ticker_upper = _normalize_ticker(overrides.ticker)
    valuation = _coerce_valuation_section(overrides.valuation)
    canonical_sections, _binding_changed = canonicalize_override_payload(
        {"overrides": dict(overrides.overrides)},
        provider_registry=provider_registry,
    )
    canonical_overrides = canonical_sections["overrides"]
    canonical_semantic_rows = dict(overrides.semantic_rows)
    file_meta = derive_overrides_file_meta(
        overrides.file_meta,
        ticker=ticker_upper,
        overrides=canonical_overrides,
        semantic_rows=canonical_semantic_rows,
        valuation=valuation,
        guards=overrides.guards,
        statement_windows=overrides.statement_windows,
    )
    schema_rank = overrides_schema_version_rank(file_meta.get("schema_version"))
    payload: dict[str, Any] = {
        "_meta": file_meta,
        "overrides": canonical_overrides,
    }
    if canonical_semantic_rows or schema_rank >= 3:
        payload["semantic_rows"] = canonical_semantic_rows
    if valuation or schema_rank >= 4:
        payload["valuation"] = valuation
    if overrides.guards is not None or schema_rank >= 5:
        payload["guards"] = (
            overrides.guards.model_dump(mode="json", exclude_none=True)
            if overrides.guards is not None
            else {}
        )
    if overrides.statement_windows or schema_rank >= 8:
        payload["statement_windows"] = serialize_statement_windows(
            overrides.statement_windows,
            provider_registry=active_registry,
        )
    return payload


def replace_ticker_overrides(
    existing: TickerOverrides,
    **changes: Any,
) -> TickerOverrides:
    """Replace fields while copying mutable sections and sharing frozen guards."""

    registry = _runtime_provider_registry(existing)
    copied = copy.deepcopy(
        existing,
        memo=({id(registry): registry} if registry is not None else None),
    )
    copied.guards = existing.guards
    return replace(copied, **changes)


_BINDING_PATCH_FIELDS = frozenset({"statement_role", "tag", "provider_binding"})


def _coerce_semantic_rows_section(data: Any) -> dict[str, dict]:
    rows = _coerce_dict_section(data, "semantic_rows")
    catalog = load_required_role_catalog()
    for required_role in rows:
        if _ROLE_RE.fullmatch(required_role):
            catalog.role(required_role)
    return rows


def _validate_binding_patch(
    required_role: str,
    patch: dict[str, Any],
) -> None:
    if not isinstance(patch, dict):
        raise TypeError(f"override binding patch {required_role!r} must be an object")
    missing = set(_BINDING_PATCH_FIELDS - set(patch))
    if missing:
        raise OverrideBindingPatchInvalid(
            required_role=required_role,
            missing=missing,
        )
    if not all(
        isinstance(patch[field], str) and patch[field].strip()
        for field in ("statement_role", "tag")
    ):
        raise ValueError(
            f"override binding patch {required_role!r} requires non-empty "
            "statement_role and tag"
        )
    if not isinstance(patch["provider_binding"], dict):
        raise ValueError(
            f"override binding patch {required_role!r} provider_binding "
            "must be an object"
        )


def merge_overrides(
    bindings: dict[str, dict[str, Any]],
    overrides: TickerOverrides,
) -> tuple[dict[str, dict[str, Any]], int]:
    """Apply per-issuer filing-line binding patches by required role."""

    merged = copy.deepcopy(bindings)
    applied_count = 0
    catalog = load_required_role_catalog()
    admitted_roles = frozenset(catalog.role_ids())
    for required_role, patch in sorted(overrides.overrides.items()):
        if required_role not in admitted_roles:
            raise UnknownOverrideRequiredRoleError(
                ticker=overrides.ticker,
                required_role=required_role,
                repair_target="required-role catalog authoring lane",
            )
        base = merged.get(required_role)
        if base is None:
            raise UnknownOverrideRequiredRoleError(
                ticker=overrides.ticker,
                required_role=required_role,
                repair_target=(
                    "pin-construction policy authoring lane: bind the required "
                    "role before applying a per-issuer patch"
                ),
            )
        if not isinstance(base, dict):
            raise TypeError(
                f"base binding {required_role!r} must be an object"
            )
        _validate_binding_patch(required_role, patch)
        updated = dict(base)
        updated.update(patch)
        merged[required_role] = updated
        applied_count += 1
    return merged, applied_count


def save_ticker_overrides(
    overrides: TickerOverrides,
    overrides_dir: Path | None = None,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> Path:
    """Write per-ticker overrides to the uppercase ticker JSON path."""

    ticker_upper = _normalize_ticker(overrides.ticker)
    directory = _override_dir(overrides_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = overrides_lock_target(ticker_upper, directory)

    with model_writer_lock(path, ticker=ticker_upper):
        payload = overrides_payload(
            overrides,
            provider_registry=provider_registry,
        )
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
    "GuardConfig",
    "OverrideBindingPatchInvalid",
    "RetiredOverridesSectionError",
    "TickerOverrides",
    "UnknownOverrideRequiredRoleError",
    "MODEL_ENGINE_OVERRIDES_DIR_ENV",
    "configured_overrides_dir",
    "derive_overrides_file_meta",
    "derive_ticker_overrides_schema_version",
    "load_ticker_overrides",
    "merge_overrides",
    "overrides_payload",
    "overrides_lock_target",
    "overrides_schema_version_rank",
    "required_overrides_schema_version",
    "resolve_overrides_dir",
    "save_ticker_overrides",
    "validate_ticker_override_path_case",
    "list_ticker_overrides",
]
