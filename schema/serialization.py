"""Disk-cache and sidecar codecs for FinancialModel."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import shutil
import struct
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple, TypeAlias

import pydantic

from .models import MODELS_SCHEMA_VERSION, FinancialModel
from .reader import READER_VERSION
from .templates.model_scale import (
    TEMPLATE_SEMANTIC_UNIT_REGISTRY_SHA256,
    assert_template_semantic_units,
)
from .valuation_schema_invariant import (
    assert_valuation_template_schema,
    should_enforce_valuation_template_schema,
)

logger = logging.getLogger(__name__)

_MAGIC = b"FMC2"
_SIDECAR_KIND = "financial_model_schema_sidecar"
_SIDECAR_VERSION = 2
_HEADER_FMT = "<4s16s16s16s"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)
_MODELS_V_BYTES = MODELS_SCHEMA_VERSION.encode("ascii").ljust(16, b" ")
_READER_V_BYTES = READER_VERSION.encode("ascii").ljust(16, b" ")
_PYDANTIC_VERSION_BYTES = pydantic.VERSION.encode("ascii")[:16].ljust(16, b" ")
_HASH_CHUNK = 1024 * 1024
_SCHEMA_DIR = Path(__file__).resolve().parent


def _compute_engine_version() -> str:
    h = hashlib.sha1()
    for name in ("dependency_graph.py", "templates/model_scale.py"):
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        h.update((_SCHEMA_DIR / name).read_bytes())
        h.update(b"\0")
    return h.hexdigest()[:16]


COMPUTE_ENGINE_VERSION = _compute_engine_version()


class ModelBundleRebuildRequired(RuntimeError):
    """Persisted bundle is outside the sole semantic-unit contract."""

    error_code = "REBUILD_REQUIRED_MODEL_BUNDLE_SEMANTIC_UNITS"


class ModelBundleSidecarRequired(ValueError):
    """The workbook lacks its exact current canonical schema sidecar."""

    error_code = "REBUILD_REQUIRED_MODEL_BUNDLE_SIDECAR"


def canonical_financial_model_payload(model: FinancialModel) -> Dict:
    """Return the sole fully revalidated JSON form used for exact bundle checks."""

    return FinancialModel.model_validate(
        model.model_dump(mode="json")
    ).model_dump(mode="json")


def _require_current_semantic_units(
    model: FinancialModel,
    *,
    origin: str,
) -> None:
    try:
        assert_template_semantic_units(model, origin=origin)
    except ValueError as exc:
        raise ModelBundleRebuildRequired(str(exc)) from exc


def _cache_root() -> Path:
    version_dir = f"v{MODELS_SCHEMA_VERSION}_{READER_VERSION}_{COMPUTE_ENGINE_VERSION}"
    configured = os.getenv("SCHEMA_SERIALIZATION_CACHE_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser() / version_dir
    here = Path(__file__).resolve().parent
    return here.parent / "data" / "cache" / "schema" / version_dir


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(_HASH_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def _key(file_path: str, cutoff: int) -> str:
    file_hash = _sha256_file(os.path.abspath(file_path))
    raw = (
        f"{file_hash}|{cutoff}|{MODELS_SCHEMA_VERSION}|{READER_VERSION}|"
        f"{COMPUTE_ENGINE_VERSION}|{pydantic.VERSION}"
    )
    return hashlib.sha1(raw.encode()).hexdigest()


def _path_for(key: str) -> Path:
    return _cache_root() / key[:2] / f"{key}.pkl"


# None is a persisted computed-but-unavailable value. A cold or stale cache is
# represented separately by the absence of BaseResults at the load boundary.
BaseResults: TypeAlias = Dict[str, Dict[int, Optional[float]]]


def sidecar_path(file_path: str) -> Path:
    """Return the adjacent canonical schema sidecar path for a workbook."""
    return Path(os.path.abspath(file_path)).with_name(f"{Path(file_path).name}.schema.json")


def _base_results_to_json(base_results: BaseResults) -> Dict[str, Dict[str, Optional[float]]]:
    return {
        item_id: {str(period): value for period, value in values.items()}
        for item_id, values in base_results.items()
    }


def _base_results_from_json(raw: object) -> BaseResults:
    if not isinstance(raw, dict):
        raise ValueError("base_results must be a mapping")

    decoded: BaseResults = {}
    for item_id, values in raw.items():
        if not isinstance(item_id, str) or not isinstance(values, dict):
            raise ValueError("base_results entries must be item-id mappings")
        decoded[item_id] = {
            int(period): None if value is None else float(value) for period, value in values.items()
        }
    return decoded


def try_load(file_path: str, cutoff: int) -> Optional[Tuple[FinancialModel, BaseResults]]:
    """Load (FinancialModel, base_results) from disk cache, or None on any miss/failure."""
    try:
        key = _key(file_path, cutoff)
    except OSError:
        return None

    cache_path = _path_for(key)
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "rb") as f:
            header = f.read(_HEADER_SIZE)
            if len(header) != _HEADER_SIZE:
                return None
            magic, models_v, reader_v, pyd_v = struct.unpack(_HEADER_FMT, header)
            if magic != _MAGIC:
                return None
            if models_v != _MODELS_V_BYTES or reader_v != _READER_V_BYTES:
                return None
            if pyd_v != _PYDANTIC_VERSION_BYTES:
                return None
            payload = f.read()

        obj = pickle.loads(payload)
        if not (isinstance(obj, tuple) and len(obj) == 2):
            return None

        model, base_results = obj
        if not isinstance(model, FinancialModel):
            return None
        if not isinstance(base_results, dict):
            return None

        model = FinancialModel.model_validate(model.model_dump())
        _require_current_semantic_units(model, origin="serialization.disk_cache")
        model._index = {}
        return model, base_results
    except (
        pickle.UnpicklingError,
        EOFError,
        OSError,
        AttributeError,
        TypeError,
        ValueError,
        pydantic.ValidationError,
    ) as exc:
        logger.warning("Schema cache load failed for %s: %s", cache_path, exc)
        return None


def try_load_sidecar(file_path: str) -> Optional[Tuple[FinancialModel, Optional[BaseResults]]]:
    """Load only the exact current canonical sidecar, or None on absence/staleness."""
    try:
        current_hash = _sha256_file(os.path.abspath(file_path))
    except OSError:
        return None
    schema_path = sidecar_path(file_path)
    if not schema_path.exists():
        return None

    try:
        with open(schema_path, encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return None
        if obj.get("kind") != _SIDECAR_KIND:
            raise ModelBundleRebuildRequired(
                "schema sidecar kind is outside the canonical bundle contract"
            )
        if obj.get("version") != _SIDECAR_VERSION:
            raise ModelBundleRebuildRequired(
                "schema sidecar version requires offline semantic-unit rebuild"
            )
        sidecar_schema_version = obj.get("models_schema_version")
        if sidecar_schema_version != MODELS_SCHEMA_VERSION:
            raise ModelBundleRebuildRequired(
                "models schema version requires offline semantic-unit rebuild"
            )
        if (
            obj.get("template_semantic_unit_registry_sha256")
            != TEMPLATE_SEMANTIC_UNIT_REGISTRY_SHA256
        ):
            raise ModelBundleRebuildRequired(
                "semantic-unit registry identity is missing or stale"
            )
        if obj.get("source_sha256") != current_hash:
            return None
        compute_engine_matches = obj.get("compute_engine_version") == COMPUTE_ENGINE_VERSION

        model = FinancialModel.model_validate(obj.get("model"))
        _require_current_semantic_units(model, origin="serialization.sidecar")
        base_results = _base_results_from_json(obj.get("base_results")) if compute_engine_matches else None
        model._index = {}
        if not compute_engine_matches:
            logger.warning(
                "Loading schema sidecar for %s with stale compute_engine_version=%r current=%r; recomputing base results",
                schema_path,
                obj.get("compute_engine_version"),
                COMPUTE_ENGINE_VERSION,
            )
        return model, base_results
    except (
        json.JSONDecodeError,
        OSError,
        AttributeError,
        TypeError,
        ValueError,
        pydantic.ValidationError,
    ) as exc:
        logger.warning("Schema sidecar load failed for %s: %s", schema_path, exc)
        return None


def require_current_sidecar(
    file_path: str,
    *,
    expected_model: FinancialModel | None = None,
) -> Tuple[FinancialModel, Optional[BaseResults]]:
    """Require the exact current v2 sidecar bound to these workbook bytes."""

    loaded = try_load_sidecar(file_path)
    if loaded is None:
        raise ModelBundleSidecarRequired(
            "workbook requires an exact current v2 schema sidecar"
        )
    model, base_results = loaded
    if (
        expected_model is not None
        and model.model_dump(mode="json")
        != canonical_financial_model_payload(expected_model)
    ):
        raise ModelBundleSidecarRequired(
            "schema sidecar model does not match the expected canonical model"
        )
    return model, base_results


def save(
    file_path: str,
    cutoff: int,
    model: FinancialModel,
    base_results: BaseResults,
) -> None:
    """Best-effort write; never raises to callers."""
    try:
        key = _key(file_path, cutoff)
        cache_path = _path_for(key)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        header = struct.pack(
            _HEADER_FMT,
            _MAGIC,
            _MODELS_V_BYTES,
            _READER_V_BYTES,
            _PYDANTIC_VERSION_BYTES,
        )
        payload = pickle.dumps((model, base_results), protocol=5)

        fd, tmp_str = tempfile.mkstemp(
            suffix=".pkl.tmp",
            prefix=key + ".",
            dir=cache_path.parent,
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(header)
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_str, cache_path)
        except Exception:
            try:
                os.unlink(tmp_str)
            except OSError:
                pass
            raise

        try:
            dir_fd = os.open(cache_path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as exc:
            logger.debug("Parent-dir fsync skipped for %s: %s", cache_path.parent, exc)
    except (OSError, pickle.PicklingError) as exc:
        logger.warning("Schema cache save failed for %s: %s", file_path, exc)


def save_sidecar(
    file_path: str,
    model: FinancialModel,
    base_results: BaseResults,
) -> None:
    """Best-effort write of canonical schema state next to the workbook."""
    canonical_payload = canonical_financial_model_payload(model)
    canonical_model = FinancialModel.model_validate(canonical_payload)
    _require_current_semantic_units(
        canonical_model,
        origin="serialization.save_sidecar",
    )
    try:
        source_hash = _sha256_file(os.path.abspath(file_path))
        schema_path = sidecar_path(file_path)
        enforce_valuation_schema = should_enforce_valuation_template_schema(
            canonical_model,
            workbook_path=file_path,
        )
        if enforce_valuation_schema:
            assert_valuation_template_schema(
                canonical_model,
                origin="serialization.save_sidecar.pre",
                workbook_path=file_path,
                sidecar_path=schema_path,
                module_path=__file__,
            )
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "kind": _SIDECAR_KIND,
            "version": _SIDECAR_VERSION,
            "models_schema_version": MODELS_SCHEMA_VERSION,
            "template_semantic_unit_registry_sha256": (
                TEMPLATE_SEMANTIC_UNIT_REGISTRY_SHA256
            ),
            "source_sha256": source_hash,
            "compute_engine_version": COMPUTE_ENGINE_VERSION,
            "model": canonical_payload,
            "base_results": _base_results_to_json(base_results),
        }

        fd, tmp_str = tempfile.mkstemp(
            suffix=".schema.json.tmp",
            prefix=schema_path.name + ".",
            dir=schema_path.parent,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, sort_keys=True, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_str, schema_path)
        except Exception:
            try:
                os.unlink(tmp_str)
            except OSError:
                pass
            raise

        try:
            dir_fd = os.open(schema_path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as exc:
            logger.debug("Parent-dir fsync skipped for %s: %s", schema_path.parent, exc)
        if enforce_valuation_schema:
            assert_valuation_template_schema(
                canonical_model,
                origin="serialization.save_sidecar.post",
                workbook_path=file_path,
                sidecar_path=schema_path,
                sidecar_hash=_sha256_file(str(schema_path)),
                module_path=__file__,
            )
    except (OSError, TypeError, ValueError) as exc:
        logger.warning("Schema sidecar save failed for %s: %s", file_path, exc)


def clear_disk() -> None:
    """Wipe the entire disk cache."""
    shutil.rmtree(_cache_root().parent, ignore_errors=True)
