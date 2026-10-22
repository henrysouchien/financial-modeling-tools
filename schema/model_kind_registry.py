"""Model-kind registry — which model kind a verified corpus is, as authored data.

Issuer-specific model-kind selection belongs in authored data artifacts,
never in issuer-named mechanism branches. The owner and configuration map is
``schema/README.md``; pipeline context is in
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``.

The registry is a *table*, not a branch. ``schema/model_kinds/registry.json``
carries one row per ``(ticker, corpus_id)`` pair; this module decodes it
fail-closed and answers the two questions mechanism code used to answer with
issuer-named constants:

* **Which coverage profile does this verified corpus resolve to?** — the row's
  ``profile_ref`` (plan §3 row A12: the ticker/corpus branch inside
  ``resolve_annual_coverage_profile``, deleted in the change that lands this
  module).
* **Which packaged corpus is deploy-active for this ticker?** — the ticker's
  single ``deploy_active`` row (plan §3 row V5: the deploy-affinity constant
  in the corpus verifier, deleted in the same change).

Rows are data naming data (plan §5.2, spec §3.5): a registered issuer's name
in the *table* is the point, not the defect — which is why the artifact is
exempt by exact path in ``tests/test_issuer_generic_grep_guard.py`` while this
module, like every module this plan lands, carries zero issuer names.

``profile_ref`` has one authored-data form (plan §5.2):

* ``artifact:<file name>`` — an authored profile artifact stored beside the
  registry, decoded through the one typed ``AnnualCoverageProfile`` contract
  by :mod:`schema.model_kind_profile_loader`.

Every refusal is typed, carries the starved identity, and names a repair lane
(plan §6). The refusals derive from ``ValueError`` deliberately — the opposite
of the capability record's choice, for the opposite reason: nothing here is
raised inside a pydantic validator, while the prepare service already maps a
``ValueError`` out of profile resolution onto its ``profile_resolution_failed``
rejection and the corpus verifier maps one onto ``corpus_unavailable``. Keeping
the derivation keeps those fail-closed rejections exactly where they were; the
stable code and the repair target ride on the exception.

Decoding is hand-rolled over ``json`` rather than delegated to a validation
library because the table is a handful of string fields whose refusals must
name their own first failing field. The consequence that matters for the split
is a dependency fact, not an import-weight one: this module needs nothing the
``AnnualCoverageProfile`` contract needs, so the half that does need it lives
in :mod:`schema.model_kind_profile_loader` while the failure vocabulary stays
here, in one home. It buys **no** import relief today, and the record does not
claim any: ``schema/__init__.py`` is an eager package facade, so importing any
``schema`` submodule executes the whole package. Measured 2026-08-20,
``import research.corpus.verify`` went from 0.020 s / 0 ``schema`` modules
before this change to ~2.5 s / 252 of them after — pydantic, lxml and
``schema.build_model_product_contracts`` included. That cost is recorded as a
residue in the plan's §14 item 13(e) for whichever lane can make the facade
lazy; it is not a reason this module exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re
import stat
from typing import Any, Final, Mapping, NoReturn


#: The registry artifact's schema identity. A payload that does not carry
#: exactly this string is refused: a future shape gets a new version and a
#: decoder that knows it, never a silent best-effort read.
REGISTRY_SCHEMA_VERSION: Final = "model-kind-registry.v1"

#: The checked-in registry artifact this package ships.
DEFAULT_REGISTRY_PATH: Final = (
    Path(__file__).resolve().parent / "model_kinds" / "registry.json"
)

#: Plan §6 codes.
MODEL_KIND_UNREGISTERED: Final = "model_kind_unregistered"
MODEL_KIND_PROFILE_ARTIFACT_INVALID: Final = "model_kind_profile_artifact_invalid"
#: §14 addition at IG-3: the registry artifact is authored data, so a
#: malformed table is its own failure mode with its own repair lane. Folding
#: it into either code above would send the reader to the wrong lane.
MODEL_KIND_REGISTRY_INVALID: Final = "model_kind_registry_invalid"

#: Repair lanes named by the refusals (plan §6).
REGISTRY_LANE: Final = "author the model-kind row in the registry artifact"
REGISTRY_REPAIR_LANE: Final = "repair the registry artifact in the model-kind data lane"
MODEL_KIND_DATA_LANE: Final = "repair the profile artifact in the model-kind data lane"

#: The sole authored-data ``profile_ref`` form (plan §5.2).
AUTHORED_ARTIFACT_ARM: Final = "artifact"

#: Canonical uppercase symbol, matching the packaged corpus manifest's rule.
_TICKER_RE: Final = re.compile(r"^[A-Z][A-Z0-9.-]*$")
#: The corpus manifest's id vocabulary, unchanged.
_ID_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
#: An authored profile artifact is a plain file name beside the registry:
#: no directory separators, so a ref can never escape the registry directory.
_ARTIFACT_NAME_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]*\.json$")

_REGISTRY_KEYS: Final = frozenset({"schema_version", "provenance", "model_kinds"})
_ROW_KEYS: Final = frozenset(
    {"ticker", "corpus_id", "packaged_corpus", "profile_ref", "deploy_active"}
)


class ModelKindRegistryError(ValueError):
    """Base typed model-kind failure with a stable code and a repair lane."""

    code: str = "model_kind_registry_error"

    def __init__(self, reason: str, *, repair: str) -> None:
        self.reason = reason
        self.repair = repair
        super().__init__(f"{self.code}: {reason} (repair: {repair})")


class ModelKindUnregistered(ModelKindRegistryError):
    """No registry row covers the demanded model kind (plan §6).

    Carries the starved identity — the ticker, the corpus id when the lookup
    named one, and every registered kind — plus the registry lane that repairs
    it. The predecessor branch raised a bare ``ValueError``; this refusal keeps
    that fail-closed property and gains the repair target.
    """

    code = MODEL_KIND_UNREGISTERED

    def __init__(
        self,
        *,
        ticker: str,
        corpus_id: str | None,
        registered_kinds: tuple[str, ...],
        source: str,
        repair: str = REGISTRY_LANE,
    ) -> None:
        self.ticker = ticker
        self.corpus_id = corpus_id
        self.registered_kinds = registered_kinds
        self.source = source
        demanded = ticker if corpus_id is None else f"{ticker}/{corpus_id}"
        super().__init__(
            f"no model kind is registered for {demanded}; "
            f"registered kinds in {source}: {list(registered_kinds)}",
            repair=repair,
        )


class ModelKindRegistryInvalid(ModelKindRegistryError):
    """The registry artifact itself is malformed (§14 addition, plan §6)."""

    code = MODEL_KIND_REGISTRY_INVALID

    def __init__(
        self,
        *,
        source: str,
        field: str,
        detail: str,
        repair: str = REGISTRY_REPAIR_LANE,
    ) -> None:
        self.source = source
        self.field = field
        self.detail = detail
        super().__init__(f"{source}: {field}: {detail}", repair=repair)


class ModelKindProfileArtifactInvalid(ModelKindRegistryError):
    """An authored profile artifact does not decode (plan §6).

    Raised by :mod:`schema.model_kind_profile_loader`; defined here so the
    model-kind failure vocabulary has one home and a consumer can catch the
    whole class with :class:`ModelKindRegistryError`.
    """

    code = MODEL_KIND_PROFILE_ARTIFACT_INVALID

    def __init__(
        self,
        *,
        artifact_ref: str,
        field: str,
        detail: str,
        repair: str = MODEL_KIND_DATA_LANE,
    ) -> None:
        self.artifact_ref = artifact_ref
        self.field = field
        self.detail = detail
        super().__init__(f"{artifact_ref}: {field}: {detail}", repair=repair)


@dataclass(frozen=True, slots=True)
class ProfileRef:
    """One row's coverage-profile pointer: an arm and its value."""

    arm: str
    value: str

    @property
    def ref(self) -> str:
        return f"{self.arm}:{self.value}"


@dataclass(frozen=True, slots=True)
class ModelKindEntry:
    """One registered model kind."""

    ticker: str
    corpus_id: str
    packaged_corpus: str
    profile_ref: ProfileRef
    deploy_active: bool

    @property
    def kind(self) -> str:
        return f"{self.ticker}/{self.corpus_id}"


@dataclass(frozen=True, slots=True)
class ModelKindRegistry:
    """The decoded registry table and the directory its artifacts live in."""

    source: Path
    provenance: str
    entries: tuple[ModelKindEntry, ...]

    @property
    def artifact_dir(self) -> Path:
        return self.source.parent

    def registered_kinds(self) -> tuple[str, ...]:
        return tuple(sorted(entry.kind for entry in self.entries))

    def entry_for(self, ticker: str, corpus_id: str) -> ModelKindEntry:
        """The row for one verified corpus; typed refusal when none exists."""

        for entry in self.entries:
            if entry.ticker == ticker and entry.corpus_id == corpus_id:
                return entry
        raise ModelKindUnregistered(
            ticker=ticker,
            corpus_id=corpus_id,
            registered_kinds=self.registered_kinds(),
            source=self.source.name,
        )

    def deploy_active_entry(self, ticker: str) -> ModelKindEntry:
        """The ticker's deploy-active row; typed refusal when none exists."""

        for entry in self.entries:
            if entry.ticker == ticker and entry.deploy_active:
                return entry
        raise ModelKindUnregistered(
            ticker=ticker,
            corpus_id=None,
            registered_kinds=self.registered_kinds(),
            source=self.source.name,
        )


def _invalid(source: str, field: str, detail: str) -> NoReturn:
    raise ModelKindRegistryInvalid(source=source, field=field, detail=detail)


def _require_mapping(value: object, *, source: str, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        _invalid(source, field, f"expected an object, got {type(value).__name__}")
    return value


def _require_string(
    value: object, *, source: str, field: str, pattern: re.Pattern[str] | None = None
) -> str:
    if not isinstance(value, str) or not value:
        _invalid(source, field, "expected a non-empty string")
    if pattern is not None and pattern.fullmatch(value) is None:
        _invalid(source, field, f"{value!r} does not match {pattern.pattern}")
    return value


def _decode_profile_ref(value: object, *, source: str, field: str) -> ProfileRef:
    text = _require_string(value, source=source, field=field)
    arm, separator, target = text.partition(":")
    if not separator or arm != AUTHORED_ARTIFACT_ARM:
        _invalid(source, field, f"{text!r} must be 'artifact:<value>'")
    _require_string(
        target, source=source, field=f"{field}.value", pattern=_ARTIFACT_NAME_RE
    )
    return ProfileRef(arm=arm, value=target)


def _decode_entry(payload: object, *, source: str, index: int) -> ModelKindEntry:
    field = f"model_kinds[{index}]"
    row = _require_mapping(payload, source=source, field=field)
    unknown = sorted(set(row) - _ROW_KEYS)
    if unknown:
        _invalid(source, field, f"unknown keys {unknown}")
    missing = sorted(_ROW_KEYS - set(row))
    if missing:
        _invalid(source, field, f"missing keys {missing}")
    deploy_active = row["deploy_active"]
    if not isinstance(deploy_active, bool):
        _invalid(
            source,
            f"{field}.deploy_active",
            f"expected a boolean, got {type(deploy_active).__name__}",
        )
    return ModelKindEntry(
        ticker=_require_string(
            row["ticker"], source=source, field=f"{field}.ticker", pattern=_TICKER_RE
        ),
        corpus_id=_require_string(
            row["corpus_id"], source=source, field=f"{field}.corpus_id", pattern=_ID_RE
        ),
        packaged_corpus=_require_string(
            row["packaged_corpus"],
            source=source,
            field=f"{field}.packaged_corpus",
            pattern=_ID_RE,
        ),
        profile_ref=_decode_profile_ref(
            row["profile_ref"], source=source, field=f"{field}.profile_ref"
        ),
        deploy_active=deploy_active,
    )


def decode_model_kind_registry(payload: object, *, source: Path) -> ModelKindRegistry:
    """Decode one registry payload fail-closed.

    Every malformation is a typed refusal naming the first offending field:
    an unknown key, a missing key, a value outside its vocabulary, a duplicate
    ``(ticker, corpus_id)`` pair, or a ticker whose deploy-active row is
    missing or ambiguous. The last rule is what makes the table *total* for the
    deploy-affinity question the corpus verifier asks: exactly one packaged
    corpus per registered ticker, never zero and never a silent first match.
    """

    name = source.name
    table = _require_mapping(payload, source=name, field="<root>")
    unknown = sorted(set(table) - _REGISTRY_KEYS)
    if unknown:
        _invalid(name, "<root>", f"unknown keys {unknown}")
    missing = sorted(_REGISTRY_KEYS - set(table))
    if missing:
        _invalid(name, "<root>", f"missing keys {missing}")
    schema_version = _require_string(
        table["schema_version"], source=name, field="schema_version"
    )
    if schema_version != REGISTRY_SCHEMA_VERSION:
        _invalid(
            name,
            "schema_version",
            f"{schema_version!r} is not {REGISTRY_SCHEMA_VERSION!r}",
        )
    provenance = _require_string(table["provenance"], source=name, field="provenance")
    rows = table["model_kinds"]
    if not isinstance(rows, list) or not rows:
        _invalid(name, "model_kinds", "expected a non-empty list of rows")
    entries = tuple(
        _decode_entry(row, source=name, index=index) for index, row in enumerate(rows)
    )
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        key = (entry.ticker, entry.corpus_id)
        if key in seen:
            _invalid(name, "model_kinds", f"duplicate model kind {entry.kind}")
        seen.add(key)
    for ticker in sorted({entry.ticker for entry in entries}):
        active = [
            entry for entry in entries if entry.ticker == ticker and entry.deploy_active
        ]
        if len(active) != 1:
            _invalid(
                name,
                "model_kinds",
                f"{ticker} carries {len(active)} deploy-active rows; exactly one "
                "packaged corpus is deploy-active per registered ticker",
            )
    return ModelKindRegistry(source=source, provenance=provenance, entries=entries)


@lru_cache(maxsize=None)
def _load_registry(resolved: str) -> ModelKindRegistry:
    source = Path(resolved)
    try:
        raw = source.read_bytes()
    except OSError as exc:
        _invalid(source.name, "<file>", f"registry artifact is unreadable: {exc}")
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        _invalid(source.name, "<file>", f"registry artifact is not valid JSON: {exc}")
    return decode_model_kind_registry(payload, source=source)


def load_model_kind_registry(path: Path | None = None) -> ModelKindRegistry:
    """Load and decode the registry artifact (the shipped one by default).

    The result is cached per resolved path: the artifact is checked-in data,
    immutable for the life of a process. :func:`clear_registry_cache` exists
    for tests that rewrite a path in place.
    """

    return _load_registry(str((path or DEFAULT_REGISTRY_PATH).resolve()))


def clear_registry_cache() -> None:
    """Drop the decoded-registry cache."""

    _load_registry.cache_clear()


def resolve_model_kind(
    ticker: str,
    corpus_id: str,
    *,
    registry: ModelKindRegistry | None = None,
) -> ModelKindEntry:
    """Resolve one verified corpus's registry row; typed on absence."""

    return (registry or load_model_kind_registry()).entry_for(ticker, corpus_id)


def active_packaged_corpus(
    ticker: str,
    *,
    registry: ModelKindRegistry | None = None,
) -> str:
    """The packaged corpus that is deploy-active for one ticker.

    Replaces the deploy-affinity constant of plan §3 row V5 with the registry
    row that carries the same fact, and refuses an unregistered ticker with the
    typed absence instead of a silent ``None``.
    """

    table = registry or load_model_kind_registry()
    return table.deploy_active_entry(ticker).packaged_corpus


def artifact_path(entry: ModelKindEntry, *, registry: ModelKindRegistry) -> Path:
    """The authored profile artifact's path for an ``artifact:`` row.

    Fail-closed on residence as well as shape: the ref is a bare file name
    beside the registry (no separators survive ``_ARTIFACT_NAME_RE``), and the
    resolved path must be a real regular file, never a symlink pointing out of
    the model-kind data tree.
    """

    if entry.profile_ref.arm != AUTHORED_ARTIFACT_ARM:
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=entry.profile_ref.ref,
            field="profile_ref",
            detail=f"{entry.kind} does not name an authored profile artifact",
        )
    candidate = registry.artifact_dir / entry.profile_ref.value
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=entry.profile_ref.ref,
            field="<file>",
            detail=f"profile artifact is missing or unreadable: {exc}",
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ModelKindProfileArtifactInvalid(
            artifact_ref=entry.profile_ref.ref,
            field="<file>",
            detail="profile artifact must be a real regular file, not a symlink",
        )
    return candidate


__all__ = [
    "AUTHORED_ARTIFACT_ARM",
    "DEFAULT_REGISTRY_PATH",
    "MODEL_KIND_DATA_LANE",
    "MODEL_KIND_PROFILE_ARTIFACT_INVALID",
    "MODEL_KIND_REGISTRY_INVALID",
    "MODEL_KIND_UNREGISTERED",
    "REGISTRY_LANE",
    "REGISTRY_REPAIR_LANE",
    "REGISTRY_SCHEMA_VERSION",
    "ModelKindEntry",
    "ModelKindProfileArtifactInvalid",
    "ModelKindRegistry",
    "ModelKindRegistryError",
    "ModelKindRegistryInvalid",
    "ModelKindUnregistered",
    "ProfileRef",
    "active_packaged_corpus",
    "artifact_path",
    "clear_registry_cache",
    "decode_model_kind_registry",
    "load_model_kind_registry",
    "resolve_model_kind",
]
