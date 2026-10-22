"""The authored style-guide library — house presentation rules, as data.

Issuer-specific presentation belongs in authored data artifacts, never in
issuer-named branches, constants, regexes, or special-cased driver keys inside
mechanism code. The owner and configuration map is ``schema/README.md``;
build/render context is in ``docs/reference/canonical-schema-spine.md``.

The house style used to be the *default field values* of ``ModelFormatter``,
identified by an issuer-named constant and citing an issuer's workbook as its
benchmark. Both facts are model presentation *content*, and content does not
belong in the module that applies it. Here they are authored artifacts under
``schema/style_guides/``: each carries its own id, its benchmark provenance,
and any rule row that differs from the mechanism's own defaults.

Two resolution entry points, both total and both fail-closed:

* :func:`library_default_style_guide` — the one artifact the library marks as
  its default, which is what a render resolves when no model kind declares a
  style guide (plan §5.1: absence is feature-off, and the feature-off arm here
  is the same house style the formatter shipped with).
* :func:`load_style_guide` — resolution by declared id. A declared id with no
  authored artifact is :class:`StyleGuideUnresolved` (plan §6,
  ``style_guide_unresolved``), never a silent fall back to the default: a model
  kind that declares a style guide has stated that its presentation differs,
  and quietly rendering it in someone else's style is exactly the silent
  default the amendment's §4 forbids.

The refusals derive from ``ValueError`` for the reason IG-3 recorded for the
registry refusals (plan §14 item 13(g)): the render and finalize boundaries
already convert a ``ValueError`` into a typed, fail-closed rejection, and a
refusal outside that hierarchy would turn a rejection into an unhandled fault.

The module names no issuer: it is scanned by
``tests/test_issuer_generic_grep_guard.py`` from birth (plan §11, "every new
module this plan lands carries zero issuer names"). The *artifacts* it reads
may name one — that is data naming data (spec §3.5).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Final, Mapping, NoReturn


#: The style artifact's schema identity. A payload that does not carry exactly
#: this string is refused: a future shape gets a new version and a decoder that
#: knows it, never a silent best-effort read.
STYLE_GUIDE_SCHEMA_VERSION: Final = "style-guide.v1"

#: The checked-in style-guide directory this package ships.
DEFAULT_STYLE_GUIDE_DIR: Final = Path(__file__).resolve().parent / "style_guides"

#: Plan §6 code.
STYLE_GUIDE_UNRESOLVED: Final = "style_guide_unresolved"
#: §14 addition at IG-5: a malformed or ambiguous artifact is its own failure
#: mode with its own repair lane, exactly as IG-3's ``model_kind_registry_invalid``
#: is separate from ``model_kind_unregistered``. Folding the two together would
#: send the reader to the wrong lane.
STYLE_GUIDE_LIBRARY_INVALID: Final = "style_guide_library_invalid"

#: Repair lanes named by the refusals (plan §6).
STYLE_LANE: Final = "author the style guide artifact in the style data lane"
STYLE_REPAIR_LANE: Final = "repair the style guide artifact in the style data lane"

_ARTIFACT_KEYS: Final = frozenset(
    {
        "schema_version",
        "style_guide_id",
        "library_default",
        "source_reference",
        "provenance",
        "rules",
    }
)


class StyleGuideError(ValueError):
    """Root of the style-library refusal vocabulary (plan §6)."""

    code: str = "style_guide_refused"

    def __init__(self, message: str, *, repair: str) -> None:
        self.repair = repair
        super().__init__(f"{self.code}: {message} — {repair}")


class StyleGuideUnresolved(StyleGuideError):
    """A declared style guide id names no authored artifact (plan §6)."""

    code = STYLE_GUIDE_UNRESOLVED

    def __init__(self, *, style_guide_id: str, authored: tuple[str, ...]) -> None:
        self.style_guide_id = style_guide_id
        self.authored = authored
        super().__init__(
            f"style guide {style_guide_id!r} is declared but not authored; the "
            f"library holds {list(authored)}",
            repair=STYLE_LANE,
        )


class StyleGuideLibraryInvalid(StyleGuideError):
    """An authored style artifact is malformed, or the library is ambiguous."""

    code = STYLE_GUIDE_LIBRARY_INVALID

    def __init__(self, *, artifact: str, field: str, detail: str) -> None:
        self.artifact = artifact
        self.field = field
        self.detail = detail
        super().__init__(
            f"style guide artifact {artifact!r} is invalid at {field!r}: {detail}",
            repair=STYLE_REPAIR_LANE,
        )


def _invalid(artifact: str, field: str, detail: str) -> NoReturn:
    raise StyleGuideLibraryInvalid(artifact=artifact, field=field, detail=detail)


@dataclass(frozen=True)
class StyleGuide:
    """One authored presentation style: its identity, benchmark, and rule rows.

    ``rules`` are field overrides applied over the mechanism's own defaults, so
    an artifact states only what it changes and the mechanism keeps owning the
    shape of a formatting rule.
    """

    style_guide_id: str
    source_reference: str
    rules: Mapping[str, Any]
    library_default: bool = False


def _decode_style_guide(payload: Any, *, artifact: str) -> StyleGuide:
    if not isinstance(payload, Mapping):
        _invalid(artifact, "<artifact>", "a style guide artifact must be an object")
    unknown = sorted(set(payload) - _ARTIFACT_KEYS)
    if unknown:
        _invalid(artifact, unknown[0], "unknown key in a style guide artifact")
    version = payload.get("schema_version")
    if version != STYLE_GUIDE_SCHEMA_VERSION:
        _invalid(
            artifact,
            "schema_version",
            f"expected {STYLE_GUIDE_SCHEMA_VERSION!r}, read {version!r}",
        )
    for field_name in ("style_guide_id", "source_reference"):
        value = payload.get(field_name)
        if not isinstance(value, str) or not value.strip():
            _invalid(artifact, field_name, "must be a non-empty string")
    library_default = payload.get("library_default", False)
    if not isinstance(library_default, bool):
        _invalid(artifact, "library_default", "must be a boolean")
    rules = payload.get("rules", {})
    if not isinstance(rules, Mapping):
        _invalid(artifact, "rules", "must be an object of field overrides")
    for rule_name in rules:
        if not isinstance(rule_name, str) or not rule_name.strip():
            _invalid(artifact, "rules", "every rule name must be a non-empty string")
    return StyleGuide(
        style_guide_id=str(payload["style_guide_id"]).strip(),
        source_reference=str(payload["source_reference"]).strip(),
        rules=dict(rules),
        library_default=library_default,
    )


@lru_cache(maxsize=None)
def _library(directory: str) -> tuple[StyleGuide, ...]:
    root = Path(directory)
    if not root.is_dir():
        _invalid(directory, "<library>", "the style guide directory does not exist")
    guides: list[StyleGuide] = []
    for path in sorted(root.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            _invalid(path.name, "<artifact>", f"unreadable style guide artifact: {exc}")
        guides.append(_decode_style_guide(payload, artifact=path.name))
    ids = [guide.style_guide_id for guide in guides]
    duplicated = sorted({value for value in ids if ids.count(value) > 1})
    if duplicated:
        _invalid(directory, "style_guide_id", f"declared twice: {duplicated}")
    defaults = [guide.style_guide_id for guide in guides if guide.library_default]
    if len(defaults) != 1:
        _invalid(
            directory,
            "library_default",
            "exactly one authored style guide must be the library default; "
            f"{len(defaults)} are marked ({defaults})",
        )
    return tuple(guides)


def authored_style_guide_ids(
    *, directory: Path | str = DEFAULT_STYLE_GUIDE_DIR
) -> tuple[str, ...]:
    """Every authored style guide id, sorted — the refusal's starved identity."""

    return tuple(sorted(guide.style_guide_id for guide in _library(str(directory))))


def library_default_style_guide(
    *, directory: Path | str = DEFAULT_STYLE_GUIDE_DIR
) -> StyleGuide:
    """The one artifact the library marks default (the feature-off arm)."""

    for guide in _library(str(directory)):
        if guide.library_default:
            return guide
    # Unreachable: `_library` refuses a library without exactly one default.
    _invalid(str(directory), "library_default", "no default style guide is authored")


def load_style_guide(
    style_guide_id: str, *, directory: Path | str = DEFAULT_STYLE_GUIDE_DIR
) -> StyleGuide:
    """Resolve a declared style guide id, fail-closed on a dangling one."""

    wanted = str(style_guide_id or "").strip()
    guides = _library(str(directory))
    if not wanted:
        raise StyleGuideUnresolved(
            style_guide_id=str(style_guide_id),
            authored=tuple(sorted(guide.style_guide_id for guide in guides)),
        )
    for guide in guides:
        if guide.style_guide_id == wanted:
            return guide
    raise StyleGuideUnresolved(
        style_guide_id=wanted,
        authored=tuple(sorted(guide.style_guide_id for guide in guides)),
    )


__all__ = [
    "DEFAULT_STYLE_GUIDE_DIR",
    "STYLE_GUIDE_LIBRARY_INVALID",
    "STYLE_GUIDE_SCHEMA_VERSION",
    "STYLE_GUIDE_UNRESOLVED",
    "StyleGuide",
    "StyleGuideError",
    "StyleGuideLibraryInvalid",
    "StyleGuideUnresolved",
    "authored_style_guide_ids",
    "library_default_style_guide",
    "load_style_guide",
]
