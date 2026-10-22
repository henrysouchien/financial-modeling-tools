"""Checked-in source-model inputs for template regeneration.

The template builder must run from checked-in inputs on any machine; ambient
operator workbook paths are provenance only, never runtime inputs. See
``schema/templates/source_inputs/README.md`` and ``schema/README.md``.

Before this module the builder's inputs were two absolute paths into one
operator's cloud-storage folder. Regeneration therefore *could not run* on any
other machine, and the artifact the compile actually loads had no reproducible
derivation — a checked-in output whose input lived outside the repository.
Stage D's golden regeneration (§3 row T4) inherits that constraint, so it is
discharged here, ahead of it.

The input is now an **artifact**: one envelope per template target, carrying

* ``model`` — the parsed source workbook, serialized through the same
  ``FinancialModel`` contract every other template artifact uses, so there is
  one serialization per contract and no fixture dialect; and
* ``provenance`` — the workbook the capture came from, the reader mode and
  options it was parsed with, and the workbook's content hash. **The machine-
  local path survives only here, as a provenance string**: nothing reads it,
  and re-capturing from a newer workbook is an authoring act with a recorded
  diff, not a silent rebuild against whatever happens to be on one laptop.

Three refusals, all fail-closed and each naming a repair lane. Together they
are **total over the read path**: every way a capture can fail to become a
model — absent, unreadable, undecodable — leaves through one of them, never as
a bare ``OSError`` or ``UnicodeDecodeError`` with no code and no lane.

* :class:`TemplateRegenerationInputMissing` (plan §6) — the builder was invoked
  without its checked-in input (no such file, a directory in its place, or a
  path whose parent is a file). This is the refusal that replaces the deleted
  machine-local path arms: an absent input is never an empty model, never a
  re-parse of some ambient workbook, and never a silent skip.
* :class:`TemplateSourceInputUnreadable` — the input is checked in but this
  process cannot read its bytes (permissions, an I/O fault, an unusable path).
  Its own code because its own repair lane: telling the operator the artifact
  "is not checked in" would be false, and sending them to fix its *contents*
  would be the wrong lane.
* :class:`TemplateSourceInputInvalid` — the input reads but does not decode
  (not UTF-8, not JSON, or a shape defect at a named field).

The two codes beyond §6's single row are recorded as the §6 amendment they
are, on the precedent the registry and style-library refusals set (plan §14
items 13(f), 17); the ``ValueError`` derivation they all share is plan §14
item 13(g)'s, recorded on :class:`TemplateSourceInputError`.

The module names no issuer: it is scanned by
``tests/test_issuer_generic_grep_guard.py`` from birth (plan §11, "every new
module this plan lands carries zero issuer names"). Reference-lane captures
live on the test side with the reference builder itself (plan §5.3's residence
menu), and this module is generic over the path it is handed.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, ClassVar, Final, Mapping, NoReturn

from ..models import FinancialModel


#: The source-input artifact's schema identity. A payload that does not carry
#: exactly this string is refused: a future shape gets a new version and a
#: decoder that knows it, never a silent best-effort read.
TEMPLATE_SOURCE_INPUT_SCHEMA_VERSION: Final = "template-source-model.v1"

#: The checked-in source-input directory this package ships.
TEMPLATE_SOURCE_INPUT_DIR: Final = Path(__file__).resolve().parent / "source_inputs"

#: Plan §6 code: the builder was invoked without its checked-in input.
TEMPLATE_REGENERATION_INPUT_MISSING: Final = "template_regeneration_input_missing"
#: A checked-in input that exists but does not decode — its own repair lane.
TEMPLATE_SOURCE_INPUT_INVALID: Final = "template_source_input_invalid"
#: A checked-in input whose bytes cannot be read at all — its own repair lane.
TEMPLATE_SOURCE_INPUT_UNREADABLE: Final = "template_source_input_unreadable"

#: Repair lanes named by the refusals (plan §6).
TEMPLATE_INPUT_LANE: Final = (
    "capture the source workbook into the template data lane "
    "(schema/templates/source_inputs/) and check the artifact in"
)
TEMPLATE_INPUT_REPAIR_LANE: Final = (
    "repair the checked-in source-input artifact in the template data lane"
)
TEMPLATE_INPUT_ACCESS_LANE: Final = (
    "restore read access to the checked-in source-input artifact in the "
    "template data lane"
)

_ENVELOPE_KEYS: Final = frozenset({"schema_version", "input_ref", "provenance", "model"})
_PROVENANCE_KEYS: Final = frozenset(
    {"source_workbook", "reader_mode", "reader_options", "source_sha256"}
)


class TemplateSourceInputError(ValueError):
    """Root of the template source-input refusal vocabulary (plan §6).

    Derives from ``ValueError`` for the reason the registry and style-library
    refusals do (plan §14 item 13(g)): the builder and regeneration boundaries
    already convert a ``ValueError`` into a typed, fail-closed rejection, and a
    refusal outside that hierarchy would turn a rejection into an unhandled
    fault.
    """

    code: ClassVar[str] = "template_source_input_refused"

    def __init__(self, message: str, *, repair: str) -> None:
        self.repair = repair
        super().__init__(f"{self.code}: {message} — {repair}")


class TemplateRegenerationInputMissing(TemplateSourceInputError):
    """The builder was invoked without its checked-in input (plan §6)."""

    code: ClassVar[str] = TEMPLATE_REGENERATION_INPUT_MISSING

    def __init__(self, *, input_ref: str, path: Path) -> None:
        self.input_ref = input_ref
        self.path = path
        super().__init__(
            f"template source input {input_ref!r} is not checked in at "
            f"{path.as_posix()}; regeneration reads checked-in inputs only, "
            "never a machine-local workbook",
            repair=TEMPLATE_INPUT_LANE,
        )


class TemplateSourceInputUnreadable(TemplateSourceInputError):
    """The checked-in input is present but its bytes cannot be read.

    Permissions, an I/O fault, or a path the operating system refuses to open.
    Not the missing-input code — the artifact *is* checked in, and saying
    otherwise would send the reader to the capture lane for a problem the
    capture does not have.
    """

    code: ClassVar[str] = TEMPLATE_SOURCE_INPUT_UNREADABLE

    def __init__(self, *, input_ref: str, path: Path, detail: str) -> None:
        self.input_ref = input_ref
        self.path = path
        self.detail = detail
        super().__init__(
            f"template source input {input_ref!r} at {path.as_posix()} could "
            f"not be read: {detail}",
            repair=TEMPLATE_INPUT_ACCESS_LANE,
        )


class TemplateSourceInputInvalid(TemplateSourceInputError):
    """A checked-in source-input artifact is malformed."""

    code: ClassVar[str] = TEMPLATE_SOURCE_INPUT_INVALID

    def __init__(self, *, input_ref: str, field: str, detail: str) -> None:
        self.input_ref = input_ref
        self.field = field
        self.detail = detail
        super().__init__(
            f"template source input {input_ref!r} is invalid at {field!r}: {detail}",
            repair=TEMPLATE_INPUT_REPAIR_LANE,
        )


def _invalid(input_ref: str, field: str, detail: str) -> NoReturn:
    raise TemplateSourceInputInvalid(input_ref=input_ref, field=field, detail=detail)


@dataclass(frozen=True)
class SourceWorkbookProvenance:
    """Where a capture came from — recorded, never read as an input.

    ``source_workbook`` is the machine-local path the capture was taken from.
    It is the *only* surviving trace of the deleted path constants (plan §7
    IG-6: "the workbook paths survive only as provenance strings in the
    artifacts' metadata"), and nothing in the build reads it.
    """

    source_workbook: str
    reader_mode: str
    reader_options: Mapping[str, Any]
    source_sha256: str


@dataclass(frozen=True)
class TemplateSourceInput:
    """One decoded checked-in input: its identity, provenance and model."""

    input_ref: str
    provenance: SourceWorkbookProvenance
    model: FinancialModel


def source_input_path(input_ref: str) -> Path:
    """The checked-in artifact path for one template target."""

    return TEMPLATE_SOURCE_INPUT_DIR / f"{input_ref}.source_model.json"


def workbook_sha256(path: Path) -> str:
    """Content hash of a source workbook, for the capture's provenance."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def encode_template_source_input(
    *,
    input_ref: str,
    model: FinancialModel,
    provenance: SourceWorkbookProvenance,
) -> str:
    """Serialize a capture. The inverse of :func:`decode_template_source_input`.

    Compact separators and a trailing newline: this is a captured artifact
    reviewed by its provenance and its hash, not an authored one reviewed line
    by line, and the indented form is more than twice the size.
    """

    payload = {
        "schema_version": TEMPLATE_SOURCE_INPUT_SCHEMA_VERSION,
        "input_ref": input_ref,
        "provenance": {
            "source_workbook": provenance.source_workbook,
            "reader_mode": provenance.reader_mode,
            "reader_options": dict(provenance.reader_options),
            "source_sha256": provenance.source_sha256,
        },
        "model": json.loads(model.model_dump_json()),
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=False) + "\n"


def decode_template_source_input(payload: Any, *, input_ref: str) -> TemplateSourceInput:
    """Decode one capture, fail-closed on every shape defect."""

    if not isinstance(payload, Mapping):
        _invalid(input_ref, "<artifact>", "a source-input artifact must be an object")
    unknown = sorted(set(payload) - _ENVELOPE_KEYS)
    if unknown:
        _invalid(input_ref, unknown[0], "unknown key in a source-input artifact")
    missing = sorted(_ENVELOPE_KEYS - set(payload))
    if missing:
        _invalid(input_ref, missing[0], "required key is absent")

    version = payload["schema_version"]
    if version != TEMPLATE_SOURCE_INPUT_SCHEMA_VERSION:
        _invalid(
            input_ref,
            "schema_version",
            f"expected {TEMPLATE_SOURCE_INPUT_SCHEMA_VERSION!r}, read {version!r}",
        )
    declared_ref = payload["input_ref"]
    if declared_ref != input_ref:
        _invalid(
            input_ref,
            "input_ref",
            f"artifact declares {declared_ref!r}; it was loaded as {input_ref!r}",
        )

    raw_provenance = payload["provenance"]
    if not isinstance(raw_provenance, Mapping):
        _invalid(input_ref, "provenance", "provenance must be an object")
    unknown_provenance = sorted(set(raw_provenance) - _PROVENANCE_KEYS)
    if unknown_provenance:
        _invalid(input_ref, "provenance", f"unknown key {unknown_provenance[0]!r}")
    missing_provenance = sorted(_PROVENANCE_KEYS - set(raw_provenance))
    if missing_provenance:
        _invalid(
            input_ref,
            "provenance",
            f"required key {missing_provenance[0]!r} is absent",
        )
    reader_options = raw_provenance["reader_options"]
    if not isinstance(reader_options, Mapping):
        _invalid(input_ref, "provenance.reader_options", "reader_options must be an object")
    for field in ("source_workbook", "reader_mode", "source_sha256"):
        value = raw_provenance[field]
        if not isinstance(value, str) or not value.strip():
            _invalid(input_ref, f"provenance.{field}", "must be a non-empty string")

    try:
        model = FinancialModel.model_validate(payload["model"])
    except Exception as exc:
        _invalid(input_ref, "model", f"does not decode as a FinancialModel: {exc}")

    return TemplateSourceInput(
        input_ref=input_ref,
        provenance=SourceWorkbookProvenance(
            source_workbook=raw_provenance["source_workbook"],
            reader_mode=raw_provenance["reader_mode"],
            reader_options=dict(reader_options),
            source_sha256=raw_provenance["source_sha256"],
        ),
        model=model,
    )


def load_template_source_input(path: Path, *, input_ref: str) -> TemplateSourceInput:
    """Read one checked-in capture, fail-closed on every read-side failure.

    The three refusals of this module's vocabulary are **total** over this
    function: the path does not resolve to a file (plan §6's missing-input
    refusal), its bytes cannot be read, or they do not decode. Nothing leaves
    here as an untyped ``OSError`` or ``UnicodeDecodeError`` — a refusal with
    no code and no repair lane is the failure mode the plan's §6 discipline
    exists to prevent, and the callers' ``except (TypeError, ValueError)``
    boundaries would not catch one either.
    """

    try:
        raw = path.read_text(encoding="utf-8")
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as exc:
        raise TemplateRegenerationInputMissing(input_ref=input_ref, path=path) from exc
    except UnicodeDecodeError as exc:
        raise TemplateSourceInputInvalid(
            input_ref=input_ref,
            field="<artifact>",
            detail=f"is not UTF-8 text: {exc}",
        ) from exc
    except OSError as exc:
        raise TemplateSourceInputUnreadable(
            input_ref=input_ref, path=path, detail=str(exc)
        ) from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TemplateSourceInputInvalid(
            input_ref=input_ref,
            field="<artifact>",
            detail=f"is not valid JSON: {exc}",
        ) from exc
    return decode_template_source_input(payload, input_ref=input_ref)


def load_template_source_model(path: Path, *, input_ref: str) -> FinancialModel:
    """The parsed source workbook a template build starts from."""

    return load_template_source_input(path, input_ref=input_ref).model


__all__ = [
    "TEMPLATE_SOURCE_INPUT_SCHEMA_VERSION",
    "TEMPLATE_SOURCE_INPUT_DIR",
    "TEMPLATE_REGENERATION_INPUT_MISSING",
    "TEMPLATE_SOURCE_INPUT_INVALID",
    "TEMPLATE_SOURCE_INPUT_UNREADABLE",
    "TEMPLATE_INPUT_LANE",
    "TEMPLATE_INPUT_REPAIR_LANE",
    "TEMPLATE_INPUT_ACCESS_LANE",
    "TemplateSourceInputError",
    "TemplateRegenerationInputMissing",
    "TemplateSourceInputUnreadable",
    "TemplateSourceInputInvalid",
    "SourceWorkbookProvenance",
    "TemplateSourceInput",
    "source_input_path",
    "workbook_sha256",
    "encode_template_source_input",
    "decode_template_source_input",
    "load_template_source_input",
    "load_template_source_model",
]
