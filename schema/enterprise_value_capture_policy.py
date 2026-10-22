"""Enterprise-value capture policy — which concepts the enterprise-value roles read, as data.

Issuer-specific capture choices belong in authored data artifacts, never in
issuer-named mechanism branches, constants, regexes, or special-cased driver
keys. See ``schema/README.md`` and
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``.

The dated enterprise-value projection makes two consumer choices on top of the
filing: **which XBRL concepts count as each enterprise-value role**, and **which
selected roles enter a declared adjustment with which sign**. Both live in one
authored artifact, ``schema/capture_policies/enterprise_value.json``, read here.

The artifact holds no issuer rows. It is one generic taxonomy rule applied to
every issuer's own filings, and there is no refusal for an issuer the policy
does not list: no such issuer exists. Each role names ordered alternative
concept groups — a partition of the balance (its parts are summed) and then
the stated total that stands in when the filing states no part. The first
group the filing states is the one read; the selection itself is applied by the
projector in ``api/research/enterprise_value.py``.

The refusal derives from ``ValueError`` for the reason IG-3 recorded for the
registry refusals (plan §14 item 13(g)): the bundle-build boundary already
converts a ``ValueError`` from this lane into a typed, fail-closed rejection,
and a refusal outside that hierarchy would turn a rejection into an unhandled
fault. Only a malformed shipped artifact refuses
(:class:`EnterpriseValueCapturePolicyInvalid`).

The module names no issuer (plan §11).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Final, Literal, Mapping, NoReturn

from .build_model_product_contracts import DeclaredEnterpriseValueAdjustmentEffect


#: The artifact's schema identity. A payload that does not carry exactly this
#: string is refused: a future shape gets a new version and a decoder that knows
#: it, never a silent best-effort read.
CAPTURE_POLICY_SCHEMA_VERSION: Final = "enterprise-value-capture-policy.v2"

#: The checked-in capture-policy artifact this package ships.
DEFAULT_CAPTURE_POLICY_PATH: Final = (
    Path(__file__).resolve().parent / "capture_policies" / "enterprise_value.json"
)

#: §14 addition at IG-5: a malformed artifact is its own failure mode with its
#: own repair lane (IG-3's ``model_kind_registry_invalid`` precedent).
EV_CAPTURE_POLICY_INVALID: Final = "ev_capture_policy_invalid"

#: Repair lane named by the refusal (plan §6).
CAPTURE_POLICY_REPAIR_LANE: Final = (
    "repair the enterprise-value capture-policy artifact in the capture-policy "
    "data lane"
)

#: The default basis stamped on a declared adjustment that names no other.
DEFAULT_DECLARED_ADJUSTMENT_BASIS: Final = (
    "issuer_presented_balance_declared_disclosure_only"
)

_ARTIFACT_KEYS: Final = frozenset(
    {"schema_version", "provenance", "qname_selection", "declared_adjustments"}
)
_ADJUSTMENT_KEYS: Final = frozenset({"role_label", "effect", "basis", "legs"})
_LEG_KEYS: Final = frozenset({"role_label", "sign"})


class EnterpriseValueCapturePolicyError(ValueError):
    """Root of the capture-policy refusal vocabulary (plan §6)."""

    code: str = "ev_capture_policy_refused"

    def __init__(self, message: str, *, repair: str) -> None:
        self.repair = repair
        super().__init__(f"{self.code}: {message} — {repair}")


class EnterpriseValueCapturePolicyInvalid(EnterpriseValueCapturePolicyError):
    """The authored capture-policy artifact is malformed."""

    code = EV_CAPTURE_POLICY_INVALID

    def __init__(self, *, field: str, detail: str) -> None:
        self.field = field
        self.detail = detail
        super().__init__(
            f"enterprise-value capture policy is invalid at {field!r}: {detail}",
            repair=CAPTURE_POLICY_REPAIR_LANE,
        )


def _invalid(field: str, detail: str) -> NoReturn:
    raise EnterpriseValueCapturePolicyInvalid(field=field, detail=detail)


@dataclass(frozen=True, slots=True)
class DeclaredAdjustmentLegSpec:
    """One selected role entering a declared adjustment with a stated sign.

    ``sign`` is declared valuation treatment, not a restatement of a provider
    fact, and it is never inferred from leg order: a positional rule silently
    encodes one industry's balance-sheet shape and yields a plausible-looking
    wrong number for any other ordering.
    """

    role_label: str
    sign: Literal["add", "subtract"]


@dataclass(frozen=True, slots=True)
class DeclaredAdjustmentSpec:
    """A company-specific enterprise-value adjustment, declared as data.

    A selected non-core role that no spec claims defaults to its own single-leg
    ``disclosure_only`` row: one gross balance, source-bound, entering no
    arithmetic. That default asserts no offsetting and infers no sign, so a new
    selected role can never move enterprise value without an explicit
    declaration.
    """

    role_label: str
    effect: DeclaredEnterpriseValueAdjustmentEffect
    legs: tuple[DeclaredAdjustmentLegSpec, ...]
    basis: str = DEFAULT_DECLARED_ADJUSTMENT_BASIS


@dataclass(frozen=True, slots=True)
class EnterpriseValueCapturePolicy:
    """The authored enterprise-value capture policy, one for every issuer.

    ``qname_selection`` maps each role to ordered alternative concept groups.
    The first group with at least one stated, undimensioned concept is the
    role's concepts; a group lists a partition of the balance (its parts are
    summed) and a later group lists the stated total that stands in when the
    filing states no part.
    """

    qname_selection: Mapping[str, tuple[tuple[str, ...], ...]]
    declared_adjustments: tuple[DeclaredAdjustmentSpec, ...]


def _decode_leg(payload: Any, *, where: str) -> DeclaredAdjustmentLegSpec:
    if not isinstance(payload, Mapping):
        _invalid(where, "a declared adjustment leg must be an object")
    unknown = sorted(set(payload) - _LEG_KEYS)
    if unknown:
        _invalid(f"{where}.{unknown[0]}", "unknown key in a declared adjustment leg")
    role_label = payload.get("role_label")
    sign = payload.get("sign")
    if not isinstance(role_label, str) or not role_label.strip():
        _invalid(f"{where}.role_label", "must be a non-empty string")
    if sign not in ("add", "subtract"):
        _invalid(f"{where}.sign", f"must be 'add' or 'subtract', read {sign!r}")
    return DeclaredAdjustmentLegSpec(role_label=role_label.strip(), sign=sign)


def _decode_adjustment(payload: Any, *, where: str) -> DeclaredAdjustmentSpec:
    if not isinstance(payload, Mapping):
        _invalid(where, "a declared adjustment must be an object")
    unknown = sorted(set(payload) - _ADJUSTMENT_KEYS)
    if unknown:
        _invalid(f"{where}.{unknown[0]}", "unknown key in a declared adjustment")
    role_label = payload.get("role_label")
    if not isinstance(role_label, str) or not role_label.strip():
        _invalid(f"{where}.role_label", "must be a non-empty string")
    effect = payload.get("effect")
    if effect not in (
        "disclosure_only",
        "adds_to_enterprise_value",
        "subtracts_from_enterprise_value",
    ):
        _invalid(
            f"{where}.effect",
            "must be one of 'disclosure_only', 'adds_to_enterprise_value', "
            f"'subtracts_from_enterprise_value'; read {effect!r}",
        )
    basis = payload.get("basis", DEFAULT_DECLARED_ADJUSTMENT_BASIS)
    if not isinstance(basis, str) or not basis.strip():
        _invalid(f"{where}.basis", "must be a non-empty string")
    legs = payload.get("legs")
    if not isinstance(legs, (list, tuple)) or not legs:
        _invalid(f"{where}.legs", "a declared adjustment must carry at least one leg")
    return DeclaredAdjustmentSpec(
        role_label=role_label.strip(),
        effect=effect,
        legs=tuple(
            _decode_leg(leg, where=f"{where}.legs[{index}]")
            for index, leg in enumerate(legs)
        ),
        basis=basis.strip(),
    )


def _decode_selection(
    selection: Any,
) -> Mapping[str, tuple[tuple[str, ...], ...]]:
    where = "qname_selection"
    if not isinstance(selection, Mapping) or not selection:
        _invalid(where, "the policy must select at least one role")
    decoded: dict[str, tuple[tuple[str, ...], ...]] = {}
    for role, groups in selection.items():
        if not isinstance(role, str) or not role.strip():
            _invalid(where, "every role must be a non-empty string")
        role_where = f"{where}.{role}"
        if not isinstance(groups, (list, tuple)) or not groups:
            _invalid(role_where, "a selected role must name at least one concept group")
        decoded_groups: list[tuple[str, ...]] = []
        for position, group in enumerate(groups):
            group_where = f"{role_where}[{position}]"
            if not isinstance(group, (list, tuple)) or not group:
                _invalid(group_where, "a concept group must name at least one concept")
            if any(not isinstance(qname, str) or not qname.strip() for qname in group):
                _invalid(group_where, "every selected concept must be a non-empty string")
            decoded_groups.append(tuple(qname.strip() for qname in group))
        decoded[role.strip()] = tuple(decoded_groups)
    return decoded


def _decode_adjustments(adjustments: Any) -> tuple[DeclaredAdjustmentSpec, ...]:
    where = "declared_adjustments"
    if not isinstance(adjustments, (list, tuple)):
        _invalid(where, "must be a list of adjustments")
    return tuple(
        _decode_adjustment(adjustment, where=f"{where}[{position}]")
        for position, adjustment in enumerate(adjustments)
    )


@lru_cache(maxsize=None)
def _policy(path: str) -> EnterpriseValueCapturePolicy:
    artifact = Path(path)
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        _invalid("<artifact>", f"unreadable capture-policy artifact: {exc}")
    if not isinstance(payload, Mapping):
        _invalid("<artifact>", "the capture-policy artifact must be an object")
    unknown = sorted(set(payload) - _ARTIFACT_KEYS)
    if unknown:
        _invalid(unknown[0], "unknown key in the capture-policy artifact")
    missing = sorted(_ARTIFACT_KEYS - set(payload))
    if missing:
        _invalid(missing[0], "missing key in the capture-policy artifact")
    version = payload.get("schema_version")
    if version != CAPTURE_POLICY_SCHEMA_VERSION:
        _invalid(
            "schema_version",
            f"expected {CAPTURE_POLICY_SCHEMA_VERSION!r}, read {version!r}",
        )
    provenance = payload.get("provenance")
    if not isinstance(provenance, str) or not provenance.strip():
        _invalid("provenance", "must be a non-empty string")
    return EnterpriseValueCapturePolicy(
        qname_selection=_decode_selection(payload.get("qname_selection")),
        declared_adjustments=_decode_adjustments(payload.get("declared_adjustments")),
    )


def generic_enterprise_value_capture_policy(
    *, path: Path | str = DEFAULT_CAPTURE_POLICY_PATH
) -> EnterpriseValueCapturePolicy:
    """The one capture policy every issuer's own filings are read under.

    A malformed shipped artifact raises
    :class:`EnterpriseValueCapturePolicyInvalid`; there is no per-issuer
    lookup and so no issuer for which the policy can be absent.
    """

    return _policy(str(path))


__all__ = [
    "CAPTURE_POLICY_REPAIR_LANE",
    "CAPTURE_POLICY_SCHEMA_VERSION",
    "DEFAULT_CAPTURE_POLICY_PATH",
    "DEFAULT_DECLARED_ADJUSTMENT_BASIS",
    "EV_CAPTURE_POLICY_INVALID",
    "DeclaredAdjustmentLegSpec",
    "DeclaredAdjustmentSpec",
    "EnterpriseValueCapturePolicy",
    "EnterpriseValueCapturePolicyError",
    "EnterpriseValueCapturePolicyInvalid",
    "generic_enterprise_value_capture_policy",
]
