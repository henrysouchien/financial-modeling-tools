"""In-memory provider registry with observable routing decisions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import InitVar, dataclass
from typing import Generic, Literal, TypeAlias, TypeVar, overload

from schema.model_state_identity import validate_sha256_hex
from schema.versioned_artifact import VersionedArtifactRef, domain_sha256

from industry_slice_core.profile_normalization import ProviderCapability
from .contracts import CapabilityNotAdvertisedError
from .ports import (
    CompanyProfilePort,
    EstimatesPort,
    IssuerUniversePort,
    KpiObservationsPort,
    MetricSeriesPort,
    NonGaapBridgesPort,
    PeerUniversePort,
    PriceSeriesPort,
    QuotePort,
    StatementEvidenceQueryPort,
    StatementFactsPort,
    ValuationCompsEvidenceQueryPort,
)


ProviderId: TypeAlias = str
MissingValuePolicy: TypeAlias = Literal["preserve", "zero_is_missing"]
CapabilityImplementation: TypeAlias = (
    StatementFactsPort
    | StatementEvidenceQueryPort
    | ValuationCompsEvidenceQueryPort
    | CompanyProfilePort
    | QuotePort
    | EstimatesPort
    | IssuerUniversePort
    | KpiObservationsPort
    | NonGaapBridgesPort
    | MetricSeriesPort
    | PeerUniversePort
    | PriceSeriesPort
)
ImplementationT_co = TypeVar(
    "ImplementationT_co",
    bound=CapabilityImplementation,
    covariant=True,
)

FMP_PROVIDER_ID: ProviderId = "fmp"
EDGAR_PROVIDER_ID: ProviderId = "edgar"
LEGACY_PROVIDER_IDS = frozenset({FMP_PROVIDER_ID, EDGAR_PROVIDER_ID})
# Legacy population accepts this token only to emit its dedicated retirement
# error; it is not a provider and must never enter a route or artifact field.
NON_PROVIDER_SOURCE_SENTINELS = frozenset({"both"})
_AUTHORITATIVE_ANNUAL_CAPABILITIES = frozenset(
    {
        ProviderCapability.STATEMENT_FACTS,
        ProviderCapability.OPERATING_OBSERVATIONS,
        ProviderCapability.ISSUER_UNIVERSE,
        ProviderCapability.VALUATION_COMPS,
    }
)


class UnknownProviderIdError(ValueError):
    """A provider ID was used without a corresponding registry entry."""

    def __init__(
        self,
        provider_id: ProviderId,
        *,
        known_provider_ids: Iterable[ProviderId],
        entry_point: str = "provider ID",
    ) -> None:
        self.provider_id = provider_id
        self.known_provider_ids = frozenset(known_provider_ids)
        known = ", ".join(sorted(self.known_provider_ids)) or "none"
        super().__init__(
            f"{entry_point} {provider_id!r} is not registered "
            f"(known provider IDs: {known})"
        )


def normalize_provider_id(value: object) -> ProviderId:
    """Normalize provider names to canonical registry IDs."""

    provider_id = str(value or "").strip().lower()
    if not provider_id:
        raise ValueError("provider ID must be a non-empty string")
    return provider_id


def require_registered_provider_id(
    value: object,
    *,
    provider_registry: ProviderRegistry | None = None,
    entry_point: str = "provider ID",
) -> ProviderId:
    """Normalize and validate a provider ID against the active registry.

    Artifact loads without runtime context accept only the two legacy IDs.
    Callers loading an artifact for an extension provider pass
    ``context={"provider_registry": registry}`` to Pydantic validation.
    """

    provider_id = normalize_provider_id(value)
    known = (
        provider_registry.known_provider_ids()
        if provider_registry is not None
        else LEGACY_PROVIDER_IDS
    )
    if provider_id not in known:
        raise UnknownProviderIdError(
            provider_id,
            known_provider_ids=known,
            entry_point=entry_point,
        )
    return provider_id


def inject_provider_registry_validation_context(
    _model_type: type[object],
    value: object,
    info: object,
) -> object:
    """Carry Pydantic validation context into a stdlib-dataclass ``InitVar``."""

    context = getattr(info, "context", None)
    if not isinstance(value, dict) or not isinstance(context, Mapping):
        return value
    registry = context.get("provider_registry")
    if not isinstance(registry, ProviderRegistry):
        return value
    payload = dict(value)
    payload.setdefault("provider_registry", registry)
    return payload


@dataclass(frozen=True)
class RoutingContext:
    """Minimal routing context for capability resolution."""

    preferred_provider: ProviderId | None = None


@dataclass(frozen=True)
class RoutingDecision:
    """Trusted record of which provider was selected for a capability fetch."""

    provider_id: ProviderId
    capability: ProviderCapability
    provider_registry: InitVar[object] = None

    def __post_init__(self, provider_registry: object) -> None:
        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        object.__setattr__(
            self,
            "provider_id",
            require_registered_provider_id(
                self.provider_id,
                provider_registry=registry,
                entry_point="routing-decision provider",
            ),
        )


@dataclass
class RoutedImplementation(Generic[ImplementationT_co]):
    """Implementation plus the routing decision that selected it."""

    implementation: ImplementationT_co
    routing_decision: RoutingDecision
    authoritative_registration: AuthoritativeCapabilityRegistration | None = None


@dataclass(frozen=True)
class AuthoritativeCapabilityRegistration:
    """Immutable adapter/runtime identity required by annual provider ports."""

    adapter_implementation_ref: VersionedArtifactRef
    adapter_implementation_hash: str
    adapter_contract_version: Literal[1]
    adapter_runtime_identity_ref: VersionedArtifactRef
    adapter_runtime_identity_hash: str
    registration_digest: str

    def __post_init__(self) -> None:
        for ref, companion_hash, field_name in (
            (
                self.adapter_implementation_ref,
                self.adapter_implementation_hash,
                "adapter_implementation_hash",
            ),
            (
                self.adapter_runtime_identity_ref,
                self.adapter_runtime_identity_hash,
                "adapter_runtime_identity_hash",
            ),
        ):
            validate_sha256_hex(companion_hash, field_name=field_name)
            if ref.content_sha256 != companion_hash:
                raise ValueError(f"{field_name} must equal ref content_sha256")
        validate_sha256_hex(
            self.registration_digest,
            field_name="registration_digest",
        )


def build_authoritative_capability_registration(
    *,
    provider_id: ProviderId,
    capability: ProviderCapability,
    provider_registry_revision: str,
    adapter_implementation_ref: VersionedArtifactRef,
    adapter_runtime_identity_ref: VersionedArtifactRef,
) -> AuthoritativeCapabilityRegistration:
    """Construct the sole frozen registration projection used by annual ports."""

    normalized_provider_id = normalize_provider_id(provider_id)
    registration_digest = domain_sha256(
        "provider-capability-registration.v1",
        {
            "provider_id": normalized_provider_id,
            "provider_capability": capability.value,
            "provider_registry_revision": provider_registry_revision,
            "adapter_implementation_ref": adapter_implementation_ref,
            "adapter_implementation_hash": (
                adapter_implementation_ref.content_sha256
            ),
            "adapter_contract_version": 1,
            "adapter_runtime_identity_ref": adapter_runtime_identity_ref,
            "adapter_runtime_identity_hash": (
                adapter_runtime_identity_ref.content_sha256
            ),
        },
    )
    return AuthoritativeCapabilityRegistration(
        adapter_implementation_ref=adapter_implementation_ref,
        adapter_implementation_hash=adapter_implementation_ref.content_sha256,
        adapter_contract_version=1,
        adapter_runtime_identity_ref=adapter_runtime_identity_ref,
        adapter_runtime_identity_hash=adapter_runtime_identity_ref.content_sha256,
        registration_digest=registration_digest,
    )


class ProviderRegistry:
    """Brokerage-pattern registry: provider_name → {capability → implementation}."""

    def __init__(
        self,
        *,
        default_providers: Mapping[ProviderCapability, ProviderId] | None = None,
        registry_revision: str = "provider-registry.runtime.v1",
    ) -> None:
        normalized_revision = str(registry_revision).strip()
        if not normalized_revision:
            raise ValueError("provider registry revision must be non-empty")
        self._registry_revision = normalized_revision
        self._implementations: dict[
            ProviderId,
            dict[str, CapabilityImplementation],
        ] = {}
        self._authoritative_registrations: dict[
            tuple[ProviderId, str],
            AuthoritativeCapabilityRegistration,
        ] = {}
        self._default_providers: dict[str, ProviderId] = {
            capability.value: normalize_provider_id(provider_id)
            for capability, provider_id in (default_providers or {}).items()
        }

    def register(
        self,
        provider_name: ProviderId,
        capability: ProviderCapability,
        implementation: CapabilityImplementation,
        *,
        authoritative_registration: AuthoritativeCapabilityRegistration | None = None,
    ) -> None:
        provider_id = normalize_provider_id(provider_name)
        registration_key = (provider_id, capability.value)
        authority_required = capability in _AUTHORITATIVE_ANNUAL_CAPABILITIES and (
            (
                capability is ProviderCapability.STATEMENT_FACTS
                and isinstance(implementation, StatementEvidenceQueryPort)
            )
            or capability
            not in {
                ProviderCapability.STATEMENT_FACTS,
                ProviderCapability.OPERATING_OBSERVATIONS,
            }
        )
        if (
            authority_required
            and authoritative_registration is None
        ):
            raise ValueError(
                f"{capability.value} registration requires immutable "
                "adapter/runtime identity"
            )
        bucket = self._implementations.setdefault(provider_id, {})
        if capability.value in bucket and (
            authority_required or registration_key in self._authoritative_registrations
        ):
            raise ValueError(f"{capability.value} registration is immutable")
        bucket[capability.value] = implementation
        if authoritative_registration is None:
            self._authoritative_registrations.pop(registration_key, None)
        else:
            self._authoritative_registrations[registration_key] = (
                authoritative_registration
            )

    @property
    def registry_revision(self) -> str:
        return self._registry_revision

    def set_default_provider(
        self,
        capability: ProviderCapability,
        provider_name: ProviderId,
    ) -> None:
        self._default_providers[capability.value] = normalize_provider_id(provider_name)


    def advertised(self, provider_name: ProviderId) -> frozenset[ProviderCapability]:
        provider_id = self.require_registered(provider_name)
        return frozenset(
            ProviderCapability(capability)
            for capability in self._implementations.get(provider_id, {})
        )

    def providers(self) -> frozenset[ProviderId]:
        """Return every provider with a registered capability implementation."""

        return frozenset(self._implementations)

    def known_provider_ids(self) -> frozenset[ProviderId]:
        return (
            self.providers()
            | frozenset(self._default_providers.values())
            | LEGACY_PROVIDER_IDS
        )

    def require_registered(
        self,
        provider_name: object,
        *,
        entry_point: str = "provider ID",
    ) -> ProviderId:
        return require_registered_provider_id(
            provider_name,
            provider_registry=self,
            entry_point=entry_point,
        )


    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.STATEMENT_FACTS],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[StatementFactsPort | StatementEvidenceQueryPort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.OPERATING_OBSERVATIONS],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[KpiObservationsPort]: ...


    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.NON_GAAP_BRIDGES],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[NonGaapBridgesPort]: ...
    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.VALUATION_COMPS],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[ValuationCompsEvidenceQueryPort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.ISSUER_UNIVERSE],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[IssuerUniversePort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.COMPANY_PROFILE],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[CompanyProfilePort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.QUOTE],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[QuotePort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.ESTIMATES],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[EstimatesPort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.METRIC_SERIES],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[MetricSeriesPort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.PEER_UNIVERSE],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[PeerUniversePort]: ...

    @overload
    def resolve(
        self,
        capability: Literal[ProviderCapability.PRICE_SERIES],
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[PriceSeriesPort]: ...

    @overload
    def resolve(
        self,
        capability: ProviderCapability,
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[CapabilityImplementation]: ...

    def resolve(
        self,
        capability: ProviderCapability,
        routing_context: RoutingContext | None = None,
    ) -> RoutedImplementation[CapabilityImplementation]:
        context = routing_context or RoutingContext()
        provider_id = (
            self.require_registered(
                context.preferred_provider,
                entry_point="preferred provider",
            )
            if context.preferred_provider is not None
            else self._default_providers.get(capability.value)
        )
        if provider_id is None:
            raise KeyError(f"no provider registered for capability {capability.value}")
        implementations = self._implementations.get(provider_id, {})
        implementation = implementations.get(capability.value)
        if implementation is None:
            raise CapabilityNotAdvertisedError(
                provider=provider_id,
                capability=capability,
            )
        return RoutedImplementation(
            implementation=implementation,
            routing_decision=RoutingDecision(
                provider_id=provider_id,
                capability=capability,
                provider_registry=self,
            ),
            authoritative_registration=self._authoritative_registrations.get(
                (provider_id, capability.value)
            ),
        )
