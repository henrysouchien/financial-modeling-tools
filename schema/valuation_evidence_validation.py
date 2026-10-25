"""Valuation-evidence checks: source bindings, bundle coverage, peer selection.

Engine (B) checks over the Store types of
``schema/build_model_product_contracts.py``.
``validate_valuation_evidence_source_bindings`` is the one source-binding
contract shared by the SourceBundle and the valuation-evidence snapshot.
``validate_source_bundle_valuation_evidence`` and
``validate_source_bundle_binding_and_selection`` run what the Store's
``SourceBundle`` decode and ``validate_source_bundle_binding`` used to reach by
calling Engine procedures; per ruling R10 of
``docs/design/financial-model-container-refactor-plan-2026-10-02.md`` §9 the
Engine callers run them: the bundle producer and the compile path for the
first, every production caller of the binding check for the second.
``valuation_selection_findings`` reports unresolved peer-derived selections
and unbundled evidence citations as declinable warning Findings; neither is a
digest or coverage-profile binding refusal.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import re
from typing import Any, Literal, Mapping, Sequence, TypeGuard

from industry_slice_core.thesis_shared_sources import SourceId, SourceRecord
from value_semantics_core import (
    ForwardEpsEstimate,
    ForwardEvAdjustedEbitdaAvailable,
    ForwardPeAvailable,
    Fy1ForwardPeriodAvailable,
    NtmForwardEvAdjustedEbitdaAvailable,
    derive_forward_pe,
    select_fy1_forward_period,
)

from .build_findings import make_finding
from .build_model_product_contracts import (
    EDGAR_API_COVER_FACTS_RESPONSE_ARTIFACT_KIND,
    EDGAR_API_EV_RESPONSE_ARTIFACT_KINDS,
    EDGAR_API_EV_RESPONSE_PAIR_FIELDS,
    EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND,
    VALUATION_MULTIPLE_QUANTUM,
    FiscalPeriodFacts,
    HistoricalBasisFact,
    ModelSpec,
    NtmQuarterlyAdjustedEbitdaOperand,
    NtmQuarterlyEpsOperand,
    NtmReportedPeriodAnchor,
    SourceBundle,
    ValuationObservation,
    _EDGAR_API_EV_RESPONSE_IDENTITY,
    _EDGAR_API_SCALE_EXPONENTS,
    _EDGAR_API_SCALE_FALLBACK,
    _EDGAR_API_SHARES_OUTSTANDING_FACT_NAME,
    _EDGAR_METRIC_SERIES_ARTIFACT_CODEC,
    _EDGAR_METRIC_SERIES_ARTIFACT_KIND,
    _OfficialCloseBinding,
    _PASS_THROUGH_CASH_TERMS,
    _VALUATION_DEFINITION_SCOPE,
    edgar_api_financials_fact_is_undimensioned,
    validate_source_bundle_binding,
)
from .edgar_metric_series_v1 import (
    expected_ntm_anchor_metric_series_request_params,
    validate_edgar_metric_series_payload,
)
from .finding_vocabulary import Finding
from .fmp_vendor_response_v1 import (
    FmpVendorScalarV1,
    ValidatedFmpVendorResponseV1,
    decode_canonical_json_mapping,
    decode_canonical_json_records,
    validate_fmp_vendor_response_v1,
)
from .provider_port.adapters.edgar import (
    latest_reported_period_accepted_by_cutoff,
    reported_period_candidates_from_validated_payload,
)
from .provider_port.contracts import ReportedPeriod
from .valuation_evidence_derivations import (
    _ForwardEvOperands,
    _has_fmp_provider_lineage,
    derive_observation_forward_ev_adjusted_ebitda,
    derive_relative_selection,
    derive_valuation_coverage,
    is_market_plane_print_admissible,
    market_plane_locator_as_of_date,
)
from .versioned_artifact import canonical_json_bytes


def _is_shares_observation_shape(
    value: tuple[
        Literal["price", "shares", "debt", "cash", "declared_adjustment"] | None,
        date | None,
    ],
) -> TypeGuard[tuple[Literal["shares"], date]]:
    """Narrow the correlated shape enforced by ValuationObservation."""

    return value[0] == "shares" and value[1] is not None


def validate_valuation_evidence_source_bindings(
    *,
    observations: Sequence[ValuationObservation],
    fiscal_calendar: Sequence[FiscalPeriodFacts],
    sources: Sequence[SourceRecord],
    cutoff_at: datetime,
    retrieved_at: datetime,
    historical_basis_facts: Sequence[HistoricalBasisFact] = (),
) -> None:
    """Validate the one source-binding contract shared by bundle and projection."""

    source_by_id = {item.id: item for item in sources}
    if len(source_by_id) != len(sources):
        raise ValueError("valuation evidence source ids must be unique")
    if len({row.fact_id for row in historical_basis_facts}) != len(
        historical_basis_facts
    ):
        raise ValueError("valuation historical-basis fact ids must be unique")

    def bound_artifact_source(
        *,
        source_id: SourceId,
        artifact_sha256: str | None,
        evidence_kind: str,
    ) -> tuple[SourceRecord, datetime, str]:
        source = source_by_id.get(source_id)
        if source is None:
            raise ValueError(f"{evidence_kind} source_id must resolve")
        key_fields = source.key_fields or {}
        if (
            artifact_sha256 is not None
            and key_fields.get("artifact_sha256") != artifact_sha256
        ):
            raise ValueError(
                f"{evidence_kind} source artifact_sha256 must match exactly"
            )
        logical_locator = key_fields.get("logical_locator")
        if not isinstance(logical_locator, str) or not logical_locator.strip():
            raise ValueError(
                f"{evidence_kind} source requires a nonblank logical_locator"
            )
        try:
            source_retrieved_at = datetime.fromisoformat(
                str(source.retrieved_at).replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{evidence_kind} source retrieved_at must be aware UTC"
            ) from exc
        if (
            source_retrieved_at.tzinfo is None
            or source_retrieved_at.utcoffset() != timedelta(0)
        ):
            raise ValueError(f"{evidence_kind} source retrieved_at must be aware UTC")
        return source, source_retrieved_at, logical_locator

    def canonical_json_field(
        source: SourceRecord,
        *,
        field_name: str,
        expected_type: type[dict] | type[list],
        evidence_kind: str,
    ) -> dict[str, object] | list[object]:
        raw_value = (source.key_fields or {}).get(field_name)
        try:
            parsed = (
                decode_canonical_json_mapping(
                    raw_value,
                    field_name=f"{evidence_kind} {field_name}",
                )
                if expected_type is dict
                else decode_canonical_json_records(
                    raw_value,
                    field_name=f"{evidence_kind} {field_name}",
                )
            )
        except ValueError as exc:
            raise ValueError(
                f"{evidence_kind} {field_name} must be canonical JSON"
            ) from exc
        return parsed

    def canonical_utc_timestamp(value: object, *, field_name: str) -> datetime:
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be canonical aware UTC")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be canonical aware UTC") from exc
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError(f"{field_name} must be canonical aware UTC")
        if value not in {parsed.isoformat(), parsed.isoformat().replace("+00:00", "Z")}:
            raise ValueError(f"{field_name} must be canonical aware UTC")
        return parsed

    def sole_provider_instant(
        values: Sequence[object],
        *,
        field_name: str,
    ) -> datetime:
        """Fold every stated provider timestamp into the one instant it names.

        The owning API spells filing acceptance with a fractional second
        (``2026-08-05T14:02:21.000Z``), and the capture retains its bytes
        verbatim because a sha256 pins them. A validator has no business
        policing a spelling it does not own, so the literals are parsed and the
        instants compared. Precedent: ``schema/edgar_metric_series_v1.py``.
        """

        instants: set[datetime] = set()
        for value in values:
            if value is None:
                continue
            if not isinstance(value, str) or not value.endswith("Z"):
                raise ValueError(f"{field_name} must be a canonical UTC datetime")
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(
                    f"{field_name} must be a canonical UTC datetime"
                ) from exc
            if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
                raise ValueError(f"{field_name} must be a canonical UTC datetime")
            instants.add(parsed)
        if len(instants) != 1:
            raise ValueError(f"{field_name} must state exactly one value")
        return next(iter(instants))

    def canonical_iso_date(value: object, *, field_name: str) -> date:
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be a canonical ISO date")
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a canonical ISO date") from exc
        if parsed.isoformat() != value:
            raise ValueError(f"{field_name} must be a canonical ISO date")
        return parsed

    def fmp_response_lineage(
        source: SourceRecord,
        *,
        source_retrieved_at: datetime,
        expected_endpoint: str,
        evidence_kind: str,
    ) -> tuple[ValidatedFmpVendorResponseV1, datetime]:
        key_fields = source.key_fields or {}
        if (
            source.provider != "fmp"
            or source.endpoint_or_filing_id != expected_endpoint
        ):
            raise ValueError(f"{evidence_kind} provider endpoint is invalid")
        retained_response_sha256 = key_fields.get("normalized_response_sha256")
        if retained_response_sha256 is None:
            retained_response_sha256 = key_fields.get("response_sha256")
        try:
            response = validate_fmp_vendor_response_v1(
                endpoint=expected_endpoint,
                validated_params_json=key_fields.get("validated_params"),
                normalized_records_json=key_fields.get("normalized_records"),
                lineage_descriptor_json=key_fields.get("lineage_descriptor"),
                artifact_sha256=key_fields.get("artifact_sha256"),
                retained_response_sha256=retained_response_sha256,
            )
        except ValueError as exc:
            raise ValueError(f"{evidence_kind}: {exc}") from exc
        if any(
            retained is not None and retained != response.response_sha256
            for retained in (
                key_fields.get("normalized_response_sha256"),
                key_fields.get("response_sha256"),
            )
        ):
            raise ValueError(f"{evidence_kind}: retained response_sha256 mismatch")
        observed_at = canonical_utc_timestamp(
            key_fields.get("observed_at"),
            field_name=f"{evidence_kind} observed_at",
        )
        if observed_at > source_retrieved_at:
            raise ValueError(
                f"{evidence_kind} source retrieval cannot predate observation"
            )
        return response, observed_at

    def edgar_metric_series_candidates(
        source: SourceRecord,
        *,
        source_retrieved_at: datetime,
        expected_issuer: str,
        evidence_kind: str,
    ) -> tuple[dict[str, object], tuple[tuple[int, ReportedPeriod], ...]]:
        key_fields = source.key_fields or {}
        if (
            source.provider != "edgar"
            or source.endpoint_or_filing_id != "metric_series"
        ):
            raise ValueError(f"{evidence_kind} provider endpoint is invalid")
        if key_fields.get("artifact_kind") != _EDGAR_METRIC_SERIES_ARTIFACT_KIND:
            raise ValueError(f"{evidence_kind} artifact kind is invalid")
        if key_fields.get("artifact_codec") != _EDGAR_METRIC_SERIES_ARTIFACT_CODEC:
            raise ValueError(f"{evidence_kind} artifact codec is invalid")
        if key_fields.get("contract_version") != "edgar-metric-series.v1":
            raise ValueError(f"{evidence_kind} contract version is invalid")
        if key_fields.get("issuer") != expected_issuer:
            raise ValueError(f"{evidence_kind} issuer is invalid")
        captured_at = canonical_utc_timestamp(
            key_fields.get("captured_at"),
            field_name=f"{evidence_kind} captured_at",
        )
        if captured_at != source_retrieved_at:
            raise ValueError(
                f"{evidence_kind} retrieved_at must equal captured_at custody time"
            )

        raw_params = key_fields.get("validated_params")
        if not isinstance(raw_params, str):
            raise ValueError(f"{evidence_kind} validated_params must be canonical JSON")
        try:
            params_bytes = canonical_json_bytes(raw_params)
            decoded_params = json.loads(params_bytes)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"{evidence_kind} validated_params must be canonical JSON"
            ) from exc
        if not isinstance(decoded_params, dict):
            raise ValueError(f"{evidence_kind} validated_params must be an object")
        params = {str(key): value for key, value in decoded_params.items()}

        try:
            payload_bytes = canonical_json_bytes(source.text)
            decoded_payload = json.loads(payload_bytes)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"{evidence_kind} payload must be canonical JSON") from exc
        if not isinstance(decoded_payload, dict):
            raise ValueError(f"{evidence_kind} payload must be an object")
        artifact_sha256 = key_fields.get("artifact_sha256")
        if (
            not isinstance(artifact_sha256, str)
            or hashlib.sha256(payload_bytes + b"\n").hexdigest() != artifact_sha256
        ):
            raise ValueError(
                f"{evidence_kind} artifact hash must bind canonical JSON plus one LF"
            )
        try:
            validated = validate_edgar_metric_series_payload(
                decoded_payload,
                expected_ticker=expected_issuer,
                expected_metric_name=str(params.get("metric_name") or ""),
                expected_request_params=params,
            )
            candidates = reported_period_candidates_from_validated_payload(validated)
        except ValueError as exc:
            raise ValueError(f"{evidence_kind}: {exc}") from exc
        for _row_index, candidate in candidates:
            if (
                candidate.source_available_on is not None
                and candidate.source_available_on > source_retrieved_at.date()
            ):
                raise ValueError(
                    f"{evidence_kind} retrieval cannot predate source availability"
                )
            if (
                candidate.source_accepted_at is not None
                and candidate.source_accepted_at > source_retrieved_at
            ):
                raise ValueError(
                    f"{evidence_kind} retrieval cannot predate source acceptance"
                )
        return params, candidates

    def sole_response_string(
        values: Sequence[object],
        *,
        field_name: str,
    ) -> str:
        stated = {value for value in values if value is not None}
        if len(stated) != 1:
            raise ValueError(f"{field_name} must state exactly one value")
        only = next(iter(stated))
        if not isinstance(only, str) or not only.strip():
            raise ValueError(f"{field_name} must state exactly one value")
        return only

    def edgar_api_ev_response(
        source: SourceRecord,
        *,
        source_retrieved_at: datetime,
        expected_kind: str,
        expected_issuer: str,
        evidence_kind: str,
    ) -> tuple[Mapping[str, object], tuple[Mapping[str, object], ...]]:
        """Validate one captured edgar API response and return its facts."""

        key_fields = source.key_fields or {}
        endpoint, url_or_endpoint, own_pair_field = _EDGAR_API_EV_RESPONSE_IDENTITY[
            expected_kind
        ]
        if (
            source.provider != "edgar"
            or source.endpoint_or_filing_id != endpoint
            or key_fields.get("artifact_kind") != expected_kind
            or key_fields.get("url_or_endpoint") != url_or_endpoint
            or key_fields.get("issuer") != expected_issuer
            or key_fields.get(own_pair_field) != source.id
        ):
            raise ValueError(f"{evidence_kind} capture identity is invalid")
        response = canonical_json_field(
            source,
            field_name="response",
            expected_type=dict,
            evidence_kind=evidence_kind,
        )
        response_text = key_fields.get("response")
        if (
            not isinstance(response_text, str)
            or not isinstance(response, dict)
            or key_fields.get("response_sha256")
            != hashlib.sha256(response_text.encode("utf-8")).hexdigest()
        ):
            raise ValueError(
                f"{evidence_kind} response_sha256 must bind the captured bytes"
            )
        params = canonical_json_field(
            source,
            field_name="validated_params",
            expected_type=dict,
            evidence_kind=evidence_kind,
        )
        if not isinstance(params, dict) or params.get("ticker") != expected_issuer:
            raise ValueError(f"{evidence_kind} request must bind exactly its issuer")
        if (
            expected_kind == EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND
            and params.get("full_year_mode") != "true"
        ):
            raise ValueError(f"{evidence_kind} request must bind the annual filing")
        canonical_json_field(
            source,
            field_name="lineage_descriptor",
            expected_type=dict,
            evidence_kind=evidence_kind,
        )
        capture_manifest_sha256 = key_fields.get("capture_manifest_sha256")
        if (
            not isinstance(capture_manifest_sha256, str)
            or not capture_manifest_sha256.strip()
        ):
            raise ValueError(f"{evidence_kind} requires its capture manifest digest")
        observed_at = canonical_utc_timestamp(
            key_fields.get("response_observed_at"),
            field_name=f"{evidence_kind} response_observed_at",
        )
        if observed_at != source_retrieved_at:
            raise ValueError(
                f"{evidence_kind} retrieved_at must equal its response observation"
            )
        if observed_at > cutoff_at:
            raise ValueError(f"post-cutoff {evidence_kind} cannot enter dated EV")
        if source_retrieved_at > retrieved_at:
            raise ValueError(f"{evidence_kind} retrieval postdates evidence retrieval")
        raw_facts = response.get("facts")
        if (
            not isinstance(raw_facts, list)
            or not raw_facts
            or any(not isinstance(row, Mapping) for row in raw_facts)
        ):
            raise ValueError(f"{evidence_kind} response must state fact objects")
        return response, tuple(row for row in raw_facts if isinstance(row, Mapping))

    def financials_current_source_refs(
        facts: Sequence[Mapping[str, object]],
        *,
        evidence_kind: str,
    ) -> tuple[Mapping[str, object], ...]:
        current_refs: list[Mapping[str, object]] = []
        for fact in facts:
            source_ref = fact.get("source_ref")
            if not isinstance(source_ref, Mapping):
                raise ValueError(f"{evidence_kind} fact requires an XBRL source_ref")
            current = source_ref.get("current")
            if current is None:
                continue
            if not isinstance(current, Mapping):
                raise ValueError(f"{evidence_kind} fact current source_ref is invalid")
            current_refs.append(current)
        if not current_refs:
            raise ValueError(f"{evidence_kind} response states no current filing facts")
        return tuple(current_refs)

    def edgar_api_ev_filing_identity(
        source: SourceRecord,
        *,
        expected_issuer: str,
    ) -> datetime:
        """Bind the one filing both captures describe, and its acceptance time."""

        key_fields = source.key_fields or {}
        pair_ids: list[str] = []
        for field_name in EDGAR_API_EV_RESPONSE_PAIR_FIELDS:
            pair_id = key_fields.get(field_name)
            if not isinstance(pair_id, str) or not pair_id:
                raise ValueError(
                    "valuation EV response capture must name both response captures"
                )
            pair_ids.append(pair_id)
        if len(set(pair_ids)) != len(pair_ids):
            raise ValueError("valuation EV response captures must be distinct")
        accessions: list[str] = []
        accepted_at: datetime | None = None
        for pair_id, expected_kind in zip(
            pair_ids,
            EDGAR_API_EV_RESPONSE_ARTIFACT_KINDS,
            strict=True,
        ):
            evidence_kind = f"valuation EV {expected_kind}"
            pair_source, pair_retrieved_at, _pair_locator = bound_artifact_source(
                source_id=pair_id,
                artifact_sha256=None,
                evidence_kind=evidence_kind,
            )
            response, facts = edgar_api_ev_response(
                pair_source,
                source_retrieved_at=pair_retrieved_at,
                expected_kind=expected_kind,
                expected_issuer=expected_issuer,
                evidence_kind=evidence_kind,
            )
            if expected_kind == EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND:
                metadata = response.get("metadata")
                metadata_source = (
                    metadata.get("source") if isinstance(metadata, Mapping) else None
                )
                if not isinstance(metadata_source, Mapping):
                    raise ValueError(f"{evidence_kind} requires filing metadata")
                current_refs = financials_current_source_refs(
                    facts,
                    evidence_kind=evidence_kind,
                )
                accessions.append(
                    sole_response_string(
                        (
                            metadata_source.get("accession"),
                            *(ref.get("accession") for ref in current_refs),
                        ),
                        field_name=f"{evidence_kind} accession",
                    )
                )
                accepted_at = sole_provider_instant(
                    tuple(ref.get("accepted_at") for ref in current_refs),
                    field_name=f"{evidence_kind} accepted_at",
                )
            else:
                filing = response.get("filing")
                if not isinstance(filing, Mapping):
                    raise ValueError(f"{evidence_kind} requires filing metadata")
                accessions.append(
                    sole_response_string(
                        (
                            filing.get("accession"),
                            *(fact.get("accession") for fact in facts),
                        ),
                        field_name=f"{evidence_kind} accession",
                    )
                )
            if accepted_at is not None and accepted_at > pair_retrieved_at:
                raise ValueError(
                    "valuation EV response capture cannot predate filing acceptance"
                )
        if accepted_at is None or len(set(accessions)) != 1:
            raise ValueError(
                "valuation EV response captures must describe one exact filing"
            )
        if accepted_at > cutoff_at:
            raise ValueError("post-cutoff filing cannot enter dated EV")
        return accepted_at

    def edgar_api_fact_index(
        ref: str,
        *,
        logical_locator: str,
        value_field: str,
        fact_count: int,
        evidence_kind: str,
    ) -> int:
        result_key = result_key_for_ref(
            ref,
            logical_locator=logical_locator,
            evidence_kind=evidence_kind,
        )
        match = re.fullmatch(
            rf"response\.facts\[(0|[1-9][0-9]*)\]\.{re.escape(value_field)}",
            result_key,
        )
        if match is None or int(match.group(1)) >= fact_count:
            raise ValueError(
                f"{evidence_kind} ref must address one captured response fact"
            )
        return int(match.group(1))

    def financials_fact_native_value(
        fact: Mapping[str, object],
        *,
        evidence_kind: str,
    ) -> Decimal:
        """Recover native units from one ``/api/financials`` fact.

        The endpoint states the displayed number plus a scale label; the label is
        the power-of-ten multiplier. An unrecognised label fails closed rather
        than defaulting to one.
        """

        stated = fact.get("current_period_value")
        if isinstance(stated, bool) or not isinstance(stated, (int, float)):
            raise ValueError(f"{evidence_kind} fact must state a numeric value")
        label = fact.get("scale")
        if not isinstance(label, str):
            raise ValueError(f"{evidence_kind} fact must state its scale")
        exponent = _EDGAR_API_SCALE_EXPONENTS.get(label)
        if exponent is None:
            fallback = _EDGAR_API_SCALE_FALLBACK.fullmatch(label)
            if fallback is None:
                raise ValueError(f"{evidence_kind} fact scale is unrecognised: {label}")
            exponent = int(fallback.group(1))
        return Decimal(str(stated)) * Decimal(10) ** exponent

    def cover_facts_native_value(
        fact: Mapping[str, object],
        *,
        evidence_kind: str,
    ) -> Decimal:
        """Read native units from one ``/api/filing/cover-facts`` fact.

        The endpoint applies the inline ``scale`` when it parses the tag, so the
        stated value is already native and must never be scaled again.
        """

        stated = fact.get("value")
        if isinstance(stated, bool) or not isinstance(stated, (int, float)):
            raise ValueError(f"{evidence_kind} fact must state a numeric value")
        scale = fact.get("scale")
        if scale is not None and (isinstance(scale, bool) or not isinstance(scale, int)):
            raise ValueError(f"{evidence_kind} fact scale is unrecognised: {scale!r}")
        return Decimal(str(stated))

    def undimensioned_financials_fact(
        fact: Mapping[str, object],
        *,
        evidence_kind: str,
    ) -> str:
        """Return one balance-sheet fact's qname, rejecting a dimensioned one."""

        qname = fact.get("tag")
        if not isinstance(qname, str) or not qname.strip():
            raise ValueError(f"{evidence_kind} fact must state its concept qname")
        if not edgar_api_financials_fact_is_undimensioned(fact):
            raise ValueError(
                f"{evidence_kind} must bind the undimensioned fact: {qname}"
            )
        return qname

    def financials_ev_fact(
        ref: str,
        *,
        facts: Sequence[Mapping[str, object]],
        logical_locator: str,
        evidence_kind: str,
    ) -> tuple[tuple[str, ...], Decimal, date]:
        """Resolve the balance-sheet concept a component ref names, if any.

        A ref to the response's fact list itself binds a role the filing
        states none of its selected concepts for (an issuer with no long-term
        debt): zero, as of the response's one current report date, bound to
        no concept; ``require_assumed_zero_absent`` checks the observation's
        assumed-zero concepts against these facts.
        """

        if ref == f"{logical_locator}#response.facts":
            return (
                (),
                Decimal(0),
                canonical_iso_date(
                    sole_response_string(
                        tuple(
                            current.get("report_date")
                            for current in financials_current_source_refs(
                                facts, evidence_kind=evidence_kind
                            )
                        ),
                        field_name=f"{evidence_kind} report_date",
                    ),
                    field_name=f"{evidence_kind} report_date",
                ),
            )
        fact = facts[
            edgar_api_fact_index(
                ref,
                logical_locator=logical_locator,
                value_field="current_period_value",
                fact_count=len(facts),
                evidence_kind=evidence_kind,
            )
        ]
        current_ref = financials_current_source_refs(
            (fact,),
            evidence_kind=evidence_kind,
        )[0]
        return (
            (undimensioned_financials_fact(fact, evidence_kind=evidence_kind),),
            financials_fact_native_value(fact, evidence_kind=evidence_kind),
            canonical_iso_date(
                current_ref.get("report_date"),
                field_name=f"{evidence_kind} report_date",
            ),
        )

    def require_own_funds_concept(qname: str, *, evidence_kind: str) -> None:
        """Reject cash bound through a concept that holds other people's money."""

        local_name = qname.split(":", 1)[-1].lower()
        if any(
            all(term in local_name for term in terms)
            for terms in _PASS_THROUGH_CASH_TERMS
        ):
            raise ValueError(
                f"{evidence_kind} must not bind a pass-through balance: {qname}"
            )

    def require_assumed_zero_absent(
        observation: ValuationObservation,
        *,
        facts: Sequence[Mapping[str, object]],
        qname_field: str,
        value_field: str,
        evidence_kind: str,
    ) -> None:
        """Require every assumed-zero concept to be absent or stated as zero."""

        for qname in observation.assumed_zero_qnames:
            for fact in facts:
                if fact.get(qname_field) != qname:
                    continue
                if qname_field == "tag" and not (
                    edgar_api_financials_fact_is_undimensioned(fact)
                ):
                    continue
                stated = fact.get(value_field)
                if (
                    isinstance(stated, bool)
                    or not isinstance(stated, (int, float))
                    or stated != 0
                ):
                    raise ValueError(
                        f"{evidence_kind} assumed-zero concept is stated by the "
                        f"captured response: {qname}"
                    )

    def result_key_for_ref(
        ref: str,
        *,
        logical_locator: str,
        evidence_kind: str,
    ) -> str:
        prefix = f"{logical_locator}#"
        if not ref.startswith(prefix):
            raise ValueError(f"{evidence_kind} ref must resolve within its source")
        result_key = ref.removeprefix(prefix)
        if not result_key:
            raise ValueError(f"{evidence_kind} ref must identify a scalar")
        return result_key

    def bind_profile_currency(
        *,
        observation: ValuationObservation,
        evidence_kind: str,
    ) -> datetime | None:
        """Bind one issuer's reporting currency to its exact FMP profile row."""

        if observation.currency_source_id is None:
            return None
        currency_source, currency_retrieved_at, currency_locator = (
            bound_artifact_source(
                source_id=observation.currency_source_id,
                artifact_sha256=None,
                evidence_kind=evidence_kind,
            )
        )
        currency_response, currency_observed_at = fmp_response_lineage(
            currency_source,
            source_retrieved_at=currency_retrieved_at,
            expected_endpoint="profile",
            evidence_kind=f"{evidence_kind} source",
        )
        currency_records = decode_canonical_json_records(
            (currency_source.key_fields or {}).get("normalized_records"),
            field_name=f"{evidence_kind} normalized_records",
        )
        if (
            dict(currency_response.validated_params) != {"symbol": observation.issuer}
            or len(currency_records) != 1
            or not isinstance(currency_records[0], Mapping)
            or currency_records[0].get("symbol") != observation.issuer
            or currency_records[0].get("currency") != "USD"
            or observation.currency != "USD"
            or observation.currency_ref != f"{currency_locator}#data[0].currency"
            or not is_market_plane_print_admissible(
                locator=currency_locator,
                as_of_close=observation.as_of_close,
                observed_at=currency_observed_at,
                cutoff_at=cutoff_at,
            )
        ):
            raise ValueError(f"{evidence_kind} must bind exact profile row")
        return currency_observed_at

    def scalar_for_ref(
        ref: str,
        *,
        logical_locator: str,
        response: ValidatedFmpVendorResponseV1,
        expected_field: str,
        evidence_kind: str,
    ) -> FmpVendorScalarV1:
        result_key = result_key_for_ref(
            ref,
            logical_locator=logical_locator,
            evidence_kind=evidence_kind,
        )
        try:
            return response.scalar(result_key, expected_field=expected_field)
        except ValueError as exc:
            raise ValueError(f"{evidence_kind}: {exc}") from exc

    def bind_official_close_fields(
        *,
        observation: ValuationObservation,
        close_ref: str | None,
        evidence_kind: str,
    ) -> _OfficialCloseBinding:
        issuer = observation.issuer
        raw_ref_date = market_plane_locator_as_of_date(observation.raw_ref)
        market_date = (
            observation.as_of_close
            if observation.metric in {"fwd_pe", "fwd_ev_adj_ebitda"}
            and market_plane_locator_as_of_date(close_ref) is not None
            and raw_ref_date is not None
            and observation.raw_market_date == min(observation.as_of_close, raw_ref_date)
            else observation.raw_market_date
        )
        if issuer is None or market_date is None or close_ref is None:
            raise ValueError(
                f"{evidence_kind} requires issuer, market date, and close ref"
            )
        return _OfficialCloseBinding(
            issuer=issuer,
            market_date=market_date,
            close_ref=close_ref,
        )


    def bind_fmp_official_close(
        *,
        observation: ValuationObservation,
        source: SourceRecord,
        source_retrieved_at: datetime,
        logical_locator: str,
        close_ref: str,
        evidence_kind: str,
    ) -> tuple[datetime, FmpVendorScalarV1, FmpVendorScalarV1]:
        issuer = observation.issuer
        raw_ref_date = market_plane_locator_as_of_date(observation.raw_ref)
        market_date = (
            observation.as_of_close
            if observation.metric in {"fwd_pe", "fwd_ev_adj_ebitda"}
            and market_plane_locator_as_of_date(close_ref) is not None
            and raw_ref_date is not None
            and observation.raw_market_date == min(observation.as_of_close, raw_ref_date)
            else observation.raw_market_date
        )
        if issuer is None or market_date is None:
            raise ValueError(f"{evidence_kind} requires issuer and market date")
        close_binding = _OfficialCloseBinding(
            issuer=issuer,
            market_date=market_date,
            close_ref=close_ref,
        )
        key_fields = source.key_fields or {}
        if key_fields.get("native_field") != "close":
            raise ValueError(
                f"{evidence_kind} source must be historical_price_eod close"
            )
        response, observed_at = fmp_response_lineage(
            source,
            source_retrieved_at=source_retrieved_at,
            expected_endpoint="historical_price_eod",
            evidence_kind=evidence_kind,
        )
        normalized_records = decode_canonical_json_records(
            key_fields.get("normalized_records"),
            field_name=f"{evidence_kind} normalized_records",
        )
        if len(normalized_records) != 1:
            raise ValueError(f"{evidence_kind} requires exactly one provider row")
        close_date_text = close_binding.market_date.isoformat()
        expected_params = {
            "from": close_date_text,
            "serietype": "line",
            "symbol": close_binding.issuer,
            "to": close_date_text,
        }
        if dict(response.validated_params) != expected_params:
            raise ValueError(
                f"{evidence_kind} request must bind issuer and exact date range"
            )
        if key_fields.get("snapshot_manifest_sha256") != (
            observation.snapshot_manifest_sha256
        ):
            raise ValueError(f"{evidence_kind} must match snapshot manifest")
        if not is_market_plane_print_admissible(
            locator=logical_locator,
            as_of_close=observation.as_of_close,
            observed_at=observed_at,
            cutoff_at=cutoff_at,
        ):
            raise ValueError(
                f"post-cutoff close observation cannot enter {evidence_kind}"
            )
        close_scalar = scalar_for_ref(
            close_binding.close_ref,
            logical_locator=logical_locator,
            response=response,
            expected_field="close",
            evidence_kind=evidence_kind,
        )
        date_ref = f"{logical_locator}#data[{close_scalar.row_index}].date"
        date_scalar = scalar_for_ref(
            date_ref,
            logical_locator=logical_locator,
            response=response,
            expected_field="date",
            evidence_kind=f"{evidence_kind} date",
        )
        if (
            close_scalar.row_index != date_scalar.row_index
            or close_scalar.row_index != 0
            or close_scalar.as_decimal() != observation.raw_close
            or date_scalar.as_date() != close_binding.market_date
            or close_scalar.period_as_date() != close_binding.market_date
            or date_scalar.period is not None
        ):
            raise ValueError(f"{evidence_kind} close and date must bind one exact row")
        return observed_at, close_scalar, date_scalar

    def bind_reported_period_anchor(
        *,
        observation: ValuationObservation,
        anchor: NtmReportedPeriodAnchor,
        evidence_kind: str,
    ) -> str:
        """Bind one retained anchor to the exact controlled EDGAR source pair."""

        selection_source_ids = anchor.selection_source_ids
        if selection_source_ids is None or len(selection_source_ids) != 2:
            raise ValueError(
                f"{evidence_kind} requires annual and quarterly raw sources"
            )
        anchor_rows: list[tuple[SourceRecord, str, int, ReportedPeriod]] = []
        source_ids_by_full_year_mode: dict[bool, SourceId] = {}
        params_by_full_year_mode: dict[bool, dict[str, object]] = {}
        for selection_source_id in selection_source_ids:
            selection_source, selection_retrieved_at, selection_locator = (
                bound_artifact_source(
                    source_id=selection_source_id,
                    artifact_sha256=None,
                    evidence_kind=f"{evidence_kind} raw source",
                )
            )
            if selection_retrieved_at > retrieved_at:
                raise ValueError(
                    f"{evidence_kind} source retrieval cannot postdate evidence retrieval"
                )
            selection_params, selection_candidates = edgar_metric_series_candidates(
                selection_source,
                source_retrieved_at=selection_retrieved_at,
                expected_issuer=observation.issuer or "",
                evidence_kind=f"{evidence_kind} raw source",
            )
            if (selection_source.key_fields or {}).get(
                "snapshot_manifest_sha256"
            ) != observation.snapshot_manifest_sha256:
                raise ValueError(f"{evidence_kind} source must match snapshot manifest")
            full_year_mode = selection_params.get("full_year_mode") == "true"
            if full_year_mode in source_ids_by_full_year_mode:
                raise ValueError(
                    f"{evidence_kind} requires exactly one annual and one quarterly source"
                )
            source_ids_by_full_year_mode[full_year_mode] = selection_source_id
            params_by_full_year_mode[full_year_mode] = selection_params
            anchor_rows.extend(
                (selection_source, selection_locator, row_index, candidate)
                for row_index, candidate in selection_candidates
            )
        if set(source_ids_by_full_year_mode) != {False, True}:
            raise ValueError(
                f"{evidence_kind} requires exactly one annual and one quarterly source"
            )
        if observation.raw_market_date is None:
            raise ValueError(f"{evidence_kind} requires an exact market date")
        expected_annual_params = expected_ntm_anchor_metric_series_request_params(
            observation.issuer or "",
            as_of_close=observation.raw_market_date,
            full_year_mode=True,
        )
        expected_quarterly_params = expected_ntm_anchor_metric_series_request_params(
            observation.issuer or "",
            as_of_close=observation.raw_market_date,
            full_year_mode=False,
        )
        if (
            params_by_full_year_mode[True] != expected_annual_params
            or params_by_full_year_mode[False] != expected_quarterly_params
        ):
            raise ValueError(
                f"{evidence_kind} sources must match the controlled annual and "
                "quarterly acquisition windows"
            )
        try:
            selected_candidate = latest_reported_period_accepted_by_cutoff(
                tuple(row[3] for row in anchor_rows),
                cutoff_at=cutoff_at,
            )
        except ValueError as exc:
            raise ValueError(f"{evidence_kind} selection failed: {exc}") from exc
        if selected_candidate is None:
            raise ValueError(
                f"{evidence_kind} source has no candidate accepted by cutoff"
            )
        selected_rows = tuple(
            row for row in anchor_rows if row[3] == selected_candidate
        )
        if len(selected_rows) != 1:
            raise ValueError(f"{evidence_kind} latest source row is ambiguous")
        selected_source, selected_locator, selected_row_index, selected_candidate = (
            selected_rows[0]
        )
        expected_row_locator = f"{selected_locator}#series[{selected_row_index}]"
        if (
            anchor.source_id != selected_source.id
            or anchor.period_start != selected_candidate.period_start
            or anchor.period_end != selected_candidate.period_end
            or anchor.period_type != selected_candidate.period_type
            or anchor.source_provider != selected_candidate.source_provider
            or anchor.native_source_id != selected_candidate.native_source_id
            or anchor.source_available_on != selected_candidate.source_available_on
            or anchor.source_accepted_at != selected_candidate.source_accepted_at
            or anchor.source_locator != selected_candidate.source_locator
        ):
            raise ValueError(
                f"{evidence_kind} must equal the unique latest raw source row"
            )
        return f"{expected_row_locator}.period_end"

    for observation in observations:
        observation_source, observation_retrieved_at, observation_locator = (
            bound_artifact_source(
                source_id=observation.source_id,
                artifact_sha256=None,
                evidence_kind="valuation observation",
            )
        )
        if not is_market_plane_print_admissible(
            locator=observation_locator,
            as_of_close=observation.as_of_close,
            observed_at=observation_retrieved_at,
            cutoff_at=retrieved_at,
        ):
            raise ValueError(
                "valuation observation source retrieval cannot postdate evidence retrieval"
            )
        if observation_source.provider != observation.provider:
            raise ValueError("valuation observation provider must match its raw source")
        if (
            observation.raw_ref != observation_locator
            and not observation.raw_ref.startswith(f"{observation_locator}#")
        ):
            raise ValueError(
                "valuation observation raw_ref must resolve within its raw source"
            )

        if observation.raw_close_source_id is not None:
            close_source, close_retrieved_at, close_locator = bound_artifact_source(
                source_id=observation.raw_close_source_id,
                artifact_sha256=None,
                evidence_kind="valuation raw close",
            )
            if not is_market_plane_print_admissible(
                locator=close_locator,
                as_of_close=observation.as_of_close,
                observed_at=close_retrieved_at,
                cutoff_at=retrieved_at,
            ):
                raise ValueError(
                    "valuation raw close source retrieval cannot postdate evidence retrieval"
                )
            if close_source.provider != observation.provider:
                raise ValueError("valuation raw close provider must match observation")
            if observation.raw_close_ref is None or (
                observation.raw_close_ref != close_locator
                and not observation.raw_close_ref.startswith(f"{close_locator}#")
            ):
                raise ValueError(
                    "valuation raw close ref must resolve within its source"
                )

        ntm_operands = observation.ntm_operands
        if (
            ntm_operands is None
            and observation.metric == "fwd_pe"
            and observation.definition_evidence is not None
            and not _has_fmp_provider_lineage(observation_source)
        ):
            raise ValueError(
                "definition-bearing annual valuation requires provider-lineaged inputs"
            )
        if (
            ntm_operands is None
            and observation.metric == "fwd_pe"
            and _has_fmp_provider_lineage(observation_source)
        ):
            estimate_key_fields = observation_source.key_fields or {}
            if estimate_key_fields.get("native_field") != "epsAvg":
                raise ValueError(
                    "valuation annual estimate source must identify epsAvg"
                )
            estimate_response, estimate_observed_at = fmp_response_lineage(
                observation_source,
                source_retrieved_at=observation_retrieved_at,
                expected_endpoint="analyst_estimates",
                evidence_kind="valuation annual estimate source",
            )
            expected_estimate_params = {
                "limit": 10,
                "period": "annual",
                "symbol": observation.issuer,
            }
            if dict(estimate_response.validated_params) != expected_estimate_params:
                raise ValueError(
                    "valuation annual estimate request must bind exact provider params"
                )
            if estimate_key_fields.get("snapshot_manifest_sha256") != (
                observation.snapshot_manifest_sha256
            ):
                raise ValueError(
                    "valuation annual estimate must match snapshot manifest"
                )
            if not is_market_plane_print_admissible(
                locator=observation_locator,
                as_of_close=observation.as_of_close,
                observed_at=estimate_observed_at,
                cutoff_at=cutoff_at,
            ):
                raise ValueError(
                    "post-cutoff estimate observation cannot enter annual valuation"
                )
            estimate_period = observation.estimate_period_evidence
            if estimate_period is None or (
                estimate_period.mapping_rule != "provider_annual_period_end_exact_match"
            ):
                raise ValueError(
                    "valuation annual estimate requires exact period-end mapping"
                )
            eps_scalar = scalar_for_ref(
                observation.raw_ref,
                logical_locator=observation_locator,
                response=estimate_response,
                expected_field="epsAvg",
                evidence_kind="valuation annual EPS",
            )
            estimate_date_ref = (
                f"{observation_locator}#data[{eps_scalar.row_index}].date"
            )
            estimate_date_scalar = scalar_for_ref(
                estimate_date_ref,
                logical_locator=observation_locator,
                response=estimate_response,
                expected_field="date",
                evidence_kind="valuation annual estimate period",
            )
            estimate_period_end = estimate_date_scalar.as_date()
            if (
                eps_scalar.row_index != estimate_date_scalar.row_index
                or eps_scalar.period_as_date() != estimate_period_end
                or estimate_date_scalar.period is not None
                or estimate_period.raw_provider_period
                != estimate_period_end.isoformat()
            ):
                raise ValueError(
                    "valuation annual EPS and period must bind one exact row"
                )
            matching_fiscal_facts = tuple(
                fact
                for fact in fiscal_calendar
                if fact.issuer == observation.issuer
                and fact.basis == observation.fiscal_basis
            )
            if len(matching_fiscal_facts) != 1:
                raise ValueError(
                    "valuation annual estimate requires one matching fiscal fact"
                )
            annual_fiscal_fact = matching_fiscal_facts[0]
            if (
                annual_fiscal_fact.source_id != observation.source_id
                or annual_fiscal_fact.period_type != "annual"
                or annual_fiscal_fact.derivation_rule
                != "adjacent_provider_annual_period_ends"
                or annual_fiscal_fact.available_at != estimate_observed_at
                or annual_fiscal_fact.period_end != estimate_period_end
                or len(annual_fiscal_fact.source_locators) != 2
                or annual_fiscal_fact.source_locators[1] != estimate_date_ref
            ):
                raise ValueError(
                    "valuation annual estimate must bind its exact fiscal fact"
                )
            fiscal_predecessor_scalar = scalar_for_ref(
                annual_fiscal_fact.source_locators[0],
                logical_locator=observation_locator,
                response=estimate_response,
                expected_field="date",
                evidence_kind="valuation annual fiscal predecessor",
            )
            matching_target_dates = tuple(
                scalar
                for scalar in estimate_response.scalars
                if scalar.field == "date"
                and scalar.as_date().year == annual_fiscal_fact.period_end.year
            )
            matching_predecessor_dates = tuple(
                scalar
                for scalar in estimate_response.scalars
                if scalar.field == "date"
                and scalar.as_date().year
                == (annual_fiscal_fact.period_start - timedelta(days=1)).year
            )
            if (
                len(matching_target_dates) != 1
                or len(matching_predecessor_dates) != 1
                or fiscal_predecessor_scalar.row_index == estimate_date_scalar.row_index
                or fiscal_predecessor_scalar.period is not None
                or fiscal_predecessor_scalar.as_date()
                != annual_fiscal_fact.period_start - timedelta(days=1)
            ):
                raise ValueError(
                    "valuation annual fiscal predecessor must bind exact provider date"
                )
            if observation.raw_close_source_id is None:
                raise ValueError(
                    "valuation annual forward P/E requires an official close source"
                )
            annual_close_source, annual_close_retrieved_at, annual_close_locator = (
                bound_artifact_source(
                    source_id=observation.raw_close_source_id,
                    artifact_sha256=None,
                    evidence_kind="valuation annual official close",
                )
            )
            if not _has_fmp_provider_lineage(annual_close_source):
                raise ValueError(
                    "valuation annual forward P/E requires provider-lineaged close"
                )
            assert observation.raw_close_ref is not None
            close_observed_at, _, _ = bind_fmp_official_close(
                observation=observation,
                source=annual_close_source,
                source_retrieved_at=annual_close_retrieved_at,
                logical_locator=annual_close_locator,
                close_ref=observation.raw_close_ref,
                evidence_kind="valuation annual official close",
            )
            if observation.retrieved_at != max(estimate_observed_at, close_observed_at):
                raise ValueError(
                    "valuation annual observation time must equal latest provider input"
                )
            result = derive_forward_pe(
                price=observation.raw_close,
                estimate=ForwardEpsEstimate(
                    period_end=estimate_period_end,
                    eps=eps_scalar.as_decimal(),
                    analyst_count=None,
                ),
                quantum=VALUATION_MULTIPLE_QUANTUM,
            )
            if (
                not isinstance(result, ForwardPeAvailable)
                or result.multiple != observation.value
            ):
                raise ValueError(
                    "valuation annual forward P/E must equal shared recomputation"
                )

        ev_operands = observation.forward_adjusted_ebitda_operands
        ntm_ev_operands = observation.ntm_forward_adjusted_ebitda_operands
        forward_ev_operands: _ForwardEvOperands | None = (
            ev_operands if ev_operands is not None else ntm_ev_operands
        )
        if forward_ev_operands is not None:
            if observation.issuer is None:
                raise ValueError("forward EV/adjusted EBITDA requires an issuer")
            estimate_fields = observation_source.key_fields or {}
            estimate_response, estimate_observed_at = fmp_response_lineage(
                observation_source,
                source_retrieved_at=observation_retrieved_at,
                expected_endpoint="analyst_estimates",
                evidence_kind="forward adjusted EBITDA estimate source",
            )
            if estimate_fields.get("snapshot_manifest_sha256") != (
                observation.snapshot_manifest_sha256
            ):
                raise ValueError(
                    "forward adjusted EBITDA source must match snapshot manifest"
                )
            if not is_market_plane_print_admissible(
                locator=observation_locator,
                as_of_close=observation.as_of_close,
                observed_at=estimate_observed_at,
                cutoff_at=cutoff_at,
            ):
                raise ValueError(
                    "post-cutoff estimate cannot enter forward adjusted EBITDA"
                )
            anchor = forward_ev_operands.reported_anchor
            selected_anchor_period_end_ref = bind_reported_period_anchor(
                observation=observation,
                anchor=anchor,
                evidence_kind="forward adjusted EBITDA reported anchor",
            )
            if ev_operands is not None:
                if dict(estimate_response.validated_params) != {
                    "limit": 10,
                    "period": "annual",
                    "symbol": observation.issuer,
                }:
                    raise ValueError(
                        "forward adjusted EBITDA request must bind exact annual params"
                    )
                date_scalars = tuple(
                    scalar for scalar in estimate_response.scalars if scalar.field == "date"
                )
                period_selection = select_fy1_forward_period(
                    period_ends=tuple(scalar.as_date() for scalar in date_scalars),
                    last_reported_period_end=anchor.period_end,
                )
                if not isinstance(period_selection, Fy1ForwardPeriodAvailable):
                    raise ValueError(
                        "forward adjusted EBITDA requires one earliest future annual date: "
                        f"{period_selection.reason}"
                    )
                earliest_date = period_selection.period_end
                selected_date_scalar = next(
                    scalar for scalar in date_scalars if scalar.as_date() == earliest_date
                )
                period_scalar = scalar_for_ref(
                    ev_operands.estimate_period_end_ref,
                    logical_locator=observation_locator,
                    response=estimate_response,
                    expected_field="date",
                    evidence_kind="forward adjusted EBITDA estimate period",
                )
                denominator_scalar = scalar_for_ref(
                    observation.raw_ref,
                    logical_locator=observation_locator,
                    response=estimate_response,
                    expected_field="ebitdaAvg",
                    evidence_kind="forward adjusted EBITDA denominator",
                )
                estimate_period = observation.estimate_period_evidence
                if (
                    period_scalar != selected_date_scalar
                    or denominator_scalar.row_index != period_scalar.row_index
                    or period_scalar.period is not None
                    or denominator_scalar.period_as_date() != earliest_date
                    or Decimal(denominator_scalar.canonical_value) / Decimal("1000000")
                    != ev_operands.adjusted_ebitda
                    or estimate_period is None
                    or estimate_period.mapping_rule
                    != "provider_annual_period_end_exact_match"
                    or estimate_period.raw_provider_period != earliest_date.isoformat()
                ):
                    raise ValueError(
                        "forward adjusted EBITDA must bind the earliest exact annual row"
                    )
                matching_fiscal_facts = tuple(
                    fact
                    for fact in fiscal_calendar
                    if fact.issuer == observation.issuer
                    and fact.basis == observation.fiscal_basis
                )
                if len(matching_fiscal_facts) != 1:
                    raise ValueError(
                        "forward adjusted EBITDA requires one matching fiscal fact"
                    )
                fiscal_fact = matching_fiscal_facts[0]
                if (
                    fiscal_fact.source_id != observation.source_id
                    or fiscal_fact.period_type != "annual"
                    or fiscal_fact.derivation_rule != "adjacent_provider_annual_period_ends"
                    or fiscal_fact.available_at != estimate_observed_at
                    or fiscal_fact.period_end != earliest_date
                    or len(fiscal_fact.source_locators) != 2
                    or fiscal_fact.source_locators[1] != ev_operands.estimate_period_end_ref
                ):
                    raise ValueError(
                        "forward adjusted EBITDA must bind its exact fiscal fact"
                    )
            if ntm_ev_operands is not None:
                ntm_validated_params = estimate_response.validated_params
                ntm_validated_limit = ntm_validated_params.get("limit")
                if (
                    ntm_validated_params.get("symbol") != observation.issuer
                    or ntm_validated_params.get("period") != "quarter"
                    or isinstance(ntm_validated_limit, bool)
                    or not isinstance(ntm_validated_limit, int)
                    or not 4 <= ntm_validated_limit <= 100
                ):
                    raise ValueError(
                        "NTM forward adjusted EBITDA estimate source must be "
                        "analyst_estimates with exact symbol, period=quarter, "
                        "and bounded limit"
                    )
                if observation.raw_ref != f"{observation_locator}#response":
                    raise ValueError(
                        "NTM forward adjusted EBITDA must bind its whole estimate response"
                    )
                ebitda_date_scalars = tuple(
                    scalar for scalar in estimate_response.scalars if scalar.field == "date"
                )
                ebitda_date_rows = {scalar.row_index for scalar in ebitda_date_scalars}
                if any(
                    scalar.row_index not in ebitda_date_rows
                    for scalar in estimate_response.scalars
                    if scalar.field == "ebitdaAvg"
                ):
                    raise ValueError(
                        "NTM forward adjusted EBITDA rows require exact date coordinates"
                    )
                ebitda_future_dates: dict[date, list[FmpVendorScalarV1]] = {}
                for scalar in ebitda_date_scalars:
                    scalar_date = scalar.as_date()
                    if scalar_date > anchor.period_end:
                        ebitda_future_dates.setdefault(scalar_date, []).append(scalar)
                ebitda_selected_dates = tuple(sorted(ebitda_future_dates))[:4]
                if len(ebitda_selected_dates) < 4:
                    raise ValueError(
                        "NTM forward adjusted EBITDA estimate source requires four "
                        "future quarter dates"
                    )
                ebitda_selected_date_scalars: list[FmpVendorScalarV1] = []
                for ebitda_selected_date in ebitda_selected_dates:
                    ebitda_date_rows_for_date = ebitda_future_dates[ebitda_selected_date]
                    if len(ebitda_date_rows_for_date) != 1:
                        raise ValueError(
                            "NTM forward adjusted EBITDA selected estimate date "
                            "is ambiguous"
                        )
                    ebitda_selected_date_scalars.append(ebitda_date_rows_for_date[0])
                if tuple(
                    (f"{observation_locator}#{scalar.result_key}", scalar.as_date())
                    for scalar in ebitda_selected_date_scalars
                ) != tuple(
                    (quarter.estimate_period_end_ref, quarter.period_end)
                    for quarter in ntm_ev_operands.quarterly_adjusted_ebitda
                ):
                    raise ValueError(
                        "NTM forward adjusted EBITDA operands must bind the earliest "
                        "four exact provider dates after the selected anchor"
                    )
                previous_ebitda_quarter: NtmQuarterlyAdjustedEbitdaOperand | None = None
                for ebitda_quarter in ntm_ev_operands.quarterly_adjusted_ebitda:
                    ebitda_period_scalar = scalar_for_ref(
                        ebitda_quarter.estimate_period_end_ref,
                        logical_locator=observation_locator,
                        response=estimate_response,
                        expected_field="date",
                        evidence_kind="NTM forward adjusted EBITDA estimate period",
                    )
                    ebitda_scalar = scalar_for_ref(
                        ebitda_quarter.adjusted_ebitda_ref,
                        logical_locator=observation_locator,
                        response=estimate_response,
                        expected_field="ebitdaAvg",
                        evidence_kind="NTM forward adjusted EBITDA denominator",
                    )
                    if (
                        ebitda_period_scalar.row_index != ebitda_scalar.row_index
                        or ebitda_period_scalar.as_date() != ebitda_quarter.period_end
                        or ebitda_period_scalar.period is not None
                        or ebitda_scalar.period_as_date() != ebitda_quarter.period_end
                        or Decimal(ebitda_scalar.canonical_value) / Decimal("1000000")
                        != ebitda_quarter.adjusted_ebitda
                    ):
                        raise ValueError(
                            "NTM forward adjusted EBITDA date and denominator refs "
                            "must bind one exact row"
                        )
                    if ebitda_quarter.analyst_count_ref is not None:
                        ebitda_count_scalar = scalar_for_ref(
                            ebitda_quarter.analyst_count_ref,
                            logical_locator=observation_locator,
                            response=estimate_response,
                            expected_field="numAnalystsEbitda",
                            evidence_kind="NTM forward adjusted EBITDA analyst count",
                        )
                        if (
                            ebitda_count_scalar.row_index != ebitda_period_scalar.row_index
                            or ebitda_count_scalar.as_integer()
                            != ebitda_quarter.analyst_count
                            or ebitda_count_scalar.period_as_date()
                            != ebitda_quarter.period_end
                        ):
                            raise ValueError(
                                "NTM forward adjusted EBITDA analyst count must bind "
                                "the estimate row"
                            )
                    ebitda_fiscal_source, ebitda_fiscal_retrieved_at, _ = (
                        bound_artifact_source(
                            source_id=ebitda_quarter.fiscal_source_id,
                            artifact_sha256=None,
                            evidence_kind=(
                                "NTM forward adjusted EBITDA quarter fiscal evidence"
                            ),
                        )
                    )
                    if ebitda_fiscal_retrieved_at > retrieved_at:
                        raise ValueError(
                            "NTM forward adjusted EBITDA fiscal source retrieval cannot "
                            "postdate evidence retrieval"
                        )
                    if ebitda_fiscal_source.id != observation_source.id:
                        raise ValueError(
                            "adjacent provider quarter ends must use the estimate source"
                        )
                    expected_ebitda_predecessor = (
                        anchor.period_end
                        if previous_ebitda_quarter is None
                        else previous_ebitda_quarter.period_end
                    )
                    expected_ebitda_predecessor_ref = (
                        selected_anchor_period_end_ref
                        if previous_ebitda_quarter is None
                        else previous_ebitda_quarter.fiscal_period_end_ref
                    )
                    if (
                        ebitda_quarter.fiscal_period_end_ref
                        != ebitda_quarter.estimate_period_end_ref
                        or ebitda_quarter.fiscal_predecessor_period_end
                        != expected_ebitda_predecessor
                        or ebitda_quarter.fiscal_predecessor_period_end_ref
                        != expected_ebitda_predecessor_ref
                    ):
                        raise ValueError(
                            "NTM forward adjusted EBITDA fiscal boundaries must bind the "
                            "selected anchor and adjacent provider quarter ends"
                        )
                    previous_ebitda_quarter = ebitda_quarter
            if (
                observation.raw_close_source_id is None
                or observation.raw_close_ref is None
            ):
                raise ValueError(
                    "forward adjusted EBITDA requires an official close source"
                )
            close_source, close_retrieved_at, close_locator = bound_artifact_source(
                source_id=observation.raw_close_source_id,
                artifact_sha256=None,
                evidence_kind="forward adjusted EBITDA official close",
            )
            close_observed_at, _, _ = bind_fmp_official_close(
                observation=observation,
                source=close_source,
                source_retrieved_at=close_retrieved_at,
                logical_locator=close_locator,
                close_ref=observation.raw_close_ref,
                evidence_kind="forward adjusted EBITDA official close",
            )
            forward_currency_observed_at = bind_profile_currency(
                observation=observation,
                evidence_kind="forward adjusted EBITDA currency",
            )
            if forward_currency_observed_at is None:
                raise ValueError(
                    "forward adjusted EBITDA requires a source-bound profile currency"
                )
            component_by_id = {
                row.observation_id: row
                for row in observations
                if row.observation_id
                in forward_ev_operands.enterprise_value_component_ids
            }
            if set(component_by_id) != set(
                forward_ev_operands.enterprise_value_component_ids
            ):
                raise ValueError(
                    "forward adjusted EBITDA EV component ids must resolve"
                )
            price_components = tuple(
                row for row in component_by_id.values() if row.component_type == "price"
            )
            if len(price_components) != 1:
                raise ValueError(
                    "forward adjusted EBITDA requires one exact price component"
                )
            price_component = price_components[0]
            expected_ev_market_date = (
                min(
                    price_component.as_of_close,
                    market_plane_locator_as_of_date(observation_locator)
                    or price_component.as_of_close,
                )
                if ev_operands is not None
                else price_component.as_of_close
            )
            if (
                observation.raw_close != price_component.value
                or observation.raw_market_date != expected_ev_market_date
                or observation.raw_close_source_id != price_component.source_id
                or observation.raw_close_ref != price_component.raw_ref
            ):
                raise ValueError(
                    "forward adjusted EBITDA close must equal its exact price component"
                )
            assert observation.definition_evidence is not None
            definition = observation.definition_evidence
            definition_source, definition_retrieved_at, definition_locator = (
                bound_artifact_source(
                    source_id=definition.provider_doc_source_id,
                    artifact_sha256=definition.provider_doc_artifact_sha256,
                    evidence_kind="forward adjusted EBITDA provider definition",
                )
            )
            definition_fields = definition_source.key_fields or {}
            definition_sha256 = definition.provider_doc_artifact_sha256
            expected_definition_locator = (
                f"provider-doc://fmp/{definition_sha256}/claims/"
                "analyst_estimates/ebitdaAvg"
            )
            source_url = definition_fields.get("source_url")
            if (
                definition_source.type != "other"
                or definition_source.source_id
                != (
                    f"fmp-provider-definition:{definition_sha256}:"
                    "analyst_estimates:ebitdaAvg"
                )
                or definition_source.provider != "fmp"
                or definition_source.endpoint_or_filing_id != "analyst_estimates"
                or definition_source.text != definition.verbatim_definition_text
                or not isinstance(definition_source.section_header, str)
                or not definition_source.section_header.strip()
                or definition_locator != expected_definition_locator
                or not isinstance(source_url, str)
                or not source_url.startswith("https://")
                or definition_fields
                != {
                    "artifact_sha256": definition_sha256,
                    "logical_locator": expected_definition_locator,
                    "provider_endpoint": "analyst_estimates",
                    "provider_field": "ebitdaAvg",
                    "source_kind": "normalized_bounded_official_faq_capture",
                    "source_url": source_url,
                    "provider_basis_scope": "consensus_non_gaap_ebitda",
                }
                or definition_retrieved_at > estimate_observed_at
            ):
                raise ValueError(
                    "forward adjusted EBITDA must bind the exact provider definition"
                )
            expected_retrieved_at = max(
                estimate_observed_at,
                close_observed_at,
                forward_currency_observed_at,
                definition_retrieved_at,
                *(row.retrieved_at for row in component_by_id.values()),
            )
            result = derive_observation_forward_ev_adjusted_ebitda(
                observation,
                observations,
            )
            if (
                not isinstance(
                    result,
                    ForwardEvAdjustedEbitdaAvailable
                    | NtmForwardEvAdjustedEbitdaAvailable,
                )
                or result.multiple != observation.value
                or observation.retrieved_at != expected_retrieved_at
            ):
                raise ValueError(
                    "forward EV/adjusted EBITDA must equal exact source-bound recomputation"
                )
        if observation.metric == "quote" and _has_fmp_provider_lineage(
            observation_source
        ):
            if observation.raw_ref != f"{observation_locator}#data[0].close":
                raise ValueError("valuation focal quote must bind exact data[0].close")
            quote_observed_at, quote_scalar, _ = bind_fmp_official_close(
                observation=observation,
                source=observation_source,
                source_retrieved_at=observation_retrieved_at,
                logical_locator=observation_locator,
                close_ref=observation.raw_ref,
                evidence_kind="valuation focal quote official close",
            )
            if (
                observation.retrieved_at != quote_observed_at
                or quote_scalar.as_decimal() != observation.value
                or observation.raw_market_date != observation.as_of_close
            ):
                raise ValueError(
                    "valuation focal quote must bind exact value, date, and observation time"
                )

        if observation.component_type is not None:
            if observation.issuer is None:
                raise ValueError("dated EV component requires an issuer")
            currency_observed_at = bind_profile_currency(
                observation=observation,
                evidence_kind="valuation EV currency",
            )

            if observation.component_type == "price":
                close_observed_at, close_scalar, _ = bind_fmp_official_close(
                    observation=observation,
                    source=observation_source,
                    source_retrieved_at=observation_retrieved_at,
                    logical_locator=observation_locator,
                    close_ref=observation.raw_ref,
                    evidence_kind="valuation EV official close",
                )
                if (
                    close_scalar.as_decimal() != observation.value
                    or observation.component_as_of != observation.as_of_close
                    or observation.retrieved_at
                    != max(close_observed_at, currency_observed_at or close_observed_at)
                ):
                    raise ValueError(
                        "valuation EV price must bind exact close and time"
                    )
            else:
                binds_cover_facts = observation.component_type == "shares"
                own_kind = (
                    EDGAR_API_COVER_FACTS_RESPONSE_ARTIFACT_KIND
                    if binds_cover_facts
                    else EDGAR_API_FINANCIALS_RESPONSE_ARTIFACT_KIND
                )
                response_evidence_kind = f"valuation EV {own_kind}"
                _response, response_facts = edgar_api_ev_response(
                    observation_source,
                    source_retrieved_at=observation_retrieved_at,
                    expected_kind=own_kind,
                    expected_issuer=observation.issuer,
                    evidence_kind=response_evidence_kind,
                )
                accepted_at = edgar_api_ev_filing_identity(
                    observation_source,
                    expected_issuer=observation.issuer,
                )
                if accepted_at > observation_retrieved_at:
                    raise ValueError(
                        "valuation EV response capture cannot predate filing acceptance"
                    )
                require_assumed_zero_absent(
                    observation,
                    facts=response_facts,
                    qname_field="fact_name" if binds_cover_facts else "tag",
                    value_field="value" if binds_cover_facts else "current_period_value",
                    evidence_kind=response_evidence_kind,
                )

                shares_shape = (
                    observation.component_type,
                    observation.component_as_of,
                )
                if _is_shares_observation_shape(shares_shape):
                    _, component_as_of = shares_shape
                    share_indexes = tuple(
                        index
                        for index, fact in enumerate(response_facts)
                        if fact.get("fact_name")
                        == _EDGAR_API_SHARES_OUTSTANDING_FACT_NAME
                    )
                    if not share_indexes:
                        raise ValueError(
                            "valuation EV shares require reported share facts"
                        )
                    # Every reported occurrence contributes, which covers single-
                    # and multi-class filers with no per-issuer branch. The ref
                    # binds the first, so a tampered ref cannot survive.
                    native_value = sum(
                        (
                            cover_facts_native_value(
                                response_facts[index],
                                evidence_kind=response_evidence_kind,
                            )
                            for index in share_indexes
                        ),
                        Decimal(0),
                    )
                    reported_dates = {
                        canonical_iso_date(
                            response_facts[index].get("as_of_date"),
                            field_name=f"{response_evidence_kind} as_of_date",
                        )
                        for index in share_indexes
                    }
                    if (
                        observation.raw_ref
                        != (
                            f"{observation_locator}#response.facts"
                            f"[{share_indexes[0]}].value"
                        )
                        or len(reported_dates) != 1
                        or component_as_of != next(iter(reported_dates))
                    ):
                        raise ValueError(
                            "valuation EV shares must bind every reported share fact "
                            "at one exact date"
                        )
                    if observation.split_history_source_id is None:
                        raise ValueError(
                            "valuation EV shares require split-history source"
                        )
                    split_source, split_retrieved_at, split_locator = (
                        bound_artifact_source(
                            source_id=observation.split_history_source_id,
                            artifact_sha256=None,
                            evidence_kind="valuation EV split history",
                        )
                    )
                    split_response, split_observed_at = fmp_response_lineage(
                        split_source,
                        source_retrieved_at=split_retrieved_at,
                        expected_endpoint="stock_splits",
                        evidence_kind="valuation EV split-history source",
                    )
                    split_records = decode_canonical_json_records(
                        (split_source.key_fields or {}).get("normalized_records"),
                        field_name="valuation EV split-history normalized_records",
                    )
                    ev_price_observation = next(
                        (
                            row
                            for row in observations
                            if row.issuer == observation.issuer
                            and row.as_of_close == observation.as_of_close
                            and row.component_type == "price"
                        ),
                        None,
                    )
                    ev_price_basis_date = observation.as_of_close
                    if ev_price_observation is not None:
                        ev_price_source, ev_price_retrieved_at, _ = bound_artifact_source(
                            source_id=ev_price_observation.source_id,
                            artifact_sha256=None,
                            evidence_kind="valuation EV official close",
                        )
                        _, ev_price_observed_at = fmp_response_lineage(
                            ev_price_source,
                            source_retrieved_at=ev_price_retrieved_at,
                            expected_endpoint="historical_price_eod",
                            evidence_kind="valuation EV official close",
                        )
                        ev_price_basis_date = max(
                            observation.as_of_close,
                            ev_price_observed_at.astimezone(timezone.utc).date(),
                        )
                    event_refs: list[tuple[date, int, str]] = []
                    for row_index, record in enumerate(split_records):
                        if not isinstance(record, Mapping):
                            raise ValueError("valuation EV split row must be an object")
                        raw_date = record.get("date")
                        if not isinstance(raw_date, str):
                            raise ValueError(
                                "valuation EV split row requires exact date"
                            )
                        event_date = canonical_iso_date(
                            raw_date,
                            field_name="valuation EV split date",
                        )
                        if (
                            component_as_of
                            < event_date
                            <= ev_price_basis_date
                        ):
                            event_refs.append(
                                (
                                    event_date,
                                    row_index,
                                    f"{split_locator}#data[{row_index}].date",
                                )
                            )
                    expected_event_refs = tuple(
                        item[2] for item in sorted(event_refs)
                    )
                    if (
                        dict(split_response.validated_params)
                        != {"symbol": observation.issuer}
                        or observation.split_history_ref != f"{split_locator}#response"
                        or observation.split_events_between != expected_event_refs
                        or expected_event_refs
                        or (
                            market_plane_locator_as_of_date(split_locator) is None
                            and split_observed_at.date() < observation.as_of_close
                        )
                        or not is_market_plane_print_admissible(
                            locator=split_locator,
                            as_of_close=observation.as_of_close,
                            observed_at=split_observed_at,
                            cutoff_at=cutoff_at,
                        )
                        or observation.value != native_value / Decimal("1000000")
                        or observation.retrieved_at
                        != max(accepted_at, split_observed_at)
                    ):
                        raise ValueError(
                            "valuation EV shares require exact no-split share basis"
                        )
                elif observation.component_type in {"debt", "cash"}:
                    provenance = observation.signed_refs
                    if provenance:
                        bound_list: list[str] = []
                        native_value = Decimal(0)
                        report_dates: set[date] = set()
                        for line in provenance:
                            line_qnames, line_value, line_date = financials_ev_fact(
                                line.ref,
                                facts=response_facts,
                                logical_locator=observation_locator,
                                evidence_kind=response_evidence_kind,
                            )
                            if not line_qnames:
                                raise ValueError(
                                    "valuation EV monetary component differs from source"
                                )
                            bound_list.extend(line_qnames)
                            native_value += Decimal(line.sign) * line_value
                            report_dates.add(line_date)
                        bound_qnames = tuple(bound_list)
                        report_date = (
                            next(iter(report_dates))
                            if len(report_dates) == 1
                            else None
                        )
                    else:
                        bound_qnames, native_value, report_date = financials_ev_fact(
                            observation.raw_ref,
                            facts=response_facts,
                            logical_locator=observation_locator,
                            evidence_kind=response_evidence_kind,
                        )
                    if observation.component_type == "cash":
                        for qname in bound_qnames:
                            require_own_funds_concept(
                                qname,
                                evidence_kind="valuation EV corporate cash",
                            )
                    if (
                        (
                            provenance
                            and (
                                observation.raw_ref != provenance[0].ref
                                or report_date is None
                            )
                        )
                        or observation.component_as_of != report_date
                        or observation.value != native_value / Decimal("1000000")
                        # A zero bound to no concept names what it read as zero.
                        or not (bound_qnames or observation.assumed_zero_qnames)
                        or currency_observed_at is None
                        or observation.retrieved_at
                        != max(accepted_at, currency_observed_at)
                    ):
                        raise ValueError(
                            "valuation EV monetary component differs from source"
                        )
                else:
                    adjustment = observation.declared_adjustment
                    if adjustment is None:
                        raise ValueError(
                            "declared EV adjustment requires its declared legs"
                        )
                    leg_report_dates: set[date] = set()
                    for leg in adjustment.legs:
                        leg_qnames, leg_native_value, leg_report_date = (
                            financials_ev_fact(
                                leg.ref,
                                facts=response_facts,
                                logical_locator=observation_locator,
                                evidence_kind=(
                                    "valuation EV declared adjustment leg"
                                ),
                            )
                        )
                        if leg.value != leg_native_value / Decimal("1000000") or not (
                            leg_qnames or observation.assumed_zero_qnames
                        ):
                            raise ValueError(
                                "valuation EV declared adjustment legs differ from "
                                "source"
                            )
                        leg_report_dates.add(leg_report_date)
                    if (
                        observation.raw_ref != adjustment.legs[0].ref
                        or len(leg_report_dates) != 1
                        or observation.component_as_of != next(iter(leg_report_dates))
                        or currency_observed_at is None
                        or observation.retrieved_at
                        != max(accepted_at, currency_observed_at)
                    ):
                        raise ValueError(
                            "valuation EV declared adjustment differs from source"
                        )

        if ntm_operands is not None:
            observation_key_fields = observation_source.key_fields or {}
            estimate_response, estimate_observed_at = fmp_response_lineage(
                observation_source,
                source_retrieved_at=observation_retrieved_at,
                expected_endpoint="analyst_estimates",
                evidence_kind="valuation NTM estimate source",
            )
            validated_params = estimate_response.validated_params
            validated_limit = validated_params.get("limit")
            if (
                validated_params.get("symbol") != observation.issuer
                or validated_params.get("period") != "quarter"
                or isinstance(validated_limit, bool)
                or not isinstance(validated_limit, int)
                or not 4 <= validated_limit <= 100
            ):
                raise ValueError(
                    "valuation NTM estimate source must be analyst_estimates "
                    "with exact symbol, period=quarter, and bounded limit"
                )
            if observation.retrieved_at != estimate_observed_at:
                raise ValueError(
                    "valuation NTM observation retrieved_at must match source observed_at"
                )
            if (
                observation_key_fields.get("snapshot_manifest_sha256")
                != observation.snapshot_manifest_sha256
            ):
                raise ValueError(
                    "valuation NTM snapshot manifest must match estimate source"
                )
            if observation.raw_close_source_id is None:
                raise ValueError("valuation NTM requires a raw close source")
            ntm_close_source, ntm_close_retrieved_at, ntm_close_locator = (
                bound_artifact_source(
                    source_id=observation.raw_close_source_id,
                    artifact_sha256=None,
                    evidence_kind="valuation NTM official close",
                )
            )
            close_binding = bind_official_close_fields(
                observation=observation,
                close_ref=observation.raw_close_ref,
                evidence_kind="valuation NTM official close",
            )
            close_key_fields = ntm_close_source.key_fields or {}
            if close_key_fields.get("native_field") != "close":
                raise ValueError(
                    "valuation NTM close source must be historical_price_eod close"
                )
            close_response, close_observed_at = fmp_response_lineage(
                ntm_close_source,
                source_retrieved_at=ntm_close_retrieved_at,
                expected_endpoint="historical_price_eod",
                evidence_kind="valuation NTM official close source",
            )
            close_params = close_response.validated_params
            close_date_text = close_binding.market_date.isoformat()
            if dict(close_params) != {
                "from": close_date_text,
                "serietype": "line",
                "symbol": close_binding.issuer,
                "to": close_date_text,
            }:
                raise ValueError(
                    "valuation NTM close request must bind issuer and exact date range"
                )
            if not is_market_plane_print_admissible(
                locator=ntm_close_locator,
                as_of_close=observation.as_of_close,
                observed_at=close_observed_at,
                cutoff_at=cutoff_at,
            ):
                raise ValueError(
                    "post-cutoff close observation cannot enter valuation NTM"
                )
            if ntm_operands.official_close_date_ref is None:
                raise ValueError("valuation NTM requires an official close date ref")
            close_scalar = scalar_for_ref(
                close_binding.close_ref,
                logical_locator=ntm_close_locator,
                response=close_response,
                expected_field="close",
                evidence_kind="valuation NTM official close",
            )
            close_date_scalar = scalar_for_ref(
                ntm_operands.official_close_date_ref,
                logical_locator=ntm_close_locator,
                response=close_response,
                expected_field="date",
                evidence_kind="valuation NTM official close date",
            )
            if (
                close_scalar.row_index != close_date_scalar.row_index
                or close_scalar.as_decimal() != observation.raw_close
                or close_date_scalar.as_date() != close_binding.market_date
                or close_scalar.period_as_date() != close_binding.market_date
                or close_date_scalar.period is not None
            ):
                raise ValueError(
                    "valuation NTM official close and date must bind one exact row"
                )
            anchor = ntm_operands.reported_anchor
            selected_anchor_period_end_ref = bind_reported_period_anchor(
                observation=observation,
                anchor=anchor,
                evidence_kind="valuation NTM reported anchor",
            )

            estimate_date_scalars = tuple(
                scalar for scalar in estimate_response.scalars if scalar.field == "date"
            )
            estimate_date_by_row = {
                scalar.row_index: scalar for scalar in estimate_date_scalars
            }
            if any(
                scalar.row_index not in estimate_date_by_row
                for scalar in estimate_response.scalars
                if scalar.field == "epsAvg"
            ):
                raise ValueError(
                    "valuation NTM estimate EPS rows require exact date coordinates"
                )
            future_dates: dict[date, list[FmpVendorScalarV1]] = {}
            for scalar in estimate_date_scalars:
                scalar_date = scalar.as_date()
                if scalar_date > anchor.period_end:
                    future_dates.setdefault(scalar_date, []).append(scalar)
            selected_estimate_dates = tuple(sorted(future_dates))[:4]
            if len(selected_estimate_dates) < 4:
                raise ValueError(
                    "valuation NTM estimate source requires four future quarter dates"
                )
            selected_date_scalars: list[FmpVendorScalarV1] = []
            for selected_date in selected_estimate_dates:
                date_rows = future_dates[selected_date]
                if len(date_rows) != 1:
                    raise ValueError(
                        "valuation NTM selected estimate date is ambiguous"
                    )
                selected_date_scalars.append(date_rows[0])
            expected_period_bindings = tuple(
                (
                    f"{observation_locator}#{scalar.result_key}",
                    scalar.as_date(),
                )
                for scalar in selected_date_scalars
            )
            actual_period_bindings = tuple(
                (quarter.estimate_period_end_ref, quarter.period_end)
                for quarter in ntm_operands.quarterly_eps
            )
            if actual_period_bindings != expected_period_bindings:
                raise ValueError(
                    "valuation NTM operands must bind the earliest four exact "
                    "provider dates after the selected anchor"
                )

            previous_quarter: NtmQuarterlyEpsOperand | None = None
            for quarter in ntm_operands.quarterly_eps:
                period_scalar = scalar_for_ref(
                    quarter.estimate_period_end_ref,
                    logical_locator=observation_locator,
                    response=estimate_response,
                    expected_field="date",
                    evidence_kind="valuation NTM estimate period",
                )
                eps_scalar = scalar_for_ref(
                    quarter.eps_ref,
                    logical_locator=observation_locator,
                    response=estimate_response,
                    expected_field="epsAvg",
                    evidence_kind="valuation NTM EPS",
                )
                row_index = period_scalar.row_index
                if (
                    row_index != eps_scalar.row_index
                    or period_scalar.as_date() != quarter.period_end
                    or period_scalar.period is not None
                    or eps_scalar.as_decimal() != quarter.eps
                    or eps_scalar.period_as_date() != quarter.period_end
                ):
                    raise ValueError(
                        "valuation NTM date and EPS refs must bind one exact row"
                    )
                if quarter.analyst_count_ref is not None:
                    count_scalar = scalar_for_ref(
                        quarter.analyst_count_ref,
                        logical_locator=observation_locator,
                        response=estimate_response,
                        expected_field="numAnalystsEps",
                        evidence_kind="valuation NTM analyst count",
                    )
                    if (
                        count_scalar.row_index != row_index
                        or count_scalar.as_integer() != quarter.analyst_count
                        or count_scalar.period_as_date() != quarter.period_end
                    ):
                        raise ValueError(
                            "valuation NTM analyst count must bind the estimate row"
                        )

                fiscal_source, fiscal_retrieved_at, fiscal_locator = (
                    bound_artifact_source(
                        source_id=quarter.fiscal_source_id,
                        artifact_sha256=None,
                        evidence_kind="valuation NTM quarter fiscal evidence",
                    )
                )
                if fiscal_retrieved_at > retrieved_at:
                    raise ValueError(
                        "valuation NTM fiscal source retrieval cannot postdate "
                        "evidence retrieval"
                    )
                if (
                    quarter.fiscal_boundary_rule
                    == "adjacent_provider_quarter_period_ends"
                ):
                    if fiscal_source.id != observation_source.id:
                        raise ValueError(
                            "adjacent provider quarter ends must use the estimate source"
                        )
                    fiscal_available_at = estimate_observed_at
                    fiscal_end_scalar = scalar_for_ref(
                        quarter.fiscal_period_end_ref,
                        logical_locator=fiscal_locator,
                        response=estimate_response,
                        expected_field="date",
                        evidence_kind="valuation NTM fiscal period end",
                    )
                    if (
                        fiscal_end_scalar.as_date() != quarter.period_end
                        or quarter.fiscal_period_end_ref
                        != quarter.estimate_period_end_ref
                    ):
                        raise ValueError(
                            "valuation NTM fiscal period end must bind its estimate date"
                        )
                    expected_predecessor = (
                        anchor.period_end
                        if previous_quarter is None
                        else previous_quarter.period_end
                    )
                    expected_predecessor_ref = (
                        selected_anchor_period_end_ref
                        if previous_quarter is None
                        else previous_quarter.fiscal_period_end_ref
                    )
                    if (
                        quarter.fiscal_predecessor_period_end != expected_predecessor
                        or quarter.fiscal_predecessor_period_end_ref
                        != expected_predecessor_ref
                    ):
                        raise ValueError(
                            "valuation NTM predecessor end must bind anchor or prior quarter"
                        )
                else:
                    if fiscal_source.provider != "edgar":
                        raise ValueError(
                            "reported quarter duration must use an EDGAR source"
                        )
                    fiscal_key_fields = fiscal_source.key_fields or {}
                    raw_fiscal_candidates = canonical_json_field(
                        fiscal_source,
                        field_name="reported_quarter_duration_candidates",
                        expected_type=list,
                        evidence_kind="valuation NTM reported-quarter fiscal source",
                    )
                    assert isinstance(raw_fiscal_candidates, list)
                    fiscal_candidates_json = fiscal_key_fields.get(
                        "reported_quarter_duration_candidates"
                    )
                    if (
                        not isinstance(fiscal_candidates_json, str)
                        or fiscal_source.text != fiscal_candidates_json
                        or hashlib.sha256(
                            fiscal_candidates_json.encode("utf-8")
                        ).hexdigest()
                        != fiscal_key_fields.get("artifact_sha256")
                    ):
                        raise ValueError(
                            "valuation NTM fiscal candidates must equal source artifact"
                        )
                    expected_candidate_keys = {
                        "issuer",
                        "boundary_rule",
                        "period_start",
                        "period_end",
                        "period_start_ref",
                        "period_end_ref",
                        "available_at",
                        "accepted_at",
                    }
                    for candidate in raw_fiscal_candidates:
                        if (
                            not isinstance(candidate, dict)
                            or set(candidate) != expected_candidate_keys
                            or candidate.get("issuer") != observation.issuer
                            or candidate.get("boundary_rule")
                            != "reported_quarter_duration"
                        ):
                            raise ValueError(
                                "valuation NTM reported-quarter candidate has invalid shape"
                            )
                        candidate_start = canonical_iso_date(
                            candidate.get("period_start"),
                            field_name="valuation NTM fiscal period_start",
                        )
                        candidate_end = canonical_iso_date(
                            candidate.get("period_end"),
                            field_name="valuation NTM fiscal period_end",
                        )
                        candidate_available_at = canonical_utc_timestamp(
                            candidate.get("available_at"),
                            field_name="valuation NTM fiscal available_at",
                        )
                        candidate_accepted_at = canonical_utc_timestamp(
                            candidate.get("accepted_at"),
                            field_name="valuation NTM fiscal accepted_at",
                        )
                        if (
                            candidate_start >= candidate_end
                            or candidate_accepted_at > candidate_available_at
                            or candidate_available_at > fiscal_retrieved_at
                        ):
                            raise ValueError(
                                "valuation NTM reported-quarter candidate timing is invalid"
                            )
                        for ref_name in ("period_start_ref", "period_end_ref"):
                            ref = candidate.get(ref_name)
                            if not isinstance(ref, str):
                                raise ValueError(
                                    "valuation NTM reported-quarter refs are invalid"
                                )
                            result_key_for_ref(
                                ref,
                                logical_locator=fiscal_locator,
                                evidence_kind="valuation NTM reported-quarter fiscal",
                            )
                    expected_fiscal_candidate = {
                        "issuer": observation.issuer,
                        "boundary_rule": "reported_quarter_duration",
                        "period_start": quarter.period_start.isoformat(),
                        "period_end": quarter.period_end.isoformat(),
                        "period_start_ref": quarter.fiscal_period_start_ref,
                        "period_end_ref": quarter.fiscal_period_end_ref,
                    }
                    matching_fiscal_candidates = tuple(
                        candidate
                        for candidate in raw_fiscal_candidates
                        if isinstance(candidate, dict)
                        and {
                            key: candidate.get(key) for key in expected_fiscal_candidate
                        }
                        == expected_fiscal_candidate
                    )
                    if len(matching_fiscal_candidates) != 1:
                        raise ValueError(
                            "valuation NTM reported-quarter values must match source"
                        )
                    fiscal_candidate = matching_fiscal_candidates[0]
                    fiscal_available_at = canonical_utc_timestamp(
                        fiscal_candidate.get("available_at"),
                        field_name="valuation NTM fiscal available_at",
                    )
                    fiscal_accepted_at = canonical_utc_timestamp(
                        fiscal_candidate.get("accepted_at"),
                        field_name="valuation NTM fiscal accepted_at",
                    )
                    if fiscal_accepted_at > fiscal_available_at:
                        raise ValueError(
                            "valuation NTM fiscal availability cannot predate acceptance"
                        )
                    for fiscal_ref in (
                        quarter.fiscal_period_start_ref,
                        quarter.fiscal_period_end_ref,
                    ):
                        assert fiscal_ref is not None
                        result_key_for_ref(
                            fiscal_ref,
                            logical_locator=fiscal_locator,
                            evidence_kind="valuation NTM reported-quarter fiscal",
                        )
                if fiscal_available_at > cutoff_at:
                    raise ValueError(
                        "post-cutoff fiscal availability cannot define valuation NTM"
                    )
                if fiscal_available_at > fiscal_retrieved_at:
                    raise ValueError(
                        "valuation NTM fiscal retrieval cannot predate availability"
                    )
                previous_quarter = quarter

            if observation.currency_source_id is not None:
                (
                    ntm_currency_source,
                    ntm_currency_retrieved_at,
                    ntm_currency_locator,
                ) = bound_artifact_source(
                    source_id=observation.currency_source_id,
                    artifact_sha256=None,
                    evidence_kind="valuation NTM currency",
                )
                ntm_currency_response, ntm_currency_observed_at = fmp_response_lineage(
                    ntm_currency_source,
                    source_retrieved_at=ntm_currency_retrieved_at,
                    expected_endpoint="profile",
                    evidence_kind="valuation NTM currency source",
                )
                ntm_currency_records = decode_canonical_json_records(
                    (ntm_currency_source.key_fields or {}).get("normalized_records"),
                    field_name="valuation NTM currency normalized_records",
                )
                if (
                    dict(ntm_currency_response.validated_params)
                    != {"symbol": observation.issuer}
                    or len(ntm_currency_records) != 1
                    or not isinstance(ntm_currency_records[0], Mapping)
                    or ntm_currency_records[0].get("symbol") != observation.issuer
                    or ntm_currency_records[0].get("currency") != "USD"
                    or observation.currency != "USD"
                    or observation.currency_ref
                    != f"{ntm_currency_locator}#data[0].currency"
                    or not is_market_plane_print_admissible(
                        locator=ntm_currency_locator,
                        as_of_close=observation.as_of_close,
                        observed_at=ntm_currency_observed_at,
                        cutoff_at=cutoff_at,
                    )
                ):
                    raise ValueError(
                        "valuation NTM forward P/E currency must bind exact profile row"
                    )

            if observation.split_history_source_id is None:
                if observation.split_events_between:
                    raise ValueError(
                        "valuation NTM split events require a split-history source"
                    )
            else:
                ntm_split_source, ntm_split_retrieved_at, ntm_split_locator = (
                    bound_artifact_source(
                        source_id=observation.split_history_source_id,
                        artifact_sha256=None,
                        evidence_kind="valuation NTM split history",
                    )
                )
                ntm_split_response, ntm_split_observed_at = fmp_response_lineage(
                    ntm_split_source,
                    source_retrieved_at=ntm_split_retrieved_at,
                    expected_endpoint="stock_splits",
                    evidence_kind="valuation NTM split-history source",
                )
                ntm_split_records = decode_canonical_json_records(
                    (ntm_split_source.key_fields or {}).get("normalized_records"),
                    field_name="valuation NTM split-history normalized_records",
                )
                ntm_price_basis_date = max(
                    observation.as_of_close,
                    close_observed_at.astimezone(timezone.utc).date(),
                )
                ntm_event_refs: list[tuple[date, int, str]] = []
                for row_index, record in enumerate(ntm_split_records):
                    if not isinstance(record, Mapping):
                        raise ValueError("valuation NTM split row must be an object")
                    raw_split_date = record.get("date")
                    if not isinstance(raw_split_date, str):
                        raise ValueError("valuation NTM split row requires exact date")
                    split_event_date = canonical_iso_date(
                        raw_split_date,
                        field_name="valuation NTM split date",
                    )
                    if anchor.period_end < split_event_date <= ntm_price_basis_date:
                        ntm_event_refs.append(
                            (
                                split_event_date,
                                row_index,
                                f"{ntm_split_locator}#data[{row_index}].date",
                            )
                        )
                expected_ntm_event_refs = tuple(
                    item[2] for item in sorted(ntm_event_refs)
                )
                if (
                    dict(ntm_split_response.validated_params)
                    != {"symbol": observation.issuer}
                    or observation.split_history_ref != f"{ntm_split_locator}#response"
                    or observation.split_events_between != expected_ntm_event_refs
                    or (
                        market_plane_locator_as_of_date(ntm_split_locator) is None
                        and ntm_split_observed_at.date() < observation.as_of_close
                    )
                    or not is_market_plane_print_admissible(
                        locator=ntm_split_locator,
                        as_of_close=observation.as_of_close,
                        observed_at=ntm_split_observed_at,
                        cutoff_at=cutoff_at,
                    )
                ):
                    raise ValueError(
                        "valuation NTM forward P/E split basis must bind exact events"
                    )

        definition = observation.definition_evidence
        if definition is None:
            continue
        if (
            definition.provider_endpoint,
            definition.provider_field,
        ) != _VALUATION_DEFINITION_SCOPE.get(
            (observation.provider, observation.metric)
        ):
            raise ValueError(
                "valuation definition evidence scope must match its forward metric"
            )
        definition_source, definition_retrieved_at, _ = bound_artifact_source(
            source_id=definition.provider_doc_source_id,
            artifact_sha256=definition.provider_doc_artifact_sha256,
            evidence_kind="valuation definition evidence",
        )
        if definition_source.provider != observation.provider:
            raise ValueError(
                "valuation definition evidence provider must match observation"
            )
        definition_key_fields = definition_source.key_fields or {}
        if (
            definition_source.endpoint_or_filing_id != definition.provider_endpoint
            or definition_key_fields.get("provider_endpoint")
            != definition.provider_endpoint
            or definition_key_fields.get("provider_field") != definition.provider_field
        ):
            raise ValueError(
                "valuation definition evidence scope must match its source"
            )
        definition_claims = {
            "provider_basis_scope": definition.provider_basis_scope,
            "provider_currency_semantics": definition.provider_currency_semantics,
            "provider_numeric_scale": definition.provider_numeric_scale,
        }
        if any(
            value is not None and definition_key_fields.get(field_name) != value
            for field_name, value in definition_claims.items()
        ):
            raise ValueError(
                "valuation definition evidence semantic claims must match its source"
            )
        if definition_retrieved_at > cutoff_at:
            raise ValueError(
                "post-cutoff valuation definition evidence cannot enter valuation evidence"
            )
        if definition_retrieved_at > retrieved_at:
            raise ValueError(
                "valuation definition evidence source retrieval cannot postdate evidence retrieval"
            )

    snapshot_as_of_close = max(
        (obs.as_of_close for obs in observations),
        default=None,
    )
    for fact in fiscal_calendar:
        if not fact.has_exact_annual_source_evidence:
            continue
        if fact.source_id is None or fact.available_at is None:
            raise ValueError("exact fiscal period evidence is incomplete")
        fiscal_source, fiscal_retrieved_at, logical_locator = bound_artifact_source(
            source_id=fact.source_id,
            artifact_sha256=fact.source_artifact_sha256,
            evidence_kind="fiscal period evidence",
        )
        fiscal_locator_date = market_plane_locator_as_of_date(logical_locator)
        fiscal_source_observed_at = (fiscal_source.key_fields or {}).get("observed_at")
        has_unmodified_market_print = (
            fiscal_locator_date is not None
            and isinstance(fiscal_source_observed_at, str)
            and canonical_utc_timestamp(
                fiscal_source_observed_at,
                field_name="fiscal source observed_at",
            )
            == fact.available_at
        )
        effective_fiscal_close = snapshot_as_of_close or cutoff_at.date()
        if not (
            is_market_plane_print_admissible(
                locator=logical_locator,
                as_of_date=fiscal_locator_date,
                as_of_close=effective_fiscal_close or retrieved_at.date(),
                observed_at=fiscal_retrieved_at,
                cutoff_at=retrieved_at,
            )
            if has_unmodified_market_print
            else fiscal_retrieved_at <= retrieved_at
        ):
            raise ValueError(
                "fiscal period evidence source retrieval cannot postdate evidence retrieval"
            )
        if any(
            locator != logical_locator and not locator.startswith(f"{logical_locator}#")
            for locator in fact.source_locators
        ):
            raise ValueError(
                "fiscal source_locators must resolve within their source artifact"
            )
        if has_unmodified_market_print and effective_fiscal_close is not None:
            fiscal_admissible = is_market_plane_print_admissible(
                locator=logical_locator,
                as_of_date=fiscal_locator_date,
                as_of_close=effective_fiscal_close,
                observed_at=fact.available_at,
                cutoff_at=cutoff_at,
            )
        else:
            fiscal_admissible = fact.available_at <= cutoff_at
        if not fiscal_admissible:
            raise ValueError(
                "post-cutoff fiscal period evidence cannot enter valuation evidence"
            )
        if _has_fmp_provider_lineage(fiscal_source):
            fiscal_key_fields = fiscal_source.key_fields or {}
            if fiscal_key_fields.get("native_field") != "epsAvg":
                raise ValueError(
                    "annual fiscal source must identify analyst-estimate epsAvg"
                )
            fiscal_response, fiscal_observed_at = fmp_response_lineage(
                fiscal_source,
                source_retrieved_at=fiscal_retrieved_at,
                expected_endpoint="analyst_estimates",
                evidence_kind="annual fiscal period evidence",
            )
            expected_fiscal_params = {
                "limit": 10,
                "period": "annual",
                "symbol": fact.issuer,
            }
            if dict(fiscal_response.validated_params) != expected_fiscal_params:
                raise ValueError("annual fiscal source must bind exact provider params")
            if fact.available_at != fiscal_observed_at:
                raise ValueError(
                    "annual fiscal availability must equal provider observed_at"
                )
            if (
                fact.derivation_rule != "adjacent_provider_annual_period_ends"
                or len(fact.source_locators) != 2
            ):
                raise ValueError(
                    "lineaged annual fiscal evidence requires two adjacent date refs"
                )
            predecessor_scalar = scalar_for_ref(
                fact.source_locators[0],
                logical_locator=logical_locator,
                response=fiscal_response,
                expected_field="date",
                evidence_kind="annual fiscal predecessor end",
            )
            target_scalar = scalar_for_ref(
                fact.source_locators[1],
                logical_locator=logical_locator,
                response=fiscal_response,
                expected_field="date",
                evidence_kind="annual fiscal target end",
            )
            predecessor_end = fact.period_start - timedelta(days=1)
            predecessor_year_dates = tuple(
                scalar
                for scalar in fiscal_response.scalars
                if scalar.field == "date"
                and scalar.as_date().year == predecessor_end.year
            )
            target_year_dates = tuple(
                scalar
                for scalar in fiscal_response.scalars
                if scalar.field == "date"
                and scalar.as_date().year == fact.period_end.year
            )
            if (
                len(predecessor_year_dates) != 1
                or len(target_year_dates) != 1
                or predecessor_scalar.row_index == target_scalar.row_index
                or predecessor_scalar.period is not None
                or target_scalar.period is not None
                or predecessor_scalar.as_date() != predecessor_end
                or target_scalar.as_date() != fact.period_end
            ):
                raise ValueError(
                    "annual fiscal boundaries must bind exact adjacent provider dates"
                )


def validate_source_bundle_valuation_evidence(bundle: SourceBundle) -> None:
    """Check a SourceBundle's valuation evidence against its own sources.

    The source bindings of every valuation observation and fiscal fact, then
    the stored ``valuation_coverage`` against its pure recomputation. Ruling
    R10 moved both out of ``SourceBundle``'s decode-time validator; the bundle
    producer and the compile path run them.
    """

    validate_valuation_evidence_source_bindings(
        observations=bundle.valuation_observations,
        fiscal_calendar=bundle.fiscal_calendar,
        sources=bundle.sources,
        cutoff_at=bundle.cutoff_at,
        retrieved_at=bundle.retrieved_at,
        historical_basis_facts=bundle.historical_basis_facts,
    )
    derived = derive_valuation_coverage(
        bundle.valuation_observations,
        bundle.cutoff_at,
        bundle.market_calendar,
        bundle.fiscal_calendar,
        requirements=bundle.valuation_requirements,
        focal_ticker=bundle.coverage_profile.focal_ticker,
        peer_tickers=bundle.coverage_profile.peer_tickers,
        minimum_covered_peer_count=(
            bundle.coverage_profile.minimum_covered_peer_count
        ),
    )
    if bundle.valuation_coverage != derived:
        raise ValueError("valuation coverage differs from pure recomputation")


def validate_source_bundle_binding_and_selection(
    source_bundle: SourceBundle,
    model_spec: ModelSpec,
    *,
    persisted_source_bundle_payload: Mapping[str, Any] | None = None,
    persisted_source_bundle_wire_digest: str | None = None,
) -> None:
    """Fail closed when a reviewed specification is not bound to this bundle.

    Runs the Store's ``validate_source_bundle_binding`` on digest and coverage
    profile only. ``valuation_selection_findings`` reports unresolved peer-derived
    selections and unbundled ``evidence_ids`` as the declinable
    ``valuation_selection_unresolved`` warning at compile/publish.
    """

    validate_source_bundle_binding(
        source_bundle,
        model_spec,
        persisted_source_bundle_payload=persisted_source_bundle_payload,
        persisted_source_bundle_wire_digest=persisted_source_bundle_wire_digest,
    )


def valuation_selection_findings(
    source_bundle: SourceBundle,
    model_spec: ModelSpec,
) -> tuple[Finding, ...]:
    """One ``valuation_selection_unresolved`` per method with selection gaps.

    Derives the relative selection under the spec's peer policy (ruling R10);
    ``observed.selection_fix`` is the repair: copy the resolved selection, or
    fall back to ``thesis_judgment`` when the peer selection is blocked. Unbundled
    ``evidence_ids`` on any relative control or family authorization are also
    recorded, including when the specification has no valuation policy.
    """

    observation_ids = {
        item.observation_id for item in source_bundle.valuation_observations
    }
    relative_selection = {
        item.method: item
        for item in derive_relative_selection(
            source_bundle.valuation_observations,
            source_bundle.cutoff_at,
            source_bundle.market_calendar,
            source_bundle.fiscal_calendar,
            model_spec.valuation_policy.peer_policy,
            focal_ticker=source_bundle.coverage_profile.focal_ticker,
            valuation_requirements=source_bundle.valuation_requirements,
            selected_fiscal_basis=(
                source_bundle.coverage_profile.selected_relative_fiscal_basis
            ),
        )
    } if model_spec.valuation_policy is not None else {}
    findings: list[Finding] = []
    emitted_methods: set[str] = set()
    for authorization in model_spec.valuation_family_authorizations:
        if (
            authorization.selection_basis != "peer_derived"
            or model_spec.valuation_policy is None
        ):
            continue
        resolution = relative_selection[authorization.method]
        if (
            resolution.status == "resolved"
            and authorization.base == resolution.selected_value
            and authorization.evidence_ids == resolution.contributing_ids
        ):
            continue
        if resolution.status == "resolved":
            selection_fix = (
                f"{authorization.method}: copy peer_derived "
                f"base={resolution.selected_value} and evidence_ids="
                f"{list(resolution.contributing_ids)!r}"
            )
        else:
            selection_fix = (
                f"{authorization.method}: use thesis_judgment; "
                f"peer_derived unavailable (status={resolution.status})"
            )
        observed = {
            "selection_fix": selection_fix,
            "status": resolution.status,
        }
        unresolved = tuple(sorted(set(authorization.evidence_ids) - observation_ids))
        if unresolved:
            observed["unresolved_evidence_ids"] = ",".join(unresolved)
        findings.append(
            make_finding(
                "valuation_selection_unresolved",
                message=(
                    f"{authorization.method} peer_derived selection does not "
                    "resolve against this SourceBundle "
                    f"(status={resolution.status}); call "
                    "get_valuation_policy_context, then "
                    "fms_persist_valuation_policy with the selection_fix"
                ),
                item_id=authorization.method,
                observed=observed,
            )
        )
        emitted_methods.add(authorization.method)
    for row in (
        *(
            model_spec.valuation_policy.relative_methods
            if model_spec.valuation_policy is not None
            else ()
        ),
        *model_spec.valuation_family_authorizations,
    ):
        if row.method in emitted_methods:
            continue
        unresolved = tuple(sorted(set(row.evidence_ids) - observation_ids))
        if not unresolved:
            continue
        selection_fix = (
            f"{row.method}: cite only SourceBundle observation ids as evidence_ids; "
            f"not in this bundle: {', '.join(unresolved)}"
        )
        findings.append(
            make_finding(
                "valuation_selection_unresolved",
                message=(
                    f"{row.method} evidence_ids are not observations of this "
                    "SourceBundle; call get_valuation_policy_context, then "
                    "fms_persist_valuation_policy with the selection_fix"
                ),
                item_id=row.method,
                observed={
                    "unresolved_evidence_ids": ",".join(unresolved),
                    "selection_fix": selection_fix,
                },
            )
        )
        emitted_methods.add(row.method)
    return tuple(findings)
