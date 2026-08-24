from __future__ import annotations

from typing import Any

from alt_hot_scanner.identity import safe_identity_component
from alt_hot_scanner.universe.scope_registry import (
    SCOPE_REGISTRY_SCHEMA_VERSION,
    candidate_set_digest,
    scope_registry_identity,
    scope_registry_payload_identity,
)


def build_reviewed_scope_registry_fixture(
    candidates: list[str], exchange_payload: dict[str, Any], *, audited_at: str
) -> dict[str, Any]:
    """Build test-only reviewed dispositions; production builders must require a review input."""
    identities = sorted(candidates)
    exchange = {item["symbol"]: item for item in exchange_payload.get("symbols", [])}
    records: list[dict[str, Any]] = []
    stable_positives: list[str] = []
    leveraged_positives: list[str] = []
    for identity in identities:
        item = exchange.get(identity, {})
        quote = item.get("quoteAsset")
        base = item.get("baseAsset") or (
            identity.removesuffix("USDT") if identity.endswith("USDT") else identity
        )
        stable = base in {"FRAX", "USDC", "USTC"}
        leveraged = "Leveraged Token" in item.get("underlyingSubType", [])
        in_usdt_perpetual = quote == "USDT" and item.get("contractType") == "PERPETUAL"
        if stable:
            product_scope = "excluded_stablecoin"
            stable_positives.append(identity)
        elif leveraged:
            product_scope = "excluded_leveraged_token"
            leveraged_positives.append(identity)
        elif identity == "BTCUSDT" and in_usdt_perpetual:
            product_scope = "benchmark_only"
        elif in_usdt_perpetual:
            product_scope = "in_scope_crypto_perpetual"
        else:
            product_scope = "excluded_non_usdt_product"
        evidence: list[dict[str, str]] = [
            {
                "fact": "exact_product_and_underlying_metadata",
                "source_identifier": identity,
                "source_type": "fixture_exchange_info",
            }
        ]
        if stable or leveraged:
            exclusion_class = "stablecoin" if stable else "leveraged_token"
            evidence.append(
                {
                    "evidence_class": "direct_positive_exclusion",
                    "exclusion_class": exclusion_class,
                    "source_name": "Authoritative fixture source",
                    "source_type": "project_or_issuer_documentation",
                    "source_identifier": f"fixture:{identity}:{exclusion_class}",
                    "source_url": f"https://official.example/{identity}/{exclusion_class}",
                    "reviewed_at": audited_at,
                    "evidence_summary": f"Directly identifies {identity} as {exclusion_class}.",
                    "review_version": "test-reviewed-scope-v1",
                }
            )
        records.append(
            {
                "contract_identity": identity,
                "base_asset": base,
                "quote_asset": quote,
                "product_scope": product_scope,
                "is_crypto_underlying": True,
                "is_stablecoin_underlying": stable,
                "is_leveraged_token": leveraged,
                "stablecoin_evidence_status": (
                    "positive_exclusion_evidence"
                    if stable
                    else "reviewed_finite_universe_negative"
                ),
                "leveraged_evidence_status": (
                    "positive_exclusion_evidence"
                    if leveraged
                    else "reviewed_finite_universe_negative"
                ),
                "scope_audit_status": "complete",
                "safe_storage_component": safe_identity_component(identity),
                "evidence": evidence,
            }
        )
    registry: dict[str, Any] = {
        "schema_version": SCOPE_REGISTRY_SCHEMA_VERSION,
        "audit_methodology": "test-only exact finite candidate review",
        "audit_timestamp": audited_at,
        "reviewer_parser_version": "test-reviewed-scope-v1",
        "candidate_count": len(identities),
        "candidate_identities": identities,
        "candidate_set_digest": candidate_set_digest(identities),
        "stablecoin_positive_exclusions": sorted(stable_positives),
        "leveraged_token_positive_exclusions": sorted(leveraged_positives),
        "noncrypto_index_composite_exclusions": [],
        "unresolved_identities": [],
        "records": records,
    }
    registry["registry_payload_id"] = scope_registry_payload_identity(registry)
    registry["registry_id"] = scope_registry_identity(registry)
    return registry
