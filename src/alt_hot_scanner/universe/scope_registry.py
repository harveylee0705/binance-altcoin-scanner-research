from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

from alt_hot_scanner.identity import require_semantic_contract_identity, safe_identity_component

SCOPE_REGISTRY_SCHEMA_VERSION = "historical-scope-registry-v1"
SCOPE_REVIEWER_VERSION = "finite-universe-review-v1"

_DELIVERY_IDENTITY = re.compile(r".+_(?:2\d{5}|SETTLED)")
_ARCHIVE_ONLY_COMPOSITES = {"BLUEBIRDUSDT", "DOTECOUSDT", "FOOTBALLUSDT"}
_ARCHIVE_ONLY_CRYPTO = {
    "1000BTTCUSDT", "AERGOUSDT", "AKROUSDT", "ANCUSDT", "ANTUSDT", "AUDIOUSDT",
    "BDXNUSDT", "BTCSTUSDT", "BTSUSDT", "BTTUSDT", "BZRXUSDT", "COCOSUSDT",
    "DODOUSDT", "EOSUSDT", "FRONTUSDT", "GALUSDT", "HNTUSDT", "KEEPUSDT",
    "LENDUSDT", "LUNAUSDT", "MATICUSDT", "MBLUSDT", "NUUSDT", "RNDRUSDT",
    "SRMUSDT", "SXPUSDT", "TOMOUSDT", "YFIIUSDT",
}
_ADDITIONAL_STABLECOINS = {"FRAX"}
_NON_ALTCOIN_ASSETS = {"PAXG", "XAUT"}


def canonical_candidate_set(candidates: list[str] | tuple[str, ...]) -> list[str]:
    validated = [
        require_semantic_contract_identity(value, f"candidate[{index}]")
        for index, value in enumerate(candidates)
    ]
    if len(validated) != len(set(validated)):
        raise ValueError("Historical candidate identities must be unique")
    return sorted(validated)


def candidate_set_digest(candidates: list[str] | tuple[str, ...]) -> str:
    payload = json.dumps(
        canonical_candidate_set(candidates), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_historical_scope_registry(
    candidates: list[str] | tuple[str, ...],
    exchange_info_payload: dict[str, Any],
    *,
    stablecoin_underlyings: list[str],
    audited_at: str | None = None,
) -> dict[str, Any]:
    """Audit a finite archive universe; negatives are valid only for its exact digest."""
    identities = canonical_candidate_set(candidates)
    current: dict[str, dict[str, Any]] = {}
    for position, item in enumerate(exchange_info_payload.get("symbols", [])):
        if type(item) is not dict:
            raise ValueError("exchangeInfo symbols must contain objects")
        symbol = require_semantic_contract_identity(
            item.get("symbol"), f"exchangeInfo.symbols[{position}].symbol"
        )
        if symbol in current:
            raise ValueError("exchangeInfo contains duplicate semantic identities")
        current[symbol] = item
    stablecoins = set(stablecoin_underlyings) | _ADDITIONAL_STABLECOINS
    records: list[dict[str, Any]] = []
    for identity in identities:
        item = current.get(identity)
        evidence: list[dict[str, str]] = []
        quote_asset: str | None = None
        base_asset: str | None = None
        product_scope = "unresolved"
        is_crypto: bool | None = None
        stable: bool | None = None
        leveraged: bool | None = None

        if item is not None:
            quote_asset = item.get("quoteAsset")
            base_asset = item.get("baseAsset")
            is_crypto = item.get("underlyingType") == "COIN"
            evidence.append(
                {
                    "source_type": "official_current_exchange_info",
                    "source_identifier": identity,
                    "fact": "exact_product_and_underlying_metadata",
                }
            )
            if quote_asset != "USDT" or item.get("marginAsset") != "USDT":
                product_scope = "excluded_non_usdt_product"
            elif item.get("contractType") != "PERPETUAL":
                product_scope = (
                    "excluded_noncrypto_or_index"
                    if not is_crypto
                    else "excluded_delivery_or_settlement_archive_identity"
                )
            elif not is_crypto:
                product_scope = "excluded_noncrypto_or_index"
            else:
                stable = base_asset in stablecoins
                leveraged = any(
                    "LEVERAGED" in str(value).upper()
                    for value in item.get("underlyingSubType", [])
                )
                if base_asset in _NON_ALTCOIN_ASSETS:
                    product_scope = "excluded_noncrypto_backed_or_non_altcoin"
                elif stable:
                    product_scope = "excluded_stablecoin"
                elif leveraged:
                    product_scope = "excluded_leveraged_token"
                elif identity == "BTCUSDT":
                    product_scope = "benchmark_only"
                else:
                    product_scope = "in_scope_crypto_perpetual"
        elif _DELIVERY_IDENTITY.fullmatch(identity):
            product_scope = "excluded_delivery_or_settlement_archive_identity"
            evidence.append(
                {
                    "source_type": "official_archive_identity",
                    "source_identifier": identity,
                    "fact": "dated_or_settled_contract_identity",
                }
            )
        elif not identity.endswith("USDT"):
            product_scope = "excluded_non_usdt_product"
            evidence.append(
                {
                    "source_type": "official_archive_identity",
                    "source_identifier": identity,
                    "fact": "not_an_exact_usdt_identity",
                }
            )
        elif identity in _ARCHIVE_ONLY_COMPOSITES:
            quote_asset = "USDT"
            base_asset = identity[:-4]
            is_crypto = False
            stable = False
            leveraged = False
            product_scope = "excluded_composite_or_index"
            evidence.append(
                {
                    "source_type": "reviewed_official_binance_futures_corpus",
                    "source_identifier": identity,
                    "fact": "named_index_or_composite_perpetual",
                }
            )
        elif identity in _ARCHIVE_ONLY_CRYPTO:
            quote_asset = "USDT"
            base_asset = identity[:-4]
            is_crypto = True
            stable = base_asset in stablecoins
            leveraged = False
            product_scope = (
                "excluded_stablecoin" if stable else "in_scope_crypto_perpetual"
            )
            evidence.append(
                {
                    "source_type": "reviewed_official_binance_futures_corpus",
                    "source_identifier": identity,
                    "fact": "archive_only_crypto_perpetual_identity",
                }
            )

        complete = product_scope != "unresolved"
        if complete and quote_asset == "USDT" and product_scope not in {
            "excluded_non_usdt_or_non_perpetual",
            "excluded_non_usdt_product",
            "excluded_delivery_or_settlement_archive_identity",
        }:
            if stable is None:
                stable = False
            if leveraged is None:
                leveraged = False
        records.append(
            {
                "contract_identity": identity,
                "safe_storage_component": safe_identity_component(identity),
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "product_scope": product_scope,
                "is_crypto_underlying": is_crypto,
                "is_stablecoin_underlying": stable,
                "is_leveraged_token": leveraged,
                "stablecoin_evidence_status": (
                    "positive_exclusion_evidence"
                    if stable is True
                    else "reviewed_finite_universe_negative"
                    if stable is False
                    else "unresolved"
                ),
                "leveraged_evidence_status": (
                    "positive_exclusion_evidence"
                    if leveraged is True
                    else "reviewed_finite_universe_negative"
                    if leveraged is False
                    else "unresolved"
                ),
                "scope_audit_status": "complete" if complete else "unresolved",
                "evidence": evidence,
            }
        )
    digest = candidate_set_digest(identities)
    positives = lambda field: [r["contract_identity"] for r in records if r[field] is True]
    return {
        "schema_version": SCOPE_REGISTRY_SCHEMA_VERSION,
        "candidate_set_digest": digest,
        "candidate_count": len(identities),
        "candidate_identities": identities,
        "audit_timestamp": audited_at or datetime.now(UTC).isoformat(),
        "audit_methodology": (
            "exact finite archive-universe review using official current exchangeInfo, official "
            "archive identity, and reviewed official Binance Futures corpus evidence"
        ),
        "reviewer_parser_version": SCOPE_REVIEWER_VERSION,
        "stablecoin_positive_exclusions": positives("is_stablecoin_underlying"),
        "leveraged_token_positive_exclusions": positives("is_leveraged_token"),
        "noncrypto_index_composite_exclusions": [
            r["contract_identity"]
            for r in records
            if r["is_crypto_underlying"] is False
            or r["product_scope"] == "excluded_composite_or_index"
            or r["product_scope"] == "excluded_noncrypto_backed_or_non_altcoin"
        ],
        "unresolved_identities": [
            r["contract_identity"] for r in records if r["scope_audit_status"] != "complete"
        ],
        "records": records,
    }


def verify_scope_registry(registry: dict[str, Any], candidates: list[str]) -> dict[str, Any]:
    if registry.get("schema_version") != SCOPE_REGISTRY_SCHEMA_VERSION:
        raise ValueError("Historical scope registry has an unsupported schema")
    canonical = canonical_candidate_set(candidates)
    if registry.get("candidate_identities") != canonical:
        raise ValueError("Historical scope registry candidate identities changed")
    if registry.get("candidate_set_digest") != candidate_set_digest(canonical):
        raise ValueError("Historical scope registry candidate-set digest mismatch")
    records = registry.get("records")
    if type(records) is not list or len(records) != len(canonical):
        raise ValueError("Historical scope registry is incomplete")
    by_identity = {record.get("contract_identity"): record for record in records}
    if set(by_identity) != set(canonical) or len(by_identity) != len(records):
        raise ValueError("Historical scope registry record identities are incomplete")
    return by_identity
