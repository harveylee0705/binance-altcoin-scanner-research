from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from alt_hot_scanner.identity import (
    IdentityValidationError,
    require_archive_symbol_identity,
    require_binance_token,
    require_canonical_text,
    require_identity_sequence,
    require_stablecoin_underlyings,
)
from alt_hot_scanner.utils.numeric import strict_millisecond_timestamp


@dataclass(frozen=True)
class ContractRecord:
    symbol: str | None
    base_asset: str | None
    quote_asset: str | None
    margin_asset: str | None
    contract_type: str | None
    market_family: str | None
    product_family: str | None
    underlying_type: str | None
    underlying_subtype: tuple[str, ...] | None
    is_crypto_underlying: bool | None
    is_stablecoin_underlying: bool | None
    is_leveraged_token: bool | None
    stablecoin_evidence_status: str
    stablecoin_conflict_status: str
    leveraged_evidence_status: str
    leveraged_conflict_status: str
    is_benchmark_btc: bool | None
    is_eth: bool | None
    scope_classification_status: str
    scope_classification_provenance: str | None
    exchange_info_onboard_at: pd.Timestamp | None
    exchange_info_delivery_at: pd.Timestamp | None = None
    latest_known_status: str | None = None
    metadata_acquired_at: pd.Timestamp | None = None
    metadata_source: str = "unknown"
    metadata_raw_snapshot_path: str | None = None
    metadata_raw_snapshot_sha256: str | None = None


def records_from_exchange_info(
    payload: dict,
    acquired_at: pd.Timestamp | None = None,
    *,
    raw_snapshot_path: str | None = None,
    raw_snapshot_sha256: str | None = None,
    stablecoin_underlyings: list[str] | None = None,
    reviewed_classification: list[dict] | None = None,
) -> pd.DataFrame:
    """Normalize current exchangeInfo while retaining its current-snapshot provenance."""
    acquired = acquired_at
    configured_stablecoins = (
        require_stablecoin_underlyings(stablecoin_underlyings)
        if stablecoin_underlyings is not None
        else frozenset()
    )
    reviewed_by_key: dict[tuple[str, str], dict] = {}
    for evidence in reviewed_classification or []:
        asset = require_binance_token(evidence.get("asset"), "classification asset")
        dimension = evidence.get("dimension")
        if dimension not in {"stablecoin_underlying", "leveraged_token"}:
            raise ValueError("Classification evidence has an unsupported dimension")
        if type(evidence.get("value")) is not bool:
            raise ValueError("Classification evidence value must be boolean")
        if evidence.get("evidence_status") != "accepted_reviewed":
            raise ValueError("Classification evidence must be accepted and reviewed")
        for field in ("source_type", "source_identifier", "reviewed_parser_version"):
            require_canonical_text(evidence.get(field), f"classification {field}")
        key = (asset, dimension)
        if key in reviewed_by_key:
            raise ValueError("Duplicate reviewed classification evidence")
        reviewed_by_key[key] = evidence
    records: list[dict] = []
    for item in payload["symbols"]:
        delivery_ms = item.get("deliveryDate")
        raw_subtype = item.get("underlyingSubType")
        try:
            symbol = require_archive_symbol_identity(item.get("symbol"), "symbol")
            base_asset = require_archive_symbol_identity(item.get("baseAsset"), "baseAsset")
            quote_asset = require_binance_token(item.get("quoteAsset"), "quoteAsset")
            margin_asset = require_binance_token(item.get("marginAsset"), "marginAsset")
            contract_type = require_binance_token(item.get("contractType"), "contractType")
            underlying_type = require_binance_token(item.get("underlyingType"), "underlyingType")
            underlying_subtype = require_identity_sequence(raw_subtype, "underlyingSubType")
            if symbol != f"{base_asset}{quote_asset}":
                raise IdentityValidationError("symbol must equal baseAsset plus quoteAsset")
            classification_fields_valid = True
        except IdentityValidationError:
            symbol = item.get("symbol")
            base_asset = item.get("baseAsset")
            quote_asset = item.get("quoteAsset")
            margin_asset = item.get("marginAsset")
            contract_type = item.get("contractType")
            underlying_type = item.get("underlyingType")
            underlying_subtype = None
            classification_fields_valid = False
        normalized_subtypes = (
            {subtype.upper() for subtype in underlying_subtype}
            if classification_fields_valid and underlying_subtype is not None
            else set()
        )
        metadata_stable_positive = "STABLECOIN" in normalized_subtypes
        metadata_leveraged_positive = any(
            "LEVERAGED" in subtype for subtype in normalized_subtypes
        )
        stable_review = (
            reviewed_by_key.get((base_asset, "stablecoin_underlying"))
            if type(base_asset) is str
            else None
        )
        leveraged_review = (
            reviewed_by_key.get((symbol, "leveraged_token")) if type(symbol) is str else None
        )
        stable_values = [
            value
            for value in (
                True if metadata_stable_positive else None,
                True if type(base_asset) is str and base_asset in configured_stablecoins else None,
                stable_review.get("value") if stable_review else None,
            )
            if value is not None
        ]
        leveraged_values = [
            value
            for value in (
                True if metadata_leveraged_positive else None,
                leveraged_review.get("value") if leveraged_review else None,
            )
            if value is not None
        ]
        stable_conflict = len(set(stable_values)) > 1
        leveraged_conflict = len(set(leveraged_values)) > 1
        stable_value = stable_values[0] if stable_values and not stable_conflict else None
        leveraged_value = (
            leveraged_values[0] if leveraged_values and not leveraged_conflict else None
        )
        stable_status = (
            "conflicting_affirmative_evidence"
            if stable_conflict
            else "accepted_reviewed_evidence"
            if stable_review
            else "affirmative_configured_positive_guard"
            if type(base_asset) is str and base_asset in configured_stablecoins
            else "affirmative_exchange_info_subtype"
            if metadata_stable_positive
            else "unresolved_no_affirmative_negative_evidence"
        )
        leveraged_status = (
            "conflicting_affirmative_evidence"
            if leveraged_conflict
            else "accepted_reviewed_evidence"
            if leveraged_review
            else "affirmative_exchange_info_subtype"
            if metadata_leveraged_positive
            else "unresolved_no_affirmative_negative_evidence"
        )
        onboard_ms = (
            strict_millisecond_timestamp(item.get("onboardDate"), "onboardDate")
            if item.get("onboardDate") is not None
            else None
        )
        delivery_at = None
        if delivery_ms is not None:
            delivery_at = pd.to_datetime(
                strict_millisecond_timestamp(delivery_ms, "deliveryDate"),
                unit="ms",
                utc=True,
            )
        classification_resolved = (
            classification_fields_valid
            and type(stable_value) is bool
            and type(leveraged_value) is bool
            and not stable_conflict
            and not leveraged_conflict
        )
        record = ContractRecord(
            symbol=symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            margin_asset=margin_asset,
            contract_type=contract_type,
            market_family="USDM" if classification_fields_valid else None,
            product_family="FUTURES" if classification_fields_valid else None,
            underlying_type=underlying_type,
            underlying_subtype=underlying_subtype,
            is_crypto_underlying=(
                underlying_type == "COIN" if classification_fields_valid else None
            ),
            is_stablecoin_underlying=(stable_value if classification_fields_valid else None),
            is_leveraged_token=(leveraged_value if classification_fields_valid else None),
            stablecoin_evidence_status=(
                stable_status if classification_fields_valid else "unresolved_invalid_identity"
            ),
            stablecoin_conflict_status=("conflict" if stable_conflict else "none"),
            leveraged_evidence_status=(
                leveraged_status if classification_fields_valid else "unresolved_invalid_identity"
            ),
            leveraged_conflict_status=("conflict" if leveraged_conflict else "none"),
            is_benchmark_btc=(symbol == "BTCUSDT" if classification_fields_valid else None),
            is_eth=(symbol == "ETHUSDT" if classification_fields_valid else None),
            scope_classification_status=(
                "resolved_reviewed_evidence" if classification_resolved else "unresolved"
            ),
            scope_classification_provenance=(
                "versioned_classification_evidence_and_positive_exclusion_guard"
                if classification_resolved
                else None
            ),
            exchange_info_onboard_at=(
                pd.to_datetime(onboard_ms, unit="ms", utc=True)
                if onboard_ms is not None
                else None
            ),
            exchange_info_delivery_at=delivery_at,
            latest_known_status=item.get("status"),
            metadata_acquired_at=acquired,
            metadata_source="https://fapi.binance.com/fapi/v1/exchangeInfo",
            metadata_raw_snapshot_path=raw_snapshot_path,
            metadata_raw_snapshot_sha256=raw_snapshot_sha256,
        )
        records.append(asdict(record))
    return pd.DataFrame.from_records(records)


def filter_instrument_scope(
    metadata: pd.DataFrame, stablecoin_underlyings: list[str] | None = None
) -> pd.DataFrame:
    """Apply frozen scope from explicit metadata; unresolved classification fails closed."""
    required = {
        "symbol",
        "base_asset",
        "quote_asset",
        "margin_asset",
        "contract_type",
        "market_family",
        "product_family",
        "underlying_type",
        "underlying_subtype",
        "is_crypto_underlying",
        "is_stablecoin_underlying",
        "is_leveraged_token",
        "scope_classification_status",
        "scope_classification_provenance",
    }
    missing = required - set(metadata.columns)
    if missing:
        raise ValueError(f"Instrument classification is unresolved; missing {sorted(missing)}")
    # Retain configuration validation as a legacy conflict guard, but scope admission
    # depends on captured per-contract evidence rather than list membership.
    configured_stablecoins = (
        require_stablecoin_underlyings(stablecoin_underlyings)
        if stablecoin_underlyings is not None
        else frozenset()
    )

    def valid_row(row: pd.Series) -> bool:
        try:
            symbol = require_archive_symbol_identity(row["symbol"], "symbol")
            base = require_archive_symbol_identity(row["base_asset"], "base_asset")
            quote = require_binance_token(row["quote_asset"], "quote_asset")
            require_binance_token(row["margin_asset"], "margin_asset")
            require_binance_token(row["contract_type"], "contract_type")
            require_binance_token(row["market_family"], "market_family")
            require_binance_token(row["product_family"], "product_family")
            require_binance_token(row["underlying_type"], "underlying_type")
            require_identity_sequence(row["underlying_subtype"], "underlying_subtype")
            require_canonical_text(
                row["scope_classification_provenance"],
                "scope_classification_provenance",
            )
        except IdentityValidationError:
            return False
        return (
            symbol == f"{base}{quote}"
            and isinstance(row["is_crypto_underlying"], (bool, np.bool_))
            and isinstance(row["is_stablecoin_underlying"], (bool, np.bool_))
            and isinstance(row["is_leveraged_token"], (bool, np.bool_))
            and row["scope_classification_status"] != "unresolved"
        )

    canonical_identity = metadata.apply(valid_row, axis=1)
    mask = (
        canonical_identity
        & metadata["quote_asset"].eq("USDT")
        & metadata["margin_asset"].eq("USDT")
        & metadata["contract_type"].eq("PERPETUAL")
        & metadata["market_family"].eq("USDM")
        & metadata["product_family"].eq("FUTURES")
        & metadata["is_crypto_underlying"].eq(True)
        & metadata["is_stablecoin_underlying"].eq(False)
        & metadata["is_leveraged_token"].eq(False)
        & ~metadata["base_asset"].isin(configured_stablecoins)
    )
    return metadata.loc[mask].copy()
