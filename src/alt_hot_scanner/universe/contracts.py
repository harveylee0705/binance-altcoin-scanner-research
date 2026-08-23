from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pandas as pd

from alt_hot_scanner.identity import (
    IdentityValidationError,
    require_binance_token,
    require_canonical_text,
    require_identity_sequence,
    require_stablecoin_underlyings,
)


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
) -> pd.DataFrame:
    """Normalize current exchangeInfo while retaining its current-snapshot provenance."""
    acquired = acquired_at or pd.Timestamp(datetime.now(UTC))
    records: list[dict] = []
    for item in payload["symbols"]:
        delivery_ms = item.get("deliveryDate")
        raw_subtype = item.get("underlyingSubType")
        try:
            symbol = require_binance_token(item.get("symbol"), "symbol")
            base_asset = require_binance_token(item.get("baseAsset"), "baseAsset")
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
            is_stablecoin_underlying=(
                "STABLECOIN" in normalized_subtypes if classification_fields_valid else None
            ),
            is_leveraged_token=(
                any("LEVERAGED" in subtype for subtype in normalized_subtypes)
                if classification_fields_valid
                else None
            ),
            is_benchmark_btc=(symbol == "BTCUSDT" if classification_fields_valid else None),
            is_eth=(symbol == "ETHUSDT" if classification_fields_valid else None),
            scope_classification_status=(
                "resolved_current_exchange_info" if classification_fields_valid else "unresolved"
            ),
            scope_classification_provenance=(
                "binance_exchange_info_underlying_type_and_nonempty_subtype"
                if classification_fields_valid
                else None
            ),
            exchange_info_onboard_at=pd.to_datetime(
                item.get("onboardDate"), unit="ms", utc=True
            ),
            exchange_info_delivery_at=(
                pd.to_datetime(delivery_ms, unit="ms", utc=True) if delivery_ms else None
            ),
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
    if stablecoin_underlyings is not None:
        require_stablecoin_underlyings(stablecoin_underlyings)

    def valid_row(row: pd.Series) -> bool:
        try:
            symbol = require_binance_token(row["symbol"], "symbol")
            base = require_binance_token(row["base_asset"], "base_asset")
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
            and type(row["is_crypto_underlying"]) is bool
            and type(row["is_stablecoin_underlying"]) is bool
            and type(row["is_leveraged_token"]) is bool
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
    )
    return metadata.loc[mask].copy()
