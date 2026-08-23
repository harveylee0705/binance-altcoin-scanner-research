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
    is_leveraged_token: bool | None
    classification_provenance: str | None
    onboard_timestamp: pd.Timestamp | None
    first_valid_timestamp: pd.Timestamp | None = None
    delisting_announcement_timestamp: pd.Timestamp | None = None
    last_trading_timestamp: pd.Timestamp | None = None
    delivery_timestamp: pd.Timestamp | None = None
    status: str | None = None
    metadata_acquired_at: pd.Timestamp | None = None
    metadata_source: str = "unknown"


def records_from_exchange_info(
    payload: dict, acquired_at: pd.Timestamp | None = None
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
            is_leveraged_token=(
                any("LEVERAGED" in subtype.upper() for subtype in underlying_subtype)
                if classification_fields_valid and underlying_subtype is not None
                else None
            ),
            classification_provenance=(
                "binance_exchange_info_underlying_type_and_nonempty_subtype"
                if classification_fields_valid
                else None
            ),
            onboard_timestamp=pd.to_datetime(item.get("onboardDate"), unit="ms", utc=True),
            delivery_timestamp=(
                pd.to_datetime(delivery_ms, unit="ms", utc=True) if delivery_ms else None
            ),
            status=item.get("status"),
            metadata_acquired_at=acquired,
            metadata_source="https://fapi.binance.com/fapi/v1/exchangeInfo",
        )
        records.append(asdict(record))
    return pd.DataFrame.from_records(records)


def filter_instrument_scope(
    metadata: pd.DataFrame, stablecoin_underlyings: list[str]
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
        "is_leveraged_token",
        "classification_provenance",
    }
    missing = required - set(metadata.columns)
    if missing:
        raise ValueError(f"Instrument classification is unresolved; missing {sorted(missing)}")
    stable = require_stablecoin_underlyings(stablecoin_underlyings)

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
            require_canonical_text(row["classification_provenance"], "classification_provenance")
        except IdentityValidationError:
            return False
        return symbol == f"{base}{quote}" and type(row["is_leveraged_token"]) is bool

    canonical_identity = metadata.apply(valid_row, axis=1)
    base = metadata["base_asset"]
    mask = (
        canonical_identity
        & metadata["quote_asset"].eq("USDT")
        & metadata["margin_asset"].eq("USDT")
        & metadata["contract_type"].eq("PERPETUAL")
        & metadata["market_family"].eq("USDM")
        & metadata["product_family"].eq("FUTURES")
        & metadata["underlying_type"].eq("COIN")
        & ~base.isin(stable)
        & metadata["is_leveraged_token"].eq(False)
    )
    return metadata.loc[mask].copy()
