from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pandas as pd


@dataclass(frozen=True)
class ContractRecord:
    symbol: str | None
    base_asset: str | None
    quote_asset: str | None
    margin_asset: str | None
    contract_type: str | None
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
        valid_subtype = (
            isinstance(raw_subtype, list)
            and bool(raw_subtype)
            and all(isinstance(value, str) and value.strip() for value in raw_subtype)
        )
        underlying_subtype = tuple(raw_subtype) if valid_subtype else None
        classification_fields_valid = (
            all(
                isinstance(item.get(field), str) and item[field].strip()
                for field in (
                    "symbol",
                    "baseAsset",
                    "quoteAsset",
                    "marginAsset",
                    "contractType",
                    "underlyingType",
                )
            )
            and valid_subtype
        )
        record = ContractRecord(
            symbol=item.get("symbol"),
            base_asset=item.get("baseAsset"),
            quote_asset=item.get("quoteAsset"),
            margin_asset=item.get("marginAsset"),
            contract_type=item.get("contractType"),
            underlying_type=item.get("underlyingType"),
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
        "underlying_type",
        "underlying_subtype",
        "is_leveraged_token",
        "classification_provenance",
    }
    missing = required - set(metadata.columns)
    if missing:
        raise ValueError(f"Instrument classification is unresolved; missing {sorted(missing)}")
    stable = set(stablecoin_underlyings)
    symbol = metadata["symbol"].astype("string")
    base = metadata["base_asset"].astype("string")
    subtype_is_explicit = metadata["underlying_subtype"].map(
        lambda value: (
            isinstance(value, (tuple, list))
            and bool(value)
            and all(isinstance(item, str) and item.strip() for item in value)
        )
    )
    provenance = metadata["classification_provenance"].astype("string")
    mask = (
        symbol.notna()
        & symbol.str.strip().ne("")
        & base.notna()
        & base.str.strip().ne("")
        & metadata["quote_asset"].eq("USDT")
        & metadata["margin_asset"].eq("USDT")
        & metadata["contract_type"].eq("PERPETUAL")
        & metadata["underlying_type"].eq("COIN")
        & ~base.isin(stable)
        & metadata["is_leveraged_token"].eq(False)
        & subtype_is_explicit
        & provenance.notna()
        & provenance.str.strip().ne("")
    )
    return metadata.loc[mask].copy()
