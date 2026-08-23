from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pandas as pd


@dataclass(frozen=True)
class ContractRecord:
    symbol: str
    base_asset: str
    quote_asset: str
    margin_asset: str
    contract_type: str
    underlying_type: str
    underlying_subtype: tuple[str, ...]
    is_leveraged_token: bool
    classification_provenance: str
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
        underlying_subtype = tuple(item.get("underlyingSubType", []))
        record = ContractRecord(
            symbol=item["symbol"],
            base_asset=item["baseAsset"],
            quote_asset=item["quoteAsset"],
            margin_asset=item["marginAsset"],
            contract_type=item["contractType"],
            underlying_type=item.get("underlyingType", ""),
            underlying_subtype=underlying_subtype,
            is_leveraged_token=any(
                "LEVERAGED" in subtype.upper() for subtype in underlying_subtype
            ),
            classification_provenance="binance_exchange_info_underlying_classification",
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
        "is_leveraged_token",
        "classification_provenance",
    }
    missing = required - set(metadata.columns)
    if missing:
        raise ValueError(f"Instrument classification is unresolved; missing {sorted(missing)}")
    stable = set(stablecoin_underlyings)
    base = metadata["base_asset"].astype(str)
    mask = (
        metadata["quote_asset"].eq("USDT")
        & metadata["margin_asset"].eq("USDT")
        & metadata["contract_type"].eq("PERPETUAL")
        & metadata["underlying_type"].eq("COIN")
        & ~base.isin(stable)
        & metadata["is_leveraged_token"].eq(False)
        & metadata["classification_provenance"].notna()
    )
    return metadata.loc[mask].copy()
