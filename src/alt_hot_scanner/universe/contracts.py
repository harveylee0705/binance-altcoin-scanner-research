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
        record = ContractRecord(
            symbol=item["symbol"],
            base_asset=item["baseAsset"],
            quote_asset=item["quoteAsset"],
            margin_asset=item["marginAsset"],
            contract_type=item["contractType"],
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
    """Apply the frozen instrument classification without using future liquidity/status."""
    stable = set(stablecoin_underlyings)
    leveraged_suffixes = ("UP", "DOWN", "BULL", "BEAR")
    base = metadata["base_asset"].astype(str)
    mask = (
        metadata["quote_asset"].eq("USDT")
        & metadata["margin_asset"].eq("USDT")
        & metadata["contract_type"].eq("PERPETUAL")
        & ~base.isin(stable)
        & ~base.str.endswith(leveraged_suffixes)
    )
    return metadata.loc[mask].copy()
