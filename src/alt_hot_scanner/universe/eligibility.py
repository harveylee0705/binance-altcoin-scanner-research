from __future__ import annotations

import pandas as pd

from alt_hot_scanner.identity import require_binance_token


def apply_point_in_time_eligibility(
    bars: pd.DataFrame,
    contracts: pd.DataFrame,
    *,
    minimum_age_days: int = 30,
    timestamp_col: str = "close_time",
) -> pd.DataFrame:
    """Join contract evidence and compute scanner eligibility at each completed timestamp."""
    required = {
        "symbol",
        "official_trading_start_at",
        "delisting_announcement_published_at",
        "scope_classification_status",
    }
    missing = required - set(contracts.columns)
    if missing:
        raise ValueError(f"Contract metadata is missing {sorted(missing)}")
    for source_name, values in (("bars", bars["symbol"]), ("contracts", contracts["symbol"])):
        validated_symbols: set[str] = set()
        for position, value in enumerate(values):
            if type(value) is str and value in validated_symbols:
                continue
            validated_symbols.add(
                require_binance_token(value, f"{source_name}.symbol[{position}]")
            )
    if contracts["symbol"].duplicated().any():
        raise ValueError("Contract metadata must have one row per symbol")

    result = bars.merge(contracts, on="symbol", how="left", validate="many_to_one")
    signal_time = pd.to_datetime(result[timestamp_col], utc=True)
    trading_start = pd.to_datetime(result["official_trading_start_at"], utc=True)
    age = signal_time - trading_start
    result["contract_age_days"] = age.dt.total_seconds() / 86_400
    has_listing_evidence = trading_start.notna()
    old_enough = age >= pd.Timedelta(days=minimum_age_days)
    announced = pd.to_datetime(result["delisting_announcement_published_at"], utc=True)
    before_announcement = announced.isna() | signal_time.lt(announced)
    has_market_data = result["close"].notna()
    scope_resolved = result["scope_classification_status"].ne("unresolved")
    result["is_eligible"] = (
        scope_resolved
        & has_listing_evidence
        & old_enough
        & before_announcement
        & has_market_data
    )
    result["is_eth"] = result["symbol"].eq("ETHUSDT")
    result["is_benchmark"] = result["symbol"].eq("BTCUSDT")
    return result
