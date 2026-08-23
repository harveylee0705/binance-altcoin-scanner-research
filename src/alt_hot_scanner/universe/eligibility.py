from __future__ import annotations

import pandas as pd


def apply_point_in_time_eligibility(
    bars: pd.DataFrame,
    contracts: pd.DataFrame,
    *,
    minimum_age_days: int = 30,
    timestamp_col: str = "close_time",
) -> pd.DataFrame:
    """Join contract evidence and compute scanner eligibility at each completed timestamp."""
    required = {"symbol", "onboard_timestamp", "delisting_announcement_timestamp"}
    missing = required - set(contracts.columns)
    if missing:
        raise ValueError(f"Contract metadata is missing {sorted(missing)}")
    if contracts["symbol"].duplicated().any():
        raise ValueError("Contract metadata must have one row per symbol")

    result = bars.merge(contracts, on="symbol", how="left", validate="many_to_one")
    signal_time = pd.to_datetime(result[timestamp_col], utc=True)
    onboard = pd.to_datetime(result["onboard_timestamp"], utc=True)
    age = signal_time - onboard
    result["contract_age_days"] = age.dt.total_seconds() / 86_400
    has_listing_evidence = onboard.notna()
    old_enough = age >= pd.Timedelta(days=minimum_age_days)
    announced = pd.to_datetime(result["delisting_announcement_timestamp"], utc=True)
    before_announcement = announced.isna() | signal_time.lt(announced)
    has_market_data = result["close"].notna()
    result["is_eligible"] = (
        has_listing_evidence & old_enough & before_announcement & has_market_data
    )
    result["is_eth"] = result["symbol"].eq("ETHUSDT")
    result["is_benchmark"] = result["symbol"].eq("BTCUSDT")
    return result
