from __future__ import annotations

import pandas as pd

from alt_hot_scanner.identity import require_archive_symbol_identity


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
        "delisting_announcement_published_at",
        "scope_classification_status",
    }
    missing = required - set(contracts.columns)
    if missing:
        raise ValueError(f"Contract metadata is missing {sorted(missing)}")
    contracts = contracts.copy()
    if "eligibility_age_anchor_at" not in contracts.columns:
        if "official_trading_start_at" not in contracts.columns:
            raise ValueError("Contract metadata lacks an eligibility-age anchor")
        contracts["eligibility_age_anchor_at"] = contracts["official_trading_start_at"]
        contracts["eligibility_age_anchor_basis"] = contracts[
            "official_trading_start_at"
        ].map(lambda value: "exact_official_original_launch" if pd.notna(value) else "unresolved")
    for source_name, values in (("bars", bars["symbol"]), ("contracts", contracts["symbol"])):
        validated_symbols: set[str] = set()
        for position, value in enumerate(values):
            if type(value) is str and value in validated_symbols:
                continue
            validated_symbols.add(
                require_archive_symbol_identity(value, f"{source_name}.symbol[{position}]")
            )
    if contracts["symbol"].duplicated().any():
        raise ValueError("Contract metadata must have one row per symbol")

    result = bars.merge(contracts, on="symbol", how="left", validate="many_to_one")
    signal_time = pd.to_datetime(result[timestamp_col], utc=True, format="mixed")
    age_anchor = pd.to_datetime(
        result["eligibility_age_anchor_at"], utc=True, format="mixed"
    )
    age = signal_time - age_anchor
    result["contract_age_days"] = age.dt.total_seconds() / 86_400
    has_listing_evidence = age_anchor.notna() & result["eligibility_age_anchor_basis"].isin(
        [
            "exact_official_original_launch",
            "first_observed_binance_futures_trade",
            "legacy_pre_research_start_adjudicated",
        ]
    )
    old_enough = age >= pd.Timedelta(days=minimum_age_days)
    announced = pd.to_datetime(
        result["delisting_announcement_published_at"], utc=True, format="mixed"
    )
    before_announcement = announced.isna() | signal_time.lt(announced)
    has_market_data = result["close"].notna()
    scope_resolved = result["scope_classification_status"].ne("unresolved")
    scope_included = (
        result["scope_disposition"].eq("in_scope_crypto_perpetual")
        if "scope_disposition" in result.columns
        else ~result["symbol"].eq("BTCUSDT")
    )
    result["is_eligible"] = (
        scope_resolved
        & scope_included
        & has_listing_evidence
        & old_enough
        & before_announcement
        & has_market_data
    )
    result["is_eth"] = result["symbol"].eq("ETHUSDT")
    result["is_benchmark"] = result["symbol"].eq("BTCUSDT")
    return result
