from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alt_hot_scanner.data.normalize import validate_normalized_1h


@dataclass(frozen=True)
class AggregationResult:
    bars: pd.DataFrame
    rejected: pd.DataFrame


def aggregate_1h_to_4h(frame: pd.DataFrame) -> AggregationResult:
    """Build strict UTC 4H bars and return rejected group diagnostics separately."""
    validate_normalized_1h(frame)
    data = frame.sort_values(["symbol", "open_time"]).copy()
    data["four_hour_open"] = data["open_time"].dt.floor("4h")
    data["hour_offset"] = (
        (data["open_time"] - data["four_hour_open"]).dt.total_seconds() // 3600
    ).astype(int)

    keys = ["symbol", "four_hour_open"]
    diagnostics = data.groupby(keys, sort=True).agg(
        source_hour_count=("open_time", "size"),
        unique_hour_count=("open_time", "nunique"),
        offsets=("hour_offset", lambda values: tuple(sorted(values))),
    )
    valid = (
        diagnostics["source_hour_count"].eq(4)
        & diagnostics["unique_hour_count"].eq(4)
        & diagnostics["offsets"].map(lambda value: value == (0, 1, 2, 3))
    )
    valid_keys = diagnostics.loc[valid].reset_index()[keys]
    accepted = data.merge(valid_keys, on=keys, how="inner")
    bars = (
        accepted.groupby(keys, sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            base_volume=("base_volume", "sum"),
            quote_volume=("quote_volume", "sum"),
            trade_count=("trade_count", "sum"),
            close_time=("close_time", "last"),
            source_hour_count=("open_time", "size"),
        )
        .reset_index()
        .rename(columns={"four_hour_open": "open_time"})
    )
    rejected = diagnostics.loc[~valid].reset_index().rename(columns={"four_hour_open": "open_time"})
    return AggregationResult(bars=bars, rejected=rejected)
