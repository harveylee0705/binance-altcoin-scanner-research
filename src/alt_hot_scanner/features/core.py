from __future__ import annotations

import numpy as np
import pandas as pd


def wilder_atr(frame: pd.DataFrame, period: int = 14) -> pd.Series:
    """Return exact Wilder ATR seeded by a period-length simple mean of true range."""
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    close = frame["close"].to_numpy(dtype=float)
    previous_close = np.concatenate(([np.nan], close[:-1]))
    true_range = np.maximum.reduce(
        [high - low, np.abs(high - previous_close), np.abs(low - previous_close)]
    )
    true_range[0] = high[0] - low[0]
    atr = np.full(len(frame), np.nan, dtype=float)
    if len(frame) >= period:
        atr[period - 1] = np.mean(true_range[:period])
        for index in range(period, len(frame)):
            atr[index] = ((period - 1) * atr[index - 1] + true_range[index]) / period
    return pd.Series(atr, index=frame.index, name=f"atr_{period}")


def add_time_series_features(
    bars: pd.DataFrame,
    *,
    atr_period: int = 14,
    liquidity_observations: int = 180,
) -> pd.DataFrame:
    """Compute symbol-local, past-only Scanner v0.1 inputs before cross-sectional ranking."""
    pieces: list[pd.DataFrame] = []
    for _, source in bars.groupby("symbol", sort=False):
        group = source.sort_values("open_time").copy()
        group["continuity_segment"] = group["open_time"].diff().ne(pd.Timedelta(hours=4)).cumsum()
        for _, segment in group.groupby("continuity_segment", sort=False):
            pieces.append(_add_contiguous_features(segment, atr_period, liquidity_observations))
    return (
        pd.concat(pieces, ignore_index=True)
        .sort_values(["open_time", "symbol"])
        .reset_index(drop=True)
    )


def _add_contiguous_features(
    group: pd.DataFrame, atr_period: int, liquidity_observations: int
) -> pd.DataFrame:
    """Compute features inside one gap-free 4H segment so windows never bridge missing data."""
    group = group.copy()
    close = group["close"]
    for label, window in (("1d", 6), ("3d", 18), ("7d", 42)):
        group[f"return_{label}"] = close.div(close.shift(window)).sub(1.0)

    prior_4h_median = group["quote_volume"].shift(1).rolling(30, min_periods=30).median()
    group["vol_exp_4h_baseline"] = prior_4h_median
    group["vol_exp_4h_raw"] = group["quote_volume"].div(prior_4h_median.where(prior_4h_median > 0))

    current_24h = group["quote_volume"].rolling(6, min_periods=6).sum()
    prior_24h_median = current_24h.shift(1).rolling(20, min_periods=20).median()
    group["current_24h_quote_volume"] = current_24h
    group["vol_exp_24h_baseline"] = prior_24h_median
    group["vol_exp_24h_raw"] = current_24h.div(prior_24h_median.where(prior_24h_median > 0))
    group["median_30d_daily_quote_volume"] = current_24h.rolling(
        liquidity_observations, min_periods=liquidity_observations
    ).median()

    group["prev_20d_high"] = group["high"].shift(1).rolling(120, min_periods=120).max()
    previous_close = close.shift(1)
    group["true_range"] = pd.concat(
        [
            group["high"] - group["low"],
            (group["high"] - previous_close).abs(),
            (group["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    group["atr_14"] = wilder_atr(group, atr_period)
    group["distance_to_20d_high_atr"] = (group["prev_20d_high"] - close) / group["atr_14"].where(
        group["atr_14"] > 0
    )
    return add_structure_tags(group)


def add_structure_tags(group: pd.DataFrame) -> pd.DataFrame:
    """Attach deterministic descriptive structure tags; never use them in HotScore."""
    result = group.copy()
    above = result["close"] > result["prev_20d_high"]
    previously_not_above = result["close"].shift(1) <= result["prev_20d_high"].shift(1)
    result["is_fresh_breakout"] = (above & previously_not_above).fillna(False)

    trigger_levels = result["prev_20d_high"].where(result["is_fresh_breakout"])
    result["breakout_level"] = trigger_levels.ffill()
    last_trigger_position: int | None = None
    ages: list[float] = []
    for position, triggered in enumerate(result["is_fresh_breakout"].to_numpy()):
        if triggered:
            last_trigger_position = position
        ages.append(np.nan if last_trigger_position is None else position - last_trigger_position)
    result["bars_since_breakout"] = ages

    distance_above = (result["close"] - result["breakout_level"]) / result["atr_14"].where(
        result["atr_14"] > 0
    )
    approaching = result["distance_to_20d_high_atr"].between(0.0, 0.75, inclusive="both")
    early = result["bars_since_breakout"].between(1, 3, inclusive="both") & distance_above.le(2.0)
    extended = result["breakout_level"].notna() & distance_above.gt(2.0)
    result["stage"] = np.select(
        [result["is_fresh_breakout"], early, extended, approaching],
        ["TRIGGERED", "EARLY_TREND", "EXTENDED", "APPROACHING"],
        default="OTHER",
    )
    return result
