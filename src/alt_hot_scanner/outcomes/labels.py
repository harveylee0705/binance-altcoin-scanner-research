from __future__ import annotations

import numpy as np
import pandas as pd

from alt_hot_scanner.analysis.splits import assert_authorized_splits, horizon_fits_split

HORIZONS = {"1d": 6, "3d": 18, "7d": 42}


def _complete_future_window(group: pd.DataFrame, bars: int, split_config: dict) -> pd.Series:
    endpoint = group["open_time"].shift(-bars)
    elapsed_ok = endpoint.sub(group["open_time"]).eq(pd.Timedelta(hours=4 * bars))
    split_ok = horizon_fits_split(group["open_time"], endpoint, split_config)
    return elapsed_ok & split_ok


def _path_label(
    future_highs: np.ndarray,
    future_lows: np.ndarray,
    close: float,
    atr: float,
) -> str:
    upper = close + 2.0 * atr
    lower = close - 1.0 * atr
    for high, low in zip(future_highs, future_lows, strict=True):
        hit_upper = high >= upper
        hit_lower = low <= lower
        if hit_upper and hit_lower:
            return "ambiguous"
        if hit_upper:
            return "success"
        if hit_lower:
            return "failure"
    return "censored"


def _add_symbol_outcomes(group: pd.DataFrame, split_config: dict) -> pd.DataFrame:
    result = group.sort_values("open_time").copy()
    for label, bars in HORIZONS.items():
        complete = _complete_future_window(result, bars, split_config)
        future_close = result["close"].shift(-bars)
        result[f"fwd_return_{label}"] = future_close.div(result["close"]).sub(1.0).where(complete)
        result[f"future_bar_count_{label}"] = np.where(complete, bars, 0)

        if label in {"3d", "7d"}:
            high_matrix = np.column_stack(
                [result["high"].shift(-step).to_numpy() for step in range(1, bars + 1)]
            )
            low_matrix = np.column_stack(
                [result["low"].shift(-step).to_numpy() for step in range(1, bars + 1)]
            )
            mfe = (
                pd.DataFrame(high_matrix).max(axis=1).to_numpy() / result["close"].to_numpy() - 1.0
            )
            mae = pd.DataFrame(low_matrix).min(axis=1).to_numpy() / result["close"].to_numpy() - 1.0
            result[f"mfe_{label}"] = pd.Series(mfe, index=result.index).where(complete)
            result[f"mae_{label}"] = pd.Series(mae, index=result.index).where(complete)
            barrier = pd.Series(pd.NA, index=result.index, dtype="string")
            valid = complete & result["atr_14"].notna() & result["atr_14"].gt(0)
            for row_position in np.flatnonzero(valid.to_numpy()):
                barrier.iloc[row_position] = _path_label(
                    high_matrix[row_position],
                    low_matrix[row_position],
                    float(result["close"].iloc[row_position]),
                    float(result["atr_14"].iloc[row_position]),
                )
            result[f"barrier_{label}"] = barrier
    return result


def add_outcome_labels(
    observations: pd.DataFrame,
    split_config: dict,
    *,
    allowed_splits: list[str] | None = None,
) -> pd.DataFrame:
    """Add future-only labels while enforcing authorization and split-contained horizons."""
    assert_authorized_splits(observations["open_time"], split_config, allowed_splits)
    pieces = [
        _add_symbol_outcomes(group, split_config)
        for _, group in observations.groupby("symbol", sort=False)
    ]
    result = pd.concat(pieces, ignore_index=True)

    btc_columns = ["open_time", *[f"fwd_return_{label}" for label in HORIZONS]]
    benchmark = result.loc[result["symbol"].eq("BTCUSDT"), btc_columns].copy()
    if benchmark["open_time"].duplicated().any():
        raise ValueError("BTC benchmark must have one row per timestamp")
    benchmark = benchmark.rename(
        columns={f"fwd_return_{label}": f"btc_fwd_return_{label}" for label in HORIZONS}
    )
    result = result.merge(benchmark, on="open_time", how="left", validate="many_to_one")

    alt = result["is_eligible"] & ~result["is_benchmark"]
    for label in HORIZONS:
        result[f"btc_relative_return_{label}"] = (
            result[f"fwd_return_{label}"] - result[f"btc_fwd_return_{label}"]
        )
        medians = result.loc[alt].groupby("open_time")[f"fwd_return_{label}"].median()
        result[f"cross_sectional_excess_return_{label}"] = result[f"fwd_return_{label}"] - result[
            "open_time"
        ].map(medians)
    return result.sort_values(["open_time", "symbol"]).reset_index(drop=True)
