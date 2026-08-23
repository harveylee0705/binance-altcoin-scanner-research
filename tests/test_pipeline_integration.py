from __future__ import annotations

import pandas as pd
from conftest import make_4h

from alt_hot_scanner.pipeline import build_vertical_slice
from alt_hot_scanner.utils.config import load_config


def test_synthetic_end_to_end_vertical_slice() -> None:
    config = load_config("config/research_v0_1.yaml")
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT"]
    bars = []
    contracts = []
    for offset, symbol in enumerate(symbols):
        frame = make_4h(symbol, 250, "2023-02-01")
        frame["close"] = frame["close"] * (1 + offset * 0.0002 * pd.Series(range(250)))
        frame["open"] = frame["close"]
        frame["high"] = frame["close"] * 1.01
        frame["low"] = frame["close"] * 0.99
        frame["quote_volume"] = frame["quote_volume"] * (offset + 1)
        bars.append(frame)
        contracts.append(
            {
                "symbol": symbol,
                "onboard_timestamp": pd.Timestamp("2020-01-01T00:00:00Z"),
                "delisting_announcement_timestamp": pd.NaT,
            }
        )
    result = build_vertical_slice(
        pd.concat(bars, ignore_index=True),
        pd.DataFrame(contracts),
        config,
        allowed_splits=["development"],
    )
    expected = {
        "is_eligible",
        "rs_3d_pct",
        "vol_exp_24h_pct",
        "hot_score",
        "stage",
        "episode_id",
        "fwd_return_3d",
        "btc_relative_return_3d",
        "cross_sectional_excess_return_3d",
        "barrier_7d",
    }
    assert expected.issubset(result.columns)
    alt = result.loc[~result["is_benchmark"]]
    assert alt["is_eligible"].all()
    assert alt["hot_score"].notna().any()
    assert alt["fwd_return_7d"].notna().any()
