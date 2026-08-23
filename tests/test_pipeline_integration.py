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
                "base_asset": symbol.removesuffix("USDT"),
                "quote_asset": "USDT",
                "margin_asset": "USDT",
                "contract_type": "PERPETUAL",
                "market_family": "USDM",
                "product_family": "FUTURES",
                "underlying_type": "COIN",
                "underlying_subtype": ("synthetic-crypto",),
                "is_crypto_underlying": True,
                "is_stablecoin_underlying": False,
                "is_leveraged_token": False,
                "scope_classification_status": "resolved_synthetic_fixture",
                "scope_classification_provenance": "synthetic_test_fixture",
                "official_trading_start_at": pd.Timestamp("2020-01-01T00:00:00Z"),
                "delisting_announcement_published_at": pd.NaT,
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
