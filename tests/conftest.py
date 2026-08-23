from __future__ import annotations

import numpy as np
import pandas as pd


def make_1h(symbol: str, periods: int, start: str = "2023-01-01") -> pd.DataFrame:
    opens = pd.date_range(start, periods=periods, freq="1h", tz="UTC")
    price = 100.0 + np.arange(periods) * 0.1
    return pd.DataFrame(
        {
            "symbol": symbol,
            "open_time": opens,
            "open": price,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price + 0.2,
            "base_volume": np.ones(periods),
            "close_time": opens + pd.Timedelta(hours=1) - pd.Timedelta(milliseconds=1),
            "quote_volume": np.arange(periods, dtype=float) + 10.0,
            "trade_count": np.ones(periods, dtype="int64"),
            "taker_buy_base_volume": np.full(periods, 0.5),
            "taker_buy_quote_volume": np.full(periods, 5.0),
        }
    )


def make_4h(symbol: str, periods: int, start: str = "2023-01-01") -> pd.DataFrame:
    opens = pd.date_range(start, periods=periods, freq="4h", tz="UTC")
    price = 100.0 + np.arange(periods) * 0.1
    return pd.DataFrame(
        {
            "symbol": symbol,
            "open_time": opens,
            "open": price,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price + 0.2,
            "base_volume": np.ones(periods),
            "quote_volume": np.arange(periods, dtype=float) + 10.0,
            "trade_count": np.ones(periods, dtype="int64"),
            "close_time": opens + pd.Timedelta(hours=4) - pd.Timedelta(milliseconds=1),
            "source_hour_count": np.full(periods, 4),
        }
    )
