from __future__ import annotations

import pandas as pd
from conftest import make_1h

from alt_hot_scanner.data.aggregate import aggregate_1h_to_4h


def test_1h_to_4h_uses_documented_utc_boundaries() -> None:
    hourly = make_1h("ETHUSDT", 8)
    result = aggregate_1h_to_4h(hourly)
    assert result.rejected.empty
    assert result.bars["open_time"].tolist() == [
        pd.Timestamp("2023-01-01T00:00:00Z"),
        pd.Timestamp("2023-01-01T04:00:00Z"),
    ]
    first = result.bars.iloc[0]
    assert first["open"] == hourly.iloc[0]["open"]
    assert first["close"] == hourly.iloc[3]["close"]
    assert first["high"] == hourly.iloc[:4]["high"].max()
    assert first["quote_volume"] == hourly.iloc[:4]["quote_volume"].sum()
    assert first["source_hour_count"] == 4


def test_incomplete_4h_group_is_rejected_not_filled() -> None:
    hourly = make_1h("ETHUSDT", 8).drop(index=6)
    result = aggregate_1h_to_4h(hourly)
    assert len(result.bars) == 1
    assert len(result.rejected) == 1
    assert result.rejected.iloc[0]["offsets"] == (0, 1, 3)
