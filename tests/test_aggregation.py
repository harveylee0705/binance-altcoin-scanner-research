from __future__ import annotations

import pandas as pd
import pytest
from conftest import make_1h

from alt_hot_scanner.data.aggregate import aggregate_1h_to_4h
from alt_hot_scanner.data.normalize import validate_normalized_1h


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


def test_non_utc_normalized_bars_are_rejected() -> None:
    hourly = make_1h("ETHUSDT", 4)
    hourly["open_time"] = hourly["open_time"].dt.tz_convert("Asia/Bangkok")
    hourly["close_time"] = hourly["close_time"].dt.tz_convert("Asia/Bangkok")
    with pytest.raises(ValueError, match="open_time must use the UTC timezone"):
        validate_normalized_1h(hourly)


def test_malformed_hour_close_is_rejected() -> None:
    hourly = make_1h("ETHUSDT", 4)
    hourly["close_time"] = pd.Timestamp("2030-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="close_time must equal"):
        validate_normalized_1h(hourly)
