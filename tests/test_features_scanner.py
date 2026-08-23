from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from conftest import make_4h

from alt_hot_scanner.features.core import add_time_series_features, wilder_atr
from alt_hot_scanner.scanner.episodes import add_hot_episodes
from alt_hot_scanner.scanner.scoring import SCORE_COMPONENTS, add_cross_sectional_scanner


def test_return_windows_and_past_only_rolling_baselines() -> None:
    bars = make_4h("ETHUSDT", 200)
    featured = add_time_series_features(bars)
    assert featured.loc[42, "return_7d"] == pytest.approx(
        bars.loc[42, "close"] / bars.loc[0, "close"] - 1
    )
    assert featured.loc[30, "vol_exp_4h_baseline"] == pytest.approx(
        bars.loc[:29, "quote_volume"].median()
    )
    expected_24h = bars["quote_volume"].rolling(6).sum()
    assert featured.loc[25, "vol_exp_24h_baseline"] == pytest.approx(
        expected_24h.loc[5:24].median()
    )

    changed = bars.copy()
    changed.loc[30, "quote_volume"] = 1e12
    changed_feature = add_time_series_features(changed)
    assert changed_feature.loc[30, "vol_exp_4h_baseline"] == featured.loc[30, "vol_exp_4h_baseline"]


def test_previous_20d_high_excludes_current_bar() -> None:
    bars = make_4h("ETHUSDT", 125)
    bars.loc[120, "high"] = 9999.0
    featured = add_time_series_features(bars)
    assert featured.loc[120, "prev_20d_high"] == bars.loc[:119, "high"].max()
    assert featured.loc[121, "prev_20d_high"] == 9999.0


def test_feature_windows_never_bridge_a_missing_4h_boundary() -> None:
    bars = make_4h("ETHUSDT", 80).drop(index=40).reset_index(drop=True)
    featured = add_time_series_features(bars)
    after_gap = featured.loc[featured["open_time"].eq(pd.Timestamp("2023-01-07T20:00:00Z"))].iloc[0]
    assert pd.isna(after_gap["return_1d"])
    assert pd.isna(after_gap["vol_exp_4h_baseline"])
    assert pd.isna(after_gap["atr_14"])


def test_wilder_atr_seed_and_recursion() -> None:
    bars = make_4h("ETHUSDT", 16)
    bars[["open", "close"]] = 100.0
    bars["high"] = 101.0
    bars["low"] = 99.0
    atr = wilder_atr(bars, 14)
    assert atr.iloc[:13].isna().all()
    assert atr.iloc[13] == pytest.approx(2.0)
    assert atr.iloc[15] == pytest.approx(2.0)


def test_percentiles_and_hot_score_are_equal_weighted() -> None:
    time = pd.Timestamp("2023-01-01T00:00:00Z")
    frame = pd.DataFrame(
        {
            "symbol": ["AAAUSDT", "BBBUSDT", "BTCUSDT"],
            "open_time": [time] * 3,
            "is_eligible": [True] * 3,
            "is_benchmark": [False, False, True],
            "return_1d": [1.0, 2.0, 100.0],
            "return_3d": [2.0, 1.0, 100.0],
            "return_7d": [1.0, 2.0, 100.0],
            "vol_exp_4h_raw": [1.0, 2.0, 100.0],
            "vol_exp_24h_raw": [1.0, 2.0, 100.0],
            "median_30d_daily_quote_volume": [1.0, 2.0, 100.0],
        }
    )
    result = add_cross_sectional_scanner(frame)
    aaa = result.loc[result["symbol"].eq("AAAUSDT")].iloc[0]
    bbb = result.loc[result["symbol"].eq("BBBUSDT")].iloc[0]
    assert aaa["rs_1d_pct"] == 0.5
    assert bbb["rs_1d_pct"] == 1.0
    assert aaa["hot_score"] == pytest.approx(np.mean([aaa[name] for name in SCORE_COMPONENTS]))
    assert bbb["hot_score"] == pytest.approx(np.mean([bbb[name] for name in SCORE_COMPONENTS]))
    assert pd.isna(result.loc[result["symbol"].eq("BTCUSDT"), "hot_score"]).all()


def test_hot_episode_created_only_on_state_entry() -> None:
    times = pd.date_range("2023-01-01", periods=7, freq="4h", tz="UTC")
    observations = pd.DataFrame(
        {
            "symbol": ["ETHUSDT"] * 7,
            "open_time": times,
            "is_hot": [False, True, True, False, True, True, False],
        }
    )
    result = add_hot_episodes(observations)
    assert result["is_episode_start"].tolist() == [False, True, False, False, True, False, False]
    ids = result["episode_id"].dropna().tolist()
    assert ids == [
        "ETHUSDT-HOT-000001",
        "ETHUSDT-HOT-000001",
        "ETHUSDT-HOT-000002",
        "ETHUSDT-HOT-000002",
    ]


def test_hot_episode_restarts_after_missing_scanner_timestamp() -> None:
    observations = pd.DataFrame(
        {
            "symbol": ["ETHUSDT"] * 3,
            "open_time": pd.to_datetime(
                [
                    "2023-01-01T00:00:00Z",
                    "2023-01-01T04:00:00Z",
                    "2023-01-01T12:00:00Z",
                ]
            ),
            "is_hot": [True, True, True],
        }
    )
    result = add_hot_episodes(observations)
    assert result["is_episode_start"].tolist() == [True, False, True]
