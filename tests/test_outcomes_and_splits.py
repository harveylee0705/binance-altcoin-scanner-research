from __future__ import annotations

import pandas as pd
import pytest
from conftest import make_4h

from alt_hot_scanner.analysis.splits import assert_authorized_splits
from alt_hot_scanner.outcomes.labels import _path_label, add_outcome_labels

SPLITS = {
    "development": {"start": "2020-01-01T00:00:00Z", "end": "2023-06-30T23:59:59.999999Z"},
    "validation": {"start": "2023-07-01T00:00:00Z", "end": "2024-12-31T23:59:59.999999Z"},
    "final_holdout": {"start": "2025-01-01T00:00:00Z", "end": None},
    "default_allowed_for_analysis": ["development"],
}


def _outcome_input() -> pd.DataFrame:
    pieces = []
    for symbol, increment, benchmark in (("BTCUSDT", 0.1, True), ("ETHUSDT", 0.2, False)):
        frame = make_4h(symbol, 60, "2023-05-01")
        frame["close"] = 100 + increment * pd.Series(range(60))
        frame["high"] = frame["close"] + 0.5
        frame["low"] = frame["close"] - 0.5
        frame["atr_14"] = 1.0
        frame["is_eligible"] = True
        frame["is_benchmark"] = benchmark
        pieces.append(frame)
    return pd.concat(pieces, ignore_index=True)


def test_future_returns_are_aligned_and_relative_labels_use_same_timestamp() -> None:
    source = _outcome_input()
    result = add_outcome_labels(source, SPLITS)
    eth0 = result.loc[result["symbol"].eq("ETHUSDT")].iloc[0]
    expected = (100 + 18 * 0.2) / 100 - 1
    btc_expected = (100 + 18 * 0.1) / 100 - 1
    assert eth0["fwd_return_3d"] == pytest.approx(expected)
    assert eth0["btc_relative_return_3d"] == pytest.approx(expected - btc_expected)
    assert eth0["cross_sectional_excess_return_3d"] == pytest.approx(0.0)
    assert eth0["mfe_3d"] == pytest.approx((100 + 18 * 0.2 + 0.5) / 100 - 1)


def test_barrier_path_outcomes_and_same_bar_ambiguity() -> None:
    assert _path_label([102.1], [99.5], 100.0, 1.0) == "success"
    assert _path_label([101.0], [98.9], 100.0, 1.0) == "failure"
    assert _path_label([102.1], [98.9], 100.0, 1.0) == "ambiguous"
    assert _path_label([101.0, 101.5], [99.5, 99.2], 100.0, 1.0) == "censored"


def test_split_guard_rejects_validation_by_default() -> None:
    timestamps = pd.Series([pd.Timestamp("2023-07-01T00:00:00Z")])
    with pytest.raises(PermissionError, match="validation"):
        assert_authorized_splits(timestamps, SPLITS)


def test_split_guard_fails_closed_outside_registered_coverage() -> None:
    timestamps = pd.Series([pd.Timestamp("2019-12-31T20:00:00Z")])
    with pytest.raises(PermissionError, match="outside frozen coverage"):
        assert_authorized_splits(timestamps, SPLITS)


def test_forward_label_never_crosses_split_boundary() -> None:
    source = _outcome_input()
    source["open_time"] = source.groupby("symbol")["open_time"].transform(
        lambda _: pd.date_range("2023-06-25", periods=60, freq="4h", tz="UTC")
    )
    source["close_time"] = (
        source["open_time"] + pd.Timedelta(hours=4) - pd.Timedelta(milliseconds=1)
    )
    result = add_outcome_labels(source, SPLITS, allowed_splits=["development", "validation"])
    row = result.loc[
        result["symbol"].eq("ETHUSDT")
        & result["open_time"].eq(pd.Timestamp("2023-06-30T00:00:00Z"))
    ].iloc[0]
    assert pd.isna(row["fwd_return_1d"])
