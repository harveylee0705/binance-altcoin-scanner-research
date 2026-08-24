from __future__ import annotations

import pandas as pd
import pytest
from conftest import make_4h

from alt_hot_scanner.universe.eligibility import apply_point_in_time_eligibility


def _contract(listing: str, announcement: str | None = None) -> pd.DataFrame:
    anchor = pd.Timestamp(listing)
    return pd.DataFrame(
        {
            "symbol": ["ETHUSDT"],
            "official_trading_start_at": [anchor],
            "eligibility_age_anchor_at": [anchor],
            "eligibility_age_anchor_basis": ["first_observed_binance_futures_trade"],
            "first_observed_trade_at": [anchor],
            "first_observed_trade_evidence_status": [
                "checksum_verified_official_binance_futures_trade"
            ],
            "delisting_announcement_published_at": [
                pd.Timestamp(announcement) if announcement else pd.NaT
            ],
            "scope_classification_status": ["resolved_test_fixture"],
            "latest_known_status": ["TRADING"],
        }
    )


def test_listing_and_30_full_calendar_day_rule() -> None:
    bars = make_4h("ETHUSDT", 2, "2023-01-31T00:00:00Z")
    contracts = _contract("2023-01-01T04:00:00Z")
    result = apply_point_in_time_eligibility(bars, contracts)
    # First close is 1 ms short of 30 full days; second close is safely beyond it.
    assert not bool(result.iloc[0]["is_eligible"])
    assert bool(result.iloc[1]["is_eligible"])


def test_listing_timestamp_cannot_substitute_for_verified_trade_anchor() -> None:
    bars = make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z")
    contracts = _contract("2023-01-01").drop(
        columns=[
            "eligibility_age_anchor_at",
            "eligibility_age_anchor_basis",
            "first_observed_trade_at",
            "first_observed_trade_evidence_status",
        ]
    )
    with pytest.raises(ValueError, match="missing"):
        apply_point_in_time_eligibility(bars, contracts)


def test_no_new_events_at_or_after_official_announcement() -> None:
    bars = make_4h("ETHUSDT", 2, "2023-03-01T00:00:00Z")
    contracts = _contract("2023-01-01T00:00:00Z", "2023-03-01T05:00:00Z")
    result = apply_point_in_time_eligibility(bars, contracts)
    assert bool(result.iloc[0]["is_eligible"])
    assert not bool(result.iloc[1]["is_eligible"])


def test_missing_listing_evidence_fails_closed() -> None:
    bars = make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z")
    contracts = _contract("2023-01-01").assign(
        eligibility_age_anchor_at=pd.NaT,
        first_observed_trade_at=pd.NaT,
        first_observed_trade_evidence_status="unresolved",
    )
    with pytest.raises(ValueError, match="verified first-trade"):
        apply_point_in_time_eligibility(bars, contracts)


@pytest.mark.parametrize("symbol", ["BTCUSDT ", "ethusdt", 123])
def test_benchmark_and_eth_tagging_reject_noncanonical_symbol_identity(symbol: object) -> None:
    bars = make_4h("BTCUSDT", 1, "2023-03-01T00:00:00Z")
    bars["symbol"] = symbol
    contracts = _contract("2023-01-01").assign(symbol=symbol)
    with pytest.raises(ValueError):
        apply_point_in_time_eligibility(bars, contracts)


def test_null_delisting_announcement_does_not_approximate_a_cutoff() -> None:
    bars = make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z")
    result = apply_point_in_time_eligibility(bars, _contract("2023-01-01"))
    assert bool(result.iloc[0]["is_eligible"])


def test_current_status_never_back_filters_historical_rows() -> None:
    bars = make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z")
    delisted = _contract("2023-01-01").assign(latest_known_status="DELISTED")
    result = apply_point_in_time_eligibility(bars, delisted)
    assert bool(result.iloc[0]["is_eligible"])


def test_unknown_scope_classification_fails_closed() -> None:
    bars = make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z")
    contracts = _contract("2023-01-01").assign(scope_classification_status="unresolved")
    result = apply_point_in_time_eligibility(bars, contracts)
    assert not bool(result.iloc[0]["is_eligible"])


def test_btc_benchmark_and_eth_tagging() -> None:
    bars = pd.concat(
        [
            make_4h("BTCUSDT", 1, "2023-03-01T00:00:00Z"),
            make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z"),
        ],
        ignore_index=True,
    )
    contracts = pd.concat(
        [_contract("2023-01-01").assign(symbol="BTCUSDT"), _contract("2023-01-01")],
        ignore_index=True,
    )
    result = apply_point_in_time_eligibility(bars, contracts)
    assert bool(result.loc[result["symbol"].eq("BTCUSDT"), "is_benchmark"].iloc[0])
    assert bool(result.loc[result["symbol"].eq("ETHUSDT"), "is_eth"].iloc[0])


def test_mixed_iso_precision_in_lifecycle_anchors_is_supported() -> None:
    bars = pd.concat(
        [
            make_4h("AAAUSDT", 1, "2023-03-01T00:00:00Z"),
            make_4h("BBBUSDT", 1, "2023-03-01T00:00:00Z"),
        ],
        ignore_index=True,
    )
    contracts = pd.DataFrame(
        {
            "symbol": ["AAAUSDT", "BBBUSDT"],
            "eligibility_age_anchor_at": [
                "2023-01-01T00:00:00+00:00",
                "2023-01-01T00:00:00.123000+00:00",
            ],
            "eligibility_age_anchor_basis": [
                "first_observed_binance_futures_trade",
                "first_observed_binance_futures_trade",
            ],
            "first_observed_trade_at": [
                "2023-01-01T00:00:00+00:00",
                "2023-01-01T00:00:00.123000+00:00",
            ],
            "first_observed_trade_evidence_status": [
                "checksum_verified_official_binance_futures_trade",
                "checksum_verified_official_binance_futures_trade",
            ],
            "delisting_announcement_published_at": [None, None],
            "scope_classification_status": ["resolved", "resolved"],
            "scope_disposition": [
                "in_scope_crypto_perpetual",
                "in_scope_crypto_perpetual",
            ],
        }
    )
    assert apply_point_in_time_eligibility(bars, contracts)["is_eligible"].all()


def test_relisting_intervals_block_gaps_and_reset_thirty_day_age() -> None:
    times = pd.to_datetime(
        [
            "2022-12-31T23:59:59Z",
            "2023-02-01T00:00:00Z",
            "2023-03-01T00:00:00Z",
            "2023-03-20T00:00:00Z",
            "2023-04-30T23:59:59.999Z",
            "2023-05-01T00:00:00Z",
        ],
        utc=True,
        format="mixed",
    )
    bars = pd.DataFrame({"symbol": "AAAUSDT", "close_time": times, "close": 1.0})
    intervals = [
        {
            "symbol": "AAAUSDT",
            "lifecycle_episode_id": "AAAUSDT:1",
            "age_live_anchor_at": "2023-01-01T00:00:00Z",
            "anchor_basis": "first_observed_binance_futures_trade",
            "eligibility_end_at": "2023-03-01T00:00:00Z",
            "last_trading_at": "2023-03-10T00:00:00Z",
        },
        {
            "symbol": "AAAUSDT",
            "lifecycle_episode_id": "AAAUSDT:2",
            "age_live_anchor_at": "2023-04-01T00:00:00Z",
            "anchor_basis": "first_observed_binance_futures_trade",
            "eligibility_end_at": None,
            "last_trading_at": None,
        },
    ]
    contracts = pd.DataFrame(
        {
            "symbol": ["AAAUSDT"],
            "eligibility_age_anchor_at": ["2023-01-01T00:00:00Z"],
            "eligibility_age_anchor_basis": ["first_observed_binance_futures_trade"],
            "first_observed_trade_at": ["2023-01-01T00:00:00Z"],
            "first_observed_trade_evidence_status": [
                "checksum_verified_official_binance_futures_trade"
            ],
            "delisting_announcement_published_at": [None],
            "scope_classification_status": ["resolved"],
            "scope_disposition": ["in_scope_crypto_perpetual"],
            "lifecycle_intervals": [intervals],
        }
    )
    result = apply_point_in_time_eligibility(bars, contracts)
    assert result["is_eligible"].tolist() == [False, True, False, False, False, True]
    assert result.loc[result.index[-1], "selected_lifecycle_episode_id"] == "AAAUSDT:2"
