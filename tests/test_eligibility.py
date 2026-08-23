from __future__ import annotations

import pandas as pd
import pytest
from conftest import make_4h

from alt_hot_scanner.universe.eligibility import apply_point_in_time_eligibility


def _contract(onboard: str, announcement: str | None = None) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["ETHUSDT"],
            "onboard_timestamp": [pd.Timestamp(onboard)],
            "delisting_announcement_timestamp": [
                pd.Timestamp(announcement) if announcement else pd.NaT
            ],
        }
    )


def test_listing_and_30_full_calendar_day_rule() -> None:
    bars = make_4h("ETHUSDT", 2, "2023-01-31T00:00:00Z")
    contracts = _contract("2023-01-01T04:00:00Z")
    result = apply_point_in_time_eligibility(bars, contracts)
    # First close is 1 ms short of 30 full days; second close is safely beyond it.
    assert not bool(result.iloc[0]["is_eligible"])
    assert bool(result.iloc[1]["is_eligible"])


def test_no_new_events_at_or_after_official_announcement() -> None:
    bars = make_4h("ETHUSDT", 2, "2023-03-01T00:00:00Z")
    contracts = _contract("2023-01-01T00:00:00Z", "2023-03-01T05:00:00Z")
    result = apply_point_in_time_eligibility(bars, contracts)
    assert bool(result.iloc[0]["is_eligible"])
    assert not bool(result.iloc[1]["is_eligible"])


def test_missing_listing_evidence_fails_closed() -> None:
    bars = make_4h("ETHUSDT", 1, "2023-03-01T00:00:00Z")
    result = apply_point_in_time_eligibility(
        bars, _contract("2023-01-01").assign(onboard_timestamp=pd.NaT)
    )
    assert not bool(result.iloc[0]["is_eligible"])


@pytest.mark.parametrize("symbol", ["BTCUSDT ", "ethusdt", 123])
def test_benchmark_and_eth_tagging_reject_noncanonical_symbol_identity(symbol: object) -> None:
    bars = make_4h("BTCUSDT", 1, "2023-03-01T00:00:00Z")
    bars["symbol"] = symbol
    contracts = _contract("2023-01-01").assign(symbol=symbol)
    with pytest.raises(ValueError):
        apply_point_in_time_eligibility(bars, contracts)
