from __future__ import annotations

import pandas as pd

from alt_hot_scanner.features.core import add_time_series_features
from alt_hot_scanner.outcomes.labels import add_outcome_labels
from alt_hot_scanner.scanner.episodes import add_hot_episodes
from alt_hot_scanner.scanner.scoring import add_cross_sectional_scanner
from alt_hot_scanner.universe.contracts import filter_instrument_scope
from alt_hot_scanner.universe.eligibility import apply_point_in_time_eligibility


def build_vertical_slice(
    bars_4h: pd.DataFrame,
    contracts: pd.DataFrame,
    config: dict,
    *,
    allowed_splits: list[str] | None = None,
) -> pd.DataFrame:
    """Run normalized 4H data through eligibility, features, score, episodes, and labels."""
    scoped_contracts = filter_instrument_scope(
        contracts, config["universe"]["stablecoin_underlyings"]
    )
    scoped_bars = bars_4h.loc[bars_4h["symbol"].isin(scoped_contracts["symbol"])].copy()
    if scoped_bars.empty:
        raise ValueError("No bars remain after mandatory frozen instrument-scope classification")
    eligible = apply_point_in_time_eligibility(
        scoped_bars,
        scoped_contracts,
        minimum_age_days=config["universe"]["minimum_age_calendar_days"],
    )
    features = add_time_series_features(
        eligible,
        atr_period=config["features"]["atr_period"],
        liquidity_observations=config["features"]["liquidity_30d_observations_4h"],
    )
    observations = add_hot_episodes(add_cross_sectional_scanner(features))
    return add_outcome_labels(
        observations,
        config["splits"],
        allowed_splits=allowed_splits,
    )
