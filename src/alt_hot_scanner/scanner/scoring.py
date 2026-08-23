from __future__ import annotations

import numpy as np
import pandas as pd

SCORE_COMPONENTS = [
    "rs_1d_pct",
    "rs_3d_pct",
    "rs_7d_pct",
    "vol_exp_4h_pct",
    "vol_exp_24h_pct",
]


def _rank_eligible(frame: pd.DataFrame, source: str, target: str) -> pd.DataFrame:
    result = frame.copy()
    result[target] = np.nan
    eligible = result["is_eligible"] & ~result["is_benchmark"] & result[source].notna()
    result.loc[eligible, target] = (
        result.loc[eligible].groupby("open_time")[source].rank(method="average", pct=True)
    )
    return result


def add_cross_sectional_scanner(features: pd.DataFrame) -> pd.DataFrame:
    """Rank eligible altcoins, apply frozen equal weights, and mark the top decile."""
    result = features.copy()
    mappings = {
        "return_1d": "rs_1d_pct",
        "return_3d": "rs_3d_pct",
        "return_7d": "rs_7d_pct",
        "vol_exp_4h_raw": "vol_exp_4h_pct",
        "vol_exp_24h_raw": "vol_exp_24h_pct",
        "median_30d_daily_quote_volume": "liquidity_pct",
    }
    for source, target in mappings.items():
        result = _rank_eligible(result, source, target)

    complete = result[SCORE_COMPONENTS].notna().all(axis=1)
    result["hot_score"] = np.nan
    result.loc[complete, "hot_score"] = result.loc[complete, SCORE_COMPONENTS].mean(axis=1)
    result = _rank_eligible(result, "hot_score", "hot_score_pct")
    result["hot_decile"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    ranked = result["hot_score_pct"].notna()
    result.loc[ranked, "hot_decile"] = (
        np.ceil(result.loc[ranked, "hot_score_pct"] * 10).clip(1, 10).astype(int)
    )
    # HOT is a name for membership in the primary D10 bucket, not a second
    # independently thresholded selection rule.  With average percentile ranks,
    # ties may make D10 empty or contain more than exactly 10% of a cross-section.
    result["is_hot"] = result["hot_decile"].eq(10).fillna(False)
    return result.sort_values(["open_time", "symbol"]).reset_index(drop=True)
