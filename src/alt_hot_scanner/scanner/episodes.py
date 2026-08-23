from __future__ import annotations

import pandas as pd


def add_hot_episodes(observations: pd.DataFrame) -> pd.DataFrame:
    """Assign one episode to each continuous HOT state, with no tuned cooldown."""
    result = observations.sort_values(["symbol", "open_time"]).copy()
    prior_hot = result.groupby("symbol", sort=False)["is_hot"].shift(1).eq(True)
    prior_time = result.groupby("symbol", sort=False)["open_time"].shift(1)
    is_consecutive = result["open_time"].sub(prior_time).eq(pd.Timedelta(hours=4))
    result["is_episode_start"] = result["is_hot"] & ~(prior_hot & is_consecutive)
    ordinal = result.groupby("symbol", sort=False)["is_episode_start"].cumsum()
    result["episode_id"] = pd.Series(pd.NA, index=result.index, dtype="string")
    hot = result["is_hot"]
    result.loc[hot, "episode_id"] = (
        result.loc[hot, "symbol"] + "-HOT-" + ordinal.loc[hot].astype(int).astype(str).str.zfill(6)
    )
    return result.sort_values(["open_time", "symbol"]).reset_index(drop=True)
