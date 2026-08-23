from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def classify_split(timestamps: pd.Series, split_config: dict) -> pd.Series:
    """Classify UTC timestamps into the frozen chronological splits."""
    ts = pd.to_datetime(timestamps, utc=True)
    result = pd.Series(pd.NA, index=timestamps.index, dtype="string")
    for name in ("development", "validation", "final_holdout"):
        bounds = split_config[name]
        start = pd.Timestamp(bounds["start"])
        end = (
            pd.Timestamp(bounds["end"])
            if bounds.get("end")
            else pd.Timestamp.max.tz_localize("UTC")
        )
        result.loc[(ts >= start) & (ts <= end)] = name
    return result


def assert_authorized_splits(
    timestamps: pd.Series,
    split_config: dict,
    allowed: Iterable[str] | None = None,
) -> None:
    """Fail closed when a caller accesses a split it did not explicitly authorize."""
    authorized = set(allowed or split_config["default_allowed_for_analysis"])
    classified = classify_split(timestamps, split_config)
    if classified.isna().any():
        raise PermissionError("Research split access denied for timestamp outside frozen coverage")
    observed = set(classified.unique())
    unauthorized = observed - authorized
    if unauthorized:
        raise PermissionError(
            f"Research split access denied for {sorted(unauthorized)}; authorized={sorted(authorized)}"
        )


def horizon_fits_split(
    signal_times: pd.Series,
    outcome_times: pd.Series,
    split_config: dict,
) -> pd.Series:
    """Return true only where signal and label endpoint belong to the same frozen split."""
    return (
        classify_split(signal_times, split_config).eq(classify_split(outcome_times, split_config))
        & classify_split(signal_times, split_config).notna()
    )
