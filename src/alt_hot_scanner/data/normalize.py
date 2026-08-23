from __future__ import annotations

import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def normalize_kline_frame(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Normalize a Binance 12-column kline frame and enforce its data contract."""
    if raw.shape[1] != len(KLINE_COLUMNS):
        raise ValueError(f"Expected 12 Binance kline columns, found {raw.shape[1]}")
    frame = raw.copy()
    frame.columns = KLINE_COLUMNS
    if str(frame.iloc[0, 0]).lower() in {"open_time", "open time"}:
        frame = frame.iloc[1:].copy()

    numeric = [
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
    frame["trade_count"] = pd.to_numeric(frame["trade_count"], errors="raise").astype("int64")
    for column in ("open_time", "close_time"):
        values = pd.to_numeric(frame[column], errors="raise").astype("int64")
        # USD-M archive timestamps are milliseconds. This also rejects accidental microseconds.
        if values.abs().max() >= 10**14:
            raise ValueError(f"Unexpected non-millisecond timestamp in {column}")
        frame[column] = pd.to_datetime(values, unit="ms", utc=True)
    frame.insert(0, "symbol", symbol)
    frame = frame.drop(columns="ignore").sort_values("open_time").reset_index(drop=True)
    validate_normalized_1h(frame)
    return frame


def read_kline_zip(path: str | Path, symbol: str) -> pd.DataFrame:
    """Read the sole CSV member from an immutable Binance archive ZIP."""
    with zipfile.ZipFile(path) as archive:
        csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_members) != 1:
            raise ValueError(f"Expected one CSV member, found {csv_members}")
        payload = archive.read(csv_members[0])
    return normalize_kline_frame(pd.read_csv(io.BytesIO(payload), header=None), symbol)


def validate_normalized_1h(frame: pd.DataFrame) -> None:
    required = {"symbol", *KLINE_COLUMNS[:-1]}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing normalized fields: {sorted(missing)}")
    if frame.duplicated(["symbol", "open_time"]).any():
        raise ValueError("Duplicate symbol/open_time bars")
    if not frame["open_time"].dt.tz:
        raise ValueError("open_time must be timezone-aware UTC")
    aligned = (
        frame["open_time"].dt.minute.eq(0)
        & frame["open_time"].dt.second.eq(0)
        & frame["open_time"].dt.microsecond.eq(0)
    )
    if not aligned.all():
        raise ValueError("1H opens must align to exact UTC hours")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC prices must be positive")
    high_floor = frame[["open", "low", "close"]].max(axis=1)
    low_ceiling = frame[["open", "high", "close"]].min(axis=1)
    if (frame["high"] < high_floor).any() or (frame["low"] > low_ceiling).any():
        raise ValueError("Invalid OHLC relationship")
    if (frame[["base_volume", "quote_volume", "trade_count"]] < 0).any().any():
        raise ValueError("Volumes and trade_count must be nonnegative")
    if not np.isfinite(frame[["open", "high", "low", "close", "quote_volume"]]).all().all():
        raise ValueError("Non-finite required market value")
