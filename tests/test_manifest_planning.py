from __future__ import annotations

import pandas as pd

from scripts.download_from_plan import key_fields
from scripts.prepare_full_manifest import last_completed_month


def test_default_plan_ends_at_last_fully_completed_month() -> None:
    assert last_completed_month(pd.Timestamp("2026-08-23T11:00:00Z")) == pd.Period(
        "2026-07", freq="M"
    )
    assert last_completed_month(pd.Timestamp("2026-08-01T00:00:00Z")) == pd.Period(
        "2026-07", freq="M"
    )


def test_archive_key_fields_are_auditable() -> None:
    key = "data/futures/um/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2023-06.zip"
    assert key_fields(key) == {"symbol": "ETHUSDT", "interval": "1h", "period": "2023-06"}
