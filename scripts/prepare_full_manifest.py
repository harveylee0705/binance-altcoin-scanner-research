from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from alt_hot_scanner.data.binance_public import (
    collision_resistant_run_id,
    list_archive_symbols,
    monthly_kline_key,
    write_json_exclusive,
)
from alt_hot_scanner.utils.config import load_config


def last_completed_month(now: pd.Timestamp) -> pd.Period:
    """Return the month before the current UTC calendar month."""
    timestamp = pd.Timestamp(now)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.to_period("M") - 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare, but do not execute, a full archive plan")
    parser.add_argument("--config", default="config/research_v0_1.yaml")
    parser.add_argument("--end-month", help="YYYY-MM; default is the current fully ended month")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / args.config)
    symbols = [symbol for symbol in list_archive_symbols() if symbol.endswith("USDT")]
    start = pd.Timestamp(config["data"]["start"]).tz_localize(None).to_period("M")
    now = pd.Timestamp.now(tz="UTC")
    end = pd.Period(args.end_month, freq="M") if args.end_month else last_completed_month(now)
    months = [str(period) for period in pd.period_range(start, end, freq="M")]
    payload = {
        "created_at": datetime.now(UTC).isoformat(),
        "purpose": "download_plan_only_no_market_data_downloaded",
        "source": config["data"]["archive_index_url"],
        "warning": (
            "Archive discovery includes delisted objects but suffix alone is not final instrument "
            "classification; join authoritative metadata and quarantine unresolved symbols."
        ),
        "symbols_discovered": len(symbols),
        "months": months,
        "objects": [
            monthly_kline_key(symbol, "1h", month) for symbol in symbols for month in months
        ],
    }
    run_id = collision_resistant_run_id()
    target = root / "reports" / f"full_download_plan_{run_id}.json"
    write_json_exclusive(target, payload)
    print(f"Prepared {len(payload['objects']):,} candidate object keys in {target}")
    print("No full-history market data was downloaded.")


if __name__ == "__main__":
    main()
