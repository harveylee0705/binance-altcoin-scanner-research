from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from alt_hot_scanner.data.aggregate import aggregate_1h_to_4h
from alt_hot_scanner.data.binance_public import (
    collision_resistant_run_id,
    validate_archive_object_key,
    validate_manifest_archive,
    write_json_exclusive,
)
from alt_hot_scanner.data.normalize import read_kline_zip


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize and aggregate verified archives one symbol at a time"
    )
    parser.add_argument("--attempt-manifest", required=True)
    parser.add_argument("--symbol-limit", type=int, help="Optional bounded smoke-run symbol count")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    raw_root = root / "data" / "raw"
    attempts = json.loads((root / args.attempt_manifest).read_text(encoding="utf-8"))
    if type(attempts) is not list or any(type(item) is not dict for item in attempts):
        raise ValueError("Attempt manifest must be a list of objects")
    allowed_statuses = {"verified", "missing", "failed"}
    if any(item.get("status") not in allowed_statuses for item in attempts):
        raise ValueError("Attempt manifest contains an invalid status")
    failures = [item for item in attempts if item["status"] == "failed"]
    if failures:
        raise RuntimeError(
            "Attempt manifest contains failures; resolve or explicitly create a clean verified manifest"
        )
    by_symbol: dict[str, list[dict]] = {}
    for item in attempts:
        if item["status"] != "verified":
            continue
        identity = validate_archive_object_key(item.get("object_key"))
        by_symbol.setdefault(identity.symbol, []).append(item)
    symbols = sorted(by_symbol)[: args.symbol_limit]

    quality: list[dict] = []
    for symbol in symbols:
        frames = []
        for item in sorted(by_symbol[symbol], key=lambda row: row["object_key"]):
            verified = validate_manifest_archive(item, raw_root)
            frame = read_kline_zip(verified.local_path, verified.identity.symbol)
            frame["source_key"] = verified.identity.object_key
            frame["source_sha256"] = verified.sha256
            frames.append(frame)
        hourly = pd.concat(frames, ignore_index=True).sort_values("open_time")
        if hourly.duplicated(["symbol", "open_time"]).any():
            raise ValueError(f"Duplicate archive bars for {symbol}")
        aggregate = aggregate_1h_to_4h(hourly)
        output = root / "data" / "interim" / "bars_4h" / f"symbol={symbol}" / "part.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        aggregate.bars.to_parquet(output, index=False)
        quality.append(
            {
                "symbol": symbol,
                "source_archives": len(frames),
                "normalized_1h_rows": len(hourly),
                "completed_4h_rows": len(aggregate.bars),
                "rejected_4h_groups": len(aggregate.rejected),
                "first_open_time": hourly["open_time"].min().isoformat(),
                "last_close_time": hourly["close_time"].max().isoformat(),
            }
        )
    run_id = collision_resistant_run_id()
    target = root / "reports" / f"full_processing_quality_{run_id}.json"
    write_json_exclusive(target, quality)
    print(f"Processed {len(quality):,} symbols independently; quality report: {target}")


if __name__ == "__main__":
    main()
