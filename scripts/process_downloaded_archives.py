from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from alt_hot_scanner.data.aggregate import aggregate_1h_to_4h
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
    attempts = json.loads((root / args.attempt_manifest).read_text(encoding="utf-8"))
    failures = [item for item in attempts if item["status"] == "failed"]
    if failures:
        raise RuntimeError(
            "Attempt manifest contains failures; resolve or explicitly create a clean verified manifest"
        )
    by_symbol: dict[str, list[dict]] = {}
    for item in attempts:
        if item["status"] != "verified":
            continue
        parts = item["object_key"].split("/")
        symbol = parts[5]
        by_symbol.setdefault(symbol, []).append(item)
    symbols = sorted(by_symbol)[: args.symbol_limit]

    quality: list[dict] = []
    for symbol in symbols:
        frames = []
        for item in sorted(by_symbol[symbol], key=lambda row: row["object_key"]):
            frame = read_kline_zip(item["local_path"], symbol)
            frame["source_key"] = item["object_key"]
            frame["source_sha256"] = item["computed_sha256"]
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
    target = root / "reports" / "full_processing_quality.json"
    target.write_text(json.dumps(quality, indent=2), encoding="utf-8")
    print(f"Processed {len(quality):,} symbols independently; quality report: {target}")


if __name__ == "__main__":
    main()
