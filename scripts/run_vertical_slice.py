from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from alt_hot_scanner.data.aggregate import aggregate_1h_to_4h
from alt_hot_scanner.data.binance_public import (
    collision_resistant_run_id,
    download_verified_archive,
    fetch_exchange_info_snapshot,
    monthly_kline_key,
    write_download_manifest,
    write_json_exclusive,
)
from alt_hot_scanner.data.normalize import read_kline_zip
from alt_hot_scanner.pipeline import build_vertical_slice
from alt_hot_scanner.universe.contracts import (
    filter_instrument_scope,
    records_from_exchange_info,
)
from alt_hot_scanner.utils.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the development-only Scanner v0.1 slice")
    parser.add_argument("--config", default="config/research_v0_1.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / args.config)
    slice_config = config["vertical_slice"]
    if slice_config["permitted_split"] != "development":
        raise PermissionError("Kickoff slice must remain development-only")
    if pd.Timestamp(slice_config["analysis_end"]) > pd.Timestamp(
        config["splits"]["development"]["end"]
    ):
        raise PermissionError("Slice analysis_end exceeds development split")

    run_id = collision_resistant_run_id()
    raw_root = root / "data" / "raw"
    records = []
    quality_records = []
    normalized: list[pd.DataFrame] = []
    for symbol in slice_config["symbols"]:
        symbol_frames: list[pd.DataFrame] = []
        for month in slice_config["archive_months"]:
            key = monthly_kline_key(symbol, "1h", month)
            record = download_verified_archive(key, raw_root)
            records.append(record)
            frame = read_kline_zip(record.local_path, symbol)
            quality_records.append(
                {
                    "object_key": key,
                    "symbol": symbol,
                    "row_count": len(frame),
                    "first_open_time": frame["open_time"].min().isoformat(),
                    "last_close_time": frame["close_time"].max().isoformat(),
                    "duplicate_key_count": int(frame.duplicated(["symbol", "open_time"]).sum()),
                    "checksum_verified": record.checksum_verified,
                }
            )
            frame["source_key"] = key
            frame["source_sha256"] = record.computed_sha256
            symbol_frames.append(frame)
        symbol_data = pd.concat(symbol_frames, ignore_index=True).sort_values("open_time")
        if symbol_data.duplicated(["symbol", "open_time"]).any():
            raise ValueError(f"Duplicate source bars across archives for {symbol}")
        symbol_path = root / "data" / "interim" / "normalized_1h" / f"{symbol}.parquet"
        symbol_path.parent.mkdir(parents=True, exist_ok=True)
        symbol_data.to_parquet(symbol_path, index=False)
        normalized.append(symbol_data)

    write_download_manifest(records, raw_root / "manifests" / f"slice_{run_id}.json")
    write_json_exclusive(
        raw_root / "manifests" / f"slice_quality_{run_id}.json", quality_records
    )
    snapshot_path = raw_root / "metadata" / f"exchange_info_{run_id}.json"
    exchange_info = fetch_exchange_info_snapshot(snapshot_path)
    contracts = records_from_exchange_info(exchange_info, pd.Timestamp(datetime.now(UTC)))
    contracts = filter_instrument_scope(contracts, config["universe"]["stablecoin_underlyings"])
    selected_contracts = contracts.loc[contracts["symbol"].isin(slice_config["symbols"])].copy()
    missing_metadata = set(slice_config["symbols"]) - set(selected_contracts["symbol"])
    if missing_metadata:
        raise RuntimeError(
            f"Current official metadata missing slice symbols: {sorted(missing_metadata)}"
        )

    aggregate = aggregate_1h_to_4h(pd.concat(normalized, ignore_index=True))
    aggregate.bars.to_parquet(root / "data" / "interim" / "slice_4h.parquet", index=False)
    aggregate.rejected.to_parquet(
        root / "data" / "interim" / "slice_rejected_4h_groups.parquet", index=False
    )
    full = build_vertical_slice(
        aggregate.bars,
        selected_contracts,
        config,
        allowed_splits=["development"],
    )
    start = pd.Timestamp(slice_config["analysis_start"])
    end = pd.Timestamp(slice_config["analysis_end"])
    output = full.loc[full["open_time"].between(start, end, inclusive="both")].copy()
    output_path = root / "data" / "processed" / "vertical_slice_observations.parquet"
    output.to_parquet(output_path, index=False)

    # Audit engineering completeness only; never summarize score/outcome performance here.
    summary = {
        "run_id": run_id,
        "purpose": "engineering_vertical_slice_not_scanner_evaluation",
        "authorized_split": "development",
        "validation_or_holdout_inspected": False,
        "symbols": slice_config["symbols"],
        "raw_archives_verified": len(records),
        "normalized_1h_rows": int(sum(len(frame) for frame in normalized)),
        "completed_4h_rows": len(aggregate.bars),
        "rejected_4h_groups": len(aggregate.rejected),
        "output_rows": len(output),
        "eligible_altcoin_rows": int((output["is_eligible"] & ~output["is_benchmark"]).sum()),
        "complete_hot_score_rows": int(output["hot_score"].notna().sum()),
        "complete_3d_label_rows": int(output["fwd_return_3d"].notna().sum()),
        "output_path": str(output_path),
        "metadata_snapshot": str(snapshot_path),
    }
    report_path = root / "reports" / f"vertical_slice_audit_{run_id}.json"
    write_json_exclusive(report_path, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
