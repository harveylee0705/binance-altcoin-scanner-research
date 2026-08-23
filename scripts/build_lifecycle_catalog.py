from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.announcements import acquire_announcement_corpus
from alt_hot_scanner.data.binance_public import (
    INDEX_HOST,
    collision_resistant_run_id,
    discover_archive_months,
    discover_archive_symbol_candidates,
    fetch_exchange_info_snapshot,
    sha256_file,
    write_bytes_exclusive,
    write_json_exclusive,
)
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog, catalog_coverage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build lifecycle evidence only; never download full OHLCV or evaluate Scanner v0.1"
    )
    parser.add_argument(
        "--resume-run",
        help="Resume a run ID that reached the archive checkpoint; evidence is never replaced",
    )
    return parser.parse_args()


def _json_bytes(records: list[dict[str, Any]]) -> bytes:
    return json.dumps(records, indent=2, default=str, sort_keys=True).encode("utf-8")


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    run_id = args.resume_run or collision_resistant_run_id()
    raw_root = root / "data" / "raw" / "lifecycle" / run_id
    report_root = root / "reports" / "lifecycle" / run_id
    if report_root.exists():
        report_root = root / "reports" / "lifecycle" / (
            f"{run_id}_catalog_{collision_resistant_run_id()}"
        )
    discovered_at = datetime.now(UTC)
    archive_raw_paths: dict[str, list[str]] = {}
    archive_raw_hashes: dict[str, list[str]] = {}

    def observer(label: str):
        def save(page: int, url: str, payload: bytes) -> None:
            digest = hashlib.sha256(payload).hexdigest()
            path = raw_root / "archive_index" / f"{label}_page_{page:03d}_{digest[:16]}.xml"
            write_bytes_exclusive(path, payload)
            archive_raw_paths.setdefault(label, []).append(str(path.resolve()))
            archive_raw_hashes.setdefault(label, []).append(digest)

        return save

    checkpoint_path = raw_root / "archive_checkpoint.json"
    if args.resume_run:
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Archive checkpoint not found: {checkpoint_path}")
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        symbols = checkpoint["canonical_symbols"]
        quarantined_prefixes = checkpoint["quarantined_prefixes"]
        symbol_audit = checkpoint["symbol_audit"]
        archive_frame = pd.DataFrame(checkpoint["archive_observations"])
    else:
        symbol_discovery = discover_archive_symbol_candidates(
            page_observer=observer("symbols")
        )
        symbols = list(symbol_discovery.symbols)
        quarantined_prefixes = list(symbol_discovery.quarantined_prefixes)
        symbol_audit = asdict(symbol_discovery.audit)
        observations: list[dict[str, Any]] = []
        for position, symbol in enumerate(symbols, start=1):
            observation = asdict(
                discover_archive_months(
                    symbol,
                    discovered_at=discovered_at,
                    page_observer=observer(symbol),
                )
            )
            observation["archive_raw_snapshot_paths"] = archive_raw_paths[symbol]
            observation["archive_raw_snapshot_sha256s"] = archive_raw_hashes[symbol]
            observations.append(observation)
            if position % 100 == 0:
                print(f"Archive lifecycle bounds: {position}/{len(symbols)} symbols", flush=True)
        archive_frame = pd.DataFrame.from_records(observations)
        write_json_exclusive(
            checkpoint_path,
            {
                "canonical_symbols": symbols,
                "quarantined_prefixes": quarantined_prefixes,
                "symbol_audit": symbol_audit,
                "archive_observations": archive_frame.to_dict("records"),
            },
        )

    exchange_path = raw_root / "exchange_info.json"
    if exchange_path.exists():
        exchange_payload = json.loads(exchange_path.read_text(encoding="utf-8"))
    else:
        exchange_payload = fetch_exchange_info_snapshot(exchange_path)
    exchange_sha256, _ = sha256_file(exchange_path)
    exchange_records = records_from_exchange_info(
        exchange_payload,
        pd.Timestamp(datetime.now(UTC)),
        raw_snapshot_path=str(exchange_path.resolve()),
        raw_snapshot_sha256=exchange_sha256,
    )

    announcement_frame, announcement_audit = acquire_announcement_corpus(
        raw_root / "announcements", set(symbols)
    )
    catalog = build_lifecycle_catalog(archive_frame, exchange_records, announcement_frame)
    coverage = catalog_coverage(catalog)
    noncanonical_usdt = sum(symbol.endswith("USDT") for symbol in quarantined_prefixes)
    coverage["canonical_archive_symbols"] = coverage["total_archive_discovered_symbols"]
    coverage["total_archive_discovered_symbols"] = symbol_audit["unique_prefix_count"]
    coverage["total_usdt_candidate_symbols"] += noncanonical_usdt
    coverage["archive_only_symbols"] += noncanonical_usdt
    coverage["unresolved_classification_cases"] += noncanonical_usdt
    coverage["unresolved_lifecycle_cases"] += noncanonical_usdt
    coverage["currently_quarantined"] += noncanonical_usdt
    coverage["unresolved_categories"]["noncanonical_archive_identities"] = len(
        quarantined_prefixes
    )
    coverage["unresolved_categories"]["noncanonical_usdt_candidates"] = noncanonical_usdt
    coverage["segments"]["delisted_or_archive_only"]["symbols"] += noncanonical_usdt
    coverage["segments"]["delisted_or_archive_only"]["quarantined"] += (
        noncanonical_usdt
    )
    coverage["archive_index"] = {
        **symbol_audit,
        "raw_symbol_index_snapshots": [
            {
                "path": str(path.resolve()),
                "sha256": sha256_file(path)[0],
            }
            for path in sorted((raw_root / "archive_index").glob("symbols_page_*.xml"))
        ],
        "total_discovered_symbol_prefixes": symbol_audit["unique_prefix_count"],
        "canonical_symbol_count": len(symbols),
        "quarantined_noncanonical_symbol_count": len(quarantined_prefixes),
        "quarantined_noncanonical_symbols": quarantined_prefixes,
        "total_symbol_month_index_pages": int(archive_frame["index_page_count"].sum()),
        "total_returned_month_keys": int(archive_frame["returned_key_count"].sum()),
        "all_symbol_month_pages_completed": True,
    }
    coverage["announcement_corpus"] = announcement_audit
    coverage["prohibitions"] = {
        "full_ohlcv_download_executed": False,
        "scanner_outcomes_inspected": False,
        "validation_or_holdout_inspected": False,
    }

    report_root.mkdir(parents=True, exist_ok=False)
    write_bytes_exclusive(
        report_root / "archive_observations.json", _json_bytes(archive_frame.to_dict("records"))
    )
    write_bytes_exclusive(
        report_root / "announcement_evidence.json",
        _json_bytes(announcement_frame.to_dict("records")),
    )
    write_bytes_exclusive(
        report_root / "lifecycle_catalog.json", _json_bytes(catalog.to_dict("records"))
    )
    unresolved = catalog.loc[
        catalog["scope_classification_status"].eq("unresolved")
        | catalog["official_trading_start_at"].isna()
    ]
    write_bytes_exclusive(
        report_root / "unresolved_queue.json", _json_bytes(unresolved.to_dict("records"))
    )
    onboard = pd.to_datetime(catalog["exchange_info_onboard_at"], utc=True)
    official = pd.to_datetime(catalog["official_trading_start_at"], utc=True)
    discrepancy_seconds = (official - onboard).dt.total_seconds().abs()
    discrepancies = catalog.loc[
        official.notna() & onboard.notna() & discrepancy_seconds.gt(3600),
        [
            "symbol",
            "official_trading_start_at",
            "exchange_info_onboard_at",
            "listing_source_url",
            "listing_article_id",
        ],
    ].copy()
    discrepancies["absolute_difference_seconds"] = discrepancy_seconds.loc[
        discrepancies.index
    ]
    coverage["material_listing_onboard_discrepancies_over_one_hour"] = len(discrepancies)
    write_bytes_exclusive(
        report_root / "listing_onboard_discrepancy_queue.json",
        _json_bytes(discrepancies.to_dict("records")),
    )
    write_json_exclusive(
        report_root / "noncanonical_archive_prefix_queue.json",
        {
            "status": "quarantined_fail_closed",
            "prefixes": quarantined_prefixes,
        },
    )
    write_json_exclusive(report_root / "coverage.json", coverage)
    write_json_exclusive(
        report_root / "acquisition_manifest.json",
        {
            "run_id": run_id,
            "created_at": datetime.now(UTC).isoformat(),
            "purpose": "historical_contract_lifecycle_catalog_no_scanner_evaluation",
            "archive_index_source": INDEX_HOST,
            "raw_root": str(raw_root.resolve()),
            "report_root": str(report_root.resolve()),
            "catalog_schema_version": coverage["catalog_schema_version"],
        },
    )
    print(json.dumps({"run_id": run_id, "coverage": coverage}, indent=2), flush=True)


if __name__ == "__main__":
    main()
