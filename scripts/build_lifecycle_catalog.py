from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
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
    observed_zip_keys_from_index_snapshots,
    sha256_file,
    write_bytes_exclusive,
    write_json_exclusive,
)
from alt_hot_scanner.data.provenance import (
    load_snapshot_provenance,
    record_new_snapshot_provenance,
)
from alt_hot_scanner.universe.authorization import (
    build_bundle_payload,
    catalog_readiness,
)
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog, catalog_coverage
from alt_hot_scanner.utils.config import load_config

EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"


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
    config_path = root / "config" / "research_v0_1.yaml"
    config = load_config(config_path)
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
            record_new_snapshot_provenance(
                path,
                url=url,
                parser_version="binance-s3-listobjectsv2-v2",
            )
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
        if "observed_archive_object_keys" not in archive_frame.columns:
            archive_frame["observed_archive_object_keys"] = archive_frame.apply(
                lambda row: list(
                    observed_zip_keys_from_index_snapshots(
                        row["archive_raw_snapshot_paths"], row["symbol"]
                    )
                ),
                axis=1,
            )
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
        record_new_snapshot_provenance(
            exchange_path,
            url=EXCHANGE_INFO_URL,
            parser_version="binance-exchange-info-v2",
        )
    exchange_sha256, _ = sha256_file(exchange_path)
    exchange_provenance = load_snapshot_provenance(
        exchange_path,
        expected_url=EXCHANGE_INFO_URL,
        expected_sha256=exchange_sha256,
    )
    exchange_records = records_from_exchange_info(
        exchange_payload,
        (
            pd.Timestamp(exchange_provenance["original_retrieval_timestamp"])
            if exchange_provenance is not None
            else None
        ),
        raw_snapshot_path=str(exchange_path.resolve()),
        raw_snapshot_sha256=exchange_sha256,
        stablecoin_underlyings=config["universe"]["stablecoin_underlyings"],
    )

    announcement_frame, announcement_audit = acquire_announcement_corpus(
        raw_root / "announcements", set(symbols)
    )
    catalog = build_lifecycle_catalog(
        archive_frame,
        exchange_records,
        announcement_frame,
        announcement_search_completed=True,
    )
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
    coverage["exchange_info_provenance"] = {
        "sha256": exchange_sha256,
        "original_retrieval_timestamp": (
            exchange_provenance["original_retrieval_timestamp"]
            if exchange_provenance is not None
            else None
        ),
        "retrieval_time_status": (
            "preserved_immutable_sidecar"
            if exchange_provenance is not None
            else "legacy_cached_original_retrieval_time_unresolved"
        ),
    }
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
    unresolved = catalog.loc[catalog["historical_inclusion_readiness"].ne("ready")]
    write_bytes_exclusive(
        report_root / "unresolved_queue.json", _json_bytes(unresolved.to_dict("records"))
    )
    discrepancies = catalog.loc[
        catalog["onboard_start_discrepancy_status"].eq("unresolved_material_discrepancy"),
        [
            "symbol",
            "official_trading_start_at",
            "exchange_info_onboard_at",
            "listing_source_url",
            "listing_article_id",
            "onboard_start_discrepancy_seconds",
            "onboard_start_discrepancy_status",
        ],
    ].copy()
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
    classification_evidence = []
    for row in exchange_records.to_dict("records"):
        for dimension, value_field, status_field, conflict_field in (
            (
                "stablecoin_underlying",
                "is_stablecoin_underlying",
                "stablecoin_evidence_status",
                "stablecoin_conflict_status",
            ),
            (
                "leveraged_token",
                "is_leveraged_token",
                "leveraged_evidence_status",
                "leveraged_conflict_status",
            ),
        ):
            classification_evidence.append(
                {
                    "asset": row["base_asset"] if dimension == "stablecoin_underlying" else row["symbol"],
                    "contract_identity": row["symbol"],
                    "dimension": dimension,
                    "value": row[value_field],
                    "source_type": "official_exchange_info_or_frozen_positive_guard",
                    "source_identifier": row["metadata_source"],
                    "source_url": row["metadata_source"],
                    "raw_snapshot_sha256": row["metadata_raw_snapshot_sha256"],
                    "reviewed_parser_version": "classification-evidence-v1",
                    "evidence_status": row[status_field],
                    "conflict_status": row[conflict_field],
                }
            )
    write_bytes_exclusive(
        report_root / "classification_evidence.json", _json_bytes(classification_evidence)
    )
    prior_catalog_path = root / "reports" / "lifecycle" / run_id / "lifecycle_catalog.json"
    prior_catalog = (
        pd.DataFrame(json.loads(prior_catalog_path.read_text(encoding="utf-8")))
        if prior_catalog_path.exists() and prior_catalog_path.parent != report_root
        else pd.DataFrame()
    )
    old_starts = (
        prior_catalog.set_index("symbol")["official_trading_start_at"].dropna().to_dict()
        if not prior_catalog.empty
        else {}
    )
    new_starts = catalog.set_index("symbol")["official_trading_start_at"].dropna().to_dict()
    former_article_ids = set(
        prior_catalog.loc[
            prior_catalog["official_trading_start_at"].notna(), "listing_article_id"
        ].dropna()
    ) if not prior_catalog.empty else set()
    rejected_former_evidence = announcement_frame.loc[
        announcement_frame["article_code"].isin(former_article_ids)
        & announcement_frame["match_status"].eq("rejected_semantic_class")
    ]
    rejection_classes = {}
    for semantic_class, rows in rejected_former_evidence.groupby("article_semantic_class"):
        rejection_classes[semantic_class] = {
            "article_count": int(rows["article_code"].nunique()),
            "symbol_count": int(rows["symbol"].nunique()),
            "symbols": sorted(rows["symbol"].unique().tolist()),
        }
    listing_reaudit = {
        "former_accepted_start_count": len(old_starts),
        "accepted_exact_original_start_count": len(new_starts),
        "rejected_former_start_count": len(set(old_starts) - set(new_starts)),
        "rejected_former_starts": sorted(set(old_starts) - set(new_starts)),
        "changed_start_values": sorted(
            symbol
            for symbol in set(old_starts) & set(new_starts)
            if str(old_starts[symbol]) != str(new_starts[symbol])
        ),
        "ambiguous_or_unresolved_listing_count": int(
            catalog["official_trading_start_at"].isna().sum()
        ),
        "former_false_positive_rejections_by_semantic_class": rejection_classes,
    }
    write_json_exclusive(report_root / "listing_reaudit.json", listing_reaudit)
    readiness = catalog_readiness(catalog, quarantined_prefixes)
    coverage["recomputed_readiness"] = readiness
    write_json_exclusive(report_root / "readiness.json", readiness)
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
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    bundle = build_bundle_payload(
        report_root=report_root,
        config_path=config_path,
        artifact_names=[
            "lifecycle_catalog.json",
            "coverage.json",
            "classification_evidence.json",
            "announcement_evidence.json",
            "archive_observations.json",
            "noncanonical_archive_prefix_queue.json",
            "readiness.json",
        ],
        code_commit=commit,
        created_at=datetime.now(UTC).isoformat(),
    )
    write_json_exclusive(report_root / "lifecycle_bundle.json", bundle)
    print(json.dumps({"run_id": run_id, "coverage": coverage}, indent=2), flush=True)


if __name__ == "__main__":
    main()
