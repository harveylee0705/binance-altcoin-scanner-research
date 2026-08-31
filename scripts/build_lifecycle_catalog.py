from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.announcements import acquire_announcement_corpus
from alt_hot_scanner.data.binance_public import (
    INDEX_HOST,
    acquire_episode_first_observed_trade,
    acquire_first_observed_trade,
    acquire_futures_server_time_snapshot,
    collision_resistant_run_id,
    discover_archive_months,
    discover_archive_symbol_candidates,
    discover_frontier_daily_symbol_candidates,
    fetch_exchange_info_snapshot,
    list_archive_index,
    observed_daily_trade_keys_from_index_snapshots,
    sha256_file,
    validate_daily_trade_object_key,
    write_bytes_exclusive,
    write_json_exclusive,
)
from alt_hot_scanner.data.provenance import (
    load_snapshot_provenance,
    record_new_snapshot_provenance,
)
from alt_hot_scanner.identity import safe_identity_component
from alt_hot_scanner.universe.adjudications import (
    BOUNDARY_INDEX_SCHEMA_VERSION,
    episode_freshness_review_required,
    load_lifecycle_adjudications,
    require_exclusive_utc_day_boundary,
    verify_boundary_index,
)
from alt_hot_scanner.universe.authorization import (
    build_bundle_payload,
    build_lifecycle_freshness,
    catalog_readiness,
)
from alt_hot_scanner.universe.checkpoint import (
    ARCHIVE_CHECKPOINT_SCHEMA_VERSION,
    verify_archive_checkpoint,
)
from alt_hot_scanner.universe.contracts import records_from_exchange_info
from alt_hot_scanner.universe.delisting_registry import (
    cms_corpus_binding,
    load_delisting_registry,
    verify_cms_corpus_binding,
    verify_delisting_records_against_announcement_evidence,
)
from alt_hot_scanner.universe.eligibility_oracle import (
    EPISODE_EVIDENCE_SCHEMA_VERSION,
    content_identity,
    run_eligibility_oracle,
)
from alt_hot_scanner.universe.evidence_replay import (
    build_primitive_evidence_manifest,
)
from alt_hot_scanner.universe.lifecycle import build_lifecycle_catalog, catalog_coverage
from alt_hot_scanner.universe.scope_registry import (
    build_candidate_inventory,
    candidate_inventory_difference,
    verify_scope_registry,
)
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
    parser.add_argument("--scope-registry", required=True, help="Exact independently reviewed registry")
    parser.add_argument(
        "--adjudications", required=True, help="Exact reviewed lifecycle adjudication file"
    )
    parser.add_argument("--delisting-registry", required=True)
    parser.add_argument("--delisting-review", required=True)
    parser.add_argument(
        "--required-valid-through-utc",
        required=True,
        help="Exact exclusive UTC day boundary required from lifecycle evidence",
    )
    return parser.parse_args()


def _json_bytes(records: list[dict[str, Any]]) -> bytes:
    return json.dumps(records, indent=2, default=str, sort_keys=True).encode("utf-8")


def main() -> None:
    args = parse_args()
    require_exclusive_utc_day_boundary(
        args.required_valid_through_utc, "required_valid_through_utc"
    )
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
    server_time_evidence = acquire_futures_server_time_snapshot(
        raw_root / "binance_futures_server_time.json"
    )
    if args.required_valid_through_utc > server_time_evidence[
        "maximum_archive_backed_horizon_utc"
    ]:
        raise RuntimeError(
            "Required freshness horizon exceeds the maximum previous-day Binance archive horizon"
        )

    def observer(label: str):
        def save(page: int, url: str, payload: bytes) -> None:
            digest = hashlib.sha256(payload).hexdigest()
            safe_label = (
                "symbols"
                if label == "symbols"
                else "frontier_daily_symbols"
                if label == "frontier_daily_symbols"
                else safe_identity_component(label)
            )
            path = raw_root / "archive_index" / f"{safe_label}_page_{page:03d}_{digest[:16]}.xml"
            if path.exists():
                if sha256_file(path)[0] != digest:
                    raise FileExistsError(f"Existing raw archive-index snapshot differs: {path}")
                load_snapshot_provenance(path, expected_url=url, expected_sha256=digest)
            else:
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
    frontier_discovery: Any
    if args.resume_run:
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Archive checkpoint not found: {checkpoint_path}")
        checkpoint = verify_archive_checkpoint(checkpoint_path, raw_root)
        symbols = checkpoint["canonical_symbols"]
        quarantined_prefixes = checkpoint["quarantined_prefixes"]
        symbol_audit = checkpoint["symbol_audit"]
        archive_raw_paths["symbols"] = checkpoint["symbol_raw_snapshot_paths"]
        archive_raw_hashes["symbols"] = checkpoint["symbol_raw_snapshot_sha256s"]
        archive_frame = pd.DataFrame(checkpoint["archive_observations"])
        frontier_discovery = discover_frontier_daily_symbol_candidates(
            page_observer=observer("frontier_daily_symbols")
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
        frontier_discovery = discover_frontier_daily_symbol_candidates(
            page_observer=observer("frontier_daily_symbols")
        )
        write_json_exclusive(
            checkpoint_path,
            {
                "schema_version": ARCHIVE_CHECKPOINT_SCHEMA_VERSION,
                "canonical_symbols": symbols,
                "quarantined_prefixes": quarantined_prefixes,
                "symbol_audit": symbol_audit,
                "symbol_raw_snapshot_paths": archive_raw_paths["symbols"],
                "symbol_raw_snapshot_sha256s": archive_raw_hashes["symbols"],
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

    frontier_symbols = list(frontier_discovery.symbols)
    frontier_quarantine = list(frontier_discovery.quarantined_prefixes)
    all_candidates = sorted(
        {*symbols, *quarantined_prefixes, *frontier_symbols, *frontier_quarantine}
    )
    monthly_discovery_layer = {
        "layer": "historical_monthly_candidates",
        "source_prefix": "data/futures/um/monthly/klines/",
        "candidate_identities": sorted([*symbols, *quarantined_prefixes]),
        "raw_snapshot_paths": archive_raw_paths.get("symbols", []),
        "raw_snapshot_sha256s": archive_raw_hashes.get("symbols", []),
        "retrieval_provenance": "immutable_raw_snapshot_sidecars",
        "source_urls": symbol_audit.get("source_urls", []),
        "index_audit": symbol_audit,
    }
    frontier_discovery_layer = {
        "layer": "frontier_daily_candidates",
        "source_prefix": "data/futures/um/daily/klines/",
        "candidate_identities": sorted([*frontier_symbols, *frontier_quarantine]),
        "raw_snapshot_paths": archive_raw_paths.get("frontier_daily_symbols", []),
        "raw_snapshot_sha256s": archive_raw_hashes.get("frontier_daily_symbols", []),
        "retrieval_provenance": "immutable_raw_snapshot_sidecars",
        "source_urls": frontier_discovery.audit.source_urls,
        "index_audit": asdict(frontier_discovery.audit),
    }
    candidate_inventory = build_candidate_inventory(
        all_candidates,
        discovered_at=discovered_at.isoformat(),
        source_identifier="official_binance_monthly_and_frontier_daily_archive_prefix_inventory",
        discovery_layers=[monthly_discovery_layer, frontier_discovery_layer],
    )
    scope_registry_path = Path(args.scope_registry).resolve(strict=True)
    scope_registry = json.loads(scope_registry_path.read_text(encoding="utf-8"))
    if scope_registry.get("candidate_set_digest") != candidate_inventory[
        "candidate_set_digest"
    ]:
        report_root.mkdir(parents=True, exist_ok=False)
        write_json_exclusive(report_root / "candidate_inventory.json", candidate_inventory)
        write_json_exclusive(
            report_root / "candidate_scope_review_required.json",
            candidate_inventory_difference(candidate_inventory, scope_registry),
        )
        raise RuntimeError("Candidate digest changed; independent scope review is required")
    scope_by_identity = verify_scope_registry(
        scope_registry,
        all_candidates,
        registry_path=scope_registry_path,
        repository_root=root,
    )
    lifecycle_adjudications = load_lifecycle_adjudications(
        Path(args.adjudications),
        candidate_set_digest=candidate_inventory["candidate_set_digest"],
    )
    delisting_registry_path = Path(args.delisting_registry).resolve(strict=True)
    delisting_review_path = Path(args.delisting_review).resolve(strict=True)
    delisting_registry = load_delisting_registry(
        delisting_registry_path,
        candidate_set_digest=candidate_inventory["candidate_set_digest"],
        review_path=delisting_review_path,
    )
    unicode_in_scope = [
        identity
        for identity in quarantined_prefixes
        if scope_by_identity[identity]["product_scope"]
        in {"in_scope_crypto_perpetual", "benchmark_only"}
    ]
    extra_observations: list[dict[str, Any]] = []
    for symbol in unicode_in_scope:
        observation = asdict(
            discover_archive_months(
                symbol,
                discovered_at=discovered_at,
                page_observer=observer(symbol),
            )
        )
        observation["archive_raw_snapshot_paths"] = archive_raw_paths[symbol]
        observation["archive_raw_snapshot_sha256s"] = archive_raw_hashes[symbol]
        extra_observations.append(observation)
    if extra_observations:
        archive_frame = pd.concat(
            [archive_frame, pd.DataFrame.from_records(extra_observations)], ignore_index=True
        )

    # Preserve reviewed frontier candidates that do not have monthly evidence
    # yet as explicit blocking catalog rows; never silently drop them.
    daily_only_symbols = sorted(set(frontier_symbols) - set(symbols))
    if daily_only_symbols:
        frontier_blocked = [
            {
                "symbol": symbol,
                "first_archive_month": None,
                "last_archive_month": None,
                "archive_discovery_timestamp": discovered_at.isoformat(),
                "archive_source_url": INDEX_HOST,
                "archive_discovery_provenance": (
                    "frontier_daily_candidate_only_no_monthly_archive_blocking"
                ),
                "archive_parser_version": "binance-frontier-daily-candidate-v1",
                "index_page_count": 0,
                "returned_key_count": 0,
                "unique_archive_count": 0,
                "any_page_truncated": False,
                "observed_archive_object_keys": [],
                "archive_raw_snapshot_paths": [],
                "archive_raw_snapshot_sha256s": [],
            }
            for symbol in daily_only_symbols
        ]
        archive_frame = pd.concat(
            [archive_frame, pd.DataFrame.from_records(frontier_blocked)], ignore_index=True
        )

    probe_symbols = sorted(
        identity
        for identity, scope in scope_by_identity.items()
        if scope["product_scope"] in {"in_scope_crypto_perpetual", "benchmark_only"}
    )
    probe_checkpoint = raw_root / "trade_probe" / "first_observed_trades.json"
    if probe_checkpoint.exists():
        first_trade_records = json.loads(probe_checkpoint.read_text(encoding="utf-8"))
        if sorted(row.get("symbol") for row in first_trade_records) != probe_symbols:
            raise ValueError("First-observed-trade checkpoint candidate identities changed")

    else:
        first_trade_records = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            pending = {
                executor.submit(
                    acquire_first_observed_trade, symbol, raw_root / "trade_probe"
                ): symbol
                for symbol in probe_symbols
            }
            for position, future in enumerate(as_completed(pending), start=1):
                first_trade_records.append(asdict(future.result()))
                if position % 25 == 0:
                    print(
                        f"First-observed-trade lifecycle probe: {position}/{len(probe_symbols)}",
                        flush=True,
                    )
        first_trade_records.sort(key=lambda row: row["symbol"])
        write_bytes_exclusive(probe_checkpoint, _json_bytes(first_trade_records))
    first_trade_frame = pd.DataFrame.from_records(first_trade_records)

    boundary_rows: list[dict[str, Any]] = []
    for symbol in probe_symbols:
        safe_symbol = safe_identity_component(symbol)
        boundary_root = raw_root / "daily_trade_frontier_index"
        existing_paths = sorted(boundary_root.glob(f"{safe_symbol}_page_*.xml"))

        def save_boundary_page(
            page: int,
            url: str,
            payload: bytes,
            boundary_root: Path = boundary_root,
            safe_symbol: str = safe_symbol,
        ) -> None:
            digest = hashlib.sha256(payload).hexdigest()
            path = boundary_root / f"{safe_symbol}_page_{page:03d}_{digest[:16]}.xml"
            if path.exists():
                load_snapshot_provenance(path, expected_url=url, expected_sha256=digest)
                return
            write_bytes_exclusive(path, payload)
            record_new_snapshot_provenance(
                path, url=url, parser_version="binance-daily-trade-index-frontier-v1"
            )

        if not existing_paths:
            list_archive_index(
                f"data/futures/um/daily/trades/{symbol}/",
                page_observer=save_boundary_page,
            )
            existing_paths = sorted(boundary_root.glob(f"{safe_symbol}_page_*.xml"))
        try:
            keys = observed_daily_trade_keys_from_index_snapshots(
                [str(path.resolve()) for path in existing_paths], symbol
            )
        except ValueError as exc:
            if "No daily trade ZIP objects" not in str(exc):
                raise
            keys = ()
        dates = sorted({validate_daily_trade_object_key(key).period for key in keys})
        boundary_rows.append(
            {
                "schema_version": BOUNDARY_INDEX_SCHEMA_VERSION,
                "symbol": symbol,
                "source_identifier": f"data/futures/um/daily/trades/{symbol}/",
                "raw_snapshot_paths": [str(path.resolve()) for path in existing_paths],
                "raw_snapshot_sha256s": [sha256_file(path)[0] for path in existing_paths],
                "observed_archive_dates": dates,
                "observed_archive_keys": list(keys),
                "observed_daily_trade_dates": dates,
                "parser_version": "binance-daily-trade-index-frontier-v1",
            }
        )
    verify_boundary_index(lifecycle_adjudications, boundary_rows)
    episode_review = episode_freshness_review_required(
        boundary_rows,
        lifecycle_adjudications,
        required_valid_through_utc=args.required_valid_through_utc,
        reviewed_delisting_records=delisting_registry["records"],
    )
    if episode_review is not None:
        report_root.mkdir(parents=True, exist_ok=False)
        write_json_exclusive(report_root / "episode_freshness_review_required.json", episode_review)
        raise RuntimeError("Episode freshness review is required; lifecycle adjudications were unchanged")
    episode_freshness_evidence = {
        "schema_version": "lifecycle-episode-freshness-v1",
        "required_valid_through_utc": args.required_valid_through_utc,
        "symbols": boundary_rows,
    }
    first_trade_by_symbol = {row["symbol"]: row for row in first_trade_records}
    boundary_by_symbol = {row["symbol"]: row for row in boundary_rows}
    episode_trade_records: list[dict[str, Any]] = []
    for symbol in probe_symbols:
        adjudication = lifecycle_adjudications["by_symbol"].get(symbol)
        specs = adjudication["episodes"] if adjudication else [{"episode_id": f"{symbol}:1"}]
        for position, spec in enumerate(specs):
            if position == 0:
                episode_trade_records.append(
                    {**first_trade_by_symbol[symbol], "lifecycle_episode_id": spec["episode_id"]}
                )
                continue
            archive_date = adjudication["gap_evidence"][
                "first_post_gap_trade_archive_date"
            ]
            keys = [
                key
                for key in boundary_by_symbol[symbol]["observed_archive_keys"]
                if validate_daily_trade_object_key(key).period == archive_date
            ]
            if len(keys) != 1:
                raise ValueError(f"Reviewed post-gap archive is not unique for {symbol}")
            episode_trade_records.append(
                acquire_episode_first_observed_trade(
                    symbol,
                    keys[0],
                    raw_root / "trade_probe",
                    episode_id=spec["episode_id"],
                )
            )
    episode_trade_records.sort(key=lambda row: row["lifecycle_episode_id"])
    episode_evidence_core = {
        "schema_version": EPISODE_EVIDENCE_SCHEMA_VERSION,
        "candidate_set_digest": candidate_inventory["candidate_set_digest"],
        "records": episode_trade_records,
    }
    episode_evidence = {
        **episode_evidence_core,
        "evidence_id": content_identity(episode_evidence_core),
    }

    announcement_frame, announcement_audit = acquire_announcement_corpus(
        raw_root / "announcements", set(all_candidates)
    )
    observed_cms_binding = cms_corpus_binding(announcement_audit)
    if delisting_registry.get("official_cms_corpus") != observed_cms_binding:
        report_root.mkdir(parents=True, exist_ok=False)
        write_json_exclusive(
            report_root / "delisting_freshness_review_required.json",
            {
                "schema_version": "lifecycle-delisting-freshness-review-required-v1",
                "status": "review_required",
                "observed_corpus": observed_cms_binding,
                "reviewed_registry_corpus": delisting_registry.get("official_cms_corpus"),
                "reviewed_registry_id": delisting_registry.get("registry_id"),
                "mismatch": {
                    "official_cms_corpus_changed": True,
                    "registry_must_be_re-reviewed": True,
                },
                "reason": "Complete CMS acquisition is not exactly covered by the supplied reviewed delisting registry.",
            },
        )
        raise RuntimeError("Refreshed CMS corpus requires delisting review")
    verify_cms_corpus_binding(delisting_registry, announcement_audit)
    verify_delisting_records_against_announcement_evidence(
        delisting_registry, announcement_frame.to_dict("records")
    )
    cms_started = pd.Timestamp(announcement_audit["rebuild_started_at"])
    if cms_started.tzinfo is None:
        cms_started = cms_started.tz_localize("UTC")
    else:
        cms_started = cms_started.tz_convert("UTC")
    cms_started_utc = cms_started.isoformat().replace("+00:00", "Z")
    server_horizon = server_time_evidence["maximum_archive_backed_horizon_utc"]
    delisting_horizon = min(
        (server_horizon, cms_started_utc),
        key=lambda value: pd.Timestamp(value),
    )
    bound_raw_paths = {
        server_time_evidence["path"],
        *monthly_discovery_layer["raw_snapshot_paths"],
        *frontier_discovery_layer["raw_snapshot_paths"],
        *(path for row in boundary_rows for path in row["raw_snapshot_paths"]),
        *(
            str(path.resolve())
            for path in (raw_root / "announcements").iterdir()
            if path.is_file() and not path.name.endswith(".provenance.json")
        ),
    }
    freshness = build_lifecycle_freshness(
        required_valid_through_utc=args.required_valid_through_utc,
        candidate_valid_through_utc=server_horizon,
        episode_valid_through_utc=server_horizon,
        delisting_valid_through_utc=delisting_horizon,
        server_time_evidence=server_time_evidence,
        frontier_candidate_discovery={
            "layers": [monthly_discovery_layer, frontier_discovery_layer],
            "candidate_set_digest": candidate_inventory["candidate_set_digest"],
        },
        episode_freshness_evidence=episode_freshness_evidence,
        announcement_corpus={
            "identity": observed_cms_binding["identity"],
            "sha256": observed_cms_binding["sha256"],
            "acquisition_started_at_utc": cms_started_utc,
        },
        bound_raw_evidence=[
            {"path": path, "sha256": sha256_file(path)[0]}
            for path in sorted(bound_raw_paths)
        ],
    )
    catalog = build_lifecycle_catalog(
        archive_frame,
        exchange_records,
        announcement_frame,
        announcement_search_completed=True,
        first_observed_trades=first_trade_frame,
        episode_first_observed_trades=episode_trade_records,
        delisting_registry_records=delisting_registry["records"],
        scope_registry_records=scope_registry["records"],
        lifecycle_adjudications=lifecycle_adjudications,
    )
    coverage = catalog_coverage(catalog)
    noncanonical_usdt = sum(symbol.endswith("USDT") for symbol in quarantined_prefixes)
    coverage["canonical_archive_symbols"] = len(symbols)
    coverage["total_archive_discovered_symbols"] = symbol_audit["unique_prefix_count"]
    coverage["canonical_usdt_candidates"] = sum(symbol.endswith("USDT") for symbol in symbols)
    coverage["noncanonical_usdt_candidates"] = noncanonical_usdt
    coverage["in_scope_crypto_perpetual_candidates"] = sum(
        scope["product_scope"] in {"in_scope_crypto_perpetual", "benchmark_only"}
        for scope in scope_by_identity.values()
    )
    all_quarantined = sorted({*quarantined_prefixes, *frontier_quarantine})
    coverage["unresolved_categories"]["noncanonical_archive_identities"] = len(
        all_quarantined
    )
    coverage["unresolved_categories"]["noncanonical_usdt_candidates"] = sum(
        symbol.endswith("USDT") for symbol in all_quarantined
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
    coverage["candidate_discovery"] = {
        "layers": [monthly_discovery_layer, frontier_discovery_layer],
        "candidate_set_digest": candidate_inventory["candidate_set_digest"],
        "historical_monthly_candidate_count": len(monthly_discovery_layer["candidate_identities"]),
        "frontier_daily_candidate_count": len(frontier_discovery_layer["candidate_identities"]),
        "union_candidate_count": len(all_candidates),
    }
    coverage["announcement_corpus"] = announcement_audit
    coverage["scope_registry"] = {
        "candidate_set_digest": scope_registry["candidate_set_digest"],
        "registry_payload_id": scope_registry["registry_payload_id"],
        "registry_id": scope_registry["registry_id"],
        "independent_review_id": scope_registry["independent_review"]["identifier"],
        "stablecoin_positive_exclusions": len(
            scope_registry["stablecoin_positive_exclusions"]
        ),
        "leveraged_token_positive_exclusions": len(
            scope_registry["leveraged_token_positive_exclusions"]
        ),
        "noncrypto_index_composite_exclusions": len(
            scope_registry["noncrypto_index_composite_exclusions"]
        ),
        "unresolved": len(scope_registry["unresolved_identities"]),
    }
    coverage["lifecycle_adjudications"] = {
        "adjudication_id": lifecycle_adjudications["adjudication_id"],
        "reviewed_symbols": sorted(lifecycle_adjudications["by_symbol"]),
        "genuine_relisting_symbols": sorted(
            symbol
            for symbol, record in lifecycle_adjudications["by_symbol"].items()
            if len(record["episodes"]) > 1
        ),
    }
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
    write_json_exclusive(report_root / "candidate_inventory.json", candidate_inventory)
    write_bytes_exclusive(
        report_root / "archive_observations.json", _json_bytes(archive_frame.to_dict("records"))
    )
    write_bytes_exclusive(
        report_root / "announcement_evidence.json",
        _json_bytes(announcement_frame.to_dict("records")),
    )
    write_json_exclusive(report_root / "announcement_corpus_audit.json", announcement_audit)
    write_bytes_exclusive(
        report_root / "historical_scope_registry.json", scope_registry_path.read_bytes()
    )
    write_bytes_exclusive(
        report_root / "lifecycle_adjudications.json",
        lifecycle_adjudications["path"].read_bytes(),
    )
    write_bytes_exclusive(
        report_root / "lifecycle_daily_trade_boundaries.json", _json_bytes(boundary_rows)
    )
    write_bytes_exclusive(
        report_root / "first_observed_trades.json", _json_bytes(first_trade_records)
    )
    write_json_exclusive(
        report_root / "episode_first_observed_trades.json", episode_evidence
    )
    write_json_exclusive(report_root / "lifecycle_freshness.json", freshness)
    write_bytes_exclusive(
        report_root / "historical_delisting_cutoff_registry.json",
        delisting_registry_path.read_bytes(),
    )
    write_bytes_exclusive(
        report_root / "delisting_registry_independent_review.json",
        delisting_review_path.read_bytes(),
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
            "status": "reviewed_finite_universe",
            "prefixes": all_quarantined,
            "dispositions": {
                identity: scope_by_identity[identity]["product_scope"]
                for identity in all_quarantined
            },
        },
    )
    classification_evidence = []
    for row in scope_registry["records"]:
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
            direct = next(
                (
                    item
                    for item in row["evidence"]
                    if item.get("evidence_class") == "direct_positive_exclusion"
                    and item.get("exclusion_class")
                    == ("stablecoin" if dimension == "stablecoin_underlying" else "leveraged_token")
                ),
                None,
            )
            classification_evidence.append(
                {
                    "asset": row["base_asset"] if dimension == "stablecoin_underlying" else row["contract_identity"],
                    "contract_identity": row["contract_identity"],
                    "dimension": dimension,
                    "value": row[value_field],
                    "source_type": "candidate_set_bound_historical_scope_registry",
                    "source_identifier": scope_registry["registry_id"],
                    "source_url": direct["source_url"] if direct is not None else None,
                    "raw_snapshot_sha256": exchange_sha256,
                    "reviewed_parser_version": scope_registry["reviewer_parser_version"],
                    "evidence_status": row[status_field],
                    "conflict_status": "none",
                    "positive_exclusion_evidence": direct,
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
    unresolved_noncanonical = [
        identity
        for identity in all_quarantined
        if scope_by_identity[identity]["scope_audit_status"] != "complete"
    ]
    readiness = catalog_readiness(catalog, unresolved_noncanonical)
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
    primitive_manifest = build_primitive_evidence_manifest(
        raw_root,
        first_trades=first_trade_records,
        episode_trades=episode_trade_records,
    )
    write_json_exclusive(
        report_root / "primitive_evidence_manifest.json", primitive_manifest
    )
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    oracle_report = run_eligibility_oracle(
        report_root,
        repository_root=root,
        config_path=config_path,
        executable_commit=commit,
    )
    write_json_exclusive(
        report_root / "independent_eligibility_verification_report.json", oracle_report
    )
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
            "historical_scope_registry.json",
            "first_observed_trades.json",
            "episode_first_observed_trades.json",
            "lifecycle_freshness.json",
            "historical_delisting_cutoff_registry.json",
            "delisting_registry_independent_review.json",
            "announcement_corpus_audit.json",
            "candidate_inventory.json",
            "lifecycle_adjudications.json",
            "lifecycle_daily_trade_boundaries.json",
            "primitive_evidence_manifest.json",
            "independent_eligibility_verification_report.json",
        ],
        code_commit=commit,
        created_at=datetime.now(UTC).isoformat(),
    )
    write_json_exclusive(report_root / "lifecycle_bundle.json", bundle)
    print(json.dumps({"run_id": run_id, "coverage": coverage}, indent=2), flush=True)


if __name__ == "__main__":
    main()
