from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from alt_hot_scanner.universe.delisting_registry import DELISTING_REGISTRY_SCHEMA_VERSION


def _identity(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(payload).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Propose an immutable delisting cutoff registry")
    parser.add_argument("--source-report", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = Path(args.source_report).resolve(strict=True)
    inventory = json.loads((report / "candidate_inventory.json").read_text("utf-8"))
    scope = json.loads((report / "historical_scope_registry.json").read_text("utf-8"))
    catalog = json.loads((report / "lifecycle_catalog.json").read_text("utf-8"))
    audit_path = report / "announcement_corpus_audit.json"
    audit = json.loads(audit_path.read_text("utf-8"))
    in_scope = {
        row["contract_identity"]
        for row in scope["records"]
        if row["product_scope"] in {"in_scope_crypto_perpetual", "benchmark_only"}
    }
    records: list[dict[str, Any]] = []
    for row in sorted(catalog, key=lambda item: item["symbol"]):
        if row["symbol"] not in in_scope:
            continue
        intervals = row["lifecycle_intervals"]
        if isinstance(intervals, str):
            intervals = json.loads(intervals)
        for position, interval in enumerate(intervals):
            cutoff = interval.get("delisting_announcement_published_at")
            terminal = interval.get("last_trading_at")
            final_current = position == len(intervals) - 1 and row.get(
                "present_in_current_exchange_info"
            ) is True and row.get("latest_known_status") == "TRADING"
            status = (
                "accepted_exact_cutoff"
                if cutoff is not None
                else "not_applicable_current_episode"
                if final_current
                else "reviewed_no_reliable_cutoff"
            )
            records.append(
                {
                    "symbol": row["symbol"],
                    "lifecycle_episode_id": interval["lifecycle_episode_id"],
                    "article_code": interval.get("delisting_article_id"),
                    "official_article_url": interval.get("delisting_source_url"),
                    "raw_article_sha256": interval.get("delisting_raw_snapshot_sha256"),
                    "official_publication_timestamp": cutoff,
                    "terminal_last_trading_at": terminal,
                    "product_event_disposition": (
                        "binance_usdm_futures_termination" if cutoff else None
                    ),
                    "review_status": status,
                    "evidence_summary": (
                        "Exact official Binance USD-M Futures termination publication accepted."
                        if cutoff
                        else "Comprehensive official delisting search found no reliable exact cutoff."
                        if status == "reviewed_no_reliable_cutoff"
                        else "Current trading lifecycle episode; no termination cutoff applies."
                    ),
                }
            )
    core = {
        "schema_version": DELISTING_REGISTRY_SCHEMA_VERSION,
        "candidate_set_digest": inventory["candidate_set_digest"],
        "official_cms_corpus": {
            "identity": _identity(audit),
            "sha256": hashlib.sha256(audit_path.read_bytes()).hexdigest(),
            "delisting_catalog_id": next(
                item["catalog_id"]
                for item in audit["catalogs"]
                if item["event_type"] == "delisting"
            ),
        },
        "reviewed_contract_identities": sorted(in_scope),
        "review_version": "delisting-cutoff-finite-universe-review-v1",
        "records": records,
    }
    payload = {**core, "registry_id": _identity(core)}
    Path(args.output).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", "utf-8")
    print(json.dumps({"records": len(records), "registry_id": payload["registry_id"]}))


if __name__ == "__main__":
    main()
