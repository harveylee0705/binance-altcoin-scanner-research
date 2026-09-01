from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from alt_hot_scanner.data.acquisition import (
    acquisition_executable_tree_sha256,
    build_mixed_source_inventory,
    create_frozen_plan,
    create_run_identity,
    discover_daily_1h_objects,
    fetch_frozen_cutoff,
    select_cutoff_exclusive_utc,
    verify_acquisition_authorization,
    verify_lifecycle_runtime_boundary,
)
from alt_hot_scanner.data.binance_public import (
    collision_resistant_run_id,
    write_bytes_exclusive,
    write_json_exclusive,
)
from alt_hot_scanner.universe.authorization import verify_approval_pin, verify_lifecycle_bundle
from alt_hot_scanner.universe.contracts import filter_instrument_scope
from alt_hot_scanner.utils.config import load_config


def last_completed_month(now: pd.Timestamp) -> pd.Period:
    timestamp = pd.Timestamp(now)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.to_period("M") - 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare one immutable monthly > daily > conditional API source plan; downloads nothing"
    )
    parser.add_argument("--config", default="config/research_v0_1.yaml")
    parser.add_argument("--bundle", required=True, help="Exact authorized lifecycle bundle")
    parser.add_argument("--approval", required=True, help="Exact active lifecycle approval pin")
    parser.add_argument(
        "--acquisition-authorization",
        required=True,
        help="Exact independent PASS authorization for the acquisition/full-build executable",
    )
    parser.add_argument(
        "--cutoff-exclusive-utc",
        help="Optional exact completed-1H UTC cutoff; defaults to Binance's latest completed hour",
    )
    return parser.parse_args()


def _monthly_keys(verified_bundle: dict, approved_symbols: set[str]) -> list[str]:
    rows = json.loads(
        verified_bundle["artifacts"]["archive_observations.json"].read_text(encoding="utf-8")
    )
    keys: list[str] = []
    for row in rows:
        if row.get("symbol") not in approved_symbols:
            continue
        observed = row.get("observed_archive_object_keys")
        if type(observed) is not list:
            raise ValueError("Lifecycle archive observations lack exact monthly ZIP keys")
        keys.extend(observed)
    return sorted(set(keys))


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    bundle_path = Path(args.bundle).resolve()
    approval_path = Path(args.approval).resolve()
    acquisition_auth_path = Path(args.acquisition_authorization).resolve()

    verified = verify_lifecycle_bundle(bundle_path)
    verified_approval = verify_approval_pin(approval_path, verified)
    if not verified["readiness"]["authorization_ready"]:
        raise RuntimeError("Lifecycle readiness is blocked; acquisition planning prohibited")
    lifecycle_commit = verified_approval["approval"]["lifecycle_evidence_code_commit"]
    verify_lifecycle_runtime_boundary(root, lifecycle_commit)
    acquisition_auth = verify_acquisition_authorization(acquisition_auth_path, root)

    lifecycle_horizon = verified["bundle"]["lifecycle_evidence_valid_through_utc"]
    latest_completed, server_time_raw = fetch_frozen_cutoff()
    cutoff = select_cutoff_exclusive_utc(
        latest_completed_utc=latest_completed,
        lifecycle_evidence_valid_through_utc=lifecycle_horizon,
        requested_cutoff_exclusive_utc=args.cutoff_exclusive_utc,
    )

    requested_config = (root / args.config).resolve()
    if requested_config != verified["config_path"]:
        raise RuntimeError("Requested config is not exact config bound into lifecycle bundle")
    config = load_config(verified["config_path"])
    catalog = verified["catalog"].copy()
    catalog["underlying_subtype"] = catalog["underlying_subtype"].map(
        lambda value: tuple(json.loads(value)) if isinstance(value, str) else value
    )
    scoped = filter_instrument_scope(catalog, config["universe"]["stablecoin_underlyings"])
    approved_symbols = set(scoped.loc[scoped["historical_inclusion_readiness"].eq("ready"), "symbol"])
    if not approved_symbols:
        raise RuntimeError("No lifecycle-approved symbols are ready")

    candidate_inventory = json.loads(
        verified["artifacts"]["candidate_inventory.json"].read_text(encoding="utf-8")
    )
    frozen_candidates = set(candidate_inventory.get("candidate_identities", []))
    if not frozen_candidates:
        raise RuntimeError("Frozen lifecycle candidate universe is empty")

    run_id = collision_resistant_run_id()
    evidence_root = root / "reports" / "source_discovery" / run_id
    evidence_root.mkdir(parents=True, exist_ok=False)

    server_time_path = evidence_root / "binance_futures_server_time.json"
    write_bytes_exclusive(server_time_path, server_time_raw)
    server_time_sha = hashlib.sha256(server_time_raw).hexdigest()

    daily_pages: list[dict] = []

    def observe(page: int, url: str, payload: bytes) -> None:
        digest = hashlib.sha256(payload).hexdigest()
        page_path = evidence_root / f"daily_index_{len(daily_pages) + 1:06d}_{digest}.xml"
        write_bytes_exclusive(page_path, payload)
        daily_pages.append(
            {
                "sequence": len(daily_pages) + 1,
                "page": page,
                "url": url,
                "sha256": digest,
                "path": str(page_path.resolve()),
            }
        )

    daily, discovery_evidence = discover_daily_1h_objects(page_observer=observe)
    warmup_start = pd.Timestamp(config["data"]["start"]).tz_localize(None).to_period("M") - 1
    inventory, boundary = build_mixed_source_inventory(
        monthly_keys=_monthly_keys(verified, approved_symbols),
        daily_identities=daily,
        approved_symbols=approved_symbols,
        frozen_candidate_universe=frozen_candidates,
        warmup_start_month=str(warmup_start),
        cutoff_exclusive_utc=cutoff,
        daily_discovery_evidence=discovery_evidence,
        lifecycle_catalog=verified["catalog"],
    )

    run_identity = create_run_identity(
        cutoff_exclusive_utc=cutoff,
        lifecycle_evidence_valid_through_utc=lifecycle_horizon,
        lifecycle_bundle_id=verified["bundle"]["bundle_id"],
        lifecycle_approval_id=verified_approval["approval"]["approval_id"],
        lifecycle_evidence_code_commit=lifecycle_commit,
        acquisition_executable_commit=acquisition_auth["acquisition_executable_commit"],
        acquisition_executable_tree_sha256=acquisition_executable_tree_sha256(root),
        acquisition_authorization_id=acquisition_auth["authorization_id"],
        config_sha256=verified["bundle"]["config"]["sha256"],
        source_inventory_sha256=inventory["inventory_sha256"],
        split_definitions=config["splits"],
        run_id=run_id,
    )
    plan = create_frozen_plan(
        run_identity=run_identity,
        lifecycle_bundle=str(bundle_path),
        lifecycle_approval=str(approval_path),
        acquisition_authorization=str(acquisition_auth_path),
        source_inventory_payload=inventory,
        monthly_daily_boundary_utc=boundary,
        server_time_evidence_path=str(server_time_path),
        server_time_evidence_sha256=server_time_sha,
        daily_discovery_pages=daily_pages,
    )
    plan_path = root / "reports" / f"full_source_plan_{run_id}.json"
    write_json_exclusive(plan_path, plan)
    print(f"Prepared {len(inventory['entries']):,} exact source entries in {plan_path}")
    print(f"Frozen completed-1H cutoff: {cutoff}")
    print("No historical market payload was downloaded.")


if __name__ == "__main__":
    main()
