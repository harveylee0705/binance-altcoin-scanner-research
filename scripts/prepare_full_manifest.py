from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from alt_hot_scanner.data.binance_public import (
    collision_resistant_run_id,
    validate_archive_object_key,
    write_json_exclusive,
)
from alt_hot_scanner.universe.authorization import (
    PLAN_SCHEMA_VERSION,
    build_plan_integrity,
    verify_lifecycle_bundle,
)
from alt_hot_scanner.universe.contracts import filter_instrument_scope
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
    parser.add_argument(
        "--bundle",
        help="Content-bound lifecycle bundle; defaults to the newest local bundle",
    )
    return parser.parse_args()


def prepare_plan_payload(
    bundle_path: Path,
    *,
    end_month: pd.Period,
) -> dict:
    verified = verify_lifecycle_bundle(bundle_path)
    if not verified["readiness"]["authorization_ready"]:
        raise RuntimeError("Lifecycle/integrity gate is incomplete; do not prepare full history")
    config = load_config(verified["config_path"])
    catalog = verified["catalog"].copy()
    catalog["underlying_subtype"] = catalog["underlying_subtype"].map(
        lambda value: tuple(json.loads(value)) if isinstance(value, str) else value
    )
    scoped = filter_instrument_scope(catalog, config["universe"]["stablecoin_underlyings"])
    ready = scoped.loc[scoped["historical_inclusion_readiness"].eq("ready")]
    symbols = set(ready["symbol"])
    if not symbols:
        raise RuntimeError("No lifecycle-approved symbols are ready for acquisition planning")
    archive_rows = json.loads(
        verified["artifacts"]["archive_observations.json"].read_text(encoding="utf-8")
    )
    start_month = pd.Timestamp(config["data"]["start"]).tz_localize(None).to_period("M")
    warmup_start = start_month - 1
    objects: list[str] = []
    for row in archive_rows:
        if row.get("symbol") not in symbols:
            continue
        observed = row.get("observed_archive_object_keys")
        if type(observed) is not list:
            raise ValueError("Archive observations lack exact observed ZIP object keys")
        for key in observed:
            identity = validate_archive_object_key(key)
            period = pd.Period(identity.period, freq="M")
            if warmup_start <= period <= end_month:
                objects.append(identity.object_key)
    objects = sorted(set(objects))
    if not objects:
        raise RuntimeError("No observed archive ZIP objects satisfy the approved date policy")
    bundle = verified["bundle"]
    payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "purpose": "authorized_full_history_archive_download",
        "source": config["data"]["archive_index_url"],
        "lifecycle_bundle": str(Path(bundle_path).resolve()),
        "lifecycle_bundle_id": bundle["bundle_id"],
        "artifact_hashes": {
            name: descriptor["sha256"] for name, descriptor in bundle["artifacts"].items()
        },
        "config_sha256": bundle["config"]["sha256"],
        "readiness": verified["readiness"],
        "planning_basis": "exact_observed_monthly_zip_objects",
        "warmup_start_month": str(warmup_start),
        "end_month": str(end_month),
        "symbols_discovered": len(symbols),
        "objects": objects,
    }
    payload["plan_integrity"] = build_plan_integrity(payload)
    return payload


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    requested_config = (root / args.config).resolve()
    if args.bundle:
        bundle_path = Path(args.bundle).resolve()
    else:
        candidates = sorted((root / "reports" / "lifecycle").glob("*/lifecycle_bundle.json"))
        if not candidates:
            raise RuntimeError("Build and approve a lifecycle bundle before preparing full history")
        bundle_path = candidates[-1]
    verified = verify_lifecycle_bundle(bundle_path)
    if verified["config_path"] != requested_config:
        raise RuntimeError("Requested config is not the exact config bound into the lifecycle bundle")
    now = pd.Timestamp.now(tz="UTC")
    end = pd.Period(args.end_month, freq="M") if args.end_month else last_completed_month(now)
    payload = prepare_plan_payload(bundle_path, end_month=end)
    run_id = collision_resistant_run_id()
    target = root / "reports" / f"full_download_plan_{run_id}.json"
    write_json_exclusive(target, payload)
    print(f"Prepared {len(payload['objects']):,} candidate object keys in {target}")
    print("No full-history market data was downloaded.")


if __name__ == "__main__":
    main()
