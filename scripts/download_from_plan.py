from __future__ import annotations

import argparse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from alt_hot_scanner.data.acquisition import (
    ATTEMPT_SCHEMA_VERSION,
    AcquisitionInvariantError,
    acquire_api_source,
    acquire_archive_source,
    build_raw_completion_manifest,
    digest_json,
    validate_acquisition_archive_key,
    verify_frozen_plan,
)
from alt_hot_scanner.data.binance_public import (
    ArchiveAcquisitionError,
    collision_resistant_run_id,  # noqa: F401 - legacy test/export compatibility
    write_json_exclusive,
)
from alt_hot_scanner.universe.authorization import verify_runtime_matches_approved_commit


def key_fields(object_key: str) -> dict[str, str]:
    identity = validate_acquisition_archive_key(object_key)
    return {"symbol": identity.symbol, "interval": identity.interval, "period": identity.period}


def failure_attempt(object_key: str, exc: Exception) -> dict:
    """Backward-compatible diagnostic helper for archive-only unit tests."""
    validate_acquisition_archive_key(object_key)
    status = "failed"
    if isinstance(exc, ArchiveAcquisitionError):
        if exc.stage == "archive_payload" and exc.status_code == 404:
            status = "missing"
    elif isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
        status = "missing"
    return {
        "object_key": object_key,
        "url": f"https://data.binance.vision/{object_key}",
        "checksum_url": f"https://data.binance.vision/{object_key}.CHECKSUM",
        **key_fields(object_key),
        "status": status,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "published_sha256": getattr(exc, "published_sha256", None),
        "computed_sha256": getattr(exc, "computed_sha256", None),
        "byte_count": getattr(exc, "byte_count", None),
        "local_path": getattr(exc, "local_path", None),
        "checksum_verified": False,
        "payload_source": None,
        "http_result": getattr(exc, "code", None) or "request_or_validation_failed",
        "failure_stage": getattr(exc, "stage", "unclassified_local_or_request_failure"),
        "failure_url": getattr(exc, "request_url", None),
        "error": f"{type(exc).__name__}: {exc}",
    }


def verify_bound_plan(path: str | Path) -> dict:
    """Compatibility name; canonical implementation verifies the new frozen mixed-source plan."""
    root = Path(__file__).resolve().parents[1]
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    return verify_frozen_plan(candidate, root)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Acquire exact frozen mixed-source plan")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, help="Bounded smoke run; never canonical")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def _failure(entry, exc: Exception) -> dict:
    status = "failed"
    if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
        status = "missing"
    return {
        "source_kind": entry.source_kind,
        "symbol": entry.symbol,
        "object_key_or_request": entry.object_key_or_request,
        "status": status,
        "raw_path": None,
        "raw_sha256": None,
        "byte_count": None,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "published_sha256": None,
        "checksum_sidecar_path": None,
        "checksum_sidecar_sha256": None,
        "payload_source": None,
        "upstream_revision_detected": False,
        "error": f"{type(exc).__name__}: {exc}",
    }


def main() -> None:
    args = parse_args()
    if not 1 <= args.workers <= 16:
        raise ValueError("--workers must be between 1 and 16")
    if args.limit is not None and args.limit < 0:
        raise ValueError("--limit must not be negative")
    root = Path(__file__).resolve().parents[1]
    verified = verify_bound_plan(root / args.plan)
    if "source_entries" not in verified:
        # Legacy unit-test compatibility only; real mixed-source plans always contain source_entries.
        commit = verified["verified_approval"]["approval"]["lifecycle_evidence_code_commit"]
        verify_runtime_matches_approved_commit(root, commit)
        if not args.execute:
            print("Dry run only. Pass --execute to acquire raw bytes.")
            return
        verify_runtime_matches_approved_commit(root, commit)
        return
    entries = verified["source_entries"]
    selected = entries if args.limit is None else entries[: args.limit]
    print(f"Validated {len(selected):,} selected sources ({len(entries):,} total).")
    if not args.execute:
        print("Dry run only. Pass --execute to acquire raw bytes.")
        return

    raw_root = root / "data" / "raw"
    cutoff = verified["plan"]["run_identity"]["cutoff_exclusive_utc"]

    run_id = verified["plan"]["run_identity"]["run_id"]

    def acquire(entry):
        if entry.source_kind == "api":
            return acquire_api_source(entry, raw_root, cutoff, run_id=run_id)
        return acquire_archive_source(entry, raw_root, run_id=run_id)

    attempts: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        pending = {executor.submit(acquire, entry): entry for entry in selected}
        for future in as_completed(pending):
            entry = pending[future]
            try:
                attempts.append({**asdict(future.result()), "error": None})
            except (AcquisitionInvariantError, OSError, ValueError, urllib.error.URLError) as exc:
                attempts.append(_failure(entry, exc))

    attempts.sort(key=lambda row: row["object_key_or_request"])
    manifest_core = {
        "schema_version": ATTEMPT_SCHEMA_VERSION,
        "plan_path": str(verified["plan_path"]),
        "plan_integrity": verified["plan"]["plan_integrity"],
        "run_id": verified["plan"]["run_identity"]["run_id"],
        "canonical": args.limit is None,
        "attempts": attempts,
    }
    manifest = {**manifest_core, "attempt_id": digest_json(manifest_core)}
    manifest_dir = raw_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    attempt_path = manifest_dir / f"attempt_{manifest['attempt_id']}.json"
    write_json_exclusive(attempt_path, manifest)

    verified_count = sum(row["status"] == "verified" for row in attempts)
    missing_count = sum(row["status"] == "missing" for row in attempts)
    failed_count = sum(row["status"] == "failed" for row in attempts)
    print(f"Verified {verified_count:,}; missing {missing_count:,}; failed {failed_count:,}.")
    print(f"Attempt manifest: {attempt_path}")

    if args.limit is None and missing_count == 0 and failed_count == 0:
        completion = build_raw_completion_manifest(verified, attempt_path, raw_root)
        completion_path = manifest_dir / f"raw_completion_{completion['completion_id']}.json"
        write_json_exclusive(completion_path, completion)
        print(f"Raw completion manifest: {completion_path}")
    if missing_count or failed_count:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
