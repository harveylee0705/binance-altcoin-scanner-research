from __future__ import annotations

import argparse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from alt_hot_scanner.data.binance_public import (
    ArchiveAcquisitionError,
    collision_resistant_run_id,
    download_verified_archive,
    validate_archive_object_key,
    write_json_exclusive,
)
from alt_hot_scanner.universe.authorization import (
    verify_bound_plan,
    verify_runtime_matches_approved_commit,
)


def key_fields(object_key: str) -> dict[str, str]:
    identity = validate_archive_object_key(object_key)
    return {
        "symbol": identity.symbol,
        "interval": identity.interval,
        "period": identity.period,
    }


def failure_attempt(object_key: str, exc: Exception) -> dict:
    """Preserve failure stage and all checksum evidence; only archive 404 means missing."""
    if isinstance(exc, ArchiveAcquisitionError):
        status = (
            "missing" if exc.stage == "archive_payload" and exc.status_code == 404 else "failed"
        )
        failure_stage = exc.stage
        failure_url = exc.request_url
        status_code = exc.status_code
        published = exc.published_sha256
        computed = exc.computed_sha256
        byte_count = exc.byte_count
        local_path = exc.local_path
    else:
        status = "failed"
        failure_stage = "unclassified_local_or_request_failure"
        failure_url = None
        status_code = getattr(exc, "code", None)
        published = None
        computed = None
        byte_count = None
        local_path = None
    return {
        "object_key": object_key,
        "url": f"https://data.binance.vision/{object_key}",
        "checksum_url": f"https://data.binance.vision/{object_key}.CHECKSUM",
        **key_fields(object_key),
        "status": status,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "published_sha256": published,
        "computed_sha256": computed,
        "byte_count": byte_count,
        "local_path": local_path,
        "checksum_verified": False,
        "payload_source": None,
        "http_result": status_code or "request_or_validation_failed",
        "failure_stage": failure_stage,
        "failure_url": failure_url,
        "error": f"{type(exc).__name__}: {exc}",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resumable checksum-verified archive downloader")
    parser.add_argument("--plan", required=True, help="Exact prepared plan path")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, help="Optional bounded smoke-run object count")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Required safety switch; without it only validates and summarizes the plan",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.workers <= 16:
        raise ValueError("--workers must be between 1 and 16")
    root = Path(__file__).resolve().parents[1]
    plan_path = root / args.plan
    verified_plan = verify_bound_plan(plan_path)
    plan = verified_plan["plan"]
    approved_commit = verified_plan["verified_approval"]["approval"][
        "lifecycle_evidence_code_commit"
    ]
    verify_runtime_matches_approved_commit(root, approved_commit)
    if args.limit is not None and args.limit < 0:
        raise ValueError("--limit must not be negative")
    selected = plan["objects"][: args.limit]
    objects = [validate_archive_object_key(key).object_key for key in selected]
    print(
        f"Validated plan with {len(objects):,} selected objects ({len(plan['objects']):,} total)."
    )
    if not args.execute:
        print("Dry run only. Pass --execute to begin downloads.")
        return

    verify_runtime_matches_approved_commit(root, approved_commit)
    raw_root = root / "data" / "raw"
    attempts: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        pending = {
            executor.submit(download_verified_archive, key, raw_root): key for key in objects
        }
        for future in as_completed(pending):
            key = pending[future]
            try:
                record = future.result()
                attempt = asdict(record)
                attempt.update(
                    {
                        **key_fields(key),
                        "status": "verified",
                        "http_result": record.payload_source,
                        "failure_stage": None,
                        "failure_url": None,
                        "error": None,
                    }
                )
                attempts.append(attempt)
            except (ArchiveAcquisitionError, OSError, ValueError, urllib.error.URLError) as exc:
                attempts.append(failure_attempt(key, exc))

    run_id = collision_resistant_run_id()
    manifest_path = raw_root / "manifests" / f"full_download_attempts_{run_id}.json"
    attempts.sort(key=lambda item: item["object_key"])
    write_json_exclusive(manifest_path, attempts)
    verified = sum(attempt["status"] == "verified" for attempt in attempts)
    missing = sum(attempt["status"] == "missing" for attempt in attempts)
    failures = sum(attempt["status"] == "failed" for attempt in attempts)
    print(f"Verified {verified:,}; expected-but-missing {missing:,}; failed {failures:,}.")
    print(f"Attempt manifest: {manifest_path}")
    if failures:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
