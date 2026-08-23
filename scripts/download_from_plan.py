from __future__ import annotations

import argparse
import json
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from alt_hot_scanner.data.binance_public import download_verified_archive


def key_fields(object_key: str) -> dict[str, str]:
    parts = object_key.split("/")
    symbol = parts[5]
    interval = parts[6]
    period = parts[-1].removeprefix(f"{symbol}-{interval}-").removesuffix(".zip")
    return {"symbol": symbol, "interval": interval, "period": period}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resumable checksum-verified archive downloader")
    parser.add_argument("--plan", default="reports/full_download_plan.json")
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
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    objects = plan["objects"][: args.limit]
    print(
        f"Validated plan with {len(objects):,} selected objects ({len(plan['objects']):,} total)."
    )
    if not args.execute:
        print("Dry run only. Pass --execute to begin downloads.")
        return

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
                        "error": None,
                    }
                )
                attempts.append(attempt)
            except (OSError, ValueError, urllib.error.URLError) as exc:
                status = (
                    "missing"
                    if isinstance(exc, urllib.error.HTTPError) and exc.code == 404
                    else "failed"
                )
                attempts.append(
                    {
                        "object_key": key,
                        "url": f"https://data.binance.vision/{key}",
                        **key_fields(key),
                        "status": status,
                        "retrieved_at": datetime.now(UTC).isoformat(),
                        "published_sha256": None,
                        "computed_sha256": None,
                        "byte_count": None,
                        "local_path": None,
                        "checksum_verified": False,
                        "payload_source": None,
                        "http_result": 404
                        if status == "missing"
                        else "request_or_validation_failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    manifest_path = raw_root / "manifests" / f"full_download_attempts_{run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(attempts, indent=2), encoding="utf-8")
    verified = sum(attempt["status"] == "verified" for attempt in attempts)
    missing = sum(attempt["status"] == "missing" for attempt in attempts)
    failures = sum(attempt["status"] == "failed" for attempt in attempts)
    print(f"Verified {verified:,}; expected-but-missing {missing:,}; failed {failures:,}.")
    print(f"Attempt manifest: {manifest_path}")
    if failures:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
