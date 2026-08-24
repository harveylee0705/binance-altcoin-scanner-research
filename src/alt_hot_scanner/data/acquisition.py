from __future__ import annotations

"""Performance-blind, frozen acquisition/build contracts for Binance USD-M 1H data."""

import hashlib
import json
import os
import tempfile
import urllib.parse
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.data.binance_public import _read_url
from alt_hot_scanner.data.normalize import validate_normalized_1h

SOURCE_POLICY_VERSION = "monthly-daily-api-v1"
GAP_KINDS = {
    "expected_lifecycle_gap", "pre_post_lifecycle_non_source_period",
    "unexplained_missing_planned_archive", "missing_1h_row_inside_expected_source_interval",
    "duplicate_timestamp", "source_period_spillover", "other_unresolved_gap",
}

class AcquisitionInvariantError(ValueError):
    pass

@dataclass(frozen=True)
class FrozenRunIdentity:
    cutoff_exclusive_utc: str
    lifecycle_bundle_id: str
    lifecycle_approval_id: str
    executable_commit: str
    config_sha256: str
    source_inventory_sha256: str
    source_policy_version: str
    split_definitions: dict[str, Any]
    run_id: str

@dataclass(frozen=True)
class SourceEntry:
    source_kind: str
    symbol: str
    object_key_or_request: str
    source_period: str
    checksum_sha256: str | None
    discovery_evidence: tuple[str, ...]
    selected_canonical_range: tuple[str, str]
    raw_path: str | None = None
    raw_sha256: str | None = None

@dataclass(frozen=True)
class GapRecord:
    kind: str
    symbol: str
    start: str | None
    end: str | None
    evidence: str
    resolution: str | None = None


def floor_to_1h(value: pd.Timestamp | datetime) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").floor("h")


def freeze_cutoff_from_server(server_time_ms: int) -> str:
    if type(server_time_ms) is not int or server_time_ms < 0:
        raise ValueError("Binance server time must be a non-negative integer milliseconds value")
    return floor_to_1h(pd.to_datetime(server_time_ms, unit="ms", utc=True)).isoformat()


def fetch_frozen_cutoff(server_reader: Callable[[], bytes] = lambda: _read_url("https://fapi.binance.com/fapi/v1/time")) -> str:
    payload = json.loads(server_reader())
    if set(payload) != {"serverTime"} or type(payload["serverTime"]) is not int:
        raise AcquisitionInvariantError("Binance server time response is not exact")
    return freeze_cutoff_from_server(payload["serverTime"])


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def create_run_identity(*, cutoff_exclusive_utc: str, lifecycle_bundle_id: str, lifecycle_approval_id: str,
                        executable_commit: str, config_sha256: str, source_inventory_sha256: str,
                        split_definitions: dict[str, Any], run_id: str) -> FrozenRunIdentity:
    cutoff = floor_to_1h(pd.Timestamp(cutoff_exclusive_utc)).isoformat()
    return FrozenRunIdentity(cutoff, lifecycle_bundle_id, lifecycle_approval_id, executable_commit,
                             config_sha256, source_inventory_sha256, SOURCE_POLICY_VERSION,
                             split_definitions, run_id)


def assert_cutoff_frozen(identity: FrozenRunIdentity, candidate: str) -> None:
    if floor_to_1h(pd.Timestamp(candidate)).isoformat() != identity.cutoff_exclusive_utc:
        raise AcquisitionInvariantError("Frozen run cutoff cannot move")


def source_inventory(entries: list[SourceEntry]) -> dict[str, Any]:
    rows = [asdict(entry) for entry in sorted(entries, key=lambda x: (x.symbol, x.source_kind, x.object_key_or_request))]
    return {"schema_version": "source-inventory-v1", "source_policy_version": SOURCE_POLICY_VERSION,
            "entries": rows, "inventory_sha256": digest_json(rows)}


def require_exact_inventory(entries: list[SourceEntry], inventory: dict[str, Any]) -> None:
    expected = inventory.get("entries")
    actual = [asdict(x) for x in sorted(entries, key=lambda x: (x.symbol, x.source_kind, x.object_key_or_request))]
    if inventory.get("inventory_sha256") != digest_json(expected) or actual != expected:
        raise AcquisitionInvariantError("Source set is not the frozen exact inventory")


def parse_daily_kline_key(key: str) -> tuple[str, str]:
    from alt_hot_scanner.data.binance_public import validate_archive_object_key
    identity = validate_archive_object_key(key)
    if identity.source_kind != "daily":
        raise ValueError("Expected official daily 1H kline object key")
    return identity.symbol, identity.period


def source_period_bounds(source_kind: str, period: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(period, tz="UTC")
    end = (start + (pd.offsets.MonthBegin(1) if source_kind == "monthly" else pd.Timedelta(days=1)))
    return start, end


def validate_source_containment(frame: pd.DataFrame, source_kind: str, period: str) -> None:
    start, end = source_period_bounds(source_kind, period)
    if frame.empty:
        return
    if frame["open_time"].min() < start or frame["open_time"].max() >= end:
        raise AcquisitionInvariantError("Rows spill outside source period encoded by object key")


def classify_gaps(symbol: str, timestamps: pd.Series, expected_start: str | None = None,
                  expected_end: str | None = None, *, lifecycle_gap: bool = False) -> list[GapRecord]:
    values = pd.Series(pd.to_datetime(timestamps, utc=True)).sort_values().drop_duplicates()
    result: list[GapRecord] = []
    if lifecycle_gap:
        result.append(GapRecord("expected_lifecycle_gap", symbol, expected_start, expected_end, "lifecycle bundle"))
    if values.empty:
        if expected_start and expected_end:
            result.append(GapRecord("unexplained_missing_planned_archive", symbol, expected_start, expected_end, "empty source"))
        return result
    diffs = values.diff().dropna()
    for idx in diffs[diffs > pd.Timedelta(hours=1)].index:
        previous = values.loc[idx - 1] if idx > 0 else values.iloc[0]
        current = values.loc[idx]
        result.append(GapRecord("missing_1h_row_inside_expected_source_interval", symbol,
                                previous.isoformat(), current.isoformat(), "non-consecutive canonical timestamps"))
    return result


def validate_canonical_1h(frame: pd.DataFrame, cutoff_exclusive_utc: str, source_entries: list[SourceEntry]) -> None:
    validate_normalized_1h(frame)
    cutoff = pd.Timestamp(cutoff_exclusive_utc)
    if frame["open_time"].max() >= cutoff:
        raise AcquisitionInvariantError("Canonical 1H layer contains a bar at or after frozen cutoff")
    if "source_key" not in frame.columns or "source_sha256" not in frame.columns:
        raise AcquisitionInvariantError("Canonical 1H rows require source lineage")
    allowed = {entry.object_key_or_request for entry in source_entries}
    if not set(frame["source_key"]).issubset(allowed):
        raise AcquisitionInvariantError("Canonical rows cite a source outside the frozen inventory")


def api_tail_request(symbol: str, start: pd.Timestamp, cutoff_exclusive_utc: str) -> dict[str, Any] | None:
    start = floor_to_1h(start)
    end = pd.Timestamp(cutoff_exclusive_utc)
    if start >= end:
        return None
    return {"endpoint": "https://fapi.binance.com/fapi/v1/klines", "symbol": symbol,
            "interval": "1h", "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000) - 1, "limit": 1500}


def validate_api_tail(rows: list[list[Any]], symbol: str, start: pd.Timestamp, cutoff_exclusive_utc: str) -> None:
    expected = floor_to_1h(start)
    cutoff = pd.Timestamp(cutoff_exclusive_utc)
    seen: set[int] = set()
    for row in rows:
        if len(row) < 7:
            raise AcquisitionInvariantError("Malformed API kline row")
        opened = pd.to_datetime(int(row[0]), unit="ms", utc=True)
        if opened != expected or opened >= cutoff or int(row[0]) in seen:
            raise AcquisitionInvariantError("API is permitted only as a contiguous latest suffix")
        seen.add(int(row[0])); expected += pd.Timedelta(hours=1)


def store_api_response(raw_root: str | Path, symbol: str, request: dict[str, Any], payload: bytes, retrieved_at: str) -> SourceEntry:
    """Persist an API tail response as immutable raw evidence with request provenance."""
    safe_symbol = symbol.upper()
    request_id = hashlib.sha256(canonical_json(request)).hexdigest()
    target = Path(raw_root).resolve() / "api_tail" / safe_symbol / f"{request_id}.json"
    record = {"request": request, "retrieved_at": retrieved_at,
              "payload_sha256": hashlib.sha256(payload).hexdigest(),
              "payload": json.loads(payload)}
    raw = canonical_json(record)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() != raw:
            raise AcquisitionInvariantError("API raw response path is immutable")
    else:
        target.write_bytes(raw)
    return api_tail_entry(symbol, request, payload, retrieved_at)


def api_tail_entry(symbol: str, request: dict[str, Any], raw_payload: bytes, retrieved_at: str) -> SourceEntry:
    return SourceEntry(
        "api", symbol, request_url(request), f"{request['startTime']}-{request['endTime']}",
        hashlib.sha256(raw_payload).hexdigest(), ("fapi server request",),
        (pd.to_datetime(request["startTime"], unit="ms", utc=True).isoformat(),
         pd.to_datetime(request["endTime"] + 1, unit="ms", utc=True).isoformat()),
        raw_path=None, raw_sha256=hashlib.sha256(raw_payload).hexdigest(),
    )



def request_url(request: dict[str, Any]) -> str:
    return "https://fapi.binance.com/fapi/v1/klines?" + urllib.parse.urlencode(request)


def require_complete_attempt_manifest(plan: dict[str, Any], attempts: list[dict[str, Any]], *, canonical: bool = True) -> None:
    """Reject limited/manual manifests; completion is bound to the exact approved plan."""
    if not canonical:
        raise AcquisitionInvariantError("Smoke/limited attempt manifests are non-canonical")
    required = list(plan.get("objects", []))
    if plan.get("planning_basis") not in {"exact_observed_monthly_zip_objects", "exact_observed_source_plan"}:
        raise AcquisitionInvariantError("Plan is not an exact approved source plan")
    actual = {row.get("object_key") for row in attempts}
    if len(actual) != len(attempts) or actual != set(required):
        raise AcquisitionInvariantError("Attempt set is not the exact approved source plan")
    if any(row.get("status") != "verified" for row in attempts):
        raise AcquisitionInvariantError("Missing/failed source attempts block canonical completion")


def reject_unapproved_sources(entries: list[SourceEntry], inventory: dict[str, Any]) -> None:
    allowed = {row["object_key_or_request"] for row in inventory.get("entries", [])}
    extra = {row.object_key_or_request for row in entries} - allowed
    if extra:
        raise AcquisitionInvariantError(f"Unapproved sources: {sorted(extra)}")


def compare_source_rows(old: pd.DataFrame, new: pd.DataFrame, key: list[str] | None = None) -> dict[str, Any]:
    key = key or ["symbol", "open_time"]
    left = old.set_index(key).sort_index(); right = new.set_index(key).sort_index()
    common = left.index.intersection(right.index)
    changed = int((left.loc[common].compare(right.loc[common]) != 0).any(axis=1).sum()) if len(common) else 0
    return {"common_rows": len(common), "changed_rows": changed,
            "classification": "upstream_source_revision" if changed else "identical"}


def classify_raw_conflict(expected_sha256: str, actual_sha256: str, published_sha256: str | None) -> str:
    if actual_sha256 == expected_sha256:
        return "verified"
    if published_sha256 and actual_sha256 == published_sha256:
        return "upstream_source_revision"
    return "local_corruption"


def publish_staged(path: str | Path, writer: Callable[[Path], None]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        writer(stage)
        marker = stage / "_SUCCESS"
        marker.write_text("complete\n", encoding="utf-8")
        try:
            os.rename(stage, target)
        except OSError as exc:
            if target.exists():
                raise FileExistsError(target) from exc
            raise
        return str(target)
    except Exception:
        if stage.exists():
            import shutil
            shutil.rmtree(stage, ignore_errors=True)
        raise


def gate_artifact(name: str, checks: dict[str, bool], details: dict[str, Any] | None = None) -> dict[str, Any]:
    if not checks or not all(checks.values()):
        status = "failed"
    else:
        status = "passed"
    return {"schema_version": "performance-blind-gate-v1", "gate": name, "status": status,
            "checks": checks, "details": details or {}, "performance_aggregate_emitted": False}


def require_gate_pass(artifact: dict[str, Any], allowed: set[str]) -> None:
    if artifact.get("gate") not in allowed or artifact.get("status") != "passed" or artifact.get("performance_aggregate_emitted"):
        raise AcquisitionInvariantError("Engineering gate is not a passing performance-blind artifact")
