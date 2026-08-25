from __future__ import annotations

"""Canonical, performance-blind full-history build stages A through E.

No function in this module computes aggregate Scanner performance.  Gate artifacts contain only
structural invariants, row counts, lineage hashes, and split/file identities.
"""

import hashlib
import io
import json
import urllib.parse
from pathlib import Path
from typing import Any

import pandas as pd

from alt_hot_scanner.analysis.splits import classify_split
from alt_hot_scanner.data.acquisition import (
    AcquisitionInvariantError,
    SourceEntry,
    derived_gate,
    digest_json,
    publish_staged,
    require_gate_pass,
    sha256_path,
    validate_api_tail,
    validate_canonical_1h,
    validate_source_containment,
    verify_raw_completion,
    verify_source_policy,
)
from alt_hot_scanner.data.aggregate import aggregate_1h_to_4h
from alt_hot_scanner.data.binance_public import write_json_exclusive
from alt_hot_scanner.data.normalize import normalize_kline_frame, read_kline_zip
from alt_hot_scanner.features.core import add_time_series_features
from alt_hot_scanner.outcomes.labels import add_outcome_labels
from alt_hot_scanner.scanner.episodes import add_hot_episodes
from alt_hot_scanner.scanner.scoring import add_cross_sectional_scanner
from alt_hot_scanner.universe.eligibility import apply_point_in_time_eligibility
from alt_hot_scanner.utils.config import load_config

STAGE_MANIFEST_SCHEMA = "canonical-stage-manifest-v1"


def _sha_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _frame_logical_sha(frame: pd.DataFrame) -> str:
    ordered = frame.copy()
    sort_keys = [key for key in ("symbol", "open_time") if key in ordered.columns]
    if sort_keys:
        ordered = ordered.sort_values(sort_keys).reset_index(drop=True)
    payload = ordered.to_json(
        orient="records",
        date_format="iso",
        date_unit="ms",
        double_precision=15,
        force_ascii=True,
    ).encode("utf-8")
    return _sha_bytes(payload)


def _write_frame(stage: Path, frame: pd.DataFrame, stem: str = "data") -> dict[str, Any]:
    ordered = frame.copy()
    sort_keys = [key for key in ("symbol", "open_time") if key in ordered.columns]
    if sort_keys:
        ordered = ordered.sort_values(sort_keys).reset_index(drop=True)
    try:
        import pyarrow  # noqa: F401

        filename = f"{stem}.parquet"
        path = stage / filename
        ordered.to_parquet(path, index=False)
        storage = "parquet"
    except ImportError:
        filename = f"{stem}.table.json"
        path = stage / filename
        payload = ordered.to_json(
            orient="table",
            date_format="iso",
            date_unit="ms",
            double_precision=15,
            force_ascii=True,
            index=False,
        ).encode("utf-8")
        path.write_bytes(payload)
        storage = "json_table_fallback"
    return {
        "file": filename,
        "storage_format": storage,
        "file_sha256": sha256_path(path),
        "logical_sha256": _frame_logical_sha(ordered),
        "row_count": len(ordered),
        "columns": list(ordered.columns),
    }


def _read_frame(stage: Path, descriptor: dict[str, Any]) -> pd.DataFrame:
    path = stage / descriptor["file"]
    if not path.is_file() or sha256_path(path) != descriptor["file_sha256"]:
        raise AcquisitionInvariantError("Canonical stage data file is missing or changed")
    if descriptor["storage_format"] == "parquet":
        frame = pd.read_parquet(path)
    elif descriptor["storage_format"] == "json_table_fallback":
        frame = pd.read_json(io.StringIO(path.read_text(encoding="utf-8")), orient="table")
    else:
        raise AcquisitionInvariantError("Canonical stage storage format is unsupported")
    if list(frame.columns) != descriptor["columns"] or len(frame) != descriptor["row_count"]:
        raise AcquisitionInvariantError("Canonical stage schema/row count changed")
    if _frame_logical_sha(frame) != descriptor["logical_sha256"]:
        raise AcquisitionInvariantError("Canonical stage logical content changed")
    return frame


def _write_stage_manifest(stage: Path, core: dict[str, Any]) -> dict[str, Any]:
    payload = {"schema_version": STAGE_MANIFEST_SCHEMA, **core}
    payload["manifest_id"] = digest_json(payload)
    path = stage / "manifest.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def load_stage(stage_path: str | Path, expected_stage: str) -> tuple[dict[str, Any], pd.DataFrame]:
    stage = Path(stage_path).resolve(strict=True)
    if not (stage / "_SUCCESS").is_file():
        raise AcquisitionInvariantError("Canonical stage lacks success marker")
    manifest_path = stage / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != STAGE_MANIFEST_SCHEMA or manifest.get("stage") != expected_stage:
        raise AcquisitionInvariantError("Canonical stage manifest schema/name is invalid")
    supplied_id = manifest.get("manifest_id")
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    if supplied_id != digest_json(core):
        raise AcquisitionInvariantError("Canonical stage manifest identity is invalid")
    frame = _read_frame(stage, manifest["data"])
    return manifest, frame


def _load_gate(path: str | Path, allowed: set[str]) -> dict[str, Any]:
    artifact = json.loads(Path(path).read_text(encoding="utf-8"))
    require_gate_pass(artifact, allowed)
    return artifact


def write_gate(path: str | Path, artifact: dict[str, Any]) -> None:
    require_gate_pass(artifact, {artifact["gate"]})
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    write_json_exclusive(target, artifact)


def derive_gate_a(
    verified_plan: dict[str, Any], completion_path: str | Path, raw_root: str | Path
) -> dict[str, Any]:
    completion = verify_raw_completion(completion_path, verified_plan, raw_root)
    cutoff = verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"]
    verify_source_policy(verified_plan["source_entries"], cutoff)
    expected = {entry.object_key_or_request for entry in verified_plan["source_entries"]}
    actual = {row["object_key_or_request"] for row in completion["source_records"]}
    return derived_gate(
        "A",
        {
            "plan_integrity": verified_plan["plan"]["plan_integrity"],
            "raw_completion_id": completion["completion_id"],
        },
        {
            "authoritative_plan_reverified": True,
            "exact_source_set_complete": actual == expected,
            "raw_hashes_reverified": True,
            "cutoff_frozen": bool(verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"]),
            "source_policy_reverified": True,
        },
        {
            "source_count": len(actual),
            "run_id": completion["run_id"],
        },
    )


def _api_frame(
    raw_path: Path, entry: SourceEntry, cutoff_exclusive_utc: str
) -> pd.DataFrame:
    raw = raw_path.read_bytes()
    payload = json.loads(raw)
    if type(payload) is not list or not payload:
        raise AcquisitionInvariantError("Verified API raw page is empty/malformed at processing")
    parsed = urllib.parse.urlsplit(entry.object_key_or_request)
    query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
    request = {
        "endpoint": f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
        "symbol": query["symbol"][0],
        "interval": query["interval"][0],
        "startTime": int(query["startTime"][0]),
        "endTime": int(query["endTime"][0]),
        "limit": int(query["limit"][0]),
    }
    if request["symbol"] != entry.symbol:
        raise AcquisitionInvariantError("API raw source symbol differs from frozen request")
    validate_api_tail(payload, request, cutoff_exclusive_utc)
    return normalize_kline_frame(pd.DataFrame(payload), entry.symbol)


def _catalog_active_intervals(
    catalog: pd.DataFrame, symbol: str, clip_start: pd.Timestamp, clip_end: pd.Timestamp
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    rows = catalog.loc[catalog["symbol"].eq(symbol)]
    if len(rows) != 1:
        raise AcquisitionInvariantError(f"Lifecycle catalog identity missing/duplicated: {symbol}")
    row = rows.iloc[0]
    intervals = row.get("lifecycle_intervals")
    if isinstance(intervals, str) and intervals:
        intervals = json.loads(intervals)
    candidates: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    if intervals:
        for interval in intervals:
            start = pd.Timestamp(interval["age_live_anchor_at"])
            last = interval.get("last_trading_at")
            end = pd.Timestamp(last).floor("h") + pd.Timedelta(hours=1) if last else clip_end
            candidates.append((start.floor("h"), end))
    else:
        start_value = row.get("eligibility_age_anchor_at") or row.get("first_observed_trade_at")
        if pd.notna(start_value):
            start = pd.Timestamp(start_value).floor("h")
            last = row.get("last_trading_at")
            end = pd.Timestamp(last).floor("h") + pd.Timedelta(hours=1) if pd.notna(last) else clip_end
            candidates.append((start, end))
    clipped: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in candidates:
        start = max(start, clip_start)
        end = min(end, clip_end)
        if start < end:
            clipped.append((start, end))
    return clipped


def _required_acquisition_window(verified_plan: dict[str, Any]) -> tuple[pd.Timestamp, pd.Timestamp]:
    config = load_config(verified_plan["verified_bundle"]["config_path"])
    research_start = pd.Timestamp(config["data"]["start"])
    if research_start.tzinfo is None:
        research_start = research_start.tz_localize("UTC")
    else:
        research_start = research_start.tz_convert("UTC")
    warmup_month = research_start.tz_localize(None).to_period("M") - 1
    required_start = warmup_month.start_time.tz_localize("UTC")
    cutoff = pd.Timestamp(verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"])
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    if required_start >= cutoff:
        raise AcquisitionInvariantError("Required acquisition window is empty or reversed")
    return required_start, cutoff


def _active_gap_records(
    frame: pd.DataFrame,
    catalog: pd.DataFrame,
    expected_symbols: set[str],
    required_start: pd.Timestamp,
    cutoff: pd.Timestamp,
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    records: list[dict[str, Any]] = []
    boundary_map: dict[str, list[str]] = {}
    for symbol in sorted(expected_symbols):
        active = _catalog_active_intervals(catalog, symbol, required_start, cutoff)
        symbol_times = frame.loc[frame["symbol"].eq(symbol), "open_time"]
        boundary_map[symbol] = sorted({point.isoformat() for pair in active for point in pair})
        for start, end in active:
            values = symbol_times.loc[(symbol_times >= start) & (symbol_times < end)].sort_values()
            if values.empty:
                records.append(
                    {
                        "kind": "active_interval_empty",
                        "symbol": symbol,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    }
                )
                continue
            if values.iloc[0] > start:
                records.append(
                    {
                        "kind": "leading_active_gap",
                        "symbol": symbol,
                        "start": start.isoformat(),
                        "end": values.iloc[0].isoformat(),
                    }
                )
            for previous, current in zip(values.iloc[:-1], values.iloc[1:]):
                if current - previous > pd.Timedelta(hours=1):
                    records.append(
                        {
                            "kind": "interior_active_gap",
                            "symbol": symbol,
                            "start": (previous + pd.Timedelta(hours=1)).isoformat(),
                            "end": current.isoformat(),
                        }
                    )
            expected_last_open = end - pd.Timedelta(hours=1)
            if values.iloc[-1] < expected_last_open:
                records.append(
                    {
                        "kind": "trailing_active_gap",
                        "symbol": symbol,
                        "start": (values.iloc[-1] + pd.Timedelta(hours=1)).isoformat(),
                        "end": end.isoformat(),
                    }
                )
    return records, boundary_map


def _expected_source_symbols(verified_plan: dict[str, Any]) -> set[str]:
    approved = verified_plan.get("approved_symbols")
    if approved is not None:
        return set(approved)
    # Synthetic/bounded integration fixtures predate the verifier return-field addition.
    return {entry.symbol for entry in verified_plan["source_entries"]}


def _rebuild_normalized_frame(
    verified_plan: dict[str, Any], completion_path: str | Path, raw_root: str | Path
) -> tuple[dict[str, Any], pd.DataFrame, list[dict[str, Any]], dict[str, list[str]]]:
    completion = verify_raw_completion(completion_path, verified_plan, raw_root)
    source_by_key = {entry.object_key_or_request: entry for entry in verified_plan["source_entries"]}
    frames: list[pd.DataFrame] = []
    for record in completion["source_records"]:
        entry = source_by_key[record["object_key_or_request"]]
        if entry.source_kind in {"monthly", "daily"}:
            frame = read_kline_zip(record["raw_path"], entry.symbol)
            validate_source_containment(frame, entry.source_kind, entry.source_period)
        else:
            frame = _api_frame(
                Path(record["raw_path"]),
                entry,
                verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"],
            )
            start = pd.Timestamp(entry.selected_canonical_range[0])
            end = pd.Timestamp(entry.selected_canonical_range[1])
            if frame["open_time"].min() < start or frame["open_time"].max() >= end:
                raise AcquisitionInvariantError("API rows spill outside selected canonical range")
        frame = frame.copy()
        frame["source_key"] = entry.object_key_or_request
        frame["source_sha256"] = record["raw_sha256"]
        frames.append(frame)
    if not frames:
        raise AcquisitionInvariantError("Raw completion contains no canonical market sources")
    hourly = pd.concat(frames, ignore_index=True).sort_values(["symbol", "open_time"]).reset_index(drop=True)
    validate_canonical_1h(
        hourly,
        verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"],
        verified_plan["source_entries"],
    )
    catalog = verified_plan["verified_bundle"]["catalog"]
    unknown = sorted(set(hourly["symbol"]) - set(catalog["symbol"]))
    if unknown:
        raise AcquisitionInvariantError(f"Canonical 1H contains unknown lifecycle identities: {unknown}")
    required_start, cutoff = _required_acquisition_window(verified_plan)
    gaps, boundaries = _active_gap_records(
        hourly, catalog, _expected_source_symbols(verified_plan), required_start, cutoff
    )
    if gaps:
        raise AcquisitionInvariantError(f"Unresolved active-source gaps block Gate B: {gaps[:5]}")
    return completion, hourly, gaps, boundaries


def build_normalized_1h_stage(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    target: str | Path,
) -> dict[str, Any]:
    gate_a = _load_gate(gate_a_path, {"A"})
    expected_gate = derive_gate_a(verified_plan, completion_path, raw_root)
    if gate_a != expected_gate:
        raise AcquisitionInvariantError("Stored Gate A is not the currently derived Gate A")
    completion, hourly, gaps, boundaries = _rebuild_normalized_frame(
        verified_plan, completion_path, raw_root
    )
    required_start, cutoff = _required_acquisition_window(verified_plan)

    output: dict[str, Any] = {}

    def writer(stage: Path) -> None:
        data = _write_frame(stage, hourly)
        output.update(
            _write_stage_manifest(
                stage,
                {
                    "stage": "normalized_1h",
                    "run_id": completion["run_id"],
                    "upstream_gate_id": gate_a["gate_id"],
                    "raw_completion_id": completion["completion_id"],
                    "data": data,
                    "active_gap_records": gaps,
                    "lifecycle_boundaries": boundaries,
                    "required_acquisition_window": [
                        required_start.isoformat(),
                        cutoff.isoformat(),
                    ],
                },
            )
        )

    publish_staged(target, writer)
    return output


def derive_gate_b(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    normalized_stage: str | Path,
) -> dict[str, Any]:
    gate_a = _load_gate(gate_a_path, {"A"})
    if gate_a != derive_gate_a(verified_plan, completion_path, raw_root):
        raise AcquisitionInvariantError("Gate B cannot trust stale/fabricated Gate A")
    manifest, frame = load_stage(normalized_stage, "normalized_1h")
    if manifest["upstream_gate_id"] != gate_a["gate_id"]:
        raise AcquisitionInvariantError("Normalized stage is not bound to Gate A")
    _, replay, replay_gaps, replay_boundaries = _rebuild_normalized_frame(
        verified_plan, completion_path, raw_root
    )
    if _frame_logical_sha(replay) != manifest["data"]["logical_sha256"]:
        raise AcquisitionInvariantError("Normalized stage is not exact reprocessing of verified raw sources")
    validate_canonical_1h(
        frame,
        verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"],
        verified_plan["source_entries"],
    )
    # Re-derive source containment from each row's frozen source lineage.
    entries = {entry.object_key_or_request: entry for entry in verified_plan["source_entries"]}
    for source_key, group in frame.groupby("source_key", sort=False):
        entry = entries.get(source_key)
        if entry is None:
            raise AcquisitionInvariantError("Normalized stage cites unplanned source")
        if entry.source_kind in {"monthly", "daily"}:
            validate_source_containment(group, entry.source_kind, entry.source_period)
        else:
            start = pd.Timestamp(entry.selected_canonical_range[0])
            end = pd.Timestamp(entry.selected_canonical_range[1])
            if group["open_time"].min() < start or group["open_time"].max() >= end:
                raise AcquisitionInvariantError("Normalized API lineage spills outside plan")
    required_start, cutoff = _required_acquisition_window(verified_plan)
    expected_window = [required_start.isoformat(), cutoff.isoformat()]
    if manifest.get("required_acquisition_window") != expected_window:
        raise AcquisitionInvariantError("Normalized stage required acquisition window changed")
    gaps, boundaries = _active_gap_records(
        frame,
        verified_plan["verified_bundle"]["catalog"],
        _expected_source_symbols(verified_plan),
        required_start,
        cutoff,
    )
    if gaps or replay_gaps or boundaries != replay_boundaries or boundaries != manifest["lifecycle_boundaries"]:
        raise AcquisitionInvariantError("Normalized stage gap/boundary evidence is not re-derived")
    return derived_gate(
        "B",
        {"gate_a_id": gate_a["gate_id"], "normalized_manifest_id": manifest["manifest_id"]},
        {
            "normalized_contract_reverified": True,
            "duplicate_keys_zero": not frame.duplicated(["symbol", "open_time"]).any(),
            "source_containment_reverified": True,
            "unresolved_active_gaps_zero": not gaps,
            "cutoff_reverified": True,
        },
        {
            "row_count": len(frame),
            "symbol_count": frame["symbol"].nunique(),
            "normalized_logical_sha256": manifest["data"]["logical_sha256"],
        },
    )


def _rejected_is_boundary(
    symbol: str,
    group_open: pd.Timestamp,
    boundary_map: dict[str, list[str]],
    cutoff: pd.Timestamp,
) -> bool:
    group_end = group_open + pd.Timedelta(hours=4)
    points = [pd.Timestamp(value) for value in boundary_map.get(symbol, [])]
    points.append(cutoff)
    return any(group_open < point < group_end for point in points)


def _require_current_gate_b(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
) -> dict[str, Any]:
    stored = _load_gate(gate_b_path, {"B"})
    expected = derive_gate_b(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        normalized_stage=normalized_stage,
    )
    if stored != expected:
        raise AcquisitionInvariantError("Stored Gate B is not the currently derived Gate B")
    return stored


def build_4h_stage(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    target: str | Path,
) -> dict[str, Any]:
    gate_b = _require_current_gate_b(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
    )
    normalized_manifest, hourly = load_stage(normalized_stage, "normalized_1h")
    if gate_b["upstream"].get("normalized_manifest_id") != normalized_manifest["manifest_id"]:
        raise AcquisitionInvariantError("4H build received normalized stage not authorized by Gate B")
    aggregate = aggregate_1h_to_4h(hourly)
    cutoff = pd.Timestamp(verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"])
    unexplained = [
        row
        for row in aggregate.rejected.to_dict("records")
        if not _rejected_is_boundary(
            row["symbol"],
            pd.Timestamp(row["open_time"]),
            normalized_manifest["lifecycle_boundaries"],
            cutoff,
        )
    ]
    if unexplained:
        raise AcquisitionInvariantError(f"Unreconciled rejected 4H groups: {unexplained[:5]}")
    output: dict[str, Any] = {}

    def writer(stage: Path) -> None:
        data = _write_frame(stage, aggregate.bars)
        rejected = _write_frame(stage, aggregate.rejected, "rejected")
        output.update(
            _write_stage_manifest(
                stage,
                {
                    "stage": "completed_4h",
                    "run_id": normalized_manifest["run_id"],
                    "upstream_gate_id": gate_b["gate_id"],
                    "normalized_manifest_id": normalized_manifest["manifest_id"],
                    "data": data,
                    "rejected": rejected,
                    "rejected_all_reconciled": True,
                },
            )
        )

    publish_staged(target, writer)
    return output


def derive_gate_c(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    stage_4h: str | Path,
) -> dict[str, Any]:
    gate_b = _require_current_gate_b(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
    )
    normalized_manifest, hourly = load_stage(normalized_stage, "normalized_1h")
    manifest, bars = load_stage(stage_4h, "completed_4h")
    if (
        manifest["upstream_gate_id"] != gate_b["gate_id"]
        or manifest["normalized_manifest_id"] != normalized_manifest["manifest_id"]
    ):
        raise AcquisitionInvariantError("4H stage upstream binding mismatch")
    aggregate = aggregate_1h_to_4h(hourly)
    if _frame_logical_sha(aggregate.bars) != manifest["data"]["logical_sha256"]:
        raise AcquisitionInvariantError("4H stage does not equal strict re-aggregation of Gate B")
    rejected = _read_frame(Path(stage_4h).resolve(), manifest["rejected"])
    if _frame_logical_sha(rejected) != _frame_logical_sha(aggregate.rejected):
        raise AcquisitionInvariantError("4H rejected-group diagnostics changed")
    cutoff = pd.Timestamp(verified_plan["plan"]["run_identity"]["cutoff_exclusive_utc"])
    unexplained = [
        row
        for row in aggregate.rejected.to_dict("records")
        if not _rejected_is_boundary(
            row["symbol"],
            pd.Timestamp(row["open_time"]),
            normalized_manifest["lifecycle_boundaries"],
            cutoff,
        )
    ]
    if unexplained or not manifest["rejected_all_reconciled"]:
        raise AcquisitionInvariantError("Gate C has unreconciled rejected 4H groups")
    return derived_gate(
        "C",
        {"gate_b_id": gate_b["gate_id"], "completed_4h_manifest_id": manifest["manifest_id"]},
        {
            "strict_reaggregation_equal": True,
            "accepted_groups_complete": bool((bars["source_hour_count"] == 4).all())
            if len(bars)
            else True,
            "rejected_groups_reconciled": True,
        },
        {
            "row_count": len(bars),
            "rejected_group_count": len(rejected),
            "completed_4h_logical_sha256": manifest["data"]["logical_sha256"],
        },
    )


def _eligibility_projection(frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "symbol",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "trade_count",
        "close_time",
        "source_hour_count",
        "contract_age_days",
        "is_eligible",
        "is_eth",
        "is_benchmark",
    ]
    if "selected_lifecycle_episode_id" in frame.columns:
        required.append("selected_lifecycle_episode_id")
    return frame[required].copy()


def _require_current_gate_c(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    gate_c_path: str | Path,
    stage_4h: str | Path,
) -> dict[str, Any]:
    stored = _load_gate(gate_c_path, {"C"})
    expected = derive_gate_c(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
        stage_4h=stage_4h,
    )
    if stored != expected:
        raise AcquisitionInvariantError("Stored Gate C is not the currently derived Gate C")
    return stored


def build_lifecycle_stage(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    gate_c_path: str | Path,
    stage_4h: str | Path,
    target: str | Path,
) -> dict[str, Any]:
    gate_c = _require_current_gate_c(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
    )
    manifest_4h, bars = load_stage(stage_4h, "completed_4h")
    if gate_c["upstream"].get("completed_4h_manifest_id") != manifest_4h["manifest_id"]:
        raise AcquisitionInvariantError("Lifecycle stage received 4H data not authorized by Gate C")
    contracts = verified_plan["verified_bundle"]["catalog"].copy()
    unknown = sorted(set(bars["symbol"]) - set(contracts["symbol"]))
    if unknown:
        raise AcquisitionInvariantError(f"4H stage has unknown lifecycle identities: {unknown}")
    config = load_config(verified_plan["verified_bundle"]["config_path"])
    eligible = apply_point_in_time_eligibility(
        bars,
        contracts,
        minimum_age_days=config["universe"]["minimum_age_calendar_days"],
    )
    projection = _eligibility_projection(eligible)
    if projection.loc[projection["symbol"].eq("BTCUSDT"), "is_eligible"].any():
        raise AcquisitionInvariantError("BTC became tradable in lifecycle stage")
    if "ETHUSDT" in set(projection["symbol"]) and not projection.loc[
        projection["symbol"].eq("ETHUSDT"), "is_eth"
    ].all():
        raise AcquisitionInvariantError("ETH lifecycle tag changed")
    output: dict[str, Any] = {}

    def writer(stage: Path) -> None:
        data = _write_frame(stage, projection)
        output.update(
            _write_stage_manifest(
                stage,
                {
                    "stage": "lifecycle_eligibility",
                    "run_id": manifest_4h["run_id"],
                    "upstream_gate_id": gate_c["gate_id"],
                    "completed_4h_manifest_id": manifest_4h["manifest_id"],
                    "lifecycle_bundle_id": verified_plan["verified_bundle"]["bundle"]["bundle_id"],
                    "data": data,
                },
            )
        )

    publish_staged(target, writer)
    return output


def derive_gate_d(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    gate_c_path: str | Path,
    stage_4h: str | Path,
    lifecycle_stage: str | Path,
) -> dict[str, Any]:
    gate_c = _require_current_gate_c(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
    )
    manifest_4h, bars = load_stage(stage_4h, "completed_4h")
    manifest, stored = load_stage(lifecycle_stage, "lifecycle_eligibility")
    if (
        manifest["upstream_gate_id"] != gate_c["gate_id"]
        or manifest["completed_4h_manifest_id"] != manifest_4h["manifest_id"]
    ):
        raise AcquisitionInvariantError("Lifecycle stage upstream binding mismatch")
    if manifest["lifecycle_bundle_id"] != verified_plan["verified_bundle"]["bundle"]["bundle_id"]:
        raise AcquisitionInvariantError("Lifecycle stage uses wrong frozen lifecycle evidence")
    config = load_config(verified_plan["verified_bundle"]["config_path"])
    replay = _eligibility_projection(
        apply_point_in_time_eligibility(
            bars,
            verified_plan["verified_bundle"]["catalog"].copy(),
            minimum_age_days=config["universe"]["minimum_age_calendar_days"],
        )
    )
    if (
        _frame_logical_sha(replay) != manifest["data"]["logical_sha256"]
        or _frame_logical_sha(stored) != _frame_logical_sha(replay)
    ):
        raise AcquisitionInvariantError("Lifecycle eligibility stage does not replay frozen evidence")
    btc_ok = not replay.loc[replay["symbol"].eq("BTCUSDT"), "is_eligible"].any()
    eth_ok = "ETHUSDT" not in set(replay["symbol"]) or replay.loc[
        replay["symbol"].eq("ETHUSDT"), "is_eth"
    ].all()
    return derived_gate(
        "D",
        {"gate_c_id": gate_c["gate_id"], "lifecycle_manifest_id": manifest["manifest_id"]},
        {
            "frozen_lifecycle_replayed": True,
            "unknown_identities_zero": True,
            "btc_benchmark_not_tradable": bool(btc_ok),
            "eth_tag_reproduced": bool(eth_ok),
        },
        {
            "row_count": len(replay),
            "lifecycle_logical_sha256": manifest["data"]["logical_sha256"],
            "lifecycle_bundle_id": manifest["lifecycle_bundle_id"],
        },
    )


def _scanner_engineering_frame(eligible: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    features = add_time_series_features(
        eligible,
        atr_period=config["features"]["atr_period"],
        liquidity_observations=config["features"]["liquidity_30d_observations_4h"],
    )
    scored = add_cross_sectional_scanner(features)
    episodes = add_hot_episodes(scored)
    split = classify_split(episodes["open_time"], config["splits"])
    registered = episodes.loc[split.notna()].copy()
    allowed = ["development", "validation", "final_holdout"]
    return add_outcome_labels(registered, config["splits"], allowed_splits=allowed)


def _require_current_gate_d(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    gate_c_path: str | Path,
    stage_4h: str | Path,
    gate_d_path: str | Path,
    lifecycle_stage: str | Path,
) -> dict[str, Any]:
    stored = _load_gate(gate_d_path, {"D"})
    expected = derive_gate_d(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
        lifecycle_stage=lifecycle_stage,
    )
    if stored != expected:
        raise AcquisitionInvariantError("Stored Gate D is not the currently derived Gate D")
    return stored


def build_scanner_engineering_stage(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    gate_c_path: str | Path,
    stage_4h: str | Path,
    gate_d_path: str | Path,
    lifecycle_stage: str | Path,
    target: str | Path,
) -> dict[str, Any]:
    gate_d = _require_current_gate_d(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
        gate_d_path=gate_d_path,
        lifecycle_stage=lifecycle_stage,
    )
    lifecycle_manifest, eligible = load_stage(lifecycle_stage, "lifecycle_eligibility")
    if gate_d["upstream"].get("lifecycle_manifest_id") != lifecycle_manifest["manifest_id"]:
        raise AcquisitionInvariantError("Scanner stage received lifecycle data not authorized by Gate D")
    config = load_config(verified_plan["verified_bundle"]["config_path"])
    observations = _scanner_engineering_frame(eligible, config)
    split_labels = classify_split(observations["open_time"], config["splits"])
    if split_labels.isna().any():
        raise AcquisitionInvariantError("Scanner engineering output escaped frozen split coverage")
    output: dict[str, Any] = {}

    def writer(stage: Path) -> None:
        split_descriptors: dict[str, Any] = {}
        for split_name in ("development", "validation", "final_holdout"):
            shard = observations.loc[split_labels.eq(split_name)].copy()
            split_dir = stage / split_name
            split_dir.mkdir(parents=True, exist_ok=True)
            descriptor = _write_frame(split_dir, shard)
            split_descriptors[split_name] = {
                **descriptor,
                "relative_dir": split_name,
                "access_status": "sealed_until_explicit_authorization",
            }
        output.update(
            _write_stage_manifest(
                stage,
                {
                    "stage": "scanner_engineering_sealed",
                    "run_id": lifecycle_manifest["run_id"],
                    "upstream_gate_id": gate_d["gate_id"],
                    "lifecycle_manifest_id": lifecycle_manifest["manifest_id"],
                    "data": {
                        "file": "split_partitions",
                        "storage_format": "sealed_split_set",
                        "file_sha256": digest_json(split_descriptors),
                        "logical_sha256": _frame_logical_sha(observations),
                        "row_count": len(observations),
                        "columns": list(observations.columns),
                    },
                    "sealed_splits": split_descriptors,
                    "gate_f_created": False,
                },
            )
        )

    publish_staged(target, writer)
    return output


def _load_sealed(stage_path: str | Path, manifest: dict[str, Any]) -> pd.DataFrame:
    root = Path(stage_path).resolve()
    frames: list[pd.DataFrame] = []
    for split_name in ("development", "validation", "final_holdout"):
        descriptor = manifest["sealed_splits"][split_name]
        if descriptor.get("access_status") != "sealed_until_explicit_authorization":
            raise AcquisitionInvariantError("Sealed split access status changed")
        frames.append(_read_frame(root / descriptor["relative_dir"], descriptor))
    combined = pd.concat(frames, ignore_index=True)
    if _frame_logical_sha(combined) != manifest["data"]["logical_sha256"]:
        raise AcquisitionInvariantError("Sealed split set does not reproduce scanner logical content")
    return combined


def derive_gate_e(
    *,
    verified_plan: dict[str, Any],
    completion_path: str | Path,
    raw_root: str | Path,
    gate_a_path: str | Path,
    gate_b_path: str | Path,
    normalized_stage: str | Path,
    gate_c_path: str | Path,
    stage_4h: str | Path,
    gate_d_path: str | Path,
    lifecycle_stage: str | Path,
    scanner_stage: str | Path,
) -> dict[str, Any]:
    gate_d = _require_current_gate_d(
        verified_plan=verified_plan,
        completion_path=completion_path,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized_stage,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
        gate_d_path=gate_d_path,
        lifecycle_stage=lifecycle_stage,
    )
    lifecycle_manifest, eligible = load_stage(lifecycle_stage, "lifecycle_eligibility")
    stage = Path(scanner_stage).resolve(strict=True)
    if not (stage / "_SUCCESS").is_file():
        raise AcquisitionInvariantError("Scanner engineering stage lacks success marker")
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    supplied_id = manifest.get("manifest_id")
    core = {key: value for key, value in manifest.items() if key != "manifest_id"}
    if manifest.get("schema_version") != STAGE_MANIFEST_SCHEMA or supplied_id != digest_json(core):
        raise AcquisitionInvariantError("Scanner engineering stage manifest identity is invalid")
    if (
        manifest.get("stage") != "scanner_engineering_sealed"
        or manifest["upstream_gate_id"] != gate_d["gate_id"]
    ):
        raise AcquisitionInvariantError("Scanner engineering stage upstream binding mismatch")
    if (
        manifest["lifecycle_manifest_id"] != lifecycle_manifest["manifest_id"]
        or manifest.get("gate_f_created") is not False
    ):
        raise AcquisitionInvariantError("Scanner engineering stage sealing contract changed")
    stored = _load_sealed(stage, manifest)
    config = load_config(verified_plan["verified_bundle"]["config_path"])
    replay = _scanner_engineering_frame(eligible, config)
    if (
        _frame_logical_sha(replay) != manifest["data"]["logical_sha256"]
        or _frame_logical_sha(stored) != _frame_logical_sha(replay)
    ):
        raise AcquisitionInvariantError("Scanner engineering sealed data does not replay frozen Scanner")
    split_labels = classify_split(replay["open_time"], config["splits"])
    if split_labels.isna().any():
        raise AcquisitionInvariantError("Scanner engineering rows escaped frozen splits")
    # Deliberately no grouping by HotScore/HOT/outcome, no means/probabilities/correlations.
    split_counts = {
        name: int(split_labels.eq(name).sum())
        for name in ("development", "validation", "final_holdout")
    }
    return derived_gate(
        "E",
        {"gate_d_id": gate_d["gate_id"], "scanner_manifest_id": manifest["manifest_id"]},
        {
            "scanner_replay_equal": True,
            "split_coverage_exact": True,
            "validation_sealed": manifest["sealed_splits"]["validation"]["access_status"]
            == "sealed_until_explicit_authorization",
            "holdout_sealed": manifest["sealed_splits"]["final_holdout"]["access_status"]
            == "sealed_until_explicit_authorization",
            "gate_f_absent": manifest.get("gate_f_created") is False,
            "performance_aggregate_absent": True,
        },
        {
            "row_count": len(replay),
            "split_row_counts": split_counts,
            "scanner_logical_sha256": manifest["data"]["logical_sha256"],
            "sealed_split_manifest_sha256": manifest["data"]["file_sha256"],
        },
    )
