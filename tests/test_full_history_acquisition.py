from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from itertools import pairwise
from pathlib import Path
from threading import Event

import pandas as pd
import pytest

from alt_hot_scanner.data.acquisition import (
    ACQUISITION_AUTH_SCHEMA_VERSION,
    ACQUISITION_EXECUTABLE_PATHS,
    ATTEMPT_SCHEMA_VERSION,
    LIFECYCLE_CRITICAL_PATHS,
    AcquisitionInvariantError,
    SourceEntry,
    acquire_api_source,
    acquire_archive_source,
    acquisition_executable_tree_sha256,
    api_tail_requests,
    build_mixed_source_inventory,
    build_raw_completion_manifest,
    classify_gaps,
    classify_raw_conflict,
    derived_gate,
    digest_json,
    discover_daily_1h_objects,
    gate_artifact,
    publish_staged,
    replay_daily_discovery_pages,
    require_complete_attempt_manifest,
    require_gate_pass,
    validate_acquisition_archive_key,
    validate_api_tail,
    validate_daily_kline_object_key,
    verify_acquisition_authorization,
    verify_lifecycle_runtime_boundary,
)
from alt_hot_scanner.data.binance_public import validate_archive_object_key
from alt_hot_scanner.data.full_history import (
    build_4h_stage,
    build_lifecycle_stage,
    build_normalized_1h_stage,
    build_scanner_engineering_stage,
    derive_gate_a,
    derive_gate_b,
    derive_gate_c,
    derive_gate_d,
    derive_gate_e,
    write_gate,
)

MONTH = "data/futures/um/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2024-01.zip"
DAILY = "data/futures/um/daily/klines/ETHUSDT/1h/ETHUSDT-1h-2024-02-03.zip"


def _xml(*, prefix: str, prefixes: list[str] | None = None, keys: list[str] | None = None,
         truncated: bool = False, token: str | None = None) -> bytes:
    prefixes = prefixes or []
    keys = keys or []
    common = "".join(f"<CommonPrefixes><Prefix>{value}</Prefix></CommonPrefixes>" for value in prefixes)
    contents = "".join(f"<Contents><Key>{value}</Key></Contents>" for value in keys)
    next_token = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Prefix>{prefix}</Prefix><KeyCount>{len(prefixes) + len(keys)}</KeyCount>"
        f"<IsTruncated>{str(truncated).lower()}</IsTruncated>{common}{contents}{next_token}"
        "</ListBucketResult>"
    ).encode()


def test_lifecycle_parser_stays_monthly_only_and_acquisition_parser_owns_daily() -> None:
    assert validate_archive_object_key(MONTH).period == "2024-01"
    with pytest.raises(ValueError, match="monthly"):
        validate_archive_object_key(DAILY)
    identity = validate_daily_kline_object_key(DAILY)
    assert identity.source_kind == "daily" and identity.period == "2024-02-03"
    assert validate_acquisition_archive_key(DAILY) == identity


def test_daily_pagination_reaches_late_page_and_frozen_pages_replay(tmp_path: Path) -> None:
    root = "data/futures/um/daily/klines/"
    eth_prefix = f"{root}ETHUSDT/1h/"
    late_prefix = f"{root}LATEUSDT/1h/"
    calls: list[str] = []
    frozen_pages: list[dict[str, object]] = []

    def reader(url: str) -> bytes:
        calls.append(url)
        if "prefix=data%2Ffutures%2Fum%2Fdaily%2Fklines%2F&delimiter=%2F" in url:
            if "continuation-token" not in url:
                return _xml(
                    prefix=root,
                    prefixes=[f"{root}ETHUSDT/"],
                    truncated=True,
                    token="page 2",
                )
            return _xml(prefix=root, prefixes=[f"{root}LATEUSDT/"], truncated=False)
        if "ETHUSDT%2F1h%2F" in url:
            key = f"{eth_prefix}ETHUSDT-1h-2024-02-03.zip"
            return _xml(prefix=eth_prefix, keys=[key, f"{key}.CHECKSUM"])
        if "LATEUSDT%2F1h%2F" in url:
            key = f"{late_prefix}LATEUSDT-1h-2024-02-03.zip"
            return _xml(prefix=late_prefix, keys=[key, f"{key}.CHECKSUM"])
        raise AssertionError(url)

    def observer(page: int, url: str, payload: bytes) -> None:
        sequence = len(frozen_pages) + 1
        path = tmp_path / f"page-{sequence}.xml"
        path.write_bytes(payload)
        frozen_pages.append(
            {
                "sequence": sequence,
                "page": page,
                "url": url,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "path": str(path.resolve()),
            }
        )

    identities, evidence = discover_daily_1h_objects(reader=reader, page_observer=observer)
    replayed, replay_evidence = replay_daily_discovery_pages(frozen_pages)
    assert [item.symbol for item in identities] == ["ETHUSDT", "LATEUSDT"]
    assert [asdict(item) for item in replayed] == [asdict(item) for item in identities]
    assert len(evidence) == len(replay_evidence) == 4
    assert any("continuation-token=page+2" in url for url in calls)


def test_daily_discovery_rejects_zip_without_observed_checksum_sidecar() -> None:
    root = "data/futures/um/daily/klines/"
    eth_prefix = f"{root}ETHUSDT/1h/"

    def reader(url: str) -> bytes:
        if "delimiter=%2F" in url:
            return _xml(prefix=root, prefixes=[f"{root}ETHUSDT/"])
        key = f"{eth_prefix}ETHUSDT-1h-2024-02-03.zip"
        return _xml(prefix=eth_prefix, keys=[key])

    with pytest.raises(AcquisitionInvariantError, match="checksum sidecar"):
        discover_daily_1h_objects(reader=reader)


def test_mixed_source_policy_selects_monthly_then_daily_then_latest_api_suffix() -> None:
    d1 = validate_daily_kline_object_key(DAILY.replace("2024-02-03", "2024-02-01"))
    d2 = validate_daily_kline_object_key(DAILY.replace("2024-02-03", "2024-02-02"))
    inventory, boundary = build_mixed_source_inventory(
        monthly_keys=[MONTH],
        daily_identities=[d1, d2],
        approved_symbols={"ETHUSDT"},
        frozen_candidate_universe={"ETHUSDT"},
        warmup_start_month="2024-01",
        cutoff_exclusive_utc="2024-02-03T12:00:00Z",
        daily_discovery_evidence=[{"sha256": "c" * 64}],
        lifecycle_catalog=_active_catalog(),
    )
    entries = [SourceEntry(**row) for row in inventory["entries"]]
    assert boundary == "2024-02-01T00:00:00+00:00"
    assert [entry.source_kind for entry in entries] == ["monthly", "daily", "daily", "api"]
    ranges = [tuple(pd.Timestamp(value) for value in entry.selected_canonical_range) for entry in entries]
    assert all(left[1] <= right[0] for left, right in pairwise(ranges))
    assert ranges[-1] == (
        pd.Timestamp("2024-02-03T00:00:00Z"),
        pd.Timestamp("2024-02-03T12:00:00Z"),
    )


def _active_catalog(symbol: str = "ETHUSDT") -> pd.DataFrame:
    return pd.DataFrame([{
        "symbol": symbol,
        "eligibility_age_anchor_at": "2020-01-01T00:00:00Z",
        "first_observed_trade_at": "2020-01-01T00:00:00Z",
        "last_trading_at": None,
        "lifecycle_intervals": "[]",
    }])


def test_missing_whole_leading_active_month_is_fatal_before_api_planning() -> None:
    feb = MONTH.replace("2024-01", "2024-02")
    mar = MONTH.replace("2024-01", "2024-03")
    with pytest.raises(
        AcquisitionInvariantError,
        match=r"Missing lifecycle-active monthly archive for ETHUSDT at 2024-01; API repair prohibited",
    ):
        build_mixed_source_inventory(
            monthly_keys=[feb, mar],
            daily_identities=[],
            approved_symbols={"ETHUSDT"},
            frozen_candidate_universe={"ETHUSDT"},
            warmup_start_month="2024-01",
            cutoff_exclusive_utc="2024-04-01T00:00:00Z",
            daily_discovery_evidence=[],
            lifecycle_catalog=_active_catalog(),
        )


def test_missing_whole_trailing_active_month_is_fatal_before_api_planning() -> None:
    feb = MONTH.replace("2024-01", "2024-02")
    with pytest.raises(
        AcquisitionInvariantError,
        match=r"Missing lifecycle-active monthly archive for ETHUSDT at 2024-03; API repair prohibited",
    ):
        build_mixed_source_inventory(
            monthly_keys=[MONTH, feb],
            daily_identities=[],
            approved_symbols={"ETHUSDT"},
            frozen_candidate_universe={"ETHUSDT"},
            warmup_start_month="2024-01",
            cutoff_exclusive_utc="2024-04-01T00:00:00Z",
            daily_discovery_evidence=[],
            lifecycle_catalog=_active_catalog(),
        )


def test_daily_unknown_lifecycle_identity_halts_and_interior_gap_cannot_be_api_repaired() -> None:
    daily = [validate_daily_kline_object_key(DAILY)]
    with pytest.raises(AcquisitionInvariantError, match="absent frozen lifecycle"):
        build_mixed_source_inventory(
            monthly_keys=[MONTH],
            daily_identities=daily,
            approved_symbols={"ETHUSDT"},
            frozen_candidate_universe={"BTCUSDT"},
            warmup_start_month="2023-12",
            cutoff_exclusive_utc="2024-02-05T12:00:00Z",
            daily_discovery_evidence=[{"sha256": "a" * 64}],
            lifecycle_catalog=_active_catalog(),
        )

    d1 = validate_daily_kline_object_key(DAILY.replace("2024-02-03", "2024-02-01"))
    d3 = validate_daily_kline_object_key(DAILY)
    with pytest.raises(AcquisitionInvariantError, match="Interior daily archive gap"):
        build_mixed_source_inventory(
            monthly_keys=[MONTH],
            daily_identities=[d1, d3],
            approved_symbols={"ETHUSDT"},
            frozen_candidate_universe={"ETHUSDT"},
            warmup_start_month="2024-01",
            cutoff_exclusive_utc="2024-02-05T12:00:00Z",
            daily_discovery_evidence=[{"sha256": "b" * 64}],
            lifecycle_catalog=_active_catalog(),
        )


def test_api_is_exact_latest_suffix_and_short_or_current_bar_is_rejected() -> None:
    requests = api_tail_requests(
        "ETHUSDT", pd.Timestamp("2024-02-01T01:00:00Z"), "2024-02-01T04:00:00Z"
    )
    assert len(requests) == 1
    request = requests[0]
    rows = []
    for hour in (1, 2, 3):
        opened = pd.Timestamp(f"2024-02-01T{hour:02d}:00:00Z")
        rows.append([int(opened.timestamp() * 1000), 1, 2, 0.5, 1.5, 10, int((opened + pd.Timedelta(hours=1) - pd.Timedelta(milliseconds=1)).timestamp()*1000), 15, 1, 5, 7, 0])
    validate_api_tail(rows, request, "2024-02-01T04:00:00Z")
    with pytest.raises(AcquisitionInvariantError, match="short/empty"):
        validate_api_tail(rows[:-1], request, "2024-02-01T04:00:00Z")
    bad = [*rows, [int(pd.Timestamp("2024-02-01T04:00:00Z").timestamp()*1000), 1, 2, .5, 1.5, 1, 0, 1, 1, 1, 1, 0]]
    with pytest.raises(AcquisitionInvariantError, match="contiguous suffix"):
        validate_api_tail(bad, request, "2024-02-01T04:00:00Z")


def test_exact_api_response_bytes_are_persisted_and_rehashable(tmp_path: Path) -> None:
    request = api_tail_requests("ETHUSDT", pd.Timestamp("2024-02-01T01:00Z"), "2024-02-01T02:00Z")[0]
    entry = SourceEntry(
        "api", "ETHUSDT",
        "https://fapi.binance.com/fapi/v1/klines?" + __import__("urllib.parse").parse.urlencode({k: v for k, v in request.items() if k != "endpoint"}),
        f"{request['startTime']}-{request['endTime']}",
        ("test",),
        ("2024-02-01T01:00:00+00:00", "2024-02-01T02:00:00+00:00"),
    )
    opened = pd.Timestamp("2024-02-01T01:00Z")
    raw = json.dumps([[int(opened.timestamp()*1000),1,2,.5,1.5,10,int((opened+pd.Timedelta(hours=1)-pd.Timedelta(milliseconds=1)).timestamp()*1000),15,1,5,7,0]], separators=(",", ":")).encode()
    record = acquire_api_source(
        entry,
        tmp_path,
        "2024-02-01T02:00Z",
        reader=lambda _: raw,
        retrieved_at="2024-02-01T02:01:00Z",
        run_id="frozen-run",
    )
    assert Path(record.raw_path).read_bytes() == raw
    assert hashlib.sha256(Path(record.raw_path).read_bytes()).hexdigest() == record.raw_sha256

    changed = raw.replace(b'"10"', b'"11"') if b'"10"' in raw else raw.replace(b',10,', b',11,')
    assert changed != raw
    with pytest.raises(AcquisitionInvariantError, match="API response changed"):
        acquire_api_source(
            entry,
            tmp_path,
            "2024-02-01T02:00Z",
            reader=lambda _: changed,
            run_id="frozen-run",
        )


def test_leading_interior_and_trailing_gaps_are_detected() -> None:
    gaps = classify_gaps(
        "ETHUSDT",
        pd.Series(pd.to_datetime(["2024-01-01T01:00Z", "2024-01-01T03:00Z"])),
        "2024-01-01T00:00Z",
        "2024-01-01T05:00Z",
    )
    assert {gap.kind for gap in gaps} == {"leading_source_gap", "interior_source_gap", "trailing_source_gap"}


def test_archive_revision_and_local_corruption_are_distinct_and_versions_immutable(tmp_path: Path) -> None:
    entry = SourceEntry("monthly", "ETHUSDT", MONTH, "2024-01", ("test",), ("2024-01-01T00:00:00+00:00", "2024-02-01T00:00:00+00:00"))
    first = b"first zip bytes"
    second = b"second zip bytes"
    first_hash = hashlib.sha256(first).hexdigest()
    second_hash = hashlib.sha256(second).hexdigest()
    state = {"payload": first, "hash": first_hash}

    def reader(url: str) -> bytes:
        if url.endswith(".CHECKSUM"):
            return f"{state['hash']}  ETHUSDT-1h-2024-01.zip\n".encode()
        return state["payload"]

    one = acquire_archive_source(entry, tmp_path, reader=reader, run_id="run-one")
    assert one.upstream_revision_detected is False
    state.update(payload=second, hash=second_hash)
    with pytest.raises(AcquisitionInvariantError, match="upstream archive revision"):
        acquire_archive_source(entry, tmp_path, reader=reader, run_id="run-one")
    two = acquire_archive_source(entry, tmp_path, reader=reader, run_id="run-two")
    assert two.upstream_revision_detected is True
    assert Path(one.raw_path).read_bytes() == first and Path(two.raw_path).read_bytes() == second
    assert classify_raw_conflict(first_hash, second_hash, first_hash) == "upstream_source_revision"
    assert classify_raw_conflict(first_hash, first_hash, second_hash) == "local_corruption"


def test_caller_cannot_construct_gate_and_fabricated_pass_is_rejected() -> None:
    with pytest.raises(AcquisitionInvariantError, match="Caller-supplied"):
        gate_artifact("A", {"anything": True})
    fake = {
        "schema_version": "derived-performance-blind-gate-v2",
        "gate": "A",
        "status": "passed",
        "upstream": {},
        "checks": {"fake": True},
        "details": {},
        "performance_aggregate_emitted": False,
        "gate_id": "0" * 64,
    }
    with pytest.raises(AcquisitionInvariantError, match="identity"):
        require_gate_pass(fake, {"A"})


def test_success_marker_is_fsynced_through_writable_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_fsync = os.fsync

    def require_writable_descriptor(fd: int) -> None:
        # Windows rejects fsync on the read-only descriptor used by the previous implementation.
        # A zero-byte write makes that portability contract executable on POSIX as well.
        os.write(fd, b"")
        original_fsync(fd)

    monkeypatch.setattr(os, "fsync", require_writable_descriptor)
    target = tmp_path / "stage"
    publish_staged(target, lambda stage: (stage / "data").write_text("ok", encoding="utf-8"))
    assert (target / "_SUCCESS").read_text(encoding="utf-8") == "complete\n"


def test_atomic_publication_rejects_concurrent_writer(tmp_path: Path) -> None:
    target = tmp_path / "stage"

    def writer(stage: Path) -> None:
        (stage / "data").write_text("ok", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(publish_staged, target, writer) for _ in range(2)]
    successes = 0
    failures = 0
    for future in futures:
        try:
            future.result()
            successes += 1
        except (AcquisitionInvariantError, FileExistsError):
            failures += 1
    assert (successes, failures) == (1, 1)
    assert (target / "_SUCCESS").is_file()


def test_failed_concurrent_contender_does_not_remove_owned_lock(tmp_path: Path) -> None:
    target = tmp_path / "stage"
    lock = tmp_path / ".stage.lock"
    entered = Event()
    release = Event()

    def slow_writer(stage: Path) -> None:
        entered.set()
        assert release.wait(timeout=5)
        (stage / "data").write_text("ok", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=1) as pool:
        owner = pool.submit(publish_staged, target, slow_writer)
        assert entered.wait(timeout=5)
        try:
            assert lock.is_file()
            with pytest.raises(AcquisitionInvariantError, match="Concurrent publication lock exists"):
                publish_staged(target, lambda stage: None)
            assert lock.is_file()
        finally:
            release.set()
        assert owner.result() == str(target.resolve())

    assert not lock.exists()
    assert (target / "_SUCCESS").is_file()


def test_authoritative_plan_not_copied_attempt_objects_controls_completion(tmp_path: Path) -> None:
    planned = SourceEntry("api", "ETHUSDT", "planned", "x", ("e",), ("2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"))
    verified = {"plan": {"run_identity": {"run_id": "r"}, "plan_integrity": "p"}, "plan_path": tmp_path / "plan.json", "source_entries": [planned]}
    forged = {
        "schema_version": ATTEMPT_SCHEMA_VERSION,
        "plan_path": str(verified["plan_path"]),
        "plan_integrity": "p",
        "run_id": "r",
        "canonical": True,
        "attempts": [{"object_key_or_request": "forged", "status": "verified"}],
    }
    with pytest.raises(AcquisitionInvariantError, match="authoritative source plan"):
        require_complete_attempt_manifest(verified, forged)


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def test_lifecycle_critical_runtime_mutation_is_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "lifecycle-repo"
    repo.mkdir()
    _git("init", cwd=repo)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    for relative in LIFECYCLE_CRITICAL_PATHS:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"frozen {relative}\n", encoding="utf-8")
    _git("add", ".", cwd=repo)
    _git("commit", "-m", "frozen lifecycle boundary", cwd=repo)
    commit = _git("rev-parse", "HEAD", cwd=repo)
    verify_lifecycle_runtime_boundary(repo, commit)
    mutated = repo / LIFECYCLE_CRITICAL_PATHS[0]
    mutated.write_text(mutated.read_text(encoding="utf-8") + "mutation\n", encoding="utf-8")
    with pytest.raises(AcquisitionInvariantError, match="Lifecycle-critical runtime differs"):
        verify_lifecycle_runtime_boundary(repo, commit)


def test_lifecycle_boundary_and_separate_acquisition_authorization(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[1]
    # The remediation checkout may change acquisition files, but frozen lifecycle-critical files must still match 47b5.
    verify_lifecycle_runtime_boundary(source_root, "47b5feb45aec82f967ff5679fb8166f89039356a")

    repo = tmp_path / "repo"
    repo.mkdir()
    _git("init", cwd=repo)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    for relative in ACQUISITION_EXECUTABLE_PATHS:
        destination = repo / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((source_root / relative).read_bytes())
    _git("add", ".", cwd=repo)
    _git("commit", "-m", "reviewed acquisition executable", cwd=repo)
    commit = _git("rev-parse", "HEAD", cwd=repo)
    tree = acquisition_executable_tree_sha256(repo)
    review = tmp_path / "review.txt"
    review.write_text("PASS\n", encoding="utf-8")
    core = {
        "schema_version": ACQUISITION_AUTH_SCHEMA_VERSION,
        "purpose": "full_history_acquisition_and_performance_blind_build",
        "acquisition_executable_commit": commit,
        "acquisition_paths": list(ACQUISITION_EXECUTABLE_PATHS),
        "acquisition_tree_sha256": tree,
        "review_artifact": {"path": str(review.resolve()), "sha256": hashlib.sha256(review.read_bytes()).hexdigest(), "verdict": "PASS"},
        "status": "active",
        "authorized_at": "2026-08-25T00:00:00Z",
    }
    auth = {**core, "authorization_id": digest_json(core)}
    auth_path = tmp_path / "auth.json"
    auth_path.write_text(json.dumps(auth), encoding="utf-8")
    assert verify_acquisition_authorization(auth_path, repo)["acquisition_executable_commit"] == commit
    first = repo / ACQUISITION_EXECUTABLE_PATHS[0]
    first.write_bytes(first.read_bytes() + b"\n# mutation\n")
    with pytest.raises(AcquisitionInvariantError, match="differs from reviewed"):
        verify_acquisition_authorization(auth_path, repo)


def _monthly_zip_bytes(symbol: str, period: str) -> bytes:
    month = pd.Period(period, freq="M")
    start = month.start_time.tz_localize("UTC")
    end = (month + 1).start_time.tz_localize("UTC")
    rows: list[str] = []
    for opened in pd.date_range(start, end - pd.Timedelta(hours=1), freq="h"):
        opened_ms = int(opened.timestamp() * 1000)
        close_ms = int(
            (opened + pd.Timedelta(hours=1) - pd.Timedelta(milliseconds=1)).timestamp() * 1000
        )
        rows.append(
            ",".join(
                str(value)
                for value in (
                    opened_ms,
                    100.0,
                    101.0,
                    99.0,
                    100.5,
                    10.0,
                    close_ms,
                    1005.0,
                    10,
                    5.0,
                    502.5,
                    0,
                )
            )
        )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{symbol}-1h-{period}.csv", "\n".join(rows).encode("utf-8"))
    return buffer.getvalue()


def _monthly_gap_verified_plan(
    tmp_path: Path, *, observed_period: str, cutoff: str
) -> tuple[dict, Path, Path]:
    symbol = "ETHUSDT"
    month = pd.Period(observed_period, freq="M")
    source_start = month.start_time.tz_localize("UTC")
    source_end = (month + 1).start_time.tz_localize("UTC")
    object_key = (
        f"data/futures/um/monthly/klines/{symbol}/1h/"
        f"{symbol}-1h-{observed_period}.zip"
    )
    entry = SourceEntry(
        "monthly",
        symbol,
        object_key,
        observed_period,
        ("synthetic observed monthly object",),
        (source_start.isoformat(), source_end.isoformat()),
    )
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    raw = _monthly_zip_bytes(symbol, observed_period)
    raw_path = raw_root / f"{symbol}-1h-{observed_period}.zip"
    raw_path.write_bytes(raw)
    raw_sha = hashlib.sha256(raw).hexdigest()
    sidecar = raw_root / f"{raw_path.name}.CHECKSUM"
    sidecar_bytes = f"{raw_sha}  {raw_path.name}\n".encode()
    sidecar.write_bytes(sidecar_bytes)

    plan_path = tmp_path / "plan.json"
    plan_path.write_text("{}", encoding="utf-8")
    plan = {
        "run_identity": {"run_id": "monthly-gap-run", "cutoff_exclusive_utc": cutoff},
        "plan_integrity": "monthly-gap-plan",
    }
    source_root = Path(__file__).resolve().parents[1]
    catalog = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "eligibility_age_anchor_at": "2019-12-01T00:00:00Z",
                "first_observed_trade_at": "2019-12-01T00:00:00Z",
                "last_trading_at": None,
                "lifecycle_intervals": "[]",
            }
        ]
    )
    verified = {
        "plan": plan,
        "plan_path": plan_path.resolve(),
        "source_entries": [entry],
        "verified_bundle": {
            "catalog": catalog,
            "config_path": (source_root / "config/research_v0_1.yaml").resolve(),
        },
    }
    attempt = {
        "source_kind": "monthly",
        "symbol": symbol,
        "object_key_or_request": object_key,
        "status": "verified",
        "raw_path": str(raw_path.resolve()),
        "raw_sha256": raw_sha,
        "byte_count": len(raw),
        "retrieved_at": "2026-08-25T00:00:00Z",
        "published_sha256": raw_sha,
        "checksum_sidecar_path": str(sidecar.resolve()),
        "checksum_sidecar_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
        "payload_source": "synthetic_checksum_verified_archive",
        "upstream_revision_detected": False,
        "error": None,
    }
    attempt_core = {
        "schema_version": ATTEMPT_SCHEMA_VERSION,
        "plan_path": str(plan_path.resolve()),
        "plan_integrity": "monthly-gap-plan",
        "run_id": "monthly-gap-run",
        "canonical": True,
        "attempts": [attempt],
    }
    attempt_payload = {**attempt_core, "attempt_id": digest_json(attempt_core)}
    attempt_path = raw_root / "attempt.json"
    attempt_path.write_text(json.dumps(attempt_payload), encoding="utf-8")
    completion = build_raw_completion_manifest(verified, attempt_path, raw_root)
    completion_path = raw_root / "completion.json"
    completion_path.write_text(json.dumps(completion), encoding="utf-8")
    return verified, raw_root, completion_path


def test_gate_b_rejects_whole_leading_lifecycle_active_month(tmp_path: Path) -> None:
    verified, raw_root, completion = _monthly_gap_verified_plan(
        tmp_path, observed_period="2020-01", cutoff="2020-02-01T00:00:00Z"
    )
    gate_a_path = tmp_path / "A.json"
    write_gate(gate_a_path, derive_gate_a(verified, completion, raw_root))
    with pytest.raises(AcquisitionInvariantError, match="leading_active_gap"):
        build_normalized_1h_stage(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            target=tmp_path / "normalized",
        )


def test_gate_b_rejects_whole_trailing_lifecycle_active_month(tmp_path: Path) -> None:
    verified, raw_root, completion = _monthly_gap_verified_plan(
        tmp_path, observed_period="2019-12", cutoff="2020-02-01T00:00:00Z"
    )
    gate_a_path = tmp_path / "A.json"
    write_gate(gate_a_path, derive_gate_a(verified, completion, raw_root))
    with pytest.raises(AcquisitionInvariantError, match="trailing_active_gap"):
        build_normalized_1h_stage(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            target=tmp_path / "normalized",
        )


def _api_rows(start: pd.Timestamp, hours: int, drift: float) -> bytes:
    rows = []
    for index in range(hours):
        opened = start + pd.Timedelta(hours=index)
        price = 100 + drift * index
        rows.append([
            int(opened.timestamp()*1000), price, price+1, price-1, price+0.2, 10+index,
            int((opened+pd.Timedelta(hours=1)-pd.Timedelta(milliseconds=1)).timestamp()*1000),
            (10+index)*(price+0.2), 10, 5, 5*(price+0.2), 0,
        ])
    return json.dumps(rows, separators=(",", ":")).encode()


def _synthetic_verified_plan(tmp_path: Path) -> tuple[dict, Path, Path]:
    source_root = Path(__file__).resolve().parents[1]
    start = pd.Timestamp("2019-12-01T00:00:00Z")
    hours = 1000
    cutoff = start + pd.Timedelta(hours=hours)
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    entries = []
    attempts = []
    for symbol, drift in (("BTCUSDT", .01), ("ETHUSDT", .03), ("ALTUSDT", .06)):
        request = api_tail_requests(symbol, start, cutoff.isoformat())[0]
        url = "https://fapi.binance.com/fapi/v1/klines?" + __import__("urllib.parse").parse.urlencode({k:v for k,v in request.items() if k != "endpoint"})
        entry = SourceEntry("api", symbol, url, f"{request['startTime']}-{request['endTime']}", ("synthetic",), (start.isoformat(), cutoff.isoformat()))
        entries.append(entry)
        raw = _api_rows(start, hours, drift)
        path = raw_root / f"{symbol}.response"
        path.write_bytes(raw)
        attempts.append({
            "source_kind": "api", "symbol": symbol, "object_key_or_request": url, "status": "verified",
            "raw_path": str(path.resolve()), "raw_sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw),
            "retrieved_at": "2026-08-25T00:00:00Z", "published_sha256": None,
            "checksum_sidecar_path": None, "checksum_sidecar_sha256": None,
            "payload_source": "exact_raw_api_response_bytes", "upstream_revision_detected": False, "error": None,
        })
    plan_path = tmp_path / "plan.json"
    plan = {"run_identity": {"run_id": "synthetic-run", "cutoff_exclusive_utc": cutoff.isoformat()}, "plan_integrity": "synthetic-plan"}
    plan_path.write_text("{}", encoding="utf-8")
    config_path = source_root / "config/research_v0_1.yaml"
    catalog = pd.DataFrame([
        {
            "symbol": symbol,
            "delisting_announcement_published_at": None,
            "scope_classification_status": "complete",
            "scope_disposition": "benchmark_only" if symbol == "BTCUSDT" else "in_scope_crypto_perpetual",
            "eligibility_age_anchor_at": "2019-01-01T00:00:00Z",
            "eligibility_age_anchor_basis": "first_observed_binance_futures_trade",
            "first_observed_trade_at": "2019-01-01T00:00:00Z",
            "first_observed_trade_evidence_status": "checksum_verified_official_binance_futures_trade",
            "last_trading_at": None,
            "lifecycle_intervals": "[]",
        }
        for symbol in ("BTCUSDT", "ETHUSDT", "ALTUSDT")
    ])
    verified = {
        "plan": plan,
        "plan_path": plan_path.resolve(),
        "source_entries": entries,
        "verified_bundle": {"catalog": catalog, "config_path": config_path.resolve(), "bundle": {"bundle_id": "lifecycle-bundle"}},
    }
    attempt_core = {
        "schema_version": ATTEMPT_SCHEMA_VERSION,
        "plan_path": str(plan_path.resolve()),
        "plan_integrity": "synthetic-plan",
        "run_id": "synthetic-run",
        "canonical": True,
        "attempts": attempts,
    }
    attempt = {**attempt_core, "attempt_id": digest_json(attempt_core)}
    attempt_path = raw_root / "attempt.json"
    attempt_path.write_text(json.dumps(attempt), encoding="utf-8")
    completion = build_raw_completion_manifest(verified, attempt_path, raw_root)
    completion_path = raw_root / "completion.json"
    completion_path.write_text(json.dumps(completion), encoding="utf-8")
    return verified, raw_root, completion_path


def test_synthetic_executable_chain_gate_a_through_e_and_tamper_rejection(tmp_path: Path) -> None:
    verified, raw_root, completion = _synthetic_verified_plan(tmp_path)
    gates = tmp_path / "gates"
    gate_a_path = gates / "A.json"
    write_gate(gate_a_path, derive_gate_a(verified, completion, raw_root))

    normalized = tmp_path / "normalized"
    build_normalized_1h_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        target=normalized,
    )
    gate_b_path = gates / "B.json"
    gate_b = derive_gate_b(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        normalized_stage=normalized,
    )
    write_gate(gate_b_path, gate_b)

    # A self-hashed fabricated PASS cannot replace the gate derived from actual upstream bytes.
    fake_b = derived_gate("B", gate_b["upstream"], {"fabricated": True}, {"fake": True})
    gate_b_path.write_text(json.dumps(fake_b), encoding="utf-8")
    with pytest.raises(AcquisitionInvariantError, match="currently derived Gate B"):
        build_4h_stage(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            gate_b_path=gate_b_path,
            normalized_stage=normalized,
            target=tmp_path / "must-not-publish",
        )
    gate_b_path.write_text(json.dumps(gate_b), encoding="utf-8")

    stage_4h = tmp_path / "four_hour"
    build_4h_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        target=stage_4h,
    )
    gate_c_path = gates / "C.json"
    write_gate(
        gate_c_path,
        derive_gate_c(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            gate_b_path=gate_b_path,
            normalized_stage=normalized,
            stage_4h=stage_4h,
        ),
    )

    lifecycle = tmp_path / "lifecycle"
    build_lifecycle_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
        target=lifecycle,
    )
    gate_d_path = gates / "D.json"
    write_gate(
        gate_d_path,
        derive_gate_d(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            gate_b_path=gate_b_path,
            normalized_stage=normalized,
            gate_c_path=gate_c_path,
            stage_4h=stage_4h,
            lifecycle_stage=lifecycle,
        ),
    )

    scanner = tmp_path / "scanner"
    build_scanner_engineering_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
        gate_d_path=gate_d_path,
        lifecycle_stage=lifecycle,
        target=scanner,
    )
    gate_e = derive_gate_e(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        gate_c_path=gate_c_path,
        stage_4h=stage_4h,
        gate_d_path=gate_d_path,
        lifecycle_stage=lifecycle,
        scanner_stage=scanner,
    )
    assert gate_e["status"] == "passed" and gate_e["performance_aggregate_emitted"] is False
    serialized_details = json.dumps(gate_e["details"]).lower()
    for forbidden in ("mean_return", "path_probability", "success_rate", "hot_performance"):
        assert forbidden not in serialized_details
    assert not any(path.name.lower().startswith("gate_f") for path in gates.glob("*"))
    manifest = json.loads((scanner / "manifest.json").read_text())
    assert manifest["gate_f_created"] is False
    assert manifest["sealed_splits"]["validation"]["access_status"].startswith("sealed")
    assert manifest["sealed_splits"]["final_holdout"]["access_status"].startswith("sealed")

    # Tamper verified raw bytes after Gate A/B: downstream re-derivation must fail.
    first_raw = next(raw_root.glob("*.response"))
    first_raw.write_bytes(first_raw.read_bytes() + b" ")
    with pytest.raises(AcquisitionInvariantError, match="Raw source changed"):
        derive_gate_b(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            normalized_stage=normalized,
        )
