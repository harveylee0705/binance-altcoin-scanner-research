from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from alt_hot_scanner.data.acquisition import (
    AcquisitionInvariantError,
    api_tail_request,
    classify_gaps,
    classify_raw_conflict,
    fetch_frozen_cutoff,
    gate_artifact,
    publish_staged,
    require_complete_attempt_manifest,
    validate_api_tail,
    validate_source_containment,
)
from alt_hot_scanner.data.binance_public import validate_archive_object_key

MONTH = "data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip"
DAILY = "data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-02-03.zip"

def test_daily_object_grammar_and_containment() -> None:
    identity = validate_archive_object_key(DAILY)
    assert identity.source_kind == "daily" and identity.period == "2024-02-03"
    with pytest.raises(ValueError):
        validate_archive_object_key(DAILY.replace("2024-02-03", "2024-02-31"))
    frame = pd.DataFrame({"open_time": pd.to_datetime(["2024-02-03T00:00:00Z"]), "x": [1]})
    validate_source_containment(frame, "daily", "2024-02-03")
    with pytest.raises(AcquisitionInvariantError):
        validate_source_containment(pd.DataFrame({"open_time": pd.to_datetime(["2024-02-04T00:00:00Z"])}), "daily", "2024-02-03")

def test_monthly_daily_boundaries_are_disjoint() -> None:
    assert validate_archive_object_key(MONTH).source_kind == "monthly"
    monthly_end = pd.Timestamp("2024-01-01", tz="UTC") + pd.offsets.MonthBegin(1)
    daily_start = pd.Timestamp("2024-02-01", tz="UTC")
    assert monthly_end == daily_start

def test_server_cutoff_and_incomplete_bar() -> None:
    assert fetch_frozen_cutoff(lambda: json.dumps({"serverTime": 1706785500123}).encode()) == "2024-02-01T11:00:00+00:00"
    req = api_tail_request("BTCUSDT", pd.Timestamp("2024-02-01T01:00:00Z"), "2024-02-01T03:00:00Z")
    assert req and req["interval"] == "1h" and req["endTime"] < int(pd.Timestamp("2024-02-01T03:00:00Z").timestamp() * 1000)
    rows = [[int(pd.Timestamp("2024-02-01T01:00:00Z").timestamp()*1000)] + [0]*6, [int(pd.Timestamp("2024-02-01T02:00:00Z").timestamp()*1000)] + [0]*6]
    validate_api_tail(rows, "BTCUSDT", pd.Timestamp("2024-02-01T01:00:00Z"), "2024-02-01T03:00:00Z")
    with pytest.raises(AcquisitionInvariantError):
        validate_api_tail(rows + [[int(pd.Timestamp("2024-02-01T04:00:00Z").timestamp()*1000)]], "BTCUSDT", pd.Timestamp("2024-02-01T01:00:00Z"), "2024-02-01T05:00:00Z")

def test_api_cannot_repair_interior_gap_and_gaps_are_classified() -> None:
    gaps = classify_gaps("BTCUSDT", pd.to_datetime(["2024-01-01T00:00Z", "2024-01-01T02:00Z"]))
    assert gaps[0].kind == "missing_1h_row_inside_expected_source_interval"
    with pytest.raises(AcquisitionInvariantError):
        validate_api_tail([[0]], "BTCUSDT", pd.Timestamp("1970-01-01T02:00Z"), "1970-01-01T04:00Z")

def test_attempt_completion_rejects_missing_limit_and_unapproved() -> None:
    plan = {"planning_basis": "exact_observed_source_plan", "objects": [MONTH, DAILY]}
    verified = [{"object_key": MONTH, "status": "verified"}, {"object_key": DAILY, "status": "verified"}]
    require_complete_attempt_manifest(plan, verified)
    for attempts, canonical in [(verified[:1], True), (verified, False), ([{**verified[0], "status": "missing"}, verified[1]], True)]:
        with pytest.raises(AcquisitionInvariantError):
            require_complete_attempt_manifest(plan, attempts, canonical=canonical)

def test_revision_classification_and_blind_gates() -> None:
    assert classify_raw_conflict("a", "b", "b") == "upstream_source_revision"
    assert classify_raw_conflict("a", "b", "c") == "local_corruption"
    gate = gate_artifact("A", {"cutoff": True, "inventory": True})
    assert gate["status"] == "passed" and gate["performance_aggregate_emitted"] is False
    assert gate_artifact("E", {"performance": False})["status"] == "failed"

def test_staged_publication_failure_and_success(tmp_path: Path) -> None:
    target = tmp_path / "canonical"
    def writer(stage: Path) -> None:
        (stage / "part").write_text("complete", encoding="utf-8")
    assert Path(publish_staged(target, writer), "_SUCCESS").exists()
    with pytest.raises(FileExistsError):
        publish_staged(target, writer)
    failed = tmp_path / "failed"
    def bad(stage: Path) -> None:
        (stage / "partial").write_text("not published", encoding="utf-8")
        raise RuntimeError("fixture failure")
    with pytest.raises(RuntimeError):
        publish_staged(failed, bad)
    assert not failed.exists()
