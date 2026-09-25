from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def scheduler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path
    (root / "scripts").mkdir()
    (root / "tasks" / "queued").mkdir(parents=True)
    (root / "scripts" / "noop.py").write_text("print('ok')\n", encoding="utf-8")
    source = Path(__file__).parents[1] / "scripts" / "research_scheduler.py"
    spec = importlib.util.spec_from_file_location("research_scheduler_test", source)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", root)
    monkeypatch.setattr(module, "TASK_DIR", root / "tasks" / "queued")
    monkeypatch.setattr(module, "STATE_PATH", root / ".auto_runtime" / "research_scheduler_v1.json")
    monkeypatch.setattr(module, "CANARY_PATH", root / ".auto_runtime" / "lifecycle_canary_v1.json")
    return module


def write_task(root: Path, *, task_id: str = "R1", kind: str = "research", max_attempts: int = 2) -> Path:
    payload = {
        "task_id": task_id,
        "program_id": "SCANNER-V0.1",
        "priority": "P0",
        "command": ["python", "scripts/noop.py"],
        "timeout_seconds": 10,
        "max_attempts": max_attempts,
        "writes": ["reports/auto/"],
        "reads": ["docs/RESEARCH_SPEC_V0_1.md"],
        "mutable_resources": ["reports/auto"],
        "provider_dependencies": [],
        "auto_authorized": True,
        "task_kind": kind,
    }
    path = root / "tasks" / "queued" / f"{task_id}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_canary_replay_is_idempotent(scheduler) -> None:
    first = scheduler.run_canary()
    second = scheduler.run_canary()
    assert first["status"] == "COMPLETED"
    assert second["idempotent_replay"] is True
    assert second["idempotent_replay_verified_at"]


def test_completed_task_does_not_reexecute(scheduler) -> None:
    write_task(scheduler.ROOT)
    first = scheduler.execute("R1")
    second = scheduler.execute("R1")
    assert first["claimed"] is True
    assert first["task"]["status"] == "COMPLETED"
    assert second["claimed"] is False
    assert second["reason"] == "already_completed"
    assert scheduler.load_state()["tasks"]["R1"]["attempts"] == 1


def test_status_init_does_not_recover_live_running_task(scheduler) -> None:
    write_task(scheduler.ROOT)
    scheduler.init_state()
    scheduler.claim("R1")
    observed = scheduler.init_state()
    assert observed["tasks"]["R1"]["status"] == "RUNNING"
    assert observed["lane"]["primary"] == "R1"


def test_daemon_startup_recovery_preserves_attempt_count(scheduler) -> None:
    write_task(scheduler.ROOT)
    scheduler.init_state()
    scheduler.claim("R1")
    recovered = scheduler.recover_interrupted_runs()
    assert recovered["tasks"]["R1"]["status"] == "QUEUED"
    assert recovered["tasks"]["R1"]["attempts"] == 1
    assert recovered["tasks"]["R1"]["last_error"] == "recovered_interrupted_run"
    assert recovered["lane"]["primary"] is None


def test_manifest_drift_requires_decision(scheduler) -> None:
    path = write_task(scheduler.ROOT)
    scheduler.init_state()
    data = json.loads(path.read_text())
    data["timeout_seconds"] = 11
    path.write_text(json.dumps(data), encoding="utf-8")
    state = scheduler.init_state()
    assert state["tasks"]["R1"]["status"] == "DECISION_REQUIRED"
    assert state["autonomy_ready"] is False


def test_bounded_retry_exhaustion(scheduler) -> None:
    path = write_task(scheduler.ROOT, max_attempts=1)
    data = json.loads(path.read_text())
    data["command"] = ["python", "scripts/fail.py"]
    path.write_text(json.dumps(data), encoding="utf-8")
    (scheduler.ROOT / "scripts" / "fail.py").write_text("raise SystemExit(7)\n", encoding="utf-8")
    out = scheduler.execute("R1")
    assert out["task"]["status"] == "FAILED"
    assert scheduler.load_state()["tasks"]["R1"]["attempts"] == 1


def test_only_python_repo_scripts_are_allowed(scheduler) -> None:
    path = write_task(scheduler.ROOT)
    data = json.loads(path.read_text())
    data["command"] = ["bash", "scripts/noop.py"]
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="project Python"):
        scheduler.discover_manifests()


def test_research_task_cannot_fake_real_work_with_pytest(scheduler) -> None:
    path = write_task(scheduler.ROOT)
    data = json.loads(path.read_text())
    data["command"] = ["python", "-m", "pytest"]
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="bounded repository script"):
        scheduler.discover_manifests()


def test_restart_guard_detects_duplicate_attempt(scheduler) -> None:
    write_task(scheduler.ROOT)
    scheduler.execute("R1")
    state = scheduler.load_state()
    state["qualification"]["restart_requested_at"] = scheduler.now_iso()
    state["qualification"]["completed_research_attempts_at_restart"] = {"R1": 1}
    scheduler.save_state(state)
    assert scheduler.restart_without_duplicate_ok(scheduler.load_state()) is True
    state = scheduler.load_state()
    state["tasks"]["R1"]["attempts"] = 2
    scheduler.save_state(state)
    assert scheduler.restart_without_duplicate_ok(scheduler.load_state()) is False
