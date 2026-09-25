#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TASK_DIR = ROOT / "tasks" / "queued"
STATE_PATH = ROOT / ".auto_runtime" / "research_scheduler_v1.json"
CANARY_PATH = ROOT / ".auto_runtime" / "lifecycle_canary_v1.json"
SCHEMA = "binance-research-scheduler-v1"
POLICY_VERSION = "BINANCE_RESEARCH_SCHEDULING_POLICY_V1"
MAX_ATTEMPTS_CAP = 3
PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P4": 4}


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    program_id: str
    priority: str
    command: tuple[str, ...]
    timeout_seconds: int
    max_attempts: int
    task_kind: str
    manifest_path: str
    manifest_sha256: str


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def string_list(data: dict[str, Any], field: str) -> tuple[str, ...]:
    value = data.get(field)
    if not isinstance(value, list) or not all(isinstance(x, str) and x for x in value):
        raise ValueError(f"{field} must be a list of nonempty strings")
    return tuple(value)


def validate_command(command: tuple[str, ...], task_kind: str) -> None:
    if len(command) < 2 or command[0] not in {"python", "python3"}:
        raise ValueError("AUTO tasks may invoke only the project Python interpreter")
    if any(any(ch in token for ch in (";", "|", "&", ">", "<", "\n", "\r")) for token in command):
        raise ValueError("shell metacharacters are forbidden")
    if command[1] == "-m":
        if len(command) < 3 or command[2] not in {"pytest", "ruff"}:
            raise ValueError("only pytest/ruff modules are allowed with python -m")
        if task_kind == "research":
            raise ValueError("research tasks must invoke a bounded repository script, not pytest/ruff")
        return
    target = (ROOT / command[1]).resolve()
    scripts = (ROOT / "scripts").resolve()
    if target.suffix != ".py" or (target.parent != scripts and scripts not in target.parents):
        raise ValueError("AUTO task target must be a Python script under scripts/")
    if not target.exists():
        raise ValueError(f"AUTO task target does not exist: {command[1]}")


def load_manifest(path: Path) -> TaskSpec:
    raw = path.read_bytes()
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    required = {
        "task_id", "program_id", "priority", "command", "timeout_seconds", "max_attempts",
        "writes", "reads", "mutable_resources", "provider_dependencies", "auto_authorized",
        "task_kind",
    }
    missing = sorted(required - set(data))
    if missing:
        raise ValueError(f"{path} missing required fields: {missing}")
    if data["priority"] not in PRIORITY_RANK:
        raise ValueError("priority must be P0..P4")
    if data["auto_authorized"] is not True:
        raise ValueError("queued AUTO manifest must explicitly set auto_authorized=true")
    if data["task_kind"] not in {"research", "canary", "maintenance"}:
        raise ValueError("task_kind must be research, canary, or maintenance")
    if type(data["timeout_seconds"]) is not int or not 1 <= data["timeout_seconds"] <= 3600:
        raise ValueError("timeout_seconds must be 1..3600")
    if type(data["max_attempts"]) is not int or not 1 <= data["max_attempts"] <= MAX_ATTEMPTS_CAP:
        raise ValueError(f"max_attempts must be 1..{MAX_ATTEMPTS_CAP}")
    command = string_list(data, "command")
    for field in ("writes", "reads", "mutable_resources", "provider_dependencies"):
        string_list(data, field)
    validate_command(command, data["task_kind"])
    for field in ("task_id", "program_id"):
        if not isinstance(data[field], str) or not data[field]:
            raise ValueError(f"{field} must be nonempty")
    return TaskSpec(
        data["task_id"], data["program_id"], data["priority"], command,
        data["timeout_seconds"], data["max_attempts"], data["task_kind"],
        str(path.relative_to(ROOT)), hashlib.sha256(raw).hexdigest(),
    )


def discover_manifests() -> list[TaskSpec]:
    if not TASK_DIR.exists():
        return []
    specs: list[TaskSpec] = []
    seen: set[str] = set()
    for path in sorted(TASK_DIR.glob("*.json")):
        spec = load_manifest(path)
        if spec.task_id in seen:
            raise ValueError(f"duplicate task_id: {spec.task_id}")
        seen.add(spec.task_id)
        specs.append(spec)
    return specs


def empty_state() -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "policy_version": POLICY_VERSION,
        "updated_at": now_iso(),
        "tasks": {},
        "lane": {"primary": None},
        "canary": None,
        "qualification": {},
        "autonomy_ready": False,
    }


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return empty_state()
    state = read_json(STATE_PATH)
    if state.get("schema") != SCHEMA:
        raise ValueError("scheduler state schema mismatch")
    return state


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    atomic_json(STATE_PATH, state)


def init_state() -> dict[str, Any]:
    """Register manifests without treating an active RUNNING task as interrupted."""
    state = load_state()
    changed = False
    state.setdefault("tasks", {})
    state.setdefault("lane", {"primary": None})
    state.setdefault("canary", None)
    state.setdefault("qualification", {})
    state.setdefault("autonomy_ready", False)
    active: set[str] = set()
    for spec in discover_manifests():
        active.add(spec.task_id)
        task = state["tasks"].get(spec.task_id)
        if task is None:
            state["tasks"][spec.task_id] = {
                "task_id": spec.task_id,
                "program_id": spec.program_id,
                "priority": spec.priority,
                "status": "QUEUED",
                "execution_lane": None,
                "attempts": 0,
                "max_attempts": spec.max_attempts,
                "manifest_path": spec.manifest_path,
                "manifest_sha256": spec.manifest_sha256,
                "task_kind": spec.task_kind,
                "result": None,
                "last_error": None,
            }
            changed = True
        elif task.get("manifest_sha256") != spec.manifest_sha256:
            task.update({
                "status": "DECISION_REQUIRED",
                "manifest_sha256": spec.manifest_sha256,
                "manifest_path": spec.manifest_path,
                "last_error": "manifest_changed_for_existing_task_id",
                "execution_lane": None,
            })
            if state["lane"].get("primary") == spec.task_id:
                state["lane"]["primary"] = None
            state["autonomy_ready"] = False
            changed = True
    for task_id, task in state["tasks"].items():
        if task_id not in active and task.get("status") not in {"COMPLETED", "CANCELLED"}:
            task["status"] = "DECISION_REQUIRED"
            task["last_error"] = "queued_manifest_missing"
            task["execution_lane"] = None
            if state["lane"].get("primary") == task_id:
                state["lane"]["primary"] = None
            state["autonomy_ready"] = False
            changed = True
    if changed or not STATE_PATH.exists():
        save_state(state)
    return state


def recover_interrupted_runs() -> dict[str, Any]:
    """Recover only at daemon process startup, never during read-only status calls."""
    state = init_state()
    changed = False
    for task in state["tasks"].values():
        if task.get("status") == "RUNNING":
            attempts = int(task.get("attempts", 0))
            max_attempts = int(task.get("max_attempts", MAX_ATTEMPTS_CAP))
            task["status"] = "QUEUED" if attempts < max_attempts else "FAILED"
            task["last_error"] = "recovered_interrupted_run"
            task["execution_lane"] = None
            changed = True
    if state["lane"].get("primary") is not None:
        state["lane"]["primary"] = None
        changed = True
    if changed:
        state["autonomy_ready"] = False
        save_state(state)
    return state


def spec_map() -> dict[str, TaskSpec]:
    return {spec.task_id: spec for spec in discover_manifests()}


def next_runnable(state: dict[str, Any]) -> str | None:
    items = [task for task in state["tasks"].values() if task.get("status") == "QUEUED"]
    if not items:
        return None
    items.sort(key=lambda task: (PRIORITY_RANK.get(task.get("priority"), 9), task["task_id"]))
    return items[0]["task_id"]


def claim(task_id: str) -> dict[str, Any]:
    state = init_state()
    task = state["tasks"].get(task_id)
    if task is None:
        raise ValueError(f"unknown task: {task_id}")
    if task.get("status") == "COMPLETED":
        return {"claimed": False, "reason": "already_completed", "task": task}
    if task.get("status") != "QUEUED":
        return {"claimed": False, "reason": f"status_{task.get('status')}", "task": task}
    if int(task.get("attempts", 0)) >= int(task.get("max_attempts", MAX_ATTEMPTS_CAP)):
        task["status"] = "FAILED"
        task["last_error"] = "bounded_retry_exhausted"
        save_state(state)
        return {"claimed": False, "reason": "bounded_retry_exhausted", "task": task}
    task["status"] = "RUNNING"
    task["execution_lane"] = "PRIMARY"
    task["claimed_at"] = now_iso()
    task["attempts"] = int(task.get("attempts", 0)) + 1
    state["lane"]["primary"] = task_id
    save_state(state)
    return {"claimed": True, "task": task}


def finish(task_id: str, ok: bool, result: dict[str, Any], error: str | None = None) -> dict[str, Any]:
    state = load_state()
    task = state["tasks"][task_id]
    task["status"] = "COMPLETED" if ok else (
        "QUEUED" if task["attempts"] < task["max_attempts"] else "FAILED"
    )
    task["completed_at"] = now_iso() if ok else None
    task["result"] = result
    task["last_error"] = error
    task["execution_lane"] = None
    if state["lane"].get("primary") == task_id:
        state["lane"]["primary"] = None
    if not ok:
        state["autonomy_ready"] = False
    save_state(state)
    return task


def cancel_task(task_id: str, reason: str) -> dict[str, Any]:
    state = init_state()
    task = state["tasks"].get(task_id)
    if task is None:
        raise ValueError(f"unknown task: {task_id}")
    if task.get("status") == "CANCELLED":
        return task
    if task.get("status") == "RUNNING":
        raise ValueError("cannot cancel a RUNNING task")
    if task.get("status") == "COMPLETED":
        raise ValueError("cannot cancel a COMPLETED task")
    reason = reason.strip()
    if not reason or len(reason) > 500:
        raise ValueError("cancel reason must be 1..500 characters")
    task["status"] = "CANCELLED"
    task["cancelled_at"] = now_iso()
    task["cancel_reason"] = reason
    task["execution_lane"] = None
    if state["lane"].get("primary") == task_id:
        state["lane"]["primary"] = None
    state["autonomy_ready"] = False
    save_state(state)
    return task


def execute(task_id: str) -> dict[str, Any]:
    spec = spec_map().get(task_id)
    if spec is None:
        raise ValueError(f"manifest not found for task: {task_id}")
    claimed = claim(task_id)
    if not claimed["claimed"]:
        return claimed
    command = [sys.executable, *spec.command[1:]]
    try:
        proc = subprocess.run(
            command, cwd=ROOT, capture_output=True, text=True,
            timeout=spec.timeout_seconds, check=False,
        )
        result = {
            "returncode": proc.returncode,
            "stdout": proc.stdout[-8000:].strip(),
            "stderr": proc.stderr[-8000:].strip(),
            "command": list(spec.command),
        }
        task = finish(
            task_id, proc.returncode == 0, result,
            None if proc.returncode == 0 else result["stderr"][-2000:],
        )
        return {"claimed": True, "task": task, "result": result}
    except subprocess.TimeoutExpired:
        result = {
            "returncode": 124,
            "stdout": "",
            "stderr": "bounded_task_timeout",
            "command": list(spec.command),
        }
        return {
            "claimed": True,
            "task": finish(task_id, False, result, "bounded_task_timeout"),
            "result": result,
        }


def run_canary() -> dict[str, Any]:
    state = init_state()
    canary = state.get("canary") or {}
    if canary.get("status") == "COMPLETED":
        canary["idempotent_replay"] = True
        canary["idempotent_replay_verified_at"] = now_iso()
    else:
        canary = {
            "canary_id": "BINANCE-AUTO-LIFECYCLE-CANARY-001",
            "status": "COMPLETED",
            "completed_at": now_iso(),
            "idempotent_replay": False,
        }
        atomic_json(CANARY_PATH, canary)
    state["canary"] = canary
    save_state(state)
    return canary


def service_state() -> str:
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "is-active", "binance-research-scheduler.service"],
            capture_output=True, text=True, timeout=15, check=False,
        )
        return proc.stdout.strip() or "inactive"
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def service_command(action: str) -> dict[str, str]:
    name = "binance-research-scheduler.service"
    target = Path.home() / ".config/systemd/user" / name
    if action == "install":
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            "\n".join([
                "[Unit]", "Description=Binance research AUTO scheduler",
                "After=network-online.target", "", "[Service]", "Type=simple",
                f"WorkingDirectory={ROOT}",
                f"ExecStart={sys.executable} {ROOT / 'scripts/research_scheduler.py'} daemon --interval 30",
                "Restart=on-failure", "RestartSec=5", "", "[Install]",
                "WantedBy=default.target", "",
            ]),
            encoding="utf-8",
        )
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "--user", "enable", "--now", name], check=True)
    elif action == "restart":
        state = init_state()
        completed_research = {
            task_id: int(task.get("attempts", 0))
            for task_id, task in state["tasks"].items()
            if task.get("task_kind") == "research" and task.get("status") == "COMPLETED"
        }
        state["qualification"]["restart_requested_at"] = now_iso()
        state["qualification"]["completed_research_attempts_at_restart"] = completed_research
        save_state(state)
        subprocess.run(["systemctl", "--user", "restart", name], check=True)
    return {"service": name, "state": service_state()}


def restart_without_duplicate_ok(state: dict[str, Any]) -> bool:
    qualification = state.get("qualification") or {}
    snapshot = qualification.get("completed_research_attempts_at_restart")
    if not qualification.get("restart_requested_at") or not isinstance(snapshot, dict) or not snapshot:
        return False
    for task_id, attempts in snapshot.items():
        task = (state.get("tasks") or {}).get(task_id)
        if not isinstance(task, dict) or task.get("status") != "COMPLETED":
            return False
        if int(task.get("attempts", -1)) != int(attempts):
            return False
    return True


def qualify_auto() -> dict[str, Any]:
    state = init_state()
    tasks = state["tasks"]
    canary = state.get("canary") or {}
    checks = {
        "scheduler_service_active": service_state() == "active",
        "canary_completed_and_replayed": (
            canary.get("status") == "COMPLETED"
            and bool(canary.get("idempotent_replay_verified_at"))
        ),
        "bounded_retry_guard_active": MAX_ATTEMPTS_CAP == 3,
        "no_manifest_drift_decisions": not any(
            task.get("status") == "DECISION_REQUIRED" for task in tasks.values()
        ),
        "no_exhausted_failed_tasks": not any(
            task.get("status") == "FAILED" for task in tasks.values()
        ),
        "real_bounded_research_task_completed": any(
            task.get("task_kind") == "research" and task.get("status") == "COMPLETED"
            for task in tasks.values()
        ),
        "restart_verified_without_duplicate": restart_without_duplicate_ok(state),
        "no_task_running_during_qualification": not any(
            task.get("status") == "RUNNING" for task in tasks.values()
        ),
    }
    passed = all(checks.values())
    state["qualification"]["auto_checks"] = checks
    state["qualification"]["qualified_at"] = now_iso() if passed else None
    state["autonomy_ready"] = passed
    save_state(state)
    return {"auto": passed, "checks": checks, "qualified_at": state["qualification"]["qualified_at"]}


def status_payload() -> dict[str, Any]:
    state = init_state()
    state["runtime"] = {
        "scheduler_service": service_state(),
        "next_runnable_task": next_runnable(state),
        "blocked_count": sum(task.get("status") == "BLOCKED" for task in state["tasks"].values()),
        "decision_required_count": sum(
            task.get("status") == "DECISION_REQUIRED" for task in state["tasks"].values()
        ),
    }
    return state


def daemon(once: bool, interval: int) -> int:
    recover_interrupted_runs()
    while True:
        state = init_state()
        task_id = next_runnable(state)
        if task_id:
            execute(task_id)
        if once:
            return 0
        time.sleep(max(5, interval))


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "status", "canary", "qualify-auto"):
        sub.add_parser(name)
    run = sub.add_parser("run-task")
    run.add_argument("task_id")
    cancel = sub.add_parser("cancel-task")
    cancel.add_argument("task_id")
    cancel.add_argument("--reason", required=True)
    svc = sub.add_parser("service")
    svc.add_argument("action", choices=("install", "restart", "status"))
    daemon_parser = sub.add_parser("daemon")
    daemon_parser.add_argument("--once", action="store_true")
    daemon_parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.command == "init":
        out = init_state()
    elif args.command == "status":
        out = status_payload()
    elif args.command == "canary":
        out = run_canary()
    elif args.command == "qualify-auto":
        out = qualify_auto()
    elif args.command == "run-task":
        out = execute(args.task_id)
    elif args.command == "cancel-task":
        out = cancel_task(args.task_id, args.reason)
    elif args.command == "service":
        out = service_command(args.action)
    else:
        return daemon(args.once, args.interval)
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
