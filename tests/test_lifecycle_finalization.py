from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.download_from_plan as downloader
from alt_hot_scanner.data.announcements import classify_article_semantics
from alt_hot_scanner.universe.adjudications import load_lifecycle_adjudications
from alt_hot_scanner.universe.authorization import (
    APPROVAL_SCHEMA_VERSION,
    APPROVAL_STATE_SCHEMA_VERSION,
    content_identity,
    sha256_path,
    verify_approval_pin,
    verify_runtime_matches_approved_commit,
)
from alt_hot_scanner.universe.evidence_replay import (
    build_primitive_evidence_manifest,
    verify_primitive_manifest,
)
from alt_hot_scanner.universe.scope_registry import (
    build_candidate_inventory,
    candidate_inventory_difference,
)


def test_candidate_digest_change_requires_review_without_scope_assignment() -> None:
    old = build_candidate_inventory(
        ["AAAUSDT"], discovered_at="2026-01-01T00:00:00Z", source_identifier="fixture"
    )
    new = build_candidate_inventory(
        ["AAAUSDT", "NEWUSDT"],
        discovered_at="2026-01-02T00:00:00Z",
        source_identifier="fixture",
    )
    review_required = candidate_inventory_difference(new, old)
    assert review_required["status"] == "review_required"
    assert review_required["added_candidates"] == ["NEWUSDT"]
    assert "records" not in review_required


def _approval_fixture(tmp_path: Path, status: str) -> tuple[Path, dict]:
    review = tmp_path / "review.md"
    review.write_text("PASS")
    state_path = tmp_path / "approval-state.json"
    bundle = {
        "bundle_id": "a" * 64,
        "code_commit": "b" * 40,
        "config": {"sha256": "c" * 64},
    }
    core = {
        "schema_version": APPROVAL_SCHEMA_VERSION,
        "lifecycle_bundle_id": bundle["bundle_id"],
        "lifecycle_evidence_code_commit": bundle["code_commit"],
        "config_sha256": bundle["config"]["sha256"],
        "independent_review_artifact": {
            "identifier": "review-1",
            "path": str(review.resolve()),
            "sha256": sha256_path(review),
        },
        "approval_purpose": "full_history_acquisition_after_lifecycle_audit",
        "approval_status": "active",
        "approval_state_registry": {"path": str(state_path.resolve())},
        "approved_at": "2026-01-01T00:00:00Z",
    }
    approval = {**core, "approval_id": content_identity(core)}
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(json.dumps(approval))
    state_core = {
        "schema_version": APPROVAL_STATE_SCHEMA_VERSION,
        "approvals": {
            approval["approval_id"]: {
                "status": status,
                "superseded_by": "d" * 64 if status == "superseded" else None,
                "reason": "fixture transition",
            }
        },
    }
    state_path.write_text(
        json.dumps({**state_core, "registry_id": content_identity(state_core)})
    )
    return approval_path, {"bundle": bundle}


@pytest.mark.parametrize("status", ["revoked", "superseded"])
def test_revoked_and_superseded_approvals_fail_closed(tmp_path: Path, status: str) -> None:
    approval_path, verified = _approval_fixture(tmp_path, status)
    with pytest.raises(ValueError, match=status):
        verify_approval_pin(approval_path, verified)


def test_active_approval_state_is_accepted(tmp_path: Path) -> None:
    approval_path, verified = _approval_fixture(tmp_path, "active")
    assert verify_approval_pin(approval_path, verified)["approval"]["approval_status"] == (
        "active"
    )


@pytest.mark.parametrize(
    ("changed_path", "accepted"),
    [(None, True), ("docs/review.md", True), ("src/module.py", False), ("config/x.yaml", False)],
)
def test_runtime_commit_pin_allows_only_docs_drift(
    tmp_path: Path, changed_path: str | None, accepted: bool
) -> None:
    for directory in ("src", "scripts", "config", "docs"):
        (tmp_path / directory).mkdir()
    (tmp_path / "src" / "module.py").write_text("VALUE = 1\n")
    (tmp_path / "scripts" / "run.py").write_text("print('ok')\n")
    (tmp_path / "config" / "x.yaml").write_text("value: 1\n")
    (tmp_path / "docs" / "review.md").write_text("initial\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='fixture'\nversion='0'\n")
    commands = [
        ["git", "init"],
        ["git", "config", "user.email", "fixture@example.test"],
        ["git", "config", "user.name", "Fixture"],
        ["git", "add", "."],
        ["git", "commit", "-m", "baseline"],
    ]
    for command in commands:
        subprocess.run(command, cwd=tmp_path, check=True, capture_output=True)
    approved = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if changed_path is not None:
        target = tmp_path / changed_path
        target.write_text(target.read_text() + "changed\n")
        subprocess.run(["git", "add", changed_path], cwd=tmp_path, check=True)
        subprocess.run(
            ["git", "commit", "-m", "later change"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
    if accepted:
        verify_runtime_matches_approved_commit(tmp_path, approved)
    else:
        with pytest.raises(ValueError, match="differs"):
            verify_runtime_matches_approved_commit(tmp_path, approved)


@pytest.mark.parametrize(("execute", "expected_checks"), [(False, 1), (True, 2)])
def test_downloader_rechecks_commit_at_dry_run_and_execute_boundaries(
    monkeypatch: pytest.MonkeyPatch, execute: bool, expected_checks: int
) -> None:
    checks: list[str] = []
    monkeypatch.setattr(
        downloader,
        "parse_args",
        lambda: SimpleNamespace(plan="ignored.json", workers=1, limit=None, execute=execute),
    )
    monkeypatch.setattr(
        downloader,
        "verify_bound_plan",
        lambda path: {
            "plan": {"objects": []},
            "verified_approval": {
                "approval": {"lifecycle_evidence_code_commit": "a" * 40}
            },
        },
    )
    monkeypatch.setattr(
        downloader,
        "verify_runtime_matches_approved_commit",
        lambda root, commit: checks.append(commit),
    )
    monkeypatch.setattr(downloader, "write_json_exclusive", lambda path, value: None)
    monkeypatch.setattr(downloader, "collision_resistant_run_id", lambda: "fixture")
    downloader.main()
    assert checks == ["a" * 40] * expected_checks


def test_primitive_manifest_detects_raw_evidence_mutation(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    exchange = raw / "exchange_info.json"
    exchange.write_text('{"symbols": []}')
    manifest = build_primitive_evidence_manifest(raw, first_trades=[])
    manifest_path = tmp_path / "primitive_evidence_manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    assert verify_primitive_manifest(manifest_path)["manifest_id"] == manifest["manifest_id"]
    exchange.write_text('{"symbols": ["changed"]}')
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_primitive_manifest(manifest_path)


def test_futures_product_semantics_reject_coin_margined_and_spot_delistings() -> None:
    assert classify_article_semantics(
        "Binance Futures Will Delist CRV Coin-Margined Perpetual Contract", ""
    ) == "irrelevant"
    assert classify_article_semantics(
        "Binance Will Delist ABCUSDT on Spot",
        "ABCUSDT Perpetual Contract remains available on Binance Futures.",
    ) == "irrelevant"


def test_reviewed_conflict_adjudications_preserve_required_dispositions() -> None:
    root = Path(__file__).resolve().parents[1]
    digest = "b323b3c3ba1010654d9ce1a47325364f7c7e624624857554b7e945f38f91c445"
    adjudications = load_lifecycle_adjudications(
        root / "config" / "lifecycle_adjudications_v1.json",
        candidate_set_digest=digest,
    )["by_symbol"]
    required = {
        "KNCUSDT",
        "AERGOUSDT",
        "AIAUSDT",
        "CTKUSDT",
        "CVCUSDT",
        "CVXUSDT",
        "MAVIAUSDT",
        "OMGUSDT",
        "SLPUSDT",
        "XEMUSDT",
    }
    assert required <= set(adjudications)
    assert adjudications["KNCUSDT"]["rejected_listing_articles"]
    assert {
        symbol for symbol, record in adjudications.items() if len(record["episodes"]) > 1
    } == {
        "AERGOUSDT",
        "AIAUSDT",
        "CTKUSDT",
        "CVCUSDT",
        "CVXUSDT",
        "ICPUSDT",
        "MAVIAUSDT",
        "SLPUSDT",
    }
    assert adjudications["OMGUSDT"]["episodes"][-1]["termination_basis"]
    assert adjudications["XEMUSDT"]["episodes"][-1]["termination_basis"]
