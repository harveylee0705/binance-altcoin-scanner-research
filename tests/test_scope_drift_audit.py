from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.audit_scope_drift import audit, write_deterministic


def fixture(tmp_path: Path) -> tuple[Path, Path]:
    review = tmp_path / "review.json"
    inventory = tmp_path / "inventory.json"
    current = "a" * 64
    reviewed = "b" * 64
    review.write_text(json.dumps({
        "schema_version": "candidate-scope-review-required-v1",
        "status": "review_required",
        "candidate_set_digest": current,
        "reviewed_candidate_set_digest": reviewed,
        "added_candidates": ["NEWUSDT"],
        "removed_candidates": [],
    }), encoding="utf-8")
    inventory.write_text(json.dumps({
        "candidate_set_digest": current,
        "candidate_count": 2,
        "candidate_identities": ["OLDUSDT", "NEWUSDT"],
    }), encoding="utf-8")
    return review, inventory


def test_scope_drift_audit_preserves_review_boundary(tmp_path: Path) -> None:
    review, inventory = fixture(tmp_path)
    result = audit(review, inventory)
    assert result["status"] == "REVIEW_REQUIRED"
    assert result["research_action"] == "NO_SCOPE_ASSIGNMENT_OR_AUTO_APPROVAL"
    assert result["added_candidate_count"] == 1
    assert result["removed_candidate_count"] == 0


def test_scope_drift_audit_rejects_no_drift_or_digest_mismatch(tmp_path: Path) -> None:
    review, inventory = fixture(tmp_path)
    payload = json.loads(review.read_text())
    payload["reviewed_candidate_set_digest"] = payload["candidate_set_digest"]
    review.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="do not demonstrate drift"):
        audit(review, inventory)

    review, inventory = fixture(tmp_path)
    inv = json.loads(inventory.read_text())
    inv["candidate_set_digest"] = "c" * 64
    inventory.write_text(json.dumps(inv), encoding="utf-8")
    with pytest.raises(ValueError, match="inventory digest"):
        audit(review, inventory)


def test_scope_drift_output_is_idempotent_and_fail_closed(tmp_path: Path) -> None:
    review, inventory = fixture(tmp_path)
    result = audit(review, inventory)
    output = tmp_path / "audit.json"
    write_deterministic(output, result)
    write_deterministic(output, result)
    output.write_text("{}\n", encoding="utf-8")
    with pytest.raises(FileExistsError):
        write_deterministic(output, result)
