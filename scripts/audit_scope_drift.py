#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def audit(review_path: Path, inventory_path: Path) -> dict[str, Any]:
    review = load_object(review_path)
    inventory = load_object(inventory_path)
    if review.get("schema_version") != "candidate-scope-review-required-v1":
        raise ValueError("unexpected review-required schema")
    if review.get("status") != "review_required":
        raise ValueError("artifact does not require independent scope review")
    current = review.get("candidate_set_digest")
    reviewed = review.get("reviewed_candidate_set_digest")
    if not isinstance(current, str) or len(current) != 64:
        raise ValueError("current candidate digest is malformed")
    if not isinstance(reviewed, str) or len(reviewed) != 64:
        raise ValueError("reviewed candidate digest is malformed")
    if current == reviewed:
        raise ValueError("candidate digests do not demonstrate drift")
    if inventory.get("candidate_set_digest") != current:
        raise ValueError("inventory digest does not match review-required artifact")
    added = review.get("added_candidates")
    removed = review.get("removed_candidates")
    identities = inventory.get("candidate_identities")
    if not isinstance(added, list) or not all(isinstance(x, str) for x in added):
        raise ValueError("added_candidates is malformed")
    if not isinstance(removed, list) or not all(isinstance(x, str) for x in removed):
        raise ValueError("removed_candidates is malformed")
    if not isinstance(identities, list) or inventory.get("candidate_count") != len(identities):
        raise ValueError("candidate inventory count is inconsistent")
    if not set(added).issubset(set(identities)):
        raise ValueError("added candidates are missing from current inventory")
    return {
        "schema_version": "auto-scope-drift-audit-v1",
        "status": "REVIEW_REQUIRED",
        "research_action": "NO_SCOPE_ASSIGNMENT_OR_AUTO_APPROVAL",
        "current_candidate_set_digest": current,
        "reviewed_candidate_set_digest": reviewed,
        "candidate_count": inventory["candidate_count"],
        "added_candidate_count": len(added),
        "removed_candidate_count": len(removed),
        "added_candidates": added,
        "removed_candidates": removed,
        "review_required_sha256": sha256_path(review_path),
        "candidate_inventory_sha256": sha256_path(inventory_path),
    }


def write_deterministic(path: Path, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise FileExistsError(f"existing audit output differs: {path}")
        return
    path.write_bytes(raw)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit a frozen lifecycle candidate-scope drift artifact without assigning scope.")
    parser.add_argument("--review-required", required=True)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = audit(Path(args.review_required), Path(args.inventory))
    write_deterministic(Path(args.output), result)
    print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
