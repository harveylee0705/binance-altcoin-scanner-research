from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from alt_hot_scanner.identity import require_semantic_contract_identity

CANDIDATE_INVENTORY_SCHEMA_VERSION = "lifecycle-candidate-inventory-v1"
SCOPE_REGISTRY_SCHEMA_VERSION = "historical-scope-registry-v2"
SCOPE_REVIEW_SCHEMA_VERSION = "historical-scope-independent-review-v1"


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _content_identity(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def canonical_candidate_set(candidates: list[str] | tuple[str, ...]) -> list[str]:
    validated = [
        require_semantic_contract_identity(value, f"candidate[{index}]")
        for index, value in enumerate(candidates)
    ]
    if len(validated) != len(set(validated)):
        raise ValueError("Historical candidate identities must be unique")
    return sorted(validated)


def candidate_set_digest(candidates: list[str] | tuple[str, ...]) -> str:
    payload = json.dumps(
        canonical_candidate_set(candidates), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_candidate_inventory(
    candidates: list[str] | tuple[str, ...],
    *,
    discovered_at: str,
    source_identifier: str,
    discovery_layers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the replaceable machine inventory; this never supplies scope dispositions."""
    identities = canonical_candidate_set(candidates)
    core = {
        "schema_version": CANDIDATE_INVENTORY_SCHEMA_VERSION,
        "candidate_set_digest": candidate_set_digest(identities),
        "candidate_count": len(identities),
        "candidate_identities": identities,
        "discovered_at": discovered_at,
        "source_identifier": source_identifier,
    }
    if discovery_layers is not None:
        if not discovery_layers:
            raise ValueError("Candidate discovery layers must not be empty")
        core["discovery_layers"] = discovery_layers
    return {**core, "inventory_id": _content_identity(core)}


def candidate_inventory_difference(
    inventory: dict[str, Any], reviewed_registry: dict[str, Any]
) -> dict[str, Any]:
    discovered = set(inventory.get("candidate_identities", []))
    reviewed = set(reviewed_registry.get("candidate_identities", []))
    return {
        "schema_version": "candidate-scope-review-required-v1",
        "status": "review_required",
        "candidate_set_digest": inventory.get("candidate_set_digest"),
        "reviewed_candidate_set_digest": reviewed_registry.get("candidate_set_digest"),
        "added_candidates": sorted(discovered - reviewed),
        "removed_candidates": sorted(reviewed - discovered),
        "message": "Candidate inventory changed; no scope negatives were assigned automatically.",
    }


def scope_registry_payload_identity(registry: dict[str, Any]) -> str:
    payload = {
        key: value
        for key, value in registry.items()
        if key not in {"registry_id", "registry_payload_id", "independent_review"}
    }
    return _content_identity(payload)


def scope_registry_identity(registry: dict[str, Any]) -> str:
    return _content_identity({key: value for key, value in registry.items() if key != "registry_id"})


def _verify_positive_evidence(record: dict[str, Any], exclusion_class: str) -> None:
    evidence = record.get("evidence")
    if type(evidence) is not list:
        raise ValueError("Scope registry evidence must be a list")
    direct = [
        item
        for item in evidence
        if type(item) is dict
        and item.get("evidence_class") == "direct_positive_exclusion"
        and item.get("exclusion_class") == exclusion_class
    ]
    required = {
        "evidence_class",
        "exclusion_class",
        "source_name",
        "source_type",
        "source_identifier",
        "source_url",
        "reviewed_at",
        "evidence_summary",
        "review_version",
    }
    if len(direct) != 1 or set(direct[0]) != required:
        raise ValueError(f"Positive {exclusion_class} exclusion lacks exact direct evidence")
    if any(
        not isinstance(direct[0][field], str) or not direct[0][field].strip()
        for field in required
    ):
        raise ValueError(f"Positive {exclusion_class} evidence contains an empty field")


def _verify_review_artifact(
    registry: dict[str, Any], registry_path: Path, repository_root: Path
) -> dict[str, Any]:
    descriptor = registry.get("independent_review")
    required = {"identifier", "path", "sha256", "verdict"}
    if type(descriptor) is not dict or set(descriptor) != required:
        raise ValueError("Reviewed scope registry lacks an exact independent-review descriptor")
    if descriptor.get("verdict") != "PASS":
        raise ValueError("Scope registry independent review did not pass")
    review_path = (repository_root / descriptor["path"]).resolve()
    if not review_path.is_relative_to(repository_root.resolve()):
        raise ValueError("Scope review artifact escapes the repository")
    payload = review_path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != descriptor.get("sha256"):
        raise ValueError("Scope review artifact hash changed")
    review = json.loads(payload)
    if (
        review.get("schema_version") != SCOPE_REVIEW_SCHEMA_VERSION
        or review.get("review_id") != descriptor.get("identifier")
        or review.get("verdict") != "PASS"
        or review.get("candidate_set_digest") != registry.get("candidate_set_digest")
        or review.get("scope_registry_payload_id") != registry.get("registry_payload_id")
    ):
        raise ValueError("Scope review artifact does not approve this exact registry payload")
    review_core = {key: value for key, value in review.items() if key != "review_id"}
    if review.get("review_id") != _content_identity(review_core):
        raise ValueError("Scope review artifact identity is invalid")
    if registry_path.resolve() == review_path:
        raise ValueError("Scope registry cannot review itself")
    return review


def verify_scope_registry(
    registry: dict[str, Any],
    candidates: list[str],
    *,
    registry_path: str | Path | None = None,
    repository_root: str | Path | None = None,
    require_independent_review: bool = True,
) -> dict[str, Any]:
    if registry.get("schema_version") != SCOPE_REGISTRY_SCHEMA_VERSION:
        raise ValueError("Historical scope registry has an unsupported schema")
    canonical = canonical_candidate_set(candidates)
    if registry.get("candidate_identities") != canonical:
        raise ValueError("Historical scope registry candidate identities changed")
    digest = candidate_set_digest(canonical)
    if registry.get("candidate_set_digest") != digest:
        raise ValueError("Historical scope registry candidate-set digest mismatch")
    if registry.get("candidate_count") != len(canonical):
        raise ValueError("Historical scope registry candidate count is invalid")
    if registry.get("registry_payload_id") != scope_registry_payload_identity(registry):
        raise ValueError("Historical scope registry payload identity is invalid")
    if registry.get("registry_id") != scope_registry_identity(registry):
        raise ValueError("Historical scope registry identity is invalid")

    records = registry.get("records")
    if type(records) is not list or len(records) != len(canonical):
        raise ValueError("Historical scope registry is incomplete")
    by_identity = {record.get("contract_identity"): record for record in records}
    if set(by_identity) != set(canonical) or len(by_identity) != len(records):
        raise ValueError("Historical scope registry record identities are incomplete")
    if registry.get("unresolved_identities"):
        raise ValueError("Historical scope registry contains unresolved identities")

    stable_positives = sorted(
        identity
        for identity, record in by_identity.items()
        if record.get("is_stablecoin_underlying") is True
    )
    leveraged_positives = sorted(
        identity
        for identity, record in by_identity.items()
        if record.get("is_leveraged_token") is True
    )
    if registry.get("stablecoin_positive_exclusions") != stable_positives:
        raise ValueError("Stablecoin positive-exclusion index disagrees with registry records")
    if registry.get("leveraged_token_positive_exclusions") != leveraged_positives:
        raise ValueError("Leveraged-token positive-exclusion index disagrees with registry records")

    for record in records:
        if record.get("scope_audit_status") != "complete":
            raise ValueError("Reviewed scope registry contains an incomplete disposition")
        stable = record.get("is_stablecoin_underlying")
        leveraged = record.get("is_leveraged_token")
        if stable is True:
            if record.get("stablecoin_evidence_status") != "positive_exclusion_evidence":
                raise ValueError("Stablecoin positive has the wrong evidence status")
            _verify_positive_evidence(record, "stablecoin")
        elif stable is False and record.get("stablecoin_evidence_status") != (
            "reviewed_finite_universe_negative"
        ):
            raise ValueError("Stablecoin finite-universe negative has the wrong evidence status")
        if leveraged is True:
            if record.get("leveraged_evidence_status") != "positive_exclusion_evidence":
                raise ValueError("Leveraged-token positive has the wrong evidence status")
            _verify_positive_evidence(record, "leveraged_token")
        elif leveraged is False and record.get("leveraged_evidence_status") != (
            "reviewed_finite_universe_negative"
        ):
            raise ValueError("Leveraged-token finite-universe negative has the wrong evidence status")

    if require_independent_review:
        if registry_path is None or repository_root is None:
            raise ValueError("Independent scope review requires registry and repository paths")
        _verify_review_artifact(registry, Path(registry_path), Path(repository_root))
    return by_identity
